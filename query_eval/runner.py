"""Suite orchestration for the research-grade part-2 evaluation pipeline.

What this module owns:
    - Child-process execution of one `(dataset, query, mode, repetition)` cell.
    - Deterministic suite orchestration across datasets, queries, and modes.
    - Manifest creation and raw ledger writing.

What this module does not own:
    - Dataset registry definitions.
    - Backend semantics.
    - Aggregate-table logic.

How this relates to the evaluation pipeline:
    This module is the canonical executor for part 2. Notebooks should call into
    it rather than timing ad hoc notebook cells.

Source of truth:
    The canonical execution model is parent-process orchestration plus one fresh
    subprocess per cell repetition.
"""

from __future__ import annotations

import json
import platform
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from .artifacts import ensure_artifacts_for_datasets
from .metrics import compute_correctness_measurement, sample_difference_lines
from .modes import run_mode_query
from .persistence import append_run_record_jsonl, write_json
from .profiling import measure_callable
from .registry import (
    ACTIVE_TEXT_DATASET_SLUGS,
    BASELINE_MODE_NAME,
    MODE_NAMES,
    QUERY_IDS,
    get_dataset_spec,
    get_project_root,
    get_query_payload,
    get_results_root,
)
from .reports import build_reports_from_raw_jsonl
from .specs import CellRunSpec, MemoryMeasurement, RunConfig, RunRecord, TimingMeasurement


def execute_cell_run(cell_run_spec: CellRunSpec, run_config: RunConfig, code_version: str | None = None) -> RunRecord:
    """Execute one cell locally inside the current process.

    Purpose:
        Provide the child-process payload used by the suite runner and CLI.

    Arguments:
        cell_run_spec: Explicit identity of the run.
        run_config: Evaluation configuration.
        code_version: Optional pre-resolved code version string.

    Returns:
        A fully populated `RunRecord`.

    Raises:
        AssertionError: If strict validation detects a full-decompression mismatch.
    """

    dataset_spec = get_dataset_spec(cell_run_spec.dataset_slug)
    artifact_spec = ensure_artifacts_for_datasets([dataset_spec])[dataset_spec.slug]
    query_payload = get_query_payload(dataset_spec.slug, cell_run_spec.query_id)

    candidate_callable = lambda: run_mode_query(
        cell_run_spec.mode_name,
        artifact_spec,
        query_payload,
    )

    if run_config.profiling_enabled:
        candidate_matches, timing, memory = measure_callable(candidate_callable)
    else:
        candidate_matches = candidate_callable()
        timing = TimingMeasurement(wall_time_ms=0.0, cpu_time_ms=0.0)
        memory = MemoryMeasurement(peak_rss_mb=0.0)

    if cell_run_spec.mode_name == BASELINE_MODE_NAME:
        baseline_matches = candidate_matches
    else:
        # Correctness must be computed outside the timed region. The baseline is
        # therefore executed separately and is never folded into the candidate
        # performance measurements.
        baseline_matches = run_mode_query(BASELINE_MODE_NAME, artifact_spec, query_payload)

    correctness = compute_correctness_measurement(baseline_matches, candidate_matches)
    sampled_false_positives, sampled_false_negatives = sample_difference_lines(
        baseline_matches,
        candidate_matches,
        sample_limit=run_config.sample_difference_limit,
    )

    if run_config.strict_validation and cell_run_spec.mode_name == "full_decompression":
        if not correctness.exact_set_match:
            raise AssertionError(
                "full_decompression diverged from decompressed_text, which violates the part-2 validation contract."
            )

    return RunRecord(
        dataset_slug=dataset_spec.slug,
        query_id=cell_run_spec.query_id,
        mode_name=cell_run_spec.mode_name,
        query_payload=query_payload,
        repetition_index=cell_run_spec.repetition_index,
        is_warmup=cell_run_spec.is_warmup,
        artifact_spec=artifact_spec,
        config_label=run_config.config_label,
        config_version=run_config.config_version,
        code_version=code_version or get_code_version(),
        timing=timing,
        memory=memory,
        match_count=len(candidate_matches),
        result_lines=candidate_matches,
        correctness=correctness,
        sampled_false_positives=sampled_false_positives,
        sampled_false_negatives=sampled_false_negatives,
    )


def run_suite(
    dataset_slugs: list[str] | None = None,
    query_ids: list[str] | None = None,
    mode_names: list[str] | None = None,
    run_config: RunConfig | None = None,
) -> Path:
    """Run the full or filtered evaluation suite in fresh subprocesses.

    Purpose:
        Enforce the paper-grade execution model where each measured repetition is
        isolated in its own process.

    Returns:
        The created suite results directory.
    """

    effective_run_config = run_config or RunConfig()
    dataset_slugs = dataset_slugs or list(ACTIVE_TEXT_DATASET_SLUGS)
    query_ids = query_ids or list(QUERY_IDS)
    mode_names = mode_names or list(MODE_NAMES)
    code_version = get_code_version()

    dataset_specs = [get_dataset_spec(dataset_slug) for dataset_slug in dataset_slugs]
    artifact_specs = ensure_artifacts_for_datasets(dataset_specs)

    suite_results_directory = _create_suite_results_directory(effective_run_config)
    raw_jsonl_path = suite_results_directory / "raw_runs.jsonl"

    write_json(
        suite_results_directory / "manifest.json",
        {
            "created_at": datetime.now().isoformat(),
            "platform": platform.platform(),
            "python_executable": sys.executable,
            "code_version": code_version,
            "run_config": {
                "repetitions": effective_run_config.repetitions,
                "warmups": effective_run_config.warmups,
                "profiling_enabled": effective_run_config.profiling_enabled,
                "strict_validation": effective_run_config.strict_validation,
                "config_label": effective_run_config.config_label,
                "config_version": effective_run_config.config_version,
                "sample_difference_limit": effective_run_config.sample_difference_limit,
            },
            "datasets": [dataset_spec.slug for dataset_spec in dataset_specs],
            "queries": query_ids,
            "modes": mode_names,
            "artifact_specs": {
                dataset_slug: artifact_spec.to_json_dict()
                for dataset_slug, artifact_spec in artifact_specs.items()
            },
        },
    )

    for dataset_slug in dataset_slugs:
        for query_id in query_ids:
            for mode_name in mode_names:
                for warmup_index in range(effective_run_config.warmups):
                    warmup_record = _run_cell_in_subprocess(
                        CellRunSpec(
                            dataset_slug=dataset_slug,
                            query_id=query_id,
                            mode_name=mode_name,  # type: ignore[arg-type]
                            repetition_index=warmup_index,
                            is_warmup=True,
                        ),
                        effective_run_config,
                        code_version,
                    )
                    append_run_record_jsonl(raw_jsonl_path, warmup_record)

                for repetition_index in range(effective_run_config.repetitions):
                    measured_record = _run_cell_in_subprocess(
                        CellRunSpec(
                            dataset_slug=dataset_slug,
                            query_id=query_id,
                            mode_name=mode_name,  # type: ignore[arg-type]
                            repetition_index=repetition_index,
                            is_warmup=False,
                        ),
                        effective_run_config,
                        code_version,
                    )
                    append_run_record_jsonl(raw_jsonl_path, measured_record)

    build_reports_from_raw_jsonl(suite_results_directory)
    return suite_results_directory


def get_code_version() -> str:
    """Return the current git revision or a fallback marker."""

    try:
        completed_process = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=get_project_root(),
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        return "unknown"
    return completed_process.stdout.strip() or "unknown"


def _run_cell_in_subprocess(
    cell_run_spec: CellRunSpec,
    run_config: RunConfig,
    code_version: str,
) -> dict[str, Any]:
    """Execute one cell in a fresh subprocess and parse its JSON output."""

    command = [
        sys.executable,
        "-m",
        "query_eval.cli",
        "run-cell",
        "--dataset",
        cell_run_spec.dataset_slug,
        "--query-id",
        cell_run_spec.query_id,
        "--mode",
        cell_run_spec.mode_name,
        "--repetition-index",
        str(cell_run_spec.repetition_index),
        "--config-label",
        run_config.config_label,
        "--config-version",
        run_config.config_version,
        "--sample-difference-limit",
        str(run_config.sample_difference_limit),
        "--code-version",
        code_version,
        "--profiling-enabled",
        str(run_config.profiling_enabled).lower(),
        "--strict-validation",
        str(run_config.strict_validation).lower(),
    ]
    if cell_run_spec.is_warmup:
        command.extend(["--is-warmup", "true"])
    else:
        command.extend(["--is-warmup", "false"])

    completed_process = subprocess.run(
        command,
        cwd=get_project_root(),
        capture_output=True,
        text=True,
    )
    if completed_process.returncode != 0:
        raise RuntimeError(
            "Child cell execution failed.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed_process.stdout}\n"
            f"stderr:\n{completed_process.stderr}"
        )
    return json.loads(completed_process.stdout)


def _create_suite_results_directory(run_config: RunConfig) -> Path:
    """Create a timestamped suite results directory."""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suite_directory = get_results_root() / f"{timestamp}_{run_config.config_label}"
    suite_directory.mkdir(parents=True, exist_ok=False)
    return suite_directory
