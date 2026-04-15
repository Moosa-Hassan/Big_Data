"""Typed specifications for the research-grade query evaluation subsystem.

What this module owns:
    - The canonical dataclass shapes used across the evaluation pipeline.
    - JSON-safe serialization helpers for those dataclasses.

What this module does not own:
    - Dataset registration.
    - Artifact generation.
    - Query execution.
    - Aggregate report generation.

How this relates to the evaluation pipeline:
    Every other part of the part-2 architecture depends on these types. The
    goal is to prevent schema drift as the codebase grows from the current
    five-dataset setup into the full sixteen-dataset TEXT benchmark.

Source of truth:
    The dataclasses in this file define the authoritative in-memory and
    persisted record shapes for part 2.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from pathlib import Path
from typing import Any, Literal

ModeName = Literal["decompressed_text", "full_decompression", "minor_optimization"]
QueryPayload = str | tuple[str, ...]


@dataclass(frozen=True)
class DatasetSpec:
    """Metadata for one dataset registered in the evaluation suite.

    Purpose:
        Capture the canonical identity, local staging layout, and paper-derived
        dataset characteristics used by the experiment protocol.

    Arguments:
        slug: Stable programmatic identifier used by the Python evaluation code.
        display_name: Human-readable dataset name.
        system_type: Broad category used in the paper's taxonomy.
        loghub_directory_name: Exact LogHub subdirectory name.
        sample_log_filename: Exact 2k sample filename that this suite executes.
        template_csv_filename: Companion template CSV staged for debugging and
            manual curation. This file is intentionally not a runtime dependency
            of the query engine.
        average_length: AL value reported in the LogLite paper.
        number_of_different_lengths: NDL value reported in the LogLite paper.
        is_active_part2: Whether the dataset participates in the active five-
            dataset part-2 experiment matrix.
        paper_notes: Optional textual note describing why the dataset matters.

    Returns:
        Not applicable. This is a value object.

    Side Effects:
        None.

    Notes:
        Paths are derived elsewhere from the project root. This keeps the same
        registry reusable across different machines and staging locations.
    """

    slug: str
    display_name: str
    system_type: str
    loghub_directory_name: str
    sample_log_filename: str
    template_csv_filename: str
    average_length: int
    number_of_different_lengths: int
    is_active_part2: bool = False
    paper_notes: str | None = None


@dataclass(frozen=True)
class ArtifactSpec:
    """Concrete artifact paths for one dataset.

    Purpose:
        Standardize where the raw input, compressed bitstream, decompressed text,
        and L-window dump live so execution code never rebuilds paths ad hoc.

    Arguments:
        raw_log_path: Staged raw LogHub sample used as compression input.
        compressed_binary_path: LogLite-B `.lite.b` bitstream path.
        decompressed_text_path: Full decompression text output path.
        window_path: L-window dump path produced by the extended `xorc-cli`.

    Returns:
        Not applicable. This is a value object.

    Side Effects:
        None.
    """

    raw_log_path: Path
    compressed_binary_path: Path
    decompressed_text_path: Path
    window_path: Path

    def to_json_dict(self) -> dict[str, str]:
        """Return a JSON-safe mapping of artifact paths.

        Purpose:
            Preserve artifact lineage in manifests and raw run ledgers.

        Returns:
            A dictionary whose values are stringified absolute or repo-relative
            paths suitable for JSON serialization.
        """

        return {
            "raw_log_path": str(self.raw_log_path),
            "compressed_binary_path": str(self.compressed_binary_path),
            "decompressed_text_path": str(self.decompressed_text_path),
            "window_path": str(self.window_path),
        }


@dataclass(frozen=True)
class QuerySpec:
    """Definition of one registered query family.

    Purpose:
        Separate query registration from query execution. The dataset payload map
        fixes experiment semantics up front so notebook cells cannot drift away
        from the evaluation protocol.

    Arguments:
        query_id: Stable identifier such as `common` or `conjunctive`.
        family: Descriptive family label used in reports.
        description: Human-readable explanation of what behavior this query is
            intended to stress.
        dataset_payloads: Mapping from dataset slug to the concrete keyword,
            phrase, or conjunction payload used for that dataset.
    """

    query_id: str
    family: str
    description: str
    dataset_payloads: dict[str, QueryPayload]

    def get_payload(self, dataset_slug: str) -> QueryPayload:
        """Return the registered payload for a dataset.

        Arguments:
            dataset_slug: Dataset whose payload should be returned.

        Returns:
            The registered query payload. This is either a single string or a
            tuple of strings for conjunctive queries.

        Raises:
            KeyError: If the query has no payload registered for the dataset.
        """

        try:
            return self.dataset_payloads[dataset_slug]
        except KeyError as error:
            raise KeyError(
                f"Query '{self.query_id}' has no payload for dataset '{dataset_slug}'."
            ) from error


@dataclass(frozen=True)
class RunConfig:
    """Configuration for one evaluation suite execution.

    Purpose:
        Capture execution policy in one explicit object so every run is
        reproducible and traceable.

    Arguments:
        repetitions: Number of measured repetitions per cell.
        warmups: Number of warm-up executions per cell.
        profiling_enabled: Whether to collect wall/CPU/RSS metrics.
        strict_validation: Whether modes that are expected to be exact should
            raise if they diverge from the baseline.
        config_label: Human-readable configuration label written to manifests.
        config_version: Stable version string for the evaluation protocol.
        sample_difference_limit: Maximum number of false-positive or false-
            negative lines to persist per run.
    """

    repetitions: int = 10
    warmups: int = 1
    profiling_enabled: bool = True
    strict_validation: bool = True
    config_label: str = "part2_research_eval"
    config_version: str = "part2.v1"
    sample_difference_limit: int = 10


@dataclass(frozen=True)
class CellRunSpec:
    """Explicit identity of one child-process execution cell.

    Purpose:
        Represent one `(dataset, query, mode, repetition)` invocation in the
        canonical subprocess-based experiment model.
    """

    dataset_slug: str
    query_id: str
    mode_name: ModeName
    repetition_index: int
    is_warmup: bool


@dataclass(frozen=True)
class TimingMeasurement:
    """Wall-clock and CPU timing outputs for one run."""

    wall_time_ms: float
    cpu_time_ms: float


@dataclass(frozen=True)
class MemoryMeasurement:
    """Peak resident-set measurement for one run."""

    peak_rss_mb: float


@dataclass(frozen=True)
class CorrectnessMeasurement:
    """Set-comparison metrics for one candidate mode against the baseline."""

    exact_set_match: bool
    tp: int
    fp: int
    fn: int
    precision: float
    recall: float
    f1: float


@dataclass(frozen=True)
class RunRecord:
    """One raw machine-readable execution record.

    Purpose:
        Persist the smallest self-contained research record for one warm-up or
        measured execution. The record deliberately stores rich context so later
        analysis can be reproduced from raw JSONL alone.

    Arguments:
        dataset_slug: Dataset identity.
        query_id: Registered query family id.
        mode_name: Execution mode used for this run.
        query_payload: Concrete keyword or keyword tuple used for this run.
        repetition_index: Repetition number within the cell.
        is_warmup: Whether the run is a warm-up rather than a measured trial.
        artifact_spec: Artifact lineage used by the run.
        config_label: Human-readable evaluation configuration label.
        config_version: Stable protocol version string.
        code_version: Git revision or fallback version marker.
        timing: Timing measurements for the candidate execution only.
        memory: Memory measurement for the candidate execution only.
        match_count: Number of returned lines.
        result_lines: Full returned line list for auditability.
        correctness: Retrieval metrics against the decompressed-text baseline.
        sampled_false_positives: Deterministic sample of extra candidate lines.
        sampled_false_negatives: Deterministic sample of missed baseline lines.
        decoded_records / decoded_bytes / skipped_records / skipped_bytes /
            fallback_count: Future-facing fields reserved for later
            instrumentation work.
    """

    dataset_slug: str
    query_id: str
    mode_name: ModeName
    query_payload: QueryPayload
    repetition_index: int
    is_warmup: bool
    artifact_spec: ArtifactSpec
    config_label: str
    config_version: str
    code_version: str
    timing: TimingMeasurement
    memory: MemoryMeasurement
    match_count: int
    result_lines: list[str]
    correctness: CorrectnessMeasurement
    sampled_false_positives: list[str]
    sampled_false_negatives: list[str]
    decoded_records: int | None = None
    decoded_bytes: int | None = None
    skipped_records: int | None = None
    skipped_bytes: int | None = None
    fallback_count: int | None = None

    def to_json_dict(self) -> dict[str, Any]:
        """Return a JSON-safe representation of the run record.

        Purpose:
            Preserve raw run outputs in JSONL without relying on external
            serializers or post-processing code.

        Returns:
            A nested dictionary containing only JSON-serializable values.
        """

        return _json_safe(asdict(self))


def _json_safe(value: Any) -> Any:
    """Recursively normalize dataclass payloads into JSON-safe values.

    Purpose:
        Convert nested dataclasses and `Path` objects into plain Python values so
        raw ledgers and manifests stay machine-readable.
    """

    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return _json_safe(asdict(value))
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    return value
