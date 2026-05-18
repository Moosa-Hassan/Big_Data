"""Aggregate report builders for raw part-2 experiment ledgers.

What this module owns:
    - JSONL-to-CSV aggregation for cell-, query-, dataset-, and suite-level
      summaries.

What this module does not own:
    - Running experiments.
    - Raw result persistence.
    - Plotting or notebook presentation.

How this relates to the evaluation pipeline:
    The runner writes raw JSONL first. This module then regenerates all summary
    tables from raw records so aggregate outputs remain reproducible.

Source of truth:
    Raw JSONL run records are the canonical input to all summaries built here.
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path
from statistics import median
from typing import Any

from .persistence import read_jsonl, write_csv_rows
from .registry import get_dataset_registry, get_query_registry


def build_reports_from_raw_jsonl(results_directory: Path) -> dict[str, Path]:
    """Build all aggregate CSVs from a raw JSONL ledger.

    Arguments:
        results_directory: Suite results directory containing `raw_runs.jsonl`.

    Returns:
        A mapping from report name to written CSV path.
    """

    raw_records = read_jsonl(results_directory / "raw_runs.jsonl")
    measured_records = [record for record in raw_records if not record.get("is_warmup", False)]

    cell_rows = _build_cell_rows(measured_records)
    query_rows = _aggregate_rows(cell_rows, ["query_id", "mode_name"], "query_level")
    dataset_rows = _aggregate_rows(cell_rows, ["dataset_slug", "mode_name"], "dataset_level")
    suite_rows = _aggregate_rows(cell_rows, ["mode_name"], "suite_summary")
    static_bloom_rows = _build_static_bloom_rows(cell_rows)
    query_manifest_rows = _build_query_manifest_rows()
    dataset_coverage_rows = _build_dataset_coverage_rows()
    complete_summary_rows = _build_complete_summary_rows(cell_rows)
    external_baseline_rows = _build_external_baseline_rows(cell_rows)
    planner_rows = _build_planner_strategy_rows(cell_rows)
    qidx_size_rows = _build_qidx_size_rows(results_directory)
    amortization_rows = _build_amortization_rows(results_directory, cell_rows)

    output_paths = {
        "cell_level": results_directory / "cell_level_aggregate.csv",
        "query_level": results_directory / "query_level_aggregate.csv",
        "dataset_level": results_directory / "dataset_level_aggregate.csv",
        "suite_summary": results_directory / "suite_summary.csv",
        "static_bloom_summary": results_directory / "static_bloom_summary.csv",
        "query_manifest": results_directory / "query_manifest.csv",
        "dataset_coverage": results_directory / "dataset_coverage.csv",
        "complete_evaluation_summary": results_directory / "complete_evaluation_summary.csv",
        "external_baseline_summary": results_directory / "external_baseline_summary.csv",
        "planner_strategy_summary": results_directory / "planner_strategy_summary.csv",
        "qidx_size_summary": results_directory / "qidx_size_summary.csv",
        "amortization_summary": results_directory / "amortization_summary.csv",
        "adversarial_publishability_report": results_directory / "adversarial_publishability_report.md",
    }
    write_csv_rows(output_paths["cell_level"], cell_rows)
    write_csv_rows(output_paths["query_level"], query_rows)
    write_csv_rows(output_paths["dataset_level"], dataset_rows)
    write_csv_rows(output_paths["suite_summary"], suite_rows)
    write_csv_rows(output_paths["static_bloom_summary"], static_bloom_rows)
    write_csv_rows(output_paths["query_manifest"], query_manifest_rows)
    write_csv_rows(output_paths["dataset_coverage"], dataset_coverage_rows)
    write_csv_rows(output_paths["complete_evaluation_summary"], complete_summary_rows)
    write_csv_rows(output_paths["external_baseline_summary"], external_baseline_rows)
    write_csv_rows(output_paths["planner_strategy_summary"], planner_rows)
    write_csv_rows(output_paths["qidx_size_summary"], qidx_size_rows)
    write_csv_rows(output_paths["amortization_summary"], amortization_rows)
    output_paths["adversarial_publishability_report"].write_text(
        _build_adversarial_publishability_report(cell_rows, qidx_size_rows, amortization_rows),
        encoding="utf-8",
    )
    return output_paths


def _build_cell_rows(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate raw measured records into cell-level summaries."""

    grouped_records: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        key = (record["dataset_slug"], record["query_id"], record["mode_name"])
        grouped_records[key].append(record)

    rows: list[dict[str, Any]] = []
    for dataset_slug, query_id, mode_name in sorted(grouped_records):
        group = grouped_records[(dataset_slug, query_id, mode_name)]
        rows.append(
            {
                "dataset_slug": dataset_slug,
                "query_id": query_id,
                "mode_name": mode_name,
                "scale": _first_present(group, lambda record: record.get("scale")),
                "effective_line_count": _first_present(group, lambda record: record.get("effective_line_count")),
                "measured_repetitions": len(group),
                "median_wall_time_ms": _median_from(group, lambda record: record["timing"]["wall_time_ms"]),
                "median_cpu_time_ms": _median_from(group, lambda record: record["timing"]["cpu_time_ms"]),
                "median_peak_rss_mb": _median_from(group, lambda record: record["memory"]["peak_rss_mb"]),
                "median_match_count": _median_from(group, lambda record: record["match_count"]),
                "median_tp": _median_from(group, lambda record: record["correctness"]["tp"]),
                "median_fp": _median_from(group, lambda record: record["correctness"]["fp"]),
                "median_fn": _median_from(group, lambda record: record["correctness"]["fn"]),
                "median_precision": _median_from(group, lambda record: record["correctness"]["precision"]),
                "median_recall": _median_from(group, lambda record: record["correctness"]["recall"]),
                "median_f1": _median_from(group, lambda record: record["correctness"]["f1"]),
                "median_decoded_records": _median_optional_from(group, lambda record: record.get("decoded_records")),
                "median_decoded_bytes": _median_optional_from(group, lambda record: record.get("decoded_bytes")),
                "median_skipped_records": _median_optional_from(group, lambda record: record.get("skipped_records")),
                "median_skipped_bytes": _median_optional_from(group, lambda record: record.get("skipped_bytes")),
                "median_fallback_count": _median_optional_from(group, lambda record: record.get("fallback_count")),
                "median_bloom_rejected_records": _median_optional_from(
                    group, lambda record: record.get("bloom_rejected_records")
                ),
                "median_bloom_candidate_records": _median_optional_from(
                    group, lambda record: record.get("bloom_candidate_records")
                ),
                "median_total_records": _median_optional_from(group, lambda record: record.get("total_records")),
                "median_bloom_skip_rate": _median_optional_from(
                    group,
                    lambda record: _safe_rate(
                        record.get("bloom_rejected_records"),
                        record.get("total_records"),
                    ),
                ),
                "planner_strategy": _majority_present(group, lambda record: record.get("planner_strategy")),
                "median_postings_lists_touched": _median_optional_from(
                    group, lambda record: record.get("postings_lists_touched")
                ),
                "median_postings_ids_read": _median_optional_from(group, lambda record: record.get("postings_ids_read")),
                "median_verified_records": _median_optional_from(group, lambda record: record.get("verified_records")),
                "median_verified_bytes": _median_optional_from(group, lambda record: record.get("verified_bytes")),
                "all_runs_exact_set_match": all(
                    record["correctness"]["exact_set_match"] for record in group
                ),
                "any_run_exact_set_match": any(
                    record["correctness"]["exact_set_match"] for record in group
                ),
            }
        )
    return rows


def _aggregate_rows(
    rows: list[dict[str, Any]],
    group_fields: list[str],
    level_label: str,
) -> list[dict[str, Any]]:
    """Aggregate already summarized rows into a coarser report level."""

    grouped_rows: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = tuple(row[field] for field in group_fields)
        grouped_rows[key].append(row)

    aggregate_rows: list[dict[str, Any]] = []
    for key in sorted(grouped_rows):
        group = grouped_rows[key]
        aggregate_row = {field: value for field, value in zip(group_fields, key)}
        aggregate_row["aggregate_level"] = level_label
        aggregate_row["cell_count"] = len(group)
        aggregate_row["median_wall_time_ms"] = _median_from(group, lambda row: row["median_wall_time_ms"])
        aggregate_row["median_cpu_time_ms"] = _median_from(group, lambda row: row["median_cpu_time_ms"])
        aggregate_row["median_peak_rss_mb"] = _median_from(group, lambda row: row["median_peak_rss_mb"])
        aggregate_row["median_match_count"] = _median_from(group, lambda row: row["median_match_count"])
        aggregate_row["median_tp"] = _median_from(group, lambda row: row["median_tp"])
        aggregate_row["median_fp"] = _median_from(group, lambda row: row["median_fp"])
        aggregate_row["median_fn"] = _median_from(group, lambda row: row["median_fn"])
        aggregate_row["median_precision"] = _median_from(group, lambda row: row["median_precision"])
        aggregate_row["median_recall"] = _median_from(group, lambda row: row["median_recall"])
        aggregate_row["median_f1"] = _median_from(group, lambda row: row["median_f1"])
        aggregate_row["median_decoded_records"] = _median_optional_from(
            group, lambda row: row.get("median_decoded_records")
        )
        aggregate_row["median_decoded_bytes"] = _median_optional_from(group, lambda row: row.get("median_decoded_bytes"))
        aggregate_row["median_skipped_records"] = _median_optional_from(
            group, lambda row: row.get("median_skipped_records")
        )
        aggregate_row["median_skipped_bytes"] = _median_optional_from(group, lambda row: row.get("median_skipped_bytes"))
        aggregate_row["median_fallback_count"] = _median_optional_from(group, lambda row: row.get("median_fallback_count"))
        aggregate_row["median_bloom_rejected_records"] = _median_optional_from(
            group, lambda row: row.get("median_bloom_rejected_records")
        )
        aggregate_row["median_bloom_candidate_records"] = _median_optional_from(
            group, lambda row: row.get("median_bloom_candidate_records")
        )
        aggregate_row["median_total_records"] = _median_optional_from(group, lambda row: row.get("median_total_records"))
        aggregate_row["median_bloom_skip_rate"] = _median_optional_from(
            group, lambda row: row.get("median_bloom_skip_rate")
        )
        aggregate_row["all_cells_exact_set_match"] = all(
            row["all_runs_exact_set_match"] for row in group
        )
        aggregate_rows.append(aggregate_row)
    return aggregate_rows


def _build_static_bloom_rows(cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build the static-Bloom report table used by visualization and writing."""

    rows_by_key = {(row["dataset_slug"], row["query_id"], row["mode_name"]): row for row in cell_rows}
    query_registry = get_query_registry()
    rows: list[dict[str, Any]] = []
    for dataset_slug, query_id, mode_name in sorted(rows_by_key):
        if mode_name != "static_bloom":
            continue
        static_row = rows_by_key[(dataset_slug, query_id, mode_name)]
        baseline_row = rows_by_key.get((dataset_slug, query_id, "decompressed_text"))
        full_row = rows_by_key.get((dataset_slug, query_id, "full_decompression"))
        speedup = None
        if full_row is not None and static_row["median_wall_time_ms"]:
            speedup = full_row["median_wall_time_ms"] / static_row["median_wall_time_ms"]
        query_spec = query_registry[query_id]
        rows.append(
            {
                "dataset_slug": dataset_slug,
                "query_id": query_id,
                "token_safe": query_spec.token_safe,
                "is_stress_query": query_spec.is_stress_query,
                "expected_selectivity_band": query_spec.expected_selectivity_band,
                "baseline_match_count": baseline_row["median_match_count"] if baseline_row else None,
                "static_bloom_match_count": static_row["median_match_count"],
                "false_positives": static_row["median_fp"],
                "false_negatives": static_row["median_fn"],
                "precision": static_row["median_precision"],
                "recall": static_row["median_recall"],
                "f1": static_row["median_f1"],
                "median_wall_time_ms": static_row["median_wall_time_ms"],
                "speedup_vs_full_decompression": speedup,
                "bloom_skip_rate": static_row.get("median_bloom_skip_rate"),
                "bloom_rejected_records": static_row.get("median_bloom_rejected_records"),
                "bloom_candidate_records": static_row.get("median_bloom_candidate_records"),
                "total_records": static_row.get("median_total_records"),
                "exact_set_match": static_row["all_runs_exact_set_match"],
            }
        )
    return rows


def _build_query_manifest_rows() -> list[dict[str, Any]]:
    """Return one CSV row per locked dataset/query payload."""

    rows: list[dict[str, Any]] = []
    for query_id, query_spec in get_query_registry().items():
        for dataset_slug, payload in sorted(query_spec.dataset_payloads.items()):
            rows.append(
                {
                    "dataset_slug": dataset_slug,
                    "query_id": query_id,
                    "family": query_spec.family,
                    "payload": " && ".join(payload) if isinstance(payload, tuple) else payload,
                    "token_safe": query_spec.token_safe,
                    "is_stress_query": query_spec.is_stress_query,
                    "expected_selectivity_band": query_spec.expected_selectivity_band,
                }
            )
    return rows


def _build_dataset_coverage_rows() -> list[dict[str, Any]]:
    """Return one CSV row per registered dataset with paper metadata."""

    rows: list[dict[str, Any]] = []
    for dataset_slug, dataset_spec in get_dataset_registry().items():
        rows.append(
            {
                "dataset_slug": dataset_slug,
                "display_name": dataset_spec.display_name,
                "system_type": dataset_spec.system_type,
                "average_length": dataset_spec.average_length,
                "number_of_different_lengths": dataset_spec.number_of_different_lengths,
                "sample_log_filename": dataset_spec.sample_log_filename,
            }
        )
    return rows


def _build_complete_summary_rows(cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build token-safe/stress breakdown rows for the complete evaluation."""

    query_registry = get_query_registry()
    enriched_rows = [
        {
            **row,
            "token_safe": query_registry[row["query_id"]].token_safe,
            "is_stress_query": query_registry[row["query_id"]].is_stress_query,
        }
        for row in cell_rows
    ]
    summary_rows: list[dict[str, Any]] = []
    for mode_name in sorted({row["mode_name"] for row in enriched_rows}):
        for label, predicate in (
            ("all_queries", lambda row: True),
            ("token_safe_queries", lambda row: row["token_safe"]),
            ("stress_queries", lambda row: row["is_stress_query"]),
        ):
            group = [row for row in enriched_rows if row["mode_name"] == mode_name and predicate(row)]
            if not group:
                continue
            summary_rows.append(
                {
                    "mode_name": mode_name,
                    "query_group": label,
                    "cell_count": len(group),
                    "exact_cell_count": sum(1 for row in group if row["all_runs_exact_set_match"]),
                    "median_wall_time_ms": _median_from(group, lambda row: row["median_wall_time_ms"]),
                    "median_recall": _median_from(group, lambda row: row["median_recall"]),
                    "median_f1": _median_from(group, lambda row: row["median_f1"]),
                    "median_bloom_skip_rate": _median_optional_from(group, lambda row: row.get("median_bloom_skip_rate")),
                }
            )
    return summary_rows


def _build_external_baseline_rows(cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build a summary focused on grep/ripgrep comparators."""

    rows_by_key = {(row["dataset_slug"], row["query_id"], row["mode_name"]): row for row in cell_rows}
    query_registry = get_query_registry()
    rows: list[dict[str, Any]] = []
    for dataset_slug, query_id, mode_name in sorted(rows_by_key):
        if mode_name not in {"grep_plaintext", "ripgrep_plaintext"}:
            continue
        row = rows_by_key[(dataset_slug, query_id, mode_name)]
        qidx3_python = rows_by_key.get((dataset_slug, query_id, "static_qgram_index_mmap_compact"))
        qidx3_cpp = rows_by_key.get((dataset_slug, query_id, "static_qgram_index_mmap_cpp"))
        query_spec = query_registry[query_id]
        rows.append(
            {
                "dataset_slug": dataset_slug,
                "query_id": query_id,
                "mode_name": mode_name,
                "token_safe": query_spec.token_safe,
                "is_stress_query": query_spec.is_stress_query,
                "median_wall_time_ms": row["median_wall_time_ms"],
                "exact_set_match": row["all_runs_exact_set_match"],
                "speedup_vs_qidx3_python": _safe_divide(
                    qidx3_python["median_wall_time_ms"] if qidx3_python else None,
                    row["median_wall_time_ms"],
                ),
                "speedup_vs_qidx3_cpp": _safe_divide(
                    qidx3_cpp["median_wall_time_ms"] if qidx3_cpp else None,
                    row["median_wall_time_ms"],
                ),
                "baseline_label": (
                    "hybrid_external_postfilter"
                    if str(row.get("planner_strategy") or "").endswith("_hybrid_postfilter")
                    else "external_single_term_or_phrase"
                ),
            }
        )
    return rows


def _build_planner_strategy_rows(cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Summarize qidx3 planner choices and verification work."""

    query_registry = get_query_registry()
    grouped_rows: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in cell_rows:
        if row["mode_name"] not in {"static_qgram_index_mmap_compact", "static_qgram_index_mmap_cpp"}:
            continue
        strategy = row.get("planner_strategy") or "unknown"
        query_spec = query_registry[row["query_id"]]
        group_label = "stress_queries" if query_spec.is_stress_query else "token_safe_queries"
        grouped_rows[(row["mode_name"], group_label, strategy)].append(row)
        grouped_rows[(row["mode_name"], "all_queries", strategy)].append(row)

    rows: list[dict[str, Any]] = []
    for (mode_name, query_group, strategy), group in sorted(grouped_rows.items()):
        rows.append(
            {
                "mode_name": mode_name,
                "query_group": query_group,
                "planner_strategy": strategy,
                "cell_count": len(group),
                "exact_cell_count": sum(1 for row in group if row["all_runs_exact_set_match"]),
                "median_wall_time_ms": _median_from(group, lambda row: row["median_wall_time_ms"]),
                "median_verified_records": _median_optional_from(group, lambda row: row.get("median_verified_records")),
                "median_postings_ids_read": _median_optional_from(
                    group, lambda row: row.get("median_postings_ids_read")
                ),
            }
        )
    return rows


def _build_qidx_size_rows(results_directory: Path) -> list[dict[str, Any]]:
    """Build sidecar size and overhead rows from the suite manifest."""

    manifest_path = results_directory / "manifest.json"
    if not manifest_path.exists():
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    rows: list[dict[str, Any]] = []
    for dataset_slug, artifact_spec in sorted(manifest.get("artifact_specs", {}).items()):
        raw_bytes = _path_size(artifact_spec.get("raw_log_path"))
        static_compressed_bytes = _path_size(artifact_spec.get("static_compressed_binary_path"))
        qidx2_bytes = _path_size(artifact_spec.get("static_qgram_mmap_index_path"))
        qidx3_bytes = _path_size(artifact_spec.get("static_qgram_compact_index_path"))
        rows.append(
            {
                "dataset_slug": dataset_slug,
                "scale": artifact_spec.get("scale"),
                "effective_line_count": artifact_spec.get("effective_line_count"),
                "raw_bytes": raw_bytes,
                "static_compressed_bytes": static_compressed_bytes,
                "qidx2_bytes": qidx2_bytes,
                "qidx3_bytes": qidx3_bytes,
                "qidx3_over_raw_ratio": _safe_divide(qidx3_bytes, raw_bytes),
                "qidx3_over_static_compressed_ratio": _safe_divide(qidx3_bytes, static_compressed_bytes),
                "qidx3_over_qidx2_ratio": _safe_divide(qidx3_bytes, qidx2_bytes),
                "qidx3_smaller_than_qidx2": (
                    None if qidx2_bytes is None or qidx3_bytes is None else qidx3_bytes < qidx2_bytes
                ),
            }
        )
    return rows


def _build_amortization_rows(results_directory: Path, cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Estimate query-count break-even points from build metrics and median cells."""

    build_rows = _read_build_metric_rows(results_directory)
    if not build_rows:
        return []
    rows_by_key = {(row["dataset_slug"], row["query_id"], row["mode_name"]): row for row in cell_rows}
    output_rows: list[dict[str, Any]] = []
    comparators = ("full_decompression", "static_bloom", "grep_plaintext", "ripgrep_plaintext")
    for build_row in build_rows:
        dataset_slug = build_row["dataset_slug"]
        qidx3_build_ms = _to_optional_float(build_row.get("qidx3_build_ms"))
        total_preprocessing_ms = _to_optional_float(build_row.get("total_preprocessing_ms"))
        if total_preprocessing_ms is None:
            total_preprocessing_ms = sum(
                value
                for value in (
                    _to_optional_float(build_row.get("stage_dataset_ms")),
                    _to_optional_float(build_row.get("original_artifacts_ms")),
                    _to_optional_float(build_row.get("static_artifacts_ms")),
                    qidx3_build_ms,
                )
                if value is not None
            )
        for query_id in sorted({row["query_id"] for row in cell_rows if row["dataset_slug"] == dataset_slug}):
            qidx3_row = rows_by_key.get((dataset_slug, query_id, "static_qgram_index_mmap_cpp")) or rows_by_key.get(
                (dataset_slug, query_id, "static_qgram_index_mmap_compact")
            )
            if qidx3_row is None:
                continue
            for comparator in comparators:
                comparator_row = rows_by_key.get((dataset_slug, query_id, comparator))
                if comparator_row is None:
                    continue
                per_query_savings_ms = comparator_row["median_wall_time_ms"] - qidx3_row["median_wall_time_ms"]
                output_rows.append(
                    {
                        "dataset_slug": dataset_slug,
                        "query_id": query_id,
                        "scale": build_row.get("scale"),
                        "qidx3_mode": qidx3_row["mode_name"],
                        "comparator_mode": comparator,
                        "qidx3_build_ms": qidx3_build_ms,
                        "total_preprocessing_ms": total_preprocessing_ms,
                        "qidx3_median_wall_time_ms": qidx3_row["median_wall_time_ms"],
                        "comparator_median_wall_time_ms": comparator_row["median_wall_time_ms"],
                        "per_query_savings_ms": per_query_savings_ms,
                        "break_even_queries_qidx3_build_only": _break_even_queries(
                            qidx3_build_ms,
                            per_query_savings_ms,
                        ),
                        "break_even_queries_total_preprocessing": _break_even_queries(
                            total_preprocessing_ms,
                            per_query_savings_ms,
                        ),
                    }
                )
    return output_rows


def _build_adversarial_publishability_report(
    cell_rows: list[dict[str, Any]],
    qidx_size_rows: list[dict[str, Any]],
    amortization_rows: list[dict[str, Any]],
) -> str:
    """Write an explicit adversarial publishability audit."""

    exact_modes = {
        "full_decompression",
        "static_qgram_index",
        "static_qgram_index_mmap",
        "static_qgram_index_mmap_compact",
        "static_qgram_index_mmap_cpp",
        "grep_plaintext",
        "ripgrep_plaintext",
    }
    exact_failures = [
        row
        for row in cell_rows
        if row["mode_name"] in exact_modes and not row["all_runs_exact_set_match"]
    ]
    stress_rows = [row for row in _enrich_query_rows(cell_rows) if row["is_stress_query"]]
    qidx3_stress_failures = [
        row
        for row in stress_rows
        if row["mode_name"] in {"static_qgram_index_mmap_compact", "static_qgram_index_mmap_cpp"}
        and not row["all_runs_exact_set_match"]
    ]
    qidx3_storage_flags = [
        row
        for row in qidx_size_rows
        if (row.get("qidx3_over_raw_ratio") is not None and row["qidx3_over_raw_ratio"] > 1.0)
        or (row.get("qidx3_over_static_compressed_ratio") is not None and row["qidx3_over_static_compressed_ratio"] > 4.0)
    ]
    qidx3_cpp_vs_external = _summarize_qidx3_external_risk(cell_rows, "static_qgram_index_mmap_cpp")
    qidx3_python_vs_external = _summarize_qidx3_external_risk(cell_rows, "static_qgram_index_mmap_compact")
    break_even_values = [
        row["break_even_queries_qidx3_build_only"]
        for row in amortization_rows
        if row.get("break_even_queries_qidx3_build_only") not in (None, "")
    ]
    median_break_even = float(median(break_even_values)) if break_even_values else None

    lines = [
        "# Adversarial Publishability Report",
        "",
        "## Claim Boundary",
        "",
        "This evaluation supports an exact indexed static LogLite sidecar claim. It should not be framed as pure compressed-domain search, because qidx3 stores a normalized decoded line slab for exact verification.",
        "",
        "## Exactness Audit",
        "",
        f"- Exact-mode failures: {len(exact_failures)} cells.",
        f"- qidx3 stress-query failures: {len(qidx3_stress_failures)} cells.",
        "- Build time is recorded separately in artifact build metrics and is not included in timed query measurements.",
        "",
        "## Runtime Risks",
        "",
        f"- Native C++ qidx3 slower than grep/ripgrep cells: {qidx3_cpp_vs_external['slower_cells']} of {qidx3_cpp_vs_external['compared_cells']} compared cells.",
        f"- Python reference qidx3 slower than grep/ripgrep cells: {qidx3_python_vs_external['slower_cells']} of {qidx3_python_vs_external['compared_cells']} compared cells.",
        "- Plaintext grep/ripgrep remain external baselines; conjunctive payloads are labeled as hybrid post-filtered baselines.",
        "",
        "## Storage Risks",
        "",
        f"- qidx3 storage flags: {len(qidx3_storage_flags)} datasets with qidx3/raw > 1.0 or qidx3/static-compressed > 4.0.",
        "- Report qidx3/raw, qidx3/static-compressed, and qidx3/qidx2 ratios alongside runtime claims.",
        "",
        "## Amortization",
        "",
        f"- Median qidx3-build-only break-even query count: {median_break_even if median_break_even is not None else 'unavailable'}.",
        "- Treat preprocessing as an offline indexing cost and report the amortization point explicitly.",
        "",
        "## Verdict",
        "",
    ]
    if exact_failures or qidx3_stress_failures:
        lines.append("Not publishable yet: exactness failures must be fixed before making performance claims.")
    elif (
        qidx3_cpp_vs_external["compared_cells"]
        and qidx3_cpp_vs_external["slower_cells"] > qidx3_cpp_vs_external["compared_cells"] / 2
    ):
        lines.append("Publishable only with careful framing: exact native qidx3 works, but plaintext grep/ripgrep are often faster.")
    else:
        lines.append("Potentially publishable after 100k/full-scale reruns, provided storage overhead and build amortization are reported honestly.")
    lines.append("")
    return "\n".join(lines)


def _median_from(records: list[dict[str, Any]], value_getter) -> float:
    """Return the median of one numeric projection from a record list."""

    return float(median(value_getter(record) for record in records))


def _median_optional_from(records: list[dict[str, Any]], value_getter) -> float | None:
    """Return the median of present numeric values, or `None` if absent."""

    values = [value_getter(record) for record in records]
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return float(median(numeric_values))


def _first_present(records: list[dict[str, Any]], value_getter) -> Any:
    """Return the first non-empty projected value from a record list."""

    for record in records:
        value = value_getter(record)
        if value not in (None, ""):
            return value
    return None


def _majority_present(records: list[dict[str, Any]], value_getter) -> Any:
    """Return the most frequent non-empty projected value from a record list."""

    counts: dict[Any, int] = defaultdict(int)
    for record in records:
        value = value_getter(record)
        if value not in (None, ""):
            counts[value] += 1
    if not counts:
        return None
    return sorted(counts.items(), key=lambda item: (-item[1], str(item[0])))[0][0]


def _safe_rate(numerator: Any, denominator: Any) -> float | None:
    """Compute a rate only when both values are present and denominator is non-zero."""

    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)


def _safe_divide(numerator: Any, denominator: Any) -> float | None:
    """Return numerator / denominator if both are usable and denominator is non-zero."""

    if numerator in (None, "") or denominator in (None, "", 0):
        return None
    try:
        return float(numerator) / float(denominator)
    except (TypeError, ValueError, ZeroDivisionError):
        return None


def _path_size(raw_path: Any) -> int | None:
    """Return file size for a JSON path value."""

    if raw_path in (None, ""):
        return None
    path = Path(str(raw_path))
    return path.stat().st_size if path.exists() else None


def _read_build_metric_rows(results_directory: Path) -> list[dict[str, Any]]:
    """Read scale-local artifact build metrics referenced by the manifest."""

    manifest_path = results_directory / "manifest.json"
    if not manifest_path.exists():
        return []
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    artifact_specs = manifest.get("artifact_specs", {})
    metric_paths = {
        Path(spec["static_qgram_compact_index_path"]).parent / "artifact_build_metrics.csv"
        for spec in artifact_specs.values()
        if spec.get("static_qgram_compact_index_path")
    }
    rows: list[dict[str, Any]] = []
    for metric_path in sorted(metric_paths):
        if not metric_path.exists() or metric_path.stat().st_size == 0:
            continue
        with metric_path.open("r", encoding="utf-8", newline="") as handle:
            rows.extend(csv.DictReader(handle))
    latest_rows: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in rows:
        key = (row.get("dataset_slug"), row.get("scale"))
        previous = latest_rows.get(key)
        if previous is None or _build_metric_row_score(row) >= _build_metric_row_score(previous):
            latest_rows[key] = row
    return list(latest_rows.values())


def _build_metric_row_score(row: dict[str, Any]) -> tuple[float, float]:
    """Prefer rows that captured actual qidx3 construction over no-op checks."""

    qidx3_build_ms = _to_optional_float(row.get("qidx3_build_ms")) or 0.0
    total_preprocessing_ms = _to_optional_float(row.get("total_preprocessing_ms")) or 0.0
    return qidx3_build_ms, total_preprocessing_ms


def _to_optional_float(value: Any) -> float | None:
    """Convert a possibly-empty CSV value to float."""

    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _break_even_queries(build_ms: Any, per_query_savings_ms: Any) -> float | None:
    """Return the query count needed to amortize a build cost."""

    build_value = _to_optional_float(build_ms)
    savings_value = _to_optional_float(per_query_savings_ms)
    if build_value is None or savings_value is None or savings_value <= 0:
        return None
    return build_value / savings_value


def _enrich_query_rows(cell_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Attach query metadata to cell rows."""

    query_registry = get_query_registry()
    return [
        {
            **row,
            "token_safe": query_registry[row["query_id"]].token_safe,
            "is_stress_query": query_registry[row["query_id"]].is_stress_query,
        }
        for row in cell_rows
    ]


def _summarize_qidx3_external_risk(cell_rows: list[dict[str, Any]], qidx3_mode: str) -> dict[str, int]:
    """Count qidx3 cells that are slower than plaintext external baselines."""

    rows_by_key = {(row["dataset_slug"], row["query_id"], row["mode_name"]): row for row in cell_rows}
    compared_cells = 0
    slower_cells = 0
    for dataset_slug, query_id, mode_name in sorted(rows_by_key):
        if mode_name not in {"grep_plaintext", "ripgrep_plaintext"}:
            continue
        external_row = rows_by_key[(dataset_slug, query_id, mode_name)]
        qidx3_row = rows_by_key.get((dataset_slug, query_id, qidx3_mode))
        if qidx3_row is None:
            continue
        compared_cells += 1
        if qidx3_row["median_wall_time_ms"] > external_row["median_wall_time_ms"]:
            slower_cells += 1
    return {"compared_cells": compared_cells, "slower_cells": slower_cells}
