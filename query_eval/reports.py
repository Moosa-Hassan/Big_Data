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

    output_paths = {
        "cell_level": results_directory / "cell_level_aggregate.csv",
        "query_level": results_directory / "query_level_aggregate.csv",
        "dataset_level": results_directory / "dataset_level_aggregate.csv",
        "suite_summary": results_directory / "suite_summary.csv",
        "static_bloom_summary": results_directory / "static_bloom_summary.csv",
        "query_manifest": results_directory / "query_manifest.csv",
        "dataset_coverage": results_directory / "dataset_coverage.csv",
        "complete_evaluation_summary": results_directory / "complete_evaluation_summary.csv",
    }
    write_csv_rows(output_paths["cell_level"], cell_rows)
    write_csv_rows(output_paths["query_level"], query_rows)
    write_csv_rows(output_paths["dataset_level"], dataset_rows)
    write_csv_rows(output_paths["suite_summary"], suite_rows)
    write_csv_rows(output_paths["static_bloom_summary"], static_bloom_rows)
    write_csv_rows(output_paths["query_manifest"], query_manifest_rows)
    write_csv_rows(output_paths["dataset_coverage"], dataset_coverage_rows)
    write_csv_rows(output_paths["complete_evaluation_summary"], complete_summary_rows)
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


def _safe_rate(numerator: Any, denominator: Any) -> float | None:
    """Compute a rate only when both values are present and denominator is non-zero."""

    if numerator is None or denominator in (None, 0):
        return None
    return float(numerator) / float(denominator)
