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

    output_paths = {
        "cell_level": results_directory / "cell_level_aggregate.csv",
        "query_level": results_directory / "query_level_aggregate.csv",
        "dataset_level": results_directory / "dataset_level_aggregate.csv",
        "suite_summary": results_directory / "suite_summary.csv",
    }
    write_csv_rows(output_paths["cell_level"], cell_rows)
    write_csv_rows(output_paths["query_level"], query_rows)
    write_csv_rows(output_paths["dataset_level"], dataset_rows)
    write_csv_rows(output_paths["suite_summary"], suite_rows)
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
        aggregate_row["all_cells_exact_set_match"] = all(
            row["all_runs_exact_set_match"] for row in group
        )
        aggregate_rows.append(aggregate_row)
    return aggregate_rows


def _median_from(records: list[dict[str, Any]], value_getter) -> float:
    """Return the median of one numeric projection from a record list."""

    return float(median(value_getter(record) for record in records))
