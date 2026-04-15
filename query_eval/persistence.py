"""Persistence helpers for raw and aggregate experiment outputs.

What this module owns:
    - JSON, JSONL, and CSV writing helpers.
    - Deterministic directory creation for result outputs.

What this module does not own:
    - Run orchestration.
    - Metric computation.
    - Report aggregation logic.

How this relates to the evaluation pipeline:
    The runner writes raw ledgers through this module, and the reporting layer
    consumes those ledgers to generate aggregate tables.

Source of truth:
    Raw JSONL written through this module is the canonical persisted record of a
    suite execution.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .specs import RunRecord


def ensure_directory(path: Path) -> None:
    """Create a directory if it does not already exist."""

    path.mkdir(parents=True, exist_ok=True)


def ensure_parent_directory(path: Path) -> None:
    """Create the parent directory for an output file if needed."""

    ensure_directory(path.parent)


def append_run_record_jsonl(path: Path, run_record: RunRecord | dict[str, Any]) -> None:
    """Append one raw run record to a JSONL ledger.

    Arguments:
        path: Target JSONL file.
        run_record: Either a typed `RunRecord` or an already-serialized mapping.
    """

    ensure_parent_directory(path)
    payload = run_record.to_json_dict() if isinstance(run_record, RunRecord) else run_record
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    """Write one JSON payload using deterministic formatting."""

    ensure_parent_directory(path)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")


def write_csv_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    """Write a list of homogeneous dictionaries to CSV."""

    ensure_parent_directory(path)
    if not rows:
        with path.open("w", encoding="utf-8") as handle:
            handle.write("")
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    """Load JSONL records from disk."""

    if not path.exists():
        return []

    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                records.append(json.loads(stripped))
    return records
