"""Notebook-compatible query entrypoints backed by the registry-driven engine.

What this module owns:
    - The public `(mode_chosen, dataset)` functions required by the project
      notes.
    - The thin wrapper that maps those calls onto the internal registry-driven
      execution model.

What this module does not own:
    - Artifact generation semantics.
    - Mode implementation details.
    - Profiling and reporting.

How this relates to the evaluation pipeline:
    This is the user-facing execution surface for part 2. Notebooks and later
    modules should call these functions instead of re-implementing query logic.

Source of truth:
    Query payloads come from `query_eval.registry`, and execution semantics come
    from `query_eval.modes`.
"""

from __future__ import annotations

from .artifacts import ensure_artifacts_for_dataset
from .modes import run_mode_query
from .registry import get_dataset_spec, get_query_payload


def run_query(mode_chosen: str, dataset: str, query_id: str) -> list[str]:
    """Run one registered query on one dataset under one mode.

    Purpose:
        Bridge the note-compatible public API and the internal evaluation
        architecture without leaking registry or artifact details into callers.

    Arguments:
        mode_chosen: Requested execution mode string.
        dataset: Dataset slug such as `linux` or `apache`.
        query_id: Registered query family id.

    Returns:
        The list of matched lines returned by the chosen mode.

    Raises:
        KeyError: If the dataset or query id is unknown.
        ValueError: If the mode is invalid.
        RuntimeError: If required artifacts cannot be generated.
    """

    dataset_spec = get_dataset_spec(dataset)
    artifact_spec = ensure_artifacts_for_dataset(dataset_spec)
    query_payload = get_query_payload(dataset_spec.slug, query_id)
    return run_mode_query(mode_chosen, artifact_spec, query_payload)


def query_common(mode_chosen: str, dataset: str) -> list[str]:
    """Run the registered `common` query for a dataset."""

    return run_query(mode_chosen, dataset, "common")


def query_phrase(mode_chosen: str, dataset: str) -> list[str]:
    """Run the registered `phrase` query for a dataset."""

    return run_query(mode_chosen, dataset, "phrase")


def query_selective(mode_chosen: str, dataset: str) -> list[str]:
    """Run the registered `selective` query for a dataset."""

    return run_query(mode_chosen, dataset, "selective")


def query_conjunctive(mode_chosen: str, dataset: str) -> list[str]:
    """Run the registered `conjunctive` query for a dataset."""

    return run_query(mode_chosen, dataset, "conjunctive")
