"""Execution-mode dispatch for registered part-2 queries.

What this module owns:
    - Mapping mode names to backend implementations.
    - The mode-specific rules for which artifacts are required.

What this module does not own:
    - Dataset staging.
    - Query registration.
    - Metrics and profiling.

How this relates to the evaluation pipeline:
    The runner and notebook-facing query wrappers call into this module to keep
    mode semantics centralized and auditable.

Source of truth:
    Mode names are defined in `query_eval.registry`, and backend semantics are
    implemented in `query_eval.search_backends`.
"""

from __future__ import annotations

from .registry import validate_mode_name
from .search_backends import (
    keyword_search_loglite_binary_full_decompression,
    keyword_search_loglite_binary_minor_optimization,
    keyword_search_plaintext_file,
)
from .specs import ArtifactSpec, ModeName, QueryPayload
from .window_loader import load_l_window_from_txt


def run_mode_query(
    mode_name: str,
    artifact_spec: ArtifactSpec,
    query_payload: QueryPayload,
) -> list[str]:
    """Execute one registered query payload under one mode.

    Purpose:
        Centralize all mode-dependent branching so query wrappers and runners can
        remain mode-agnostic.

    Arguments:
        mode_name: One of the three registered execution modes.
        artifact_spec: Artifact bundle for the selected dataset.
        query_payload: Concrete keyword or keyword tuple for the query.

    Returns:
        The list of matched lines returned by the selected backend.

    Raises:
        ValueError: If the mode name is unsupported.
        FileNotFoundError: If a required artifact is missing.
    """

    validated_mode = validate_mode_name(mode_name)
    if validated_mode == "decompressed_text":
        return _run_decompressed_text_mode(artifact_spec, query_payload)
    if validated_mode == "full_decompression":
        return _run_full_decompression_mode(artifact_spec, query_payload)
    return _run_minor_optimization_mode(artifact_spec, query_payload)


def _run_decompressed_text_mode(artifact_spec: ArtifactSpec, query_payload: QueryPayload) -> list[str]:
    """Execute the baseline plaintext scan over the decompressed artifact."""

    return keyword_search_plaintext_file(artifact_spec.decompressed_text_path, query_payload)


def _run_full_decompression_mode(artifact_spec: ArtifactSpec, query_payload: QueryPayload) -> list[str]:
    """Execute exact bitstream search via faithful sequential decompression."""

    return keyword_search_loglite_binary_full_decompression(
        artifact_spec.compressed_binary_path,
        query_payload,
    )


def _run_minor_optimization_mode(artifact_spec: ArtifactSpec, query_payload: QueryPayload) -> list[str]:
    """Execute the current length-filtering optimization path.

    Notes:
        The parsed final L-window is loaded here rather than cached globally.
        That explicit file-to-mode dependency keeps every run traceable and keeps
        subprocess cells isolated from prior state.
    """

    parsed_l_window = load_l_window_from_txt(artifact_spec.window_path)
    return keyword_search_loglite_binary_minor_optimization(
        artifact_spec.compressed_binary_path,
        parsed_l_window,
        query_payload,
    )
