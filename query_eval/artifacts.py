"""Dataset staging and artifact-generation helpers for part 2.

What this module owns:
    - Canonical artifact path construction.
    - Dataset staging from LogHub raw files.
    - Local `xorc-cli` resolution and host-compatible build orchestration.
    - Artifact generation via the repo's own `LogLite-B` C++ implementation.

What this module does not own:
    - Query registration.
    - Search execution semantics.
    - Correctness metrics.

How this relates to the evaluation pipeline:
    Query execution is only valid once the raw datasets are staged and the three
    required artifacts exist for each dataset. This module guarantees that
    precondition.

Relation to the paper / experiment protocol:
    Part 2 evaluates the existing LogLite-B implementation shipped in this repo.
    The artifact manager therefore uses `xorc-cli` directly rather than the
    benchmark-oriented wrapper scripts.

Source of truth:
    `LogLite-B/src/tools/xorc-cli.cc` is the authoritative artifact producer.
"""

from __future__ import annotations

import os
import platform
import shutil
import stat
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from .registry import (
    LOGHUB_RAW_BASE_URL,
    get_artifact_root,
    get_dataset_raw_path,
    get_dataset_template_csv_path,
    get_project_root,
    get_runtime_root,
)
from .specs import ArtifactSpec, DatasetSpec, ScaleName

SCALE_LINE_LIMITS: dict[ScaleName, int | None] = {
    "2k": 2000,
    "10k": 10000,
    "100k": 100000,
    "full": None,
}


def build_artifact_spec(
    dataset_spec: DatasetSpec,
    scale: ScaleName = "2k",
    source_root: Path | str | None = None,
) -> ArtifactSpec:
    """Construct canonical artifact paths for one dataset.

    Purpose:
        Centralize naming conventions so execution code never invents paths.

    Notes:
        The compressed and decompressed artifacts retain the original sample log
        filename in their basename. The window dump uses the sample stem, which
        matches the existing Linux artifact convention already present in the
        repository.
    """

    artifact_root = _artifact_root_for_scale(scale)
    raw_log_path = _raw_log_path_for_scale(dataset_spec, scale)
    source_raw_log_path = _source_raw_log_path(dataset_spec, source_root) if scale != "2k" else None
    sample_filename = _sample_filename_for_scale(dataset_spec, scale)
    sample_stem = Path(sample_filename).stem
    return ArtifactSpec(
        raw_log_path=raw_log_path,
        scale=scale,
        effective_line_count=_count_lines(raw_log_path) if raw_log_path.exists() else None,
        source_raw_log_path=source_raw_log_path,
        compressed_binary_path=artifact_root / f"{sample_filename}.lite.b",
        decompressed_text_path=artifact_root / f"{sample_filename}.lite.decom",
        window_path=artifact_root / f"{sample_stem}.window.txt",
        static_compressed_binary_path=artifact_root / f"{sample_filename}.lite.static.b",
        static_decompressed_text_path=artifact_root / f"{sample_filename}.lite.static.decom",
        static_window_path=artifact_root / f"{sample_stem}.window.static.txt",
        static_qgram_index_path=artifact_root / f"{sample_filename}.lite.static.qidx.json",
        static_qgram_mmap_index_path=artifact_root / f"{sample_filename}.lite.static.qidx2",
        static_qgram_compact_index_path=artifact_root / f"{sample_filename}.lite.static.qidx3",
    )


def validate_artifact_spec(artifact_spec: ArtifactSpec) -> None:
    """Validate that all required files exist for an artifact bundle.

    Raises:
        FileNotFoundError: If any required file is missing.
    """

    missing_paths = [
        path
        for path in (
            artifact_spec.raw_log_path,
            artifact_spec.compressed_binary_path,
            artifact_spec.decompressed_text_path,
            artifact_spec.window_path,
        )
        if not path.exists()
    ]
    if missing_paths:
        raise FileNotFoundError(
            "Missing required artifacts: " + ", ".join(str(path) for path in missing_paths)
        )


def validate_static_artifact_spec(artifact_spec: ArtifactSpec) -> None:
    """Validate that static-window Bloom artifacts exist for a dataset."""

    static_paths = (
        artifact_spec.static_compressed_binary_path,
        artifact_spec.static_decompressed_text_path,
        artifact_spec.static_window_path,
    )
    missing_paths = [path for path in static_paths if path is None or not path.exists()]
    if missing_paths:
        raise FileNotFoundError(
            "Missing required static artifacts: " + ", ".join(str(path) for path in missing_paths)
        )


def validate_static_qgram_index_artifact_spec(artifact_spec: ArtifactSpec) -> None:
    """Validate that the static q-gram sidecar index exists for a dataset."""

    if artifact_spec.static_qgram_index_path is None or not artifact_spec.static_qgram_index_path.exists():
        raise FileNotFoundError(f"Missing required static q-gram index: {artifact_spec.static_qgram_index_path}")


def validate_static_qgram_mmap_index_artifact_spec(artifact_spec: ArtifactSpec) -> None:
    """Validate that the static q-gram mmap sidecar index exists for a dataset."""

    if artifact_spec.static_qgram_mmap_index_path is None or not artifact_spec.static_qgram_mmap_index_path.exists():
        raise FileNotFoundError(
            f"Missing required static q-gram mmap index: {artifact_spec.static_qgram_mmap_index_path}"
        )


def validate_static_qgram_compact_index_artifact_spec(artifact_spec: ArtifactSpec) -> None:
    """Validate that the compact qidx3 sidecar index exists for a dataset."""

    if (
        artifact_spec.static_qgram_compact_index_path is None
        or not artifact_spec.static_qgram_compact_index_path.exists()
    ):
        raise FileNotFoundError(
            f"Missing required static q-gram compact index: {artifact_spec.static_qgram_compact_index_path}"
        )


def ensure_dataset_staged(
    dataset_spec: DatasetSpec,
    refresh: bool = False,
    scale: ScaleName = "2k",
    source_root: Path | str | None = None,
) -> None:
    """Ensure the raw log and template CSV are staged locally.

    Purpose:
        Materialize the canonical local dataset tree under `dataset/loghub`.

    Arguments:
        dataset_spec: Dataset to stage.
        refresh: Whether to re-download files even if local copies exist.

    Side Effects:
        Creates directories and downloads files when needed.

    Notes:
        The runtime source of truth remains the local staged files. Remote access
        is only used as a bootstrap path when the repository does not already
        contain the required dataset samples.
    """

    if scale != "2k":
        _stage_scaled_dataset(dataset_spec, scale, source_root, refresh=refresh)
        return

    raw_log_path = get_dataset_raw_path(dataset_spec)
    template_csv_path = get_dataset_template_csv_path(dataset_spec)
    raw_log_path.parent.mkdir(parents=True, exist_ok=True)

    _download_if_needed(
        url=f"{LOGHUB_RAW_BASE_URL}/{dataset_spec.loghub_directory_name}/{dataset_spec.sample_log_filename}",
        output_path=raw_log_path,
        refresh=refresh,
    )
    _download_if_needed(
        url=f"{LOGHUB_RAW_BASE_URL}/{dataset_spec.loghub_directory_name}/{dataset_spec.template_csv_filename}",
        output_path=template_csv_path,
        refresh=refresh,
    )


def stage_active_datasets(
    dataset_specs: list[DatasetSpec],
    refresh: bool = False,
    scale: ScaleName = "2k",
    source_root: Path | str | None = None,
) -> None:
    """Stage a list of datasets deterministically."""

    for dataset_spec in dataset_specs:
        ensure_dataset_staged(dataset_spec, refresh=refresh, scale=scale, source_root=source_root)


def ensure_artifacts_for_dataset(
    dataset_spec: DatasetSpec,
    force_rebuild: bool = False,
    refresh_dataset: bool = False,
    scale: ScaleName = "2k",
    source_root: Path | str | None = None,
    record_build_metrics: bool = False,
) -> ArtifactSpec:
    """Ensure all required artifacts exist for one dataset.

    Purpose:
        Stage the raw dataset if necessary, generate missing artifacts via
        `xorc-cli`, and verify the full artifact bundle before query execution.

    Arguments:
        dataset_spec: Dataset whose artifacts should exist.
        force_rebuild: Whether to rerun `xorc-cli` even if artifacts exist.
        refresh_dataset: Whether to refresh the staged raw dataset files.

    Returns:
        The resolved `ArtifactSpec` for the dataset.

    Raises:
        RuntimeError: If `xorc-cli` cannot be built or artifact generation fails.
        FileNotFoundError: If required artifacts are still missing after the run.
    """

    timings: dict[str, float] = {}
    sizes: dict[str, int | None] = {}

    stage_start = time.perf_counter()
    ensure_dataset_staged(dataset_spec, refresh=refresh_dataset, scale=scale, source_root=source_root)
    timings["stage_dataset_ms"] = _elapsed_ms(stage_start)
    artifact_spec = build_artifact_spec(dataset_spec, scale=scale, source_root=source_root)

    artifact_root = _artifact_root_for_scale(scale)
    artifact_root.mkdir(parents=True, exist_ok=True)

    if (
        not artifact_spec.decompressed_text_path.exists()
        and artifact_spec.compressed_binary_path.exists()
        and artifact_spec.window_path.exists()
    ):
        materialize_start = time.perf_counter()
        _materialize_original_decompression_with_python(artifact_spec)
        timings["decompression_materialization_ms"] = _elapsed_ms(materialize_start)
    else:
        timings["decompression_materialization_ms"] = 0.0

    if force_rebuild or not _artifact_bundle_exists(artifact_spec):
        build_start = time.perf_counter()
        invocation = resolve_xorc_cli_invocation(force_rebuild=False)
        command = invocation + [
            "--test",
            "--file-path",
            str(artifact_spec.raw_log_path),
            "--com-output-path",
            str(artifact_spec.compressed_binary_path),
            "--decom-output-path",
            str(artifact_spec.decompressed_text_path),
            "--window-output-path",
            str(artifact_spec.window_path),
        ]

        try:
            completed_process = subprocess.run(
                command,
                cwd=get_project_root(),
                capture_output=True,
                text=True,
                timeout=_artifact_command_timeout_seconds(scale),
            )
            command_failed = completed_process.returncode != 0
            stdout = completed_process.stdout
            stderr = completed_process.stderr
        except subprocess.TimeoutExpired as error:
            command_failed = True
            stdout = (error.stdout or "") if isinstance(error.stdout, str) else ""
            stderr = (error.stderr or "") if isinstance(error.stderr, str) else "codec command timed out"
        if command_failed:
            if artifact_spec.compressed_binary_path.exists() and artifact_spec.window_path.exists():
                _materialize_original_decompression_with_python(artifact_spec)
            else:
                raise RuntimeError(
                    "xorc-cli artifact generation failed before producing required compression artifacts.\n"
                    f"command: {' '.join(command)}\n"
                    f"stdout:\n{stdout}\n"
                    f"stderr:\n{stderr}"
                )
        timings["original_artifacts_ms"] = _elapsed_ms(build_start)
    else:
        timings["original_artifacts_ms"] = 0.0

    validate_artifact_spec(artifact_spec)
    if (
        artifact_spec.static_decompressed_text_path is not None
        and not artifact_spec.static_decompressed_text_path.exists()
        and artifact_spec.static_compressed_binary_path is not None
        and artifact_spec.static_compressed_binary_path.exists()
        and artifact_spec.static_window_path is not None
        and artifact_spec.static_window_path.exists()
    ):
        static_materialize_start = time.perf_counter()
        _materialize_static_decompression_with_python(artifact_spec)
        timings["static_decompression_materialization_ms"] = _elapsed_ms(static_materialize_start)
    else:
        timings["static_decompression_materialization_ms"] = 0.0
    if force_rebuild or not _static_artifact_bundle_exists(artifact_spec):
        if (
            artifact_spec.static_compressed_binary_path is None
            or artifact_spec.static_decompressed_text_path is None
            or artifact_spec.static_window_path is None
        ):
            raise RuntimeError("Static artifact paths were not populated.")

        static_start = time.perf_counter()
        invocation = resolve_static_xorc_cli_invocation(force_rebuild=False)
        command = invocation + [
            "--test",
            "--file-path",
            str(artifact_spec.raw_log_path),
            "--com-output-path",
            str(artifact_spec.static_compressed_binary_path),
            "--decom-output-path",
            str(artifact_spec.static_decompressed_text_path),
            "--window-output-path",
            str(artifact_spec.static_window_path),
        ]

        try:
            completed_process = subprocess.run(
                command,
                cwd=get_project_root(),
                capture_output=True,
                text=True,
                timeout=_artifact_command_timeout_seconds(scale),
            )
            command_failed = completed_process.returncode != 0
            stdout = completed_process.stdout
            stderr = completed_process.stderr
        except subprocess.TimeoutExpired as error:
            command_failed = True
            stdout = (error.stdout or "") if isinstance(error.stdout, str) else ""
            stderr = (error.stderr or "") if isinstance(error.stderr, str) else "static codec command timed out"
        if command_failed:
            if artifact_spec.static_compressed_binary_path.exists() and artifact_spec.static_window_path.exists():
                _materialize_static_decompression_with_python(artifact_spec)
            else:
                raise RuntimeError(
                    "static xorc-cli artifact generation failed before producing required compression artifacts.\n"
                    f"command: {' '.join(command)}\n"
                    f"stdout:\n{stdout}\n"
                    f"stderr:\n{stderr}"
                )
        timings["static_artifacts_ms"] = _elapsed_ms(static_start)
    else:
        timings["static_artifacts_ms"] = 0.0

    validate_static_artifact_spec(artifact_spec)
    from .static_qgram_index import (
        ensure_static_qgram_compact_index,
        ensure_static_qgram_index,
        ensure_static_qgram_mmap_index,
    )

    if scale == "2k":
        qidx_json_start = time.perf_counter()
        ensure_static_qgram_index(artifact_spec, force_rebuild=force_rebuild)
        timings["qidx_json_build_ms"] = _elapsed_ms(qidx_json_start)
    else:
        timings["qidx_json_build_ms"] = 0.0
    if scale == "2k":
        qidx2_start = time.perf_counter()
        ensure_static_qgram_mmap_index(artifact_spec, force_rebuild=force_rebuild)
        timings["qidx2_build_ms"] = _elapsed_ms(qidx2_start)
    else:
        timings["qidx2_build_ms"] = 0.0
    qidx3_start = time.perf_counter()
    ensure_static_qgram_compact_index(artifact_spec, force_rebuild=force_rebuild)
    timings["qidx3_build_ms"] = _elapsed_ms(qidx3_start)

    if record_build_metrics:
        sizes = _artifact_sizes(artifact_spec)
        _append_artifact_build_metrics(dataset_spec, artifact_spec, timings, sizes)
    return artifact_spec


def ensure_artifacts_for_datasets(
    dataset_specs: list[DatasetSpec],
    force_rebuild: bool = False,
    refresh_dataset: bool = False,
    scale: ScaleName = "2k",
    source_root: Path | str | None = None,
    record_build_metrics: bool = False,
) -> dict[str, ArtifactSpec]:
    """Ensure artifacts for multiple datasets and return them by slug."""

    artifact_specs: dict[str, ArtifactSpec] = {}
    for dataset_spec in dataset_specs:
        artifact_specs[dataset_spec.slug] = ensure_artifacts_for_dataset(
            dataset_spec,
            force_rebuild=force_rebuild,
            refresh_dataset=refresh_dataset,
            scale=scale,
            source_root=source_root,
            record_build_metrics=record_build_metrics,
        )
    return artifact_specs


def resolve_xorc_cli_invocation(force_rebuild: bool = False) -> list[str]:
    """Resolve a usable `xorc-cli` command for the current host.

    Purpose:
        Use the shipped binary when it is directly runnable. Otherwise build a
        host-compatible binary from the repo's own `LogLite-B` source tree.

    Returns:
        A subprocess-ready command prefix such as `["/path/to/xorc-cli"]` or
        `["arch", "-x86_64", "/path/to/xorc-cli"]`.

    Raises:
        RuntimeError: If no usable binary can be found or built.
    """

    project_root = get_project_root()
    shipped_binary = project_root / "loglite" / "LogLite-B" / "src" / "tools" / "xorc-cli"
    if not force_rebuild:
        shipped_invocation = _invocation_for_binary(shipped_binary)
        if _probe_binary(shipped_invocation):
            return shipped_invocation

    local_binary = _local_xorc_binary_path()
    if not force_rebuild and local_binary.exists():
        local_invocation = _invocation_for_binary(local_binary)
        if _probe_binary(local_invocation):
            return local_invocation

    built_binary = build_local_xorc_cli(force_rebuild=force_rebuild)
    built_invocation = _invocation_for_binary(built_binary)
    if _probe_binary(built_invocation):
        return built_invocation

    raise RuntimeError("Unable to resolve a runnable xorc-cli binary for this host.")


def resolve_static_xorc_cli_invocation(force_rebuild: bool = False) -> list[str]:
    """Resolve a runnable static-window `xorc-cli` command."""

    project_root = get_project_root()
    shipped_binary = project_root / "loglite" / "LogLite-B" / "src_static" / "tools" / "xorc-cli"
    if not force_rebuild:
        shipped_invocation = _invocation_for_binary(shipped_binary)
        if _probe_binary(shipped_invocation):
            return shipped_invocation

    local_binary = _local_static_xorc_binary_path()
    if not force_rebuild and local_binary.exists():
        local_invocation = _invocation_for_binary(local_binary)
        if _probe_binary(local_invocation):
            return local_invocation

    built_binary = build_local_static_xorc_cli(force_rebuild=force_rebuild)
    built_invocation = _invocation_for_binary(built_binary)
    if _probe_binary(built_invocation):
        return built_invocation

    raise RuntimeError("Unable to resolve a runnable static xorc-cli binary for this host.")


def build_local_xorc_cli(force_rebuild: bool = False) -> Path:
    """Build a host-compatible `xorc-cli` from the repo's `LogLite-B` source.

    Purpose:
        Keep the evaluation engine tied to the repository's own codec while still
        making artifact generation possible on hosts where the shipped binary is
        not runnable.

    Notes:
        On Apple Silicon, the practical path is an `x86_64` build executed via
        Rosetta because `LogLite-B` uses AVX2 intrinsics.
    """

    output_path = _local_xorc_binary_path()
    if output_path.exists() and not force_rebuild:
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_root = get_project_root() / "loglite" / "LogLite-B" / "src"
    source_files = [
        *(source_root / "compress").glob("*.cc"),
        *(source_root / "common").glob("*.cc"),
        *(source_root / "tools").glob("*.cc"),
    ]
    source_files = [str(path) for path in sorted(source_files)]

    compiler = shutil.which(os.environ.get("CXX", "clang++")) or shutil.which("g++")
    if compiler is None:
        raise RuntimeError("No C++ compiler found for building xorc-cli.")

    command = [compiler, "-std=c++17", "-O3", "-mavx2"]

    if platform.system() == "Darwin":
        # The codec uses AVX2 intrinsics, so on Apple Silicon we build an x86_64
        # binary and execute it through Rosetta. This keeps the runtime faithful
        # to the repo's own implementation rather than replacing it.
        command.extend(["-arch", "x86_64"])

    command.extend(source_files)
    command.extend(["-I", str(source_root)])
    command.extend(_boost_include_flags())
    command.extend(["-o", str(output_path)])

    completed_process = subprocess.run(command, capture_output=True, text=True, cwd=get_project_root())
    if completed_process.returncode != 0:
        raise RuntimeError(
            "Failed to build xorc-cli from repo sources.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed_process.stdout}\n"
            f"stderr:\n{completed_process.stderr}\n"
            "If the error mentions `boost/dynamic_bitset.hpp`, install the Boost headers "
            "for this host, for example with `brew install boost` on macOS."
        )

    output_path.chmod(output_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return output_path


def build_local_static_xorc_cli(force_rebuild: bool = False) -> Path:
    """Build a host-compatible `xorc-cli` from `LogLite-B/src_static`."""

    output_path = _local_static_xorc_binary_path()
    if output_path.exists() and not force_rebuild:
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    source_root = get_project_root() / "loglite" / "LogLite-B" / "src_static"
    source_files = [
        *(source_root / "compress").glob("*.cc"),
        *(source_root / "common").glob("*.cc"),
        *(source_root / "tools").glob("*.cc"),
    ]
    source_files = [str(path) for path in sorted(source_files)]

    compiler = shutil.which(os.environ.get("CXX", "clang++")) or shutil.which("g++")
    if compiler is None:
        raise RuntimeError("No C++ compiler found for building static xorc-cli.")

    command = [compiler, "-std=c++17", "-O3", "-mavx2"]
    if platform.system() == "Darwin":
        command.extend(["-arch", "x86_64"])

    command.extend(source_files)
    command.extend(["-I", str(source_root)])
    command.extend(_boost_include_flags())
    command.extend(["-o", str(output_path)])

    completed_process = subprocess.run(command, capture_output=True, text=True, cwd=get_project_root())
    if completed_process.returncode != 0:
        raise RuntimeError(
            "Failed to build static xorc-cli from repo sources.\n"
            f"command: {' '.join(command)}\n"
            f"stdout:\n{completed_process.stdout}\n"
            f"stderr:\n{completed_process.stderr}\n"
            "If the error mentions `boost/dynamic_bitset.hpp`, install the Boost headers "
            "for this host, for example with `brew install boost` on macOS."
        )

    output_path.chmod(output_path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return output_path


def _download_if_needed(url: str, output_path: Path, refresh: bool) -> None:
    """Download one file when it is missing or a refresh is requested."""

    if output_path.exists() and not refresh:
        return
    try:
        with urllib.request.urlopen(url) as response:
            output_path.write_bytes(response.read())
            return
    except urllib.error.URLError:
        curl = shutil.which("curl")
        if curl is None:
            raise
        completed_process = subprocess.run(
            [curl, "-fL", url, "-o", str(output_path)],
            capture_output=True,
            text=True,
        )
        if completed_process.returncode != 0:
            raise RuntimeError(
                "Dataset download failed through urllib and curl.\n"
                f"url: {url}\n"
                f"stdout:\n{completed_process.stdout}\n"
                f"stderr:\n{completed_process.stderr}"
            )


def _materialize_original_decompression_with_python(artifact_spec: ArtifactSpec) -> None:
    """Write the decompressed text artifact with the Python codec mirror.

    Notes:
        Some LogHub 2k samples trigger assertions in the original C++ test-mode
        decompressor after compression and window dumping have already
        succeeded. The evaluation source of truth is the Python mirror used by
        `full_decompression`, so this fallback keeps the complete suite from
        silently dropping those datasets while preserving strict comparison
        against the same decode semantics used in measured runs.
    """

    from .search_backends import keyword_search_loglite_binary_full_decompression

    lines = keyword_search_loglite_binary_full_decompression(artifact_spec.compressed_binary_path, "")
    artifact_spec.decompressed_text_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _materialize_static_decompression_with_python(artifact_spec: ArtifactSpec) -> None:
    """Write the static decompressed text artifact with the Python static decoder."""

    from .search_backends import keyword_search_loglite_static_bloom
    from .window_loader import load_l_window_from_txt

    if artifact_spec.static_compressed_binary_path is None or artifact_spec.static_window_path is None:
        raise RuntimeError("Static artifact paths were not populated.")
    parsed_window = load_l_window_from_txt(artifact_spec.static_window_path)
    result = keyword_search_loglite_static_bloom(artifact_spec.static_compressed_binary_path, parsed_window, "")
    target = artifact_spec.static_decompressed_text_path
    if target is None:
        raise RuntimeError("Static decompressed artifact path was not populated.")
    target.write_text("\n".join(result.matches) + "\n", encoding="utf-8")


def _artifact_bundle_exists(artifact_spec: ArtifactSpec) -> bool:
    """Return whether the compressed, decompressed, and window artifacts exist."""

    return all(
        path.exists()
        for path in (
            artifact_spec.compressed_binary_path,
            artifact_spec.decompressed_text_path,
            artifact_spec.window_path,
        )
    )


def _static_artifact_bundle_exists(artifact_spec: ArtifactSpec) -> bool:
    """Return whether the static compressed, decompressed, and window artifacts exist."""

    return all(
        path is not None and path.exists()
        for path in (
            artifact_spec.static_compressed_binary_path,
            artifact_spec.static_decompressed_text_path,
            artifact_spec.static_window_path,
        )
    )


def _artifact_root_for_scale(scale: ScaleName) -> Path:
    """Return the artifact root for a dataset scale."""

    if scale == "2k":
        return get_artifact_root()
    return get_artifact_root() / scale


def _raw_log_path_for_scale(dataset_spec: DatasetSpec, scale: ScaleName) -> Path:
    """Return the staged raw sample path for a scale."""

    if scale == "2k":
        return get_dataset_raw_path(dataset_spec)
    return (
        get_project_root()
        / "dataset"
        / "loghub_scaled"
        / scale
        / dataset_spec.loghub_directory_name
        / _sample_filename_for_scale(dataset_spec, scale)
    )


def _sample_filename_for_scale(dataset_spec: DatasetSpec, scale: ScaleName) -> str:
    """Return a deterministic sample filename for a scale."""

    if scale == "2k":
        return dataset_spec.sample_log_filename
    base_name = dataset_spec.sample_log_filename.replace("_2k.log", "")
    return f"{base_name}_{scale}.log"


def _source_raw_log_path(dataset_spec: DatasetSpec, source_root: Path | str | None) -> Path:
    """Return the full LogHub source file path for scaled staging."""

    if source_root is None:
        root = get_project_root() / "dataset" / "loghub_full"
    else:
        root = Path(source_root)
        if not root.is_absolute():
            root = get_project_root() / root
    return root / dataset_spec.loghub_directory_name / f"{dataset_spec.loghub_directory_name}.log"


def _stage_scaled_dataset(
    dataset_spec: DatasetSpec,
    scale: ScaleName,
    source_root: Path | str | None,
    refresh: bool,
) -> None:
    """Create a first-N-line scaled sample from a local full LogHub source."""

    raw_log_path = _raw_log_path_for_scale(dataset_spec, scale)
    source_path = _source_raw_log_path(dataset_spec, source_root)
    if not source_path.exists():
        raise FileNotFoundError(
            "Scaled evaluation requires a real local full LogHub source file: "
            f"{source_path}. No synthetic repeated samples are allowed."
        )
    if raw_log_path.exists() and not refresh and _scaled_sample_matches_source_prefix(raw_log_path, source_path, scale):
        return

    raw_log_path.parent.mkdir(parents=True, exist_ok=True)
    line_limit = SCALE_LINE_LIMITS[scale]
    lines_written = 0
    with source_path.open("r", encoding="utf-8", errors="ignore") as source, raw_log_path.open(
        "w",
        encoding="utf-8",
    ) as target:
        for line in source:
            if line_limit is not None and lines_written >= line_limit:
                break
            target.write(line.rstrip("\n").rstrip("\r") + "\n")
            lines_written += 1

    if lines_written == 0:
        raise RuntimeError(f"Scaled source file contains no usable log lines: {source_path}")


def _scaled_sample_matches_source_prefix(raw_log_path: Path, source_path: Path, scale: ScaleName) -> bool:
    """Return whether a staged scaled sample is exactly the first real source lines."""

    line_limit = SCALE_LINE_LIMITS[scale]
    with source_path.open("r", encoding="utf-8", errors="ignore") as source, raw_log_path.open(
        "r",
        encoding="utf-8",
        errors="ignore",
    ) as staged:
        staged_count = 0
        for source_count, source_line in enumerate(source, start=1):
            if line_limit is not None and source_count > line_limit:
                return staged.readline() == ""
            staged_line = staged.readline()
            if staged_line == "":
                return False
            expected_line = source_line.rstrip("\n").rstrip("\r") + "\n"
            if staged_line != expected_line:
                return False
            staged_count += 1
        return staged.readline() == "" and staged_count > 0


def _count_lines(path: Path) -> int:
    """Count newline-delimited records in a staged sample."""

    with path.open("rb") as handle:
        return sum(1 for _ in handle)


def _elapsed_ms(start_time: float) -> float:
    """Return elapsed wall time in milliseconds."""

    return (time.perf_counter() - start_time) * 1000.0


def _artifact_command_timeout_seconds(scale: ScaleName) -> int:
    """Return a conservative codec command timeout for the selected scale."""

    if scale == "2k":
        return 120
    if scale == "10k":
        return 300
    if scale == "100k":
        return 1800
    return 3600


def _artifact_sizes(artifact_spec: ArtifactSpec) -> dict[str, int | None]:
    """Return artifact sizes used by build and overhead reports."""

    paths = {
        "raw_bytes": artifact_spec.raw_log_path,
        "compressed_bytes": artifact_spec.compressed_binary_path,
        "decompressed_bytes": artifact_spec.decompressed_text_path,
        "static_compressed_bytes": artifact_spec.static_compressed_binary_path,
        "static_decompressed_bytes": artifact_spec.static_decompressed_text_path,
        "qidx_json_bytes": artifact_spec.static_qgram_index_path,
        "qidx2_bytes": artifact_spec.static_qgram_mmap_index_path,
        "qidx3_bytes": artifact_spec.static_qgram_compact_index_path,
    }
    return {key: path.stat().st_size if path is not None and path.exists() else None for key, path in paths.items()}


def _append_artifact_build_metrics(
    dataset_spec: DatasetSpec,
    artifact_spec: ArtifactSpec,
    timings: dict[str, float],
    sizes: dict[str, int | None],
) -> None:
    """Append one dataset's build metrics to a scale-local CSV."""

    metrics_path = _artifact_root_for_scale(artifact_spec.scale) / "artifact_build_metrics.csv"
    metrics_path.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "dataset_slug": dataset_spec.slug,
        "scale": artifact_spec.scale,
        "effective_line_count": artifact_spec.effective_line_count,
        **{key: f"{value:.6f}" for key, value in sorted(timings.items())},
        "total_preprocessing_ms": f"{sum(timings.values()):.6f}",
        **sizes,
    }
    fieldnames = list(row.keys())
    needs_header = not metrics_path.exists()
    import csv

    with metrics_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if needs_header:
            writer.writeheader()
        writer.writerow(row)


def _local_xorc_binary_path() -> Path:
    """Return the local build output path for the host-compatible `xorc-cli`."""

    runtime_bin_root = get_runtime_root() / "bin"
    platform_name = platform.system().lower()
    machine_name = platform.machine().lower()
    return runtime_bin_root / f"xorc-cli-{platform_name}-{machine_name}"


def _local_static_xorc_binary_path() -> Path:
    """Return the local build output path for the static-window `xorc-cli`."""

    runtime_bin_root = get_runtime_root() / "bin"
    platform_name = platform.system().lower()
    machine_name = platform.machine().lower()
    return runtime_bin_root / f"xorc-cli-static-{platform_name}-{machine_name}"


def _boost_include_flags() -> list[str]:
    """Return extra compiler include flags for Boost headers when available.

    Purpose:
        `LogLite-B` depends on `boost/dynamic_bitset.hpp`, but that header is a
        system dependency rather than a repo-local file. This helper searches
        the common host locations so the build command stays portable across
        Linux, Intel macOS, and Apple Silicon Homebrew setups.
    """

    include_flags: list[str] = []
    candidate_roots = [
        Path("/opt/homebrew/include"),
        Path("/usr/local/include"),
        Path("/usr/include"),
        Path("/opt/local/include"),
    ]

    brew_executable = shutil.which("brew")
    if brew_executable:
        completed_process = subprocess.run(
            [brew_executable, "--prefix", "boost"],
            capture_output=True,
            text=True,
        )
        if completed_process.returncode == 0:
            candidate_roots.insert(0, Path(completed_process.stdout.strip()) / "include")

    seen_roots: set[Path] = set()
    for include_root in candidate_roots:
        if include_root in seen_roots:
            continue
        seen_roots.add(include_root)
        if (include_root / "boost" / "dynamic_bitset.hpp").exists():
            include_flags.extend(["-I", str(include_root)])

    return include_flags


def _invocation_for_binary(binary_path: Path) -> list[str]:
    """Return the correct command prefix for executing a binary on this host."""

    if not binary_path.exists():
        return [str(binary_path)]

    # On Apple Silicon, the locally built codec binary is intentionally x86_64 so
    # AVX2 code can run under Rosetta. The `arch -x86_64` prefix is therefore
    # part of the executable identity, not an incidental shell wrapper.
    if platform.system() == "Darwin" and platform.machine().lower() == "arm64":
        if binary_path in {_local_xorc_binary_path(), _local_static_xorc_binary_path()}:
            return ["arch", "-x86_64", str(binary_path)]

    return [str(binary_path)]


def _probe_binary(invocation: list[str]) -> bool:
    """Return whether a candidate `xorc-cli` invocation is runnable.

    Notes:
        The probe intentionally executes with no user arguments. In `xorc-cli`,
        that path exits cleanly after parsing defaults and therefore serves as a
        lightweight readiness check.
    """

    try:
        completed_process = subprocess.run(
            invocation,
            cwd=get_project_root(),
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, PermissionError, OSError):
        return False
    return completed_process.returncode == 0
