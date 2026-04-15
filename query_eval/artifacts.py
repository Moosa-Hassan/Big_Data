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
from .specs import ArtifactSpec, DatasetSpec


def build_artifact_spec(dataset_spec: DatasetSpec) -> ArtifactSpec:
    """Construct canonical artifact paths for one dataset.

    Purpose:
        Centralize naming conventions so execution code never invents paths.

    Notes:
        The compressed and decompressed artifacts retain the original sample log
        filename in their basename. The window dump uses the sample stem, which
        matches the existing Linux artifact convention already present in the
        repository.
    """

    artifact_root = get_artifact_root()
    sample_stem = Path(dataset_spec.sample_log_filename).stem
    return ArtifactSpec(
        raw_log_path=get_dataset_raw_path(dataset_spec),
        compressed_binary_path=artifact_root / f"{dataset_spec.sample_log_filename}.lite.b",
        decompressed_text_path=artifact_root / f"{dataset_spec.sample_log_filename}.lite.decom",
        window_path=artifact_root / f"{sample_stem}.window.txt",
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


def ensure_dataset_staged(dataset_spec: DatasetSpec, refresh: bool = False) -> None:
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


def stage_active_datasets(dataset_specs: list[DatasetSpec], refresh: bool = False) -> None:
    """Stage a list of datasets deterministically."""

    for dataset_spec in dataset_specs:
        ensure_dataset_staged(dataset_spec, refresh=refresh)


def ensure_artifacts_for_dataset(
    dataset_spec: DatasetSpec,
    force_rebuild: bool = False,
    refresh_dataset: bool = False,
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

    ensure_dataset_staged(dataset_spec, refresh=refresh_dataset)
    artifact_spec = build_artifact_spec(dataset_spec)

    artifact_root = get_artifact_root()
    artifact_root.mkdir(parents=True, exist_ok=True)

    if force_rebuild or not _artifact_bundle_exists(artifact_spec):
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

        completed_process = subprocess.run(
            command,
            cwd=get_project_root(),
            capture_output=True,
            text=True,
        )
        if completed_process.returncode != 0:
            raise RuntimeError(
                "xorc-cli artifact generation failed.\n"
                f"command: {' '.join(command)}\n"
                f"stdout:\n{completed_process.stdout}\n"
                f"stderr:\n{completed_process.stderr}"
            )

    validate_artifact_spec(artifact_spec)
    return artifact_spec


def ensure_artifacts_for_datasets(
    dataset_specs: list[DatasetSpec],
    force_rebuild: bool = False,
    refresh_dataset: bool = False,
) -> dict[str, ArtifactSpec]:
    """Ensure artifacts for multiple datasets and return them by slug."""

    artifact_specs: dict[str, ArtifactSpec] = {}
    for dataset_spec in dataset_specs:
        artifact_specs[dataset_spec.slug] = ensure_artifacts_for_dataset(
            dataset_spec,
            force_rebuild=force_rebuild,
            refresh_dataset=refresh_dataset,
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


def _download_if_needed(url: str, output_path: Path, refresh: bool) -> None:
    """Download one file when it is missing or a refresh is requested."""

    if output_path.exists() and not refresh:
        return
    with urllib.request.urlopen(url) as response:
        output_path.write_bytes(response.read())


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


def _local_xorc_binary_path() -> Path:
    """Return the local build output path for the host-compatible `xorc-cli`."""

    runtime_bin_root = get_runtime_root() / "bin"
    platform_name = platform.system().lower()
    machine_name = platform.machine().lower()
    return runtime_bin_root / f"xorc-cli-{platform_name}-{machine_name}"


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
        if binary_path == _local_xorc_binary_path():
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
