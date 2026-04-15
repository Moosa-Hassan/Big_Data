"""Canonical registries and path conventions for part-2 evaluation.

What this module owns:
    - Supported mode names.
    - Registered TEXT datasets from the LogLite paper.
    - Registered query families and dataset-specific payloads.
    - Shared path conventions for datasets, artifacts, and evaluation outputs.

What this module does not own:
    - Downloading datasets.
    - Building `xorc-cli`.
    - Running queries.
    - Computing metrics.

How this relates to the evaluation pipeline:
    Every execution path begins here. Other modules must resolve datasets,
    queries, modes, and directory roots through this registry rather than
    hard-coding literals.

Relation to the paper / experiment protocol:
    The AL and NDL values stored here are the dataset-diversity metadata used to
    justify the active five-dataset evaluation set.

Source of truth:
    This module is the only approved place to define supported datasets,
    supported query families, and supported execution modes.
"""

from __future__ import annotations

from pathlib import Path

from .specs import DatasetSpec, ModeName, QueryPayload, QuerySpec

MODE_NAMES: tuple[ModeName, ...] = (
    "decompressed_text",
    "full_decompression",
    "minor_optimization",
)
BASELINE_MODE_NAME: ModeName = "decompressed_text"
QUERY_IDS: tuple[str, ...] = ("common", "phrase", "selective", "conjunctive")
ACTIVE_TEXT_DATASET_SLUGS: tuple[str, ...] = (
    "linux",
    "apache",
    "hdfs",
    "openstack",
    "android",
)
LOGHUB_RAW_BASE_URL = "https://raw.githubusercontent.com/logpai/loghub/master"


def get_project_root() -> Path:
    """Return the `Big_Data` repository root.

    Purpose:
        Centralize project-root discovery so every path convention in the
        evaluation package resolves from the same anchor.
    """

    return Path(__file__).resolve().parents[1]


def get_dataset_root() -> Path:
    """Return the canonical local staging directory for LogHub samples."""

    return get_project_root() / "dataset" / "loghub"


def get_artifact_root() -> Path:
    """Return the canonical artifact directory for compressed evaluation data."""

    return get_project_root() / "compressed_logs"


def get_results_root() -> Path:
    """Return the root directory for reproducible evaluation outputs."""

    return get_project_root() / "evaluation_results" / "query_eval"


def get_runtime_root() -> Path:
    """Return the runtime build/cache directory used by the evaluator."""

    return get_project_root() / ".query_eval_runtime"


def get_dataset_registry() -> dict[str, DatasetSpec]:
    """Return the full registry of sixteen TEXT datasets.

    Returns:
        An insertion-ordered mapping keyed by dataset slug.

    Notes:
        The active five datasets are intentionally a subset of this registry so
        later expansion to the full TEXT benchmark requires no architectural
        redesign.
    """

    return {
        "linux": DatasetSpec(
            slug="linux",
            display_name="Linux",
            system_type="operating systems",
            loghub_directory_name="Linux",
            sample_log_filename="Linux_2k.log",
            template_csv_filename="Linux_2k.log_templates.csv",
            average_length=91,
            number_of_different_lengths=208,
            is_active_part2=True,
            paper_notes="Control dataset and continuity with existing notebook work.",
        ),
        "apache": DatasetSpec(
            slug="apache",
            display_name="Apache",
            system_type="server applications",
            loghub_directory_name="Apache",
            sample_log_filename="Apache_2k.log",
            template_csv_filename="Apache_2k.log_templates.csv",
            average_length=90,
            number_of_different_lengths=69,
            is_active_part2=True,
            paper_notes="Low-NDL, highly regular dataset.",
        ),
        "hdfs": DatasetSpec(
            slug="hdfs",
            display_name="HDFS",
            system_type="distributed systems",
            loghub_directory_name="HDFS",
            sample_log_filename="HDFS_2k.log",
            template_csv_filename="HDFS_2k.log_templates.csv",
            average_length=140,
            number_of_different_lengths=135,
            is_active_part2=True,
            paper_notes="Distributed-system case with weaker same-length compliance.",
        ),
        "openstack": DatasetSpec(
            slug="openstack",
            display_name="OpenStack",
            system_type="distributed systems",
            loghub_directory_name="OpenStack",
            sample_log_filename="OpenStack_2k.log",
            template_csv_filename="OpenStack_2k.log_templates.csv",
            average_length=295,
            number_of_different_lengths=131,
            is_active_part2=True,
            paper_notes="Long-line stress case.",
        ),
        "android": DatasetSpec(
            slug="android",
            display_name="Android",
            system_type="mobile systems",
            loghub_directory_name="Android",
            sample_log_filename="Android_2k.log",
            template_csv_filename="Android_2k.log_templates.csv",
            average_length=123,
            number_of_different_lengths=720,
            is_active_part2=True,
            paper_notes="High-variation, high-NDL case.",
        ),
        "zookeeper": DatasetSpec(
            slug="zookeeper",
            display_name="Zookeeper",
            system_type="distributed systems",
            loghub_directory_name="Zookeeper",
            sample_log_filename="Zookeeper_2k.log",
            template_csv_filename="Zookeeper_2k.log_templates.csv",
            average_length=139,
            number_of_different_lengths=88,
        ),
        "healthapp": DatasetSpec(
            slug="healthapp",
            display_name="HealthApp",
            system_type="mobile systems",
            loghub_directory_name="HealthApp",
            sample_log_filename="HealthApp_2k.log",
            template_csv_filename="HealthApp_2k.log_templates.csv",
            average_length=92,
            number_of_different_lengths=132,
        ),
        "hpc": DatasetSpec(
            slug="hpc",
            display_name="HPC",
            system_type="supercomputers",
            loghub_directory_name="HPC",
            sample_log_filename="HPC_2k.log",
            template_csv_filename="HPC_2k.log_templates.csv",
            average_length=76,
            number_of_different_lengths=272,
        ),
        "hadoop": DatasetSpec(
            slug="hadoop",
            display_name="Hadoop",
            system_type="distributed systems",
            loghub_directory_name="Hadoop",
            sample_log_filename="Hadoop_2k.log",
            template_csv_filename="Hadoop_2k.log_templates.csv",
            average_length=122,
            number_of_different_lengths=327,
        ),
        "bgl": DatasetSpec(
            slug="bgl",
            display_name="BGL",
            system_type="supercomputers",
            loghub_directory_name="BGL",
            sample_log_filename="BGL_2k.log",
            template_csv_filename="BGL_2k.log_templates.csv",
            average_length=156,
            number_of_different_lengths=235,
        ),
        "mac": DatasetSpec(
            slug="mac",
            display_name="Mac",
            system_type="operating systems",
            loghub_directory_name="Mac",
            sample_log_filename="Mac_2k.log",
            template_csv_filename="Mac_2k.log_templates.csv",
            average_length=143,
            number_of_different_lengths=490,
        ),
        "proxifier": DatasetSpec(
            slug="proxifier",
            display_name="Proxifier",
            system_type="standalone software",
            loghub_directory_name="Proxifier",
            sample_log_filename="Proxifier_2k.log",
            template_csv_filename="Proxifier_2k.log_templates.csv",
            average_length=118,
            number_of_different_lengths=104,
        ),
        "spark": DatasetSpec(
            slug="spark",
            display_name="Spark",
            system_type="distributed systems",
            loghub_directory_name="Spark",
            sample_log_filename="Spark_2k.log",
            template_csv_filename="Spark_2k.log_templates.csv",
            average_length=87,
            number_of_different_lengths=284,
        ),
        "openssh": DatasetSpec(
            slug="openssh",
            display_name="OpenSSH",
            system_type="server applications",
            loghub_directory_name="OpenSSH",
            sample_log_filename="OpenSSH_2k.log",
            template_csv_filename="OpenSSH_2k.log_templates.csv",
            average_length=111,
            number_of_different_lengths=122,
        ),
        "thunderbird": DatasetSpec(
            slug="thunderbird",
            display_name="Thunderbird",
            system_type="supercomputers",
            loghub_directory_name="Thunderbird",
            sample_log_filename="Thunderbird_2k.log",
            template_csv_filename="Thunderbird_2k.log_templates.csv",
            average_length=153,
            number_of_different_lengths=413,
        ),
        "windows": DatasetSpec(
            slug="windows",
            display_name="Windows",
            system_type="operating systems",
            loghub_directory_name="Windows",
            sample_log_filename="Windows_2k.log",
            template_csv_filename="Windows_2k.log_templates.csv",
            average_length=243,
            number_of_different_lengths=654,
        ),
    }


def get_query_registry() -> dict[str, QuerySpec]:
    """Return the part-2 query manifest.

    Returns:
        An insertion-ordered mapping keyed by query id.

    Notes:
        Only the active five datasets receive concrete payloads in part 2. The
        registry-driven design still scales because query coverage is checked at
        registration time rather than through copy-pasted scripts.
    """

    return {
        "common": QuerySpec(
            query_id="common",
            family="high_hit_single_keyword",
            description="High-frequency keyword chosen to stress common template-heavy matches.",
            dataset_payloads={
                "linux": "kernel",
                "apache": "workerEnv",
                "hdfs": "PacketResponder",
                "openstack": "status: 200",
                "android": "PowerManagerService",
            },
        ),
        "phrase": QuerySpec(
            query_id="phrase",
            family="medium_hit_phrase",
            description="Medium-hit phrase chosen to stress stable repeated textual structures.",
            dataset_payloads={
                "linux": "failed",
                "apache": "scoreboard slot",
                "hdfs": "NameSystem.addStoredBlock",
                "openstack": "GET /v2/",
                "android": "WindowManager",
            },
        ),
        "selective": QuerySpec(
            query_id="selective",
            family="low_hit_selective_keyword",
            description="Low-hit token or phrase chosen to expose optimization misses clearly.",
            dataset_payloads={
                "linux": "28842",
                "apache": "Directory index forbidden",
                "hdfs": "replicate blk_",
                "openstack": "Deleting instance files",
                "android": "TextView",
            },
        ),
        "conjunctive": QuerySpec(
            query_id="conjunctive",
            family="multi_keyword_conjunction",
            description="Two-keyword conjunction used to evaluate stricter match semantics.",
            dataset_payloads={
                "linux": ("sshd", "failure"),
                "apache": ("mod_jk", "error"),
                "hdfs": ("PacketResponder", "terminating"),
                "openstack": ("GET /v2/", "status: 200"),
                "android": ("PowerManagerService", "acquire"),
            },
        ),
    }


def get_dataset_spec(dataset_slug: str) -> DatasetSpec:
    """Return one dataset specification by slug.

    Raises:
        KeyError: If the slug is not registered.
    """

    registry = get_dataset_registry()
    try:
        return registry[dataset_slug]
    except KeyError as error:
        raise KeyError(f"Unknown dataset slug: {dataset_slug}") from error


def get_query_spec(query_id: str) -> QuerySpec:
    """Return one query specification by id.

    Raises:
        KeyError: If the query id is not registered.
    """

    registry = get_query_registry()
    try:
        return registry[query_id]
    except KeyError as error:
        raise KeyError(f"Unknown query id: {query_id}") from error


def get_query_payload(dataset_slug: str, query_id: str) -> QueryPayload:
    """Return the concrete payload for one dataset/query pair."""

    return get_query_spec(query_id).get_payload(dataset_slug)


def iter_all_dataset_specs() -> list[DatasetSpec]:
    """Return all registered TEXT datasets in deterministic order."""

    return list(get_dataset_registry().values())


def iter_active_dataset_specs() -> list[DatasetSpec]:
    """Return the active five part-2 datasets in deterministic order."""

    registry = get_dataset_registry()
    return [registry[slug] for slug in ACTIVE_TEXT_DATASET_SLUGS]


def iter_active_query_specs() -> list[QuerySpec]:
    """Return the active four query families in deterministic order."""

    registry = get_query_registry()
    return [registry[query_id] for query_id in QUERY_IDS]


def validate_mode_name(mode_name: str) -> ModeName:
    """Validate and return a mode name.

    Arguments:
        mode_name: User- or caller-provided mode string.

    Returns:
        The validated mode string, narrowed to `ModeName`.

    Raises:
        ValueError: If the mode is unsupported.
    """

    if mode_name not in MODE_NAMES:
        raise ValueError(f"Unsupported mode '{mode_name}'. Expected one of {MODE_NAMES}.")
    return mode_name  # type: ignore[return-value]


def get_dataset_raw_path(dataset_spec: DatasetSpec) -> Path:
    """Resolve the staged raw log path for a dataset."""

    return get_dataset_root() / dataset_spec.loghub_directory_name / dataset_spec.sample_log_filename


def get_dataset_template_csv_path(dataset_spec: DatasetSpec) -> Path:
    """Resolve the staged template CSV path for a dataset."""

    return get_dataset_root() / dataset_spec.loghub_directory_name / dataset_spec.template_csv_filename
