"""Command-line entrypoints for reproducible part-2 evaluation runs.

What this module owns:
    - The CLI surface for staging datasets, generating artifacts, running one
      child-process cell, running a whole suite, and rebuilding reports.

What this module does not own:
    - Low-level codec execution logic.
    - Aggregation math.

How this relates to the evaluation pipeline:
    The runner uses this CLI for subprocess isolation, and users can call it
    directly for reproducible non-notebook execution.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .artifacts import ensure_artifacts_for_datasets, stage_active_datasets
from .registry import (
    ACTIVE_TEXT_DATASET_SLUGS,
    COMPLETE_QGRAM_MMAP_SUITE_PROFILE_NAME,
    COMPLETE_QGRAM_SUITE_PROFILE_NAME,
    COMPLETE_SUITE_PROFILE_NAME,
    MODE_NAMES,
    PUBLISHABILITY_QGRAM_COMPACT_PROFILE_NAME,
    QUERY_IDS,
    SUITE_PROFILE_NAMES,
    get_dataset_spec,
    get_suite_profile,
)
from .reports import build_reports_from_raw_jsonl
from .runner import execute_cell_run, run_suite
from .specs import CellRunSpec, RunConfig


def main(argv: list[str] | None = None) -> int:
    """Run the query-evaluation CLI."""

    parser = _build_argument_parser()
    args = parser.parse_args(argv)

    if args.command == "stage-datasets":
        dataset_specs = _resolve_dataset_specs(args.datasets)
        stage_active_datasets(
            dataset_specs,
            refresh=args.refresh,
            scale=args.scale,
            source_root=args.source_root,
        )
        print(
            json.dumps(
                {"staged_datasets": [dataset.slug for dataset in dataset_specs], "scale": args.scale},
                sort_keys=True,
            )
        )
        return 0

    if args.command == "ensure-artifacts":
        dataset_specs = _resolve_dataset_specs(args.datasets)
        artifact_specs = ensure_artifacts_for_datasets(
            dataset_specs,
            force_rebuild=args.force_rebuild,
            refresh_dataset=args.refresh_dataset,
            scale=args.scale,
            source_root=args.source_root,
            record_build_metrics=args.record_build_metrics,
        )
        print(
            json.dumps(
                {
                    "artifact_specs": {
                        slug: artifact_spec.to_json_dict()
                        for slug, artifact_spec in artifact_specs.items()
                    }
                },
                sort_keys=True,
            )
        )
        return 0

    if args.command == "run-cell":
        run_config = RunConfig(
            repetitions=1,
            warmups=0,
            profiling_enabled=_parse_bool(args.profiling_enabled),
            strict_validation=_parse_bool(args.strict_validation),
            config_label=args.config_label,
            config_version=args.config_version,
            sample_difference_limit=args.sample_difference_limit,
            scale=args.scale,
            source_root=args.source_root,
        )
        cell_run_spec = CellRunSpec(
            dataset_slug=args.dataset,
            query_id=args.query_id,
            mode_name=args.mode,
            repetition_index=args.repetition_index,
            is_warmup=_parse_bool(args.is_warmup),
        )
        run_record = execute_cell_run(cell_run_spec, run_config, code_version=args.code_version)
        print(json.dumps(run_record.to_json_dict(), sort_keys=True))
        return 0

    if args.command == "run-suite":
        suite_profile = get_suite_profile(args.profile)
        config_label = args.config_label or args.profile
        if args.config_version is not None:
            config_version = args.config_version
        elif args.profile == PUBLISHABILITY_QGRAM_COMPACT_PROFILE_NAME:
            config_version = "publishability.v1"
        elif args.profile == COMPLETE_QGRAM_MMAP_SUITE_PROFILE_NAME:
            config_version = "complete_qgram.v2"
        elif args.profile == COMPLETE_QGRAM_SUITE_PROFILE_NAME:
            config_version = "complete_qgram.v1"
        else:
            config_version = "complete_static.v1"
        run_config = RunConfig(
            repetitions=args.repetitions,
            warmups=args.warmups,
            profiling_enabled=not args.disable_profiling,
            strict_validation=not args.disable_strict_validation,
            config_label=config_label,
            config_version=config_version,
            sample_difference_limit=args.sample_difference_limit,
            scale=args.scale,
            source_root=args.source_root,
            record_build_metrics=args.record_build_metrics,
        )
        results_directory = run_suite(
            dataset_slugs=args.datasets or list(suite_profile["datasets"]),
            query_ids=args.queries or list(suite_profile["queries"]),
            mode_names=args.modes or list(suite_profile["modes"]),
            run_config=run_config,
            suite_profile=args.profile,
        )
        print(json.dumps({"results_directory": str(results_directory)}, sort_keys=True))
        return 0

    if args.command == "build-reports":
        output_paths = build_reports_from_raw_jsonl(Path(args.results_directory))
        print(json.dumps({name: str(path) for name, path in output_paths.items()}, sort_keys=True))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the top-level CLI parser."""

    parser = argparse.ArgumentParser(prog="python -m query_eval.cli")
    subparsers = parser.add_subparsers(dest="command", required=True)

    stage_parser = subparsers.add_parser("stage-datasets")
    stage_parser.add_argument("--datasets", nargs="*", default=None)
    stage_parser.add_argument("--refresh", action="store_true")
    stage_parser.add_argument("--scale", choices=("2k", "10k", "100k", "full"), default="2k")
    stage_parser.add_argument("--source-root", default=None)

    artifact_parser = subparsers.add_parser("ensure-artifacts")
    artifact_parser.add_argument("--datasets", nargs="*", default=None)
    artifact_parser.add_argument("--refresh-dataset", action="store_true")
    artifact_parser.add_argument("--force-rebuild", action="store_true")
    artifact_parser.add_argument("--scale", choices=("2k", "10k", "100k", "full"), default="2k")
    artifact_parser.add_argument("--source-root", default=None)
    artifact_parser.add_argument("--record-build-metrics", action="store_true")

    cell_parser = subparsers.add_parser("run-cell")
    cell_parser.add_argument("--dataset", required=True)
    cell_parser.add_argument("--query-id", required=True, choices=QUERY_IDS)
    cell_parser.add_argument("--mode", required=True, choices=MODE_NAMES)
    cell_parser.add_argument("--repetition-index", type=int, required=True)
    cell_parser.add_argument("--is-warmup", required=True)
    cell_parser.add_argument("--config-label", required=True)
    cell_parser.add_argument("--config-version", required=True)
    cell_parser.add_argument("--sample-difference-limit", type=int, default=10)
    cell_parser.add_argument("--code-version", default="unknown")
    cell_parser.add_argument("--profiling-enabled", default="true")
    cell_parser.add_argument("--strict-validation", default="true")
    cell_parser.add_argument("--scale", choices=("2k", "10k", "100k", "full"), default="2k")
    cell_parser.add_argument("--source-root", default=None)

    suite_parser = subparsers.add_parser("run-suite")
    suite_parser.add_argument("--profile", default=COMPLETE_SUITE_PROFILE_NAME, choices=SUITE_PROFILE_NAMES)
    suite_parser.add_argument("--datasets", nargs="*", default=None)
    suite_parser.add_argument("--queries", nargs="*", default=None)
    suite_parser.add_argument("--modes", nargs="*", default=None)
    suite_parser.add_argument("--repetitions", type=int, default=10)
    suite_parser.add_argument("--warmups", type=int, default=1)
    suite_parser.add_argument("--disable-profiling", action="store_true")
    suite_parser.add_argument("--disable-strict-validation", action="store_true")
    suite_parser.add_argument("--config-label", default=None)
    suite_parser.add_argument("--config-version", default=None)
    suite_parser.add_argument("--sample-difference-limit", type=int, default=10)
    suite_parser.add_argument("--scale", choices=("2k", "10k", "100k", "full"), default="2k")
    suite_parser.add_argument("--source-root", default=None)
    suite_parser.add_argument("--record-build-metrics", action="store_true")

    reports_parser = subparsers.add_parser("build-reports")
    reports_parser.add_argument("--results-directory", required=True)

    return parser


def _resolve_dataset_specs(dataset_slugs: list[str] | None):
    """Resolve dataset slugs or default to the active five-dataset set."""

    selected_slugs = dataset_slugs or list(ACTIVE_TEXT_DATASET_SLUGS)
    return [get_dataset_spec(dataset_slug) for dataset_slug in selected_slugs]


def _parse_bool(raw_value: str) -> bool:
    """Parse a small CLI boolean string."""

    normalized = raw_value.strip().lower()
    if normalized in {"1", "true", "yes", "y"}:
        return True
    if normalized in {"0", "false", "no", "n"}:
        return False
    raise ValueError(f"Could not parse boolean value: {raw_value}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        raise SystemExit(1)
