"""Tests for the part-2 research evaluation subsystem.

This test module intentionally mixes fast unit checks with heavier integration
checks because the part-2 architecture is only meaningful if the registry,
artifact pipeline, query dispatch, and correctness rules all line up end to end.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from query_eval.artifacts import build_artifact_spec, ensure_artifacts_for_datasets, validate_artifact_spec
from query_eval.metrics import compute_correctness_measurement, sample_difference_lines
from query_eval.queries import query_common, query_conjunctive, query_phrase, query_selective
from query_eval.registry import (
    ACTIVE_TEXT_DATASET_SLUGS,
    MODE_NAMES,
    QUERY_IDS,
    get_artifact_root,
    get_dataset_registry,
    get_dataset_spec,
    get_query_registry,
)
from query_eval.runner import execute_cell_run
from query_eval.specs import ArtifactSpec, CellRunSpec, RunConfig

QUERY_FUNCTIONS = {
    "common": query_common,
    "phrase": query_phrase,
    "selective": query_selective,
    "conjunctive": query_conjunctive,
}


class RegistryTests(unittest.TestCase):
    """Fast registry validation checks."""

    def test_registered_text_dataset_count_is_sixteen(self) -> None:
        self.assertEqual(len(get_dataset_registry()), 16)

    def test_active_dataset_set_matches_part2_plan(self) -> None:
        self.assertEqual(
            ACTIVE_TEXT_DATASET_SLUGS,
            ("linux", "apache", "hdfs", "openstack", "android"),
        )

    def test_query_manifest_covers_every_active_dataset(self) -> None:
        query_registry = get_query_registry()
        for query_id in QUERY_IDS:
            query_spec = query_registry[query_id]
            for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
                self.assertIn(dataset_slug, query_spec.dataset_payloads)

    def test_mode_names_match_execution_contract(self) -> None:
        self.assertEqual(
            MODE_NAMES,
            ("decompressed_text", "full_decompression", "minor_optimization"),
        )


class ArtifactAndMetricsUnitTests(unittest.TestCase):
    """Fast unit tests for path construction and metric logic."""

    def test_linux_artifact_path_construction(self) -> None:
        dataset_spec = get_dataset_spec("linux")
        artifact_spec = build_artifact_spec(dataset_spec)
        artifact_root = get_artifact_root()

        self.assertEqual(
            artifact_spec.compressed_binary_path,
            artifact_root / "Linux_2k.log.lite.b",
        )
        self.assertEqual(
            artifact_spec.decompressed_text_path,
            artifact_root / "Linux_2k.log.lite.decom",
        )
        self.assertEqual(
            artifact_spec.window_path,
            artifact_root / "Linux_2k.window.txt",
        )

    def test_missing_artifact_validation_raises(self) -> None:
        missing_root = Path("/tmp/query_eval_missing_artifacts")
        artifact_spec = ArtifactSpec(
            raw_log_path=missing_root / "missing.log",
            compressed_binary_path=missing_root / "missing.lite.b",
            decompressed_text_path=missing_root / "missing.lite.decom",
            window_path=missing_root / "missing.window.txt",
        )
        with self.assertRaises(FileNotFoundError):
            validate_artifact_spec(artifact_spec)

    def test_correctness_metrics_for_exact_match(self) -> None:
        measurement = compute_correctness_measurement(
            baseline_matches=["a", "b"],
            candidate_matches=["b", "a"],
        )
        self.assertTrue(measurement.exact_set_match)
        self.assertEqual((measurement.tp, measurement.fp, measurement.fn), (2, 0, 0))
        self.assertEqual((measurement.precision, measurement.recall, measurement.f1), (1.0, 1.0, 1.0))

    def test_correctness_metrics_for_mismatch(self) -> None:
        measurement = compute_correctness_measurement(
            baseline_matches=["a", "b"],
            candidate_matches=["b", "c"],
        )
        self.assertFalse(measurement.exact_set_match)
        self.assertEqual((measurement.tp, measurement.fp, measurement.fn), (1, 1, 1))
        self.assertAlmostEqual(measurement.precision, 0.5)
        self.assertAlmostEqual(measurement.recall, 0.5)
        self.assertAlmostEqual(measurement.f1, 0.5)

    def test_difference_sampling_is_deterministic(self) -> None:
        false_positives, false_negatives = sample_difference_lines(
            baseline_matches=["line3", "line1", "line2"],
            candidate_matches=["line2", "line4"],
            sample_limit=10,
        )
        self.assertEqual(false_positives, ["line4"])
        self.assertEqual(false_negatives, ["line1", "line3"])


class IntegrationTests(unittest.TestCase):
    """Integration and regression checks against the real artifact pipeline."""

    @classmethod
    def setUpClass(cls) -> None:
        dataset_specs = [get_dataset_spec(dataset_slug) for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS]
        cls.artifact_specs = ensure_artifacts_for_datasets(dataset_specs)
        cls.no_profile_run_config = RunConfig(
            repetitions=1,
            warmups=0,
            profiling_enabled=False,
            strict_validation=True,
            config_label="test_config",
            config_version="test.v1",
            sample_difference_limit=5,
        )

    def test_linux_end_to_end_exactness(self) -> None:
        baseline_matches = query_common("decompressed_text", "linux")
        full_decompression_matches = query_common("full_decompression", "linux")
        self.assertEqual(set(baseline_matches), set(full_decompression_matches))

    def test_active_dataset_common_query_smoke(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            matches = query_common("decompressed_text", dataset_slug)
            self.assertIsInstance(matches, list)
            self.assertGreater(len(matches), 0)

    def test_all_active_dataset_query_pairs_match_for_full_decompression(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    baseline_matches = query_function("decompressed_text", dataset_slug)
                    full_decompression_matches = query_function("full_decompression", dataset_slug)
                    self.assertEqual(set(baseline_matches), set(full_decompression_matches))

    def test_minor_optimization_runs_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    optimization_matches = query_function("minor_optimization", dataset_slug)
                    self.assertIsInstance(optimization_matches, list)

    def test_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common",
            mode_name="full_decompression",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)


if __name__ == "__main__":
    unittest.main()
