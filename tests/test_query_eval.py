"""Tests for the part-2 research evaluation subsystem.

This test module intentionally mixes fast unit checks with heavier integration
checks because the part-2 architecture is only meaningful if the registry,
artifact pipeline, query dispatch, and correctness rules all line up end to end.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from query_eval.artifacts import (
    build_artifact_spec,
    ensure_artifacts_for_datasets,
    validate_artifact_spec,
    validate_static_artifact_spec,
)
from query_eval.metrics import compute_correctness_measurement, sample_difference_lines
from query_eval.queries import (
    query_bloom_stress_substring,
    query_common,
    query_conjunctive,
    query_medium_token,
    query_numeric_identifier,
    query_phrase,
    query_rare_token,
    query_selective,
    run_query,
)
from query_eval.registry import (
    ACTIVE_TEXT_DATASET_SLUGS,
    COMPLETE_SUITE_PROFILE_NAME,
    MODE_NAMES,
    QUERY_IDS,
    get_complete_suite_profile,
    get_artifact_root,
    get_dataset_registry,
    get_dataset_spec,
    get_query_registry,
)
from query_eval.runner import execute_cell_run
from query_eval.specs import ArtifactSpec, CellRunSpec, RunConfig
from query_eval.search_backends import keyword_search_loglite_static_bloom
from query_eval.window_loader import load_l_window_from_txt

QUERY_FUNCTIONS = {
    "common_token": query_common,
    "medium_token": query_medium_token,
    "rare_token": query_rare_token,
    "common_phrase": query_phrase,
    "selective_phrase": query_selective,
    "numeric_identifier": query_numeric_identifier,
    "conjunctive": query_conjunctive,
    "bloom_stress_substring": query_bloom_stress_substring,
}


class RegistryTests(unittest.TestCase):
    """Fast registry validation checks."""

    def test_registered_text_dataset_count_is_sixteen(self) -> None:
        self.assertEqual(len(get_dataset_registry()), 16)

    def test_active_dataset_set_matches_complete_suite_plan(self) -> None:
        self.assertEqual(len(ACTIVE_TEXT_DATASET_SLUGS), 16)

    def test_query_manifest_covers_every_active_dataset(self) -> None:
        query_registry = get_query_registry()
        for query_id in QUERY_IDS:
            query_spec = query_registry[query_id]
            for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
                self.assertIn(dataset_slug, query_spec.dataset_payloads)

    def test_mode_names_match_execution_contract(self) -> None:
        self.assertEqual(
            MODE_NAMES,
            ("decompressed_text", "full_decompression", "minor_optimization", "static_bloom"),
        )

    def test_complete_suite_profile_resolves_full_matrix(self) -> None:
        profile = get_complete_suite_profile()
        self.assertEqual(COMPLETE_SUITE_PROFILE_NAME, "complete_static_evaluation")
        self.assertEqual(len(profile["datasets"]), 16)
        self.assertEqual(len(profile["queries"]), 8)
        self.assertEqual(len(profile["modes"]), 4)

    def test_query_metadata_is_complete(self) -> None:
        for query_id, query_spec in get_query_registry().items():
            self.assertIn(query_id, QUERY_IDS)
            self.assertIn(query_spec.expected_selectivity_band, {
                "high",
                "medium",
                "low",
                "medium_high",
                "low_medium",
                "stress",
            })
            self.assertIsInstance(query_spec.token_safe, bool)
            self.assertIsInstance(query_spec.is_stress_query, bool)
            if query_spec.is_stress_query:
                self.assertFalse(query_spec.token_safe)


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
        self.assertEqual(
            artifact_spec.static_compressed_binary_path,
            artifact_root / "Linux_2k.log.lite.static.b",
        )
        self.assertEqual(
            artifact_spec.static_decompressed_text_path,
            artifact_root / "Linux_2k.log.lite.static.decom",
        )
        self.assertEqual(
            artifact_spec.static_window_path,
            artifact_root / "Linux_2k.window.static.txt",
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

    def test_static_artifacts_exist_for_all_active_datasets(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                validate_static_artifact_spec(self.artifact_specs[dataset_slug])

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

    def test_static_bloom_runs_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id in QUERY_IDS:
                with self.subTest(dataset=dataset_slug, query=query_id):
                    static_matches = run_query("static_bloom", dataset_slug, query_id)
                    self.assertIsInstance(static_matches, list)

    def test_static_bloom_linux_notebook_regression_queries_are_exact(self) -> None:
        linux_static_queries = (
            "kernel",
            "28842",
            "Jul  2",
            "failed",
            ("sshd", "failure"),
        )
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_compressed_binary_path)
        self.assertIsNotNone(artifact_spec.static_window_path)
        parsed_static_window = load_l_window_from_txt(artifact_spec.static_window_path)

        from query_eval.search_backends import keyword_search_plaintext_file

        for query_payload in linux_static_queries:
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                static_result = keyword_search_loglite_static_bloom(
                    artifact_spec.static_compressed_binary_path,
                    parsed_static_window,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(static_result.matches))
                self.assertIsNotNone(static_result.bloom_rejected_records)

    def test_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common_token",
            mode_name="full_decompression",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)

    def test_static_bloom_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common_token",
            mode_name="static_bloom",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)
        self.assertEqual(first_record.bloom_rejected_records, second_record.bloom_rejected_records)


if __name__ == "__main__":
    unittest.main()
