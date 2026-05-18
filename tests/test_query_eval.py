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
    validate_static_qgram_compact_index_artifact_spec,
    validate_static_qgram_index_artifact_spec,
    validate_static_qgram_mmap_index_artifact_spec,
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
    COMPLETE_QGRAM_MMAP_SUITE_PROFILE_NAME,
    COMPLETE_QGRAM_SUITE_PROFILE_NAME,
    COMPLETE_SUITE_PROFILE_NAME,
    MODE_NAMES,
    PUBLISHABILITY_QGRAM_COMPACT_PROFILE_NAME,
    QUERY_IDS,
    get_complete_suite_profile,
    get_artifact_root,
    get_dataset_registry,
    get_dataset_spec,
    get_publishability_qgram_compact_profile,
    get_qgram_mmap_suite_profile,
    get_qgram_suite_profile,
    get_query_registry,
    get_suite_profile,
)
from query_eval.runner import execute_cell_run
from query_eval.specs import ArtifactSpec, CellRunSpec, RunConfig
from query_eval.search_backends import (
    keyword_search_grep_plaintext_file,
    keyword_search_loglite_static_bloom,
    keyword_search_loglite_static_qgram_index,
    keyword_search_loglite_static_qgram_index_mmap,
    keyword_search_plaintext_file,
    keyword_search_ripgrep_plaintext_file,
)
from query_eval.static_qgram_index import (
    decode_static_record_from_paths,
    decode_delta_varint_postings,
    encode_delta_varint_postings,
    intersect_sorted_postings,
    keyword_search_loglite_static_qgram_index_mmap_compact,
    keyword_search_loglite_static_qgram_index_mmap_cpp,
    load_static_qgram_compact_index_header,
    load_static_qgram_mmap_index_header,
    load_static_qgram_index,
    open_static_qgram_compact_index,
    open_static_qgram_mmap_index,
    qgrams_for_bytes,
    qgrams_for_query_term,
    qgram_values_for_bytes,
    qgram_values_for_query_term,
)
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
            (
                "decompressed_text",
                "full_decompression",
                "minor_optimization",
                "static_bloom",
                "static_qgram_index",
                "static_qgram_index_mmap",
                "static_qgram_index_mmap_compact",
                "static_qgram_index_mmap_cpp",
                "grep_plaintext",
                "ripgrep_plaintext",
            ),
        )

    def test_complete_suite_profile_resolves_full_matrix(self) -> None:
        profile = get_complete_suite_profile()
        self.assertEqual(COMPLETE_SUITE_PROFILE_NAME, "complete_static_evaluation")
        self.assertEqual(len(profile["datasets"]), 16)
        self.assertEqual(len(profile["queries"]), 8)
        self.assertEqual(len(profile["modes"]), 4)

    def test_qgram_suite_profile_resolves_full_matrix(self) -> None:
        profile = get_qgram_suite_profile()
        self.assertEqual(COMPLETE_QGRAM_SUITE_PROFILE_NAME, "complete_qgram_evaluation")
        self.assertEqual(profile, get_suite_profile(COMPLETE_QGRAM_SUITE_PROFILE_NAME))
        self.assertEqual(len(profile["datasets"]), 16)
        self.assertEqual(len(profile["queries"]), 8)
        self.assertEqual(len(profile["modes"]), 5)
        self.assertIn("static_qgram_index", profile["modes"])

    def test_qgram_mmap_suite_profile_resolves_full_matrix(self) -> None:
        profile = get_qgram_mmap_suite_profile()
        self.assertEqual(COMPLETE_QGRAM_MMAP_SUITE_PROFILE_NAME, "complete_qgram_mmap_evaluation")
        self.assertEqual(profile, get_suite_profile(COMPLETE_QGRAM_MMAP_SUITE_PROFILE_NAME))
        self.assertEqual(len(profile["datasets"]), 16)
        self.assertEqual(len(profile["queries"]), 8)
        self.assertEqual(len(profile["modes"]), 6)
        self.assertIn("static_qgram_index_mmap", profile["modes"])

    def test_publishability_suite_profile_resolves_full_matrix(self) -> None:
        profile = get_publishability_qgram_compact_profile()
        self.assertEqual(PUBLISHABILITY_QGRAM_COMPACT_PROFILE_NAME, "publishability_qgram_compact_evaluation")
        self.assertEqual(profile, get_suite_profile(PUBLISHABILITY_QGRAM_COMPACT_PROFILE_NAME))
        self.assertEqual(len(profile["datasets"]), 16)
        self.assertEqual(len(profile["queries"]), 8)
        self.assertEqual(len(profile["modes"]), 7)
        self.assertIn("static_qgram_index_mmap_compact", profile["modes"])
        self.assertIn("static_qgram_index_mmap_cpp", profile["modes"])
        self.assertIn("grep_plaintext", profile["modes"])
        self.assertIn("ripgrep_plaintext", profile["modes"])

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
        self.assertEqual(
            artifact_spec.static_qgram_index_path,
            artifact_root / "Linux_2k.log.lite.static.qidx.json",
        )
        self.assertEqual(
            artifact_spec.static_qgram_mmap_index_path,
            artifact_root / "Linux_2k.log.lite.static.qidx2",
        )
        self.assertEqual(
            artifact_spec.static_qgram_compact_index_path,
            artifact_root / "Linux_2k.log.lite.static.qidx3",
        )

    def test_scaled_artifact_path_construction_is_isolated(self) -> None:
        dataset_spec = get_dataset_spec("linux")
        artifact_spec = build_artifact_spec(dataset_spec, scale="100k", source_root="dataset/loghub_full")
        self.assertEqual(artifact_spec.scale, "100k")
        self.assertIn("loghub_scaled/100k/Linux/Linux_100k.log", str(artifact_spec.raw_log_path))
        self.assertIn("compressed_logs/100k/Linux_100k.log.lite.static.qidx3", str(artifact_spec.static_qgram_compact_index_path))
        self.assertIn("dataset/loghub_full/Linux/Linux.log", str(artifact_spec.source_raw_log_path))

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


class StaticQGramIndexUnitTests(unittest.TestCase):
    """Fast unit checks for q-gram indexing primitives."""

    def test_qgram_generation_for_short_and_long_terms(self) -> None:
        self.assertEqual(qgrams_for_query_term("a"), (1, {"61"}))
        self.assertEqual(qgrams_for_query_term("ab"), (2, {"6162"}))
        self.assertEqual(qgrams_for_query_term("abcd"), (3, {"616263", "626364"}))
        self.assertEqual(qgrams_for_query_term(""), (0, set()))

    def test_qgram_value_generation_for_mmap_index(self) -> None:
        self.assertEqual(qgram_values_for_query_term("a"), (1, {0x61}))
        self.assertEqual(qgram_values_for_query_term("ab"), (2, {0x6162}))
        self.assertEqual(qgram_values_for_query_term("abcd"), (3, {0x616263, 0x626364}))
        self.assertEqual(qgram_values_for_query_term(""), (0, set()))

    def test_qgrams_for_bytes_uses_unique_sliding_windows(self) -> None:
        self.assertEqual(qgrams_for_bytes(b"ababa", 2), {"6162", "6261"})
        self.assertEqual(qgrams_for_bytes(b"ab", 3), set())

    def test_qgram_values_for_bytes_uses_unique_sliding_windows(self) -> None:
        self.assertEqual(qgram_values_for_bytes(b"ababa", 2), {0x6162, 0x6261})
        self.assertEqual(qgram_values_for_bytes(b"ab", 3), set())

    def test_postings_intersection_returns_sorted_candidates(self) -> None:
        postings = [[1, 2, 4, 7], [2, 3, 4, 9], [0, 2, 4]]
        self.assertEqual(intersect_sorted_postings(postings), [2, 4])
        self.assertEqual(intersect_sorted_postings([[1, 3], [2, 4]]), [])

    def test_delta_varint_postings_support_sparse_and_large_record_ids(self) -> None:
        for record_ids in (
            [],
            [0],
            [1, 2, 3, 4, 5],
            [3, 128, 65535, 65536, 100000],
        ):
            encoded = encode_delta_varint_postings(record_ids)
            decoded = decode_delta_varint_postings(encoded, 0, len(encoded), expected_count=len(record_ids))
            self.assertEqual(decoded, record_ids)


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

    def test_static_qgram_indexes_exist_for_all_active_datasets(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                validate_static_qgram_index_artifact_spec(self.artifact_specs[dataset_slug])

    def test_static_qgram_mmap_indexes_exist_for_all_active_datasets(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                validate_static_qgram_mmap_index_artifact_spec(self.artifact_specs[dataset_slug])

    def test_static_qgram_compact_indexes_exist_for_all_active_datasets(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                validate_static_qgram_compact_index_artifact_spec(self.artifact_specs[dataset_slug])

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

    def test_static_qgram_index_runs_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id in QUERY_IDS:
                with self.subTest(dataset=dataset_slug, query=query_id):
                    static_matches = run_query("static_qgram_index", dataset_slug, query_id)
                    self.assertIsInstance(static_matches, list)

    def test_static_qgram_mmap_index_runs_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id in QUERY_IDS:
                with self.subTest(dataset=dataset_slug, query=query_id):
                    static_matches = run_query("static_qgram_index_mmap", dataset_slug, query_id)
                    self.assertIsInstance(static_matches, list)

    def test_static_qgram_compact_index_runs_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id in QUERY_IDS:
                with self.subTest(dataset=dataset_slug, query=query_id):
                    static_matches = run_query("static_qgram_index_mmap_compact", dataset_slug, query_id)
                    self.assertIsInstance(static_matches, list)

    def test_static_qgram_index_matches_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    baseline_matches = query_function("decompressed_text", dataset_slug)
                    qgram_matches = query_function("static_qgram_index", dataset_slug)
                    self.assertEqual(set(baseline_matches), set(qgram_matches))

    def test_static_qgram_mmap_index_matches_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    baseline_matches = query_function("decompressed_text", dataset_slug)
                    qgram_matches = query_function("static_qgram_index_mmap", dataset_slug)
                    self.assertEqual(set(baseline_matches), set(qgram_matches))

    def test_static_qgram_compact_index_matches_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    baseline_matches = query_function("decompressed_text", dataset_slug)
                    qgram_matches = query_function("static_qgram_index_mmap_compact", dataset_slug)
                    self.assertEqual(set(baseline_matches), set(qgram_matches))

    def test_static_qgram_mmap_matches_json_qgram_for_all_active_dataset_query_pairs(self) -> None:
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            for query_id, query_function in QUERY_FUNCTIONS.items():
                with self.subTest(dataset=dataset_slug, query=query_id):
                    json_qgram_matches = query_function("static_qgram_index", dataset_slug)
                    mmap_qgram_matches = query_function("static_qgram_index_mmap", dataset_slug)
                    self.assertEqual(set(json_qgram_matches), set(mmap_qgram_matches))

    def test_static_qgram_index_fixes_bloom_stress_substring_for_all_datasets(self) -> None:
        exact_dataset_count = 0
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                baseline_matches = run_query("decompressed_text", dataset_slug, "bloom_stress_substring")
                qgram_matches = run_query("static_qgram_index", dataset_slug, "bloom_stress_substring")
                self.assertEqual(set(baseline_matches), set(qgram_matches))
                exact_dataset_count += 1
        self.assertEqual(exact_dataset_count, 16)

    def test_static_qgram_mmap_fixes_bloom_stress_substring_for_all_datasets(self) -> None:
        exact_dataset_count = 0
        for dataset_slug in ACTIVE_TEXT_DATASET_SLUGS:
            with self.subTest(dataset=dataset_slug):
                baseline_matches = run_query("decompressed_text", dataset_slug, "bloom_stress_substring")
                qgram_matches = run_query("static_qgram_index_mmap", dataset_slug, "bloom_stress_substring")
                self.assertEqual(set(baseline_matches), set(qgram_matches))
                exact_dataset_count += 1
        self.assertEqual(exact_dataset_count, 16)

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

    def test_static_qgram_index_linux_notebook_regression_queries_are_exact(self) -> None:
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
        self.assertIsNotNone(artifact_spec.static_qgram_index_path)

        for query_payload in linux_static_queries:
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                qgram_result = keyword_search_loglite_static_qgram_index(
                    artifact_spec.static_compressed_binary_path,
                    artifact_spec.static_window_path,
                    artifact_spec.static_qgram_index_path,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(qgram_result.matches))
                self.assertLessEqual(qgram_result.decoded_records or 0, qgram_result.total_records or 0)

    def test_static_qgram_mmap_linux_notebook_regression_queries_are_exact(self) -> None:
        linux_static_queries = (
            "kernel",
            "28842",
            "Jul  2",
            "failed",
            ("sshd", "failure"),
        )
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_mmap_index_path)

        for query_payload in linux_static_queries:
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                qgram_result = keyword_search_loglite_static_qgram_index_mmap(
                    artifact_spec.static_qgram_mmap_index_path,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(qgram_result.matches))
                self.assertLessEqual(qgram_result.decoded_records or 0, qgram_result.total_records or 0)

    def test_static_qgram_compact_linux_notebook_regression_queries_are_exact(self) -> None:
        linux_static_queries = (
            "kernel",
            "28842",
            "Jul  2",
            "failed",
            ("sshd", "failure"),
        )
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)

        for query_payload in linux_static_queries:
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                qgram_result = keyword_search_loglite_static_qgram_index_mmap_compact(
                    artifact_spec.static_qgram_compact_index_path,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(qgram_result.matches))
                self.assertLessEqual(qgram_result.verified_records or 0, qgram_result.total_records or 0)

    def test_static_qgram_cpp_linux_notebook_regression_queries_are_exact(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)
        for query_payload in ("kernel", "28842", "Jul  2", ("sshd", "failure")):
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                qgram_result = keyword_search_loglite_static_qgram_index_mmap_cpp(
                    artifact_spec.static_qgram_compact_index_path,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(qgram_result.matches))
                self.assertIsNotNone(qgram_result.verified_records)
                self.assertIsNotNone(qgram_result.skipped_records)
                self.assertIsNotNone(qgram_result.postings_lists_touched)
                self.assertIsNotNone(qgram_result.postings_ids_read)
                self.assertLessEqual(qgram_result.verified_records or 0, qgram_result.total_records or 0)
                self.assertTrue((qgram_result.planner_strategy or "").startswith("cpp_"))

    def test_static_qgram_compact_adversarial_mac_control_line_exactness(self) -> None:
        artifact_spec = self.artifact_specs["mac"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)
        baseline_matches = keyword_search_plaintext_file(artifact_spec.decompressed_text_path, "I")
        python_result = keyword_search_loglite_static_qgram_index_mmap_compact(
            artifact_spec.static_qgram_compact_index_path,
            "I",
        )
        cpp_result = keyword_search_loglite_static_qgram_index_mmap_cpp(
            artifact_spec.static_qgram_compact_index_path,
            "I",
        )

        self.assertEqual(set(baseline_matches), set(python_result.matches))
        self.assertEqual(set(baseline_matches), set(cpp_result.matches))

    def test_external_plaintext_baselines_are_exact_for_linux_queries(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        for query_payload in ("kernel", "Jul  2", ("sshd", "failure")):
            with self.subTest(query=query_payload):
                baseline_matches = keyword_search_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                grep_result = keyword_search_grep_plaintext_file(artifact_spec.decompressed_text_path, query_payload)
                ripgrep_result = keyword_search_ripgrep_plaintext_file(
                    artifact_spec.decompressed_text_path,
                    query_payload,
                )
                self.assertEqual(set(baseline_matches), set(grep_result.matches))
                self.assertEqual(set(baseline_matches), set(ripgrep_result.matches))

    def test_static_qgram_mmap_header_and_lookup_for_linux(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_mmap_index_path)
        header = load_static_qgram_mmap_index_header(artifact_spec.static_qgram_mmap_index_path)
        self.assertEqual(header.version, 2)
        self.assertGreater(header.record_count, 0)
        self.assertEqual(header.q1_count, 256)
        self.assertEqual(header.q2_count, 65536)
        self.assertGreater(header.q3_count, 0)

        handle, mmap_buffer, view = open_static_qgram_mmap_index(artifact_spec.static_qgram_mmap_index_path)
        try:
            self.assertGreater(len(view.get_postings(1, ord("k"))), 0)
            self.assertGreater(len(view.get_postings(2, int.from_bytes(b"ke", "big"))), 0)
            self.assertGreater(len(view.get_postings(3, int.from_bytes(b"ker", "big"))), 0)
        finally:
            mmap_buffer.close()
            handle.close()

    def test_static_qgram_compact_header_lookup_and_size_for_linux(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)
        self.assertIsNotNone(artifact_spec.static_qgram_mmap_index_path)
        header = load_static_qgram_compact_index_header(artifact_spec.static_qgram_compact_index_path)
        self.assertEqual(header.version, 5)
        self.assertGreater(header.record_count, 0)
        self.assertGreater(header.q1_count, 0)
        self.assertGreater(header.q2_count, 0)
        self.assertGreater(header.q3_count, 0)
        self.assertLess(
            artifact_spec.static_qgram_compact_index_path.stat().st_size,
            artifact_spec.static_qgram_mmap_index_path.stat().st_size,
        )

        handle, mmap_buffer, view = open_static_qgram_compact_index(artifact_spec.static_qgram_compact_index_path)
        try:
            self.assertGreater(len(view.get_postings(1, ord("k"))), 0)
            self.assertGreater(len(view.get_postings(2, int.from_bytes(b"ke", "big"))), 0)
            self.assertGreater(len(view.get_postings(3, int.from_bytes(b"ker", "big"))), 0)
        finally:
            mmap_buffer.close()
            handle.close()

    def test_static_qgram_mmap_line_slab_matches_static_decompression(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_mmap_index_path)
        self.assertIsNotNone(artifact_spec.static_decompressed_text_path)
        decompressed_lines = artifact_spec.static_decompressed_text_path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).splitlines()

        handle, mmap_buffer, view = open_static_qgram_mmap_index(artifact_spec.static_qgram_mmap_index_path)
        try:
            sample_ids = {0, 1, view.record_count // 2, view.record_count - 1}
            for record_id in sample_ids:
                with self.subTest(record_id=record_id):
                    self.assertEqual(decompressed_lines[record_id], view.line_bytes(record_id).decode("utf-8", "ignore"))
        finally:
            mmap_buffer.close()
            handle.close()

    def test_static_qgram_compact_line_slab_matches_baseline_decompression(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)
        decompressed_lines = artifact_spec.decompressed_text_path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).splitlines()

        handle, mmap_buffer, view = open_static_qgram_compact_index(artifact_spec.static_qgram_compact_index_path)
        try:
            sample_ids = {0, 1, view.record_count // 2, view.record_count - 1}
            for record_id in sample_ids:
                with self.subTest(record_id=record_id):
                    self.assertEqual(decompressed_lines[record_id], view.line_bytes(record_id).decode("utf-8", "ignore"))
        finally:
            mmap_buffer.close()
            handle.close()

    def test_static_qgram_compact_planner_uses_slab_scan_for_broad_short_query(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_qgram_compact_index_path)
        baseline_matches = keyword_search_plaintext_file(artifact_spec.decompressed_text_path, " ")
        qgram_result = keyword_search_loglite_static_qgram_index_mmap_compact(
            artifact_spec.static_qgram_compact_index_path,
            " ",
        )
        self.assertEqual(set(baseline_matches), set(qgram_result.matches))
        self.assertEqual(qgram_result.planner_strategy, "line_slab_scan")
        self.assertEqual(qgram_result.verified_records, qgram_result.total_records)

    def test_static_qgram_random_access_reconstruction_matches_static_decompression(self) -> None:
        artifact_spec = self.artifact_specs["linux"]
        self.assertIsNotNone(artifact_spec.static_compressed_binary_path)
        self.assertIsNotNone(artifact_spec.static_window_path)
        self.assertIsNotNone(artifact_spec.static_qgram_index_path)
        self.assertIsNotNone(artifact_spec.static_decompressed_text_path)

        parsed_static_window = load_l_window_from_txt(artifact_spec.static_window_path)
        loaded_index = load_static_qgram_index(artifact_spec.static_qgram_index_path)
        decompressed_lines = artifact_spec.static_decompressed_text_path.read_text(
            encoding="utf-8",
            errors="ignore",
        ).splitlines()
        sample_ids = {0, 1, len(loaded_index.record_directory) // 2, len(loaded_index.record_directory) - 1}

        for record_id in sample_ids:
            with self.subTest(record_id=record_id):
                entry = loaded_index.record_directory[record_id]
                reconstructed = decode_static_record_from_paths(
                    artifact_spec.static_compressed_binary_path,
                    parsed_static_window,
                    entry,
                )
                self.assertEqual(decompressed_lines[record_id], reconstructed)

    def test_static_qgram_sampled_substrings_are_never_missed(self) -> None:
        for dataset_slug in ("linux", "apache", "android", "windows"):
            artifact_spec = self.artifact_specs[dataset_slug]
            self.assertIsNotNone(artifact_spec.static_compressed_binary_path)
            self.assertIsNotNone(artifact_spec.static_window_path)
            self.assertIsNotNone(artifact_spec.static_qgram_index_path)
            lines = [
                line
                for line in artifact_spec.decompressed_text_path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                ).splitlines()
                if len(line) >= 8
            ][:3]

            for line in lines:
                for substring_length in (1, 2, 3, 5):
                    substring = line[:substring_length]
                    with self.subTest(dataset=dataset_slug, substring=substring):
                        baseline_matches = keyword_search_plaintext_file(
                            artifact_spec.decompressed_text_path,
                            substring,
                        )
                        qgram_result = keyword_search_loglite_static_qgram_index(
                            artifact_spec.static_compressed_binary_path,
                            artifact_spec.static_window_path,
                            artifact_spec.static_qgram_index_path,
                            substring,
                        )
                        self.assertEqual(set(baseline_matches), set(qgram_result.matches))

    def test_static_qgram_mmap_sampled_substrings_are_never_missed(self) -> None:
        for dataset_slug in ("linux", "apache", "android", "windows"):
            artifact_spec = self.artifact_specs[dataset_slug]
            self.assertIsNotNone(artifact_spec.static_qgram_mmap_index_path)
            lines = [
                line
                for line in artifact_spec.decompressed_text_path.read_text(
                    encoding="utf-8",
                    errors="ignore",
                ).splitlines()
                if len(line) >= 8
            ][:3]

            for line in lines:
                for substring_length in (1, 2, 3, 5):
                    substring = line[:substring_length]
                    with self.subTest(dataset=dataset_slug, substring=substring):
                        baseline_matches = keyword_search_plaintext_file(
                            artifact_spec.decompressed_text_path,
                            substring,
                        )
                        qgram_result = keyword_search_loglite_static_qgram_index_mmap(
                            artifact_spec.static_qgram_mmap_index_path,
                            substring,
                        )
                        self.assertEqual(set(baseline_matches), set(qgram_result.matches))

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

    def test_static_qgram_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common_token",
            mode_name="static_qgram_index",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)
        self.assertEqual(first_record.decoded_records, second_record.decoded_records)
        self.assertEqual(first_record.total_records, second_record.total_records)

    def test_static_qgram_mmap_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common_token",
            mode_name="static_qgram_index_mmap",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)
        self.assertEqual(first_record.decoded_records, second_record.decoded_records)
        self.assertEqual(first_record.total_records, second_record.total_records)

    def test_static_qgram_compact_correctness_outputs_are_stable_across_repeated_runs(self) -> None:
        cell_run_spec = CellRunSpec(
            dataset_slug="linux",
            query_id="common_token",
            mode_name="static_qgram_index_mmap_compact",
            repetition_index=0,
            is_warmup=False,
        )
        first_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")
        second_record = execute_cell_run(cell_run_spec, self.no_profile_run_config, code_version="test")

        self.assertEqual(first_record.result_lines, second_record.result_lines)
        self.assertEqual(first_record.correctness, second_record.correctness)
        self.assertEqual(first_record.decoded_records, second_record.decoded_records)
        self.assertEqual(first_record.total_records, second_record.total_records)
        self.assertEqual(first_record.planner_strategy, second_record.planner_strategy)


if __name__ == "__main__":
    unittest.main()
