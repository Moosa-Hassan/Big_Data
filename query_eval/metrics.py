"""Correctness metrics for comparing search modes against the baseline.

What this module owns:
    - Exact-set comparison metrics.
    - Deterministic false-positive and false-negative sampling.

What this module does not own:
    - Query execution.
    - Timing or memory profiling.
    - Report aggregation.

How this relates to the evaluation pipeline:
    Candidate execution remains clean and measurable because correctness logic is
    computed outside the timed region and isolated here.

Source of truth:
    Correctness is always defined relative to the `decompressed_text` baseline.
"""

from __future__ import annotations

from .specs import CorrectnessMeasurement


def compute_correctness_measurement(
    baseline_matches: list[str],
    candidate_matches: list[str],
) -> CorrectnessMeasurement:
    """Compute set-based retrieval metrics against the baseline.

    Purpose:
        Compare one candidate mode to the decompressed-text source of truth.

    Arguments:
        baseline_matches: Baseline result lines.
        candidate_matches: Candidate result lines.

    Returns:
        A `CorrectnessMeasurement` containing exact-set equivalence, TP/FP/FN,
        precision, recall, and F1.

    Notes:
        Set comparison is used intentionally because line order is not part of
        the evaluation claim for part 2. The important question is whether the
        candidate finds the correct result set.
    """

    baseline_set = set(baseline_matches)
    candidate_set = set(candidate_matches)

    tp = len(baseline_set & candidate_set)
    fp = len(candidate_set - baseline_set)
    fn = len(baseline_set - candidate_set)

    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0.0

    return CorrectnessMeasurement(
        exact_set_match=(baseline_set == candidate_set),
        tp=tp,
        fp=fp,
        fn=fn,
        precision=precision,
        recall=recall,
        f1=f1,
    )


def sample_difference_lines(
    baseline_matches: list[str],
    candidate_matches: list[str],
    sample_limit: int = 10,
) -> tuple[list[str], list[str]]:
    """Return deterministic samples of false positives and false negatives.

    Purpose:
        Preserve a compact failure-analysis trace for non-exact runs.

    Arguments:
        baseline_matches: Baseline result lines.
        candidate_matches: Candidate result lines.
        sample_limit: Maximum number of sample lines to return for each side.

    Returns:
        A tuple `(false_positives, false_negatives)` where both lists are sorted
        to make repeated runs stable and diff-friendly.
    """

    baseline_set = set(baseline_matches)
    candidate_set = set(candidate_matches)
    false_positives = sorted(candidate_set - baseline_set)[:sample_limit]
    false_negatives = sorted(baseline_set - candidate_set)[:sample_limit]
    return false_positives, false_negatives
