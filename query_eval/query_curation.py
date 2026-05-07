"""Deterministic query curation for the complete static-Bloom suite.

What this module owns:
    - Inspecting staged LogHub 2k samples.
    - Selecting fixed query payloads for every complete-suite dataset.
    - Writing the locked machine-readable query manifest consumed by registry.

What this module does not own:
    - Query execution.
    - Correctness metrics.
    - Report generation.

How this relates to the evaluation pipeline:
    The complete evaluation suite must not invent queries at runtime. This
    utility is the explicit, auditable curation step that creates the locked
    manifest used by `query_eval.registry`.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any

from .registry import COMPLETE_TEXT_DATASET_SLUGS, get_dataset_raw_path, get_dataset_spec, get_project_root

TOKEN_PATTERN = re.compile(r"[A-Za-z0-9]+")
STOPWORDS = {
    "and",
    "are",
    "for",
    "from",
    "has",
    "not",
    "the",
    "this",
    "that",
    "with",
    "info",
    "debug",
    "trace",
    "warn",
    "warning",
    "error",
    "true",
    "false",
    "null",
}

QUERY_FAMILY_METADATA: dict[str, dict[str, Any]] = {
    "common_token": {
        "family": "high_hit_token",
        "description": "High-hit alphanumeric token query used to measure common keyword behavior.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "high",
    },
    "medium_token": {
        "family": "medium_hit_token",
        "description": "Moderate-hit alphanumeric token query used to avoid only testing dominant words.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "medium",
    },
    "rare_token": {
        "family": "low_hit_token",
        "description": "Low-hit alphanumeric token query used to expose rare-match behavior.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "low",
    },
    "common_phrase": {
        "family": "common_phrase",
        "description": "Repeated two-token phrase query used to validate phrase matching.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "medium_high",
    },
    "selective_phrase": {
        "family": "selective_phrase",
        "description": "Low-hit two-token phrase query used to stress selective phrase retrieval.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "low",
    },
    "numeric_identifier": {
        "family": "numeric_identifier",
        "description": "Digit-containing token query covering PIDs, IDs, block IDs, addresses, and similar values.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "low_medium",
    },
    "conjunctive": {
        "family": "multi_keyword_conjunction",
        "description": "Two-keyword conjunction requiring both payload tokens to appear in the same line.",
        "token_safe": True,
        "is_stress_query": False,
        "expected_selectivity_band": "medium",
    },
    "bloom_stress_substring": {
        "family": "substring_stress",
        "description": "Substring or punctuation-bearing query that intentionally probes Bloom-token assumptions.",
        "token_safe": False,
        "is_stress_query": True,
        "expected_selectivity_band": "stress",
    },
}


def curate_locked_manifest() -> dict[str, Any]:
    """Build the complete locked query manifest from staged raw logs."""

    manifest: dict[str, Any] = {
        "version": "complete_static.v1",
        "source": "deterministic query_eval.query_curation over staged LogHub 2k samples",
        "datasets": list(COMPLETE_TEXT_DATASET_SLUGS),
        "queries": {
            query_id: {**metadata, "dataset_payloads": {}}
            for query_id, metadata in QUERY_FAMILY_METADATA.items()
        },
        "dataset_coverage": [],
    }

    for dataset_slug in COMPLETE_TEXT_DATASET_SLUGS:
        lines = _load_dataset_lines(dataset_slug)
        profile = _profile_lines(lines)
        payloads = _select_payloads(profile)
        for query_id, query_payload in payloads.items():
            manifest["queries"][query_id]["dataset_payloads"][dataset_slug] = query_payload

        manifest["dataset_coverage"].append(
            {
                "dataset_slug": dataset_slug,
                "line_count": len(lines),
                "distinct_tokens": len(profile["token_counts"]),
                "distinct_phrases": len(profile["phrase_counts"]),
            }
        )

    return manifest


def write_locked_manifest(output_path: Path | None = None) -> Path:
    """Write the locked query manifest to disk and return the path."""

    destination = output_path or get_project_root() / "query_eval" / "locked_query_manifest.json"
    manifest = curate_locked_manifest()
    destination.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    return destination


def _load_dataset_lines(dataset_slug: str) -> list[str]:
    dataset_spec = get_dataset_spec(dataset_slug)
    raw_path = get_dataset_raw_path(dataset_spec)
    if not raw_path.exists():
        raise FileNotFoundError(f"Dataset is not staged: {raw_path}")
    return raw_path.read_text(encoding="utf-8", errors="ignore").splitlines()


def _profile_lines(lines: list[str]) -> dict[str, Any]:
    token_counts: Counter[str] = Counter()
    token_display: dict[str, str] = {}
    numeric_counts: Counter[str] = Counter()
    phrase_counts: Counter[str] = Counter()

    for line in lines:
        token_matches = list(TOKEN_PATTERN.finditer(line))
        raw_tokens = [match.group(0) for match in token_matches]
        normalized_tokens = [token.lower() for token in raw_tokens if _valid_token(token)]
        unique_tokens = set(normalized_tokens)
        for raw_token in raw_tokens:
            normalized = raw_token.lower()
            if normalized in unique_tokens and normalized not in token_display:
                token_display[normalized] = raw_token
        for token in unique_tokens:
            token_counts[token] += 1
            if any(character.isdigit() for character in token):
                numeric_counts[token] += 1

        seen_phrases: set[str] = set()
        for first_match, second_match in zip(token_matches, token_matches[1:]):
            first = first_match.group(0)
            second = second_match.group(0)
            if not _valid_token(first) or not _valid_token(second):
                continue
            # Preserve the exact separator characters from the source log so a
            # phrase payload selected here is guaranteed to be a real substring
            # under the evaluator's plain substring semantics.
            phrase = line[first_match.start() : second_match.end()]
            phrase_key = phrase.lower()
            if phrase_key not in seen_phrases:
                phrase_counts[phrase] += 1
                seen_phrases.add(phrase_key)

    return {
        "line_count": len(lines),
        "token_counts": token_counts,
        "token_display": token_display,
        "numeric_counts": numeric_counts,
        "phrase_counts": phrase_counts,
    }


def _select_payloads(profile: dict[str, Any]) -> dict[str, Any]:
    line_count = profile["line_count"]
    token_counts: Counter[str] = profile["token_counts"]
    numeric_counts: Counter[str] = profile["numeric_counts"]
    phrase_counts: Counter[str] = profile["phrase_counts"]
    token_display: dict[str, str] = profile["token_display"]

    common = _pick_token(token_counts, token_display, line_count, minimum_rate=0.15, target_rate=0.45)
    medium = _pick_token(token_counts, token_display, line_count, minimum_rate=0.04, target_rate=0.12, exclude={common.lower()})
    rare = _pick_token(
        token_counts,
        token_display,
        line_count,
        minimum_rate=1 / max(line_count, 1),
        target_rate=0.015,
        exclude={common.lower(), medium.lower()},
    )
    numeric = _pick_token(numeric_counts, token_display, line_count, minimum_rate=1 / max(line_count, 1), target_rate=0.02)
    common_phrase = _pick_phrase(phrase_counts, line_count, minimum_rate=0.03, target_rate=0.10)
    selective_phrase = _pick_phrase(phrase_counts, line_count, minimum_rate=1 / max(line_count, 1), target_rate=0.01, exclude={common_phrase})
    stress = _stress_substring(common)

    phrase_tokens = TOKEN_PATTERN.findall(common_phrase)
    conjunctive_payload = phrase_tokens[:2] if len(phrase_tokens) >= 2 else [common, medium]

    return {
        "common_token": common,
        "medium_token": medium,
        "rare_token": rare,
        "common_phrase": common_phrase,
        "selective_phrase": selective_phrase,
        "numeric_identifier": numeric,
        "conjunctive": conjunctive_payload,
        "bloom_stress_substring": stress,
    }


def _valid_token(token: str) -> bool:
    normalized = token.lower()
    return len(normalized) >= 3 and normalized not in STOPWORDS


def _pick_token(
    counts: Counter[str],
    display: dict[str, str],
    line_count: int,
    minimum_rate: float,
    target_rate: float,
    exclude: set[str] | None = None,
) -> str:
    exclude = exclude or set()
    candidates = [
        (token, count)
        for token, count in counts.items()
        if token not in exclude and count / max(line_count, 1) >= minimum_rate
    ]
    if not candidates:
        candidates = [(token, count) for token, count in counts.items() if token not in exclude]
    if not candidates:
        raise ValueError("Could not select a token query payload.")
    token, _count = min(
        candidates,
        key=lambda item: (abs((item[1] / max(line_count, 1)) - target_rate), -item[1], item[0]),
    )
    return display.get(token, token)


def _pick_phrase(
    counts: Counter[str],
    line_count: int,
    minimum_rate: float,
    target_rate: float,
    exclude: set[str] | None = None,
) -> str:
    exclude = exclude or set()
    candidates = [
        (phrase, count)
        for phrase, count in counts.items()
        if phrase not in exclude and count / max(line_count, 1) >= minimum_rate
    ]
    if not candidates:
        candidates = [(phrase, count) for phrase, count in counts.items() if phrase not in exclude]
    if not candidates:
        raise ValueError("Could not select a phrase query payload.")
    phrase, _count = min(
        candidates,
        key=lambda item: (abs((item[1] / max(line_count, 1)) - target_rate), -item[1], item[0].lower()),
    )
    return phrase


def _stress_substring(common_token: str) -> str:
    """Return a deterministic non-token substring for Bloom stress testing."""

    if len(common_token) >= 5:
        return common_token[1:4]
    if len(common_token) >= 3:
        return common_token[:2]
    return common_token


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m query_eval.query_curation")
    parser.add_argument("--write", action="store_true", help="Write query_eval/locked_query_manifest.json.")
    parser.add_argument("--output", default=None, help="Optional output path for the locked manifest.")
    args = parser.parse_args(argv)

    if args.write:
        path = write_locked_manifest(Path(args.output) if args.output else None)
        print(json.dumps({"locked_query_manifest": str(path)}, sort_keys=True))
        return 0

    print(json.dumps(curate_locked_manifest(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
