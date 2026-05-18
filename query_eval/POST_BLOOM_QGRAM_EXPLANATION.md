# Post-Bloom Query Engine: Q-Gram and mmap Improvements

This document explains what changed after the `static_bloom` implementation, how the newer exact q-gram modes work, and why the final mmap sidecar is better for the research claim.

## Starting Point: Static Bloom

The first major static LogLite improvement was `static_bloom`.

It changed the query path from:

```text
read compressed record
reconstruct line
check query terms
repeat for every record
```

to:

```text
read cheap per-record token bitmap
reject records whose bitmap cannot contain query token
reconstruct only Bloom candidates
check exact query predicate
```

This was useful because it skipped many records. On the full 16-dataset suite it reached:

- `96/128` exact dataset/query cells,
- median wall time around `51.55 ms` in the latest full run,
- median decoded records `363.5` out of `2000`,
- no false positives,
- but `11,975` total median-cell false negatives.

The weakness is algorithmic, not just implementation detail. Bloom filtering over token hashes is only a necessary condition for token-style queries. It is not exact for arbitrary substrings. If the locked query is a substring that is not represented as the same token in the bitmap, a true matching record can be rejected before exact verification. That is why the `bloom_stress_substring` family exposed failures.

## Goal After Bloom

After Bloom, the target became:

```text
keep the pruning benefit
remove false negatives
support arbitrary substring queries
make query runtime faster than static_bloom and full_decompression
preserve old modes for fair comparison
```

The key idea was to replace token-hash filtering with q-gram filtering.

A q-gram is a short byte substring. For example, with `q = 3`:

```text
pattern = "kernel"
q-grams = "ker", "ern", "rne", "nel"
```

If `"kernel"` occurs inside a log line, then all of those q-grams must also occur inside that line. This gives us an exact-safe necessary condition for substring search.

## First Post-Bloom Mode: JSON Static Q-Gram Index

The first exact implementation was `static_qgram_index`.

It builds a JSON sidecar:

```text
<sample>.lite.static.qidx.json
```

For every static LogLite record, it stores:

- record id,
- static bitstream offsets needed for random-access reconstruction,
- decoded length and metadata,
- postings lists for 1-byte, 2-byte, and 3-byte q-grams.

The index maps:

```text
q-gram -> sorted record ids containing that q-gram
```

Query execution:

```text
candidate_ids = all records

for each query term:
    if term is empty:
        continue
    q = min(3, len(term in bytes))
    grams = qgrams(term, q)
    term_candidates = intersection(postings[g] for g in grams)
    candidate_ids = candidate_ids intersection term_candidates

for each candidate record:
    reconstruct the static LogLite record
    emit it only if all query terms occur exactly in the decoded line
```

This fixed correctness:

- `static_qgram_index`: `128/128` exact cells,
- `0` false positives,
- `0` false negatives,
- stress substring improved from Bloom's `1/16` exact cells to `16/16`.

But it had a runtime problem. Every measured query had to load and hydrate a large JSON structure. The algorithm pruned better than Bloom, but the Python JSON overhead dominated. In the complete q-gram run:

- `static_qgram_index` median wall time was about `58.61 ms`,
- `static_bloom` median wall time was about `47.64 ms`.

So JSON q-gram proved the math, but not the publishable runtime.

## Final Post-Bloom Mode: Binary mmap Q-Gram Index

The final implementation is `static_qgram_index_mmap`.

It keeps the exact q-gram proof, but replaces JSON loading and static-record reconstruction with a binary mmap sidecar:

```text
<sample>.lite.static.qidx2
```

The `.qidx2` file contains:

- fixed header with magic/version/source metadata,
- stale-check metadata for the static compressed artifact and window dump,
- compact record directory,
- compact line directory,
- direct table for q=1 postings,
- direct table for q=2 postings,
- sorted dictionary for q=3 postings,
- compact `uint16` postings arrays for the current 2k-record datasets,
- normalized decoded line slab for exact verification.

The query path becomes:

```text
mmap qidx2
candidate_ids = all records

for each query term:
    q = min(3, len(term in bytes))
    grams = qgrams(term, q)
    fetch only the required postings
    intersect smallest postings first
    candidate_ids = candidate_ids intersection term_candidates

for each candidate id:
    read line bytes directly from the mmap line slab
    emit only if all query terms occur exactly
```

This removes the two major JSON q-gram costs:

- no JSON parse,
- no Python object hydration for every posting and record directory.

It also removes the timed-path reconstruction cost:

- JSON q-gram reconstructs each candidate from static compressed record data,
- mmap q-gram verifies candidate bytes directly from the line slab.

## Correctness Argument

Let `s(r)` be the baseline-normalized decoded text of record `r`.

Let `G_q(p)` be all q-grams of query pattern `p`, where:

```text
q = min(3, len(p in bytes))
```

For a non-empty query term `p`:

```text
p occurs in s(r)
=> every q-gram in G_q(p) occurs in s(r)
=> r appears in every postings list for those q-grams
=> r survives postings intersection
```

Therefore q-gram intersection cannot remove a true match.

It can keep false positives, because a line may contain all q-grams without containing the full query term in order. The final verification step removes those:

```text
line is emitted only if every query term is an exact substring of the line
```

So:

```text
static_qgram_index_mmap output == decompressed_text output
```

This is why the result is exact for arbitrary substring queries, unlike Bloom.

## Why mmap Is Faster

The old compressed modes have different costs:

```text
full_decompression:
    O(all compressed records reconstructed)

static_bloom:
    O(all record metadata scanned + Bloom candidates reconstructed)

JSON qgram:
    O(JSON load + Python object hydration + selected postings + candidate reconstruction)

mmap qgram:
    O(selected postings touched + candidate line bytes verified)
```

The asymptotic improvement is that query time no longer scales with reconstructing all records. It scales with the amount of postings data touched and the number of candidate records verified.

The engineering improvement is that mmap lets the operating system page in only the binary regions the query touches. We no longer materialize the whole JSON index as nested Python dictionaries and lists.

## Results From the Full Evaluation

Latest run:

```text
evaluation_results/query_eval/20260517_212524_publishable_qgram_runtime_fix
```

Configuration:

- 16 datasets,
- 8 query families,
- 6 modes,
- 3 warmups,
- 30 measured repetitions,
- 25,344 raw executions.

Correctness:

| Mode | Exact Cells | False Positives | False Negatives |
| --- | ---: | ---: | ---: |
| `full_decompression` | 128/128 | 0 | 0 |
| `minor_optimization` | 59/128 | 0 | 1,554 total median-cell FN |
| `static_bloom` | 96/128 | 0 | 11,975 total median-cell FN |
| `static_qgram_index` | 128/128 | 0 | 0 |
| `static_qgram_index_mmap` | 128/128 | 0 | 0 |

Runtime medians:

| Mode | Median Wall Time |
| --- | ---: |
| `decompressed_text` | 0.691 ms |
| `full_decompression` | 92.649 ms |
| `minor_optimization` | 60.105 ms |
| `static_bloom` | 51.553 ms |
| `static_qgram_index` | 63.364 ms |
| `static_qgram_index_mmap` | 4.007 ms |

Paired speedups for `static_qgram_index_mmap`:

| Compared Against | Median Speedup | 95% Bootstrap CI | Wins/Losses |
| --- | ---: | ---: | ---: |
| `full_decompression` | 22.05x | [19.66x, 24.47x] | 128/0 |
| `static_bloom` | 13.06x | [11.28x, 14.24x] | 128/0 |
| JSON `static_qgram_index` | 15.86x | [14.09x, 17.44x] | 128/0 |

Stress substring queries:

| Compared Against | Median Speedup | 95% Bootstrap CI | Wins/Losses |
| --- | ---: | ---: | ---: |
| `full_decompression` | 19.15x | [14.04x, 26.67x] | 16/0 |
| `static_bloom` | 9.83x | [7.84x, 19.20x] | 16/0 |
| JSON `static_qgram_index` | 23.50x | [20.35x, 30.69x] | 16/0 |

Statistical significance:

- all-cell sign test against full decompression: `p = 5.88e-39`,
- all-cell sign test against static Bloom: `p = 5.88e-39`,
- all-cell Wilcoxon signed-rank tests agree, with `p < 1e-30` under normal approximation,
- stress-only sign test: `p = 3.05e-05`.

## Work Reduction

Median verified or decoded records:

| Mode | Median Records | Mean Records | Median Skipped Records |
| --- | ---: | ---: | ---: |
| `static_bloom` | 363.5 | 536.7 | 1,636.5 |
| JSON `static_qgram_index` | 203.0 | 342.2 | 1,793.0 |
| `static_qgram_index_mmap` | 203.0 | 342.2 | 1,793.0 |

This shows two different wins:

1. Q-gram filtering prunes more candidate records than Bloom.
2. mmap q-gram has the same pruning as JSON q-gram, but much lower runtime overhead.

## Storage Cost

Across all 16 datasets:

- JSON q-gram sidecars: `44,619,646` bytes,
- mmap `.qidx2` sidecars: `29,764,870` bytes,
- `.qidx2` is `66.7%` of JSON size, a `33.3%` reduction.

Important caveat:

The `.qidx2` sidecars are still much larger than the compressed static LogLite artifacts:

- static compressed artifacts: about `1.68 MB`,
- `.qidx2` sidecars: about `29.8 MB`.

So the result should be presented as an indexed static LogLite sidecar, not as a zero-overhead compressed-domain search method.

## What Changed in the Code

Main new or updated pieces:

| File | Change |
| --- | --- |
| `query_eval/static_qgram_index.py` | New JSON q-gram and binary mmap q-gram builders, loaders, parsers, and search backends. |
| `query_eval/specs.py` | Added `static_qgram_index` and `static_qgram_index_mmap` mode/path fields. |
| `query_eval/artifacts.py` | Builds and validates `.qidx.json` and `.qidx2` sidecars after static artifacts exist. |
| `query_eval/registry.py` | Adds `complete_qgram_evaluation` and `complete_qgram_mmap_evaluation` profiles. |
| `query_eval/modes.py` | Dispatches `static_qgram_index` and `static_qgram_index_mmap`. |
| `query_eval/search_backends.py` | Exposes q-gram search backends. |
| `query_eval/cli.py` | Allows q-gram profiles through the CLI. |
| `query_eval/visualize_results.py` | Adds labels/colors for q-gram modes. |
| `tests/test_query_eval.py` | Adds correctness, q-gram primitive, mmap lookup, line slab, and full 16-dataset integration tests. |

## How to Rerun

From `Big_Data/`:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests/test_query_eval.py
```

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m query_eval.cli ensure-artifacts
```

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m query_eval.cli run-suite \
  --profile complete_qgram_mmap_evaluation \
  --repetitions 30 \
  --warmups 3 \
  --config-label publishable_qgram_runtime_fix \
  --config-version complete_qgram.v2
```

Avoid `--force-rebuild` on this macOS environment unless necessary. The Mac codec subprocess can hang during artifact regeneration. Non-forced validation and sidecar rebuilds are enough when artifacts already exist.

## Research Claim We Can Make

A strong and honest claim is:

> We extend the static LogLite query framework with an exact q-gram sidecar index. The final mmap implementation preserves arbitrary substring correctness across all 128 dataset/query cells and achieves a statistically significant 22.05x median speedup over full decompression and 13.06x over static Bloom on the locked 16-dataset evaluation suite.

The claim should also state the tradeoff:

> The method stores an auxiliary decoded-line verification slab and q-gram postings, so it is an indexed static-artifact search method rather than pure compressed-domain search.

## Why This Is Better Than Bloom

Bloom was fast but not exact for arbitrary substring queries. It was token-oriented and could reject true matches before verification.

Q-gram indexing is exact-safe for substrings. It cannot reject a true match because every true match must contain all selected q-grams. Exact verification then removes false positives.

mmap q-gram is the publishable version because it keeps q-gram correctness while eliminating the JSON/runtime overhead that made the first exact q-gram prototype slower than Bloom.

In short:

```text
static_bloom:
    faster than full decompression, but not exact

static_qgram_index:
    exact, but JSON overhead makes it too slow

static_qgram_index_mmap:
    exact and fast enough for the publishable runtime claim
```

## Remaining Work

Before a final paper submission, the strongest next steps are:

- scale from 2k samples to 10k, 100k, and full LogHub files,
- measure qidx2 build time and amortization point,
- compare against `grep`, `ripgrep`, and a C++ mmap implementation,
- reduce sidecar size with compressed postings or native LogLite metadata,
- add query planning for broad one-gram and two-gram terms,
- separate token-safe and stress-query claims clearly in figures and tables.
