# Publishable Runtime Fix Report: Static Q-Gram mmap Index

Run directory: `evaluation_results/query_eval/20260517_212524_publishable_qgram_runtime_fix`

Configuration:

- Profile: `complete_qgram_mmap_evaluation`
- Datasets: 16 LogHub 2k TEXT samples
- Query families: 8 per dataset
- Modes: `decompressed_text`, `full_decompression`, `minor_optimization`, `static_bloom`, `static_qgram_index`, `static_qgram_index_mmap`
- Repetitions: 30 measured, 3 warmups
- Raw runs: 25,344

## Implementation Summary

The new `static_qgram_index_mmap` mode adds a binary `.qidx2` sidecar beside each static LogLite artifact. It stores:

- fixed header and source-artifact metadata for stale detection,
- compact record and line directories,
- direct q=1/q=2 gram tables,
- sorted q=3 dictionary,
- compact `uint16` postings,
- a normalized decoded line slab for exact verification.

The query path mmaps the sidecar, retrieves only the q-gram postings required by the query, intersects the shortest postings first, then verifies candidates against the mmap line slab using the baseline all-substrings semantics.

## Correctness

| Mode | Exact Cells | Total Median-Cell FP | Total Median-Cell FN |
| --- | ---: | ---: | ---: |
| `decompressed_text` | 128/128 | 0 | 0 |
| `full_decompression` | 128/128 | 0 | 0 |
| `minor_optimization` | 59/128 | 0 | 1,554 |
| `static_bloom` | 96/128 | 0 | 11,975 |
| `static_qgram_index` | 128/128 | 0 | 0 |
| `static_qgram_index_mmap` | 128/128 | 0 | 0 |

The mmap index preserves the same proof as the JSON q-gram reference:

1. If pattern `p` occurs in line `s`, every selected q-gram of `p` occurs in `s`.
2. Therefore a true matching record appears in every selected posting list.
3. Intersecting selected postings cannot remove a true match.
4. Exact substring verification over the normalized line bytes removes false positives.
5. The returned set equals `decompressed_text`.

## Runtime

Suite-level medians:

| Mode | Median Wall Time |
| --- | ---: |
| `decompressed_text` | 0.691 ms |
| `full_decompression` | 92.649 ms |
| `minor_optimization` | 60.105 ms |
| `static_bloom` | 51.553 ms |
| `static_qgram_index` | 63.364 ms |
| `static_qgram_index_mmap` | 4.007 ms |

Paired per-cell speedups for `static_qgram_index_mmap`:

| Reference | Median Speedup | 95% Bootstrap CI | Wins/Losses |
| --- | ---: | ---: | ---: |
| `full_decompression` | 22.05x | [19.66x, 24.47x] | 128/0 |
| `static_bloom` | 13.06x | [11.28x, 14.24x] | 128/0 |
| JSON `static_qgram_index` | 15.86x | [14.09x, 17.44x] | 128/0 |

Token-safe queries only:

| Reference | Median Speedup | 95% Bootstrap CI | Wins/Losses |
| --- | ---: | ---: | ---: |
| `full_decompression` | 22.27x | [20.28x, 25.14x] | 112/0 |
| `static_bloom` | 13.33x | [11.79x, 14.54x] | 112/0 |
| JSON `static_qgram_index` | 15.06x | [12.57x, 16.53x] | 112/0 |

Stress substring queries only:

| Reference | Median Speedup | 95% Bootstrap CI | Wins/Losses |
| --- | ---: | ---: | ---: |
| `full_decompression` | 19.15x | [14.04x, 26.67x] | 16/0 |
| `static_bloom` | 9.83x | [7.84x, 19.20x] | 16/0 |
| JSON `static_qgram_index` | 23.50x | [20.35x, 30.69x] | 16/0 |

Sign tests are significant in every comparison above:

- all cells: two-sided exact sign-test `p = 5.88e-39`;
- token-safe cells: `p = 3.85e-34`;
- stress cells: `p = 3.05e-05`.

Wilcoxon signed-rank tests agree; for all-cell comparisons the normal approximation underflows, so report as `p < 1e-30`.

## Work Reduction

| Mode | Median Verified/Decoded Records | Mean Verified/Decoded Records | Median Skipped Records |
| --- | ---: | ---: | ---: |
| `static_bloom` | 363.5 | 536.7 | 1,636.5 |
| JSON `static_qgram_index` | 203.0 | 342.2 | 1,793.0 |
| `static_qgram_index_mmap` | 203.0 | 342.2 | 1,793.0 |

The mmap mode has the same candidate pruning as JSON q-gram, but removes JSON parsing, Python object hydration, and static compressed-record reconstruction from the timed query path.

## Index Size

Across all 16 datasets:

- JSON q-gram sidecars: 44,619,646 bytes
- mmap `.qidx2` sidecars: 29,764,870 bytes
- mmap sidecars are 66.7% of JSON size, a 33.3% reduction

Caveat: the `.qidx2` sidecars are still large relative to the static compressed artifacts: 29.8 MB vs 1.68 MB compressed static data across the suite. This must be reported as an index-overhead tradeoff, not hidden inside query runtime.

## Publishability Verdict

The result is research-publishable under the honest claim:

> An exact indexed static LogLite sidecar achieves 128/128 correctness and a statistically significant 22.05x median speedup over full decompression, 13.06x over static Bloom, and 15.86x over the JSON q-gram reference on the 16-dataset locked query suite.

It should not be framed as pure compressed-domain search, because verification uses a decoded normalized line slab in the sidecar. It is also not faster than scanning the already decompressed plaintext artifact; `decompressed_text` remains the lower bound at 0.691 ms median versus 4.007 ms for mmap q-gram. The publishable framing is therefore exact indexed querying over static compressed LogLite artifacts, with index build/storage cost measured separately.

Recommended next work before submission:

- scale beyond 2k samples to 10k/100k/full LogHub samples,
- report index build time and amortization point,
- compare with native `grep`/ripgrep and a C++ mmap implementation,
- add compressed/native metadata variants to reduce sidecar size,
- add query planning for broad one-gram/two-gram terms where plaintext scanning is the true lower bound.
