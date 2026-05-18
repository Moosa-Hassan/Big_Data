# Publishability Pack: qidx3 Scaled Exact Query Evaluation

This document is the current handoff for the post-Bloom publishability work.
The source of truth is still the `query_eval` package.

## What Changed

The publishability pack adds the exact compact q-gram sidecar mode:

```text
static_qgram_index_mmap_compact
```

and the native comparator:

```text
static_qgram_index_mmap_cpp
```

It also adds plaintext external baselines:

```text
grep_plaintext
ripgrep_plaintext
```

The registered profile is:

```text
publishability_qgram_compact_evaluation
```

## qidx3 Format

qidx3 is written beside the static LogLite artifact:

```text
<sample>.lite.static.qidx3
```

It stores:

- a fixed binary header with version and stale-check metadata,
- sparse q=1, q=2, and q=3 dictionaries,
- delta-varint postings with 32-bit-plus record-id support,
- a line directory,
- a normalized decoded line slab used for exact verification.

The line slab is built from the `decompressed_text` baseline artifact with the
same logical-line iterator used by the baseline search. qidx3 is still packaged
beside the static LogLite artifact and requires the static artifacts to exist,
but its correctness oracle is the baseline-normalized decoded stream. This keeps
adversarial control-line payloads exact and avoids depending on static-record
boundaries that are invisible to the evaluation baseline.

The native `static_qgram_index_mmap_cpp` comparator reads the same qidx3
dictionaries and delta-varint postings, applies the same broad-query planner,
and then verifies candidates against the mmap line slab. The old JSON q-gram and
qidx2 mmap modes remain for historical comparison. qidx2 is a 2k-era sidecar
with uint16 postings. qidx3 is the scaled sidecar.

## Correctness Claim

For a decoded line `s` and query term `p`, with `q = min(3, len(p in bytes))`:

```text
p occurs in s
=> every q-gram of p occurs in s
=> the true record appears in every selected postings list
=> postings intersection cannot drop a true match
=> exact line-slab verification removes false positives
=> qidx3 output equals decompressed_text output
```

This supports arbitrary substring exactness. It does not support a claim of pure
compressed-domain search, because qidx3 stores a decoded normalized line slab as
part of the sidecar.

## Scales

The evaluation supports:

```text
2k
10k
100k
full
```

For scaled runs, raw samples are staged from real full LogHub source files under:

```text
dataset/loghub_full/<Dataset>/<Dataset>.log
```

The staging command takes the first N real lines. If the source has fewer than N
lines, the full available file is used and `effective_line_count` records the
actual count. Synthetic repeated logs are forbidden.

Scaled generated artifacts are isolated from the 2k artifacts:

```text
compressed_logs/10k/
compressed_logs/100k/
compressed_logs/full/
dataset/loghub_scaled/10k/
dataset/loghub_scaled/100k/
dataset/loghub_scaled/full/
```

## Real LogHub Sources

The upstream LogHub README gives the raw archive links in its Download column:

```text
https://github.com/logpai/loghub
```

Examples:

```text
Linux      https://zenodo.org/records/8196385/files/Linux.tar.gz?download=1
Apache     https://zenodo.org/records/8196385/files/Apache.tar.gz?download=1
Zookeeper  https://zenodo.org/records/8196385/files/Zookeeper.tar.gz?download=1
HealthApp  https://zenodo.org/records/8196385/files/HealthApp.tar.gz?download=1
OpenSSH    https://zenodo.org/records/8196385/files/SSH.tar.gz?download=1
```

Large 100k/all-dataset runs require the large archives too, including HDFS, BGL,
Spark, Thunderbird, Windows, and Android. Do not replace those with repeated 2k
samples.

## Commands

2k regression:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v tests/test_query_eval.py
```

Stage scaled sources after real full LogHub files have been placed under
`dataset/loghub_full`:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m query_eval.cli stage-datasets \
  --scale 100k \
  --source-root dataset/loghub_full
```

Build scaled artifacts and record preprocessing metrics:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m query_eval.cli ensure-artifacts \
  --scale 100k \
  --source-root dataset/loghub_full \
  --record-build-metrics
```

Run the publishability profile:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m query_eval.cli run-suite \
  --profile publishability_qgram_compact_evaluation \
  --scale 100k \
  --source-root dataset/loghub_full \
  --repetitions 10 \
  --warmups 2 \
  --config-label publishability_100k_qgram_compact \
  --config-version publishability.v1
```

## Reports

Every suite run now writes the standard aggregate CSVs plus:

```text
external_baseline_summary.csv
planner_strategy_summary.csv
qidx_size_summary.csv
amortization_summary.csv
adversarial_publishability_report.md
```

The visual report also emits separate wall-time figures for:

```text
all_queries
token_safe_queries
stress_queries
```

## Current Verified Smoke

A 2k Linux publishability smoke was run at:

```text
evaluation_results/query_eval/20260518_184312_qidx3_2k_linux_smoke
```

Headline values from that smoke:

| Mode | Exact Cells | Median Wall Time |
| --- | ---: | ---: |
| `full_decompression` | 8/8 | 50.009 ms |
| `static_bloom` | 5/8 | 43.100 ms |
| `grep_plaintext` | 8/8 | 4.079 ms |
| `ripgrep_plaintext` | 8/8 | 4.149 ms |
| `static_qgram_index_mmap_compact` | 8/8 | 4.696 ms |
| `static_qgram_index_mmap_cpp` | 8/8 | 3.325 ms |

This is a smoke test, not the final research claim. The final claim needs real
10k, 100k, and preferably full-scale reruns.

## Publishability Rules

- Claim exact indexed static LogLite sidecar search.
- Do not claim pure compressed-domain search.
- Report qidx3 build time separately from timed queries.
- Report qidx3/raw and qidx3/static-compressed size ratios.
- Separate all-query, token-safe, and stress-query results.
- Compare honestly against grep and ripgrep.
- Treat qidx3 being slower than plaintext external baselines as a framing risk,
  not as a hidden detail.
