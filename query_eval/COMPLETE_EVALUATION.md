# Complete Static-Bloom Evaluation

This document is the handoff guide for the complete research-grade evaluation suite. The source of truth is the `query_eval` package, not `main.ipynb`.

## Purpose

The complete suite evaluates compressed-domain keyword search over LogLite-B artifacts across all registered LogHub TEXT datasets. It compares four modes:

- `decompressed_text`: correctness baseline over decompressed text.
- `full_decompression`: exact compressed-bitstream traversal with full reconstruction.
- `minor_optimization`: earlier length/window-based optimization, measured honestly even when it loses recall.
- `static_bloom`: static L-window plus 64-bit Bloom-style per-record token bitmap.

## Matrix

The final suite uses:

- 16 datasets.
- 8 query families.
- 4 execution modes.
- 1 warmup per cell.
- 10 measured repetitions per cell.

This produces:

- 512 dataset/query/mode cells.
- 5,632 raw executions.
- 5,120 measured executions.

## Datasets

The complete suite covers:

`linux`, `apache`, `hdfs`, `openstack`, `android`, `zookeeper`, `healthapp`, `hpc`, `hadoop`, `bgl`, `mac`, `proxifier`, `spark`, `openssh`, `thunderbird`, `windows`.

## Query Families

The locked manifest is stored at:

`query_eval/locked_query_manifest.json`

Families:

- `common_token`
- `medium_token`
- `rare_token`
- `common_phrase`
- `selective_phrase`
- `numeric_identifier`
- `conjunctive`
- `bloom_stress_substring`

The first seven are marked token-safe. `bloom_stress_substring` is deliberately not token-safe because it probes the edge of the static Bloom tokenizer, which hashes alphanumeric tokens rather than arbitrary substrings.

Regenerate the manifest only when deliberately changing the evaluation protocol:

```bash
python3 -m query_eval.query_curation --write
```

Do not generate queries dynamically during evaluation.

## Run Commands

From `Big_Data/`:

```bash
python3 -m query_eval.cli stage-datasets
```

```bash
python3 -m query_eval.cli ensure-artifacts
```

```bash
python3 -m query_eval.cli run-suite \
  --profile complete_static_evaluation \
  --repetitions 10 \
  --warmups 1 \
  --config-label complete_static_evaluation \
  --config-version complete_static.v1
```

The latest completed result bundle is:

`evaluation_results/query_eval/20260507_155729_complete_static_evaluation`

## Result Files

Each complete run writes:

- `manifest.json`
- `raw_runs.jsonl`
- `cell_level_aggregate.csv`
- `query_level_aggregate.csv`
- `dataset_level_aggregate.csv`
- `suite_summary.csv`
- `static_bloom_summary.csv`
- `query_manifest.csv`
- `dataset_coverage.csv`
- `complete_evaluation_summary.csv`

Use `raw_runs.jsonl` for auditability. Use the CSVs for report tables and plots.

## Final Run Headline Results

For `20260507_155729_complete_static_evaluation`:

- Raw executions: 5,632.
- Measured executions: 5,120.
- Cells: 512.
- `full_decompression` exact cells: 128/128.
- `minor_optimization` exact cells: 59/128.
- `static_bloom` exact cells: 96/128 overall.
- `static_bloom` exact token-safe cells: 95/112.
- `static_bloom` exact stress cells: 1/16.
- `static_bloom` false positives: 0.
- `static_bloom` false negatives: 11,975 across all static cells.
- Median `full_decompression` wall time: 77.60 ms.
- Median `static_bloom` wall time: 47.09 ms.
- Median `static_bloom` Bloom skip rate: 81.8%.

## Interpretation Rules

- Correctness baseline is always `decompressed_text`.
- `full_decompression` should be exact and is the compressed-domain correctness reference.
- `static_bloom` is the main next-generation optimization candidate, but it is not universally exact yet.
- Stress-substring results should be interpreted separately from token-safe results.
- Static Bloom failures are one-sided in the final run: no false positives, only false negatives.
- Do not claim arbitrary substring exactness for `static_bloom` without adding fallback logic.

## Known Environment Note

On this macOS/Apple Silicon environment, the original/static C++ test-mode decompressor can hang on `Mac_2k.log` after producing compressed and window artifacts. `query_eval.artifacts` handles this by materializing the decompressed text artifact with the Python codec mirror when compressed/window files already exist. This keeps Mac in the complete suite instead of silently dropping it.
