# Query Evaluation Subsystem

This package is the canonical execution and reporting layer for the LogLite compressed-query research project.

The older `main.ipynb` notebook is historical context. New experiment execution should live here so results are reproducible, inspectable, and easy to rerun.

## Current Status

The package has grown beyond the original part-2 prototype. It now supports the
full 16-dataset 2k suite, static Bloom, exact q-gram sidecars, qidx2 mmap, and
the publishability qidx3 path for scaled real-LogHub evaluation.

The most current publishability handoff is:

```text
query_eval/PUBLISHABILITY_QIDX3.md
```

Use that document for 10k/100k/full-scale qidx3 runs.

## Research Question

The project asks whether simple search queries over LogLite-compressed logs can be answered directly from compressed artifacts, or with limited reconstruction, while measuring the correctness and performance tradeoffs against a decompressed-text baseline.

The original part-2 evaluation matrix was:

- 5 active datasets
- 4 query families
- 3 execution modes
- 60 dataset/query/mode cells
- 1 warm-up run per cell
- 10 measured repetitions per cell
- each measured run in a fresh subprocess

## Execution Modes

| Mode | Role | Expected Correctness |
| --- | --- | --- |
| `decompressed_text` | Baseline that scans decompressed text generated from the `.lite.b` file. | Source of truth. |
| `full_decompression` | Reads the `.lite.b` bitstream and reconstructs lines while preserving LogLite window state. | Should match baseline exactly. |
| `minor_optimization` | Uses final L-window hints to skip many lines and reconstruct only likely candidates. | Experimental; may lose recall. |
| `static_bloom` | Static L-window plus per-record token bitmap filtering. | Exact for many token-safe cells, but not arbitrary substrings. |
| `static_qgram_index` | JSON sidecar q-gram index. | Exact, but JSON loading is slow. |
| `static_qgram_index_mmap` | qidx2 binary mmap sidecar for 2k datasets. | Exact on the 16-dataset 2k suite. |
| `static_qgram_index_mmap_compact` | qidx3 compact binary mmap sidecar with planner, delta-varint postings, and a baseline-normalized decoded line slab. | Exact scaled reference mode. |
| `static_qgram_index_mmap_cpp` | Native C++ qidx3 mmap postings search with the same planner and slab verification. | Exact native comparator. |
| `grep_plaintext` | External grep baseline over decompressed text, with Python post-filter for conjunctive queries. | Exact external comparator. |
| `ripgrep_plaintext` | External ripgrep baseline over decompressed text, with Python post-filter for conjunctive queries. | Exact external comparator. |

`minor_optimization` is allowed to be imperfect. False negatives in this mode are research findings, not automatically bugs.

## Active Datasets

| Slug | Dataset | AL | NDL |
| --- | --- | ---: | ---: |
| `linux` | Linux | 91 | 208 |
| `apache` | Apache | 90 | 69 |
| `hdfs` | HDFS | 140 | 135 |
| `openstack` | OpenStack | 295 | 131 |
| `android` | Android | 123 | 720 |

The complete 16-dataset suite is registered in `query_eval.registry`. The
current locked query families are:

- `common_token`
- `medium_token`
- `rare_token`
- `common_phrase`
- `selective_phrase`
- `numeric_identifier`
- `conjunctive`
- `bloom_stress_substring`

Public query functions are available in `query_eval.queries`:

- `query_common(mode_chosen, dataset)`
- `query_phrase(mode_chosen, dataset)`
- `query_selective(mode_chosen, dataset)`
- `query_conjunctive(mode_chosen, dataset)`

## Package Map

| File | Purpose |
| --- | --- |
| `specs.py` | Dataclasses for datasets, artifacts, queries, configs, and run records. |
| `registry.py` | Canonical datasets, queries, modes, and paths. |
| `artifacts.py` | Dataset staging, artifact generation, `xorc-cli` resolution, and local build fallback. |
| `window_loader.py` | Parser for `.window.txt` L-window dumps. |
| `search_backends.py` | Baseline, full-decompression, and minor-optimization query execution. |
| `modes.py` | Explicit mode dispatch. |
| `queries.py` | Public notebook-friendly query functions. |
| `metrics.py` | Exact-set comparison, TP/FP/FN, precision, recall, F1, and sampled differences. |
| `profiling.py` | Wall time, CPU time, and peak RSS measurement. |
| `persistence.py` | JSONL, JSON, and CSV helpers. |
| `runner.py` | Single-cell and full-suite orchestration in subprocesses. |
| `reports.py` | Aggregate CSV generation from the raw ledger. |
| `cli.py` | Reproducible command-line entrypoints. |

## Quick Start

Run from `Big_Data/`:

```bash
python3 -m unittest -v tests/test_query_eval.py
```

Stage datasets:

```bash
python3 -m query_eval.cli stage-datasets \
  --datasets linux apache hdfs openstack android
```

Ensure compressed artifacts exist:

```bash
python3 -m query_eval.cli ensure-artifacts \
  --datasets linux apache hdfs openstack android
```

Run the canonical suite:

```bash
python3 -m query_eval.cli run-suite \
  --datasets linux apache hdfs openstack android \
  --queries common phrase selective conjunctive \
  --modes decompressed_text full_decompression minor_optimization \
  --repetitions 10 \
  --warmups 1 \
  --config-label part2_research_eval
```

Run the qidx3 publishability profile on real 100k samples after placing full
LogHub source files under `dataset/loghub_full/<Dataset>/<Dataset>.log`:

```bash
python3 -m query_eval.cli stage-datasets \
  --scale 100k \
  --source-root dataset/loghub_full
```

```bash
python3 -m query_eval.cli ensure-artifacts \
  --scale 100k \
  --source-root dataset/loghub_full \
  --record-build-metrics
```

```bash
python3 -m query_eval.cli run-suite \
  --profile publishability_qgram_compact_evaluation \
  --scale 100k \
  --source-root dataset/loghub_full \
  --repetitions 10 \
  --warmups 2 \
  --config-label publishability_100k_qgram_compact \
  --config-version publishability.v1
```

Rebuild reports from an existing raw ledger:

```bash
python3 -m query_eval.cli build-reports \
  --results-directory evaluation_results/query_eval/<run_dir>
```

## Notebook Usage

Use notebooks for inspection, plots, and narrative, not for reimplementing execution logic.

Direct query inspection:

```python
from query_eval.queries import query_common

baseline = query_common("decompressed_text", "linux")
full = query_common("full_decompression", "linux")
minor = query_common("minor_optimization", "linux")

print(len(baseline), len(full), len(minor))
```

Load aggregate outputs:

```python
from pathlib import Path
import pandas as pd

results_dir = Path("evaluation_results/query_eval/<run_dir>")
cell_df = pd.read_csv(results_dir / "cell_level_aggregate.csv")
query_df = pd.read_csv(results_dir / "query_level_aggregate.csv")
dataset_df = pd.read_csv(results_dir / "dataset_level_aggregate.csv")
suite_df = pd.read_csv(results_dir / "suite_summary.csv")
```

## Result Files

Each suite run writes to `evaluation_results/query_eval/<run_dir>/`.

| File | Use |
| --- | --- |
| `manifest.json` | Run configuration, datasets, queries, modes, artifact paths, and code version. |
| `raw_runs.jsonl` | Source of truth. One JSON object per warm-up or measured run. |
| `cell_level_aggregate.csv` | One row per dataset/query/mode cell. Best table for most plots. |
| `query_level_aggregate.csv` | Summaries by query family and mode. |
| `dataset_level_aggregate.csv` | Summaries by dataset and mode. |
| `suite_summary.csv` | Top-level mode comparison. |
| `external_baseline_summary.csv` | grep/ripgrep comparison rows for publishability runs. |
| `planner_strategy_summary.csv` | qidx3 planner strategy and verification-work summary. |
| `qidx_size_summary.csv` | qidx3/raw/static-compressed/qidx2 size ratios. |
| `amortization_summary.csv` | Break-even query count estimates when build metrics are recorded. |
| `adversarial_publishability_report.md` | Explicit overclaim and risk audit. |

Raw records include timing, CPU, peak RSS, match count, result lines, exact-set match, TP/FP/FN, precision, recall, F1, sampled false positives, sampled false negatives, artifact paths, config labels, and placeholders for future instrumentation.

## Interpretation Rules

- Treat `decompressed_text` as the correctness baseline.
- Expect `full_decompression` to match the baseline exactly.
- Allow `minor_optimization` to trade correctness for speed; measure the tradeoff instead of hiding it.
- Use `raw_runs.jsonl` for exact claims and audit evidence.
- Use `cell_level_aggregate.csv` for most tables and figures.
- Inspect sampled false positives and false negatives before writing qualitative conclusions.

Useful columns to inspect first:

- `all_runs_exact_set_match`
- `median_fp`
- `median_fn`
- `median_recall`
- `median_wall_time_ms`
- `median_peak_rss_mb`

## Environment Notes

The shipped `xorc-cli` may not run on every host. `artifacts.py` tries the shipped binary first and falls back to a host-compatible local build from the repo's LogLite-B source.

On macOS, a missing Boost header error such as `boost/dynamic_bitset.hpp` can usually be fixed with:

```bash
brew install boost
```

Local runtime builds are stored under `.query_eval_runtime/`.

## Part-3 Handoff

The part-3 teammate should:

1. run the tests
2. run the suite through the CLI
3. inspect `cell_level_aggregate.csv`
4. inspect `raw_runs.jsonl` for surprising cells
5. use pandas or notebooks for plots
6. write the correctness/performance narrative

Do not rebuild the query engine in a notebook. The package is the source of truth.

## Related Docs

- [Big Data Project README](../readme.md)
- [Original LogLite README](../loglite/readme.md)
- [qidx3 Publishability Pack](PUBLISHABILITY_QIDX3.md)

## Complete Static-Bloom Evaluation

The final complete suite is documented in `query_eval/COMPLETE_EVALUATION.md`.

Run it from `Big_Data/` with:

```bash
python3 -m query_eval.cli run-suite \
  --profile complete_static_evaluation \
  --repetitions 10 \
  --warmups 1 \
  --config-label complete_static_evaluation \
  --config-version complete_static.v1
```

The latest completed full result bundle is:

`evaluation_results/query_eval/20260507_155729_complete_static_evaluation`

Use `static_bloom_summary.csv` for static-Bloom-specific plots and `complete_evaluation_summary.csv` for token-safe versus stress-query interpretation.
