# Query Evaluation Subsystem README


## 1. Project Summary

This project studies **querying LogLite-compressed logs**.

Instead of always fully decompressing compressed log files back into plain text,
we want to understand whether we can answer keyword and phrase queries:
- directly on the compressed representation
- or with only partial / selective reconstruction
- while measuring both correctness and performance tradeoffs

The repository contains:
- the LogLite-B codec implementation in C++
- a course/research prototype around compressed-domain querying
- staged LogHub datasets for evaluation
- a new research-grade evaluation subsystem in `query_eval/`

At a high level, the project asks:

> If logs are compressed using LogLite-B, how well can we answer simple search
> queries using different execution strategies, and what do we gain or lose in
> time, CPU, memory, and correctness?

## 2. Research Goal

The goal is not just to make a query function run once.
The goal is to build a defensible evaluation pipeline that can support:
- repeated experiments
- multiple datasets
- multiple query families
- multiple execution modes
- paper-style performance and correctness analysis

For part 2 specifically, the objective is to evaluate 3 modes across 5 active
LogHub datasets and 4 query families.

Canonical part-2 matrix:
- 5 datasets
- 4 query families
- 3 modes
- total cells: 60

Canonical measurement protocol:
- 1 warm-up run per cell
- 10 measured repetitions per cell
- each measured run in a fresh subprocess
- preserve raw per-run records
- regenerate aggregate summaries from the raw ledger

## 3. High-Level Methodology

The methodology implemented in this package is:

1. Stage raw LogHub dataset samples locally.
2. Use the repo's own `LogLite-B` C++ codec to generate standard artifacts.
3. Execute the same registered query under 3 different modes.
4. Treat the decompressed-text result set as the correctness baseline.
5. Compare every candidate mode to that baseline.
6. Record timing, CPU, memory, match count, and correctness metrics.
7. Persist raw records in JSONL.
8. Build aggregate CSV reports from the raw ledger.

This design is intentional.
Notebooks can present or inspect the results, but they should not be the place
where experiment logic is defined.

## 4. What the 3 Modes Mean

The system supports exactly 3 execution modes:
- `decompressed_text`
- `full_decompression`
- `minor_optimization`

These are defined in `query_eval.registry` and dispatched in
`query_eval.modes`.

### `decompressed_text`
This is the correctness baseline.

Behavior:
- scan the decompressed text artifact generated from the `.lite.b` file
- return all lines containing the query payload

This is the source of truth for precision / recall / F1 calculations.

### `full_decompression`
This is the faithful compressed-domain search path.

Behavior:
- read the `.lite.b` bitstream directly
- walk it entry by entry
- reconstruct lines using the same sliding-window semantics as the codec
- store only matched lines

Expectation:
- this mode should match `decompressed_text` exactly

### `minor_optimization`
This is the experimental optimization candidate.

Behavior:
- inspect the final L-window templates dumped by the codec
- identify line lengths that appear promising for a given query
- skip many lines based on those lengths
- only reconstruct some candidate lines

Important limitation:
- when this mode skips lines, it does **not** fully maintain the decode state
  required for future exact reconstruction
- therefore this mode is **not** guaranteed to be exact

This is not a bug in the evaluation architecture.
This is the behavior we want to measure honestly.

## 5. What Part 1 Established

Part 1, as assumed by part 2, established the compressed-domain query contract.
The expected function contract was:

- `keyword_search_loglite_binary_full_decompression()`
- `keyword_search_loglite_binary_minor_optimization()`

In the repository as it existed before part 2, the logic for those functions was
present in `main.ipynb` rather than in reusable modules.

Part 1 also established or motivated several important codec-facing ideas:
- compressed files use LogLite-B's bitstream format
- exact reconstruction requires maintaining the L-window state correctly
- the final L-window is useful for optimization ideas
- selective skipping may improve speed but can reduce correctness

Part 2 did **not** rewrite the research intent of part 1.
Instead, it extracted and formalized that work into a reusable package.

### What part 2 assumes from part 1

The evaluation layer assumes the following contract:
- full decompression mode is intended to be exact
- minor optimization mode is evaluable even if incomplete
- `xorc-cli` can generate a `.window.txt` dump via `--window-output-path`

### What part 2 extracted from the notebook

The notebook logic was lifted into:
- `query_eval.search_backends.keyword_search_loglite_binary_full_decompression`
- `query_eval.search_backends.keyword_search_loglite_binary_minor_optimization`

So part 3 should no longer depend on `main.ipynb` as the authoritative code path.

## 6. What Part 2 Implemented

Part 2 implemented a complete research-grade evaluation subsystem under:
- `Big_Data/query_eval/`

### Package structure

- `specs.py`
  - typed data model for datasets, queries, artifacts, configs, and run records
- `registry.py`
  - canonical dataset registry, query registry, mode registry, and shared paths
- `artifacts.py`
  - dataset staging, artifact generation, `xorc-cli` resolution, host-compatible
    local build logic
- `window_loader.py`
  - parsing `.window.txt` into an in-memory structure
- `search_backends.py`
  - actual execution logic for baseline, full decompression, and minor
    optimization
- `modes.py`
  - explicit dispatch between the 3 modes
- `queries.py`
  - public note-compatible query functions with `(mode_chosen, dataset)`
- `metrics.py`
  - exact-set comparison, TP / FP / FN, precision / recall / F1, and sampled
    differences
- `profiling.py`
  - wall time, CPU time, and peak RSS measurement
- `persistence.py`
  - JSONL, JSON, and CSV writing / reading helpers
- `runner.py`
  - single-cell execution and full-suite orchestration in subprocesses
- `reports.py`
  - raw-ledger-to-aggregate CSV summary generation
- `cli.py`
  - command-line interface for reproducible execution
- `README.md`
  - this handoff document

### Design principles implemented

The package was built to satisfy these requirements:
- modularity
- explicitness
- reproducibility
- strong readability
- auditability
- future scaling to more datasets and richer instrumentation

### Strongly typed data model

The central dataclasses are:
- `DatasetSpec`
- `ArtifactSpec`
- `QuerySpec`
- `RunConfig`
- `CellRunSpec`
- `TimingMeasurement`
- `MemoryMeasurement`
- `CorrectnessMeasurement`
- `RunRecord`

This means the evaluation pipeline no longer passes around loose dictionaries
with undocumented fields.

### Active datasets for part 2

The architecture registers all 16 TEXT datasets from the LogLite paper, but only
5 are active for part 2 execution.

Active part-2 datasets:
- `linux` with `AL=91`, `NDL=208`
- `apache` with `AL=90`, `NDL=69`
- `hdfs` with `AL=140`, `NDL=135`
- `openstack` with `AL=295`, `NDL=131`
- `android` with `AL=123`, `NDL=720`

These are active because they provide diversity in average line length and line-
length variation.

### Query families implemented

The required public query functions now exist exactly as requested:
- `query_common(mode_chosen, dataset)`
- `query_phrase(mode_chosen, dataset)`
- `query_selective(mode_chosen, dataset)`
- `query_conjunctive(mode_chosen, dataset)`

These are thin wrappers over the registry-driven engine.

### Registered query payloads

#### Linux
- `common`: `"kernel"`
- `phrase`: `"failed"`
- `selective`: `"28842"`
- `conjunctive`: `["sshd", "failure"]`

#### Apache
- `common`: `"workerEnv"`
- `phrase`: `"scoreboard slot"`
- `selective`: `"Directory index forbidden"`
- `conjunctive`: `["mod_jk", "error"]`

#### HDFS
- `common`: `"PacketResponder"`
- `phrase`: `"NameSystem.addStoredBlock"`
- `selective`: `"replicate blk_"`
- `conjunctive`: `["PacketResponder", "terminating"]`

#### OpenStack
- `common`: `"status: 200"`
- `phrase`: `"GET /v2/"`
- `selective`: `"Deleting instance files"`
- `conjunctive`: `["GET /v2/", "status: 200"]`

#### Android
- `common`: `"PowerManagerService"`
- `phrase`: `"WindowManager"`
- `selective`: `"TextView"`
- `conjunctive`: `["PowerManagerService", "acquire"]`

### Artifact generation implemented

For each active dataset, the artifact manager now ensures these files exist:
- compressed binary: `<dataset>.lite.b`
- decompressed text: `<dataset>.lite.decom`
- L-window dump: `<dataset>.window.txt`

The artifact manager does all of this:
- stage dataset files under `dataset/loghub/...`
- stage template CSVs for manual inspection
- generate artifacts using `xorc-cli --test ... --window-output-path ...`
- validate that the artifact bundle exists before queries are run

### Reproducible subprocess execution

This was a core part of the implementation.
The canonical experiment model is:
- parent process orchestrates the suite
- child process executes one `(dataset, query, mode, repetition)` cell

This reduces state contamination across runs and makes memory measurements more
credible.

### Raw result persistence

Every measured or warm-up execution is preserved as one raw JSON object written
as one line to a JSONL file.

Each raw record stores:
- dataset
- query id
- mode
- query payload
- repetition index
- warm-up flag
- artifact paths
- config label and version
- code version
- wall time ms
- CPU time ms
- peak RSS MB
- match count
- full result lines
- exact-set match flag
- TP / FP / FN
- precision / recall / F1
- sampled false positives
- sampled false negatives
- placeholder fields for future instrumentation such as decoded / skipped bytes

### Aggregate reporting

The system generates:
- `cell_level_aggregate.csv`
- `query_level_aggregate.csv`
- `dataset_level_aggregate.csv`
- `suite_summary.csv`

These are all regenerated from the raw JSONL ledger.

### Tests added

The test suite lives in:
- `Big_Data/tests/test_query_eval.py`

Coverage includes:
- registry validity
- artifact path construction
- query manifest coverage
- metrics correctness
- missing-artifact error handling
- Linux end-to-end exactness
- full-decompression equality across the active query matrix
- minor-optimization smoke execution for all active dataset-query pairs
- repeated-run correctness stability

At the time of implementation, the full test suite passed:
- 14 tests passed

## 7. Current Operational Status

The system is operational.

What has already been done:
- active datasets staged
- compressed artifacts generated
- decompressed artifacts generated
- window dumps generated
- tests passed
- a smoke suite run was completed successfully

A smoke suite results directory already exists at:
- `evaluation_results/query_eval/20260415_021241_smoke_suite`

That smoke suite demonstrated something important:
- `full_decompression` matched baseline on the tested cells
- `minor_optimization` exposed a real weakness rather than pretending to be
  exact

Example observed result from the smoke suite:
- Linux `phrase` under `minor_optimization` had recall below 1.0 and non-zero
  false negatives

This is the correct research behavior: optimization weaknesses are visible.

## 8. What Part 3 Should Do

Part 3 should **not** rebuild execution logic.
Part 3 should consume the part-2 subsystem.

### Part 3 responsibilities

The teammate doing part 3 should focus on:
- running the registered query suite
- reading raw and aggregate outputs
- comparing the 3 modes
- building tables / graphs / visual summaries
- analyzing correctness and performance tradeoffs
- writing the narrative around those results

### Part 3 should use

Use these interfaces:
- CLI for reproducible evaluation runs
- Python query functions for quick inspection or notebook debugging
- raw JSONL and aggregate CSV outputs as the source of truth for analysis

### Part 3 should not do

Do **not**:
- re-implement query logic in a notebook
- call `loglite/scripts/loglite.py`
- treat `main.ipynb` as the authoritative experiment engine
- manually parse `.window.txt` in new ad hoc cells
- recompute metrics from scratch unless debugging a specific record

## 9. Two Ways to Run the System

There are 2 supported ways to use the subsystem:

1. CLI workflow
2. Python / notebook workflow

The CLI is the preferred path for full experiment generation.
Notebook / Python usage is useful for inspection, debugging, and visualization.

## 10. CLI Workflow

### Step 0: Work from the correct directory

Use `Big_Data/` as the working directory.

```bash
cd "/Users/hp/Desktop/Big Data Analytics/project/project repo/Big_Data"
```

### Step 1: Validate setup

```bash
python3 -m unittest -v tests/test_query_eval.py
```

This verifies:
- registry correctness
- artifact generation
- mode behavior
- exactness of `full_decompression`
- basic stability of the pipeline

### Step 2: Stage datasets

```bash
python3 -m query_eval.cli stage-datasets \
  --datasets linux apache hdfs openstack android
```

This ensures:
- raw LogHub sample logs exist locally
- template CSVs exist locally

### Step 3: Ensure artifacts exist

```bash
python3 -m query_eval.cli ensure-artifacts \
  --datasets linux apache hdfs openstack android
```

This ensures the following per dataset:
- `.lite.b`
- `.lite.decom`
- `.window.txt`

### Step 4: Run the full part-2 suite

```bash
python3 -m query_eval.cli run-suite \
  --datasets linux apache hdfs openstack android \
  --queries common phrase selective conjunctive \
  --modes decompressed_text full_decompression minor_optimization \
  --repetitions 10 \
  --warmups 1 \
  --config-label part2_research_eval
```

This is the canonical part-2 run.

What it does:
- runs 60 cells total
- runs 1 warm-up per cell
- runs 10 measured repetitions per cell
- executes each repetition in a fresh subprocess
- writes raw JSONL records
- writes aggregate CSV reports

### Step 5: Rebuild reports only

If raw JSONL already exists and you only want to regenerate aggregate CSVs:

```bash
python3 -m query_eval.cli build-reports \
  --results-directory evaluation_results/query_eval/<run_dir>
```

## 11. Python or Notebook Workflow

If a teammate wants to inspect behavior interactively, they can use Python or a
notebook.

### Recommended notebook rule

Use notebooks for:
- exploration
- inspection
- plots
- reading output files

Do **not** use notebooks to define new experiment logic that duplicates the
package.

### Launch notebook from the right location

```bash
cd "/Users/hp/Desktop/Big Data Analytics/project/project repo/Big_Data"
jupyter notebook
```

### Option A: Direct query calls inside a notebook

```python
from query_eval.queries import (
    query_common,
    query_phrase,
    query_selective,
    query_conjunctive,
)

linux_common_baseline = query_common("decompressed_text", "linux")
linux_common_full = query_common("full_decompression", "linux")
linux_common_minor = query_common("minor_optimization", "linux")

print(len(linux_common_baseline))
print(len(linux_common_full))
print(len(linux_common_minor))
```

This is useful for:
- spot checks
- understanding a specific query
- debugging why one mode differs

### Option B: Run the full suite from Python

```python
from query_eval.runner import run_suite
from query_eval.specs import RunConfig

results_dir = run_suite(
    dataset_slugs=["linux", "apache", "hdfs", "openstack", "android"],
    query_ids=["common", "phrase", "selective", "conjunctive"],
    mode_names=["decompressed_text", "full_decompression", "minor_optimization"],
    run_config=RunConfig(
        repetitions=10,
        warmups=1,
        profiling_enabled=True,
        strict_validation=True,
        config_label="part2_research_eval",
        config_version="part2.v1",
        sample_difference_limit=10,
    ),
)

print(results_dir)
```

This gives notebook users the same evaluation engine as the CLI.

### Option C: Load aggregate CSVs into pandas for part 3

```python
import pandas as pd
from pathlib import Path

results_dir = Path("evaluation_results/query_eval/<run_dir>")
cell_df = pd.read_csv(results_dir / "cell_level_aggregate.csv")
query_df = pd.read_csv(results_dir / "query_level_aggregate.csv")
dataset_df = pd.read_csv(results_dir / "dataset_level_aggregate.csv")
suite_df = pd.read_csv(results_dir / "suite_summary.csv")

cell_df.head()
```

This is probably the most useful notebook pattern for part 3.

## 12. How to Inspect Results

Results are written into a timestamped directory under:
- `evaluation_results/query_eval/`

A typical run directory contains:
- `manifest.json`
- `raw_runs.jsonl`
- `cell_level_aggregate.csv`
- `query_level_aggregate.csv`
- `dataset_level_aggregate.csv`
- `suite_summary.csv`

### `manifest.json`
This tells you:
- when the run happened
- what config was used
- which datasets were active
- which queries were active
- which modes were active
- artifact paths
- code version

### `raw_runs.jsonl`
This is the most important results file.
Each line is one run record.

Use this when you need:
- per-repetition timing
- result lines for auditability
- sampled false positives and false negatives
- exact raw evidence for a graph or claim

### `cell_level_aggregate.csv`
One row per:
- dataset x query x mode

This is the best table for most part-3 visualizations.

It includes medians for:
- wall time
- CPU time
- peak RSS
- match count
- TP / FP / FN
- precision / recall / F1
- exactness flags

### `query_level_aggregate.csv`
Grouped by:
- query family x mode

Useful for statements like:
- selective queries lose more recall under the optimization
- conjunctive queries stress certain modes differently

### `dataset_level_aggregate.csv`
Grouped by:
- dataset x mode

Useful for statements like:
- Android is harder than Apache
- OpenStack is more expensive due to longer lines

### `suite_summary.csv`
Grouped by:
- mode only

Useful for top-level summary figures.

## 13. How to Interpret Results Correctly

The most important interpretation rules are:

1. `decompressed_text` is the correctness baseline.
2. `full_decompression` is expected to match the baseline exactly.
3. `minor_optimization` is not assumed exact.
4. if `minor_optimization` loses recall, that is a valid research finding.
5. raw JSONL is the source of truth; aggregate CSVs are derived summaries.

### What to look at first

If you want quick confidence checks, inspect:
- `all_runs_exact_set_match`
- `median_fp`
- `median_fn`
- `median_recall`
- `median_wall_time_ms`
- `median_peak_rss_mb`

### What makes a result trustworthy

A run is easy to audit because you can check:
- `manifest.json` for configuration
- `raw_runs.jsonl` for raw observations
- aggregate CSVs for summarized views

## 14. Environment and Host Notes

This repo's shipped `xorc-cli` binary may not be directly runnable on every
machine.

The evaluation subsystem therefore includes host-compatible build logic.

What it does:
- tries the shipped binary first
- falls back to building `xorc-cli` from the repo's own `LogLite-B` source
- on Apple Silicon, builds an `x86_64` binary and runs it under Rosetta because
  the codec uses AVX2 intrinsics

### Boost dependency

Building the local `xorc-cli` requires Boost headers.
On macOS, if the local build fails with `boost/dynamic_bitset.hpp` missing, the
fix is:

```bash
brew install boost
```

A local runtime build is stored under:
- `.query_eval_runtime/`

## 15. Recommended Workflow for the Part-3 Teammate

If you are doing part 3, the clean workflow is:

1. run tests
2. run the suite through the CLI
3. inspect `cell_level_aggregate.csv`
4. inspect `raw_runs.jsonl` for any surprising cells
5. use pandas / notebook for plotting and narrative
6. avoid touching the execution internals unless you are debugging

### Minimal checklist

```bash
cd "/Users/hp/Desktop/Big Data Analytics/project/project repo/Big_Data"
python3 -m unittest -v tests/test_query_eval.py
python3 -m query_eval.cli run-suite \
  --datasets linux apache hdfs openstack android \
  --queries common phrase selective conjunctive \
  --modes decompressed_text full_decompression minor_optimization \
  --repetitions 10 \
  --warmups 1 \
  --config-label part2_research_eval
```

Then use the newest results directory under:
- `evaluation_results/query_eval/`

## 16. For LLMs and Coding Agents

If you are an LLM reading this repository, follow these rules.

### Source of truth

Treat these as the authoritative evaluation implementation:
- `query_eval/queries.py`
- `query_eval/runner.py`
- `query_eval/cli.py`
- `query_eval/reports.py`
- `query_eval/specs.py`
- `query_eval/registry.py`

Do **not** treat `main.ipynb` as the authoritative engine.
It is historical prototype context only.

### Execution rules

If you need to run the evaluation, prefer:
- `python3 -m query_eval.cli run-suite ...`

If you only need to inspect behavior for one query, use:
- the public query functions in `query_eval.queries`

### Interpretation rules

Assume:
- `decompressed_text` is the baseline
- `full_decompression` should match the baseline
- `minor_optimization` may underperform and that is expected

Do not silently reinterpret optimization failures as bugs unless the contract is
explicitly violated.

### Output rules

When discussing results, prioritize:
- raw JSONL for exact evidence
- cell-level aggregate CSV for plots and tables
- sampled false positives / false negatives for qualitative analysis

## 17. Practical File Map

Important files for a newcomer:
- `query_eval/README.md`
- `query_eval/queries.py`
- `query_eval/cli.py`
- `query_eval/runner.py`
- `query_eval/reports.py`
- `query_eval/search_backends.py`
- `tests/test_query_eval.py`

If you only read 3 files first, read:
1. `query_eval/README.md`
2. `query_eval/queries.py`
3. `query_eval/cli.py`

## 18. Final Guidance

The system is ready for part 3 use.

Use the CLI for reproducible runs.
Use notebooks for inspection and plotting.
Use raw JSONL plus aggregate CSVs as the results source of truth.
Do not rebuild execution logic in new ad hoc notebook cells.

If part 3 is done correctly, the person working on it should spend most of their
energy on:
- analysis
- visualizations
- interpretation
- writing

and not on rebuilding the engine.
