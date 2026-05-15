# Project Context Handoff: Query-Aware Compressed Log Analysis with LogLite

This document is a complete handoff note for the Big Data Analytics project in this repository. It is written for both humans and LLM/code agents that need to understand the project quickly and continue the work without relying on chat history.

The short version: this project extends LogLite-style compressed log storage with query capabilities, evaluates several compressed-domain query strategies, and now contains a reproducible `query_eval` evaluation system that supports a complete 16-dataset static-Bloom evaluation suite.

---

## 1. Project Goal

The research problem is efficient querying over compressed logs.

Normal log analytics systems often decompress logs first and then search them. That creates a decompression bottleneck, especially when users only need a small subset of records. This project studies whether we can answer keyword-style queries directly on, or partly through, LogLite-compressed logs while avoiding unnecessary reconstruction.

The project is built around LogLite-B, a variant of LogLite included in this repository. The current direction is not to replace LogLite entirely, but to extend its compressed representation and evaluation tooling so we can test query-aware compressed-domain search.

The main research question is:

> Can we preserve query correctness while reducing the amount of decompression/reconstruction work required to answer search queries over compressed logs?

Subquestions:

- Can an exact compressed-domain search path match decompressed-text search?
- Can a selective optimization skip records safely?
- How much runtime improvement is possible?
- How does behavior change across different log systems and query types?
- Where do false negatives appear, and what does that reveal about the algorithm?

---

## 2. Repository-Level Orientation

The project root for the implementation is:

```text
Big_Data/
```

Important files/directories:

```text
Big_Data/
  query_eval/                  # Main reproducible evaluation package; source of truth.
  loglite/LogLite-B/            # Original and modified LogLite-B C++ codec code.
  dataset/loghub/               # Staged LogHub 2k datasets.
  compressed_logs/              # Generated original/static compressed artifacts.
  evaluation_results/query_eval/# Generated result bundles from evaluation runs.
  tests/test_query_eval.py      # Integration/unit tests for the evaluation stack.
  Visualization.ipynb           # Analysis/plotting notebook; not source of truth for execution.
  main.ipynb                    # Historical/prototype notebook.
  implementations.py            # Older prototype functions; useful context, not final architecture.
  notes.txt                     # Project notes and work split history.
  readme.md                     # Top-level project README.
```

The most important rule for future work:

> `query_eval` is the source of truth for experiments. Notebooks should load persisted results and visualize them; they should not reimplement query logic.

---

## 3. LogLite Compression Background

LogLite groups log records by line length using an L-window.

The L-window is a mapping:

```text
line_length -> queue/list of recent logs of that length
```

In the original/default setup:

- The L-window stores up to 8 entries per length bucket.
- A 3-bit window index is enough to refer to one of 8 entries.
- During compression, a new log line is compared against same-length entries.
- The most similar previous log is selected.
- Similarity is based on the number of equal positions.

The project notes describe the XOR-like transform as:

```text
for each position i:
  if current_log[i] == window_log[i]:
      result[i] = '\0'
  else:
      result[i] = current_log[i]
```

The resulting string contains null bytes where the current line matches the reference template and literal characters where it differs. Null runs are then run-length encoded.

### Original Bitstream Shape

For an uncompressed/raw record:

```text
flag bit = 0
next 15 bits = original line length
next 8 * length bits = raw bytes
```

For a compressed record:

```text
flag bit = 1
next K bits = window index
next 13 bits = RLE payload bit length
next payload bits = RLE-encoded XOR result
```

For the original 8-entry window, `K = 3`.

There is no padding between records. The next record starts immediately after the previous record ends.

---

## 4. Early Prototype Work

The project first implemented and evaluated two Python query functions over LogLite-B compressed binaries.

### 4.1 Full Decompression Search

Prototype function:

```python
keyword_search_loglite_binary_full_decompression()
```

Meaning:

- Walk the compressed binary.
- Reconstruct every line in order.
- Run the keyword query immediately after reconstructing each line.
- Store only matching lines instead of writing the full decompressed file.

Correctness:

- Expected to match decompressed-text search exactly.
- It does match the baseline in evaluation.

Limitation:

- It still reconstructs every record, so it does not fully solve the decompression bottleneck.
- It is mainly an exact compressed-domain reference path.

### 4.2 Minor Optimization Search

Prototype function:

```python
keyword_search_loglite_binary_minor_optimization()
```

Meaning:

- Parse the final L-window dump.
- Find lengths whose final templates contain the query keyword(s).
- During one pass over the compressed binary, only reconstruct records with matching lengths.

The idea gives speed improvements, but it has a correctness problem.

Why it fails:

- Original LogLite decompression is stateful.
- The L-window changes as records are decoded.
- If the optimization skips a record, the local decompression window is not updated.
- Later compressed records may refer to a state that the optimized decoder no longer has.

Observed behavior:

- No major false-positive problem.
- False negatives appear for some queries.
- The algorithm can miss true matching lines.

Conclusion:

- `minor_optimization` is useful as a measured baseline, but it is not a safe final algorithm.
- Future optimization needs to address L-window state loss.

---

## 5. Research-Grade Evaluation Package: `query_eval`

The project then moved from notebook experiments into a reusable evaluation architecture under:

```text
query_eval/
```

The goal was to make experiments:

- modular
- reproducible
- inspectable
- easy to rerun
- suitable for research reporting
- easy for future teammates to extend

### 5.1 Main Modules

```text
query_eval/specs.py
```
Defines typed dataclasses and central data structures:

- `DatasetSpec`
- `ArtifactSpec`
- `QuerySpec`
- `RunConfig`
- `CellRunSpec`
- `TimingMeasurement`
- `MemoryMeasurement`
- `CorrectnessMeasurement`
- `ModeRunResult`
- `RunRecord`

```text
query_eval/registry.py
```
Owns:

- dataset registry
- query IDs
- mode names
- suite profile
- path conventions

```text
query_eval/artifacts.py
```
Owns:

- dataset staging
- artifact path construction
- original LogLite artifact generation
- static LogLite artifact generation
- `xorc-cli` resolution/build fallback

```text
query_eval/search_backends.py
```
Owns Python search implementations:

- plaintext decompressed search
- full compressed binary decompression search
- minor optimization
- static Bloom search

```text
query_eval/modes.py
```
Central dispatch layer from mode names to backend implementations.

```text
query_eval/queries.py
```
Public notebook-friendly wrappers such as:

- `query_common(mode_chosen, dataset)`
- `query_phrase(mode_chosen, dataset)`
- `query_selective(mode_chosen, dataset)`
- `query_conjunctive(mode_chosen, dataset)`

It also includes wrappers for newer query families.

```text
query_eval/metrics.py
```
Computes correctness metrics:

- exact set match
- true positives
- false positives
- false negatives
- precision
- recall
- F1
- sampled FP/FN lines

```text
query_eval/profiling.py
```
Measures:

- wall time
- CPU time
- peak RSS memory

```text
query_eval/runner.py
```
Runs full suites using subprocess isolation.

Important design:

- Parent process orchestrates the suite.
- Each measured cell execution happens in a fresh subprocess.
- This avoids state bleed and makes memory measurements more defensible.

```text
query_eval/reports.py
```
Builds aggregate CSV reports from raw JSONL ledgers.

```text
query_eval/cli.py
```
Command-line entrypoint for staging, artifact generation, running cells, running suites, and rebuilding reports.

```text
query_eval/query_curation.py
```
Deterministically curates query payloads from staged datasets and writes the locked query manifest.

```text
query_eval/locked_query_manifest.json
```
Locked manifest for all complete-suite dataset/query payloads.

```text
query_eval/COMPLETE_EVALUATION.md
```
Detailed complete-suite handoff and methodology.

---

## 6. Evaluation Modes

The current complete suite evaluates four modes.

### 6.1 `decompressed_text`

Role:

- Correctness baseline.
- Scans the decompressed text artifact produced from the compressed data.

Interpretation:

- This is the source of truth for correctness metrics.

### 6.2 `full_decompression`

Role:

- Exact compressed-domain reference.
- Reads the `.lite.b` bitstream and reconstructs every line while preserving original L-window state.

Expected behavior:

- Must match `decompressed_text` exactly.
- Strict validation fails if this mode diverges.

### 6.3 `minor_optimization`

Role:

- Earlier optimization candidate.
- Uses final L-window hints and length filtering.

Expected behavior:

- May lose recall.
- Measured honestly, not treated as a strict exact mode.

Known weakness:

- Skipping records breaks dynamic L-window state.

### 6.4 `static_bloom`

Role:

- Main current optimization candidate.
- Uses static L-window and per-record 64-bit Bloom-style token bitmap.

Mechanism:

- Every record carries a 64-bit bitmap.
- Each alphanumeric token is hashed into one of 64 bit positions.
- Query keywords are hashed into a query bitmap.
- If `(record_bitmap & query_bitmap) != query_bitmap`, the record cannot contain all query token bits and is skipped.
- If the bitmap passes, the record is reconstructed and exact substring matching is applied before emitting a result.

Important:

- Bitmap collisions may cause unnecessary reconstruction, but exact post-filtering prevents false positives.
- The no-false-negative claim is strongest for token-compatible queries.
- Arbitrary substrings are not guaranteed because the bitmap hashes alphanumeric tokens, not all possible substrings.

---

## 7. Static L-Window + Bloom Codec Changes

A static codec variant exists under:

```text
loglite/LogLite-B/src_static/
```

Important C++ files:

```text
loglite/LogLite-B/src_static/common/constants.h
loglite/LogLite-B/src_static/compress/stream_compress.cc
loglite/LogLite-B/src_static/tools/xorc-cli.cc
```

Key changes:

- L-window capacity increased from 8 to 32 entries.
- Window index width increased from 3 bits to 5 bits.
- The L-window is static/append-only instead of sliding.
- Records include a 64-bit word bitmap immediately after the flag bit.
- The static window is intended to avoid state-loss problems caused by skipping records.

Static bitstream shape:

For a raw static record:

```text
flag bit = 0
next 64 bits = word bitmap
next 15 bits = original line length
next 8 * length bits = raw bytes
```

For a compressed static record:

```text
flag bit = 1
next 64 bits = word bitmap
next 5 bits = static window index
next 13 bits = RLE payload bit length
next payload bits = RLE-encoded XOR result
```

Static artifact paths:

```text
<sample>.lite.static.b
<sample>.lite.static.decom
<sample>.window.static.txt
```

Original artifact paths:

```text
<sample>.lite.b
<sample>.lite.decom
<sample>.window.txt
```

---

## 8. Dataset Program

The complete suite now targets all 16 registered LogHub TEXT datasets:

```text
linux
apache
hdfs
openstack
android
zookeeper
healthapp
hpc
hadoop
bgl
mac
proxifier
spark
openssh
thunderbird
windows
```

The older canonical five-dataset subset was:

```text
linux
apache
hdfs
openstack
android
```

That old subset is still useful historically because earlier results were based on it, but the current intended evaluation boundary is the full 16-dataset complete suite.

Datasets are staged under:

```text
dataset/loghub/<Dataset>/<Dataset>_2k.log
```

Each dataset also stages its LogHub template CSV where available.

---

## 9. Query Families

The complete suite uses 8 query families:

```text
common_token
medium_token
rare_token
common_phrase
selective_phrase
numeric_identifier
conjunctive
bloom_stress_substring
```

The first seven are token-safe query families. They are intended to align with the alphanumeric-token hashing used by `static_bloom`.

`bloom_stress_substring` is deliberately not token-safe. It probes the edge of the Bloom-token assumption and should be interpreted separately.

Query payloads are not generated at evaluation runtime. They are locked in:

```text
query_eval/locked_query_manifest.json
```

The manifest includes metadata such as:

- query ID
- family
- description
- dataset payload map
- token-safe flag
- stress-query flag
- expected selectivity band

To intentionally regenerate the manifest:

```bash
python3 -m query_eval.query_curation --write
```

Only do this if the evaluation protocol is being changed deliberately.

---

## 10. Complete Evaluation Suite

The one official full suite profile is:

```text
complete_static_evaluation
```

Matrix:

```text
16 datasets x 8 query families x 4 modes = 512 cells
```

With:

```text
1 warmup + 10 measured repetitions per cell
```

Total executions:

```text
512 warmups + 5120 measured = 5632 raw executions
```

CLI command:

```bash
python3 -m query_eval.cli run-suite \
  --profile complete_static_evaluation \
  --repetitions 10 \
  --warmups 1 \
  --config-label complete_static_evaluation \
  --config-version complete_static.v1
```

Before running the suite, stage datasets and artifacts:

```bash
python3 -m query_eval.cli stage-datasets
```

```bash
python3 -m query_eval.cli ensure-artifacts
```

Run tests:

```bash
python3 -m unittest -v tests/test_query_eval.py
```

---

## 11. Result Files

A complete suite run writes a timestamped directory under:

```text
evaluation_results/query_eval/
```

Expected files:

```text
manifest.json
raw_runs.jsonl
cell_level_aggregate.csv
query_level_aggregate.csv
dataset_level_aggregate.csv
suite_summary.csv
static_bloom_summary.csv
query_manifest.csv
dataset_coverage.csv
complete_evaluation_summary.csv
```

File purposes:

```text
manifest.json
```
Run metadata: datasets, queries, modes, config, artifact paths, code version.

```text
raw_runs.jsonl
```
Source of truth. One JSON object per warmup or measured execution.

```text
cell_level_aggregate.csv
```
One row per dataset/query/mode cell. Usually the best table for plots.

```text
query_level_aggregate.csv
```
Aggregated by query family and mode.

```text
dataset_level_aggregate.csv
```
Aggregated by dataset and mode.

```text
suite_summary.csv
```
Top-level mode comparison.

```text
static_bloom_summary.csv
```
Static-Bloom-focused table with correctness, speedup, and Bloom skip-rate fields.

```text
query_manifest.csv
```
Human-readable CSV form of the locked query manifest.

```text
dataset_coverage.csv
```
Dataset metadata coverage table.

```text
complete_evaluation_summary.csv
```
All-query, token-safe, and stress-query breakdowns by mode.

---

## 12. Recorded Final Complete-Suite Results

The final recorded complete run was:

```text
evaluation_results/query_eval/20260507_155729_complete_static_evaluation
```

Note: in the current checkout, generated result bundles may not be present. If missing, rerun the suite using the command above.

Recorded run size:

```text
raw executions: 5632
measured executions: 5120
warmups: 512
cell rows: 512
static rows: 128
```

Mode-level recorded results:

```text
decompressed_text:
  exact cells: 128/128
  median wall time: 0.655927 ms

full_decompression:
  exact cells: 128/128
  median wall time: 77.604864 ms

minor_optimization:
  exact cells: 59/128
  median wall time: 43.590344 ms
  median recall: 0.991549
  median F1: 0.995757

static_bloom:
  exact cells: 96/128 overall
  exact token-safe cells: 95/112
  exact stress cells: 1/16
  median wall time: 47.091396 ms
  median recall: 1.0
  median F1: 1.0
  median Bloom skip rate: 0.81825
  false positives: 0
  false negatives: 11975 across all static cells
```

Interpretation:

- `full_decompression` is exact and validates the Python compressed-domain decoder.
- `minor_optimization` is faster but less reliable.
- `static_bloom` skips a large fraction of records and is much stronger than `minor_optimization`, but it is not universally exact.
- Static Bloom failures are one-sided: it does not invent wrong matches; it misses some true matches.
- Stress-substring queries fail as expected because they are deliberately outside the token-hash assumption.
- Some token-safe queries also still fail, which is important research evidence for future refinement.

---

## 13. Previous Static-Bloom Five-Dataset Validation

Before the complete suite, `static_bloom` was validated on the old five-dataset/four-query matrix:

Datasets:

```text
linux
apache
hdfs
openstack
android
```

Query families:

```text
common
phrase
selective
conjunctive
```

Modes:

```text
decompressed_text
full_decompression
minor_optimization
static_bloom
```

Recorded result directory:

```text
evaluation_results/query_eval/20260507_022830_static_bloom_validation
```

Recorded headline results:

```text
raw executions: 880
measured executions: 800
cells: 80
static_bloom exact cells: 20/20
static_bloom false positives: 0
static_bloom false negatives: 0
full_decompression median wall time: 83.87 ms
static_bloom median wall time: 54.63 ms
median static_bloom skip rate: 69.2%
```

This result looked very strong, but it was limited to the older smaller query set. The complete suite revealed more nuanced behavior.

---

## 14. Visualization Layer

The visualization layer is:

```text
Visualization.ipynb
```

Its purpose:

- Load result bundles.
- Display aggregate tables.
- Plot correctness and performance.
- Separate token-safe results from stress-query results.
- Help write reports and presentations.

It should not:

- generate artifacts
- execute raw queries
- implement search logic
- replace `query_eval`

To use it:

1. Open `Visualization.ipynb`.
2. Set `RESULTS_DIR` to a complete result directory, for example:

```python
Path("evaluation_results/query_eval/20260507_155729_complete_static_evaluation")
```

3. Run the notebook top to bottom.

If the result bundle is missing, rerun the complete suite first.

---

## 15. Testing Strategy

Main test command:

```bash
python3 -m unittest -v tests/test_query_eval.py
```

The test suite checks:

- all 16 datasets are registered
- `static_bloom` is registered
- complete suite profile resolves to 16 datasets, 8 query families, and 4 modes
- every dataset has every query family
- query metadata is complete
- artifact paths are constructed correctly
- original/static artifacts exist for all active datasets
- `full_decompression` matches `decompressed_text`
- `minor_optimization` runs across all active dataset/query pairs
- `static_bloom` runs across all active dataset/query pairs
- Linux notebook-regression queries remain exact under static Bloom
- correctness outputs are stable across repeated runs

Recorded final test result:

```text
Ran 20 tests in 29.033s
OK
```

---

## 16. Known Environment and Codec Caveats

### 16.1 macOS / Apple Silicon / AVX2

LogLite-B uses AVX2 intrinsics. On Apple Silicon, the evaluation code may build x86_64 binaries and run them through Rosetta.

Local runtime builds are stored under:

```text
.query_eval_runtime/
```

If Boost headers are missing, install Boost. On macOS:

```bash
brew install boost
```

### 16.2 Python SSL Download Issue

Dataset staging originally hit Python SSL certificate verification issues on this environment. `query_eval.artifacts` now falls back to `curl` if `urllib` fails.

### 16.3 Mac Dataset C++ Decompressor Hang

On this environment, the original/static C++ `--test` decompressor can hang or assert on `Mac_2k.log` after compression and window files are already produced.

The artifact layer handles this by materializing decompressed text artifacts with the Python codec mirror when compressed/window artifacts exist.

This is intentional because it keeps Mac in the complete suite instead of silently dropping it.

### 16.4 Static Bloom Tokenization Limitation

`static_bloom` hashes alphanumeric tokens into a 64-bit bitmap. It is not an arbitrary substring index.

Therefore:

- token-safe queries are the appropriate correctness target for current static Bloom claims
- substring stress queries are intentionally adversarial
- do not claim exact arbitrary substring support without adding fallback logic

---

## 17. How to Continue the Research

Most important next steps:

1. Improve static Bloom exactness for token-safe queries.
2. Add fallback logic for unsafe substrings or label those queries as unsupported.
3. Instrument why static Bloom misses token-safe matches.
4. Compare decoded/skipped records with speedup to understand runtime bottlenecks.
5. Consider larger-than-2k datasets after the 2k complete suite is stable.
6. Improve query curation manually if some automatically selected payloads are semantically weak.
7. Add plots and report narrative from `complete_evaluation_summary.csv` and `static_bloom_summary.csv`.

Promising technical directions:

- More than one hash bit per token to reduce ambiguity.
- Store phrase-aware metadata in addition to token bitmaps.
- Add fallback mode for non-token-safe queries.
- Track exact record IDs or line offsets to debug false negatives faster.
- Add richer compression-time template metadata.
- Reduce fixed RLE traversal cost when skipping.

---

## 18. Report Narrative Guidance

A careful report should say:

- The project successfully built an exact compressed-domain full-decompression search path.
- The earlier minor optimization exposed why dynamic L-window state makes skipping hard.
- Static L-window plus Bloom metadata is a stronger design because it reduces state drift.
- The complete evaluation is substantially broader than the initial Linux-only and 5-dataset experiments.
- `static_bloom` provides meaningful skip rates and runtime improvement, but is not yet universally exact.
- No false positives were observed in the final complete static-Bloom run; failures were false negatives.
- Stress-substring queries demonstrate that the current Bloom metadata is token-oriented, not substring-complete.
- The system is now structured enough for reproducible experimentation and future improvements.

Avoid overstating:

- Do not claim static Bloom is fully exact for all queries.
- Do not claim arbitrary substring support.
- Do not present stress-query failures as surprising bugs; they are intended probes of the current design boundary.

---

## 19. Quick Command Reference

From `Big_Data/`:

```bash
python3 -m unittest -v tests/test_query_eval.py
```

```bash
python3 -m query_eval.cli stage-datasets
```

```bash
python3 -m query_eval.cli ensure-artifacts
```

```bash
python3 -m query_eval.query_curation --write
```

```bash
python3 -m query_eval.cli run-suite \
  --profile complete_static_evaluation \
  --repetitions 10 \
  --warmups 1 \
  --config-label complete_static_evaluation \
  --config-version complete_static.v1
```

```bash
python3 -m query_eval.cli build-reports \
  --results-directory evaluation_results/query_eval/<run_dir>
```

---

## 20. Current Source-of-Truth Files

For future agents, start with these files:

```text
query_eval/COMPLETE_EVALUATION.md
query_eval/README.md
query_eval/registry.py
query_eval/specs.py
query_eval/artifacts.py
query_eval/search_backends.py
query_eval/modes.py
query_eval/runner.py
query_eval/reports.py
query_eval/query_curation.py
query_eval/locked_query_manifest.json
tests/test_query_eval.py
Visualization.ipynb
```

If you need historical/prototype context, inspect:

```text
notes.txt
main.ipynb
implementations.py
loglite/LogLite-B/src_static/
```

---

## 21. One-Sentence State Summary

The project has moved from a Linux-only notebook prototype to a reproducible 16-dataset, 8-query-family, 4-mode evaluation system for compressed-domain LogLite querying, with `static_bloom` implemented as the main optimization candidate and a complete evaluation showing strong skip-rate/runtime benefits but remaining recall limitations, especially for substring stress queries and some token-safe rare/numeric cases.
