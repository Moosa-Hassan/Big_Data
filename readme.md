# LogLite-Based Compressed Log Querying

This project studies query execution over LogLite-compressed logs. Instead of always decompressing logs back to plain text before searching, it compares when a query can run directly on the compressed representation or with limited reconstruction while preserving correctness.

## Start Here

- [Query Evaluation README](./query_eval/README.md): canonical execution, measurement, and analysis workflow.
- [Original LogLite README](./loglite/readme.md): upstream LogLite build and compressor reference.
- `part3_starter.ipynb`: analysis notebook that reads generated CSV outputs.
- `main.ipynb`: historical prototype notebook. Use it for context only; do not add new experiment logic there.

## Research Goal

The project asks:

> If logs are compressed using LogLite-B, how well can simple search queries be answered by different execution strategies, and what are the time, CPU, memory, and correctness tradeoffs?

The modern evaluation layer compares three modes:

- `decompressed_text`: scan decompressed text and treat it as the correctness baseline.
- `full_decompression`: read the `.lite.b` bitstream and reconstruct lines while preserving LogLite window state.
- `minor_optimization`: use L-window hints to skip candidate lines and measure the correctness/performance tradeoff honestly.

## Recommended Workflow

Run commands from `Big_Data/`:

```bash
python3 -m unittest -v tests/test_query_eval.py
python3 -m query_eval.cli stage-datasets --datasets linux apache hdfs openstack android
python3 -m query_eval.cli ensure-artifacts --datasets linux apache hdfs openstack android
python3 -m query_eval.cli run-suite \
  --datasets linux apache hdfs openstack android \
  --queries common phrase selective conjunctive \
  --modes decompressed_text full_decompression minor_optimization \
  --repetitions 10 \
  --warmups 1 \
  --config-label part2_research_eval
```

Results are written under `evaluation_results/query_eval/<run_dir>/`. Use the raw JSONL and aggregate CSVs for part-3 analysis.

## Project Layout

| Path | Purpose |
| --- | --- |
| [`query_eval/`](./query_eval/README.md) | Canonical research-grade evaluation subsystem. |
| `tests/test_query_eval.py` | Test coverage for registry, artifacts, metrics, and mode behavior. |
| `part3_starter.ipynb` | Notebook for reading aggregate outputs and building first-pass tables or plots. |
| `main.ipynb` | Historical prototype notebook. |
| `compressed_logs/` | Generated `.lite.b`, decompressed text, and L-window artifacts. |
| `dataset/loghub/` | Local LogHub dataset samples. |
| [`loglite/`](./loglite/readme.md) | Vendor LogLite implementation and baseline compressors. |
| `notes.txt` | Scratch notes, not a documentation source of truth. |

## LogLite Reference Map

Useful LogLite-B source files:

- `loglite/LogLite-B/src/common/constants.h`: format constants such as window size, stream encoder count, max length, and RLE count.
- `loglite/LogLite-B/src/compress/stream_compress.h` and `.cc`: streaming compression and decompression logic.
- `loglite/LogLite-B/src/common/file.cc`: binary I/O helpers for `.lite` and `.lite.b`.
- `loglite/LogLite-B/src/tools/xorc-cli.cc`: CLI used to compress, decompress, test, and dump L-window snapshots.

The `loglite/baselines/` subtree contains third-party reference compressors from the original LogLite evaluation. They are kept for completeness and extended experiments, but they are not the source of truth for this project’s compressed-query evaluation.

## Documentation Ownership

- This README explains the Big Data project at a high level.
- [Query Evaluation README](./query_eval/README.md) owns all reproducible experiment workflow details.
- [Original LogLite README](./loglite/readme.md) owns upstream LogLite build and compressor usage notes.
- Notebooks should present, inspect, and plot results. They should not duplicate the execution pipeline.

## Complete Evaluation Handoff

The complete static-Bloom evaluation is implemented in `query_eval`. Start with:

`query_eval/COMPLETE_EVALUATION.md`

The latest complete result bundle is:

`evaluation_results/query_eval/20260507_155729_complete_static_evaluation`

Notebook users should load that directory in `Visualization.ipynb`; they should not reimplement evaluation logic in the notebook.
