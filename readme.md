LogLite-Based Compressed Log Querying
=====================================

This project is a course/research prototype that builds a **query engine over LogLite-compressed logs**. Instead of always fully decompressing logs back to plain text, we explore when and how we can answer queries directly from the compressed representation (or with very limited reconstruction) while keeping correctness identical to raw-text search.

High-Level Goals
----------------

- Use the LogLite compressor (template + XOR + RLE with an L-window) on real system logs from the Loghub benchmark suite.
- Implement query operators in Python/Jupyter that operate on the **binary `.lite`/`.lite.b` files** produced by LogLite.
- Compare correctness and performance of compressed-domain execution against baseline text search.

Top-Level Layout
----------------

At the root of this repository you will find:

- `query_eval/`
	The research-grade evaluation subsystem for part 2 and onward. This is the
	current authoritative execution and reporting layer for multi-dataset query
	evaluation.
	- Start here for the modern workflow:
		- `query_eval/README.md` – detailed handoff guide for humans and LLMs
		- `query_eval/cli.py` – reproducible CLI entrypoints
		- `query_eval/queries.py` – note-compatible `(mode_chosen, dataset)` query API

- `part3_starter.ipynb`
	A lightweight analysis notebook for the part-3 teammate. It reads the
	generated `evaluation_results/query_eval/...` CSV outputs and produces
	first-pass tables and plots without re-implementing the execution pipeline.

- `main.ipynb`  
	The main experimentation notebook. It:
	- Locates a target log file in `dataset/loghub` (e.g., `Linux_2k.log`).
	- Invokes LogLite (via the C++ CLI in `loglite/LogLite-B`) to compress the log.
	- Copies compressed outputs into `compressed_logs/`.
	- Parses the **L-window** templates dumped by LogLite.
	- Runs a set of baseline text queries and their compressed-domain counterparts.

- `compressed_logs/`  
	Storage for artifacts produced by running the LogLite pipeline:
	- `*.lite` / `*.lite.b` – LogLite binary compressed files (bitstream written by `write_bitset_to_file`).
	- `*.window.txt` – Text dump of the L-window (for each line length, the list of recent templates).
	These files are **generated**, not hand-edited.

- `dataset/`  
	External datasets used for experiments.
	- `dataset/loghub/` – Clone or copy of the [Loghub](https://github.com/logpai/loghub-dataset) benchmark logs.  
		Typical usage in this project:
		- `dataset/loghub/Linux/Linux_2k.log` – Small Linux log sample used for correctness-focused experiments.
		- (Larger logs can also be used once the pipeline is validated.)

- `loglite/`  
	Vendor directory containing the original LogLite implementation and related baselines. This code is C++-centric and comes largely as-is from the LogLite authors, with very light modifications for portability and observability.

- `notes.txt`  
	Scratch notes and TODOs related to experiments, observations, and future ideas. Not required for running the pipeline.

Recommended Modern Workflow
---------------------------

If you are continuing the project after the original notebook prototype, prefer
the `query_eval/` subsystem rather than adding new execution logic to
`main.ipynb`.

- For detailed project and handoff documentation:
	- `query_eval/README.md`
- For reproducible suite execution:
	- `python3 -m query_eval.cli run-suite ...`
- For part-3 analysis and first-pass plotting:
	- `part3_starter.ipynb`

LogLite Subtree
----------------

Inside `loglite/`:

- `loglite/readme.md`  
	Original README for the LogLite project (authored by the LogLite maintainers). It describes their algorithms, command-line usage, and published results.

- `loglite/LogLite-B/`  
	Main C++ implementation of the LogLite **compressor/decompressor** used by this project.

	Key locations:
	- `loglite/LogLite-B/src/common/constants.h` – Global constants controlling the format:
		- `EACH_WINDOW_SIZE_COUNT`, `STREAM_ENCODER_COUNT`, `ORIGINAL_LENGTH_COUNT`, `MAX_LEN`, `RLE_COUNT`, etc.
	- `loglite/LogLite-B/src/compress/stream_compress.h` / `.cc` – Core streaming compression logic:
		- `Stream_Compress::stream_compress(...)` – Encodes each log line into a bitstream using an L-window of templates, XOR, and RLE.
		- `Stream_Compress::stream_decompress(...)` – Reconstructs original lines from the bitstream.
		- `get_window()` – Exposes the internal L-window (added in this project for analysis).
	- `loglite/LogLite-B/src/common/file.cc` – Binary I/O helpers:
		- `write_bitset_to_file` / `read_bitset_from_file` – Define the on-disk layout of `.lite`/`.lite.b` compressed files.
	- `loglite/LogLite-B/src/tools/xorc-cli.cc` – Command-line tool used by the Python notebook:
		- `--compress` / `--decompress` / `--test` – Modes for streaming compression and decompression.
		- `--file-path` – Input raw log file.
		- `--com-output-path` – Destination for the compressed binary file.
		- `--decom-output-path` – Destination for decompressed text.
		- `--window-output-path` – (Project extension) emits the L-window snapshot used by the notebook.

- `loglite/baselines/`  
	Third-party or reference baseline compressors and tools used in the original LogLite evaluation. These are *not* central to the compressed-query work, but are included for completeness:
	- `loglite/baselines/fsst/` – FSST string compressor.
	- `loglite/baselines/loggrep-L/` and `loglite/baselines/loggrep-Z/` – LogGrep-related code and experiments.
	- `loglite/baselines/logreducer/` – LogReducer implementation.
	- `loglite/baselines/logshrink/` – LogShrink implementation.
	- `loglite/baselines/lzbench/` – LZBench framework with many classic compressors.
	- `loglite/baselines/pbc/` – PBC compression baseline.
	The Python integration in `main.ipynb` primarily uses **LogLite-B**; baselines are there to support extended experiments if desired.

- `loglite/appendix/`  
	Supplemental material shipped with the original LogLite project (plots, tables, extra scripts, etc.). Not directly used by the notebook pipeline.

- `loglite/scripts/`  
	Helper Python scripts and the build/benchmark harness for LogLite and baselines.

	Important scripts:
	- `loglite/scripts/compile.py` – Builds C++ binaries (LogLite-B and baselines). This script has been lightly modified to be more portable (e.g., using `g++` with AVX2 instead of AVX-512-only flags).
	- `loglite/scripts/loglite.py` – Convenience wrapper that:
		- Accepts a dataset name (e.g., `Linux_2k.log`).
		- Invokes the `xorc-cli` compressor with appropriate paths.
		- Manages output directories (`com_output/`, `decom_output/`, etc.).
	- `loglite/scripts/*.py` – Additional wrappers for other baselines (FSST, LogReducer, LogShrink, LZBench, PBC).

	Directories under `loglite/scripts/`:
	- `loglite/scripts/com_output/` – Default location where compressed outputs (e.g., `.lite` files) are written by the scripts.
	- `loglite/scripts/decom_output/` – Decompressed text outputs (used mainly for sanity-checking correctness).
	- `loglite/scripts/datasets/` – Working copy of input logs; the notebook typically copies logs here from `dataset/loghub/` before compression.
	- `loglite/scripts/results/` – Benchmark summaries and metrics produced by running the original LogLite experiments.

How the Pieces Fit Together
---------------------------

1. **Prepare dataset**  
	 Place or verify `dataset/loghub/...` (e.g., `dataset/loghub/Linux/Linux_2k.log`).

2. **Build LogLite-B**  
	 From `loglite/scripts/` (typically under Linux/WSL), run:

	 ```bash
	 python compile.py
	 ```

	 This compiles `loglite/LogLite-B/src/tools/xorc-cli` and other baselines.

3. **Run compression from the notebook**  
	 In `main.ipynb`:
	 - Copy the chosen log from `dataset/loghub/...` into `loglite/scripts/datasets/` (automated by the notebook).
	 - Invoke `loglite/scripts/loglite.py` for that dataset.
	 - The script runs `xorc-cli` and writes compressed files into `loglite/scripts/com_output/`.
	 - The notebook then copies those files into `compressed_logs/` for easier access from Python.

4. **Inspect templates (L-window)**  
	 - When `xorc-cli` is run with `--window-output-path`, it writes a `*.window.txt` file capturing the internal L-window (per-length template lists).
	 - `main.ipynb` parses this file into a Python dictionary: `{length: [templates...]}`.

5. **Run queries**  
	 - Baseline (uncompressed) queries: run directly over the raw `.log` file using pure Python string search, storing counts and sample lines.
	 - Compressed-domain queries: operate on the binary `.lite`/`.lite.b` file and/or the window templates to evaluate how much work can be done without fully materializing all logs.
