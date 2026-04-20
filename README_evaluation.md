## What This Branch Contains

This branch contains the implementation and evaluation of query-aware decompression
built on top of the LogLite-B framework. The original LogLite-B source has been
ported to Apple Silicon (ARM64) and extended with three progressive query modes.
All three stages are implemented in the final state of the source files — intermediate
per-stage commits were not preserved during development.

---

## What Was Built

### Stage 1 — Length Filter
Skip entire length buckets during decompression. Lines whose length does not match
the target are not reconstructed at all — zero XOR work is done for them. Window
state is still maintained for correctness.

### Stage 2 — Keyword Filter
During compression, a consensus template map is built per length bucket by tracking
which character positions are static across all logs of that length. At query time,
the keyword is matched against the template map before touching the compressed data.
Only length buckets whose template is consistent with the keyword are decompressed.
A post-filter guarantees zero false positives by doing a substring check on the
fully reconstructed line before emission.
### Stage 3 — Field Extraction (Sniper Mode)
Instead of reconstructing the full line, only the character positions within the
requested field ranges are null-filled from the pattern. All other positions are
skipped. Multiple fields can be requested in one query and are emitted
tab-separated. Combined with Stage 2, this avoids 67–89% of XOR reconstruction
work across all tested datasets.

---

## Changes to LogLite-B Source

All modifications are inside `LogLite-B/src/`. The original LogLite logic is
preserved — we only add instrumentation and new decompression paths.

| File | What Changed |
|---|---|
| `common/constants.h` | Removed Linux-only `#include <c++/10/new>` for macOS compatibility |
| `common/xor_string.h/cc` | Replaced Intel AVX-512/AVX2 SIMD with portable scalar code for ARM64 |
| `common/rle.h/cc` | Removed `immintrin.h` (x86-only), kept RLE logic intact |
| `compress/stream_compress.h` | Added `FieldRange` struct, consensus template map, timing globals, new method signatures |
| `compress/stream_compress.cc` | Added per-component chrono timers, consensus template builder, `stream_decompress_skip()`, `stream_decompress_fields()`, `save/load_template_map()`, `lengths_matching_keyword()` |
| `tools/xorc-cli.cc` | Added `--keyword`, `--filter-length`, `--field` CLI flags and routing logic |

---## New File: benchmark.py

Located at repo root. Runs all three stages in one command, verifies correctness
against ground truth, and prints a side-by-side comparison table.
---

## How to Build

Requires: clang++, Boost (`brew install boost`), macOS with Xcode CLI tools.

```bash
cd LogLite-B

clang++ -O3 -std=c++17 \
  -isysroot $(xcrun --show-sdk-path) \
  -I $(xcrun --show-sdk-path)/usr/include/c++/v1 \
  ./src/compress/*.cc \
  ./src/common/*.cc \
  ./src/tools/*.cc \
  -I ./src \
  -I $(brew --prefix boost)/include \
  -L $(brew --prefix boost)/lib \
  -o ./src/tools/xorc-cli
```

---

## How to Run

### Step 1 — Compress (generates the .tmap sidecar file)
```bash
./LogLite-B/src/tools/xorc-cli --compress \
  --file-path <input.log> \
  --com-output-path <output.lite> \
  --decom-output-path <output.decom>
```

### Step 2 — Run benchmark (all three stages)
```bash
python3 benchmark.py \
  --log scripts/datasets/Apache.log \
  --keyword "error" \
  --field 0 26 --field 27 35
```

### Stage 1 only
```bash
python3 benchmark.py \
  --log scripts/datasets/Apache.log \
  --filter-length 91
```

### Stage 2 only
```bash
python3 benchmark.py \
  --log scripts/datasets/Apache.log \
  --keyword "error"
```

---

## Datasets

Apache.log is already in `scripts/datasets/`. Download the others:

```bash
cd scripts/datasets
curl -L -o Linux.log "https://raw.githubusercontent.com/logpai/loghub/master/Linux/Linux_2k.log"
curl -L -o Spark.log "https://raw.githubusercontent.com/logpai/loghub/master/Spark/Spark_2k.log"
curl -L -o HDFS.log  "https://raw.githubusercontent.com/logpai/loghub/master/HDFS/HDFS_2k.log"
```

---

## Results Summary

All correctness checks pass (zero false positives, zero false negatives) on all
four datasets.

| Dataset | Comp Ratio | XOR Work Avoided (S3) | Total Decomp Saved (S3) |
|---|---|---|---|
| Apache | 0.0906 | 67.6% | 36.2% |
| Linux | 0.1273 | 83.5% | 46.5% |
| Spark | 0.1129 | 84.7% | 45.5% |
| HDFS | 0.3746 | 89.1% | 35.4% |

---

## Known Limitations

- ARM64 port removes Intel SIMD — scalar replacements are correct but slower than
  the original on x86. Run on Linux x86 for paper-quality speed numbers.
- The 2k Loghub samples are small. For full-scale evaluation download the complete
  datasets from Zenodo (see original `readme.md`).
- RLE decode is a fixed cost regardless of how many positions are skipped — this
  is the current performance floor and a direction for future optimization.
