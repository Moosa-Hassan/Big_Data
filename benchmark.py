#!/usr/bin/env python3
"""
LogLite Benchmark Interface — Stages 1, 2, and 3
Compares full decompression vs skip-mode vs field extraction performance.

Usage:
    # Stage 1: filter by length
    python3 benchmark.py --log <log_file> --filter-length <length>

    # Stage 2: filter by keyword
    python3 benchmark.py --log <log_file> --keyword <keyword>

    # Stage 3: extract fields from matching lines
    python3 benchmark.py --log <log_file> --keyword <keyword> --field 0 26 --field 27 35

    # Combined
    python3 benchmark.py --log <log_file> --keyword <keyword> --filter-length <length> --field 0 26
"""

import subprocess
import os
import sys
import argparse
import re

BINARY     = "/Users/mac/LogLite/LogLite-B/src/tools/xorc-cli"
OUTPUT_DIR = "/Users/mac/LogLite/test_output"

os.makedirs(OUTPUT_DIR, exist_ok=True)


def run_command(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr


def parse_timing(output):
    metrics = {}

    simple = {
        "decomp_speed_mbs"  : r"decompression speed:\s+([\d.]+)MB/s",
        "comp_speed_mbs"    : r"compression speed:\s+([\d.]+)MB/s",
        "comp_ratio"        : r"compression rate:([\d.]+)",
        "lines_emitted"     : r"Lines emitted\s+:\s+(\d+)",
        "lines_skipped"     : r"Lines skipped \(bucket\)\s+:\s+(\d+)",
        "lines_dropped"     : r"Lines dropped \(post\)\s+:\s+(\d+)",
        "buckets_matched"   : r"matches (\d+) length bucket",
        "positions_total"   : r"Positions total\s+:\s+(\d+)",
        "positions_filled"  : r"Positions filled\s+:\s+(\d+)",
        "work_avoided_pct"  : r"Work avoided\s+:\s+([\d.]+)%",
    }
    for key, pattern in simple.items():
        match = re.search(pattern, output)
        if match:
            metrics[key] = float(match.group(1))

    decomp_section = re.search(
        r"-- Decompression --(.*?)=====",
        output, re.DOTALL
    )
    if decomp_section:
        section = decomp_section.group(1)
        for key, pattern in [
            ("rle_decode_ms",      r"RLE decode\s+:\s+([\d.]+) ms"),
            ("xor_reconstruct_ms", r"XOR reconstruct\s+:\s+([\d.]+) ms"),
            ("window_update_ms",   r"Window update\s+:\s+([\d.]+) ms"),
            ("decomp_total_ms",    r"Total\s+:\s+([\d.]+) ms"),
        ]:
            match = re.search(pattern, section)
            if match:
                metrics[key] = float(match.group(1))

    return metrics


def get_top_lengths(log_file, n=10):
    lengths = {}
    with open(log_file, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            l = len(line)
            lengths[l] = lengths.get(l, 0) + 1
    return sorted(lengths.items(), key=lambda x: x[1], reverse=True)[:n]


def step_compress(log_file, com_path):
    print(f"  Compressing {os.path.basename(log_file)} ...")
    cmd = [BINARY, "--compress",
           "--file-path", log_file,
           "--com-output-path", com_path,
           "--decom-output-path", com_path + ".tmp"]
    output = run_command(cmd)
    return parse_timing(output)


def step_decompress_full(com_path, decom_path):
    print(f"  Running full decompression ...")
    cmd = [BINARY, "--decompress",
           "--file-path", com_path,
           "--decom-output-path", decom_path]
    output = run_command(cmd)
    return parse_timing(output), output


def step_decompress_skip(com_path, decom_path, keyword=None, filter_length=None):
    mode_str = []
    if keyword:       mode_str.append(f"keyword={keyword}")
    if filter_length: mode_str.append(f"length={filter_length}")
    print(f"  Running skip-mode decompression ({', '.join(mode_str)}) ...")
    cmd = [BINARY, "--decompress",
           "--file-path", com_path,
           "--decom-output-path", decom_path]
    if keyword:       cmd += ["--keyword", keyword]
    if filter_length: cmd += ["--filter-length", str(filter_length)]
    output = run_command(cmd)
    return parse_timing(output), output


def step_decompress_fields(com_path, decom_path, fields,
                           keyword=None, filter_length=None):
    mode_str = []
    if keyword:       mode_str.append(f"keyword={keyword}")
    if filter_length: mode_str.append(f"length={filter_length}")
    mode_str.append(f"{len(fields)} field(s)")
    print(f"  Running field extraction ({', '.join(mode_str)}) ...")
    cmd = [BINARY, "--decompress",
           "--file-path", com_path,
           "--decom-output-path", decom_path]
    if keyword:       cmd += ["--keyword", keyword]
    if filter_length: cmd += ["--filter-length", str(filter_length)]
    for s, e in fields:
        cmd += ["--field", str(s), str(e)]
    output = run_command(cmd)
    return parse_timing(output), output


def verify_correctness_skip(log_file, result_file, keyword=None, filter_length=None):
    """Verify Stage 1/2 output matches expected lines from original."""
    expected = []
    with open(log_file, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            length_ok  = (filter_length is None or len(line) == filter_length)
            keyword_ok = (keyword is None or keyword in line)
            if length_ok and keyword_ok:
                expected.append(line)

    got = []
    with open(result_file, "r", errors="replace") as f:
        for line in f:
            got.append(line.rstrip("\n").rstrip("\r"))
    while got and got[-1] == "":
        got.pop()

    return expected == got, len(expected), len(got)


def verify_correctness_fields(log_file, result_file, fields,
                               keyword=None, filter_length=None):
    """Verify Stage 3 output — check field extraction is correct."""
    expected = []
    with open(log_file, "r", errors="replace") as f:
        for line in f:
            line = line.rstrip("\n").rstrip("\r")
            length_ok  = (filter_length is None or len(line) == filter_length)
            keyword_ok = (keyword is None or keyword in line)
            if length_ok and keyword_ok:
                parts = []
                for s, e in fields:
                    parts.append(line[s:e])
                expected.append("\t".join(parts))

    got = []
    with open(result_file, "r", errors="replace") as f:
        for line in f:
            got.append(line.rstrip("\n").rstrip("\r"))
    while got and got[-1] == "":
        got.pop()

    return expected == got, len(expected), len(got)


def print_report(log_file, keyword, filter_length, fields,
                 comp_metrics, full_metrics, skip_metrics, field_metrics=None):

    orig_size = os.path.getsize(log_file)

    xor_full  = full_metrics.get("xor_reconstruct_ms", 0)
    xor_skip  = skip_metrics.get("xor_reconstruct_ms", 0) if skip_metrics else 0
    xor_field = field_metrics.get("xor_reconstruct_ms", 0) if field_metrics else 0

    tot_full  = full_metrics.get("decomp_total_ms", 0)
    tot_skip  = skip_metrics.get("decomp_total_ms", 0) if skip_metrics else 0
    tot_field = field_metrics.get("decomp_total_ms", 0) if field_metrics else 0

    xor_skip_saved  = (1 - xor_skip  / xor_full) * 100 if xor_full else 0
    xor_field_saved = (1 - xor_field / xor_full) * 100 if xor_full else 0
    tot_skip_saved  = (1 - tot_skip  / tot_full) * 100 if tot_full else 0
    tot_field_saved = (1 - tot_field / tot_full) * 100 if tot_full else 0

    emitted_s = int(skip_metrics.get("lines_emitted", 0))  if skip_metrics  else 0
    skipped_s = int(skip_metrics.get("lines_skipped", 0))  if skip_metrics  else 0
    dropped_s = int(skip_metrics.get("lines_dropped", 0))  if skip_metrics  else 0

    emitted_f = int(field_metrics.get("lines_emitted", 0)) if field_metrics else 0
    skipped_f = int(field_metrics.get("lines_skipped", 0)) if field_metrics else 0
    dropped_f = int(field_metrics.get("lines_dropped", 0)) if field_metrics else 0

    work_avoided = field_metrics.get("work_avoided_pct", 0) if field_metrics else 0
    total = emitted_s + skipped_s + dropped_s or emitted_f + skipped_f + dropped_f

    query_str = []
    if keyword:       query_str.append(f'keyword="{keyword}"')
    if filter_length: query_str.append(f"length={filter_length}")
    if fields:        query_str.append(f"fields={fields}")

    W = 68
    print("\n" + "=" * W)
    print("               LOGLITE BENCHMARK REPORT")
    print("=" * W)
    print(f"  Log file        : {os.path.basename(log_file)}")
    print(f"  Original size   : {orig_size / 1024:.1f} KB")
    print(f"  Query           : {', '.join(query_str)}")
    print(f"  Comp ratio      : {comp_metrics.get('comp_ratio', 0):.4f}")
    print(f"  Comp speed      : {comp_metrics.get('comp_speed_mbs', 0):.1f} MB/s")
    print("-" * W)

    # Timing table
    has_field = field_metrics is not None
    if has_field:
        print(f"  {'Metric':<30} {'Full':>8} {'Stage2':>8} {'Stage3':>8} {'S2 Save':>8} {'S3 Save':>8}")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8}")
        print(f"  {'XOR reconstruct (ms)':<30} {xor_full:>8.3f} {xor_skip:>8.3f} {xor_field:>8.3f} {xor_skip_saved:>7.1f}% {xor_field_saved:>7.1f}%")
        print(f"  {'RLE decode (ms)':<30} {full_metrics.get('rle_decode_ms',0):>8.3f} {skip_metrics.get('rle_decode_ms',0):>8.3f} {field_metrics.get('rle_decode_ms',0):>8.3f}")
        print(f"  {'Window update (ms)':<30} {full_metrics.get('window_update_ms',0):>8.3f} {skip_metrics.get('window_update_ms',0):>8.3f} {field_metrics.get('window_update_ms',0):>8.3f}")
        print(f"  {'Total decomp (ms)':<30} {tot_full:>8.3f} {tot_skip:>8.3f} {tot_field:>8.3f} {tot_skip_saved:>7.1f}% {tot_field_saved:>7.1f}%")
    else:
        print(f"  {'Metric':<30} {'Full':>8} {'Skip':>8} {'Saved':>8}")
        print(f"  {'-'*30} {'-'*8} {'-'*8} {'-'*8}")
        print(f"  {'XOR reconstruct (ms)':<30} {xor_full:>8.3f} {xor_skip:>8.3f} {xor_skip_saved:>7.1f}%")
        print(f"  {'RLE decode (ms)':<30} {full_metrics.get('rle_decode_ms',0):>8.3f} {skip_metrics.get('rle_decode_ms',0):>8.3f}")
        print(f"  {'Window update (ms)':<30} {full_metrics.get('window_update_ms',0):>8.3f} {skip_metrics.get('window_update_ms',0):>8.3f}")
        print(f"  {'Total decomp (ms)':<30} {tot_full:>8.3f} {tot_skip:>8.3f} {tot_skip_saved:>7.1f}%")

    print("-" * W)

    if has_field:
        print(f"  Stage 3 positions filled : {field_metrics.get('positions_filled',0):.0f} / "
              f"{field_metrics.get('positions_total',0):.0f} "
              f"({100 - work_avoided:.1f}%)")
        print(f"  Stage 3 work avoided     : {work_avoided:.1f}%")
        print("-" * W)
        m = field_metrics
        t = emitted_f + skipped_f + dropped_f
        if t:
            print(f"  Lines total              : {t}")
            print(f"  Lines emitted            : {emitted_f:>6}  ({100*emitted_f/t:.1f}%)")
            print(f"  Skipped (bucket)         : {skipped_f:>6}  ({100*skipped_f/t:.1f}% — zero decomp work)")
            print(f"  Dropped (post-filter)    : {dropped_f:>6}  ({100*dropped_f/t:.1f}% — decompressed, not emitted)")
    else:
        t = emitted_s + skipped_s + dropped_s
        if t:
            print(f"  Lines total              : {t}")
            print(f"  Lines emitted            : {emitted_s:>6}  ({100*emitted_s/t:.1f}%)")
            print(f"  Skipped (bucket)         : {skipped_s:>6}  ({100*skipped_s/t:.1f}% — zero decomp work)")
            print(f"  Dropped (post-filter)    : {dropped_s:>6}  ({100*dropped_s/t:.1f}% — decompressed, not emitted)")

    print("=" * W + "\n")


def main():
    parser = argparse.ArgumentParser(description="LogLite Benchmark Interface")
    parser.add_argument("--log",           required=True)
    parser.add_argument("--keyword",       default=None)
    parser.add_argument("--filter-length", default=None, type=int)
    parser.add_argument("--field",         nargs=2, type=int, action="append",
                        metavar=("START", "END"),
                        help="Field range [start:end) — can be repeated")
    parser.add_argument("--top-lengths",   action="store_true")
    parser.add_argument("--skip-compress", action="store_true",
                        help="Skip compression if .lite already exists")
    args = parser.parse_args()

    if not args.keyword and not args.filter_length and not args.field:
        print("Error: provide at least --keyword, --filter-length, or --field")
        sys.exit(1)

    log_file      = args.log
    keyword       = args.keyword
    filter_length = args.filter_length
    fields        = args.field or []

    if not os.path.exists(log_file):
        print(f"Error: log file not found: {log_file}")
        sys.exit(1)
    if not os.path.exists(BINARY):
        print(f"Error: binary not found: {BINARY}")
        sys.exit(1)

    if args.top_lengths:
        print("\nTop 10 most common line lengths:")
        print(f"  {'Length':>8}  {'Count':>8}")
        print(f"  {'-'*8}  {'-'*8}")
        for length, count in get_top_lengths(log_file):
            marker = " <-- selected" if length == filter_length else ""
            print(f"  {length:>8}  {count:>8}{marker}")
        print()

    base            = os.path.basename(log_file)
    com_path        = os.path.join(OUTPUT_DIR, base + ".lite")
    full_decom_path = os.path.join(OUTPUT_DIR, base + ".full.decom")
    skip_decom_path = os.path.join(OUTPUT_DIR, base + ".skip.decom")
    field_decom_path= os.path.join(OUTPUT_DIR, base + ".fields.decom")

    if args.skip_compress and os.path.exists(com_path):
        print(f"\n[1/4] Compression (skipped — using existing {base}.lite)")
        comp_metrics = {}
    else:
        print(f"\n[1/4] Compression")
        comp_metrics = step_compress(log_file, com_path)

    print(f"\n[2/4] Full Decompression (baseline)")
    full_metrics, _ = step_decompress_full(com_path, full_decom_path)

    print(f"\n[3/4] Stage 2 — Skip-mode Decompression")
    skip_metrics, _ = step_decompress_skip(com_path, skip_decom_path,
                                            keyword=keyword,
                                            filter_length=filter_length)

    field_metrics = None
    if fields:
        print(f"\n[4/4] Stage 3 — Field Extraction")
        field_metrics, _ = step_decompress_fields(com_path, field_decom_path,
                                                   fields,
                                                   keyword=keyword,
                                                   filter_length=filter_length)
    else:
        print(f"\n[4/4] Stage 3 — skipped (no --field arguments)")

    print(f"\n[Verifying correctness...]")
    if fields:
        correct, n_exp, n_got = verify_correctness_fields(
            log_file, field_decom_path, fields,
            keyword=keyword, filter_length=filter_length)
    else:
        correct, n_exp, n_got = verify_correctness_skip(
            log_file, skip_decom_path,
            keyword=keyword, filter_length=filter_length)

    if correct:
        print(f"  PASS — {n_exp} lines match exactly.")
    else:
        print(f"  FAIL — expected {n_exp} lines, got {n_got}.")

    print_report(log_file, keyword, filter_length, fields,
                 comp_metrics, full_metrics, skip_metrics, field_metrics)


if __name__ == "__main__":
    main()
