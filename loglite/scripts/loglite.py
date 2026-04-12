"""Top-level driver script for running LogLite-B experiments.

This script:
    * Runs LogLite-b and LogLite-B on a given dataset using their xorc-cli
        front-ends in "--test" mode (compress + decompress in one run).
    * Parses the reported compression rate / speed / decompression speed.
    * Optionally chains LogLite-B output through lzbench with Zstd or LZMA
        to get combined "LogLite-BZ" / "LogLite-BL" baselines.
    * Appends all metrics to ./results/<dataset>.txt so they can be
        compared across runs and other baselines.

Usage:
        python loglite.py <dataset_name>

Assumes datasets/<dataset_name> exists and that the C++ binaries and
lzbench have already been built.
"""

import os
import subprocess
import re
import sys

# Name of the log file to benchmark, e.g. "Apache.log".
dataset = sys.argv[1]

# Metrics from the pure LogLite-B run are cached here so we can
# combine them with downstream lzbench results.
logliteB_metrics = {}


def save_metrics_to_file(metrics, description, filename=f"./results/{dataset}.txt"):
    try:
        with open(filename, 'a') as f:
            f.write(description)
            f.write("\n")
            for k, v in metrics.items():
                f.write(f"{k}: {v}\n")
            f.write("\n")
        print(f"Metrics saved to {filename}")
    except IOError as e:
        print(f"Error writing to file: {e}")


def run_command(command, description=None, cwd=None, is_LogliteB=False):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output from the C++ tool.
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the stdout printed by xorc-cli into numeric metrics.
        metrics = parse_xorc_output(result.stdout)
        
        if all(v is not None for v in metrics.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics.items():
                print(f"{k}: {v}")
                if is_LogliteB:
                    # Cache metrics from the pure LogLite-B run so we
                    # can compute combined numbers for LogLite-BZ/BL.
                    logliteB_metrics[k] = v

            # Save to file
            save_metrics_to_file(metrics,description)
            return metrics
        else:
            print("Warning: Failed to extract some metrics")
            return metrics
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output(output):
    metrics = {
        'compression_rate': None,
        'compression_speed': None,
        'decompression_speed': None
    }
    
    # Regular expressions to match the metrics
    patterns = {
        'compression_rate': r'compression rate:([0-9.]+)',
        'compression_speed': r'compression speed: ([0-9.]+)MB/s',
        'decompression_speed': r'decompression speed: ([0-9.]+)MB/s'
    }
    
    for metric, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            try:
                metrics[metric] = float(match.group(1))
            except (ValueError, IndexError):
                continue
    
    return metrics


def run_command_lzbench(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run lzbench and capture its tabular output.
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)

        # print(result.stdout)
        
        # Parse lzbench output (we only look at the third line, which
        # corresponds to the configured codec) and extract rate/speeds.
        metrics = parse_xorc_output_lzbench(result.stdout)

        # Combine LogLite-B and lzbench metrics to get an end-to-end
        # picture for two-stage pipelines like LogLite-BZ / LogLite-BL.
        metrics_combined = {}
        metrics_combined['compression_rate'] = logliteB_metrics['compression_rate'] * metrics['compression_rate']
        metrics_combined['compression_speed'] = logliteB_metrics['compression_speed'] * metrics['compression_speed'] / (logliteB_metrics['compression_speed'] * logliteB_metrics['compression_rate'] + metrics['compression_speed'])
        metrics_combined['decompression_speed'] = logliteB_metrics['decompression_speed'] * metrics['decompression_speed'] / (logliteB_metrics['decompression_speed'] * logliteB_metrics['compression_rate'] + metrics['decompression_speed'])
        
        if all(v is not None for v in metrics_combined.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics_combined.items():
                print(f"{k}: {v}")

            # Save to file
            save_metrics_to_file(metrics_combined,description)
            return metrics_combined
        else:
            print("Warning: Failed to extract some metrics")
            return metrics
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_lzbench(output):
    
    lines = output.strip().split('\n')
    zstd_line = lines[2]  
    
    
    parts = [p.strip() for p in zstd_line.split(',')]
    
    # Extract the three metrics we care about from the CSV-like row.
    metrics = {
        'compression_rate': float(parts[5])/100,  
        'compression_speed': float(parts[1]),      
        'decompression_speed': float(parts[2])
    }

    return metrics


loglite_dir_b = "../LogLite-b"
loglite_dir_B = "../LogLite-B"

input_file_path = f"./datasets/{dataset}"
compressed_output_file_path = f"./com_output/{dataset}.lite"
decompressed_output_file_path = f"./decom_output/{dataset}.lite.decom"

# loglite-b
command = f"{loglite_dir_b}/src/tools/xorc-cli --test --file-path {input_file_path} --com-output-path {compressed_output_file_path}.b --decom-output-path {decompressed_output_file_path}.b"
print(f"********************LogLite-b***********************")
run_command(command, f"Using **LogLite-b** compress and decompress **{dataset}**", None, False)
print(f"****************************************************")

# loglite-B
command = f"{loglite_dir_B}/src/tools/xorc-cli --test --file-path {input_file_path} --com-output-path {compressed_output_file_path}.B --decom-output-path {decompressed_output_file_path}.B"
print(f"********************LogLite-B***********************")
run_command(command, f"Using **LogLite-B** compress and decompress **{dataset}**", None, True)
print(f"****************************************************")



lzbench_dir = "../baselines/lzbench"
#loglite-BZ
command = f"{lzbench_dir}/lzbench -ezstd,3 -o4 {compressed_output_file_path}.B"
print(f"********************LogLite-BZ***********************")
run_command_lzbench(command, f"Using **LogLite-BZ** compress and decompress **{dataset}**")
print(f"****************************************************")

#loglite-BL
command = f"{lzbench_dir}/lzbench -elzma,6 -o4 {compressed_output_file_path}.B"
print(f"********************LogLite-BL***********************")
run_command_lzbench(command, f"Using **LogLite-BL** compress and decompress **{dataset}**")
print(f"****************************************************")