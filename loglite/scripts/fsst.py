import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]


def save_metrics_to_file(metrics, description ,filename=f"./results/{dataset}.txt"):
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


def run_command(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the output
        metrics = parse_xorc_output_fsst(result.stdout)
        
        if all(v is not None for v in metrics.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics.items():
                print(f"{k}: {v}")

            # Save to file
            save_metrics_to_file(metrics,description)
            return metrics
        else:
            print("Warning: Failed to extract some metrics")
            return metrics
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_fsst(output):
    metrics = {
        'compression_rate': None,
        'compression_speed': None,
        'decompression_speed': None
    }
    
    # Regular expressions to match the metrics
    patterns = {
        'compression_rate': r'compression ratio: ([0-9.]+)',
        'compression_speed': r'compression speed in MB/s: ([0-9.]+)',
        'decompression_speed': r'decompression speed in MB/s: ([0-9.]+)'
    }
    
    for metric, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            try:
                metrics[metric] = float(match.group(1))
            except (ValueError, IndexError):
                continue
    
    return metrics


fsst_dir = "../baselines/fsst/paper"

input_file_path=f"./datasets/{dataset}"


command = f"{fsst_dir}/linetest fsst {input_file_path}"
print(f"********************FSST***********************")
run_command(command, f"Using **FSST** compress and decompress **{dataset}**")
print(f"****************************************************")

command = f"{fsst_dir}/linetest lz4dict {input_file_path}"
print(f"********************LZ4-d***********************")
run_command(command, f"Using **LZ4-d** compress and decompress **{dataset}**")
print(f"****************************************************")

command = f"{fsst_dir}/linetest-zstd zstddict {input_file_path}"
print(f"********************Zstd-d***********************")
run_command(command, f"Using **Zstd-d** compress and decompress **{dataset}**")
print(f"****************************************************")