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


def run_command_lzbench(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)

        # print(result.stdout)
        
        # Parse the output
        metrics = parse_xorc_output_lzbench(result.stdout)
        
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


def parse_xorc_output_lzbench(output):
    
    lines = output.strip().split('\n')
    zstd_line = lines[2]  
    
    
    parts = [p.strip() for p in zstd_line.split(',')]
    
    # 提取并转换所需指标
    metrics = {
        'compression_rate': float(parts[5])/100,  
        'compression_speed': float(parts[1]),      
        'decompression_speed': float(parts[2])
    }

    return metrics





lzbench_dir = "../baselines/lzbench"

input_file_path=f"./datasets/{dataset}"


command = f"{lzbench_dir}/lzbench -elz4 -o4 {input_file_path}"
print(f"********************LZ4***********************")
run_command_lzbench(command, f"Using **LZ4** compress and decompress **{dataset}**")
print(f"****************************************************")


command = f"{lzbench_dir}/lzbench -ezstd,3 -o4 {input_file_path}"
print(f"********************Zstd***********************")
run_command_lzbench(command, f"Using **Zstd** compress and decompress **{dataset}**")
print(f"****************************************************")


command = f"{lzbench_dir}/lzbench -elzma,6 -o4 {input_file_path}"
print(f"********************LZMA***********************")
run_command_lzbench(command, f"Using **LZMA** compress and decompress **{dataset}**")
print(f"****************************************************")