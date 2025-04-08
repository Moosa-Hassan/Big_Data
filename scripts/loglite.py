import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]

logliteB_metrics={}


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


def run_command(command,  description=None, cwd=None, is_LogliteB=False):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the output
        metrics = parse_xorc_output(result.stdout)
        
        if all(v is not None for v in metrics.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics.items():
                print(f"{k}: {v}")
                if is_LogliteB:
                    logliteB_metrics[k]=v

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
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)

        # print(result.stdout)
        
        # Parse the output
        metrics = parse_xorc_output_lzbench(result.stdout)

        metrics_combined = {}
        metrics_combined['compression_rate'] =  logliteB_metrics['compression_rate'] * metrics['compression_rate']
        metrics_combined['compression_speed'] = logliteB_metrics['compression_speed'] * metrics['compression_speed'] / (logliteB_metrics['compression_speed']*logliteB_metrics['compression_rate']+metrics['compression_speed'])
        metrics_combined['decompression_speed'] = logliteB_metrics['decompression_speed'] * metrics['decompression_speed'] / (logliteB_metrics['decompression_speed']*logliteB_metrics['compression_rate']+metrics['decompression_speed'])
        
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
    
    # 提取并转换所需指标
    metrics = {
        'compression_rate': float(parts[5])/100,  
        'compression_speed': float(parts[1]),      
        'decompression_speed': float(parts[2])
    }

    return metrics


loglite_dir_b = "../LogLite-b"
loglite_dir_B = "../LogLite-B"

input_file_path=f"./datasets/{dataset}"
compressed_output_file_path=f"./com_output/{dataset}.lite"
decompressed_output_file_path=f"./decom_output/{dataset}.lite.decom"

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