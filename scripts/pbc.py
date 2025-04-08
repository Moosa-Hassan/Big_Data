import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]

pbc_metrics={}


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

        # print(result.stdout)
        
        # Parse the output
        metrics = parse_xorc_output_pbc(result.stdout)
        
        if all(v is not None for v in metrics.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics.items():
                print(f"{k}: {v}")
                pbc_metrics[k]=v

            # Save to file
            save_metrics_to_file(metrics,description)
            return metrics
        else:
            print("Warning: Failed to extract some metrics")
            return metrics
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_pbc(output):
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


def run_command_fsst(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the output
        metrics = parse_xorc_output_fsst(result.stdout)

        metrics_combined = {}
        metrics_combined['compression_rate'] =  pbc_metrics['compression_rate'] * metrics['compression_rate']
        metrics_combined['compression_speed'] = pbc_metrics['compression_speed'] * metrics['compression_speed'] / (pbc_metrics['compression_speed']*pbc_metrics['compression_rate']+metrics['compression_speed'])
        metrics_combined['decompression_speed'] = pbc_metrics['decompression_speed'] * metrics['decompression_speed'] / (pbc_metrics['decompression_speed']*pbc_metrics['compression_rate']+metrics['decompression_speed'])
        
        if all(v is not None for v in metrics_combined.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics_combined.items():
                print(f"{k}: {v}")

            # Save to file
            save_metrics_to_file(metrics_combined,description)
            return metrics_combined
        else:
            print("Warning: Failed to extract some metrics")
            return metrics_combined
            
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


def run_command_lzbench(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the output
        metrics = parse_xorc_output_lzbench(result.stdout)

        metrics_combined = {}
        metrics_combined['compression_rate'] =  pbc_metrics['compression_rate'] * metrics['compression_rate']
        metrics_combined['compression_speed'] = pbc_metrics['compression_speed'] * metrics['compression_speed'] / (pbc_metrics['compression_speed']*pbc_metrics['compression_rate']+metrics['compression_speed'])
        metrics_combined['decompression_speed'] = pbc_metrics['decompression_speed'] * metrics['decompression_speed'] / (pbc_metrics['decompression_speed']*pbc_metrics['compression_rate']+metrics['decompression_speed'])
        
        if all(v is not None for v in metrics_combined.values()):
            # print("Successfully extracted metrics:")
            for k, v in metrics_combined.items():
                print(f"{k}: {v}")

            # Save to file
            save_metrics_to_file(metrics_combined,description)
            return metrics_combined
        else:
            print("Warning: Failed to extract some metrics")
            return metrics_combined
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_lzbench(output):
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
        metrics_combined['compression_rate'] =  pbc_metrics['compression_rate'] * metrics['compression_rate']
        metrics_combined['compression_speed'] = pbc_metrics['compression_speed'] * metrics['compression_speed'] / (pbc_metrics['compression_speed']*pbc_metrics['compression_rate']+metrics['compression_speed'])
        metrics_combined['decompression_speed'] = pbc_metrics['decompression_speed'] * metrics['decompression_speed'] / (pbc_metrics['decompression_speed']*pbc_metrics['compression_rate']+metrics['decompression_speed'])
        
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




pbc_dir = "../baselines/pbc"

input_file_path=f"./datasets/{dataset}"
compressed_output_file_path=f"./com_output/{dataset}.pbc"
decompressed_output_file_path=f"./decom_output/{dataset}.pbc.decom"


# pbc
command = f"{pbc_dir}/bin/pbc-cli --test-compress --file-path {input_file_path} --pattern-path {pbc_dir}/data/patterns/patterns/Apache.pattern  --compressed-file-path {compressed_output_file_path}"
print(f"********************PBC***********************")
run_command(command, f"Using **PBC** compress and decompress **{dataset}**")
print(f"****************************************************")


# pbc-F
fsst_dir = "../baselines/fsst"
# ../fsst/paper/linetest fsst Apache.log.pbc
command = f"{fsst_dir}/paper/linetest fsst {compressed_output_file_path}"
print(f"********************PBC-F***********************")
run_command_fsst(command, f"Using **PBC-F** compress and decompress **{dataset}**")
print(f"****************************************************")


lzbench_dir = "../baselines/lzbench"
#pbc-Z
command = f"{lzbench_dir}/lzbench -ezstd,3 -o4 {compressed_output_file_path}"
print(f"********************PBC-Z***********************")
run_command_lzbench(command, f"Using **PBC-Z** compress and decompress **{dataset}**")
print(f"****************************************************")

#pbc-L
command = f"{lzbench_dir}/lzbench -elzma,6 -o4 {compressed_output_file_path}"
print(f"********************PBC-L***********************")
run_command_lzbench(command, f"Using **PBC-L** compress and decompress **{dataset}**")
print(f"****************************************************")
