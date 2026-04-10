import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]

dataset_name=dataset.split(".")[0]


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


def run_command_loggrep(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)

        # print(result.stdout)
        
        # Parse the output
        tem['comression_time'] = parse_xorc_output_loggrep(result.stdout)
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_loggrep(output):
    for line in output.split('\n'):
        line = line.strip()
        if 'thread accum time:' in line:
            time_part = line.split('thread accum time:')[-1].strip()
            time_value = time_part.split()[0].rstrip(',')
            try:
                return float(time_value)
            except ValueError:
                return None
    return None


def get_directory_size_in_mb(dir_path):
    if not os.path.isdir(dir_path):
        raise NotADirectoryError(f"Directory not found: {dir_path}")
    
    total_size = 0
    for root, dirs, files in os.walk(dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            if os.path.isfile(file_path):
                total_size += os.path.getsize(file_path)
    
    size_mb = total_size / (1024 * 1024)
    return size_mb




loggrep_L_dir = "../baselines/loggrep-L"

input_file_path=f"../../../scripts/datasets/"

# python3 largeTest_mycopy.py ../../../scripts/datasets/

tem = {
    'size': None,
    'compressed_size': None,
    'comression_time': None
}

metrics = {
    'compression_rate': None,
    'compression_speed': None
}



command = f"python3 largeTest_mycopy.py {input_file_path} {dataset}"
print(f"********************LogGrep-L***********************")
run_command_loggrep(command, f"Using **LogGrep-L** compress **{dataset}**", f"{loggrep_L_dir}/compression/")


tem['size']=get_directory_size_in_mb(f"./datasets/{dataset_name}")
tem['compressed_size']=get_directory_size_in_mb(f"./com_output/loggrep_L/{dataset_name}")

metrics['compression_rate']=tem["compressed_size"]/tem["size"]
metrics['compression_speed']=tem["size"]/tem["comression_time"]

description=f"Using **LogGrep-L** compress **{dataset}**"

if all(v is not None for v in metrics.values()):
    for k, v in metrics.items():
        print(f"{k}: {v}")
    save_metrics_to_file(metrics,description)
else:
    print("Warning: Failed to extract some metrics")


print(f"****************************************************")







loggrep_Z_dir = "../baselines/loggrep-Z"

input_file_path=f"../../../scripts/datasets/"

# python3 largeTest_mycopy.py ../../../scripts/datasets/

tem = {
    'size': None,
    'compressed_size': None,
    'comression_time': None
}

metrics = {
    'compression_rate': None,
    'compression_speed': None
}



command = f"python3 quickTest_mycopy.py {input_file_path} {dataset}"
print(f"********************LogGrep-Z***********************")
run_command_loggrep(command, f"Using **LogGrep-Z** compress **{dataset}**", f"{loggrep_Z_dir}/compression/")


tem['size']=get_directory_size_in_mb(f"./datasets/{dataset_name}")
tem['compressed_size']=get_directory_size_in_mb(f"./com_output/loggrep_Z/{dataset_name}")

metrics['compression_rate']=tem["compressed_size"]/tem["size"]
metrics['compression_speed']=tem["size"]/tem["comression_time"]

description=f"Using **LogGrep-Z** compress **{dataset}**"

if all(v is not None for v in metrics.values()):
    for k, v in metrics.items():
        print(f"{k}: {v}")
    save_metrics_to_file(metrics,description)
else:
    print("Warning: Failed to extract some metrics")


print(f"****************************************************")