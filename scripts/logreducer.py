import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]


tem = {
    'size': None,
    'compressed_size': None,
    'comression_time': None,
    'decomression_time': None
}

metrics = {
    'compression_rate': None,
    'compression_speed': None,
    'decompression_speed': None
}

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
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def run_command_compress(command, description=None, is_comress=True,cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)
        
        # Parse the output
        time = parse_xorc_output_logreducer(result.stdout)

        if is_comress:
            tem["comression_time"]=time
        else:
            tem["decomression_time"]=time
            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)       


def parse_xorc_output_logreducer(output):
    match = re.search(r'thread accum time:\s*([0-9.]+)', output)
    if match:
        return float(match.group(1))
    return None

def get_file_size_in_mb(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb


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


dataset_name=dataset.split(".")[0]

logreducer_dir = "../baselines/logreducer"

input_file_path=f"../../scripts/datasets/{dataset}"

print(f"********************LogReducer***********************")
command = f"mkdir -p {logreducer_dir}/template/{dataset_name}"
run_command(command, f"mkdir for template")

command = f"python3 training.py -I {input_file_path} -T ./template/{dataset_name}"
run_command(command, f"training template", cwd=f"{logreducer_dir}")

# python3 /home/tangbenzhao/Development/cDev/tem1/LogLite-github/baselines/logreducer/training.py -I /home/tangbenzhao/Development/cDev/tem1/LogLite-github/scripts/datasets/Apache.log -T /home/tangbenzhao/Development/cDev/tem1/LogLite-github/baselines/logreducer/template/

command = f"mkdir -p {logreducer_dir}/out/{dataset_name}/"
run_command(command, f"mkdir for compression output")

command = f"python3 ./LogReducer.py -I {input_file_path} -T ./template/{dataset_name}/ -O ./out/{dataset_name}/"
run_command_compress(command, f"Using **LogReducer** compress **{dataset}**", True, cwd=f"{logreducer_dir}")


# mkdir -p ./decompress_out/
# python3 LogRestore.py -I ./out/Apache/ -T ./template/Apache/ -O ./decompress_out/Apache.log


command = f"mkdir -p {logreducer_dir}/decompress_out/"
run_command(command, f"mkdir for decompression output")

command = f"python3 ./LogRestore.py -I ./out/{dataset_name}/ -T ./template/{dataset_name}/ -O ./decompress_out/{dataset}"
run_command_compress(command, f"Using **LogReducer** decompress **{dataset}**", False, cwd=f"{logreducer_dir}")


tem["size"]= get_file_size_in_mb(f"./datasets/{dataset}")
tem["compressed_size"]= get_directory_size_in_mb(f"{logreducer_dir}/out/{dataset_name}/")

metrics['compression_rate']=tem["compressed_size"]/tem["size"]
metrics['compression_speed']=tem["size"]/tem["comression_time"]
metrics['decompression_speed']=tem["size"]/tem["decomression_time"]

description=f"Using **LogReducer** compress and decompress **{dataset}**"

if all(v is not None for v in metrics.values()):
    for k, v in metrics.items():
        print(f"{k}: {v}")
    save_metrics_to_file(metrics,description)
else:
    print("Warning: Failed to extract some metrics")




print(f"****************************************************")