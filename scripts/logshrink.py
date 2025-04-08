import os
import subprocess
import re
import sys

# dataset="Apache.log"
dataset=sys.argv[1]


dataset_name=dataset.split(".")[0]


metrics = {
    'compression_ratio': None,
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


def get_file_size_in_mb(file_path):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    size_bytes = os.path.getsize(file_path)
    size_mb = size_bytes / (1024 * 1024)
    return size_mb



def run_command(command, description=None, is_compress=True,cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    
    try:
        # Run command and capture output
        result = subprocess.run(command, shell=True, check=True, 
                              cwd=cwd, capture_output=True, text=True)

        parse_xorc_output_logshrink(result.stdout, is_compress)

            
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


def parse_xorc_output_logshrink(output, is_compress):
 
    for line in output.split('\n'):
        line = line.strip()
        
        if is_compress:
            if 'Compression ratio' in line and 'compression speed' in line:
                ratio_part = line.split('Compression ratio')[-1].split(',')[0].strip()
                metrics['compression_ratio'] = float(ratio_part)
                
                speed_part = line.split('compression speed')[-1].strip()
                metrics['compression_speed'] = float(speed_part)
                return
        else:
            if 'Main finished, total time cost:' in line:
                parts = line.split('Main finished, total time cost:')[-1].split(',')
                metrics['decompression_speed'] = float(parts[0].strip())
                return



logshrink_dir = "../baselines/logshrink"

input_file_dir=f"../../../scripts/datasets/"
compressed_output_file_path=f"../../../scripts/com_output/{dataset_name}/"
decompressed_output_file_path=f"../../../../scripts/decom_output/"



# logshrink
# If permissions are insufficient, use sudo
# sudo python3 ./run.py

command = f"python3 ./run.py -I {input_file_dir} -ds {dataset_name} -E E -C -K lzma -V -P -wh 50 -th 10 -NC 16 -S -outdir {compressed_output_file_path}"
print(f"********************LogShrink***********************")
run_command(command, f"Using **LogShrink** compress **{dataset}**", True, f"{logshrink_dir}/python_compression")


command = f"python3 ./decompress_run.py -E E -K lzma -I ../{compressed_output_file_path}{dataset_name} -T ../template/ -O {decompressed_output_file_path}"
run_command(command, f"Using **LogShrink** decompress **{dataset}**", False, f"{logshrink_dir}/python_compression/decompression/")

metrics['compression_ratio'] = 1 / metrics['compression_ratio']
metrics['compression_speed'] = metrics['compression_speed'] / 1024.0 / 1024.0
metrics['decompression_speed'] = get_file_size_in_mb(f"./datasets/{dataset}") / metrics['decompression_speed']



description=f"Using **LogShrink** compress and decompress **{dataset}**"

if all(v is not None for v in metrics.values()):
    for k, v in metrics.items():
        print(f"{k}: {v}")
    save_metrics_to_file(metrics,description)
else:
    print("Warning: Failed to extract some metrics")

print(f"****************************************************")
