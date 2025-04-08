import os
import subprocess

def run_command(command, description=None, cwd=None):
    if description:
        print(f"Executing: {description}")
    print(f"$ {command}")
    try:
        subprocess.run(command, shell=True, check=True, cwd=cwd)
    except subprocess.CalledProcessError as e:
        print(f"Command failed: {e}")
        exit(1)


dataset="Apache.log"
dataset_name=dataset.split(".")[0]

command = f"mkdir -p ./datasets/{dataset_name}"
run_command(command)

command = f"cp ./datasets/{dataset} ./datasets/{dataset_name}/"
run_command(command)

# LogLite
command = f"python3 loglite.py {dataset}"
run_command(command)

command = f"rm -r ./com_output/*"
run_command(command)

command = f"rm -r ./decom_output/*"
run_command(command)

# pbc
command = f"python3 pbc.py {dataset}"
run_command(command)

command = f"rm -r ./com_output/*"
run_command(command)

# fsst,lz4-d,zstd-d
command = f"python3 fsst.py {dataset}"
run_command(command)

# Lz4,Zstd,LZMA
command = f"python3 lzbench.py {dataset}"
run_command(command)

# logreducer
command = f"python3 logreducer.py {dataset}"
run_command(command)

command = f"rm -r ../baselines/logreducer/out/* ../baselines/logreducer/decompress_out/*"
run_command(command)


# logshrink
command = f"python3 logshrink.py {dataset}"
run_command(command)

command = f"rm -r ./datasets/{dataset_name}/{dataset_name}_Segment ./datasets/{dataset_name}/{dataset}.sample"
run_command(command)

command = f"rm -r ../baselines/logshrink/python_compression/template/{dataset_name}"
run_command(command)

command = f"rm -r ./com_output/*"
run_command(command)
command = f"rm -r ./decom_output/*"
run_command(command)

# loggrep
command = f"python3 loggrep.py {dataset}"
run_command(command)

command = f"rm -r ./com_output/*"
run_command(command)

command = f"rm -r ./datasets/{dataset_name}_Segment ./datasets/{dataset}.sample"
run_command(command)


command = f"rm -r ./datasets/{dataset_name}/"
run_command(command)