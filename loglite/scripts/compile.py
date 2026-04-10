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

# LogLite

# Update package list
# run_command("sudo apt update", "Updating package list")

# Install libboost-all-dev
# run_command("sudo apt install -y libboost-all-dev", "Installing libboost-all-dev")

loglite_dir_b = "../LogLite-b"
loglite_dir_B = "../LogLite-B"

# Compile the project
# Use generic g++ and AVX2 (no AVX-512 / -march=native) to avoid
# illegal-instruction crashes on CPUs/VMs without AVX-512 support.
compile_command = f"g++ -O3 -mavx2 -fdiagnostics-color=always -g {loglite_dir_b}/src/compress/*.cc {loglite_dir_b}/src/common/*.cc {loglite_dir_b}/src/tools/*.cc -I {loglite_dir_b}/src -o {loglite_dir_b}/src/tools/xorc-cli"
run_command(compile_command, "Compiling the project LogLite-b")

compile_command = f"g++ -O3 -mavx2 -fdiagnostics-color=always -g {loglite_dir_B}/src/compress/*.cc {loglite_dir_B}/src/common/*.cc {loglite_dir_B}/src/tools/*.cc -I {loglite_dir_B}/src -o {loglite_dir_B}/src/tools/xorc-cli"
run_command(compile_command, "Compiling the project LogLite-B")


print("\n-----------Compile LogLite-b and LogLite-B successfully!----------\n")


# FSST, LZ-d, Zstd-d
run_command("make -f Makefile.linux", "Building with Makefile.linux", cwd="../baselines/fsst")

paper_path = os.path.join("fsst", "paper")

# Compile linetest
linetest_cmd = "g++ -std=c++14 -O3 -W -Wall -fpermissive -olinetest -Izstd -Lzstd -g linetest.cpp -llz4 -lzstd -I.. -L.. -lfsst"
run_command(linetest_cmd, "Compiling linetest", cwd="../baselines/fsst/paper")

# Compile linetest-zstd
linetest_zstd_cmd = "g++ -std=c++14 -W -Wall -fpermissive -olinetest-zstd -Izstd -Lzstd -g -O3 linetest-zstd.cpp -llz4 -lzstd -I.. -L.. -lfsst"
run_command(linetest_zstd_cmd, "Compiling linetest-zstd", cwd="../baselines/fsst/paper")

print("\n-----------Compile FSST, LZ-d, Zstd-d successfully!----------\n")



# PBC
run_command("chmod +x build.sh", "", cwd="../baselines/pbc")
run_command("./build.sh -r", "", cwd="../baselines/pbc")
print("\n-----------Compile PBC successfully!----------\n")


# LZ4, ZSTD, LZMA
run_command("make", "make lzbench", cwd="../baselines/lzbench")
print("\n-----------Compile LZ4, ZSTD, LZMA successfully!----------\n")


# LogReducer
run_command("make", "make LogReducer", cwd="../baselines/logreducer")
print("\n-----------Compile LogReducer successfully!----------\n")

# LogShrink
run_command("pip install -r requirements.txt", "pip install for LogShrink", cwd="../baselines/logshrink")
run_command("make", "make LogShrink", cwd="../baselines/logshrink/python_compression/parser")
print("\n-----------Compile LogShrink successfully!----------\n")

# LogGrep-L
run_command("mkdir -p ./output", "mkdir ./output", cwd="../baselines/loggrep-L")
run_command("mkdir -p ./example_zip", "mkdir ./example_zip", cwd="../baselines/loggrep-L")

run_command("make", "make LogGrep-L", cwd="../baselines/loggrep-L/compression")
run_command("make", "make LogGrep-L", cwd="../baselines/loggrep-L/cmdline_loggrep")
print("\n-----------Compile LogGrep-L successfully!----------\n")

# LogGrep-Z
run_command("mkdir -p ./output", "mkdir ./output", cwd="../baselines/loggrep-Z")
run_command("mkdir -p ./example_zip", "mkdir ./example_zip", cwd="../baselines/loggrep-Z")

run_command("make", "make LogGrep-Z", cwd="../baselines/loggrep-Z/zstd-dev/lib")
run_command("make", "make LogGrep-Z", cwd="../baselines/loggrep-Z/compression")
run_command("make", "make LogGrep-Z", cwd="../baselines/loggrep-Z/query")
print("\n-----------Compile LogGrep-Z successfully!----------\n")

