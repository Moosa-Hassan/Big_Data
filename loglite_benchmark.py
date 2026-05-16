import subprocess, os, re, time

BASE    = "/mnt/c/Users/HOME/Big_Data"
SRC     = f"{BASE}/loglite/LogLite-B/src/tools/xorc-cli"
STATIC  = f"{BASE}/loglite/LogLite-B/src_static/tools/xorc-cli"
OUTDIR  = f"{BASE}/benchmark_output"
os.makedirs(OUTDIR, exist_ok=True)

DATASETS = {
    "Apache": f"{BASE}/dataset/loghub/Apache/Apache_2k.log",
    "Linux":  f"{BASE}/dataset/loghub/Linux/Linux_2k.log",
    "HDFS":   f"{BASE}/dataset/loghub/HDFS/HDFS_2k.log",
}

VERSIONS = {
    "src":        SRC,
    "src_static": STATIC,
}

def file_size_kb(path):
    try:
        return os.path.getsize(path) / 1024
    except:
        return 0

def run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.stdout + result.stderr

def parse(output, key):
    patterns = {
        "ratio":       r"compression rate[:\s]+([\d.]+)",
        "comp_speed":  r"compression speed[:\s]+([\d.]+)\s*MB/s",
        "decomp_speed":r"decompression speed[:\s]+([\d.]+)\s*MB/s",
    }
    m = re.search(patterns[key], output, re.IGNORECASE)
    return float(m.group(1)) if m else None

results = []

for ds_name, log_path in DATASETS.items():
    raw_kb = file_size_kb(log_path)
    for ver_name, cli in VERSIONS.items():
        suffix = "static.lite" if ver_name == "src_static" else "lite"
        lite  = f"{OUTDIR}/{ds_name}_{ver_name}.{suffix}" if ver_name == "src" else f"{OUTDIR}/{ds_name}_{ver_name}.static.lite"
        decom = f"{OUTDIR}/{ds_name}_{ver_name}.decom"

        print(f"\n>>> {ver_name} | {ds_name}")

        out = run([cli, "--test",
                   "--file-path",        log_path,
                   "--com-output-path",  lite,
                   "--decom-output-path",decom])
        print(out)

        comp_kb   = file_size_kb(lite)
        ratio     = parse(out, "ratio")
        comp_spd  = parse(out, "comp_speed")
        decomp_spd= parse(out, "decomp_speed")

        if ratio is None and comp_kb > 0:
            ratio = round(comp_kb / raw_kb, 4)

        results.append({
            "version": ver_name,
            "dataset": ds_name,
            "raw_KB":  round(raw_kb, 1),
            "comp_KB": round(comp_kb, 1),
            "ratio":   ratio,
            "comp_speed_MBs":  comp_spd,
            "decomp_speed_MBs":decomp_spd,
        })

# Print table
print("\n" + "="*75)
print(f"{'Version':<12} {'Dataset':<8} {'Raw KB':>8} {'Comp KB':>8} "
      f"{'Ratio':>7} {'Comp MB/s':>10} {'Decomp MB/s':>12}")
print("-"*75)
for r in results:
    print(f"{r['version']:<12} {r['dataset']:<8} {r['raw_KB']:>8} "
          f"{r['comp_KB']:>8} {str(r['ratio']):>7} "
          f"{str(r['comp_speed_MBs']):>10} {str(r['decomp_speed_MBs']):>12}")
print("="*75)

# Save CSV
csv_path = f"{OUTDIR}/comparison.csv"
with open(csv_path, "w") as f:
    f.write("version,dataset,raw_KB,comp_KB,ratio,comp_speed_MBs,decomp_speed_MBs\n")
    for r in results:
        f.write(f"{r['version']},{r['dataset']},{r['raw_KB']},{r['comp_KB']},"
                f"{r['ratio']},{r['comp_speed_MBs']},{r['decomp_speed_MBs']}\n")
print(f"\nCSV saved to: {csv_path}")
