import subprocess, os, re

BASE   = "/mnt/c/Users/HOME/Big_Data"
SRC    = f"{BASE}/loglite/LogLite-B/src/tools/xorc-cli"
STATIC = f"{BASE}/loglite/LogLite-B/src_static/tools/xorc-cli"
OUTDIR = f"{BASE}/benchmark_output"
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
RUNS = 5

def file_size_kb(path):
    try:    return os.path.getsize(path) / 1024
    except: return 0

def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True).stdout + \
           subprocess.run(cmd, capture_output=True, text=True).stderr

def run_once(cmd):
    r = subprocess.run(cmd, capture_output=True, text=True)
    return r.stdout + r.stderr

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
        lite  = f"{OUTDIR}/{ds_name}_{ver_name}.{suffix}"
        decom = f"{OUTDIR}/{ds_name}_{ver_name}.decom"

        print(f"\n>>> {ver_name} | {ds_name}  ({RUNS} runs)")

        ratios, comp_speeds, decomp_speeds = [], [], []

        for i in range(RUNS):
            out = run_once([cli, "--test",
                            "--file-path",        log_path,
                            "--com-output-path",  lite,
                            "--decom-output-path", decom])
            r  = parse(out, "ratio")
            cs = parse(out, "comp_speed")
            ds = parse(out, "decomp_speed")
            if r:  ratios.append(r)
            if cs: comp_speeds.append(cs)
            if ds: decomp_speeds.append(ds)
            print(f"  run {i+1}: ratio={r}  comp={cs} MB/s  decomp={ds} MB/s")

        avg_ratio  = round(sum(ratios)/len(ratios), 4)       if ratios       else None
        avg_comp   = round(sum(comp_speeds)/len(comp_speeds), 2) if comp_speeds else None
        avg_decomp = round(sum(decomp_speeds)/len(decomp_speeds), 2) if decomp_speeds else None
        comp_kb    = file_size_kb(lite)

        print(f"  AVG: ratio={avg_ratio}  comp={avg_comp} MB/s  decomp={avg_decomp} MB/s")

        results.append({
            "version":  ver_name,
            "dataset":  ds_name,
            "raw_KB":   round(raw_kb, 1),
            "comp_KB":  round(comp_kb, 1),
            "ratio":    avg_ratio,
            "comp_speed_MBs":  avg_comp,
            "decomp_speed_MBs": avg_decomp,
        })

print("\n" + "="*78)
print(f"{'Version':<14} {'Dataset':<8} {'Raw KB':>8} {'Comp KB':>8} "
      f"{'Ratio':>7} {'Comp MB/s':>10} {'Decomp MB/s':>12}")
print("-"*78)
for r in results:
    print(f"{r['version']:<14} {r['dataset']:<8} {r['raw_KB']:>8} "
          f"{r['comp_KB']:>8} {str(r['ratio']):>7} "
          f"{str(r['comp_speed_MBs']):>10} {str(r['decomp_speed_MBs']):>12}")
print("="*78)

csv_path = f"{OUTDIR}/comparison_avg.csv"
with open(csv_path, "w") as f:
    f.write("version,dataset,raw_KB,comp_KB,ratio,comp_speed_MBs,decomp_speed_MBs\n")
    for r in results:
        f.write(f"{r['version']},{r['dataset']},{r['raw_KB']},{r['comp_KB']},"
                f"{r['ratio']},{r['comp_speed_MBs']},{r['decomp_speed_MBs']}\n")
print(f"\nCSV saved to: {csv_path}")
