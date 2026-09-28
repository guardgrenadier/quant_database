"""Aggregate results.jsonl into the report tables.

For each (task, version, range): first-run time (rep 0), then median and
min/max range over the remaining reps.
"""
import json
import statistics
from collections import defaultdict
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
RESULTS = BENCH_DIR / "results.jsonl"

rows = [json.loads(l) for l in RESULTS.read_text().splitlines()]

def fmt(v):
    return f"{v:.2f}" if v is not None else "-"

# ---- read tasks -------------------------------------------------------
groups = defaultdict(list)
for r in rows:
    if r["task"] in ("scan", "arrow", "pandas"):
        groups[(r["task"], r["version"], r["range"])].append(r)

summary = {}
for k, rs in groups.items():
    rs = sorted(rs, key=lambda r: r["rep"])
    first = rs[0]["seconds"]
    rest = [r["seconds"] for r in rs[1:]]
    med = statistics.median(rest) if rest else first
    lo, hi = (min(rest), max(rest)) if rest else (first, first)
    peak = max(r["peak_rss_mb"] for r in rs)
    summary[k] = (first, med, lo, hi, peak)

task_names = {"scan": "任务1 聚合扫描", "arrow": "任务2a Arrow流式",
              "pandas": "任务2b pandas加载"}
for task in ("scan", "arrow", "pandas"):
    print(f"\n### {task_names[task]} (秒; first / median / range)")
    print("| 范围 | A 原始视图 | B 标准Parquet | C DuckDB表 | A−B | A/B | A−C |")
    print("|---|---:|---:|---:|---:|---:|---:|")
    for rg in ("1m", "1y", "2y", "3y"):
        a = summary.get((task, "A", rg))
        b = summary.get((task, "B", rg))
        c = summary.get((task, "C", rg))
        if not (a and b):
            continue
        am, bm = a[1], b[1]
        cm = c[1] if c else None
        print(f"| {rg} | {fmt(a[0])}/{fmt(am)}/[{fmt(a[2])}-{fmt(a[3])}] "
              f"| {fmt(b[0])}/{fmt(bm)}/[{fmt(b[2])}-{fmt(b[3])}] "
              f"| {fmt(c[0])}/{fmt(cm)}/[{fmt(c[2])}-{fmt(c[3])}] " if c else "", end="")
        print(f"| {fmt(am - bm)} | {fmt(am / bm)}x | "
              f"{fmt(am - cm) if cm else '-'} |")

# peak RSS table
print("\n### 峰值RSS (MB, 各组最大)")
print("| 任务 | 范围 | A | B | C |")
print("|---|---|---:|---:|---:|")
for task in ("scan", "arrow", "pandas"):
    for rg in ("1m", "1y", "2y", "3y"):
        vals = [summary.get((task, v, rg)) for v in "ABC"]
        if vals[0] is None:
            continue
        print(f"| {task} | {rg} | " + " | ".join(
            f"{v[4]:.0f}" if v else "-" for v in vals) + " |")

# ---- builds -----------------------------------------------------------
print("\n### B/C 生成成本(不计入读取时间)")
print("| 版本 | 范围 | 耗时s | 峰值RSS MB | 最终大小MB |")
print("|---|---|---:|---:|---:|")
for r in rows:
    if r["task"] == "build":
        print(f"| {r['version']} | {r['range']} | {r['seconds']} "
              f"| {r['peak_rss_mb']} | {r.get('final_size_mb', '-')} |")

# ---- breakdown --------------------------------------------------------
bd = defaultdict(list)
for r in rows:
    if r["task"] == "breakdown":
        bd[r["version"]].append(r)
print("\n### 转换成本拆解(1个月原始Parquet; first/median 秒)")
prev = None
for step in ("1_read", "2_rename", "3_cast_ts", "4_gen_date", "5_filter0930"):
    rs = sorted(bd[step], key=lambda r: r["rep"])
    first = rs[0]["seconds"]
    med = statistics.median(r["seconds"] for r in rs[1:])
    delta = med - prev if prev is not None else 0.0
    print(f"| {step} | {first:.3f} | {med:.3f} | +{delta:.3f} |")
    prev = med
