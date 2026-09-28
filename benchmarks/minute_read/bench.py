"""Minute-bar read benchmark: raw Parquet + on-the-fly view (A) vs standardized
Parquet (B) vs DuckDB native table (C).

Experiment only — never writes outside benchmarks/minute_read/tmp/.

Canonical transform (A applies it at query time; B/C materialize it):
    SELECT code AS symbol,
           CAST(CAST(trade_time AS TIMESTAMP) AS DATE) AS trade_date,
           CAST(trade_time AS TIMESTAMP)               AS bar_time,
           open, high, low, close, vol, amount
    FROM <raw files>
    WHERE CAST(trade_time AS TIMESTAMP)::TIME <> TIME '09:30:00'

Target types: numerics stay FLOAT (source is float32; no precision-driven
change needed). 09:30 is filtered by its real time label, not by position.

Fixed engine settings for every measurement: threads=16, memory_limit=6GB,
temp_directory under tmp/. Reported numbers: first run, then median/range of
>=3 interleaved repeats. Cache state is NOT controlled (no cache drops).
"""
import argparse
import json
import os
import resource
import sys
import time
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw" / "stock_1min"
BENCH_DIR = ROOT / "benchmarks" / "minute_read"
TMP = BENCH_DIR / "tmp"
RESULTS = BENCH_DIR / "results.jsonl"

THREADS = 16
MEMORY_LIMIT = "6GB"

RANGES = {
    "1m": ("2025-01-01", "2025-01-31"),
    "1y": ("2025-01-01", "2025-12-31"),
    "2y": ("2024-01-01", "2025-12-31"),
    "3y": ("2023-01-01", "2025-12-31"),
}

TRANSFORM_COLS = """
    code AS symbol,
    CAST(CAST(trade_time AS TIMESTAMP) AS DATE) AS trade_date,
    CAST(trade_time AS TIMESTAMP) AS bar_time,
    open, high, low, close, vol, amount
"""
FILTER_0930 = "CAST(trade_time AS TIMESTAMP)::TIME <> TIME '09:30:00'"


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def disk_free_gb():
    st = os.statvfs(TMP if TMP.exists() else ROOT)
    return st.f_bavail * st.f_frsize / 1e9


def log_result(record):
    RESULTS.parent.mkdir(parents=True, exist_ok=True)
    with RESULTS.open("a") as f:
        f.write(json.dumps(record) + "\n")


def raw_files(start, end):
    s, e = start.replace("-", ""), end.replace("-", "")
    out = []
    for ydir in sorted(RAW_DIR.glob("year=*")):
        for f in sorted(ydir.glob("*.parquet")):
            if s <= f.stem <= e:
                out.append(str(f))
    return out


def b_dir_files(start, end):
    s, e = start.replace("-", ""), end.replace("-", "")
    out = []
    for ydir in sorted((TMP / "B").glob("year=*")):
        for f in sorted(ydir.glob("*.parquet")):
            if s <= f.stem <= e:
                out.append(str(f))
    return out


def file_list_sql(files):
    return "[" + ", ".join("'" + f + "'" for f in files) + "]"


def connect(db=":memory:"):
    con = duckdb.connect(db)
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    TMP.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{TMP / 'duckdb_spill'}'")
    return con


def sql_source(version, range_name, c_table="bars"):
    """FROM-clause subquery identical in output schema/rows across versions."""
    start, end = RANGES[range_name]
    if version == "A":
        files = raw_files(start, end)
        return f"""(
            SELECT {TRANSFORM_COLS}
            FROM read_parquet({file_list_sql(files)}, union_by_name=true)
            WHERE {FILTER_0930}
        )"""
    if version == "B":
        files = b_dir_files(start, end)
        cols = ("symbol, trade_date, bar_time, open, high, low, close, "
                "vol, amount")
        return f"(SELECT {cols} FROM read_parquet({file_list_sql(files)}, union_by_name=true, hive_partitioning=false))"
    if version == "C":
        cols = ("symbol, trade_date, bar_time, open, high, low, close, "
                "vol, amount")
        return f"(SELECT {cols} FROM {c_table})"
    raise ValueError(version)


AGG_SQL = """
SELECT count(*) AS n,
       min(trade_date) AS min_d, max(trade_date) AS max_d,
       min(bar_time) AS min_t, max(bar_time) AS max_t,
       sum(open) AS s_open, sum(high) AS s_high, sum(low) AS s_low,
       sum(close) AS s_close, sum(vol) AS s_vol, sum(amount) AS s_amount,
       count(DISTINCT symbol) AS n_symbol
FROM {src}
"""


def task_scan(con, version, range_name):
    sql = AGG_SQL.format(src=sql_source(version, range_name))
    return con.execute(sql).fetchone()


def task_arrow(con, version, range_name):
    sql = f"SELECT * FROM {sql_source(version, range_name)}"
    reader = con.execute(sql).fetch_record_batch(rows_per_batch=1_000_000)
    n = 0
    nb = 0
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        n += batch.num_rows
        nb += 1
    return n, nb


def task_pandas(con, version, range_name):
    sql = f"SELECT * FROM {sql_source(version, range_name)}"
    df = con.execute(sql).fetchdf()
    mem = int(df.memory_usage(deep=True).sum())
    return len(df), mem


BREAKDOWN_STEPS = {
    # cumulative steps on one month of raw parquet; every new expression is
    # consumed by an aggregate so nothing is optimized away.
    "1_read": """
        SELECT count(*), sum(open), sum(high), sum(low), sum(close),
               sum(vol), sum(amount),
               sum(length(code)), sum(length(trade_time))
        FROM {files}
    """,
    "2_rename": """
        SELECT count(*), sum(open), sum(high), sum(low), sum(close),
               sum(vol), sum(amount),
               sum(length(symbol)), sum(length(trade_time))
        FROM (SELECT code AS symbol, trade_time, open, high, low, close,
                     vol, amount FROM {files})
    """,
    "3_cast_ts": """
        SELECT count(*), sum(open), sum(high), sum(low), sum(close),
               sum(vol), sum(amount), sum(length(symbol)),
               sum(epoch(bar_time))
        FROM (SELECT code AS symbol,
                     CAST(trade_time AS TIMESTAMP) AS bar_time,
                     open, high, low, close, vol, amount FROM {files})
    """,
    "4_gen_date": """
        SELECT count(*), sum(open), sum(high), sum(low), sum(close),
               sum(vol), sum(amount), sum(length(symbol)),
               sum(epoch(bar_time)), sum(epoch(trade_date))
        FROM (SELECT code AS symbol,
                     CAST(CAST(trade_time AS TIMESTAMP) AS DATE) AS trade_date,
                     CAST(trade_time AS TIMESTAMP) AS bar_time,
                     open, high, low, close, vol, amount FROM {files})
    """,
    "5_filter0930": """
        SELECT count(*), sum(open), sum(high), sum(low), sum(close),
               sum(vol), sum(amount), sum(length(symbol)),
               sum(epoch(bar_time)), sum(epoch(trade_date))
        FROM (SELECT code AS symbol,
                     CAST(CAST(trade_time AS TIMESTAMP) AS DATE) AS trade_date,
                     CAST(trade_time AS TIMESTAMP) AS bar_time,
                     open, high, low, close, vol, amount FROM {files})
        WHERE bar_time::TIME <> TIME '09:30:00'
    """,
}


def run_timed(fn, con, version, range_name, task, rep):
    base_rss = rss_mb()
    free0 = disk_free_gb()
    t0 = time.perf_counter()
    out = fn(con, version, range_name)
    dt = time.perf_counter() - t0
    rec = {
        "ts": datetime.now().isoformat(timespec="seconds"),
        "task": task, "version": version, "range": range_name, "rep": rep,
        "seconds": round(dt, 4),
        "peak_rss_mb": round(rss_mb(), 1),
        "rss_delta_mb": round(rss_mb() - base_rss, 1),
        "disk_free_gb_before": round(free0, 2),
        "out": str(out)[:300],
    }
    log_result(rec)
    return dt, out


# ---------------------------------------------------------------- builds

def build_b(range_max="3y"):
    """Standardized parquet, one file per day mirroring the raw layout.
    No ORDER BY (natural raw order), SNAPPY, default row group size."""
    start, end = RANGES[range_max]
    files = raw_files(start, end)
    out_root = TMP / "B"
    con = connect()
    t0 = time.perf_counter()
    base = rss_mb()
    for i, f in enumerate(files):
        day = Path(f).stem
        year = day[:4]
        odir = out_root / f"year={year}"
        odir.mkdir(parents=True, exist_ok=True)
        out = odir / f"{day}.parquet"
        if out.exists():
            continue
        con.execute(f"""
            COPY (SELECT {TRANSFORM_COLS} FROM '{f}' WHERE {FILTER_0930})
            TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        if (i + 1) % 60 == 0:
            print(f"  B build {i + 1}/{len(files)} days, "
                  f"{time.perf_counter() - t0:.0f}s", flush=True)
    dt = time.perf_counter() - t0
    total_bytes = sum(p.stat().st_size for p in out_root.rglob("*.parquet"))
    rec = {"ts": datetime.now().isoformat(timespec="seconds"), "task": "build",
           "version": "B", "range": range_max, "seconds": round(dt, 2),
           "peak_rss_mb": round(rss_mb(), 1),
           "rss_delta_mb": round(rss_mb() - base, 1),
           "final_size_mb": round(total_bytes / 1e6, 1),
           "disk_free_gb_before": round(disk_free_gb(), 2),
           "params": "per-day files mirroring raw layout; SNAPPY; default "
                     "row_group_size=122880; no ORDER BY (natural order)"}
    log_result(rec)
    print("B build:", rec, flush=True)


def build_c(range_name):
    start, end = RANGES[range_name]
    files = raw_files(start, end)
    db = TMP / f"C_{range_name}.duckdb"
    if db.exists():
        db.unlink()
    con = connect(str(db))
    base = rss_mb()
    t0 = time.perf_counter()
    con.execute(f"""
        CREATE TABLE bars AS
        SELECT {TRANSFORM_COLS}
        FROM read_parquet({file_list_sql(files)}, union_by_name=true)
        WHERE {FILTER_0930}
    """)
    con.execute("CHECKPOINT")
    dt = time.perf_counter() - t0
    con.close()
    size = db.stat().st_size
    rec = {"ts": datetime.now().isoformat(timespec="seconds"), "task": "build",
           "version": "C", "range": range_name, "seconds": round(dt, 2),
           "peak_rss_mb": round(rss_mb(), 1),
           "rss_delta_mb": round(rss_mb() - base, 1),
           "final_size_mb": round(size / 1e6, 1),
           "disk_free_gb_before": round(disk_free_gb(), 2),
           "params": "single DuckDB table 'bars', CTAS in natural file order, "
                     "no ORDER BY"}
    log_result(rec)
    print(f"C build ({range_name}):", rec, flush=True)


# ---------------------------------------------------------------- runners

def connect_for(version, range_name):
    if version == "C":
        return connect(str(TMP / f"C_{range_name}.duckdb")), True
    return connect(), False


def cmd_run(task, range_names, reps):
    """Interleaved repetitions: rotation order per rep to cancel ordering."""
    versions = ["A", "B", "C"]
    tasks = {"scan": task_scan, "arrow": task_arrow, "pandas": task_pandas}
    fn = tasks[task]
    for range_name in range_names:
        for rep in range(reps):
            order = versions[rep % 3:] + versions[:rep % 3]
            for v in order:
                if v == "C" and not (TMP / f"C_{range_name}.duckdb").exists():
                    print(f"skip C {range_name}: table not built", flush=True)
                    continue
                con, owned = connect_for(v, range_name)
                try:
                    dt, out = run_timed(fn, con, v, range_name, task, rep)
                    print(f"{task} {v} {range_name} rep{rep}: {dt:.2f}s "
                          f"{str(out)[:120]}", flush=True)
                finally:
                    con.close()


def cmd_breakdown():
    files = file_list_sql(raw_files(*RANGES["1m"]))
    con = connect()
    for step, tpl in BREAKDOWN_STEPS.items():
        sql = tpl.format(files=f"read_parquet({files}, union_by_name=true)")
        for rep in range(4):
            base = rss_mb()
            t0 = time.perf_counter()
            out = con.execute(sql).fetchone()
            dt = time.perf_counter() - t0
            log_result({"ts": datetime.now().isoformat(timespec="seconds"),
                        "task": "breakdown", "version": step, "range": "1m",
                        "rep": rep, "seconds": round(dt, 4),
                        "peak_rss_mb": round(rss_mb(), 1),
                        "rss_delta_mb": round(rss_mb() - base, 1),
                        "out": str(out)[:200]})
            print(f"breakdown {step} rep{rep}: {dt:.2f}s", flush=True)
    con.close()


def cmd_validate(range_name):
    """Cross-version consistency: aggregates + one-day bidirectional diff."""
    con = connect()
    con.execute(f"ATTACH '{TMP / f'C_{range_name}.duckdb'}' AS cdb (READ_ONLY)")
    sources = {v: sql_source(v, range_name, c_table="cdb.bars") for v in "ABC"}
    results = {}
    for v in "ABC":
        results[v] = con.execute(AGG_SQL.format(src=sources[v])).fetchone()
        print(v, results[v])

    def agg_close(r1, r2):
        # tuple: n, min_d, max_d, min_t, max_t, s_open, s_high, s_low,
        #        s_close, s_vol, s_amount, n_symbol
        for i in [0, 1, 2, 3, 4, 11]:
            if r1[i] != r2[i]:
                return False
        # float sums: relative tolerance for scan-order rounding
        for i in range(5, 11):
            if not abs(r1[i] - r2[i]) <= 1e-6 * max(abs(r1[i]), abs(r2[i])):
                return False
        return True

    ok = agg_close(results["A"], results["B"]) and agg_close(results["A"], results["C"])
    print("aggregates consistent (float tol 1e-6):", ok)

    # bidirectional diff on the first trading day present in the range
    day = con.execute(
        f"SELECT min(trade_date) FROM {sources['A']}").fetchone()[0]
    for x, y in [("A", "B"), ("B", "C"), ("A", "C")]:
        q = f"""
        WITH x AS (SELECT * FROM {sources[x]} WHERE trade_date = '{day}'),
             y AS (SELECT * FROM {sources[y]} WHERE trade_date = '{day}')
        SELECT (SELECT count(*) FROM (SELECT * FROM x EXCEPT ALL SELECT * FROM y)) AS x_minus_y,
               (SELECT count(*) FROM (SELECT * FROM y EXCEPT ALL SELECT * FROM x)) AS y_minus_x
        """
        print(f"diff {x} vs {y} on {day}:", con.execute(q).fetchone())
    con.close()
    return ok


def cmd_explain():
    """EXPLAIN ANALYZE evidence that transforms actually execute (1 month)."""
    con = connect()
    src = sql_source("A", "1m")
    sql = "EXPLAIN ANALYZE " + AGG_SQL.format(src=src)
    rows = con.execute(sql).fetchall()
    out = BENCH_DIR / "explain_analyze_A_1m.txt"
    out.write_text("\n".join(r[1] if len(r) > 1 else str(r[0]) for r in rows))
    print(f"wrote {out}")
    src = sql_source("B", "1m")
    sql = "EXPLAIN ANALYZE " + AGG_SQL.format(src=src)
    rows = con.execute(sql).fetchall()
    out = BENCH_DIR / "explain_analyze_B_1m.txt"
    out.write_text("\n".join(r[1] if len(r) > 1 else str(r[0]) for r in rows))
    print(f"wrote {out}")


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    pb = sub.add_parser("build-b")
    pb.add_argument("range", nargs="?", default="3y", choices=RANGES.keys())
    pc = sub.add_parser("build-c")
    pc.add_argument("ranges", nargs="+", choices=RANGES.keys())
    pr = sub.add_parser("run")
    pr.add_argument("task", choices=["scan", "arrow", "pandas"])
    pr.add_argument("ranges", nargs="+", choices=RANGES.keys())
    pr.add_argument("--reps", type=int, default=4)
    pv = sub.add_parser("validate")
    pv.add_argument("range", choices=RANGES.keys())
    sub.add_parser("breakdown")
    sub.add_parser("explain")
    a = p.parse_args()

    if a.cmd == "build-b":
        build_b(a.range)
    elif a.cmd == "build-c":
        for r in a.ranges:
            build_c(r)
    elif a.cmd == "run":
        cmd_run(a.task, a.ranges, a.reps)
    elif a.cmd == "validate":
        cmd_validate(a.range)
    elif a.cmd == "breakdown":
        cmd_breakdown()
    elif a.cmd == "explain":
        cmd_explain()


if __name__ == "__main__":
    main()
