"""Price-type A/B benchmark: OHLC as DOUBLE (A) vs DECIMAL(18,6) (B).

Only the OHLC types differ. Both versions: symbol VARCHAR, trade_date DATE,
bar_time TIMESTAMP, vol BIGINT, amount DECIMAL(18,4); per-day parquet files
mirroring the raw layout, SNAPPY, default row group size, no ORDER BY.
Source prices are float32 — there is no original decimal text, so the
correctness check measures DOUBLE(float32) vs DECIMAL rounding explicitly.

Fixed engine settings: threads=16, memory_limit=6GB. Cache not controlled.
"""
import argparse
import json
import os
import resource
import statistics
import time
from datetime import datetime
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = ROOT / "data" / "raw" / "stock_1min"
BENCH_DIR = ROOT / "benchmarks" / "decimal_vs_double"
TMP = BENCH_DIR / "tmp"
RESULTS = BENCH_DIR / "results.jsonl"

THREADS = 16
MEMORY_LIMIT = "6GB"

RANGES = {
    "1m": ("2025-01-01", "2025-01-31"),
    "1y": ("2025-01-01", "2025-12-31"),
    "3y": ("2023-01-01", "2025-12-31"),
}

PRICE_TYPE = {"double": "DOUBLE", "decimal": "DECIMAL(18,6)"}


def rss_mb():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0


def log_result(record):
    with RESULTS.open("a") as f:
        f.write(json.dumps(record) + "\n")


def raw_files(start, end):
    s, e = start.replace("-", ""), end.replace("-", "")
    return [str(f) for ydir in sorted(RAW_DIR.glob("year=*"))
            for f in sorted(ydir.glob("*.parquet")) if s <= f.stem <= e]


def ver_files(version, start, end):
    s, e = start.replace("-", ""), end.replace("-", "")
    return [str(f) for ydir in sorted((TMP / version).glob("year=*"))
            for f in sorted(ydir.glob("*.parquet")) if s <= f.stem <= e]


def fl_sql(files):
    return "[" + ", ".join("'" + f + "'" for f in files) + "]"


def connect():
    con = duckdb.connect()
    con.execute(f"SET threads={THREADS}")
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    TMP.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{TMP / 'spill'}'")
    return con


def source(version, range_name):
    start, end = RANGES[range_name]
    files = ver_files(version, start, end)
    cols = "symbol, trade_date, bar_time, open, high, low, close, vol, amount"
    return (f"(SELECT {cols} FROM read_parquet({fl_sql(files)}, "
            f"union_by_name=true, hive_partitioning=false))")


def build(version, range_max="3y"):
    pt = PRICE_TYPE[version]
    # DuckDB 1.5.5 direct FLOAT->DECIMAL cast is lossy (1501.0f becomes
    # 1500.999936); route through DOUBLE which is exact.
    cast = ("CAST({c} AS DOUBLE)" if version == "double"
            else f"CAST(CAST({{c}} AS DOUBLE) AS {pt})")
    start, end = RANGES[range_max]
    files = raw_files(start, end)
    con = connect()
    base = rss_mb()
    t0 = time.perf_counter()
    for i, f in enumerate(files):
        day = Path(f).stem
        odir = TMP / version / f"year={day[:4]}"
        odir.mkdir(parents=True, exist_ok=True)
        out = odir / f"{day}.parquet"
        if out.exists():
            continue
        o_e = cast.format(c="open")
        h_e = cast.format(c="high")
        l_e = cast.format(c="low")
        c_e = cast.format(c="close")
        con.execute(f"""
            COPY (
              SELECT code AS symbol,
                     CAST(CAST(trade_time AS TIMESTAMP) AS DATE) AS trade_date,
                     CAST(trade_time AS TIMESTAMP) AS bar_time,
                     {o_e} AS open, {h_e} AS high,
                     {l_e} AS low, {c_e} AS close,
                     CAST(vol AS BIGINT) AS vol,
                     CAST(amount AS DECIMAL(18,4)) AS amount
              FROM '{f}'
              WHERE CAST(trade_time AS TIMESTAMP)::TIME <> TIME '09:30:00'
            ) TO '{out}' (FORMAT PARQUET, COMPRESSION SNAPPY)
        """)
        if (i + 1) % 120 == 0:
            print(f"  {version} build {i+1}/{len(files)}, "
                  f"{time.perf_counter()-t0:.0f}s", flush=True)
    dt = time.perf_counter() - t0
    total = sum(p.stat().st_size for p in (TMP / version).rglob("*.parquet"))
    rec = {"ts": datetime.now().isoformat(timespec="seconds"), "task": "build",
           "version": version, "range": range_max, "seconds": round(dt, 2),
           "peak_rss_mb": round(rss_mb(), 1),
           "rss_delta_mb": round(rss_mb() - base, 1),
           "final_size_mb": round(total / 1e6, 1)}
    log_result(rec)
    print(f"build {version}:", rec, flush=True)


# ------------------------------------------------------------------ tasks

SCAN_SQL = """
SELECT min(open), max(open), sum(open), min(high), max(high), sum(high),
       min(low), max(low), sum(low), min(close), max(close), sum(close),
       count(*)
FROM {src}
"""

FEAT_SQL = """
SELECT count(*), sum(avg_amp), sum(avg_ret) FROM (
  SELECT symbol, trade_date,
         avg(CASE WHEN open = 0 THEN NULL ELSE (high - low) / open END) AS avg_amp,
         avg(CASE WHEN open = 0 THEN NULL ELSE (close - open) / open END) AS avg_ret
  FROM {src}
  GROUP BY symbol, trade_date
)
"""

EXPORT_COLS = {
    "double": "open, high, low, close",
    "decimal": ("CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high, "
                "CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close"),
}


def task_scan(con, version, range_name):
    return con.execute(SCAN_SQL.format(src=source(version, range_name))).fetchone()


def task_features(con, version, range_name):
    return con.execute(FEAT_SQL.format(src=source(version, range_name))).fetchone()


def task_export(con, version, range_name):
    cols = EXPORT_COLS[version]
    sql = f"SELECT {cols} FROM {source(version, range_name)}"
    reader = con.execute(sql).fetch_record_batch(rows_per_batch=1_000_000)
    n = nb = 0
    while True:
        try:
            b = reader.read_next_batch()
        except StopIteration:
            break
        n += b.num_rows
        nb += 1
    return n, nb


def run_timed(fn, con, version, range_name, task, rep):
    base = rss_mb()
    t0 = time.perf_counter()
    out = fn(con, version, range_name)
    dt = time.perf_counter() - t0
    rec = {"ts": datetime.now().isoformat(timespec="seconds"), "task": task,
           "version": version, "range": range_name, "rep": rep,
           "seconds": round(dt, 4), "peak_rss_mb": round(rss_mb(), 1),
           "rss_delta_mb": round(rss_mb() - base, 1), "out": str(out)[:200]}
    log_result(rec)
    return dt, out


def cmd_run(task, ranges, reps):
    fn = {"scan": task_scan, "features": task_features,
          "export": task_export}[task]
    versions = ["double", "decimal"]
    for rg in ranges:
        for rep in range(reps):
            order = versions if rep % 2 == 0 else versions[::-1]
            for v in order:
                con = connect()
                try:
                    dt, out = run_timed(fn, con, v, rg, task, rep)
                    print(f"{task} {v} {rg} rep{rep}: {dt:.2f}s "
                          f"{str(out)[:100]}", flush=True)
                finally:
                    con.close()


# ------------------------------------------------------------ correctness

def cmd_check(range_name="1m"):
    con = connect()
    srcs = {v: source(v, range_name) for v in ("double", "decimal")}
    # schema of both versions
    for v, s in srcs.items():
        print(v, con.execute(f"DESCRIBE SELECT * FROM {s}").fetchall())
    # row counts, pk uniqueness, nulls
    for v, s in srcs.items():
        print(v, "rows/nulls/pkdup:",
              con.execute(f"""
                SELECT count(*),
                       count(*) FILTER (open IS NULL OR high IS NULL OR
                                        low IS NULL OR close IS NULL OR
                                        vol IS NULL OR amount IS NULL),
                       count(*) - count(DISTINCT (symbol, bar_time))
                FROM {s}""").fetchone())
    # decimal rounding: float32 -> DECIMAL(18,6) rounds the exact binary value;
    # quantify vs exact DECIMAL(38,20) of the same double
    print("rounding rows (exact-dec != dec18_6):",
          con.execute(f"""
            SELECT count(*) FROM (
              SELECT CAST(CAST(open AS DOUBLE) AS DECIMAL(38,20)) AS e,
                     CAST(open AS DECIMAL(18,6)) AS d
              FROM {srcs['double']}) WHERE e <> d""").fetchone())
    # max abs diff DOUBLE vs decimal::DOUBLE + differing rows
    print("max abs diff & diff rows:",
          con.execute(f"""
            SELECT max(greatest(abs(a.open - CAST(b.open AS DOUBLE)),
                                abs(a.high - CAST(b.high AS DOUBLE)),
                                abs(a.low - CAST(b.low AS DOUBLE)),
                                abs(a.close - CAST(b.close AS DOUBLE)))),
                   sum(a.open <> CAST(b.open AS DOUBLE) OR
                       a.high <> CAST(b.high AS DOUBLE) OR
                       a.low <> CAST(b.low AS DOUBLE) OR
                       a.close <> CAST(b.close AS DOUBLE))
            FROM {srcs['double']} a JOIN {srcs['decimal']} b
              ON a.symbol = b.symbol AND a.bar_time = b.bar_time
          """).fetchone())
    print("samples:",
          con.execute(f"""
            SELECT a.symbol, a.bar_time, a.close AS dbl,
                   CAST(b.close AS DOUBLE) AS dec_as_dbl, b.close AS dec
            FROM {srcs['double']} a JOIN {srcs['decimal']} b
              ON a.symbol = b.symbol AND a.bar_time = b.bar_time
            WHERE a.close <> CAST(b.close AS DOUBLE)
            ORDER BY abs(a.close - CAST(b.close AS DOUBLE)) DESC LIMIT 5
          """).fetchall())
    con.close()


# ------------------------------------------------------------- precision

def cmd_precision():
    con = connect()
    srcs = {v: source(v, "1m") for v in ("double", "decimal")}
    day = "2025-01-02"
    # 1) high = close counts on one day
    for v, s in srcs.items():
        print(v, "high=close rows:", con.execute(
            f"SELECT count(*) FROM {s} WHERE trade_date='{day}' AND high=close"
        ).fetchone()[0])
    # rows where the equality verdict differs
    print("verdict-diff rows:", con.execute(f"""
        SELECT count(*) FROM {srcs['double']} a
        JOIN {srcs['decimal']} b ON a.symbol=b.symbol AND a.bar_time=b.bar_time
        WHERE a.trade_date='{day}' AND (a.high=a.close) <> (b.high=b.close)
    """).fetchone()[0])
    # 2) close - open max diff between versions (one day)
    print("max |(c-o)_dbl - (c-o)_dec|:", con.execute(f"""
        SELECT max(abs((a.close-a.open) - CAST(b.close-b.open AS DOUBLE)))
        FROM {srcs['double']} a
        JOIN {srcs['decimal']} b ON a.symbol=b.symbol AND a.bar_time=b.bar_time
        WHERE a.trade_date='{day}'
    """).fetchone()[0])
    # 3) cumulative intraday price diff per symbol-day, max diff (1 month)
    print("max |sum(c-o) diff| over symbol-days (1m):", con.execute(f"""
        WITH da AS (SELECT symbol, trade_date, sum(close-open) s
                    FROM {srcs['double']} GROUP BY 1,2),
             db AS (SELECT symbol, trade_date,
                           CAST(sum(close-open) AS DOUBLE) s
                    FROM {srcs['decimal']} GROUP BY 1,2)
        SELECT max(abs(da.s - db.s)) FROM da JOIN db USING (symbol, trade_date)
    """).fetchone()[0])
    # 4) daily features max diff (1 month)
    print("max feature diff (1m):", con.execute(f"""
        WITH fa AS (SELECT symbol, trade_date,
                      avg(CASE WHEN open=0 THEN NULL ELSE (high-low)/open END) amp,
                      avg(CASE WHEN open=0 THEN NULL ELSE (close-open)/open END) ret
                    FROM {srcs['double']} GROUP BY 1,2),
             fb AS (SELECT symbol, trade_date,
                      CAST(avg(CASE WHEN open=0 THEN NULL ELSE (high-low)/open END) AS DOUBLE) amp,
                      CAST(avg(CASE WHEN open=0 THEN NULL ELSE (close-open)/open END) AS DOUBLE) ret
                    FROM {srcs['decimal']} GROUP BY 1,2)
        SELECT max(abs(fa.amp-fb.amp)), max(abs(fa.ret-fb.ret))
        FROM fa JOIN fb USING (symbol, trade_date)
    """).fetchone())
    # open=0 rows?
    print("open=0 rows (1m):", con.execute(
        f"SELECT count(*) FROM {srcs['double']} WHERE open=0").fetchone()[0])
    # observation: pandas dtype without explicit cast
    df = con.execute(
        f"SELECT open, close FROM {srcs['decimal']} LIMIT 1000").fetchdf()
    print("pandas dtype of DECIMAL cols without cast:", dict(df.dtypes.astype(str)))
    con.close()


def cmd_explain():
    con = connect()
    for v in ("double", "decimal"):
        rows = con.execute("EXPLAIN " + SCAN_SQL.format(
            src=source(v, "1m"))).fetchall()
        out = BENCH_DIR / f"explain_scan_{v}_1m.txt"
        out.write_text("\n".join(str(r[0]) for r in rows))
        print("wrote", out)
    con.close()


def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    pb = sub.add_parser("build")
    pb.add_argument("version", choices=["double", "decimal"])
    pb.add_argument("range", nargs="?", default="3y", choices=RANGES.keys())
    pr = sub.add_parser("run")
    pr.add_argument("task", choices=["scan", "features", "export"])
    pr.add_argument("ranges", nargs="+", choices=RANGES.keys())
    pr.add_argument("--reps", type=int, default=4)
    sub.add_parser("check")
    sub.add_parser("precision")
    sub.add_parser("explain")
    a = p.parse_args()
    if a.cmd == "build":
        build(a.version, a.range)
    elif a.cmd == "run":
        cmd_run(a.task, a.ranges, a.reps)
    elif a.cmd == "check":
        cmd_check()
    elif a.cmd == "precision":
        cmd_precision()
    elif a.cmd == "explain":
        cmd_explain()


if __name__ == "__main__":
    main()
