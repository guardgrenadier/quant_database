import glob, time, duckdb, os

DB = 'benchmarks/stocks_min.duckdb'
if os.path.exists(DB): os.remove(DB)
con = duckdb.connect(DB)
con.execute("SET memory_limit='10GB'; SET threads=12; SET preserve_insertion_order=true;")
con.execute("""
CREATE TABLE minutes(
  code VARCHAR, trade_time TIMESTAMP,
  open FLOAT, high FLOAT, low FLOAT, close FLOAT,
  vol FLOAT, amount FLOAT, pre_close FLOAT, "change" FLOAT, pct_chg FLOAT
);
""")
files = sorted(glob.glob('data/raw/stock_1min/year=*/*.parquet'),
               key=lambda p: p.split('/')[-1])
t0 = time.time()
for i, f in enumerate(files, 1):
    con.execute(f"""
      INSERT INTO minutes
      SELECT code, trade_time::TIMESTAMP, open, high, low, close,
             vol, amount, pre_close, "change", pct_chg
      FROM read_parquet('{f}')
    """)
    if i % 200 == 0 or i == len(files):
        el = time.time() - t0
        print(f'{i}/{len(files)} files, {el:.0f}s, {i/el:.1f} f/s', flush=True)
con.execute("CHECKPOINT")
print('BUILD TOTAL %.1fs' % (time.time()-t0))
