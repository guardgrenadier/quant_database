-- 连接当前生产权威库 data/warehouse.duckdb 后执行；空库返回零行。
SELECT * REPLACE (CAST(bar_time AS VARCHAR) AS bar_time)
FROM canonical.stock_1min
WHERE ts_code = '603056.SH'
ORDER BY trade_date ASC, ts_code, bar_time
LIMIT 10000;
