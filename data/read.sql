SELECT * REPLACE (CAST(bar_time AS VARCHAR) AS bar_time)
FROM warehouse_v3_2.canonical.stock_1min
ORDER BY ts_code, bar_time 
LIMIT 1000;