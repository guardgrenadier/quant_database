{{ config(materialized='external', format='parquet', location=var('candidate_path'), tags=['minute_ingestion']) }}

-- temp 保留源数字文本；先按十进制元值乘 100，再按 half-up 舍入为整数分。
with raw as (
    select * from read_parquet('{{ var("temp_path") }}')
)
select
    cast(ts_code as varchar) as ts_code,
    try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') as trade_time,
    cast(try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') as date) as trade_date,
    try_cast(open as double) as open,
    try_cast(high as double) as high,
    try_cast(low as double) as low,
    try_cast(close as double) as close,
    try_cast(vol as bigint) as vol,
    try_cast(round(try_cast(amount as decimal(38,18)) * 100, 0) as bigint) as amount,
    try_cast(extracted_at as timestamptz) as extracted_at
from raw
