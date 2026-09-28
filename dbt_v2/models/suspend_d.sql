{{ config(materialized='external', format='parquet', location=var('candidate_path'), tags=['basic_dataset']) }}

-- 停牌事实按交易日累积；空响应也需生成零行候选文件。
select
    cast(ts_code as varchar) as ts_code,
    cast(try_strptime(trade_date, '%Y%m%d') as date) as trade_date,
    cast(suspend_type as varchar) as suspend_type,
    cast(extracted_at as timestamptz) as extracted_at
from read_parquet('{{ var("temp_path") }}')
