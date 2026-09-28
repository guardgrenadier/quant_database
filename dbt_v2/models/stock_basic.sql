{{ config(materialized='external', format='parquet', location=var('candidate_path'), tags=['basic_dataset']) }}

-- 保留源列名；快照日期由本次发布计划固定，不从证券记录推断。
select
    cast(ts_code as varchar) as ts_code,
    cast(symbol as varchar) as symbol,
    cast(name as varchar) as name,
    cast(exchange as varchar) as exchange,
    cast(market as varchar) as market,
    cast(list_status as varchar) as list_status,
    cast(try_strptime(list_date, '%Y%m%d') as date) as list_date,
    cast(try_strptime(delist_date, '%Y%m%d') as date) as delist_date,
    cast(extracted_at as timestamptz) as extracted_at,
    cast('{{ var("as_of_date") }}' as date) as snapshot_date
from read_parquet('{{ var("temp_path") }}')
