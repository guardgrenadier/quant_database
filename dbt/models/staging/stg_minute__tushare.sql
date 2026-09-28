{{
  config(
    materialized='external',
    format='parquet',
    location=var('minute_candidate_path', 'dbt/target/minute_empty.parquet'),
    tags=['minute_ingestion']
  )
}}

{% if var('minute_raw_glob', '') %}
with raw as (
    select * from read_parquet('{{ var("minute_raw_glob") }}', union_by_name=true)
)
select
    cast(ts_code as varchar) as secu_code,
    try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') as trade_time,
    cast(try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') as date) as trade_date,
    try_cast(open as double) as open,
    try_cast(high as double) as high,
    try_cast(low as double) as low,
    try_cast(close as double) as close,
    try_cast(vol as bigint) as volume,
    try_cast(round(try_cast(amount as double), 0) as bigint) as amount,
    try_cast(extracted_at as timestamptz) as extracted_at
from raw
{% else %}
select
    cast(null as varchar) as secu_code,
    cast(null as timestamp) as trade_time,
    cast(null as date) as trade_date,
    cast(null as double) as open,
    cast(null as double) as high,
    cast(null as double) as low,
    cast(null as double) as close,
    cast(null as bigint) as volume,
    cast(null as bigint) as amount,
    cast(null as timestamptz) as extracted_at
where false
{% endif %}
