{{ config(materialized='external', format='parquet', location=var('candidate_path'), tags=['basic_dataset']) }}

-- 日历行覆盖目标年份，snapshot_date 标识这次抓取对应的发布日。
select
    cast(exchange as varchar) as exchange,
    cast(try_strptime(cal_date, '%Y%m%d') as date) as cal_date,
    try_cast(is_open as integer) as is_open,
    cast(try_strptime(pretrade_date, '%Y%m%d') as date) as pretrade_date,
    cast(extracted_at as timestamptz) as extracted_at,
    cast('{{ var("as_of_date") }}' as date) as snapshot_date
from read_parquet('{{ var("temp_path") }}')
