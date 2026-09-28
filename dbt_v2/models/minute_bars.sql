{{ config(materialized='view', tags=['minute_view']) }}

-- 默认消费视图只保留约定的连续竞价时间窗，canonical 全量不删行。
with canonical as (
    select * from read_parquet('{{ var("minute_canonical_glob") }}')
), mapped as (
    select ts_code as secu_code, trade_time, trade_date,
           open, high, low, close, vol as volume, amount, extracted_at,
           cast(trade_time as time) as bar_time,
           cast(strftime(trade_date, '%Y%m%d') as integer) as date_int,
           hour(trade_time) * 100 + minute(trade_time) as time_int
    from canonical
)
select * from mapped
where (bar_time > time '09:30:00' and bar_time <= time '11:30:00')
   or (bar_time >= time '13:01:00' and bar_time <= time '15:00:00')
