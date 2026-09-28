{{ config(materialized='view', tags=['minute_view']) }}

{% if var('minute_canonical_glob', '') %}
with canonical as (
    select * from read_parquet('{{ var("minute_canonical_glob") }}', union_by_name=true)
), derived as (
    select
        *,
        cast(trade_time as time) as bar_time,
        cast(strftime(trade_date, '%Y%m%d') as integer) as date_int,
        hour(trade_time) * 100 + minute(trade_time) as time_int
    from canonical
)
select *
from derived
where (bar_time > time '09:30:00' and bar_time <= time '11:30:00')
   or (bar_time >= time '13:01:00' and bar_time <= time '15:00:00')
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
    cast(null as timestamptz) as extracted_at,
    cast(null as time) as bar_time,
    cast(null as integer) as date_int,
    cast(null as integer) as time_int
where false
{% endif %}
