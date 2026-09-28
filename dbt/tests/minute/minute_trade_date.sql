{{ config(tags=['minute_ingestion']) }}

select *
from {{ ref('stg_minute__tushare') }}
where trade_date != date '{{ var("minute_trade_date", "1900-01-01") }}'
