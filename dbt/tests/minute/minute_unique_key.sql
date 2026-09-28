{{ config(tags=['minute_ingestion']) }}

select secu_code, trade_time, count(*) as duplicate_count
from {{ ref('stg_minute__tushare') }}
group by secu_code, trade_time
having count(*) > 1
