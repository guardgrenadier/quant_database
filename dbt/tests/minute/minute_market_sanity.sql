{{ config(tags=['minute_ingestion']) }}

select *
from {{ ref('stg_minute__tushare') }}
where not isfinite(open) or not isfinite(high) or not isfinite(low) or not isfinite(close)
   or volume < 0 or amount < 0
   or high < greatest(open, low, close)
   or low > least(open, high, close)
