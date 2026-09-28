{{ config(tags=['minute_ingestion']) }}

select *
from {{ ref('stg_minute__tushare') }}
where typeof(secu_code) != 'VARCHAR'
   or typeof(trade_time) != 'TIMESTAMP'
   or typeof(trade_date) != 'DATE'
   or typeof(open) != 'DOUBLE'
   or typeof(high) != 'DOUBLE'
   or typeof(low) != 'DOUBLE'
   or typeof(close) != 'DOUBLE'
   or typeof(volume) != 'BIGINT'
   or typeof(amount) != 'BIGINT'
   or typeof(extracted_at) != 'TIMESTAMP WITH TIME ZONE'
