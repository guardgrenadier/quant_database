{{ config(tags=['minute_ingestion']) }}

select count(*) as actual_rows, {{ var('minute_expected_rows', 0) }}::bigint as expected_rows
from {{ ref('stg_minute__tushare') }}
having count(*) != {{ var('minute_expected_rows', 0) }}::bigint
