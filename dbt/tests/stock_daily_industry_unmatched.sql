{{ config(severity='warn') }}

-- 归属断档可以合法存在：逐行诊断未匹配数量，不填充、不删除，也不要求三列非空。
select ts_code, trade_date
from {{ ref('stock_daily_with_sw_industry') }}
where sw_l3_code is null
