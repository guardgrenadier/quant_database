-- 验证所有原始日线键恰有一行结果；重复匹配、丢行和新增键均为 ERROR。
with feature_keys as (
    select ts_code, trade_date, count(*) as rows_per_key
    from {{ ref('stock_daily_with_sw_industry') }}
    group by ts_code, trade_date
)
select
    coalesce(d.ts_code, f.ts_code) as ts_code,
    coalesce(d.trade_date, f.trade_date) as trade_date,
    f.rows_per_key
from {{ source('canonical', 'stock_daily') }} as d
full outer join feature_keys as f using (ts_code, trade_date)
where d.ts_code is null or f.ts_code is null or f.rows_per_key <> 1
