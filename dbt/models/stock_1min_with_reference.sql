-- Canonical 只存十列分钟事实；连续参考价在消费时按原构造规则派生。
with ordered as (
    select
        m.*,
        last_value(case when m.status = 'TRADED' then m.close end ignore nulls) over (
            partition by m.ts_code, m.trade_date
            order by m.bar_time
            rows between unbounded preceding and current row
        ) as traded_reference
    from {{ source('canonical', 'stock_1min') }} as m
)
select
    o.ts_code,
    o.trade_date,
    o.bar_time,
    o.status,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    o.amount,
    coalesce(o.traded_reference, d.pre_close) as reference_price
from ordered as o
left join {{ source('canonical', 'stock_daily') }} as d
    on o.ts_code = d.ts_code
    and o.trade_date = d.trade_date
