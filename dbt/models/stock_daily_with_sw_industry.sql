-- 以实际股票日线为基准，保留原字段及行粒度，不额外展开交易日历。
select
    d.*,
    m.l1_code as sw_l1_code,
    m.l2_code as sw_l2_code,
    m.l3_code as sw_l3_code
from {{ source('canonical', 'stock_daily') }} as d
-- 匹配完整 Y+N 历史，闭区间包含纳入日和剔除日；未匹配保留日线并输出 NULL。
left join {{ source('canonical', 'index_member_all') }} as m
    on d.ts_code = m.ts_code
    and d.trade_date >= m.in_date
    -- NULL 结束日仍表示无上界；原生无限日期避免 OR 条件退化为块嵌套循环。
    and d.trade_date <= coalesce(m.out_date, date 'infinity')
