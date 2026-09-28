{{ config(tags=['minute_view_contract'], severity='error') }}

-- 只测视图映射和时间过滤，不重复检查 canonical 的行情数据。
select 1 from {{ ref('minute_bars') }}
where trade_date = cast('{{ var("as_of_date") }}' as date)
  and (not ((bar_time > time '09:30:00' and bar_time <= time '11:30:00')
        or (bar_time >= time '13:01:00' and bar_time <= time '15:00:00'))
    or date_int != cast(strftime(trade_date, '%Y%m%d') as integer)
    or time_int != hour(trade_time) * 100 + minute(trade_time))
