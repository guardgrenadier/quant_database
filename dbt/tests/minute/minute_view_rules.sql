{{ config(tags=['minute_view']) }}

select *
from {{ ref('minute_bars') }}
where bar_time = time '09:30:00'
   or (bar_time >= time '11:31:00' and bar_time <= time '12:59:59')
   or date_int != cast(strftime(trade_date, '%Y%m%d') as integer)
   or time_int != hour(trade_time) * 100 + minute(trade_time)
