{{ config(materialized='external', format='parquet', location=var('quality_path'), tags=['minute_quality']) }}

-- 只产生告警行；值异常保留在 canonical，不在这里过滤。
with bars as (
    select * from read_parquet('{{ var("candidate_path") }}')
), enriched as (
    select b.*,
           concat_ws(',',
               case when open is null then 'open' end,
               case when high is null then 'high' end,
               case when low is null then 'low' end,
               case when close is null then 'close' end,
               case when vol is null then 'vol' end,
               case when amount is null then 'amount' end
           ) as null_fields
    from bars b
), raw as (
    select * from read_parquet('{{ var("temp_path") }}')
), plan as (
    select * from read_parquet('{{ var("plan_path") }}')
), per_code as (
    select p.ts_code, count(b.ts_code) as bar_count,
           sum(case when b.vol > 0 then b.vol else 0 end) as positive_vol,
           avg(case when b.ts_code is null then null
                    when (cast(b.trade_time as time) > time '09:30:00'
                      and cast(b.trade_time as time) <= time '11:30:00')
                      or (cast(b.trade_time as time) >= time '13:01:00'
                      and cast(b.trade_time as time) <= time '15:00:00')
                    then 0.0 else 1.0 end) as fill_share
    from plan p left join bars b on p.ts_code = b.ts_code group by p.ts_code
), baseline as (
    select median(bar_count) as median_rows, median(fill_share) as median_fill from per_code
), susp as (
    {% if var('suspend_path', '') %}
    select distinct ts_code from read_parquet('{{ var("suspend_path") }}')
    {% else %}
    select cast(null as varchar) as ts_code where false
    {% endif %}
)
select 'ohlc_relation' as check_name, ts_code,
       cast(trade_time as varchar) as trade_time, 'high/low excludes open or close' as detail,
       cast(null as varchar) as null_fields
from bars
where high < greatest(open, close, low) or low > least(open, close, high)
union all
select 'negative_value', b.ts_code, cast(b.trade_time as varchar),
       'negative price, volume, or source amount', cast(null as varchar)
from bars b join raw r on b.ts_code = r.ts_code
    and b.trade_time = try_strptime(r.trade_time, '%Y-%m-%d %H:%M:%S')
where least(b.open, b.high, b.low, b.close) < 0 or b.vol < 0
   or try_cast(r.amount as decimal(38,18)) < 0
{% if var('suspend_path', '') %}
union all
select 'suspension_mismatch', p.ts_code, cast(null as varchar),
       'zero-volume day disagrees with suspend_d', cast(null as varchar)
from per_code p left join susp s on p.ts_code = s.ts_code
where (p.positive_vol = 0 and s.ts_code is null)
   or (p.positive_vol > 0 and s.ts_code is not null)
{% else %}
union all
select 'suspend_d_missing', cast(null as varchar), cast(null as varchar),
       'same-day suspend_d publication is unavailable', cast(null as varchar)
{% endif %}
union all
select 'bar_distribution_outlier', p.ts_code, cast(null as varchar),
       'bar count or fill-share differs from same-day median', cast(null as varchar)
from per_code p cross join baseline b
where abs(p.bar_count - b.median_rows) > 40
   or p.fill_share > greatest(0.25, b.median_fill + 0.10)
union all
select 'close_only_bars', ts_code, cast(trade_time as varchar),
       'close present; open/high/low/vol/amount null', null_fields
from enriched
where close is not null and open is null and high is null and low is null
  and vol is null and amount is null
union all
select 'boundary_bar_incomplete', ts_code, cast(trade_time as varchar),
       '09:30 or 15:00 bar has null source fields', null_fields
from enriched
where cast(trade_time as time) in (time '09:30:00', time '15:00:00')
  and null_fields != ''
union all
select 'out_of_core_bars', ts_code, cast(trade_time as varchar),
       'outside conventional 241 bar times', cast(null as varchar)
from bars
where not ((cast(trade_time as time) >= time '09:30:00'
            and cast(trade_time as time) <= time '11:30:00')
        or (cast(trade_time as time) >= time '13:01:00'
            and cast(trade_time as time) <= time '15:00:00'))
