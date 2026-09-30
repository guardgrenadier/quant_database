{{ config(materialized='external', format='parquet', location=var('contract_path'), tags=['minute_contract_findings']) }}

-- 每个源行只在 raw 一侧报告一次转换问题；候选侧只查新引入的范围和主键风险。
with raw as (
    select * from read_parquet('{{ var("temp_path") }}')
), candidate as (
    select * from read_parquet('{{ var("candidate_path") }}')
), plan as (
    select * from read_parquet('{{ var("plan_path") }}')
), raw_bad as (
    select 'raw_contract' as check_name, ts_code, trade_time,
           concat_ws(',',
               case when ts_code is null or ts_code = '' then 'ts_code' end,
               case when trade_time is null or try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') is null then 'trade_time' end,
               case when freq is null or freq != '1MIN' then 'freq' end,
               case when open is not null and (try_cast(open as double) is null or not isfinite(try_cast(open as double))) then 'open' end,
               case when high is not null and (try_cast(high as double) is null or not isfinite(try_cast(high as double))) then 'high' end,
               case when low is not null and (try_cast(low as double) is null or not isfinite(try_cast(low as double))) then 'low' end,
               case when close is not null and (try_cast(close as double) is null or not isfinite(try_cast(close as double))) then 'close' end,
               case when vol is not null and try_cast(vol as bigint) is null then 'vol' end,
               case when amount is not null and (try_cast(amount as decimal(38,18)) is null
                 or try_cast(round(try_cast(amount as decimal(38,18)) * 100, 0) as bigint) is null) then 'amount' end,
               {% if var('historical', false) %}
               case when extracted_at is not null and try_cast(extracted_at as timestamptz) is null then 'extracted_at' end
               {% else %}
               case when extracted_at is null or try_cast(extracted_at as timestamptz) is null then 'extracted_at' end
               {% endif %}
           ) as detail
    from raw
), scope_bad as (
    select 'pipeline_scope' as check_name, c.ts_code,
           cast(c.trade_time as varchar) as trade_time,
           'candidate outside fixed plan/date' as detail
    from candidate c left join plan p
      on c.ts_code = p.ts_code and c.trade_date = p.trade_date
    where p.ts_code is null or c.trade_date != cast('{{ var("as_of_date") }}' as date)
), duplicate_bad as (
    select 'duplicate_key' as check_name, ts_code, cast(trade_time as varchar) as trade_time,
           cast(count(*) as varchar) || ' rows share key' as detail
    from candidate group by ts_code, trade_time having count(*) > 1
), count_bad as (
    select 'row_reconciliation' as check_name, cast(null as varchar) as ts_code,
           cast(null as varchar) as trade_time,
           'temp=' || cast((select count(*) from raw) as varchar)
             || ',candidate=' || cast((select count(*) from candidate) as varchar)
             || ',expected={{ var("expected_rows") }}' as detail
    where (select count(*) from raw) != {{ var('expected_rows') }}
       or (select count(*) from candidate) != {{ var('expected_rows') }}
)
select * from raw_bad where detail != ''
union all select * from scope_bad
union all select * from duplicate_bad
union all select * from count_bad
