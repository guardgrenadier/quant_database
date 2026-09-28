{{ config(tags=['minute_contract'], severity='error') }}

-- 检查 temp→canonical 新引入的类型、单位、主键、计划范围与行数风险。
with raw as (
    select * from read_parquet('{{ var("temp_path") }}')
), candidate as (
    select * from read_parquet('{{ var("candidate_path") }}')
), plan as (
    select * from read_parquet('{{ var("plan_path") }}')
), violations as (
    select 1 from raw
    where ts_code is null or ts_code = '' or trade_time is null or freq is null or freq != '1MIN'
       or open is null or high is null or low is null or close is null
       or vol is null or amount is null or extracted_at is null
       or try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') is null
       or try_cast(open as double) is null or try_cast(high as double) is null
       or try_cast(low as double) is null or try_cast(close as double) is null
       or try_cast(vol as bigint) is null or try_cast(amount as decimal(38,18)) is null
       or try_cast(round(try_cast(amount as decimal(38,18)) * 100, 0) as bigint) is null
       or try_cast(extracted_at as timestamptz) is null
    union all
    select 1 from candidate
    where ts_code is null or trade_time is null or trade_date is null
       or open is null or high is null or low is null or close is null
       or vol is null or amount is null or extracted_at is null
       or not isfinite(open) or not isfinite(high) or not isfinite(low) or not isfinite(close)
    union all
    select 1 from candidate c left join plan p
      on c.ts_code = p.ts_code and c.trade_date = p.trade_date
    where p.ts_code is null
    union all
    select 1 from candidate group by ts_code, trade_time having count(*) > 1
)
select 1 from violations
union all
select 1 where (select count(*) from raw) != {{ var('expected_rows') }}
union all
select 1 where (select count(*) from candidate) != {{ var('expected_rows') }}
