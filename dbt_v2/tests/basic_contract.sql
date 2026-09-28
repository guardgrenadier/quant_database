{{ config(tags=['basic_contract']) }}

-- 三个数据集只检查转换后新引入的类型、主键与行数风险。
{% set dataset = var('dataset') %}
with candidate as (
    select * from read_parquet('{{ var("candidate_path") }}')
), violations as (
    {% if dataset == 'stock_basic' %}
    select 1 from candidate
    where ts_code is null or ts_code = '' or list_status is null
       or list_status not in ('L','D','P','G','UN')
       or (list_status in ('L','D','P') and list_date is null)
       or (list_status = 'D' and delist_date is null)
       or snapshot_date != cast('{{ var("as_of_date") }}' as date) or extracted_at is null
    union all
    select 1 from candidate group by ts_code having count(*) > 1
    {% elif dataset == 'trade_cal' %}
    select 1 from candidate
    where exchange is null or exchange != 'SSE' or cal_date is null
       or is_open is null or is_open not in (0,1)
       or snapshot_date != cast('{{ var("as_of_date") }}' as date) or extracted_at is null
    union all
    select 1 from candidate group by exchange, cal_date having count(*) > 1
    {% elif dataset == 'suspend_d' %}
    select 1 from candidate
    where ts_code is null or ts_code = '' or trade_date is null
       or trade_date != cast('{{ var("as_of_date") }}' as date)
       or suspend_type is null or suspend_type != 'S' or extracted_at is null
    union all
    select 1 from candidate group by ts_code, trade_date having count(*) > 1
    {% endif %}
)
select 1 from violations
union all
select 1 where (select count(*) from candidate) != {{ var('expected_rows') }}
