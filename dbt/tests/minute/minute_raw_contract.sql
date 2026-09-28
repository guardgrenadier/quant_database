{{ config(tags=['minute_ingestion']) }}

{% if var('minute_raw_glob', '') %}
select *
from read_parquet('{{ var("minute_raw_glob") }}', union_by_name=true)
where ts_code is null
   or trade_time is null
   or freq is null
   or open is null or high is null or low is null or close is null
   or vol is null or amount is null or extracted_at is null
   or upper(cast(freq as varchar)) != '1MIN'
   or try_strptime(cast(trade_time as varchar), '%Y-%m-%d %H:%M:%S') is null
   or cast(try_strptime(cast(trade_time as varchar), '%Y-%m-%d %H:%M:%S') as date)
        != date '{{ var("minute_trade_date") }}'
   or try_cast(open as double) is null
   or try_cast(high as double) is null
   or try_cast(low as double) is null
   or try_cast(close as double) is null
   or try_cast(vol as bigint) is null
   or try_cast(amount as double) is null
   or try_cast(amount as double) < 0
   or try_cast(round(try_cast(amount as double), 0) as bigint) is null
   {% if var('minute_codes', []) %}
   or cast(ts_code as varchar) not in (
       {% for code in var('minute_codes') %}'{{ code }}'{% if not loop.last %}, {% endif %}{% endfor %}
   )
   {% endif %}
{% else %}
select 1 where false
{% endif %}
