{% macro require_published_industry_sources() %}
    {# 业务门禁：行业快照和日线所用日期须有成功版本，不能将未接入当成合法缺分类。 #}
    {% set daily = source('canonical', 'stock_daily') %}
    {% set coverage = source('meta', 'coverage') %}
    {% if execute %}
        {% set query %}
            with required as (
                select 'index_member_all' as dataset, 'SNAPSHOT' as unit_key
                union all
                select distinct 'stock_daily', strftime(trade_date, '%Y-%m-%d')
                from {{ daily }}
                union all
                -- 空业务表也必须已接入，不能从零行结果反推上游成功。
                select 'stock_daily', '<no published day>'
                where not exists (
                    select 1 from {{ coverage }}
                    where dataset = 'stock_daily' and status = 'PUBLISHED'
                )
            )
            select r.dataset, count(*) as missing_units, min(r.unit_key) as example_unit
            from required as r
            left join {{ coverage }} as c
                on r.dataset = c.dataset and r.unit_key = c.unit_key
                and c.status = 'PUBLISHED' and c.publication_id is not null
            where c.dataset is null
            group by r.dataset
        {% endset %}
        {% set missing = run_query(query) %}
        {% if missing | length %}
            {{ exceptions.raise_compiler_error('Upstream data not published: ' ~ missing.rows | list) }}
        {% endif %}
    {% endif %}
    -- Hook 只检查已有发布，不写 Canonical/Meta，不自动运行上游。
    select 1
{% endmacro %}
