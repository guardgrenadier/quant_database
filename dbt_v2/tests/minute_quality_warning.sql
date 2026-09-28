{{ config(tags=['minute_quality_warning'], severity='warn') }}

-- 语义异常只产生 dbt warning；publisher 从同一结果文件持久化详情。
select * from read_parquet('{{ var("quality_path") }}')
