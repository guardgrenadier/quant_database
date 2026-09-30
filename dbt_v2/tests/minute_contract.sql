{{ config(tags=['minute_contract'], severity='error') }}

-- findings 模型先将错误行落盘；此测试只负责让 error 阻断发布。
select * from read_parquet('{{ var("contract_path") }}')
