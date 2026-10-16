# V3.2 stock_basic 首片实施与公开接口

日期：2026-10-15。依据：用户确认的 V3.2 重构计划；核心架构见 [core_design.md](core_design.md)。

## 模型与接口

`core/models.py` 定义 frozen、slots dataclass。Scope 的 Unit 集合互斥，控制集合使用 tuple；`Check.units=None` 明确表示全局影响。`Delivery.frame` 是独立的原始 Polars 表；完整空交付仍有 Schema，失败可以无表。序列化控制模型明确拒绝 DataFrame，不采用递归 asdict 展开业务数据。

| 对象 | 接口与数据 |
|---|---|
| StockBasicDataset | name/table/schema/ddl/record_key；dependencies、resolve_scope、unit_key_sql、validate_candidate |
| TushareClient | 私有 token、timeout=40、attempts=3；query(api_name, params, fields, request_id, on_attempt) 返回完整信封 |
| TushareStockBasicAdapter | 构造时注入 Client；fetch → Delivery；validate_source → Check tuple；normalize → Candidate 或 None 与 Check tuple |
| Dataset / SourceAdapter Protocol | 声明上述共享类型接口；具体类通过组合使用，不要求继承基类 |
| Runner | run(con, dataset, adapter, params, RunOptions) → RunResult |

RunOptions 默认 landing_root=`data/landing`、keep_artifacts=False。RunResult 包含运行、Unit、成功版本、全局异常及独立的 artifact_dir/artifact_status/artifacts_complete/export_errors。

## 数据与事务

Client 原始信封 → 严格 Arrow String 构造 → Polars Raw 表 → Polars 标准化 → Candidate → DuckDB SQL 验收 → 集合式发布。JSON 在每次尝试结束即时旁路留证据，主处理流程不再读取 JSON 文件组表。

Raw 为 17 列供应商字符串字段及两列请求标签；Candidate 为原 Canonical 的 17 列。空白转 NULL，日期先检查 ASCII 八位格式再严格解析；数字代码、七位日期和非法历法日期均明确阻断。特殊历史代码、UN 空上市日期、L 未来退市日期及 symbol 映射差异保持原语义。

Dataset.unit_key_sql(alias) 将记录映射到逻辑 Unit；stock_basic 返回 SNAPSHOT 常量。Scope/合格 Unit 是关系，内核通过 JOIN 判断归属及筛选。Publisher 使用集合 DELETE/INSERT、GROUP BY 和合格 Unit LEFT JOIN，一次写入成功版本，再批量更新 Coverage 与 Unit 审计。零行 Unit 同样替换旧范围；版本使用真实 RETURNING 结果，编号允许有空隙。

版本查询与 Scope 读取共用事务快照，发布事务内批量复核。成功数据、Publication、Coverage 和成功 Unit 审计一起提交或回滚。

## Meta 与产物

| 表 | 归属与内容 |
|---|---|
| meta.run | 参数、起止时间、状态、计数摘要、全局错误与产物状态 |
| meta.run_unit | Scope 决策与原因、交付/执行状态、阶段、请求/检查引用、行数和成功版本 |
| meta.run_dependency | 实际使用的上游成功版本 |
| meta.request_attempt | 每个逻辑请求的每次尝试，PK=(run_id,request_id,attempt)；原始响应在证据文件 |
| meta.validation_check | 每项检查存一次，PK=(run_id,check_id)；NULL 影响集合表示全局 |
| meta.publication | 成功范围变更及行数，包括零行替换 |
| meta.coverage | 当前权威版本；新运行失败不覆盖已有 PUBLISHED |

元数据按阶段使用列式关系批量写入；不使用逐 Unit SELECT/UPSERT 循环或 executemany。summary 只保存 scope_counts、unit_counts、request_attempts 和 check_counts。Unit 记录细节通过引用关联，不重复展开公共信息。

产物位于 `<landing-dir>/<run_id>/`：默认成功清理，失败或部分成功保留。开启 `--keep-artifacts` 时，Runner 显式导出 scope、交付、Raw/Candidate Parquet/CSV/Schema、验收、发布和结果；stages 下保留七张 Meta 表及发布后 Canonical。README 提供导航，manifest 包含完整性和 SHA-256。诊断导出或清理失败独立报告，不撤销已提交数据；临时关系在发布/回滚和导出之后释放。

## 运行与边界

```bash
uv sync --frozen
uv run --frozen python -m quant_data ingest stock_basic --db data/warehouse_v3_2.duckdb
uv run --frozen python -m quant_data ingest stock_basic --db data/probes/new/warehouse.duckdb --keep-artifacts
uv run --frozen python -m unittest discover -s tests -v
```

默认数据库路径仍为 data/warehouse.duckdb；旧 Meta 明确提示新 --db 路径，不迁移、不删除旧数据。仅 stock_basic 为正式 Dataset，仍不自动分页、恢复或调度；多 Unit 和零行能力由通用机制测试关系验证。新增与重写函数保留中文函数说明和关键块注释。
