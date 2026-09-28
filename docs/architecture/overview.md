# 架构总览

状态：已废止（冻结保留，2026-09-25）。现行设计决策见 [ingestion-design-v2.md](ingestion-design-v2.md)；本文与 V2 冲突处以 V2 为准。

## 项目是什么

单机个人量化研究数据库。从供应商 API 抓取行情数据，落地为可重建的本地数据资产，供研究消费。运行环境为 WSL + Python 3.12 + uv。

## 技术栈

- **DuckDB**：本地分析数据库，保存数据库状态（审计表、dbt 特征表）
- **dbt (dbt-duckdb)**：canonical 建模与数据检查
- **Parquet**：raw 缓存与 canonical 明细的存储格式
- 当前数据源：ProMax 网关（Tushare 兼容协议），实测质量不佳，架构按可换源设计

## 核心原则

1. **DuckDB 管数据库状态，Parquet 管独立数据 artifact。** canonical 明细长期只保留 Parquet 一份，不复制进 DuckDB 实体表；消费走 view。
2. **简单、透明、可重建。** 任一层的产出都能从上游 artifact 重算；失败时保留现场，不静默修复数据、不补行删行凑数。
3. **不提前抽象。** 不引入 PostgreSQL、调度器、数据湖框架、repository 抽象、统一 partition 框架、factor/feature store。能力在真实需求出现时再引入。
4. **但架构缝要提前留对。** pipeline 与具体 API 解耦是真实需求（可能换源、可能多源），缝的定义见 ingestion-framework。
5. **straightforward code > premature abstraction。**

## 文档地图

| 文档 | 内容 |
| --- | --- |
| [ingestion-design-v2.md](ingestion-design-v2.md) | **现行设计决策集**（2026-09-25 起）：数据集拓扑、抓取、raw、记账、canonical、检查、发布与重跑、历史数据、旧资产处置 |
| [ingestion-framework.md](ingestion-framework.md) | 已废止（冻结保留）：V1 ingestion 架构 |
| [minute-data-ingestion.md](minute-data-ingestion.md) | 已废止（冻结保留）：V1 分钟数据设计；其中 2026-09-23 补测结论仍有效 |
| [tushare-pull-strategy.md](tushare-pull-strategy.md) | ProMax 网关日线/基本面等端点的实测行为与拉取策略（端点实测知识仍有效；架构决策以 V2 为准） |
| [../research/](../research/) | 实测报告与供应商手册，是架构决策的证据来源 |
