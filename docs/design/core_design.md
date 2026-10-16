# 个人量化数据库核心设计 V3.2

**日期：2026-10-15｜状态：核心架构与业务保证已确认**

本文定义系统的心智模型、职责、数据流与技术分工。具体类型、接口、处理策略和实现进展见配套资料。

## 一、项目定位

建设面向个人量化研究的数据平台，将供应商数据转为可追溯、可修订、可靠发布的标准业务事实，供研究与回测使用。

## 二、职责分工

| 模块 | 职责 |
|---|---|
| Runner | 组织依赖检查、范围解析、获取、验收、发布及运行审计 |
| Dataset | 定义 Schema、记录键、依赖、Scope、Publication Unit、写入政策与业务验收规则 |
| Provider Client | 处理供应商通信、鉴权、通用协议、超时与有限重试 |
| Source Adapter | 依据 Scope 组织具体接口请求、检查交付并标准化数据 |
| Validator | 汇总接口与业务检查，确定可发布范围 |
| Publisher | 原子替换合格范围，更新权威数据与成功元数据 |
| Serving | 依据已发布事实及 Coverage 提供跨 Dataset 的组合与派生查询 |

Runner 管机制，Dataset 管业务语义，Adapter 管供应商交付，Client 管通用协议。依赖与范围由 Runner 和 Dataset 确定，发布事务由 Publisher 管理；Serving 的消费转换独立于发布前验收。

## 三、关键概念

| 概念 | 含义 |
|---|---|
| Scope | 本次获取与验收的统一业务范围；区分应获取、合理排除与无法确定 |
| Publication Unit | 可以独立完整验收、原子替换的最小业务范围 |
| Landing | 供应商交付业务数据的暂存阶段 |
| Candidate | 已标准化、等待业务验收的数据，具有明确 Schema，记录可归属到 Unit |
| Canonical | 已通过验收并成功发布的 Dataset 标准业务事实 |
| Coverage | 当前业务范围的权威发布状态 |
| Publication | 成功发布的版本与变更范围，包含合法零行替换和修订 |
| Run Audit | 本次执行的请求、检查、成功或失败结果及所用上游版本 |

例如，stock_basic 的 Unit 是完整快照，suspend_daily 是一个交易日，stock_1min 是一只股票的一个交易日。

控制信息明确表达范围、交付状态、验收结论和版本；业务数据按列式批次处理。Unit 是业务粒度，与文件、表或分块组织独立，一份批次可以包含多个 Unit。

交付结果区分完整集合（包括空集合）、请求失败与已知不完整，并在可定位时归属受影响 Unit。交付完成、验收通过和发布成功分别表达。

## 四、数据流

```text
依赖检查 → 范围解析 → Source Adapter 调用 Provider Client 获取供应商数据
    → Landing → 接口侧验收 → 标准化 Candidate → 业务验收
    → Publisher 原子发布 Canonical 与成功元数据
    → Serving → 研究消费
```

依赖检查确认所需范围已有可用成功版本，Scope 解析基于实际读取的这些版本，请求分片保持同一业务目标。发布前复核上游版本一致性，发生影响判断的变化时重新评估相关范围。

接口侧验收确认供应商按已知契约交付；业务验收确认 Candidate 符合 Dataset 契约。检查结果用于明确合格与阻断范围，Publisher 依据合格范围提交。

## 五、技术分工

| 技术 | 用途 |
|---|---|
| Python | 流程组织、业务契约与供应商接入 |
| Polars | 内存业务批次的列式处理 |
| DuckDB | 权威数据与元数据存储、集合查询和事务发布 |
| dbt | Serving 的 SQL 依赖与转换，默认 View |
| Arrow / Parquet | 列式数据交换、暂存、导出与备份 |

首版采用单个 DuckDB 权威库承载 Canonical 与元数据，并受控串行访问。Ingestion、dbt 和外部研究进程按运行约定错开访问同一文件。

技术分工表达目标路线，具体代码组织、批次形式与验收执行方式由实施方案确定，已落地能力见验证记录。

## 六、可靠性保证

1. **范围与缺失可解释**：获取与验收采用一致的业务 Scope。合理排除与未知范围分别表达，无记录的含义结合 Scope、交付结果和 Coverage 判断。
2. **验收门禁明确**：业务规则属于 Dataset，ERROR 阻断受影响 Unit，WARNING 允许继续并留审计。检查明确影响范围，无法定位的全局错误可阻断整个 Run；只有验收通过的 Unit 可以发布。
3. **原子发布与旧版保护**：按 Unit 完整替换，数据、Coverage 与成功 Publication 同事务更新，发布状态以提交成功为准。失败保留旧成功版本；重跑不无意产生重复记录。一个 Run 可以部分成功，一次事务可以发布多个合格 Unit。
4. **稀疏与空集合合法**：完整验收的零记录 Unit 可以替换旧范围并发布成功版本。发布目标由验收结论明确给出，独立于 Candidate 行是否存在。
5. **事实与派生分工清楚**：Canonical 保存 Dataset 自身的标准业务事实，Serving 提供跨 Dataset 的组合与派生。稀疏 Dataset 只保存供应商报告的记录，消费时结合成功 Coverage 解释缺失。
6. **完整性边界明确**：成功发布表示符合约定的供应商交付与验收契约，不独立保证真实业务绝无遗漏。本次执行失败与当前是否存在可用成功数据分别表达。
7. **版本与过程可追溯**：Scope 所用上游版本对应实际读取的数据，提交前复核一致性；执行结果与成功变更范围可追溯。Landing 与 Candidate 在验收、提交或回滚期间持续可访问。

**配套资料：** [实施说明](ingestion_v3.2_implementation.md)、[stock_basic 正式 Schema](../../src/quant_data/datasets/stock_basic.sql)、[suspend_daily Schema 建议](canonical_suspend_daily_proposal.sql)。
