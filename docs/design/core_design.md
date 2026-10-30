# 个人量化数据库核心架构 V3.2（统一验收修订）

**更新：2026-10-29（明确审计收尾与明细留存期限）｜状态：核心架构与 invariant 已确认**

本文定义项目的核心心智模型、职责与可靠性保证。当前十个切片采用“获取、集中标准化、统一验收、原子发布”的设计；当前接口与阶段见[技术设计与实现边界](ingestion_v3.2_implementation.md)。实现细节调整不改变本文的架构约束；约束变更需另行讨论确认。

## 一、项目定位

建设面向个人量化研究的数据平台，将供应商数据转为可追溯、可修订、可靠发布的标准业务事实，供研究与回测使用。

当前已落地 `stock_basic` 完整快照、SSE `trade_calendar` 自然日日历、`suspend_daily` 停复牌、`stock_st` 每日名单、`index_basic` 目标指数快照、`stock_daily` 未复权股票日线、`index_daily` universe 内指数日线、`stk_limit` 每日涨跌停价格、`bse_mapping` 北交所代码映射和 `stock_1min` 完整分钟研究网格。指数目标为宽基白名单及供应商按类别／发布方报告的申万行业集合。多 Unit、部分成功、合法零行替换、上游版本复核与分钟派生网格已有正式切片及测试；正式 Serving 尚未接入。专属契约见[ST](stock_st_ingestion_proposal.md)、[指数快照](index_basic_ingestion_proposal.md)、[股票日线](stock_daily_ingestion_proposal.md)、[指数日线](index_daily_ingestion_proposal.md)、[涨跌停价格](stk_limit_ingestion_proposal.md)、[BJ 映射](bse_mapping_ingestion_proposal.md)及[股票分钟](stock_1min_ingestion_proposal.md)。

## 二、职责分工

| 模块 | 职责 |
|---|---|
| Runner | 组织依赖检查、范围解析、获取、准备、统一验收、发布及运行审计 |
| Dataset | 定义 Schema、记录键、依赖、Scope、Publication Unit、写入政策与业务验收规则，包括范围覆盖及零行语义 |
| Provider Client | 处理供应商通信、鉴权、通用协议、超时与有限重试 |
| Source Adapter | 依据 Scope 组织请求、记录获取结果与交付证据；集中解释和标准化源数据，输出 Candidate 与转换诊断 |
| Validator | 结合 Scope、获取结果、转换诊断及 Dataset 规则，统一形成显式发布许可 |
| Publisher | 原子替换合格范围，更新权威数据与成功元数据 |
| Serving | 依据已发布事实及 Coverage 提供跨 Dataset 的组合与派生查询 |

Runner 管机制，Dataset 管业务语义，Adapter 管供应商交付与准备，Client 管通用协议。依赖与范围由 Runner 和 Dataset 确定，只有 Validator 决定哪些 Unit 合格，发布事务由 Publisher 管理；Serving 的消费转换独立于发布前验收。

完整性与质量描述检查的关注点，不要求各自对应一个独立流程阶段。源表示和交付规则归 Adapter，标准业务规则归 Dataset；同一业务规则集中定义，避免在多个阶段重复实现。

## 三、关键概念

| 概念 | 含义 |
|---|---|
| Scope | 本次验收与发布的统一业务范围，以及可选供应商请求子集；区分应获取、合理排除与无法确定 |
| Publication Unit | 可以独立完整验收、原子替换的最小业务范围 |
| Landing / Raw | 供应商数据的暂存阶段与原始业务批次，保留源表示及必要请求标签 |
| 获取结果 | 请求、分片、分页、返回情况、获取状态及交付证据，独立于业务数据是否有行 |
| Candidate | 已按 Canonical Schema 标准化、尚未验收的数据，记录可归属到 Unit；结构正确不等于业务合格 |
| 转换诊断 | 字段映射、解析、类型转换及记录归属中的问题，以及受影响范围 |
| 验收结果 | 显式合格 Unit、阻断 Unit 及检查结果，是 Publisher 的发布依据 |
| Canonical | 已通过验收并成功发布的 Dataset 标准业务事实 |
| Coverage | 某业务范围当前是否已有权威成功数据，以及对应的发布版本 |
| Publication | 一次成功发布的版本与变更范围，包含合法零行替换和修订；不等于历史数据快照 |
| Run Audit | 本次执行的请求、检查、成功或失败结果及所用上游版本 |

`stock_basic`、`index_basic` 和 `bse_mapping` 的 Unit 各是一个完整快照，`trade_calendar` 是一个交易所的一个自然日（包含休市日），`suspend_daily`、`stock_st`、`stock_daily` 和 `stk_limit` 各以一个经 SSE 日历确认的交易日为 Unit；`index_daily` 是固定 universe 内一个代码的一个交易日；`stock_1min` 以一只股票的一个交易日为 Unit。Scope.units 始终表示发布范围，Scope.request_units 只表示其中的供应商请求子集；全天停牌分钟因此可以本地交付并发布，而不必请求银河。

控制信息明确表达范围、交付状态、验收结论和版本；业务数据按列式批次处理。Unit 是业务粒度，与文件、表或分块组织独立，一份批次可以包含多个 Unit。

获取完成表示所需请求已完成、已知交付终止条件得到满足，不代表业务范围覆盖符合预期、转换成功或数据合格。请求失败、已知截断及无法确认获取完整的情况必须明确表达；不能从返回行数反推获取状态。

**获取完成、标准化成功、验收通过、发布成功分别表达。** 字段的合法 NULL、完整零行集合、转换失败、合理排除和未知范围不能混为一谈；本次执行失败与当前仍有旧成功数据也可以同时成立。

## 四、数据流

```text
依赖检查 → 范围解析 → Source Adapter 调用 Provider Client 获取供应商数据
    → Landing / Raw 与获取证据
    → 集中准备 Candidate 与转换诊断
    → 统一验收（Scope + 获取结果 + 转换诊断 + Candidate + Dataset 规则）
    → Publisher 原子发布 Canonical 与成功元数据
    → Serving → 研究消费
```

依赖检查确认所需范围已有可用成功版本，Scope 解析基于实际读取的这些版本，请求分片保持同一业务目标。发布前复核上游版本一致性，变化时阻断受影响发布并要求重新评估相关范围。

获取与准备在信息完整的位置检测协议、源格式、范围或转换问题并记录原因；统一的是最终发布裁决，不要求把所有检查延后到最后。准备过程集中清理、解析和映射，复用中间结果，保留 Raw。

统一验收针对业务范围及其获取、转换结果作判断，不能仅检查 Candidate 表。Validator 显式给出合格 Unit；Publisher 据此替换数据，不能从 Candidate 是否有行反推发布目标。

## 五、技术分工

| 技术 | 用途 |
|---|---|
| Python | 流程组织、业务契约与供应商接入 |
| Polars | 内存业务批次的列式处理 |
| DuckDB | 权威数据与元数据存储、集合查询和事务发布 |
| dbt | Serving 的 SQL 依赖与转换，默认 View；正式模型待接入 |
| Arrow / Parquet | 列式数据交换、暂存、导出与备份 |

首版采用单个 DuckDB 权威库承载 Canonical 与元数据，并受控串行访问。Ingestion、dbt 和外部研究进程按运行约定错开访问同一文件。

逻辑职责不要求各自对应独立文件、表、类或进程。准备入口集中源数据处理，Dataset 入口集中标准业务规则；具体接口、暂存形态与批次组织由实施方案确定，不能削弱统一验收与发布职责。

## 六、可靠性保证

1. **范围与缺失可解释**：获取与验收采用一致的业务 Scope。合理排除与未知范围分别表达，无记录的含义结合 Scope、交付结果和 Coverage 判断。
2. **统一验收许可**：只有应处理、获取完成、无阻断性转换错误且满足 Dataset 规则的 Unit 可以发布。ERROR 阻断受影响范围，WARNING 留审计并允许继续；按可信信息定位影响 Unit，无法定位时全局阻断。检查未执行不能默认成通过，最终许可只能由统一验收产生。
3. **原子发布与旧版保护**：按 Unit 完整替换，数据、Coverage、成功 Publication 与成功 Unit 审计同事务更新，发布状态以提交成功为准。失败保留旧成功数据及其版本指针；重跑不无意产生重复记录。一个 Run 可以部分成功，一次事务可以发布多个合格 Unit。
4. **空集合语义明确**：零行不自动成功或失败，须结合 Scope、获取证据、转换诊断及 Dataset 契约判断。完整验收的零记录 Unit 可以替换旧范围并发布成功版本，空表须有明确 Schema；没有 Candidate 不能冒充合法空集合。股票快照要求有正常上市股票，日历每个自然日要求一行，停复牌开市日允许完整零行；合理排除不发布零行版本。发布目标独立于数据行。
5. **事实与派生分工清楚**：Canonical 保存 Dataset 自身的标准业务事实，Serving 提供跨 Dataset 的组合与派生。稀疏 Dataset 只保存供应商报告的记录，消费时结合成功 Coverage 解释缺失。`stock_1min` 是用户单独确认的例外：其 Canonical 是结合源行情、停牌与日线事实构造的完整研究网格，构造规则集中在该 Dataset，不自动扩展到其他 Dataset。
6. **完整性与转换边界明确**：成功发布表示符合约定的供应商交付与验收契约，不独立保证真实业务绝无遗漏或当日数据终态。每条源记录须形成 Candidate 或关联明确问题，不得静默去重、丢行或将非法值转成 NULL 掩盖错误；除 `stock_1min` 明确验收的派生网格外，不得补造事实。字段缺失、合法空值与转换失败须明确区分，错误不得因转换而失去归属或污染其他合格 Unit。
7. **版本与过程可追溯**：Scope 所用上游版本对应实际读取的数据，包括用于排除范围的版本，提交前复核一致性；获取、准备、验收、执行结局及成功变更范围可追溯。Raw 与 Candidate 在验收、提交或回滚期间持续可访问。原始证据保存失败在发布前阻断，可选诊断导出或清理失败独立报告，不撤销已提交发布；追溯不要求全面保存历史行版本。

**文档分工：** 本文维护架构与 invariant；[技术设计与实现边界](ingestion_v3.2_implementation.md)维护当前落地方式和限制；[V3.1.1 归档](archive/core_design_v3.1.1.md)保留设计历史及原 20 条 invariant 的对应关系。

2026-10-29 确认的审计追溯期限：Coverage、Publication、Run 和上游依赖版本长期保留；逐 Unit 执行、请求及检查明细在线保留 60 天，之后允许归档，Run 结束满 365 天后允许过期。阶段性审计不保证保存进程中断前尚未入库的过程；中断收尾只依据已提交状态，保护成功发布，不恢复请求或自动发布。详见 [Meta 审计 Spec](meta_audit_lifecycle_spec.md)。
