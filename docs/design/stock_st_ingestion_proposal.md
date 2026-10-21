# stock_st ingestion 首版契约与实现

日期：2026-10-21。状态：**按会话确认的首版方案完成实现与验收**。

依据：[核心架构 V3.2](core_design.md)、[当前实施契约](ingestion_v3.2_implementation.md)、[2026-10-20 官方接口探查](../research/tushare-official-st-index-2026-10-20.md)。本稿不改变现有架构、共享接口或已发布 Dataset 的语义。

通用设计事项见[新 Dataset 接入前确认清单](dataset_onboarding_checklist.md)。本文仅记录 stock_st 的接入方案。

## 1. stock_st 保存的业务事实

保存 **Tushare 对某个交易日报告的全市场 ST 名单**。一条记录表示某证券出现在当日名单中。

这是历史每日名单，不是新增/撤销 ST 的事件表，也不是当前 stock_basic 的扩展列。保留历史名称及供应商 type/type_name，不从名称中的 ST、*ST 推导另一套分类，不补正常股票日记录。

发布成功表示满足已确认的供应商交付及验收契约，不证明早期历史绝无遗漏、数据已经终态或该事实在历史当时已经可知。

## 2. 正式契约

### 2.1 Schema 与键

Dataset 和表名分别为 `stock_st`、`canonical.stock_st`，保留供应商全部五字段。

| 字段 | Canonical 类型 | 约束 | 说明 |
|---|---|---|---|
| ts_code | VARCHAR | NOT NULL、非空白 | 保留完整证券标识和前导零，不按当前股票表做外键筛除 |
| name | VARCHAR | 可空；缺失 WARNING | 供应商该条记录的名称，不用最新名称回填 |
| trade_date | DATE | NOT NULL | 中国市场本地业务日期 |
| type | VARCHAR | NOT NULL；首版仅接受 ST | 本轮所有实测均为 ST；新类型阻断并报告，等待确认后扩充契约 |
| type_name | VARCHAR | 可空；缺失 WARNING | 保留标签文本，不硬编码为某个中文名称 |

主键 / Record Key：`(ts_code, trade_date)`。type 作为属性保留，不借增加键字段容纳重复证券日。探查的 4,273 条月度记录在此键上唯一。正式定义见 [Dataset](../../src/quant_data/datasets/stock_st.py) 与 [DDL](../../src/quant_data/datasets/stock_st.sql)。

名称与标签允许缺失，是本 Dataset 采用的业务政策，不是“源样本出现过 NULL”的结论。代码、日期和类型决定名单事实；描述缺失留告警。五个字段本身必须全部交付，缺列不能补 NULL。

### 2.2 输入、依赖与 Scope

- CLI 使用 `ingest stock_st --start-date YYYYMMDD --end-date YYYYMMDD`，显式闭区间；不提供股票、类型或交易所子集过滤。
- 日期必须有效且起止有序，拒绝晚于上海当日的目标日期；允许显式请求当日，但发布只表示抓取时点的名单，不加未经实测的“9:20 后保证终态”规则。
- 依赖同一数据库中目标区间**每个自然日**的成功 `trade_calendar / SSE:YYYY-MM-DD` 版本，包括将被排除的休市日。
- 同一事务快照内固定版本并读取实际日历：开市日 IN_SCOPE，休市日 OUT_OF_SCOPE；成功 Coverage 下缺少实际日历事实为 UNKNOWN。
- 缺依赖在请求前阻断，不自动触发上游 ingestion；提交前复核全部固定版本。
- 不依赖 stock_basic。历史名单不因证券已经退市或当前快照没有该证券而删行。
- 官方声明的历史起点为 2000-01-01。更早的目标开市日由 Adapter 报 INCOMPLETE / ERROR，不请求、不发布；对起点之后的成功响应也不承诺历史无遗漏。不能把某个首次测到非空的日期硬定义成完整覆盖起点。

### 2.3 Unit、写入与失败

Unit 为一个交易日的全市场名单，标识为 ISO 日期 `YYYY-MM-DD`。

每个合格日完整替换该日旧集合，其他日期不受影响；相同内容重跑也生成新 Publication。某日失败保护该日旧 Canonical / Coverage，其他合格日可发布，Run 为 PARTIAL。多个合格日仍进入现有的一次发布事务，事务失败整体回滚。

休市日不请求、不生成 stock_st 成功 Coverage，也不借本次排除决策自动清除已有事实；上游修订后的下游失效传播仍不在当前内核能力内。

### 2.4 零行政策：首版阻断

**开市日 Candidate 为零行时，业务验收 ERROR，不清除旧名单、不发布空版本。** 首次运行也不能写成成功 Coverage。

理由是已有 2000 年开市日期空响应及官方早期历史缺口说明；当前证据不能自动区分“真的没有 ST”与“供应商没有覆盖”。这是一项明确的首版发布限制，不是“每个交易日必有 ST”的事实断言。

即使因此阻断，HTTP 请求及终止条件确实完成时仍记 COMPLETE，构造有五列明确 Schema 的空 Candidate；不将业务验收失败伪装成请求失败或无 Candidate。Raw、请求证据和检查原因保留。

代价：若未来出现真实无 ST 的开市日，首版也会阻断。后续如需接受“供应商本次未报告任何记录”的空集合并允许清旧，须补充证据并另行确认契约变更。

## 3. 获取、准备和检查的分工

### 3.1 Adapter.fetch：获取及源交付

每个 IN_SCOPE 日只传 `trade_date`，显式请求五字段，串行间隔沿用 1.3 秒；Client 负责现有超时和有限重试。每次尝试即时保存脱敏信封，失败继续其他日期。

首版单日返回必须 `has_more=false` 且行数低于已知 1,000 行上限；缺失/true 标记或触顶为 INCOMPLETE。首版不加自动分页，实际需要单日分页时再采用已验证的 stock_st 分页行为。此限制防止截断数据被当成完整名单发布。

按返回字段名重排；Raw 严格保留五列字符串/NULL，并加可信请求日期标签。缺列或非法 JSON 类型记录准备 ERROR，不改写确已完成的请求状态；解码失败时原值仍在信封。

### 3.2 Adapter.prepare：集中标准化与诊断

文本去首尾空白，空串转 NULL；原 Raw 不修改。源 trade_date 必须与可信请求日期的 YYYYMMDD 完全匹配，再严格转 DATE。

错日期、非法日期或 NULL 日期都按请求标签阻断该日，即使错误值恰好是本次另一个目标日，也不能污染那个日期。无法转换的行有明确诊断且留在 Raw，不用默认值、去重或静默丢行掩盖问题。

Candidate 只保留正式五列；代码、名称和标签正文不重写。

### 3.3 Dataset.validate_candidate：标准业务规则

| 级别 | 规则 | 影响范围 |
|---|---|---|
| ERROR | ts_code、trade_date、type 必填；代码非空白 | 对应交易日；无法可靠归属时全局 |
| ERROR | type 不属于首版 ST 契约 | 对应交易日 |
| ERROR | `(ts_code, trade_date)` 重复，包括同键不同名称/标签 | 对应交易日，不去重 |
| ERROR | 纳入范围的某日零行 | 对应交易日，按首版非空发布政策 |
| WARNING | name / type_name 缺失 | 对应交易日，允许发布 |

不设置固定 ST 总数、不强制名称包含 ST、不要求 type_name 等于固定中文文本，也不实现缺乏依据的数量变动阈值。

通用 Validator 继续负责 COMPLETE 门禁、Candidate Schema、记录归属、ERROR/WARNING 影响汇总和最终许可；Publisher 继续负责原子替换与成功元数据。

## 4. 实现范围与验收

已新增 `datasets/stock_st.py`、`datasets/stock_st.sql`、`sources/tushare/stock_st.py`、`tests/test_stock_st.py`。CLI 已显式装配 stock_st，README、核心架构实现清单与当前实施边界已同步。

复用当前 Dataset / SourceAdapter Protocol、Runner、Client、Validator、Publisher、审计和产物接口。未新增生产控制模型、共享接口或数据集注册框架；既有 Dataset 和冻结架构语义保持不变。

必要验收：

1. 同库发布日历后，跨开闭市区间只请求开市日；固定版本包含休市日，缺依赖在请求前阻断。
2. 正常获取五字段；Raw 日期/文本原值保留，Candidate 标准化符合 Schema，日范围完整替换且范围外数据保留。
3. 零行、缺列、严格类型错误、错日、未知类型、重复键、请求失败、has_more 风险及触顶均有可解释阻断，保护旧成功数据。
4. 描述缺失只告警；单日错误隔离，其他日可发布；日历版本变化时阻断发布并要求重算 Scope。
5. 使用已有真实 stock_st 响应重放，核对行数与字段；在独立验证库完成小区间真实 CLI 验收并保留证据，不写默认权威库。

事务回滚、通用证据故障和诊断清理机制已有内核测试；本切片验证接线及关键业务 invariant，不重复复制全部机制测试。

### 4.1 验收结果

新增 9 项 ST 测试，覆盖上述场景；全套 65 项测试中 64 项通过，1 项旧 stock_basic 真实快照重放因本地样本缺失跳过。stock_st 的已有真实响应重放已执行，两个交易日共 402 条五字段事实与发布结果逐条一致。

2026-10-21 使用正式 CLI 与官方 Client，在独立库完成真实验证：

| 执行 | 结果 |
|---|---|
| 发布 2026-10-16–10-19 的 SSE 日历 | 4 个自然日成功，包含两个休市日 |
| stock_st 同区间首次获取 | 仅请求两个开市日，各 201 条，共 402 条；生成两个成功版本 |
| stock_st 相同区间重跑 | 仍为 402 条，逐字段一致；生成两个新版本，无重复累加 |
| 发布 2000-01-04 日历后获取 ST 名单 | 获取 COMPLETE、五列空 Candidate；`st.nonempty` 阻断，CLI 返回 1，Coverage 为 FAILED，无 Publication |
| 零行失败后核对已有成功数据 | 两个已发布交易日及其成功版本未被该失败改写 |

四次成功执行均保留完整阶段产物；预期失败的零行执行保留证据与诊断，`artifacts_complete=false`。共核对 222 个清单文件 SHA-256；成功 ST 请求的原始五字段与实际 Canonical 一致。

机器可读执行摘要见 [stock-st-ingestion-summary-2026-10-21.json](../research/stock-st-ingestion-summary-2026-10-21.json)。独立库及全部原始产物位于仓库本地：

```text
data/probes/stock-st-ingestion-validation-20261021T212141505980/
  warehouse.duckdb
  summary.json
  landing/<run_id>/
```

未写入默认权威库。验证库、Raw、Candidate 和请求信封均为本地证据，不入 Git。

### 4.2 运行方式

先在同一库发布完整区间的日历，再显式执行 ST 名单：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261016 --end-date 20261019 --db data/warehouse_v3_2.duckdb
uv run --frozen python -m quant_data ingest stock_st \
  --start-date 20261016 --end-date 20261019 --db data/warehouse_v3_2.duckdb --keep-artifacts
```

首版范围不包含全历史批量下载、自动分页、调度、自动近期重抓、ST/*ST 派生分类、变更事件生成或正式 Serving 模型。
