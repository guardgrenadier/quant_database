# 个人量化数据库架构冻结稿 V3.1.1

> 历史归档：现行架构以 [核心设计 V3.2](../core_design.md) 为准。本文保留旧版表述，文末附原 invariant 的 V3.2 语义对应关系。

**日期：2026-10-13**

**状态：第一版核心架构冻结；三个代表性 Dataset 的伪代码与模拟数据传递验证已通过**

**技术栈：Python + DuckDB + dbt + Parquet**

> **版本说明（V3.1 → V3.1.1）：** 本文保留 V3.1 原有架构说明与职责边界，仅增补第十章的数据形态及传递约束、第十三章相应 Invariants，并更新第十五章验证进度。具体 Python 类、固定函数签名、供应商数据字段和验收阈值仍不冻结。

---

## 一、项目定位与设计目标

建设面向个人量化研究的数据平台，支持股票基础信息、日频/分钟行情、停复牌数据、指数权重等多类 Dataset 的可靠获取、持续更新、历史修订和研究使用。

### 核心设计原则

1. **通用机制与业务语义分离**：Runner 管流程，Dataset 管业务规则，Source Adapter 管供应商接口。
2. **可靠发布优先**：获取、校验或修订失败不得破坏已经成功发布的数据。
3. **Canonical 保存标准化业务事实**：不为下游便利而主动引入无关依赖、补齐派生事实。
4. **完整性以供应商交付契约为边界**：不承诺独立证明所有业务记录绝对完整。
5. **保持简单、可审查**：优先采用已有组件和普通 Python 函数，避免过早建设复杂框架。
6. **逻辑职责与物理存储解耦**：Dataset、Scope 和 Publication Unit 不直接决定数据库文件或 Parquet 分区粒度。

## 二、总体架构

```text
                    Ingestion Runner
                           │
                    Dependency Check
                           │
                    Scope Resolution
                    (Dataset-specific)
                           │
                           ▼
                 Source × Dataset Adapter
                           │
                    Provider Client
                           │
                     Supplier API
                           │
                           ▼
                        Landing
                           │
                 Source-side Validation
                           │
                           ▼
                  Canonical Candidate
                           │
                Canonical-side Validation
                           │
                           ▼
                        Publisher
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
           Canonical                Metadata
         (DuckDB tables)      Coverage / Publication
                                    / Audit
              │
              ▼
         dbt Serving
       (Views by default)
              │
              ▼
       Quant Research / Backtest
```

图示为逻辑执行流程，不意味着每个阶段必须对应独立进程、文件、表或类。

Provider Client 在请求路径中由 Adapter 调用，供应商响应经 Adapter 处理后进入 Landing 和后续 Validation。

## 三、各组件职责

| 组件 | 所属层 | 主要职责 |
|---|---|---|
| Runner | 通用框架 | 组织执行、依赖检查、阶段调用、失败及重试控制 |
| Dependency Check | Runner | 检查上游 Dataset 指定范围的已发布数据是否可用 |
| Scope Resolver | Dataset | 确定本次需要获取和验收的目标数据范围 |
| Provider Client | Source | 通用协议、鉴权、请求发送、错误处理和有限重试 |
| Source × Dataset Adapter | 接口适配 | 请求策略、接口交付检查、字段标准化 |
| Landing | Ingestion | 暂存供应商返回的数据 |
| Validator | 通用机制 + 专属规则 | 执行并汇总 ERROR / WARNING |
| Publisher | 通用框架 | 按 Publication Unit 原子发布 |
| Coverage / Audit | 元数据 | 记录覆盖状态、执行结果、成功变更及版本 |
| dbt Serving | 转换层 | 跨 Dataset JOIN、派生与研究消费视图 |

**禁止跨层侵入：**

- Runner 不包含具体 Dataset 的业务规则。
- Provider Client 不理解 Canonical、Scope 或 Dataset 完整性。
- Adapter 不负责跨 Dataset 依赖解析或权威发布。
- Dataset 规则不直接操作 Publisher 的事务细节。
- dbt 不参与 Canonical 发布前的权威验收。

## 四、Dataset Contract

每个 Dataset 定义自身的数据契约。

### 必须明确的内容

| 契约 | 含义 |
|---|---|
| Canonical Schema | 字段、类型、单位、时间与空值语义 |
| Record Key | 记录业务唯一性 |
| Scope | 本次目标数据范围及解析方式 |
| Dependencies | Scope 等处理环节需要的上游 Dataset |
| Publication Unit | 最小独立完整验收与原子发布范围 |
| Write Policy | 首次写入、重复执行、历史替换的规则 |
| Validation Rules | 接口侧与 Canonical 侧的验收规则 |

Contract 是逻辑概念，不强制全部写成声明式配置。

简单 Dataset 可以使用默认 Scope，不必实现专属 Resolver。

### Publication Unit

**第一版不再单独引入 Acceptance Unit。**

Publication Unit 定义为：

> Dataset 中能够被独立完整验收，并且能够原子替换与发布的最小业务范围。

示例：

| Dataset | Publication Unit |
|---|---|
| `stock_basic` | 完整快照 |
| `suspend_daily` | 一个交易日 |
| `stock_1min` | 一个股票 × 一个交易日 |
| `stock_1d` | 待结合接口确定，候选为股票日 |
| `index_weight` | 待结合接口确定，候选为指数 × 生效日期的完整成分集合 |

**重要约束：**

- 一个 Run 可以包含多个 Publication Unit。
- 一个 Run 可以部分成功。
- 一个 Publication Unit 必须完整通过阻断性验收才能发布。
- Validation 可以批量、跨 Unit 执行，但必须明确检查失败影响哪些 Unit。
- 无法定位影响范围的全局阻断错误，可以阻断整个 Run。
- 一次 DuckDB 事务可以批量提交多个 Publication Unit，不要求每个 Unit 独立开启事务。

## 五、Dependency Check 与 Scope Resolution

### 5.1 Dependency Check

归属于通用 Runner。

检查上游 Dataset 在所需范围内是否具备可用的成功发布版本。

不以“上游任务刚刚执行成功”为必要条件。

首版只要求检查依赖状态并阻断不满足的执行，不实现复杂 DAG 调度或自动触发上游任务。

### 5.2 Scope Resolution

归属于 Dataset，由 Runner 在 Source Adapter 之前调用。

负责利用输入参数及必要的上游 Canonical 数据，确定本次需要获取和验收的目标范围。

第一版只保留一个 Scope 概念，不独立区分 Request Scope / Expected Scope。

- Adapter 使用 Scope 组织请求。
- Validation 使用同一个 Scope 判断目标范围是否完成验收。
- Adapter 内部可以按接口限制拆分请求，但不改变 Dataset 目标范围。

Scope 决策需要能够区分：

- `IN_SCOPE`：应当获取并验收。
- `OUT_OF_SCOPE`：合理排除。
- `UNKNOWN`：无法可靠判定，不能视作正常缺失。

### 5.3 示例：股票分钟行情

```text
stock_basic ─────────────┐
                         │
suspend_daily(T) ────────┼── Scope Resolver
                         │         │
trade_calendar ──────────┘         ▼
                           当日应获取股票集合
                                   │
                                   ▼
                            stock_1min Adapter
```

Scope Resolver 根据上市/退市日期、交易日历和已确认的全天停牌信息，确定当日应获取的股票集合。

盘中停牌、历史上市状态边界及无成交分钟的处理方式由具体 Dataset 规则确定。

### 5.4 上游版本一致性

Scope Resolution 必须基于一组确定的上游已发布版本。

执行时：

1. Dependency Check 确定可用的上游 Publication。
2. Scope Resolver 基于相应版本的数据计算 Scope。
3. Audit 记录本次 Scope 使用的上游 Publication ID。
4. Publisher 在正式提交前再次检查相关上游版本是否变化。
5. 如果发生影响一致性判断的版本变化，阻断相应发布并要求重新评估。

版本记录必须与实际读取的数据对应，不能仅记录版本号，却在不受控情况下读取其他版本。

暂不实现自动依赖失效传播 DAG。

## 六、Provider Client 与 Source × Dataset Adapter

采用**薄 Provider Client + 专属 Adapter**。

### 6.1 Provider Client

只负责供应商通用交互能力：

- 配置、鉴权。
- HTTP / SDK 调用。
- 响应协议解析。
- 通用错误码处理。
- 超时及有限重试。
- 必要的请求日志。

不负责：

- Dataset Scope。
- 全量获取、接口专属分页或切片。
- Canonical Schema。
- Dataset 完整性与业务验收。
- 数据入库。

### 6.2 Source × Dataset Adapter

按供应商与 Dataset 的组合组织具体接口逻辑。

负责：

- 将 Scope 转换成接口请求参数。
- 根据接口限制切片、分批请求。
- 检查接口特定交付契约。
- 处理接口字段及类型差异。
- 输出标准化 Candidate 和接口侧检查结果。

不要求一个 API endpoint 对应一个 Adapter 文件。一个 Dataset 可以使用多个供应商端点完成交付。

### 6.3 Source-side Validation 归属

- Provider Client：供应商通用通信及协议有效性。
- Adapter：具体接口的参数、返回结构、行数限制、截断风险及交付语义。
- Dataset：Canonical 数据应满足的标准化业务约束。

Client 中的网络或协议错误可直接以请求异常表示，不要求全部包装为数据质量规则。

不建设额外的通用分页、RequestStrategy 或复杂 Provider SDK 框架。

## 七、Validation

采用**接口侧 + Canonical 侧**两个执行位置。

不强制将检查重新划分为独立的完整性框架和质量框架。

统一使用：

- `ERROR`：阻断受影响 Publication Unit。
- `WARNING`：允许继续，记录异常。

### 7.1 Source-side Validation

确认接口交付是否满足已知契约。

例如：

- 返回范围与请求是否一致。
- 是否存在已知行数截断风险。
- 必需字段是否存在。
- 请求是否全部完成。
- 返回零记录是否符合接口契约。

### 7.2 Canonical-side Validation

确认标准化 Candidate 满足 Dataset Contract。

例如：

- Schema 与数据类型。
- Record Key 唯一性。
- 必要字段非空。
- OHLC 价格关系。
- 数量与金额约束。
- Scope 覆盖。
- Dataset 专属时间规则。

**执行方式：Python 组织 + DuckDB SQL 计算。**

Python 负责调用规则、汇总结果、识别失败影响范围；DuckDB SQL 执行大型数据集的向量化、集合级检查。

不将百万行数据转为 Python 对象逐行校验。

dbt Tests 用于已发布数据和 Serving/Mart 的独立验证，不作为 Canonical 发布门禁。

### 7.3 完整性的保证边界

第一版以**供应商交付契约的完整性**为保证边界。

Adapter 确认已知请求、协议及接口限制得到满足，且不存在已发现的交付失败或截断。

不要求系统独立证明供应商没有遗漏任何真实业务事件。

接口请求成功不等于数据完整；对于无法独立验证的部分，需要明确依赖供应商的完整返回约定。

Coverage 的 `PUBLISHED` 表示通过项目约定的验收标准，不代表绝对业务正确。

## 八、Canonical 与 Publisher

Canonical 保存 Dataset 自身的**标准化、权威业务事实**。

允许必要的字段映射、类型转换、单位统一及语义标准化。

原则上不为其他 Dataset 的便利而增加无关依赖、补齐跨 Dataset 的派生记录。

### 8.1 Publisher Invariants

1. 按 Publication Unit 原子替换和发布。
2. 只有通过阻断性 Validation 的 Unit 可以发布。
3. 允许一个 Run 部分成功。
4. 失败不得破坏已有成功发布的版本。
5. Canonical 修改与对应 Coverage / Publication 更新必须事务一致。
6. 重复运行不得无意生成重复记录。
7. 合法的零记录结果可以成功发布。
8. 任何 Unit 的发布状态不得先于其实际事务成功提交。

### 8.2 稀疏数据

例如 `suspend_daily`：

Canonical 只存供应商报告的停复牌记录，不为当天正常交易的股票补记录。

Coverage 按交易日确认该范围是否已经完整发布。

只有在对应 Coverage 成功且接口具备完整列举语义时，才允许根据无记录推断供应商未报告相应事件。

零记录发布可以是完整且合法的结果。

## 九、Coverage、Run Audit 与 Change Audit

三者职责分开：

### Coverage

描述**当前权威数据范围的发布状态**。

需要能够区分：

- 应有数据且已发布。
- 本次目标范围尚未完成。
- 合理排除在 Scope 之外。
- 获取或验收失败。
- 状态无法确定。

不要求所有状态都以逐股票日记录的方式持久化；允许利用 Scope 规则、已有元数据和审计解释缺失。

### Run Audit

记录一次 Ingestion 执行的请求、异常、检查结果、成功和失败单位。

本次运行失败不意味着当前 Canonical 没有有效数据：历史修订失败时旧版本可以继续保留。

Scope 依赖的上游 Publication 版本也在此类审计中记录，可通过 Run ID 关联。

### Canonical Publication / Change Audit

**第一版仅为未来可能支持 dbt 增量物化保留最小的成功变更记录。**

建议逻辑字段：

| 字段 | 含义 |
|---|---|
| `publication_id` | 成功发布的唯一版本，支持可靠的提交顺序或变更游标 |
| `dataset` | 被修改的 Dataset |
| `scope` | 被成功发布或替换的业务范围 |
| `published_at` | 成功发布时间 |
| `run_id` | 对应的 Ingestion Run |

应能记录合法零记录替换及历史修订。

重点是保存**哪些范围发生成功发布变更**，不要求保存每一行的前后镜像。

不实现通用 CDC、自动下游失效传播或完整数据血缘系统。

## 十、DuckDB 物理存储

### 10.1 首版使用单个权威数据库文件

```text
data/
└── warehouse.duckdb
    ├── canonical
    │   ├── stock_basic
    │   ├── suspend_daily
    │   ├── stock_1min
    │   └── ...
    │
    ├── meta
    │   ├── coverage
    │   ├── publication
    │   └── audit / dependency ...
    │
    ├── serving
    │   └── dbt views
    │
    └── marts
        └── optional materialized models
```

上述元数据对象为逻辑示例，不冻结具体表数量与名称。

Canonical 和 Meta 必须支持同一数据库事务内的一致性更新，因此首版不拆成独立数据库文件。

不采用每 Dataset 一个文件的通用策略。

### 10.2 存储形式

| 数据对象 | 首版方案 |
|---|---|
| Landing | 临时文件、Arrow 或 Parquet |
| Candidate | Arrow / 临时 Parquet / DuckDB 临时表 |
| Canonical | DuckDB 原生表 |
| Coverage / Audit | DuckDB 原生表 |
| dbt Serving | 默认 View |
| 大型研究结果 | 按需 Table / Incremental |
| 数据交换、备份导出 | Parquet 等独立格式 |

分钟行情首版使用 DuckDB 原生表。

是否创建 ART Index、采用怎样的排序或未来迁移至 Parquet，需根据真实数据测试磁盘体积、扫描性能与历史修订开销。

### 10.3 并发与生命周期

首版采用受控串行访问策略。

- Ingestion 和 dbt 不并发写入同一数据库文件。
- 独立 Notebook / 研究进程不得假设能够在外部写入发生时同时访问该文件。
- 不引入数据库服务进程或复杂并发协调器。
- 保留一致性备份与可迁移导出能力。

未来只有出现明确的并发、数据生命周期或管理成本问题，才考虑拆分数据库或生成独立研究快照。

### 10.4 数据形态与组件传递约束（V3.1.1 增补）

本节是三个 Dataset 伪代码与模拟数据传递验证后的**实现约束**，不引入新的通用组件或独立抽象层。

**1. 控制信息与大规模业务数据分离**

- Scope、Publication Unit、依赖版本、检查结果、失败原因、Publication ID 等小规模控制信息，优先用普通 Python `dict`、`list`、`tuple` 等数据结构。
- Landing、Candidate 和批量校验/发布所处理的大规模业务记录，可采用 Arrow、DuckDB 临时表、临时 Parquet 等列式或批量形式。
- Validation 应优先使用 DuckDB SQL 等批量计算，不将百万行分钟行情转换为 Python 对象逐行验收。
- 不预先要求建立 `Scope`、`Delivery`、`UnitDelivery`、`ValidationReport` 等专用类或固定内部协议；具体组织方式由简洁性和真实需求决定。

**2. Publication Unit 逻辑粒度与物理数据组织解耦**

- 不采用“每个 Publication Unit 一个文件 / DataFrame / 临时表”的默认策略。
- 同一 Candidate 可以包含多个 Unit；Validation 和 Publisher 可在整批数据上按 Unit 识别、过滤、验收及替换。
- 可依实际内存和性能需求进行分批、分块或临时落盘，但分块不必与 Publication Unit 一一对应。

**3. Adapter 的交付状态独立于业务记录**

- Adapter 的输出在逻辑上需要同时提供供应商交付数据（或可访问的引用）与请求交付结果。
- 必须能区分完整空集合、请求失败、已知不完整交付与正常非空交付，并将可定位的失败归属到受影响的 Unit。
- 供应商一次请求可以涉及多个 Publication Unit；由 Adapter 将请求交付结果映射到相应 Unit。
- 请求完成不代表已通过 Source-side 或 Canonical-side Validation；不强制使用额外的结果类或通用请求策略框架。

**4. 验收通过的 Unit 集合独立于 Candidate 数据行**

- Validation 必须能够明确哪些 Unit 通过、哪些因 ERROR 被阻断，以及 WARNING 的归属。
- Publisher 必须显式获得已通过验收的 Publication Unit 集合和 Candidate 数据引用，不能仅由 Candidate 现存数据行反推发布目标。
- 因此，一个具有零条 Candidate 行的合法 Unit 仍然可以成功原子替换旧数据、更新 Coverage 与 Publication。

**5. Candidate 记录能够归属到 Publication Unit**

- 每条 Candidate 业务记录必须能依 Record Key / 时间字段等业务字段或表达式确定归属的 Publication Unit，不强制附加 `unit_id` 物理列。
- 无法定位 Unit 的异常应依可确定的影响范围进行阻断；无法定位影响范围的全局阻断错误可阻断整个 Run。
- 空 Candidate 应具有明确的 Canonical Schema，不能依赖裸空列表推断字段类型。

**6. Landing / Candidate 生命周期完整**

- 暂存数据从生成、检查到发布提交或回滚期间必须持续可访问。
- 采用 DuckDB 连接级临时表时须保证 Validator 和 Publisher 可以访问同一张表；Arrow、临时 Parquet 等形式亦应确保相应访问生命周期。
- 清理应在发布提交或回滚及必要记录完成之后进行；不因此强制引入新的 Workspace 类或临时文件服务。

## 十一、dbt Serving

**Canonical 保存事实，Serving 提供研究消费便利。**

Serving 允许：

- 跨 Dataset JOIN。
- 构造完整的交易状态面板。
- 派生字段和研究指标。
- 提供统一查询接口。
- 根据性能需要进行物化。

默认通过 dbt View 实现，不提前物化大量重复数据。

例如：

```text
canonical.stock_basic ──────────┐
                                ▼
                     serving.stock_trading_status
                                ▲
canonical.suspend_daily ────────┘
```

Serving 必须尊重上游 Coverage，不允许将尚未完成发布的稀疏数据直接解释为正常交易状态。

Ingestion 的 Scope Resolver 可以直接读取 Canonical，不强制依赖 Serving。

Canonical 修订后：

- 普通 View 自动反映最新底层数据。
- Materialized Model、导出的数据或缓存不自动更新。
- 未来可利用 Canonical Change Audit 确定需要重新处理的业务范围。

第一版暂不实现自动增量重算机制。

## 十二、运行与调度

第一版采用轻量 Python 执行入口。

- Runner 按 Dataset Dependencies 检查上游可用性。
- Scope Resolver 计算目标范围。
- Adapter 执行接口请求。
- Validation 对各 Publication Unit 做验收。
- Publisher 批量、原子地发布成功 Unit。
- Run Audit 记录失败和异常。

暂不引入 Dagster / Prefect。

也暂不实现：

- 自动依赖失效传播 DAG。
- 自动历史补数规划器。
- 复杂任务调度状态机。
- 分布式执行框架。

dbt 独立负责 Serving 模型的 SQL 依赖与执行。

## 十三、架构 Invariants 汇总

以下属于 V3.1.1 的强约束，后续实现不得在未明确讨论的情况下改变。

1. **职责隔离**：Runner 管机制，Dataset 管业务语义，Adapter 管接口交付，Client 管通用协议。
2. **统一 Scope**：请求和验收使用同一个 Dataset 目标范围。
3. **发布粒度统一**：Publication Unit 同时是独立完整验收与原子发布的最小业务范围。
4. **允许部分成功**：一个 Run 失败不意味着所有 Unit 失败。
5. **发布前验收**：所有正式发布数据必须通过对应的阻断性检查。
6. **事务一致性**：Canonical 与对应 Coverage / Publication 原子更新。
7. **旧版本保护**：失败的历史修订不能破坏已有成功发布的数据。
8. **缺失可解释**：无记录不自动等于完整、不存在或正常。
9. **稀疏数据合法**：完整交付的零记录可以成功发布。
10. **事实与派生分离**：Canonical 不为下游便利引入无关依赖和派生补齐。
11. **完整性边界明确**：PUBLISHED 表示符合约定交付及验收契约，不保证供应商绝对无遗漏。
12. **版本可追溯**：Scope 使用的上游发布版本必须可追踪，并在发布前执行一致性检查。
13. **变更范围可追溯**：每次成功 Canonical 发布保留未来增量计算所需的最小变更信息。
14. **逻辑与物理解耦**：Dataset / Publication Unit 不直接决定物理文件组织。
15. **Serving 不参与权威发布**：dbt 不控制 Canonical 的发布前验收和事务。
16. **控制信息与业务数据分离**：小规模控制信息优先用普通 Python 结构，大规模业务记录批量、列式处理。
17. **Unit 不等于文件**：不以每 Unit 一个文件、DataFrame 或临时表作为默认物理组织策略。
18. **交付状态独立于数据行**：Adapter 必须表达合法空结果、交付失败及不完整交付，并在可行时归属受影响 Unit。
19. **发布目标独立于数据行**：Publisher 以显式传入的合格 Unit 集合作为发布依据，不从 Candidate 行反推。
20. **Unit 归属与数据生命周期**：Candidate 记录可归属 Unit，暂存数据在验收、发布和回滚期间保持可访问。

## 十四、明确暂缓的事项

下列事项不是第一版通用架构的实施前置条件。

| 事项 | 处理方式 |
|---|---|
| PIT / 双时态历史数据 | 在涉及财务和指数权重等具体 Dataset 时讨论 |
| 供应商交叉补缺与融合 | 实际接入多供应商时讨论 |
| 自动依赖失效传播 | 暂不实现 |
| dbt 增量修订重算 | 仅保留 Change Audit |
| 完整行级 CDC | 不实现 |
| 复杂 DAG 调度器 | 暂不引入 |
| 全面版本化 Canonical | 暂不实现 |
| DuckDB 多文件拆分 | 有明确需求后再评估 |
| Parquet 事务性表格式 | 暂不引入 |
| 通用请求策略 DSL | 不实现 |
| 复杂 Validation DSL | 不实现 |

Dataset 具体字段、供应商返回规则、Scope 计算边界、接口交付证明及检查阈值，需结合真实数据确定。

---

## 十五、已完成的架构验证与下一阶段

以下三个代表性 Dataset 的伪代码与模拟数据形态传递已完成验证；**真实供应商接口和 DuckDB 原型尚未验证**：

**1. `stock_basic`：完整快照型**

重点验证完整快照验收、原子替换、空值规则和历史修订。

**2. `suspend_daily`：稀疏日集合型**

重点验证零记录发布、交付完整性、按日 Coverage 和稀疏数据语义。

**3. `stock_1min`：大量独立 Publication Unit 型**

重点验证跨 Dataset Scope、依赖版本检查、部分成功、失败重试与批量事务发布。

原定实施与验证流程（前期模拟审查已完成）：

1. 根据 V3.1.1 给出项目目录与核心数据模型。
2. 使用接近真实代码的伪代码展示 Runner、Scope Resolver、Adapter、Validator、Publisher 的输入输出与主路径。
3. 通过正常、失败、空结果、修订及重复执行的示例审阅架构。
4. 确认之后由 Agent 落地实现。
5. 仅在真实案例无法自然表达时，重新讨论必要的抽象。

**本轮已确认的事实和后续要求：**

- `stock_basic` 当前供应商确认能返回历史上所有曾上市的股票；是否具有可靠的上市、退市日期及历史状态语义，仍需依据真实字段核对。
- 三类 Dataset 均可用同一 Runner、Scope、Publication Unit 和 Publisher 机制完成模拟数据传递；不需要引入新的通用类体系或一 Unit 一文件的结构。
- 后续以真实 `stock_basic`、`suspend_daily`、`stock_1min` API 响应确认 Canonical Schema、请求交付契约、零记录语义和分钟数据时间规则，并以最小 DuckDB 原型验证事务发布和批量 Candidate。
- 只有真实案例无法由既有边界自然表达时，才重新讨论架构抽象。

**V3.1.1 最终定位：**

以一个权威 DuckDB 文件承载 Canonical 和元数据，以轻量 Python Ingestion 保证可解释的获取、验收与发布，以 dbt View 提供组合后的研究消费接口。

在保留必要的完整性、原子性和审计能力的前提下，不为尚未实现的增量重算、复杂调度和多供应商融合提前增加系统复杂度。


---

## 附录：V3.1.1 invariant 的 V3.2 语义对应

2026-10-16 文档整理时从迁移说明并入。以下章节编号指向 [核心设计 V3.2](../core_design.md)。

本表记录合并表述的对应关系；第 16 项保留控制与数据分离的语义，将具体 Python 结构选择归入实施方案。

| 原编号 | 原保证 | V3.2 对应位置 |
|---|---|---|
| 1 | 职责隔离 | §二职责分工 |
| 2 | 统一 Scope | §三 Scope；§六.1 |
| 3 | 发布粒度统一 | §三 Publication Unit；§六.3 |
| 4 | 允许部分成功 | §六.3 |
| 5 | 发布前验收 | §六.2 |
| 6 | 事务一致性 | §六.3 |
| 7 | 旧版本保护 | §六.3 |
| 8 | 缺失可解释 | §六.1、5 |
| 9 | 稀疏数据合法 | §六.4、5 |
| 10 | 事实与派生分离 | §六.5 |
| 11 | 完整性边界明确 | §六.6 |
| 12 | 版本可追溯 | §四；§六.7 |
| 13 | 成功变更范围可追溯 | §三 Publication；§六.7 |
| 14 | 逻辑与物理解耦 | §三业务批次与 Unit |
| 15 | Serving 不参与权威发布 | §二职责分工；§四数据流 |
| 16 | 控制信息与业务数据分离 | §三控制信息与列式业务批次 |
| 17 | Unit 不等于文件 | §三业务批次与 Unit |
| 18 | 交付状态独立于数据行 | §三交付结果 |
| 19 | 发布目标独立于数据行 | §六.4 |
| 20 | Unit 归属与生命周期 | §三 Candidate；§六.7 |
