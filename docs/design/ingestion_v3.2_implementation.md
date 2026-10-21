# 个人量化数据库技术设计与实现边界 V3.2

**更新：2026-10-21｜范围：统一验收内核与五个正式 Dataset**

本文说明已落地的模型、接口、执行机制和能力边界，与[核心架构](core_design.md)配套。内容依据当前代码；不重新定义冻结的架构与 invariant，也不把规划中的 Dataset 或 Serving 写成已实现能力。后续实现变化时同步更新对应章节。

## 一、实现范围与代码地图

正式入口已有 `stock_basic` 完整快照、SSE `trade_calendar` 自然日日历、`suspend_daily` 停复牌日集合、`stock_st` 每日 ST 名单与 `index_basic` 全供应商指数当前快照，均使用官方 Tushare。股票快照、日历和指数快照无上游依赖；停复牌与 ST 名单依赖成功 SSE 日历。ST 名单和指数快照首版阻断零行，停复牌允许合法零行；均复用既有统一验收和原子发布内核。

| 位置 | 职责与边界 |
|---|---|
| [`__main__.py`](../../src/quant_data/__main__.py) | CLI、凭据加载、显式装配 Dataset / Adapter / Client，持有单一连接 |
| [`db.py`](../../src/quant_data/db.py) | 初始化、事务、标识符引用、Arrow 关系注册；不自动迁移旧库 |
| [`core/models.py`](../../src/quant_data/core/models.py) | 跨模块控制模型与 Dataset / SourceAdapter Protocol |
| [`core/runner.py`](../../src/quant_data/core/runner.py)、[`deps.py`](../../src/quant_data/core/deps.py) | 编排各阶段、固定与复核上游版本，不解释股票业务规则 |
| [`core/validation.py`](../../src/quant_data/core/validation.py) | 交付、Schema、Unit 归属与检查影响汇总，给出发布许可 |
| [`core/publisher.py`](../../src/quant_data/core/publisher.py) | 按许可 Unit 集合替换数据并提交成功元数据 |
| [`core/audit.py`](../../src/quant_data/core/audit.py)、[`artifacts.py`](../../src/quant_data/core/artifacts.py) | 分阶段审计与产物管理；原始证据、可选导出各有不同失败语义 |
| [`datasets/stock_basic.py`](../../src/quant_data/datasets/stock_basic.py)、[DDL](../../src/quant_data/datasets/stock_basic.sql) | 股票业务契约、验收规则及数据库约束 |
| [`datasets/trade_calendar.py`](../../src/quant_data/datasets/trade_calendar.py)、[DDL](../../src/quant_data/datasets/trade_calendar.sql) | SSE 自然日范围、逐日唯一事实与前交易日约束 |
| [`datasets/suspend_daily.py`](../../src/quant_data/datasets/suspend_daily.py)、[DDL](../../src/quant_data/datasets/suspend_daily.sql) | 固定 SSE 日历依赖、解析开闭市 Scope、停复牌稀疏日契约 |
| [`datasets/stock_st.py`](../../src/quant_data/datasets/stock_st.py)、[DDL](../../src/quant_data/datasets/stock_st.sql) | ST 日名单、上海日期边界、非空政策及描述缺失告警 |
| [`datasets/index_basic.py`](../../src/quant_data/datasets/index_basic.py)、[DDL](../../src/quant_data/datasets/index_basic.sql) | 全供应商指数当前快照、开放分类、可空属性与零行阻断 |
| [`sources/tushare/client.py`](../../src/quant_data/sources/tushare/client.py)、[`stock_basic.py`](../../src/quant_data/sources/tushare/stock_basic.py) | 通用供应商通信与股票专属交付适配 |
| [`sources/tushare/trade_calendar.py`](../../src/quant_data/sources/tushare/trade_calendar.py) | 按年请求、逐日交付影响与日历类型标准化 |
| [`sources/tushare/suspend_daily.py`](../../src/quant_data/sources/tushare/suspend_daily.py) | 逐交易日全市场获取，源日期归属与字符串标准化 |
| [`sources/tushare/stock_st.py`](../../src/quant_data/sources/tushare/stock_st.py) | ST 全市场逐日获取、1,000 行交付边界与源日期隔离 |
| [`sources/tushare/index_basic.py`](../../src/quant_data/sources/tushare/index_basic.py) | 指数专属 1,000 行分页、终止后空页验证、严格数值/文本解码与日期准备 |

采用对象组合与普通函数。Protocol 表达现有接口，不要求具体类继承；没有 Dataset 注册框架、通用分页框架或规则 DSL。

## 二、控制模型与数据形态

### 2.1 控制模型

共享模型使用 `frozen=True, slots=True` 的 dataclass，控制集合使用 tuple。冻结限制对象字段重新赋值，不意味着内部 dict 或 DataFrame 深度不可变。

| 模型 | 主要内容与语义 |
|---|---|
| `Scope` / `ScopeReason` | `units` 为 IN_SCOPE，`out_of_scope` / `unknown` 保存 Unit 与原因；三集合互斥，标识唯一且非空 |
| `DependencyRef` / `DependencyPin` | 上游 `(dataset, unit_key)` 及本次实际使用的 `publication_id` |
| `RequestAttempt` | 一个逻辑请求的一次 HTTP 尝试：参数、字段、时间、返回标记、错误及证据路径，不含业务行 |
| `UnitDelivery` | Unit 的 `COMPLETE` / `FAILED` / `INCOMPLETE` 获取状态及关联请求 ID；不表示转换或验收成功 |
| `Delivery` | Raw `frame`、Unit 获取状态、全部尝试和诊断；获取完成但 Raw 解码失败时也可无表，须有明确 ERROR |
| `Check` | `level`、`rule`、`stage`、影响 `units`、`detail`；ERROR 阻断，WARNING 留审计 |
| `ValidationResult` | 显式 `passed_units` / `blocked_units` 及检查，与 Candidate 行是否存在无关 |
| `Publication` / `UnitResult` | 成功发布的 Unit、版本和行数；本次 Unit 的状态与阶段 |
| `RunOptions` / `RunResult` | 运行选项；运行结局、Unit、成功版本、全局错误及独立产物状态 |

`Check.units=None` 表示全局影响，空 tuple 非法；影响集合含有无法识别的 Unit 时，Validator 将其规范化为全局影响并保留原标识。WARNING 始终不阻断。

`control_data()` 只序列化控制信息，拒绝直接展开 DataFrame；序列化 Delivery 时仅保留控制信息和行数。业务数据不塞入 Run JSON 或审计摘要。

### 2.2 业务数据

```text
供应商 JSON 信封 ──→ 严格 Arrow 类型 ──→ Polars Raw ──→ Polars Candidate
       │                                                │
       └── 即时保存脱敏证据 JSON                         ▼
                                             Arrow 注册 → DuckDB 临时表
                                                        │
                                                SQL 验收与事务发布
```

Landing 是暂存阶段：当前处理对象是 `Delivery.frame` 中的内存 Raw 表；每次尝试另存 JSON 证据，主流程不读取这些文件组表。

一个批次可含多个 Unit。`Dataset.unit_key_sql(alias)` 根据业务字段或表达式映射 Unit，不要求 Candidate 增加物理 `unit_id` 列。Scope 形成 `_scope_units` 临时关系，验收与发布通过 JOIN 判断归属。

空表必须有明确 Schema；`None` 表示没有可处理表，不能替代合法零行 Candidate。当前每个 Run 使用单个内存 Raw/Candidate 批次，尚无分块或超内存执行。

严格 Raw 解码仍在读取响应时完成，失败记为 prepare 阶段的 `source.fields` / `source.types` 诊断，原值留在请求信封；不把字段或类型错误改写成请求失败。COMPLETE 只表达请求计划与终止条件完成，FAILED 表达请求失败，INCOMPLETE 表达截断、无法确认结束或超出已知覆盖。任何状态都不能替代准备与业务验收。

## 三、模块接口与调用约定

| 接口 | 输入 → 输出 / 责任 |
|---|---|
| `Dataset` 属性 | `name`、`table`、Polars `schema`、`ddl`、`record_key` |
| `dependencies(params)` | 参数 → 所需上游范围 `tuple[DependencyRef, ...]` |
| `resolve_scope(params, pins, con)` | 参数、固定版本及同快照连接 → `Scope` |
| `unit_key_sql(alias)` | SQL 表别名 → 记录所属 Unit 的 SQL 表达式 |
| `validate_candidate(con, candidate_relation, scope)` | 标准化关系及 Scope → 业务 `Check` tuple |
| `Adapter.fetch(scope, artifacts)` | 统一 Scope 及证据写入对象 → `Delivery` |
| `Adapter.prepare(delivery, scope)` | 原始交付 → `(Candidate 或 None, 准备诊断 tuple)`；集中清理、范围匹配、解析并复用临时结果 |
| `validation.validate(con, dataset, scope, delivery, candidate, preparation_checks)` | Scope、获取证据、Candidate、准备诊断及 Dataset 规则 → 唯一的 `ValidationResult` 发布许可 |
| `Client.query(api_name, params, fields, *, request_id, on_attempt)` | 请求与逐次回调 → 完整信封；终态失败抛异常 |
| `runner.run(con, dataset, adapter, params, options)` | 同一连接及显式实例 → `RunResult` |
| `publisher.publish(con, dataset, candidate, passed_units, pins, run_id)` | Candidate 关系名、发布许可及依赖版本 → 已提交 `Publication` tuple |

Dataset 的写入政策目前体现为 Unit 归属和通用 Publisher 的完整替换，没有独立 `write_policy` 配置。`record_key` 描述业务键；通用 Validator 不自动根据它检查唯一性，具体 Dataset 规则与 DDL 负责落实。

Validator 与 Publisher 复用同次运行创建的 `candidate` / `_scope_units` 临时关系，依赖同一连接及阶段顺序。它们是内核内部调用接口，不能只传一个任意 DataFrame 就独立完成发布。

`fetch + prepare` 替换旧 Adapter 两阶段检查/转换入口，不保留兼容包装。复用现有模型、Canonical Schema、Meta 布局及 CLI 参数，不引入准备结果类或规则配置框架。

## 四、执行流程与事务

### 4.1 正常路径

1. **初始化与登记**：检查已有 Meta 布局，建表并核对 Canonical 列名、顺序和类型；登记 Run 与唯一产物路径。连接时区设为 `Asia/Shanghai`。
2. **依赖与 Scope**：同一读取事务内检查所需范围已有 PUBLISHED Coverage，固定版本、解析 Scope，并记录范围决策和依赖。缺少依赖时不请求供应商；没有 IN_SCOPE Unit 时跳过获取与发布。
3. **获取**：Adapter 调用 Client；每次尝试即时写证据。fetch 返回后，Runner 批量写请求与获取审计，拒绝重复或越界的 Unit，保留 Raw 与诊断。
4. **集中准备**：只调用一次 prepare，输出 Candidate 与准备诊断；即使全部转换失败或没有 Candidate 也送入统一验收，不按行数提前放行。准备产物同时展示 Raw 解码时已发现的 prepare 诊断。
5. **统一验收**：合并获取与准备诊断，要求明确 COMPLETE、Candidate Schema 一致、记录归属合法，并调用 Dataset 业务规则。全部范围已阻断时不强行执行业务验收；部分成功仍使用原 Scope，业务 ERROR 仅影响已阻断范围时不重复记录。获取无详细原因的缺失/失败状态必须补充门禁诊断。
6. **发布与收尾**：先保存检查和失败 Unit，再将合格 Unit 交给 Publisher。发布完成后依据持久化 Unit 状态结束 Run，处理产物并释放临时关系。

未声明、FAILED 或 INCOMPLETE 的 Unit 均被阻断；有错误诊断的 COMPLETE Unit 同样不能发布。仍有未阻断范围却没有 Candidate 也是 ERROR。合法空响应与坏值全部被隔离后的 Candidate 都可能零行，必须结合证据和规则区分；检查未执行不等于通过。

### 4.2 发布事务

所有合格 Unit 在当前实现中进入**同一次发布事务**：

```text
复核依赖版本 → DELETE 合格 Unit 的旧数据 → INSERT 这些 Unit 的 Candidate
    → 写 Publication → 切换 Coverage → 写成功 Unit 审计 → COMMIT
```

Scope 外的发布许可被拒绝；没有合格 Unit 时不修改业务数据，也不生成成功版本。依赖的任何固定版本变化或消失都会阻断当前批次，要求重新运行并解析 Scope，不自动重新评估。

发布按集合执行 DELETE / INSERT / GROUP BY，元数据通过列式控制关系批量写入，不为每个 Unit 循环 SELECT / UPSERT。行数以合格 Unit 为左表关联统计，因此零行 Unit 仍能删除旧范围并获得新版本。版本使用实际 `RETURNING` 结果，不假定顺序或编号连续；回滚允许序列号有空隙。

事务中任一步失败，Canonical、Publication、Coverage 和成功 Unit 审计全部回滚。部分成功来自验收阶段只放行部分 Unit；当前一次发布事务中的合格 Unit 不会各自独立提交。

### 4.3 失败与生命周期

整个 Run 不处于一个大事务中：范围、交付、检查与失败审计分阶段保存，成功数据及成功元数据由发布事务保护。Runner 捕获普通执行异常，标记本次未发布的 IN_SCOPE Unit 并汇总结果；已提交成功 Unit 不反写成失败。

Raw/Candidate 在验收、发布提交或回滚期间保持可访问，正常收尾时在诊断导出之后释放连接级临时关系。进程强制退出、审计写入故障等不提供完整自动恢复保证，可能留下 RUNNING / PENDING 审计或暂存目录；这不能据以推断存在成功发布。

## 五、正式 Dataset 契约

### 5.1 stock_basic 范围与语义

| 项目 | 当前约定 |
|---|---|
| Canonical | `canonical.stock_basic`，一条 `ts_code` 一行 |
| Scope / Unit | 唯一 `SNAPSHOT`，无上游依赖；拒绝任何子集参数 |
| 获取范围 | `L / D / P / G / UN` 五种状态 × `SSE / SZSE / BSE` 三交易所 |
| 写入政策 | 全部请求、源与业务验收通过后，整表原子替换；相同内容重跑也产生新版本 |
| 时间语义 | 抓取时最新基础信息，上市/退市日期为市场本地 DATE；不代表历史每日快照或 PIT 数据 |

完整 17 列、可空规则和数据库约束以[正式 DDL](../../src/quant_data/datasets/stock_basic.sql)及 [Dataset Schema](../../src/quant_data/datasets/stock_basic.py)为准。`ts_code` / `symbol` 独立保留为字符串；仅 `list_date` / `delist_date` 转为 DATE，其余字段为字符串。

### 5.2 获取与标准化

- 官方 HTTPS JSON POST，显式请求全部 17 字段。15 个请求串行执行，逻辑请求起始间隔至少 1.3 秒；请求失败或截断后停止后续分片。字段/Raw 类型错误独立记为准备诊断，继续完成剩余获取计划，但仍阻断整个快照。
- Client 默认超时 40 秒、最多 3 次尝试。仅超时、HTTP 429 和 5xx 重试；权限、供应商业务码、坏信封及其他连接错误直接失败。重试等待支持 `Retry-After` 秒数或 HTTP 日期，超过 60 秒或非法值明确失败。
- Client 校验成功信封、唯一字段名、行列宽度与布尔 `has_more`；Adapter 按字段名重排，要求 17 列齐全、`has_more=False` 且行数低于 6,000。缺少标记、标记为 true 或触及上限均阻断，不使用 `count` 推断完整性，不自动分页。
- Arrow String 严格拒绝非字符串非 NULL 的业务值，避免数字隐式转码。Raw 为 17 列字符串加 `_requested_status` / `_requested_exchange`；prepare 一次清理文本后核对请求范围。
- prepare 中两列日期各解析一次、格式掩码各计算一次，再复用结果识别 ASCII 八位格式与历法错误。合法 NULL 按 Dataset 条件必填验收，非法值不作为 NULL 放行；任意准备错误阻断整个快照。Candidate 只选回 17 列，不去重、不补属性、不改写证券标识。

### 5.3 业务验收

| 级别 | 规则 |
|---|---|
| ERROR | `ts_code` / `symbol` / `name` 非空；exchange、list_status 必填且属于约定枚举；非空 is_hs 为 N/H/S |
| ERROR | L/D/P 必须有上市日期；D 必须有退市日期；两日期存在时退市不得早于上市 |
| ERROR | `ts_code` 全快照唯一；快照至少有一条 L，防止空响应清空整表 |
| WARNING | 未来上市/退市日期、ts_code 前缀与 symbol 不同、可选描述属性缺失 |

因此 UN/G 可以缺上市日期，L 可以带已公告的未来退市日期，历史特殊代码及 symbol 映射差异保留。通用机制允许零行发布，股票契约自身拒绝没有 L 的快照，两者职责不同。

上述交付契约来自[官方接口探查](../research/tushare-official-stock-suspend-2026-10-13.md)。成功只表示满足已知交付及验收规则，不证明供应商覆盖所有曾上市证券，也不证明 15 次请求处于供应商同一时点快照。当前未实现与旧快照比较数量突变的告警或独立业务完整性证明。

### 5.4 trade_calendar 范围与语义

用户确认首版只获取 SSE，作为当前沪深北研究的共同日历。Canonical 保存真实 SSE 行，不生成 SZSE/BSE 替代行，也不独立证明三市场全历史安排一致。后续 Scope 应读取并固定 `trade_calendar / SSE:YYYY-MM-DD` 的成功版本。

| 项目 | 当前约定 |
|---|---|
| Canonical / 主键 | `canonical.trade_calendar` / `(exchange, cal_date)` |
| 字段 | exchange 字符串固定 SSE；cal_date DATE；is_open BOOLEAN；pretrade_date 可空 DATE，完整约束见 [DDL](../../src/quant_data/datasets/trade_calendar.sql) |
| Scope / Unit | 显式 start_date / end_date 闭区间的每个自然日，包括休市；Unit 为 `SSE:YYYY-MM-DD`，无上游依赖 |
| 获取 | 按自然年拆请求片段，首尾年只取目标子区间；显式四字段、不传 is_open 过滤；片段之间至少间隔 1.3 秒 |
| Raw / Candidate | Raw 为三列字符串、一列整数 is_open 及请求标签；Candidate 为四列正式类型，不依赖返回列顺序或日期排序 |
| 发布 | 按日完整替换，每 Unit 恰好一行；is_open=false 是成功休市事实，零行日历不能发布 |

每片要求字段齐全及 has_more=false，不使用 count；请求失败、截断、非约定 JSON 类型或无法定位的日期/交易所越界阻断该请求片段。后两类属于准备诊断，不改写 COMPLETE。prepare 的日期与交易所身份核对同时匹配请求标签和目标 Unit，防止错误年度记录污染另一个合格年度。缺日和重复日统一由 Dataset 的 `calendar.one_row` 判断，移除重复的源缺日扫描。

prepare 清理文本一次，两列日期各解析一次。cal_date / 非空 pretrade_date 要求 ASCII 八位有效日期；JSON is_open 只接受整数或 NULL，准备时再要求非空 0/1。可定位的日期/开市标记转换失败按日 ERROR，不能通过 NULL 或 false 补造事实；这些行不进入 Candidate，Raw/信封保留原值。合法空前交易日仍保留 NULL 并告警。有值须早于 cal_date，允许引用区间外日期；未强制验证其指向成功开市事实。未来日历保留并告警，不承诺安排不修订。

按日规则支持缺失、重复、修订失败及旧 Coverage 保护；发布事务和 Meta 布局保持不变。原交付探查及真实运行见[交易日历报告](../research/tushare-official-trade-calendar-2026-10-16.md)，反映修订前实现。

### 5.5 suspend_daily 范围与语义

供应商接口为 `suspend_d`，内部表为 `canonical.suspend_daily`。只存供应商报告的稀疏事实，不补正常交易股票日，不依赖当前 stock_basic，不推导全天停牌或可交易状态。

| 项目 | 当前约定 |
|---|---|
| 字段 / 主键 | ts_code 字符串、trade_date DATE、suspend_type S/R、可空 suspend_timing 字符串；键为 `(ts_code, trade_date, suspend_type)`，见 [DDL](../../src/quant_data/datasets/suspend_daily.sql) |
| 输入与依赖 | 显式 start_date / end_date 闭区间；每个自然日都要求同库 `trade_calendar / SSE:YYYY-MM-DD` 的 PUBLISHED 版本，缺一即在获取前阻断 Run |
| Scope | 在固定版本的同一事务读取日历事实；开市日 IN_SCOPE，休市日 OUT_OF_SCOPE；若成功 Coverage 下实际缺事实则 UNKNOWN |
| Unit / 写入 | ISO 交易日 `YYYY-MM-DD`；全市场整日替换，完整零行同样写成功版本并删除旧集合；休市日不请求、不生成停复牌 Coverage |
| 获取 | 每个 IN_SCOPE 日一次全市场请求，只传 trade_date、显式四字段；串行间隔 1.3 秒，失败继续其他日期 |
| Raw / Candidate | Raw 为四列字符串加 `_requested_date`；Candidate 为四列正式类型，多 Unit 共享一张表 |

源侧要求四字段齐全、has_more=false 且低于 5,000 行，不使用 count、不自动分页。早于官方历史起点 2000-01-03 的目标开市日为 ERROR / INCOMPLETE，不请求或发布空集合。严格 Arrow String 拒绝数字代码等隐式转码；请求、类型或截断失败只阻断对应请求日。源日期去首尾空白后必须等于请求的合法 YYYYMMDD，NULL、七位、非法历法或其他日期均为 ERROR，影响按可信请求标签定位。

prepare 清理文本后只计算一次日期匹配掩码，复用于诊断和安全过滤；只对匹配的合法日期进行一次 DATE 转换。错日期行按可信请求标签 ERROR，原始值保留在 Raw/信封；这些行不进入 Candidate，防止污染其他 Unit 或引发全局解析失败。同请求日保留下来的正常行仍受整日阻断，不能部分发布该日。不去重、不拆分 timing。

业务 ERROR 包括空键、类型非 S/R、非空白时段要求及三字段键重复。同键不同 timing 也阻断整日；同日同证券的 S 与 R 可以同时存在。时段正文（含未知格式）和 NULL 保留，不以 NULL 直接生成全天停牌标记。完整零行的意义仅为该次供应商未报告事件，不承诺历史绝无遗漏或当日数据终态。

依赖审计包含休市日，因为排除决策同样使用了这些版本；发布前任何固定日历版本变化都阻断当前批次并要求重算 Scope。上游最近一次执行失败不阻止使用其仍有效的旧 PUBLISHED 数据。不自动触发日历或传播失效。原真实运行与限制见[落地记录](../research/suspend-daily-ingestion-validation-2026-10-16.md)；本轮只调整 Adapter 准备接口与统一验收，不改日期依赖或写入语义。

### 5.6 stock_st 范围与语义

正式实现、Schema、SSE 日历依赖、零行阻断、按日替换、获取/准备/验收规则及独立库验证记录集中维护于 [stock_st 文档](stock_st_ingestion_proposal.md)。沿用现有接口与 Meta，CLI 使用显式日期闭区间，未来日期拒绝；与停复牌的零行发布政策分别定义。

### 5.7 index_basic 范围与语义

正式 Schema、完整快照范围、分页交付、零行政策、验收规则及独立库验证记录集中维护于 [index_basic 文档](index_basic_ingestion_proposal.md)。保留全部 13 字段、所有返回市场与终止指数，主键 ts_code；无日历或股票表依赖，单 SNAPSHOT 整表原子替换，不接受子集参数。

每页 1,000 行，仅满页 has_more=true 可继续，短页 has_more=false 后还需空验证页确认终止；最多 100 个逻辑请求，count 不参与判断。缺列/类型错误是准备诊断，不改写确已完成的获取；零行快照由 Dataset ERROR 阻断。0 基点合法，负值告警，非有限值阻断；三个日期可空，基期晚于发布日期不阻断。没有新增共享接口、Meta 或通用分页框架。

## 六、元数据与状态语义

七张表的完整定义见 [`core/meta.sql`](../../src/quant_data/core/meta.sql)。

| 表 / 主键 | 内容与用途 |
|---|---|
| `meta.run` / run_id | 参数、起止时间、Run 状态、计数摘要、全局错误与失败阶段、产物状态 |
| `meta.run_unit` / run_id + dataset + unit_key | 本次 Scope 决策与原因、交付状态、执行状态与阶段、请求/检查引用、成功行数与版本 |
| `meta.run_dependency` / run_id + upstream_dataset + upstream_unit | Scope 实际使用的上游成功版本 |
| `meta.request_attempt` / run_id + request_id + attempt | 每次 HTTP 尝试的控制信息与原始证据路径，重试各占一行 |
| `meta.validation_check` / run_id + check_id | 每项检查只存一次，units 为 NULL 表示全局影响 |
| `meta.publication` / publication_id | 一次成功 Unit 替换的 dataset、unit_key、row_count、run_id、发布时间 |
| `meta.coverage` / dataset + unit_key | 当前权威状态与成功版本指针 |

`run_unit.request_ids` 关联逻辑请求，各次尝试由 attempt 区分；`check_ids` 关联共享检查。`run.summary` 仅存 scope_counts、unit_counts、request_attempts 和 check_counts，不复制业务记录或全部检查详情。

需要分别阅读四类状态：

| 层次 | 状态及解释 |
|---|---|
| Scope 决策 | IN_SCOPE 应获取验收；OUT_OF_SCOPE 合理排除；UNKNOWN 无法确定 |
| 获取状态 | COMPLETE 请求及终止条件完成，仍可能有准备/业务错误；FAILED 请求/协议失败；INCOMPLETE 截断、无法确认结束或超出已知覆盖；均不等于发布状态 |
| Unit 执行 | IN_SCOPE 从 PENDING 变为 FAILED 或 PUBLISHED；排除/未知分别记录 OUT_OF_SCOPE / UNKNOWN |
| Coverage | PENDING / FAILED / UNKNOWN / PUBLISHED；只有 PUBLISHED 携带成功版本，排除范围不写 Coverage |

新运行不能用 PENDING / FAILED / UNKNOWN 覆盖已有 PUBLISHED Coverage。例如，本次 Unit 可以 FAILED，而 Coverage 仍 PUBLISHED 并指向旧成功数据。

Run 初始为 RUNNING；结束时，有失败、未知、待处理 Unit 或全局异常，且已有成功 Unit，则为 PARTIAL，否则 FAILED；没有上述问题为 SUCCESS。OUT_OF_SCOPE 不算失败，因此 SUCCESS 本身不保证发布了数据：全部排除或空 Scope 可以成功且无 Publication。

Publication 记录的是成功变更范围，Canonical 仅保留当前事实；没有历史行镜像、按 publication_id 查询旧数据或通用 CDC 能力。

## 七、证据、诊断产物与清理

每个 Run 使用独立 `<landing-dir>/<run_id>/`，拒绝覆盖旧目录。

| 内容 | 保存时机与失败语义 |
|---|---|
| 原始请求证据 | 每次 HTTP 尝试结束即时写脱敏 JSON，不保存 token 请求体；证据写失败在发布前阻断获取 |
| 请求与检查审计 | fetch 返回后及验收阶段批量写 Meta；不是逐次 HTTP 尝试即时写库，因此进程中断时不保证 Meta 已有全部尝试 |
| 可选阶段导出 | `--keep-artifacts` 下导出；故障记录为 export_errors，影响诊断完整性，不撤销已提交发布 |
| 目录清理 | 默认仅 SUCCESS 清理；FAILED / PARTIAL 保留已有证据。清理故障为 CLEANUP_FAILED，业务状态独立保留 |

保留模式包含 scope、pins、请求信封、获取控制、Raw/Candidate Parquet + CSV + Schema、`preparation.json` 准备诊断、统一验收、发布和结果。prepare 文件替代旧源检查文件，并纳入产物完整性要求；Raw 解码时发现的准备诊断也集中展示，最终 Meta 中每项仅存一次。旧运行产物不改写。`stages/scope`、`validation`、`published`、`finished` 导出七张 Meta 表，published 另导出 Canonical；这些快照包含当时全表内容，不仅限本次 Run。

目录 README 提供阅读顺序，manifest 保存完整性、错误及文件 SHA-256。`artifacts_complete=True` 要求成功且关键导出齐全；失败、阶段未执行或导出故障可为 false，默认非保留模式为 null。检查诊断结果时结合 RunResult、Meta 与 manifest；证据路径是否仍有效还需结合 CLEANED / RETAINED / CLEANUP_FAILED 状态。

暂存目录不承担长期备份或历史版本库职责；不自动扫描、清理崩溃遗留或历史目录。

## 八、运行与验证

项目使用 Python 3.13 与 uv 锁定依赖，主要库为 Polars、DuckDB、PyArrow、dbt-duckdb、python-dotenv。仓库根目录 `.env` 的 `TUSHARE_TOKEN` 优先于环境变量，正式 Client 只访问官方端点。

```bash
uv sync --frozen
uv run --frozen python -m quant_data ingest stock_basic --db data/warehouse_v3_2.duckdb

# 保留完整诊断，或通过脚本创建独立观察库与产物目录
uv run --frozen python -m quant_data ingest stock_basic \
  --db data/probes/new/warehouse.duckdb --landing-dir data/probes/new/landing --keep-artifacts
uv run --frozen python scripts/capture_stock_basic_walkthrough.py

uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20100101 --end-date 20261231 --db data/warehouse_v3_2.duckdb

# 停复牌须在同一库先发布相应日期区间的 SSE 日历
uv run --frozen python -m quant_data ingest suspend_daily \
  --start-date 20200309 --end-date 20200315 --db data/warehouse_v3_2.duckdb

uv run --frozen python -m unittest discover -s tests -v
```

CLI 默认库路径仍为 `data/warehouse.duckdb`，landing_root 默认为 `data/landing`。已有旧 Meta 或 Canonical 列布局不兼容时明确报错，不自动迁移或删除原库；初始化不是完整 DDL 约束差异迁移工具。

CLI 输出 RunResult JSON，业务 SUCCESS 返回 0，FAILED / PARTIAL 返回 1；诊断不完整独立表达，可能仍返回 0。walkthrough 脚本额外要求 `artifacts_complete=True`，否则返回非零。

trade_calendar / suspend_daily / stock_st 必须提供 `--start-date` / `--end-date`（ASCII YYYYMMDD）；stock_basic / index_basic 拒绝这两个日期过滤。未接入其他交易所日历或股票/类型筛选参数，避免改变完整范围契约。

2026-10-16 验证：47 项测试中 46 项通过，1 项因本地股票真实响应样本缺失跳过。新增 11 项停复牌场景覆盖跨 Dataset 日历依赖、休市排除、实际事实缺失 UNKNOWN、零行替换、错日隔离、键冲突、已排除日修订后的版本复核、回滚与 CLI。其余测试覆盖股票、日历、通用机制与诊断故障。100 / 5,000 Unit 的机制测试 SQL 调用数均为 28，后者处理约 120 万行；这不等同于正式分钟 ingestion 的性能验收。环境脚本的正式 DDL、Arrow/Parquet 和 dbt View 检查通过。

2026-10-18 统一验收修订：52 项测试中 51 项通过，1 项旧股票真实样本重放因本地样本缺失跳过；环境烟测通过。新增场景验证同一空 Candidate 在获取失败、未知、转换失败时阻断，完整空交付才可发布；验证合法 NULL 与非法日期、Raw 原值保留、COMPLETE 与准备失败独立、准备产物及日历缺日仅产生一处业务诊断。批量机制 SQL 调用数仍为 28。

本轮未重新请求供应商；重放 2026-10-16 保存的真实响应，日历 6,209 行、停复牌 1,463 行及各次 Candidate 与修订前逐列一致。对照获取 31 份已保存响应（17 份广区间日历、14 份停复牌及其上游日历），重跑 9 次正式 Runner 流程；股票真实样本等价性仍未复验。新观察库与产物位于 `data/probes/unified-acceptance-replay-20261018T005400153706/`，`summary.json` 记录结果；此处为离线重放验证，重放 Client 不进入生产代码。

正式日历 CLI 在独立验证库完成 2010-01-01–2026-12-31 的真实运行：17 个请求、6,209 条事实及同数量的成功 Publication / Coverage（开市 4,128 天、休市 2,081 天），无 ERROR，未来 76 日保留告警。产物完整，60 个清单文件的 SHA-256 与发布后 Canonical 快照核对通过。运行与库路径见[报告](../research/tushare-official-trade-calendar-2026-10-16.md)。

2026-10-21 index_basic 接入：新增 11 项测试及 12,599 行真实快照重放通过；全套 76 项中 75 项通过，1 项既有股票真实样本缺失跳过，环境烟测通过。正式 CLI 在独立库两次各发布 12,599 行，重跑逐字段一致、产生新版本且不累加；离线零行/请求失败注入保护旧事实与成功指针，197 个产物文件哈希核对通过。数据库、请求、Coverage / Publication 与限制见 [index_basic 验收记录](index_basic_ingestion_proposal.md#42-独立库真实-cli-验收)。

## 九、当前边界与后续接入

| 方面 | 当前边界 |
|---|---|
| 正式 Dataset | stock_basic、SSE trade_calendar、suspend_daily、stock_st 与 index_basic；行情日频、分钟尚未接入 |
| 依赖管理 | 检查已有成功版本，发布前复核；不自动运行上游或传播失效 |
| 执行与数据规模 | 单连接、单内存批次、串行请求；不支持断点恢复、超内存流式处理或分布式执行 |
| 并发 | Ingestion、dbt、外部研究进程按运行约定错开访问同一文件；未实现锁协调服务 |
| Serving | dbt 默认 View 是已确认路线；环境脚本验证过 dbt View，但尚无正式 Serving/Mart 模型 |
| 历史与运维 | 未实现 PIT、全面历史版本、自动备份恢复、调度、补数规划或增量修订重算 |

后续接入 Dataset 时，先确认 Schema / Record Key、依赖与 Scope、Unit、完整替换范围、零行语义、交付证明及 ERROR / WARNING 规则，再实现专属 Dataset 与 Adapter。沿用现有内核；需要改变共享接口或冻结语义时，先提出方案确认。

V3.1.1 的[原始设计稿](个人量化数据库架构冻结稿_V3.1.1_增量修订版.md)和[归档及 invariant 对照](archive/core_design_v3.1.1.md)作为历史参考。其中“尚未验证”等进度及早期供应商假设不作为当前能力说明；当前实现以本文及代码为依据，架构约束以核心架构为准，发现冲突时需显式讨论。
