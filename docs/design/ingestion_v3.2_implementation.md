# 个人量化数据库技术设计与实现边界 V3.2

**更新：2026-10-16｜范围：当前 ingestion 内核、`stock_basic` 与 `trade_calendar`**

本文说明已落地的模型、接口、执行机制和能力边界，与[核心架构](core_design.md)配套。内容依据当前代码；不重新定义冻结的架构与 invariant，也不把规划中的 Dataset 或 Serving 写成已实现能力。后续实现变化时同步更新对应章节。

## 一、实现范围与代码地图

正式入口已有 `stock_basic` 完整快照及 SSE `trade_calendar` 自然日日历，均使用官方 Tushare。日历切片验证多 Unit 与部分成功；合法零行替换和依赖版本检查另有通用机制测试，两个正式 Dataset 本身均无上游依赖。

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
| [`sources/tushare/client.py`](../../src/quant_data/sources/tushare/client.py)、[`stock_basic.py`](../../src/quant_data/sources/tushare/stock_basic.py) | 通用供应商通信与股票专属交付适配 |
| [`sources/tushare/trade_calendar.py`](../../src/quant_data/sources/tushare/trade_calendar.py) | 按年请求、逐日交付影响与日历类型标准化 |

采用对象组合与普通函数。Protocol 表达现有接口，不要求具体类继承；没有 Dataset 注册框架、通用分页框架或规则 DSL。

## 二、控制模型与数据形态

### 2.1 控制模型

共享模型使用 `frozen=True, slots=True` 的 dataclass，控制集合使用 tuple。冻结限制对象字段重新赋值，不意味着内部 dict 或 DataFrame 深度不可变。

| 模型 | 主要内容与语义 |
|---|---|
| `Scope` / `ScopeReason` | `units` 为 IN_SCOPE，`out_of_scope` / `unknown` 保存 Unit 与原因；三集合互斥，标识唯一且非空 |
| `DependencyRef` / `DependencyPin` | 上游 `(dataset, unit_key)` 及本次实际使用的 `publication_id` |
| `RequestAttempt` | 一个逻辑请求的一次 HTTP 尝试：参数、字段、时间、返回标记、错误及证据路径，不含业务行 |
| `UnitDelivery` | Unit 的 `COMPLETE` / `FAILED` / `INCOMPLETE` 交付状态及关联请求 ID |
| `Delivery` | 原始 `frame`、Unit 交付状态、全部请求尝试和获取阶段检查；失败时可无表 |
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

## 三、模块接口与调用约定

| 接口 | 输入 → 输出 / 责任 |
|---|---|
| `Dataset` 属性 | `name`、`table`、Polars `schema`、`ddl`、`record_key` |
| `dependencies(params)` | 参数 → 所需上游范围 `tuple[DependencyRef, ...]` |
| `resolve_scope(params, pins, con)` | 参数、固定版本及同快照连接 → `Scope` |
| `unit_key_sql(alias)` | SQL 表别名 → 记录所属 Unit 的 SQL 表达式 |
| `validate_candidate(con, candidate_relation, scope)` | 标准化关系及 Scope → 业务 `Check` tuple |
| `Adapter.fetch(scope, artifacts)` | 统一 Scope 及证据写入对象 → `Delivery` |
| `Adapter.validate_source(delivery, scope)` | 原始交付 → 源检查 tuple |
| `Adapter.normalize(delivery, scope)` | 原始交付 → `(Candidate 或 None, 转换检查 tuple)` |
| `Client.query(api_name, params, fields, *, request_id, on_attempt)` | 请求与逐次回调 → 完整信封；终态失败抛异常 |
| `runner.run(con, dataset, adapter, params, options)` | 同一连接及显式实例 → `RunResult` |
| `publisher.publish(con, dataset, candidate, passed_units, pins, run_id)` | Candidate 关系名、发布许可及依赖版本 → 已提交 `Publication` tuple |

Dataset 的写入政策目前体现为 Unit 归属和通用 Publisher 的完整替换，没有独立 `write_policy` 配置。`record_key` 描述业务键；通用 Validator 不自动根据它检查唯一性，具体 Dataset 规则与 DDL 负责落实。

Validator 与 Publisher 复用同次运行创建的 `candidate` / `_scope_units` 临时关系，依赖同一连接及阶段顺序。它们是内核内部调用接口，不能只传一个任意 DataFrame 就独立完成发布。

## 四、执行流程与事务

### 4.1 正常路径

1. **初始化与登记**：检查已有 Meta 布局，建表并核对 Canonical 列名、顺序和类型；登记 Run 与唯一产物路径。连接时区设为 `Asia/Shanghai`。
2. **依赖与 Scope**：同一读取事务内检查所需范围已有 PUBLISHED Coverage，固定版本、解析 Scope，并记录范围决策和依赖。缺少依赖时不请求供应商；没有 IN_SCOPE Unit 时跳过获取与发布。
3. **获取与源验收**：Adapter 调用 Client；每次尝试结束即时写证据。fetch 返回后，Runner 批量写请求与交付审计，拒绝重复或越界的交付 Unit，再执行源检查。
4. **标准化与业务验收**：至少还有一个 COMPLETE 且未被源 ERROR 阻断的 Unit 才标准化。Validator 汇总交付和源检查，要求 Candidate 的字段名、顺序、类型匹配；创建临时表，执行 Dataset 规则并检查每行能归属 IN_SCOPE Unit。
5. **发布与收尾**：先保存检查和失败 Unit，再将合格 Unit 交给 Publisher。发布完成后依据持久化 Unit 状态结束 Run，处理产物并释放临时关系。

交付未声明、FAILED 或 INCOMPLETE 的 Unit 均被阻断。仍有可验收范围却没有 Candidate 时也是 ERROR。源检查可以提前阻断全部范围，因此不是每次运行都会生成 Candidate 或执行全部业务规则。

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

- 官方 HTTPS JSON POST，显式请求全部 17 字段。15 个请求串行执行，逻辑请求起始间隔至少 1.3 秒；任一分片失败或不完整后停止后续分片。
- Client 默认超时 40 秒、最多 3 次尝试。仅超时、HTTP 429 和 5xx 重试；权限、供应商业务码、坏信封及其他连接错误直接失败。重试等待支持 `Retry-After` 秒数或 HTTP 日期，超过 60 秒或非法值明确失败。
- Client 校验成功信封、唯一字段名、行列宽度与布尔 `has_more`；Adapter 按字段名重排，要求 17 列齐全、`has_more=False` 且行数低于 6,000。缺少标记、标记为 true 或触及上限均阻断，不使用 `count` 推断完整性，不自动分页。
- Arrow String 严格拒绝非字符串非 NULL 的业务值，避免数字隐式转码。Raw 为 17 列字符串加 `_requested_status` / `_requested_exchange`；源验收逐行核对与请求范围一致。
- 标准化去除文本两端空白并将空串转 NULL，不去重、不补默认值、不改写证券标识。日期先要求 ASCII 八位 `YYYYMMDD`，再严格解析真实历法日期；七位日期、非法日期明确阻断。

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

每片要求字段齐全及 has_more=false，不使用 count；请求失败、截断、非约定 JSON 类型或无法定位的日期/交易所越界阻断该请求片段。缺日期单独阻断对应 Unit，不推断为休市；其他日期可以发布。JSON is_open 必须是整数或 NULL，字符串、布尔与浮点不隐式转换；NULL 或非 0/1 值在验收时阻断对应日期。

cal_date / 非空 pretrade_date 要求 ASCII 八位及有效历法日期。前交易日非法时阻断对应日期；有值须早于 cal_date，允许引用区间外日期，未强制验证其一定指向成功开市事实。缺前交易日允许发布并告警；未来日历保留供应商事实并告警，不承诺未来安排不修订。格式错误已显式阻断的值在 Candidate 中可能为 NULL，Raw 和请求证据保留原值，不会通过发布许可。

按日规则支持缺失、重复、修订失败及旧 Coverage 保护；本轮没有改动共享模型、Runner、Validator、Publisher 或 Meta。交付探查和真实运行记录见[交易日历报告](../research/tushare-official-trade-calendar-2026-10-16.md)。

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
| 交付状态 | COMPLETE 获取完成；FAILED 请求失败；INCOMPLETE 已知不完整；均不等于发布状态 |
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

保留模式包含 scope、pins、请求信封、交付控制、源检查、Raw/Candidate Parquet + CSV + Schema、验收、发布和结果。`stages/scope`、`validation`、`published`、`finished` 导出七张 Meta 表，published 另导出 Canonical；这些快照包含当时全表内容，不仅限本次 Run。

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

uv run --frozen python -m unittest discover -s tests -v
```

CLI 默认库路径仍为 `data/warehouse.duckdb`，landing_root 默认为 `data/landing`。已有旧 Meta 或 Canonical 列布局不兼容时明确报错，不自动迁移或删除原库；初始化不是完整 DDL 约束差异迁移工具。

CLI 输出 RunResult JSON，业务 SUCCESS 返回 0，FAILED / PARTIAL 返回 1；诊断不完整独立表达，可能仍返回 0。walkthrough 脚本额外要求 `artifacts_complete=True`，否则返回非零。

trade_calendar 必须提供 `--start-date` / `--end-date`（ASCII YYYYMMDD）；stock_basic 拒绝这两个日期过滤。未接入其他交易所参数，避免在 SSE 事实表中混入不同口径。

2026-10-16 验证：36 项测试中 35 项通过，1 项因本地股票真实响应样本缺失跳过。覆盖源失败、严格类型与日期、业务规则、重复替换、事务回滚、旧成功数据保护、依赖漂移、部分成功、零行及产物故障，新增日历覆盖跨年请求、缺日、重复日、错误影响定位与 CLI。100 / 5,000 Unit 的机制测试 SQL 调用数均为 28，后者处理约 120 万行；这不等同于正式分钟 ingestion 的性能验收。

正式日历 CLI 在独立验证库完成 2010-01-01–2026-12-31 的真实运行：17 个请求、6,209 条事实及同数量的成功 Publication / Coverage（开市 4,128 天、休市 2,081 天），无 ERROR，未来 76 日保留告警。产物完整，60 个清单文件的 SHA-256 与发布后 Canonical 快照核对通过。运行与库路径见[报告](../research/tushare-official-trade-calendar-2026-10-16.md)。

## 九、当前边界与后续接入

| 方面 | 当前边界 |
|---|---|
| 正式 Dataset | stock_basic 与 SSE trade_calendar；suspend_daily 仍是 [Schema 建议](canonical_suspend_daily_proposal.sql)，日频、分钟尚未接入 |
| 依赖管理 | 检查已有成功版本，发布前复核；不自动运行上游或传播失效 |
| 执行与数据规模 | 单连接、单内存批次、串行请求；不支持断点恢复、超内存流式处理或分布式执行 |
| 并发 | Ingestion、dbt、外部研究进程按运行约定错开访问同一文件；未实现锁协调服务 |
| Serving | dbt 默认 View 是已确认路线；环境脚本验证过 dbt View，但尚无正式 Serving/Mart 模型 |
| 历史与运维 | 未实现 PIT、全面历史版本、自动备份恢复、调度、补数规划或增量修订重算 |

后续接入 Dataset 时，先确认 Schema / Record Key、依赖与 Scope、Unit、完整替换范围、零行语义、交付证明及 ERROR / WARNING 规则，再实现专属 Dataset 与 Adapter。沿用现有内核；需要改变共享接口或冻结语义时，先提出方案确认。

V3.1.1 的[原始设计稿](个人量化数据库架构冻结稿_V3.1.1_增量修订版.md)和[归档及 invariant 对照](archive/core_design_v3.1.1.md)作为历史参考。其中“尚未验证”等进度及早期供应商假设不作为当前能力说明；当前实现以本文及代码为依据，架构约束以核心架构为准，发现冲突时需显式讨论。
