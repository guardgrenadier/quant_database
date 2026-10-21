# index_basic ingestion 接入提案

日期：2026-10-21。状态：**末尾三组建议已获用户确认，正式实现与独立库验收已完成。**

依据：[核心架构](core_design.md)、[当前实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[协作规则](../../AGENTS.md)。

本文区分已有架构约束、供应商声明、样本观察和项目选择；证据不自动成为业务政策。第 2 节保留提案时的“建议”措辞作为决策来源，其内容已由用户在本次会话整体确认（见第 3 节），并作为正式实现依据。

## 1. 已确认结论与证据

### 1.1 已确认的项目约束

- 使用现有 Dataset / SourceAdapter Protocol、Client、Runner、Validator、Publisher、Meta 与产物接口；本提案不需要修改冻结架构或共享接口。
- 获取 COMPLETE、准备成功、业务验收通过和事务发布成功分别表达。Publisher 只发布显式许可 Unit；重跑替换、失败保护旧数据及成功版本沿用内核。
- Raw 与请求信封保留源表示；每条源记录进入 Candidate 或有明确诊断，不去重、不静默丢行、不补造属性。
- 真实验收使用新建的独立数据库与产物目录，不写默认权威库。
- 先确认本提案中的业务契约，再完成 Dataset、DDL、Adapter、CLI、必要测试和文档更新。

### 1.2 研究材料与适用范围

已阅读 [2026-10-20 报告](../research/tushare-official-st-index-2026-10-20.md)及其[机器摘要](../research/tushare-official-st-index-summary-2026-10-20.json)，并核对 [2026-10-14 报告](../research/tushare-official-daily-index-2026-10-14.md)的 index_basic 章节。后者的“index_type 全空”来自局部样本，不能作为全量结论；前者明确所有建议尚未冻结。

供应商文档依据为研究时保存的官方 [index_basic 文档](https://tushare.pro/document/2?doc_id=94)，本地快照 `data/probes/st-index-20261020-docs/index_basic.md`；摘要 documents 中记录其 URL、访问时间与 SHA-256。提案建立时未重新联网更新文档；后续实时计数及正式验收见下文。

| 证据类别 | 已知结果 | 契约影响 |
|---|---|---|
| 供应商声明 | 13 个输出字段，单次 8,000 行，每日 18 点更新，起始时间 1990-01-01；market 默认 SSE | 更新时间、默认范围及历史覆盖不能仅凭文档推断实际交付 |
| 实测全量 | 不传业务过滤，limit=1,000、offset 递增；12 个满页 + 599 行终止页 + 后续空页，共 12,599 行 | 需要分页；默认不是仅 SSE，不能按文档市场枚举拼装全量 |
| 实测 market | SSE 208、SZSE 485、CSI 9,053、CNI 1,134、BSE 2、SW 950、NH 102、CI 665 | 包含期货、行业及多资产指数；八种值为观察结果，不能自动冻结为枚举 |
| 实测唯一性 | ts_code 全量唯一，CSI 独立分页与全量中 CSI 分区多重集合相同 | 支持 ts_code 候选主键，不证明后续响应永久无重复 |
| 实测空值 | index_type 12,497、base_date 126、base_point 128、list_date 124、weight_rule 320、desc 102、exp_date 11,809 个 NULL | 字段必须交付，但这些值不能统一必填；102 条 NH 的 index_type 有值 |
| 实测终止记录 | exp_date 有值 790 条 | 不应默认过滤已终止指数 |
| 协议观察 | 成功响应 count 为 0；小页支持 has_more，默认输出只有八列 | 不用 count 判断完整性；显式请求全部字段，缺列不能补 NULL |

本轮只读复核了摘要 `groups.ib_pages` 所列的 14 份原始响应，全部文件 SHA-256 与摘要 attempts 中记录一致。进一步统计得到以下事实（未改写原研究报告）：

- `base_point` 非空最小值为 0，最大值为 15703.298；5 条为 0：`931997.CSI`、`H30401.CSI`、`H30402.CSI`、`H50059.CSI`、`H50060.CSI`。不能引入 `base_point > 0` 硬约束。
- 166 条 `base_date > list_date`；例如 `000300CAD13.CSI` 为 2012-04-27 / 2005-04-08。不能引入“基期不得晚于发布日期”硬约束。
- 样本未见 `exp_date < list_date` 或 `exp_date < base_date`，也未见负基点；这些观察不足以证明通用不变量。

上述复核输入为 `data/probes/st-index-20261020-main/requests/ib_pages-p000-attempt-01.json` 至 `ib_pages-p013-attempt-01.json`。摘要全量源记录指纹为 `c2543f18dde1820fe715afb92fc77dbe092a5edffc625fc03ed4054995c14859`。

2026-10-21 22:09（Asia/Shanghai），按用户要求重新调用官方接口核对当前数量：不加市场或代码过滤，显式 13 字段，1,000 行分页，共 14 个请求（12 个满页、599 行终止页和一个空验证页）。返回 **12,599 行、12,599 个唯一 ts_code，重复键为 0**；八种 market 及数量与上表一致，分页明确终止，14 份响应证据哈希均核对通过。执行仅保存独立探查产物，未连接数据库；这次计数不构成正式 ingestion 验收，也不代表待决业务政策已获确认。证据见本地 `data/probes/st-index-20261021T220900218316/summary.json` 及同目录 `requests/`。

### 1.3 最相近实现与测试

已阅读 [stock_basic Dataset](../../src/quant_data/datasets/stock_basic.py)、[DDL](../../src/quant_data/datasets/stock_basic.sql)、[Adapter](../../src/quant_data/sources/tushare/stock_basic.py)、[正式链路测试](../../tests/test_ingestion.py)、[CLI](../../src/quant_data/__main__.py)和 Publisher。它提供单 SNAPSHOT、无依赖、整表替换的机制参考；股票状态、交易所枚举、至少一条 L 等业务政策不适用于指数。

分页证据参考 [研究探针](../../scripts/probe_tushare_st_index.py)及[证据测试](../../tests/test_st_index_probe.py)。研究脚本不直接成为生产 Adapter，不引入通用分页框架。另已阅读 [stock_st 契约](stock_st_ingestion_proposal.md)，其日历依赖、历史起点与零行政策均不自动继承。

## 2. 逐项接入契约

下列专属契约均已作为本 Dataset 的**项目政策确认并实现**。实现内部细节可按既有协作规则自主选择，但不得改变确认后的业务含义。

### 2.1 业务事实与边界

一行代表 Tushare 当前指数列表中某个 ts_code 的基础属性。建议保存**供应商不加业务过滤可取得的完整当前快照**，包含所有返回 market、不同资产类别及已终止指数。

这是抓取时点最新属性，不是历史每日快照、指数行情、成分、权重或 PIT 事实。不从代码后缀生成交易所、不按当前股票集合筛除、不以 list_date 决定其他 Dataset 的历史起点；跨 Dataset 派生留给 Serving。

证据缺口：供应商未提供跨分页的快照版本令牌，也未独立证明该接口覆盖现实世界所有指数。成功只保证满足约定请求计划和验收规则。

### 2.2 Schema、类型与空值

建议 Dataset 为 `index_basic`，表为 `canonical.index_basic`，字段顺序保持下表。所有 13 个字段均须真实交付，包含空值的列也不能省略。

| 字段 | Canonical 类型 | 建议业务约束 |
|---|---|---|
| ts_code | VARCHAR | 必填、非空白、主键；不限定六位数字或后缀 |
| name | VARCHAR | 必填、非空白 |
| fullname | VARCHAR | 可空，缺失 WARNING |
| market | VARCHAR | 必填、非空白；保留开放字符串，不设枚举 |
| publisher | VARCHAR | 必填、非空白；保留整体文本，不拆发布机构 |
| index_type | VARCHAR | 可空；不设枚举，NULL 不单独告警 |
| category | VARCHAR | 必填、非空白；保留开放字符串，不设枚举 |
| base_date | DATE | 可空；非空须为有效 ASCII YYYYMMDD 源日期 |
| base_point | DOUBLE | 可空；JSON 数值且非布尔；非空须有限；0 合法保留，负值 WARNING |
| list_date | DATE | 可空；供应商发布日期，不据此裁剪其他历史事实 |
| weight_rule | VARCHAR | 可空，缺失 WARNING |
| desc | VARCHAR | 可空，缺失 WARNING；列名按现有 SQL 标识符引用函数处理 |
| exp_date | DATE | 可空；供应商终止日期，NULL 不直接派生另一套存续状态 |

base_date / base_point / list_date 缺失建议汇总 WARNING；index_type 与 exp_date 的合法空值不告警，避免把正常可空误作异常。name、publisher、category 的必填是本项目建议；供应商输出表中的“必选”与样本无 NULL 不能代替项目确认。

日期仅表达供应商业务日，不附加 UTC 时刻；跨市场日期保留源日期，不统一换成上海交易日。抓取时间使用现有 RequestAttempt / Run 审计时间，不加入 Canonical 业务字段。base_point 保留源指数基点数值，不作货币、比例或精度换算。

证据缺口：未获得负基点、非有限数或异常日期先后的供应商解释；不以未经确认的数学/时间顺序假设阻断真实样本。

### 2.3 Record Key

建议唯一键为 `(ts_code,)`。同键完全重复和同键属性冲突均 ERROR，阻断整个快照；不去重、不以 market 扩充键来容纳重复。由 Dataset 规则及 DDL 主键落实。

### 2.4 输入与 Scope

建议 `ingest index_basic` 不接受日期、market、ts_code、symbol、category 或其他子集参数；Dataset 对非空 params 明确拒绝。CLI 沿用 `--db`、`--landing-dir`、`--keep-artifacts`。

Scope 只有 `SNAPSHOT` 一个 IN_SCOPE Unit，不产生按交易日的排除或 UNKNOWN 日期集合。不将供应商“1990 起始”转成历史参数或过滤；未来 list_date / base_date / exp_date 保留并告警，不采用 stock_st 的未来日期拒绝政策。

### 2.5 上游依赖

建议无上游依赖，不读取 trade_calendar 或 stock_basic，不生成依赖版本 pin。此接口跨市场且为当前快照；交易日历不决定其获取范围。现有依赖复核机制保持原状，本切片不人为制造上游依赖测试。

### 2.6 Publication Unit 与写入政策

建议唯一 Unit 为 `SNAPSHOT`，与 market、分页、文件数量独立。所有页完成获取、准备及验收后，整张 `canonical.index_basic` 原子替换。

新快照中消失的旧代码会删除；即使内容相同，重跑仍产生新 Publication 并切换 Coverage。任何页失败、准备 ERROR 或业务 ERROR 均不发布局部市场/局部页面。事务失败保护旧 Canonical、Coverage 和 Publication，不修改其他 Dataset。

### 2.7 零行与缺失语义

建议完整快照零行时 Dataset 验收 ERROR：不清空旧表，不生成成功空版本，首次失败只留下失败审计和无成功指针的 Coverage。这是明确的项目发布限制，不是从股票至少一条 L 的政策推导。

即使响应为空，只要分页计划确已完整完成，获取仍为 COMPLETE，并保留 13 列明确 Schema 的空 Raw / Candidate，由业务非空规则阻断。缺字段、不能解码或转换失败不能用合法空 Candidate 掩盖。

消费者需检查 `index_basic / SNAPSHOT` 的 PUBLISHED Coverage。没有成功 Coverage 表示无权威快照；成功快照内缺某个代码只表示供应商该次列表未报告该代码，不能证明历史上不存在或不被其他接口覆盖。

### 2.8 获取与完整性证据

建议实现 Dataset 专属分页循环，不传 market 等业务过滤，仅传 `limit=1000`、`offset=0,1000,...`，显式请求全部 13 字段。逻辑请求起始间隔至少 1.3 秒；超时与有限重试沿用 Client。每次尝试即时保存脱敏信封与请求审计。

建议首版固定交付协议：

1. 按返回字段名重排，不要求列顺序稳定；每页须含全部 13 字段，不用默认八列补 NULL。
2. 满 1,000 行且 has_more=true 时继续；少于 1,000 行且 has_more=false 时为候选终止页。
3. 候选终止页之后，offset 再增加 1,000，请求一个验证页；只有零行且 has_more=false 才确认终止。验证页同样要求完整字段。
4. 缺失 has_more、超过页大小、非约定的行数/标记组合、验证页非空或标记异常，均 INCOMPLETE / ERROR；不读取 count 决定完整性。
5. 最多 100 次逻辑页请求（包含验证页）；耗尽仍未证实终止为 INCOMPLETE / ERROR。这是首版运行预算，不宣称供应商有 100,000 offset 上限；不外推 index_weight 的限制。
6. 请求终态异常为 FAILED；立即停止后续页，不用部分获取替换快照。缺列/Raw 类型错误归 prepare 诊断，不把已经完成的 HTTP 请求改记失败；只要交付标记允许，继续检查剩余获取计划。
7. 不静默去重。重复页导致的重复键由 Dataset 阻断；持续返回满页最多运行到页预算后明确失败。

证据缺口：现有全量证据覆盖到 offset=13,000，不是任意规模的分页保证。无版本令牌时，分页期间源数据变化可能导致重复或遗漏；重复键可以发现，遗漏不能全面检出。不会用固定 12,599 行、固定八种市场或数量阈值声称业务完整。

### 2.9 准备与标准化

Raw 保留全部源字段，三个日期仍为字符串，base_point 为数值，附可信 `_requested_offset` 请求页标签便于定位。源文本只接受字符串/NULL；base_point 只接受 JSON int/float/NULL，拒绝 bool、字符串数值等隐式转换。每页解码失败有明确 ERROR，原值仍在信封。

prepare 一次清理文本首尾空白，空串转 NULL；不改代码大小写、不补后缀、不改名称或描述正文。三列日期各解析一次，分别识别合法 NULL、ASCII 八位格式错误与历法错误，非法非空日期不能降格成 NULL。数值转 DOUBLE，不舍入或填零。

Candidate 仅保留 13 个正式字段。全部错误归属同一个 SNAPSHOT；任何阻断性转换错误都阻断整个快照，不以安全子集发布。Raw 保持原值，诊断包含字段、请求/页范围及数量。

### 2.10 统一验收规则

| 级别 / 归属 | 建议规则 | 影响 |
|---|---|---|
| ERROR / Adapter | 请求失败、分页未证实终止或交付标记异常 | SNAPSHOT |
| ERROR / Adapter.prepare | 缺列、非法 JSON 类型、非法日期解析 | SNAPSHOT；交付完成仍可记 COMPLETE |
| ERROR / Dataset | ts_code/name/market/publisher/category 缺失或空白 | SNAPSHOT |
| ERROR / Dataset | ts_code 重复、完整 Candidate 零行 | SNAPSHOT |
| ERROR / Dataset | 非空 base_point 为 NaN 或正负无穷 | SNAPSHOT |
| WARNING / Dataset | 可选描述或基期/基点/发布日期缺失，按字段汇总 | SNAPSHOT，保留值并允许发布 |
| WARNING / Dataset | base_point < 0 | SNAPSHOT，保留源值；0 不告警 |
| WARNING / Dataset | 任一业务日期晚于上海当前日期 | SNAPSHOT，保留未来公告值 |
| WARNING / Dataset | exp_date 早于非空 list_date 或 base_date | SNAPSHOT，保留源值，等待业务解释 |

`base_date > list_date` 保留，不加错误或常态告警；market/category/index_type 不设固定枚举，也不按后缀校验市场，不设置每个市场至少一条的门禁。通用 Schema、归属、交付门禁和最终许可仍由 Validator 负责；不在多个阶段重复定义同一业务规则。

### 2.11 更新与修订口径

建议显式重跑刷新整个当前快照，接受供应商属性修订、新增和消失。18 点是供应商文档的更新声明，不作为自动调度、禁止提前获取或终态保证；不添加未经验证的快照日期参数。

不保存历史行镜像，Publication 记录成功变更版本，不支持按旧 publication_id 还原旧属性。不承诺跨页同一时点、历史当时可知或无真实遗漏。

### 2.12 CLI、实现文件与验收证据

已新增 `datasets/index_basic.py`、`datasets/index_basic.sql`、`sources/tushare/index_basic.py`、`tests/test_index_basic.py`；已修改 CLI 显式装配及 README、当前实施文档和核心架构的已实现清单。未引入新的生产抽象、依赖包、注册框架或共享控制模型。

验收清单（执行结果见第 4 节）：

1. 正常多页、终止验证页及返回列重排；同内容重跑新版本、不累加；新快照删除消失代码；新 market、NH 及已终止指数保留。
2. 空快照、重复键、缺列、数字代码、字符串/布尔基点、非法日期、非有限基点及必填属性缺失；核对 ERROR 的阶段与 COMPLETE 的独立性。
3. 请求失败、has_more 缺失、页边界/终止页矛盾、持续重复满页及预算耗尽；逐例保护旧事实、成功 Coverage 指针及 Publication。
4. 可空字段、0 基点、base_date 晚于 list_date、未来日期及负基点/终止日期顺序告警；不静默纠正源事实。
5. 真实已保存 14 页响应离线重放，与源行经约定标准化后逐字段比较；核对 Raw 保留、Candidate Schema、元数据及证据。
6. 在 `data/probes/index-basic-ingestion-validation-<timestamp>/` 新建 warehouse.duckdb 和 landing，正式 CLI 首次获取及重跑，保存完整产物，核对 Canonical、Publication、Coverage、RequestAttempt、检查与 manifest 哈希。真实两次抓取如有源修订，应记录差异，不误当重跑追加。
7. 真实成功版本之后，在同一独立库用**明确标识的离线故障注入**验证失败保护；与真实 HTTP 验收区分，不把替身响应当供应商实测。
8. 跑必要 Dataset 测试、既有回归和环境烟测，逐项记录跳过/无法执行项及原因；既有通用事务回滚、证据故障等测试复用，不复制整套内核机制测试。

快照接口不接受业务日期范围；本任务的“真实小区间验收”建议对应**一次完整快照及一次重跑**。不能为了缩小验收成本而用一个指数或一个市场子集冒充完整快照成功 Coverage。全历史下载、指数日线/成分/权重、Serving、PIT、自动调度和断点恢复均不在首版范围。

## 3. 契约确认记录

用户于 2026-10-21 在本次会话明确回复“确认提案末尾三组建议，请开始落地”。以下三组因此作为 index_basic 正式契约，不再是待决事项；此前研究材料没有自行冻结这些政策。

1. **目标与发布政策**：全供应商当前列表，包含所有返回 market 和终止指数；无依赖、无筛选参数；ts_code 主键、单 SNAPSHOT 整表替换；零行阻断。真实验收用完整快照及重跑替代日期小区间。
2. **字段与空值政策**：保留 13 列；ts_code/name/market/publisher/category 必填，其余按表可空；market/category/index_type 开放字符串；可选描述及基期/基点/发布日期缺失汇总告警，index_type/exp_date 正常 NULL 不告警。
3. **数值、日期与交付政策**：0 基点合法，负值告警，非有限数阻断；日期解析错误阻断，未来日期及终止日期逆序告警，不约束 base_date≤list_date；采用 1,000 行分页、显式终止后空页验证和 100 次请求预算。

后续如需部分市场，仍须先明确完整目标集合、替换边界及 Unit 语义，再变更契约；当前实现不提供子集过滤。

## 4. 实现与验收结果

### 4.1 实现及必要回归

正式实现见 [Dataset](../../src/quant_data/datasets/index_basic.py)、[DDL](../../src/quant_data/datasets/index_basic.sql)、[Adapter](../../src/quant_data/sources/tushare/index_basic.py)、[CLI](../../src/quant_data/__main__.py)及[测试](../../tests/test_index_basic.py)。获取计划与页终止检查在 Adapter；准备一次清理文本、解析三列日期并保留诊断；Dataset 做标准业务验收；Publisher 继续使用原内核单事务替换和成功元数据。

新增 11 项测试全部通过，涵盖第 2.12 节边界、分页失败及预算、完整零行、重复/冲突、源坏值、可空/零基点/日期规则、Raw 与证据保留、发布回滚、DDL 最终防线及 CLI。14 页已保存官方响应的重放已实际执行，12,599 条记录与独立按契约标准化的源值逐字段一致。真实重放测试在 Git 克隆缺少本地样本时会显式跳过，不使用伪样本冒充。

```bash
uv run --frozen python -m unittest tests.test_index_basic -v
uv run --frozen python -m unittest discover -s tests -v
uv run --frozen python scripts/check_environment.py
```

全套回归 **76 项：75 项通过、1 项跳过**。跳过项为既有 stock_basic 的真实快照重放，本地旧样本缺失；本次 index_basic 的真实重放未跳过。环境烟测（既有正式 DDL、Arrow/Parquet、DuckDB、dbt View）通过；index_basic 正式 DDL 在新增测试和真实运行中另外验证。批量机制 100 / 5,000 Unit 的 SQL 调用数仍均为 28；不是指数快照性能承诺。

### 4.2 独立库真实 CLI 验收

2026-10-21 在全新目录运行正式 CLI 两次，均使用官方 Client、真实 HTTP 和完整 13 字段；没有股票/市场/日期筛选。独立库与产物路径：

```text
data/probes/index-basic-ingestion-validation-20261021T222359077676/
  warehouse.duckdb
  summary.json
  verification.py
  live_first.stdout.json
  live_rerun.stdout.json
  landing/<run_id>/
```

| 执行 | 获取与验收 | 发布结果 |
|---|---|---|
| 正式 CLI 首次获取 | 14 次请求，12×1,000 + 599 + 0，获取 COMPLETE，无 ERROR | SUCCESS / CLI 0；SNAPSHOT 12,599 行，Publication 1 |
| 正式 CLI 相同目标重跑 | 同样 14 次请求，12,599 个唯一代码，源值与首次逐字段一致 | SUCCESS / CLI 0；仍为 12,599 行，无重复累加；Publication 2 |
| 离线空响应故障注入（未做 HTTP） | 两个空页、获取 COMPLETE、13 列空 Candidate；index.nonempty / validation ERROR | FAILED；无新 Publication，旧事实及 Coverage 指针 2 完整保留 |
| 离线请求失败注入（未做 HTTP） | 获取 FAILED、source.request / fetch ERROR；合成证据明确标注注入 | FAILED；无新 Publication，旧事实及 Coverage 指针 2 完整保留 |

真实快照市场分布仍为 CSI 9,053、CNI 1,134、SW 950、CI 665、SZSE 485、SSE 208、NH 102、BSE 2。两次正式执行均只有 `index.optional_nulls` WARNING，缺失统计为 base_date 126、base_point 128、list_date 124、weight_rule 320、desc 102；不存在业务 ERROR。

逐字段核对了请求源值经独立标准化后的表、Candidate、已提交 Canonical 及发布后 Canonical 导出；核对了 SNAPSHOT 的 Coverage、Publication、成功/失败 run_unit、请求审计及空依赖审计。两个成功 Run 的 `artifacts_complete=true`；两个预期失败 Run 为 false 且证据保留。共核对 **197 个 manifest 文件条目的 SHA-256**，全部一致。

验证驱动 `verification.py` 为本地可丢弃验收工具，不进入生产代码；其中两次故障注入明确区分于真实供应商行为。机器可读摘要见 [index-basic-ingestion-summary-2026-10-21.json](../research/index-basic-ingestion-summary-2026-10-21.json)。原始业务行、数据库与请求信封仅在本地，不入 Git；全程未写默认权威库。

### 4.3 运行方式与限制

```bash
uv run --frozen python -m quant_data ingest index_basic \
  --db data/probes/my-index-validation/warehouse.duckdb \
  --landing-dir data/probes/my-index-validation/landing --keep-artifacts
```

本次没有新增共享接口、控制模型、依赖包或架构假设；核心文档只更新已实现清单和 Unit 示例。零行阻断、开放分类、数值/日期规则与分页均按已确认提案实现。

供应商跨页一致时点、现实指数全集、永久 offset 能力和历史 PIT 等证据缺口仍然存在；成功不承诺这些性质。未做长时间限流压力、供应商真实故障触发、盘中更新轮询、历史版本恢复或其他指数 Dataset 接入。单快照验收按确认方案替代日期小区间，无未完成的本切片必需步骤。
