# index_daily 接入提案

日期：2026-10-22。状态：**用户已确认提案及“1次申万＋24次宽基”的策略修订。** 当前契约允许申万 Raw 超集，仅发布固定 scope 内的代码日。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[index_basic 当前契约](index_basic_ingestion_proposal.md)、[质量对照报告](../research/galaxy-vs-tushare-daily-quality-2026-10-22.md)、[指数研究](../research/tushare-official-st-index-2026-10-20.md)。

## 1. 已确认及证据

- 用户已确认指数日线采用 Tushare，发布范围仅限 index universe；本轮另确认申万按日一次获取，允许 Raw 超集后按固定 scope 筛选。Universe 沿用已发布 index_basic，不改动 24 宽基＋申万类别／发布方筛选政策。
- index_daily 不含申万，申万需用 sw_daily；官方 [index_daily](https://tushare.pro/document/2?doc_id=95)及 [sw_daily](https://tushare.pro/document/2?doc_id=327)的字段、单位不同，不能将同名 vol/amount 原样混入统一单位列。
- 旧研究已证实指数历史可早于 list_date，index_basic 当前集合不是历史全集；close/OHLC 可有合法 NULL，不能照搬股票日线必填政策。
- 申万基础列表有 769 个代码，但一个样本日只有 413 个行业代码有行情；它包含历史／终止版本，不意味着每天所有代码必有行。不能仅因空响应就断言当日无指数或数据完整覆盖历史。
- 对照脚本以 daily 字段名单请求 sw_daily，合并样本没有 pre_close 和 pct_change，适合价格对照但不能充当统一 11 列的完整交付证据。正式实现将显式请求正确的 pct_change，且不伪造 pre_close。
- 本轮新信封 `data/probes/daily-onboarding-20261022T012318968173/`：000300.SH 的 index_daily、801010.SI 的 sw_daily 各返回 2026-10-21 一行；后者正确请求 10 字段，包括 pct_change，均无缺字段、has_more=false。未写数据库。

## 2. 逐项契约建议

| 接入项 | 项目建议 |
|---|---|
| 业务事实 | 同一个 index_daily Dataset 统一保存目标指数的供应商日行情；不新增 sw_daily Dataset，不合并同名不同代码，不补前收盘或停牌行 |
| 表与键 | canonical.index_daily；Record Key=(ts_code,trade_date)，重复／冲突阻断所属 Unit |
| 输入 | 显式 start_date/end_date 闭区间，拒绝未来、允许上海当日但不保证终态；可重复 --ts-code 选择已在 universe 的代码，不传则全部，越界代码获取前拒绝 |
| 上游依赖 | index_basic / SNAPSHOT 的 PUBLISHED 版本，加每个自然日 trade_calendar / SSE:YYYY-MM-DD；同一事务读取实际事实并固定版本，提交前全部复核，不自动运行上游 |
| Universe / Scope | 只取本次固定快照的代码；当前支持的宽基和申万都是境内市场，本提案明确选择 SSE 共同日历；开市代码日 IN_SCOPE、休市代码日 OUT_OF_SCOPE，缺日历事实／不支持路由为 UNKNOWN |
| 时间边界 | 不用 list_date、base_date 或 exp_date 裁掉请求历史；保留终止指数的历史请求，不把当前 universe 当成完整历史全集 |
| Publication Unit | 一个指数的一个交易日，标识 ts_code:YYYY-MM-DD；同一日不同指数可以独立合格／失败，按现有许可集合整 Unit 替换 |
| 获取 | 宽基按代码自然年请求 index_daily(ts_code,start_date,end_date)；申万每个开市日一次 sw_daily(trade_date)，不传 ts_code。不获取非申万全市场，不依赖逗号多代码 |
| 来源路由 | 申万 .SI 使用 sw_daily，其他当前已支持宽基用 index_daily；解析范围时核对 market 与路由一致，不默默接入新的指数族 |
| 交付 | 分别要求 false 标记、低于 index_daily 8,000 / sw_daily 4,000 行；宽基失败影响对应代码年片，申万日表失败影响当日所有选中申万 Unit，继续其他请求；不引入通用分页框架 |
| 准备 | 宽基核对请求代码／区间；申万核对 .SI 代码和可信请求日。合法但不在本次 scope 的申万行留 Raw 并记排除审计，非法日期／代码不能借筛选掩盖；仅目标记录进入 Candidate |
| 更新与修订 | 同内容重跑仍产生新版本，删除该代码日旧行；失败保留旧成功版本。Universe 变更在提交前阻断重算，不自动删除不再属于当前 universe 的历史行情 |
| 入口与消费 | ingest index_daily --start-date ... --end-date ... [--ts-code ...]；Coverage 的成功范围为代码日，必须结合固定 universe、获取证据及零行约定解释缺记录 |

Scope 用现有字符串 Unit 表达代码日，不为传递 universe 新增 Scope payload、控制模型或可变 Dataset 状态。指数代码已经包含在 Unit，Adapter 无需自行读取另一份数据库 universe；upstream pin 指向实际读取的快照。

默认单个交易日的完整 universe 请求数为24个宽基＋1个申万日表，即25个逻辑请求。多日范围中，宽基仍按代码年片合并，申万按交易日获取；若 --ts-code 子集没有申万，则不发申万请求。一次申万请求关联当日所有选中的申万代码日，包括最终零行 Unit。

## 3. Schema、可空性与单位

建议共 11 列：ts_code 必填非空白 VARCHAR，trade_date 必填 DATE；open/high/low/close/pre_close/change/pct_chg/vol/amount 为**可空 DOUBLE**。日期为供应商业务日，指数点位不作货币换算，pct_chg 为百分数。

| 源接口 | 字段映射 | 建议 Canonical 单位 |
|---|---|---|
| index_daily | 正常 11 字段；不改变原点位、百分数及合法 NULL | vol 手，amount 千元，源值不换算 |
| sw_daily | 显式 10 字段，pct_change→pct_chg；没有 pre_close，明确映射为结构性 NULL，不从 change 或历史行推导 | vol 万股×100→手；amount 万元×10→千元 |

SW 的 pre_close=NULL 是源能力缺失的已声明映射，不是缺交付列补 NULL；其余必需源列缺少仍 ERROR。两种 Raw 都保留各自原字段／原单位和可信来源标签，Canonical 才使用统一单位。仅请求目标常规行情字段，不接入 SW name、pe/pb、市值等扩展属性。

Raw 的 `_code` 在宽基行是可信请求代码，在申万日表行是有明确 String 类型的 NULL；`_start/_end` 对申万均为请求日。`source.scope_excluded` WARNING 记录请求 ID、排除行数及理由“Outside selected code-day scope”，关联该日目标 Unit。它也覆盖已经在 universe、但不在本次 --ts-code 子集的记录；不将这些记录误记为转换失败或默默丢弃。日期或代码协议不合法的行按 source.range ERROR 阻断整次申万日请求的目标范围。

数值非空时必须有限；非空价格须为正（pre_close 允许零），量额不得负；四价齐全才检查 high/low 关系。任一记录九个数值全部 NULL 为 ERROR。部分合法 NULL 保留；不因 SW 必然缺 pre_close 逐条制造告警。空键、重复、范围／类型／转换错误均阻断对应 Unit，无法定位时阻断请求片段。

量额转换依据官方声明，质量报告没有独立证明所有指数族量额口径完全可比；本 Dataset 不据统一单位强制跨指数求和或与银河比较作为硬门禁。

## 4. 已确认的零行政策

**允许完整验收的代码日零行发布，删除该代码日旧行情并生成 row_count=0 的 Publication / PUBLISHED Coverage。**

保证边界仅为“固定 universe 内该代码在该日，本次供应商完整响应没有报告行情”。它不证明真的没有交易、不把未知历史覆盖改写为退市事实，也不证明指数有完整历史。选择此语义可覆盖当前 universe 中大量历史／终止申万代码，不必令常态运行因这些代码永久 PARTIAL。

零行必须具备明确 Schema、源请求完成及 false 终止标记、字段齐全、没有阻断性准备错误。申万零行由完整当日日表中未出现该目标代码来表达，不再要求逐代码 HTTP 请求；保证边界仍仅是本次供应商未报告。请求失败、无法确认结束、坏值转换后零行不允许发布。休市排除不发布零行版本。

当前实现不自动剔除零行代码，不按 list_date/exp_date 裁历史，也不将完整零行改记请求失败或 UNKNOWN。

## 5. 实施及必要验证

新增 index_daily Dataset/DDL/Adapter、CLI 入口及必要测试；stock_daily 为另一个专属切片。沿用现有 Protocol、Runner、Client、Validator、Publisher 和 Meta，不引入注册器、基类、规则框架或新生产依赖；少量重复的数值解码可以用普通函数复用，不建立行情框架。

1. 同库发布 SSE 日历及真实 index_basic；缺 universe／日历、目标越界、版本变化都在获取前／提交前阻断。宽基只请求选中代码，申万允许日表超集，但只能发布选中 universe 代码日。
2. 覆盖宽基与申万正确路由、源 pct_change 映射、pre_close 结构性 NULL、量额换算、合法数值 NULL、错代码／错日隔离、重复键与年片失败。
3. 零行成功替换及清旧，和请求／准备失败导致的零行分别测试；部分成功及事务回滚沿用内核。
4. 使用真实宽基和申万新信封重放；质量对照缺字段样本不能补列冒充正式交付。对正确源值、Candidate、Canonical 逐列及单位核对。
5. 独立库真实小区间验收：先用 universe 内数个宽基／申万代表代码验证休市及历史零行，再用完整 universe 单日验证批量请求、重跑、失败保护、Coverage／Publication 和文件哈希。不把单日验收写成全历史下载。

首版 --ts-code 子集能力既用于按需补数，也使验收不必为每次几日样本请求全部 793 个代码；它不能绕过 universe。数据不加入默认权威库，不改写另一 agent 的研究脚本或报告。

## 6. 契约确认记录

1. 首次确认按代码日 Unit、固定版本 index_basic＋SSE 日历、逐代码年片请求实现；允许 universe 内显式 --ts-code 子集，不按基础日期裁历史。随后确认申万改为按日一次请求，宽基保留代码年片，详见第8节。
2. 允许完整代码日零行成功并清除旧行，成功只表达本次供应商未报告；合法数值 NULL 保留。
3. 统一使用 vol 手／amount 千元；SW 作×100／×10 换算，pct_change 映射、pre_close 为空，不新增复权或估值字段。

用户在本次会话明确回复“提案确认，开始实现”，以上 Unit、输入、空集合与规范单位均获确认。实现未改冻结内核、共享 Protocol 或 Meta；仅复用日线普通函数，不引入继承体系、注册器、通用分页或新依赖。

## 7. 实现与首次真实验收

代码见 [Dataset](../../src/quant_data/datasets/index_daily.py)、[DDL](../../src/quant_data/datasets/index_daily.sql)、[Adapter](../../src/quant_data/sources/tushare/index_daily.py)、[CLI](../../src/quant_data/__main__.py)及[测试](../../tests/test_daily.py)。Raw 保留源 pct_change、源量额和 API／代码／请求区间标签；两源并置产生的结构性 NULL 与缺必需字段的错误分开处理。

新增 10 项两个日线共用的正式链路测试通过，全套 82 项中 81 通过、1 项既有股票旧样本缺失跳过，环境烟测通过。测试覆盖默认选择全部已发布代码、受限子集、路由／字段、SW 单位、部分 NULL、零行清旧、跨年拆分／局部失败、超范围／转换故障隔离、上游版本复核、回滚和 CLI。新真实指数／申万响应重放已执行。

首次逐代码策略的真实验证保留在独立库 `data/probes/daily-ingestion-validation-20261022T014518303442/warehouse.duckdb`：先正式发布 SSE 日历和当时的 index_basic（793 行、Publication 5），再选择四个代表代码 `000300.SH`、`000906.SH`、`801010.SI`、`801013.SI` 获取 2026-10-16–10-19。下表是该次历史验收，不作为本轮申万批量请求数的证据。两个闭市日明确排除，固定依赖同时包含它们。

| 正式 CLI 执行 | 结果 |
|---|---|
| 四代码首跑 | 4 个逐代码年片请求，3 个代码各有两日行情，共 6 行；8 个成功代码日，其中 801013.SI 两个零行版本 |
| 同范围重跑 | 仍为 6 行、8 个新版本；逐字段一致，不累加，CLI 0、产物完整 |
| 同库发布 2002-01-04 日历后获取 000300.SH | 发布 1 条回填行情，早于基础 list_date=2005-04-08；没有被日期裁掉 |
| 选择 universe 外 A.NH | CLI 1，0 个行情请求、无新 Publication；旧行情及指针不变 |

真实 SW 请求明确使用 pct_change，pre_close 为结构性 NULL，vol×100、amount×10 后与独立标准化源值、Candidate 和已提交 Canonical 逐字段一致。成功的 row_count=0 只说明源无当日报告，不声明历史真实无行情。

另在同库离线注入指数请求失败，FAILED 且保留旧事实、Coverage 和 Publication；明确标为无 HTTP。指数实际覆盖为**四代码小区间及一个历史代码日**，不是793代码全量实盘下载；默认全部 universe 的选择和请求成员限制已由正式链路测试覆盖。

连同股票及上游验证共核对 476 个 manifest 文件条目 SHA-256，全部通过；核对请求、依赖、成功 Unit、Coverage 和 Publication。完整本地目录还包含 summary.json、verification.py、各 CLI JSON 和 landing/<run_id>，未写默认权威库。机器摘要见 [daily-ingestion-summary-2026-10-22.json](../research/daily-ingestion-summary-2026-10-22.json)。

CLI 例子（同库需先有相应日历和 index_basic）：

```bash
uv run --frozen python -m quant_data ingest index_daily \
  --start-date 20261016 --end-date 20261019 --ts-code 000300.SH --ts-code 801010.SI \
  --db data/probes/my-daily/warehouse.duckdb --landing-dir data/probes/my-daily/landing --keep-artifacts
```

首次验收未实施全历史／全 universe 实时下载；本轮完整 universe 单日验收见第8节。历史 PIT、自动 universe 失效传播、盘中终态确认或其他指数族未实现。数量及性能仅代表各次验收样本。

## 8. 申万按日批量策略修订与验收

用户随后明确要求“按1次申万＋24次宽基修改拉取策略”。因此本轮覆盖第2节获取策略，并允许申万 Raw 超集后按固定 scope 筛选；不修改代码日 Unit、Schema、单位、零行政策、universe 固定及提交前版本复核。

可行性补证 `data/probes/index-daily-batch-feasibility-20261022T111154360699/` 显示：2026-10-20 申万全日表439行；宽基逗号双代码和24代码请求均0行，不能作为批量获取。非申万全量为10,535行（8,000＋2,535及空验证页），故宽基仍按显式代码获取，不拉该全市场列表。

源码变更仅限 [指数 Adapter](../../src/quant_data/sources/tushare/index_daily.py)和[日线测试](../../tests/test_daily.py)，另同步本文、README及实施记录。新增两个必要测试覆盖单日默认25请求、Raw超集／子集排除、申万日请求／日期／触顶故障对整日成员的阻断，以及宽基和其他日的隔离。原有10项日线测试仍通过；全套 **84项中83通过、1项既有股票真实样本缺失跳过**，环境烟测通过。内核、共享模型、DDL、Dataset和CLI接口无本轮变更。

真实验证在新建独立库和产物目录：

```text
data/probes/index-daily-batch-validation-20261022T113340060289/
  warehouse.duckdb
  summary.json
  verification.py
  landing/<run_id>/
  individual-checks/
```

先正式发布2026-10-20的SSE日历及当前完整index_basic（793代码＝24宽基＋769申万），然后**不传 --ts-code**运行默认完整 universe 单日首跑和重跑：

| 核对项 | 两次真实执行结果 |
|---|---|
| 逻辑请求 | index_daily 24次、sw_daily 1次；申万参数只有 trade_date |
| Raw | 463行＝24宽基＋439申万，源量额和范围外行均保留 |
| 目标 Canonical | 438行＝24宽基＋414目标申万，无范围外新记录 |
| 排除审计 | 每次 source.scope_excluded 记录25行；结合 Raw、scope及pins可重建排除依据 |
| Publication / Coverage | 每次793个成功代码日，其中355个row_count=0；所有成功指针和行数与实际事实一致 |
| 重跑 | 逐字段一致，不累加，产生新版本；CLI 0、产物完整 |

另直接调用真实逐代码 sw_daily 验证801010.SI（1行）和801011.SI（0行），其多重集合与申万日表对应代码子集完全一致；这是额外验收请求，不属于生产25请求计划。

离线重放宽基响应并注入申万日请求失败（明确标注未进行HTTP）：Run为PARTIAL，24个宽基Unit发布成功；769个申万Unit失败，旧申万事实、Coverage指针和Publication完整保留。这不把网络失败当作完整零行，也不污染另一组。该次按现有内核语义标记 artifacts_complete=false（PARTIAL 不标完整），manifest.errors 为空且清单文件哈希均通过；两次真实成功执行的产物均标记完整。

核对源数据独立按scope筛选／标准化的表、Candidate、Canonical、全部代码日行数和成功指针；**316个manifest文件条目的SHA-256通过**，另记录并复核两份逐代码比较响应的摘要。完整单日universe本轮已真实验收；未执行全历史下载。未写默认权威库；所有数据库、原始响应和业务行仅存本地。

机器摘要：[index-daily-batch-summary-2026-10-22.json](../research/index-daily-batch-summary-2026-10-22.json)。此前四代码子集验收和476文件记录属于首次逐代码策略，保留其原始证据，不与本轮请求数或覆盖范围混同。
