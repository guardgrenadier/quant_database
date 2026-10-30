# daily_basic 接入提案

日期：2026-10-30。状态：**用户已确认落地，正式实现、回归与独立库验收完成。**

用户在本会话阅读提案与差异说明后明确要求“好的，现在请你落地daily_basic”。首版保存 **默认 18 字段**，按交易日获取全市场并完整替换；非默认 `limit_status` 因已发现历史语义冲突暂缓纳入。下述业务政策已作为本 Dataset 契约执行，共享架构与 invariant 沿用现有约定。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[官方文档](https://tushare.pro/document/2?doc_id=32)、[旧探查](../research/tushare-official-daily-index-2026-10-14.md)及[本轮机器摘要](../research/daily-basic-onboarding-summary-2026-10-30.json)。

## 1. 官方声明与本轮证据

官方文档于 2026-10-30 重新核对：提供股票每日基本面指标，交易日 15–17 点更新，每次最多 6,000 行，至少 2,000 积分；股票代码与交易日二选一。更新时段不作为当日数据完整或终态保证。默认列实为 18 列，另有非默认整数列 limit_status；旧研究中的“17 个默认字段”计数不准确，不能直接继承。

提案准备阶段使用现有 `TushareClient` 与证据写入器完成 **15 次真实请求**：13 个主场景，加一项历史状态交叉核对和一项显式 18 字段核对。全部 HTTP 200、供应商 code=0；该阶段未调用 Runner、写入数据库或生成 Coverage。原始证据、统计与哈希清单位于：

`data/probes/daily-basic-onboarding-20261030T081207214252/`

| 场景 | 结果与解释边界 |
|---|---|
| 2026-10-29 全市场，显式 19 字段 | 5,570 行、has_more=false，无重复键，字段齐全；只证明本次交付，不证明所有历史个股无遗漏 |
| 同日显式默认 18 字段 | 同为 5,570 行，与 19 字段响应去掉 limit_status 后逐字段一致；不靠供应商默认列或列序 |
| 同日 limit=3000 分页 | offset=0 为 3,000 行、true；offset=3000 为 2,570 行、false；offset=6000 为 0 行、false。合并与单次响应逐字段一致，无重复键 |
| 同日 daily 对照 | 5,570 个代码日集合相同，close 逐值一致；这是单日交叉观察，不将集合相等升级为全历史硬门禁 |
| 2000-01-04 / 2010-01-04 | 分别 913 / 1,632 行；均可请求所有 19 字段。2000 年 free_share 有 2 个 NULL、turnover_rate_f 有 3 个 NULL，历史起点未精确定位 |
| 300104.SZ，2017-04-17–28 停牌样本 | 完整零行；无行不能单独证明任意证券停牌，首版不补指标或沿用前值 |
| 688981.SH，2020-07-16–24 | 7 行；前五个返回日期 volume_ratio 为 NULL，全部 dv_ratio/dv_ttm 为 NULL；不以零或其他证券数据回填 |
| 920285.BJ，2026-10-23–26 | 2 行；volume_ratio、dv_ratio、dv_ttm 均为 NULL，北交所样本可交付 |
| 无效代码 / 2026-10-25 休市日 | 均为完整零行，获取完成与业务是否可发布分别表达 |

2026-10-29 的非空数据细节：pe 有 1,456 个 NULL、pe_ttm 1,693、pb 57、ps 2、ps_ttm 3、dv_ratio 1,812、dv_ttm 1,837、volume_ratio 2。**估值和分红 NULL 属合法源表示，不按 NULL 比例判质量失败，也不填 0。** 官方明确说明亏损 PE 为空；不据此反推任意缺失的具体财务原因。

同日 ps 存在负值，最低 -12.564；ps_ttm 有 3 条负值，最低 -17.3858。另有 3 条 free_share>float_share（300077.SZ、300142.SZ、301630.SZ）。这些源值保留；不把“所有估值为正”或“自由流通股本必不大于流通股本”设为阻断条件。后一关系仅作为可追溯告警，不擅自归因、修正或裁剪数值。

### limit_status 的独立问题

官方将 limit_status 定义为收盘涨跌状态，枚举 0–6，其中 0 为平盘。本轮 2000-01-04 的 913 行中，887 行为 0；与同日 daily 交叉核对，**其中 882 行的 daily.change 非零**。例如 000001.SZ 的 close=18.29、change=0.84，而 limit_status=0。两次交付均 has_more=false，原始值可追溯。

这表明历史样本不能直接按文档把 0 解释为可靠的平盘事实；尚未查清具体原因或有效历史边界。**已确认首版暂不发布该可选列**，并保留探查响应，不把 0 静默转换成 NULL，也不按 daily 或 stk_limit 重建状态。如果用户确实需要该列，可另确认“保存供应商报告编码、附带历史可靠性限制”的契约，再决定是否纳入；不增加当前共享模型或臆造历史切换日期。

旧研究引用的 daily_basic 原始文件本轮未找到，不能声称已重放旧文件。本轮新信封可作为实现后的真实样本重放输入。

## 2. 已确认的业务契约

| 方面 | 首版建议 |
|---|---|
| Dataset / 表 | `daily_basic` / `canonical.daily_basic` |
| 一行的含义 | 供应商报告的一个证券在一个交易日的收盘价、换手、估值、分红、股本和市值指标；是业务日期上的源事实，不是财务报表或历史可知信息快照 |
| 范围 | 按交易日获取供应商全市场股票集合，不用当前 stock_basic 筛历史，不合并其他资产指标或自动改写证券代码 |
| Record Key | `(ts_code, trade_date)`，同键重复或冲突阻断所属日，不去重 |
| 参数 | 显式 start_date/end_date，ASCII YYYYMMDD 闭区间；拒绝未来日期，允许上海当日但不保证终态；不接受证券、交易所或上市状态子集 |
| 依赖 | 仅同库整个目标自然日区间的 `trade_calendar / SSE:YYYY-MM-DD` 成功版本；不自动运行上游 |
| Scope | 依赖固定与读取同快照：开市 IN_SCOPE、休市 OUT_OF_SCOPE、成功 Coverage 下日历事实缺失为 UNKNOWN；保留用于休市排除的上游版本，发布前统一复核 |
| Publication Unit | 一个交易日的供应商全市场集合，Unit key 为 YYYY-MM-DD；分页只分请求，不拆发布 Unit |
| 替换 | 按日完整替换，重跑产生新版本，不累加；验收许可的多个日期按现有同一次发布事务提交；失败日期保留旧事实与成功指针 |
| 零行 | 开市日全市场零行 ERROR，不清除旧日集合、不生成成功空版本；休市日排除，不请求也不生成本 Dataset 的 Coverage |
| 个别证券无行 | 仅表示本次未报告，不自动解释为停牌、退市或错误；不补行、填 0 或前向沿用指标 |
| 历史 | 接受显式历史日期，2000 年样本不是精确供应商起点；不预设历史完整全集，超出可交付范围的整日空响应明确失败 |
| 修订与可知性 | 支持显式重抓目标完整日；财务口径及历史指标可能变化，成功版本只表示本次发布满足契约，不承诺财报 PIT、历史首次可得时点或当日最终完整 |

不建立 `stock_daily`、`stock_basic`、`suspend_daily`、`stock_st` 或 `stk_limit` 硬依赖。已有日线研究中“当前在市名单加停牌集确定历史应有全集”的建议未冻结，当前快照不足以可靠重建历史全集。日线集合/收盘价对照可作为独立验收证据，不成为本 Dataset 的强制生产前置，也不据此重算供应商指标。

## 3. Canonical Schema 与单位

**共 18 列：2 个键字段、16 个 DOUBLE。** 字段顺序按下表；ts_code 必填非空白字符串，trade_date 为必填市场业务 DATE，close 必填有限正数。其余 15 个指标列允许合法 NULL，非 NULL 必须为有限 JSON 数值；缺列、非法值与合法 NULL 严格区分。允许可空是项目对历史异质数据的首版政策，不表示供应商保证这些字段在任何时点都会缺失。

| 字段 | 类型 | 非空时的规则与含义 |
|---|---|---|
| ts_code | VARCHAR / pl.String | 保留完整源标识及前导零，不强制六位数字或要求存在于当前股票表 |
| trade_date | DATE / pl.Date | 严格匹配可信请求日期；抓取时间另存请求审计 |
| close | DOUBLE / pl.Float64 | 有限且 >0；保留源收盘价，不复权、不与其他表互补或换汇 |
| turnover_rate | DOUBLE / pl.Float64 | >=0，百分数，成交量/无限售流通股数的供应商口径 |
| turnover_rate_f | DOUBLE / pl.Float64 | >=0，百分数，自由流通股口径；不强制小于等于 100 |
| volume_ratio | DOUBLE / pl.Float64 | >=0，供应商量比；无足够历史的 NULL 合法，不自行重算 |
| pe | DOUBLE / pl.Float64 | 保留有符号有限值及 NULL，供应商静态市盈率；亏损 PE 为空是文档声明 |
| pe_ttm | DOUBLE / pl.Float64 | 保留有符号有限值及 NULL，滚动市盈率 |
| pb | DOUBLE / pl.Float64 | 保留有符号有限值及 NULL，市净率 |
| ps | DOUBLE / pl.Float64 | 保留有符号有限值及 NULL，最新年报口径市销率 |
| ps_ttm | DOUBLE / pl.Float64 | 保留有符号有限值及 NULL，滚动市销率 |
| dv_ratio | DOUBLE / pl.Float64 | >=0，百分数，供应商上年派现口径；不把 NULL 解释为不分红 |
| dv_ttm | DOUBLE / pl.Float64 | >=0，百分数，供应商近十二个月口径 |
| total_share | DOUBLE / pl.Float64 | >=0，万股；保留小数，不转整数股数 |
| float_share | DOUBLE / pl.Float64 | >=0，万股 |
| free_share | DOUBLE / pl.Float64 | >=0，万股，允许合法 NULL |
| total_mv | DOUBLE / pl.Float64 | >=0，万元 |
| circ_mv | DOUBLE / pl.Float64 | >=0，万元 |

单位沿用官方股本=万股、市值=万元、比率=%；百分数不除以 100，股本/市值不转换为股/元。零和 NULL 分别保存，不对浮点数人为截断小数位，不按 `close * share` 强制回算或覆盖市值。估值比率不设正数门槛，不推导财务分母。此首版仅将 close 设为业务必填数值，其余列的 NULL 不单独触发告警。

## 4. 获取与集中准备

每个开市日调用 `daily_basic(trade_date=..., limit=3000, offset=...)`，显式请求上述 18 字段。日请求语义及分页参数已实测；继续沿用 1.3 秒逻辑请求间隔，官方 Client 的超时、有限重试及脱敏证据回调保持现有政策，不新增限流配置系统。

采用专属分页，与 stk_limit 的已验证政策一致：

1. offset=0 起步；每页必须有明确布尔 has_more，少于 3,000 行且 false 才终止。满页即使 false 也续取尾页；短页 true 按实际行数前移。
2. 请求失败、缺少终止标记、空页 true、超请求页上限或重复页不能声明整日完整。重复页按字段名及无序内容识别，防止忽略 offset 后反复获取；跨页部分重复键交给 Dataset 统一验收，不去重。
3. 所有页和重试逐次留证。可信请求日与 offset 作为 Raw 标签，UnitDelivery 关联当天全部 request_ids。后续页失败时，保留已有证据和部分 Raw，阻断整个请求日，继续其他日；不发布部分页。
4. 按字段名重排并核对全部必需列。可解码的完整空响应形成明确 Schema 的 Raw；缺列/坏类型属于准备错误，不能用补 NULL 或空表绕过。获取 COMPLETE 与准备失败分别记录。
5. 源代码/日期严格字符串；指标仅接受 JSON int/float/NULL，拒绝 bool 和数值字符串的隐式转换，非有限值留给业务验收明确阻断。可复用 `sources/tushare/daily.raw_frame` 的现有解码语义。
6. prepare 集中清理文本、空串转 NULL，并核对源日期与可信请求日完全一致后转 DATE。错日及无法解析的记录归因到请求日，不污染其他合格日期；保留 Raw 及明确诊断，不静默丢行或去重。
7. Candidate 只含 18 列 Canonical Schema；不保留请求标签为业务字段，不计算复权、衍生指标或交易状态。统一 Validator 结合 Scope、交付与准备诊断给出唯一发布许可。

## 5. 验收与消费者语义

**ERROR，阻断可信受影响日：** 请求或分页不完整，缺列、源类型错误、非法/错日，空业务键，重复业务键，缺 close、close<=0，任一非 NULL 数值非有限，上述非负指标出现负值，以及开市日全市场零行。无法定位影响范围时沿用现有全局阻断，不按有行或 NULL 比例推断成功。

**WARNING，保留原值并允许发布：** 非 NULL 数值之间出现 `free_share>float_share`、`float_share>total_share` 或 `circ_mv>total_mv`，按日及具体关系计数。这里是发现源属性不一致的诊断，不能据此改值或使三个真实样本阻断整日。仅在两侧均存在时比较；NULL 不参与该关系检查。缺失属于已允许语义，不额外生成海量逐证券告警。

不把换手率上限 100、估值正数、全部数值非空、市值与价格乘股本完全相等、当前名单覆盖或日线集合绝对相等设为门禁。源值虽被保留，使用者仍须理解其口径与修订限制。

消费者先检查目标日的 PUBLISHED Coverage，再读取实际源行。成功覆盖但个股无行只表示本次未报告，未成功覆盖的缺失无法解释为该股没有指标。本次 Run FAILED/PARTIAL 与仍有旧成功 Coverage 可以同时成立； Publication 不提供历史行镜像，指标抓取时点不能替代财务公布时点。

## 6. 实现范围与必要验证

新增 `datasets/daily_basic.py`、`.sql`、`sources/tushare/daily_basic.py`、`tests/test_daily_basic.py`，并修改 CLI 显式装配、README 与当前能力清单。正式入口为：

```bash
uv run --frozen python -m quant_data ingest daily_basic \
  --start-date 20261028 --end-date 20261029 \
  --db data/probes/my-daily-basic/warehouse.duckdb --keep-artifacts
```

同库日历须先成功发布；CLI 不自动运行上游。

复用已有日历函数、严格数值解码、TushareClient、Runner、Validator、Publisher 与 Meta；专属请求/验收保持本 Dataset 内。没有新增共享控制模型、基类、通用分页框架或外部依赖，现有接口与内核未变。正式代码见 [Dataset](../../src/quant_data/datasets/daily_basic.py)、[DDL](../../src/quant_data/datasets/daily_basic.sql)、[Adapter](../../src/quant_data/sources/tushare/daily_basic.py) 和 [测试](../../tests/test_daily_basic.py)。

必要验证计划：

1. 日历缺失零供应商请求，休市排除及实际日历缺失的 UNKNOWN，发布前包括排除日的版本复核。
2. 多页拼接、满页 false 的尾页确认、短页 true、后页失败、缺标记/空页 true/重复页与跨页重复键；一日失败仍允许另一合格日发布。
3. 18 列 Schema、字段重排、合法 NULL 与零、负 PS/PS_TTM、股本关系告警；缺字段、坏值、bool/数值字符串、错日、非有限数、负非负指标、close 门禁及整日零行。
4. 新真实近期/2000 年/新股信封重放，独立核对 Raw、Candidate、Canonical，不把替身测试当成真实供应商验收。
5. 新独立库正式 CLI 小区间及重跑，验证按日替换、失败保护、Coverage/Publication、事务回滚、CLI 退出状态和证据哈希；不写默认权威库。
6. 运行必要链路与受影响回归，数据量和控制 SQL 仍沿用单连接、单内存批次边界。

提案准备阶段的 15 次请求全部成功，19 份证据 JSON 的 SHA-256 核对通过；对近期、2000 年、2010 年及两组新股共 8,124 条源记录按建议行级规则进行可丢弃检查，无阻断性冲突，3 条股本关系告警。该检查仅说明建议规则与已取样记录相容，不替代正式 Validator、发布测试或全历史证明。当时尚未实现生产 Dataset；正式实现验收另见第 8 节。

## 7. 用户确认与首版边界

用户已授权按提案落地：**默认 18 字段、limit_status 暂缓、仅依赖日历、全市场逐日分页、按日完整替换、整日零行阻断、估值有符号值与合法 NULL 保留、股本/市值关系仅告警。** 这些是用户接受的项目政策，不是由一次实测推断的供应商全历史保证。

具体历史下载区间尚未指定；首版小区间实现/验收不依赖先确定全历史回填任务。精确历史起点、其他日期的覆盖差异、limit_status 可靠历史边界、供应商跨页快照一致性及盘后修订均未穷尽验证。不新增 PIT、财务报表 ingestion、Serving 选股视图、自动调度、自动上游运行、增量规划或全历史下载。后续真实数据若冲突于确认后的规则，显式报告并讨论，不能通过静默放宽规则改变契约。

## 8. 实现验收记录

新增 9 项正式链路测试全部通过：覆盖开闭市 Scope、UNKNOWN 和依赖版本复核、完整日替换与重跑、分页终止与进度、后续页失败隔离、跨页重复键、坏字段/类型/日期、合法 NULL/零/负估值/高换手率及单位保留、股本/市值关系告警、整日零行、数值门禁、发布回滚、证据失败和 CLI 三类结局。

真实信封离线重放 2026-10-29 全市场 5,570 行、2000-01-04 全市场 913 行及 688981.SH 上市首日 1 行，均经过正式 Adapter/Runner 发布至测试临时库，Raw/Candidate/Canonical 与独立期望逐字段一致。近期样本仅记录一项关系告警，包含自由流通股本大于流通股本的 3 行；历史和新股样本的 NULL 保留且不额外告警。日历是明确的测试输入，不将该项重放称为实时供应商获取。

全套 `uv run --frozen python -m unittest discover -s tests -q` 共 150 项：149 项通过，1 项因旧 stock_basic 本地真实样本缺失跳过；新增每日指标重放没有跳过。`scripts/check_environment.py` 通过，核心批量机制仍为固定 28 次 SQL。

独立库 `data/probes/daily-basic-ingestion-validation-20261030/warehouse.duckdb` 使用正式 CLI 真实发布 2026-10-28–29 的两个日历 Unit，再执行两次 daily_basic。每次四个请求、两个日期各 5,570 行，合计 **11,140 行**，均 SUCCESS、无 ERROR、产物完整；重跑事实逐字段相同，生成新版本且不累加。每次仅一项自由流通股本关系 WARNING，detail 分别记录两个日期各 3 行；与原始响应独立统计一致。源响应、Raw、Candidate、Canonical 逐字段核对通过，估值负数、合法 NULL 和原始单位均保留。

随后两项明确标注为无 HTTP 的故障注入通过：完整零行使两个日期均失败，旧事实、Coverage 和 Publication 不变；10-28 第二页失败时该日旧事实及版本不变，10-29 回放已保存真实响应后仍可独立发布，Run 为 PARTIAL。这些是故障实验，不作为真实供应商故障或实时获取证据。连同成功运行共核对 **230 个产物清单文件 SHA-256**，全部通过。

机器摘要见 [daily-basic-ingestion-summary-2026-10-30.json](../research/daily-basic-ingestion-summary-2026-10-30.json)，可丢弃验证脚本与完整产物留在上述独立目录。未修改默认权威库；共享 Protocol、内核与 Meta 未变，也未引入新抽象或额外依赖。本轮没有未完成的必要实现或验收；limit_status、财务 PIT、自动调度及全历史下载仍按首版边界处理。
