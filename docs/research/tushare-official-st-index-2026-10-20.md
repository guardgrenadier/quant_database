# 官方 stock_st / index_daily / index_basic / index_weight / index_member_all 探查

日期：2026-10-20，Asia/Shanghai；请求时段：22:59–23:08。

目标：为 ingestion V3.2 的 Dataset / Adapter 契约提供证据。本报告只提出接入建议，不冻结 Schema、Scope、Unit 或零行语义；未修改正式 ingestion、Canonical、Coverage 或架构。

## 1. 结果与证据边界

保存了 **176 次 HTTP 尝试**：174 次 `code=0`，2 次预期的 `50101` 分页边界错误，无自动重试；另有一次人为中断的在途请求，没有响应证据，不计入这 176 次。五接口尝试数分别为 stock_st 35、index_basic 41、index_daily 28、index_weight 46、index_member_all 26。

| 接口 | 本轮核心证据 | 对接入的影响 |
|---|---|---|
| stock_st | 9 月完整分页 4,273 行；按日 201 / 199 行；早期历史有空响应 | 每日稀疏名单；无参数会跨日期截断；早期空响应不能证明无 ST |
| index_basic | 全量 12,599 行、八种 market；CSI 单市场 9,053 行 | 必须分页；文档市场列表不完整，不能只枚举文档市场后声称全量 |
| index_daily | 10-19 完整 9,473 行，43 行 close 为 NULL；10-05 有 2,681 行 | close 不能统一必填；全量指数不能仅按 SSE 开市日确定 Scope |
| index_weight | 全市场取得 105,000 行仍未结束；offset 大于 100,000 报错 | 单日全市场分页无法取尽；需要指数等条件分片 |
| index_member_all | Y 5,918 + N 2,006，分页与独立请求一致 | 是申万分类归属；默认仅 Y，历史集合需要显式获取 |

数量均为这次 token、参数与抓取时点下的观察，不是永久阈值。分页终止只证明约定请求计划的交付，不独立证明真实世界绝无遗漏、跨请求快照一致或数据已经终态。

机器可读摘要：[tushare-official-st-index-summary-2026-10-20.json](tushare-official-st-index-summary-2026-10-20.json)。`requests` 保存参数、显式字段、状态、字段类型、NULL/空串、候选键重复、逐尝试路径和原始文件 SHA-256；`groups` 保存分页与合并统计；`findings` 保存多重集合比较及交叉核对。原始业务行仍在本地，不入 Git。

本地目录均在仓库根目录下：

- `data/probes/st-index-20261020-smoke/`：五接口连通性样本。
- `data/probes/st-index-20261020-main/`：主矩阵、完整指数列表和日行情分页；权重小页探查在第 13 个完整页之后中断，详见 `run-note.json`，该分页组不能当作完整交付。
- `data/probes/st-index-20261020-weight-pages/`：7,000 行权重分页，15 个成功页后遇到 offset 边界，明确未完成。
- `data/probes/st-index-20261020-followup/`：名单/申万/CSI 分页、参数与字段补证、权重 offset 边界。
- `data/probes/st-index-20261020-calendar-check/`：SSE 休市日的指数行情反例。
- `data/probes/st-index-20261020-docs/`：五份官方 Markdown 和两份休市公告快照；摘要包含访问时间、URL、SHA-256。

## 2. 通用协议与获取证据

复用现有 `TushareClient` / `Artifacts.record_attempt`，仅访问 `https://api.tushare.pro`，token 从项目 `.env` 优先读取且不写证据。串行逻辑请求起始间隔至少 1.3 秒；40 秒超时及 Client 原有有限重试规则保持不变。本轮没有重试样本，不能据此增加重试能力或证明限流阈值。

1. 成功响应保留 `fields/items/has_more/count`。本轮所有成功响应的 `count` 仍为 0，不用它判断总量。
2. 使用返回字段名定位列，不依赖请求列序。无效字段可能被直接忽略，HTTP 成功也不能替代必需列检查。
3. 小页满页时出现 `has_more=true`，不足页时为 false；探针对已终止页再请求一个后续空页验证。每页和合并结果均保留重复，不静默去重。
4. 核对使用按字段名排序的**多重集合**，行顺序变化不影响比较，重复行仍影响结果。指数日行情、申万 Y/N、CSI 分区及指数权重单月的独立核对均一致。
5. `index_weight` 的 offset 上限是本轮实测的该接口行为；不能未经验证泛化为其他接口的保证。

## 3. stock_st：每日 ST 名单

官方依据：[ST 股票列表](https://tushare.pro/document/2?doc_id=397)。文档声明五个输出字段、单次 1,000 行、9:20 更新，并说明历史从 2000 年开始且早期无法补齐。更新时间是官方声明，本轮没有做盘中轮询验证。

| 用例 | 行数 | 观察 |
|---|---:|---|
| `st_20261019` / `st_20261016` | 各 201 | has_more=false |
| `st_today`（10-20） | 199 | 当前抓取时点的名单，不代表固定终态 |
| `st_20261018` / `st_20261030` | 0 / 0 | 周日、未来日均返回带字段的空集合 |
| `st_20250813` / `st_20200312` | 175 / 139 | 近期及历史按日可取 |
| `st_20150708` / `st_20100104` | 55 / 137 | 历史样本 |
| `st_history_20010102` / `st_history_20050104` | 50 / 126 | 2001 年已存在可取得记录 |
| 2000-01-03、01-04、01-05 | 均 0 | 结合官方历史缺口说明，不能认定这些日期没有 ST 股票 |
| `st_default` | 1,000 | 跨 10-14 至 10-20，has_more=true；无参数不是当前名单 |
| `st_range` | 1,000 | 9 月区间触顶截断 |
| `st_pages` | 4,273 | 1,000×4 + 273 + 0，明确终止；两种候选键均无重复 |

过滤补证：`000010.SZ` 在 10-19 单日返回 1 行，10-16–10-19 闭区间返回 2 行，日期为区间两端；两页 limit=2/offset=0、2 返回不同记录。`st_day_range` 与显式 trade_date 的 201 行逐行等价；默认输出字段和显式五字段也等价。无效代码返回 0 行。

全部观察到的 `type` 为 `ST`、`type_name` 为“风险警示板”，而 name 同时包含 `ST` 和 `*ST` 名称。**type 不能用于区分 ST 与 *ST，也不能把每天重复出现的股票解释为当天新增风险事件。** 历史 name 保留供应商原值，不用当前股票名称覆盖。

建议待确认：

- 候选 Schema：`ts_code/name/type/type_name` 为字符串，`trade_date` 为 DATE；样本五字段均无 NULL。股票标识不数值化，不从名称派生风险类别。
- 样本 `(ts_code, trade_date)` 已唯一，增加 type 后仍唯一；可把前者作为候选 Record Key，但这只是现有名单粒度证据。
- 以一个经已发布 SSE 日历确认的交易日全市场名单作为 Unit，逐日获取并替换，避免长区间触顶；不依赖当前 stock_basic 去删历史证券。
- **开放问题：开市日完整零行是否允许清空旧名单。** 休市、未来、历史缺口和合法无名单均可能表现为 0 行；在历史覆盖及零行契约确认前，不能照搬 suspend_daily 的空集合发布规则。单日低于 1,000 且 has_more=false 仅解决获取终止，未解决历史真实性。

## 4. index_basic：供应商指数列表快照

官方依据：[指数基本信息](https://tushare.pro/document/2?doc_id=94)。文档写 market 默认 SSE、单次 8,000 行、每日 18 点更新；本轮实际默认行为和市场枚举与该描述有差异。

无参数默认字段只有八列，返回 8,000 行且 has_more=true。显式请求全部 13 字段后分页，得到 **12,599 条 ts_code 唯一记录**：

| market | 行数 | 验证 |
|---|---:|---|
| SSE | 208 | 独立过滤可完整取得 |
| SZSE | 485 | 同上 |
| CSI | 9,053 | 单次截断为 8,000；1,000×9 + 53 + 0，和全量中 CSI 分区逐行一致 |
| CNI | 1,134 | 文档市场说明未列出，独立参数有效 |
| BSE | 2 | 同上 |
| SW | 950 | 申万指数列表 |
| NH | 102 | 南华期货指数，如 `A.NH`；独立参数有效 |
| CI | 665 | 中信行业指数，如 `CI005001.CI`；独立参数有效 |

`MSCI/CICC/OTH` 本轮各返回 0，不能用这些文档枚举组成当前完整快照。全量终止页为 599 行，后续空页验证通过。`symbol='000300,000001'` 实际取得 `000300.SH/000001.SH` 两条；无效 ts_code 为空。

空值证据：index_type 为 NULL 12,497 条，其余 102 条来自 NH，因此“index_type 全空”也不是全量事实；base_date 126、base_point 128、list_date 124、weight_rule 320、desc 102 条为 NULL。790 条有 exp_date；不应在获取快照时丢掉终止指数。本轮没有未来 list_date，但不能据此将未来日期作为硬错误。

建议待确认：全供应商快照作为一个 Unit，`ts_code` 为候选键，按已验证分页取尽；market 保留供应商字符串，不按文档七种值硬过滤。13 字段全部保留，`base_date/list_date/exp_date` 转可空 DATE，`base_point` 为可空 DOUBLE，其余字符串；必填与可空仍需形成正式规则。完整快照不能为空，不能用默认八字段响应补 NULL 冒充完整字段交付。

它包含证券、期货、行业等多类指数；如项目只需要某部分，须先确认目标全集，再将该范围写入 Scope 和替换契约。

## 5. index_daily：跨市场日集合与 NULL

官方依据：[指数日线行情](https://tushare.pro/document/2?doc_id=95)。文档 ts_code 标为必选，但实测不传可取得全市场日集合；不含申万行业行情。文档量额单位分别为手、千元，本轮未独立核对跨市场的单位/币种一致性，建议先保留源值，不擅自换算或跨市场求和。

`id_day`（10-19）返回 9,473 行、has_more=false；1,000 行分页为 1,000×9 + 473 + 0，与单请求完整多重集合相同，无 `(ts_code, trade_date)` 重复。全部代码能在本轮 index_basic 快照中找到：

| market | 日行情条数 |
|---|---:|
| SSE | 200 |
| SZSE | 386 |
| CSI | 7,786 |
| CNI | 1,004 |
| BSE | 2 |
| NH | 95 |

申万 `801010.SI`、中信 `CI005001.CI` 单代码本日均为空；南华 `A.NH` 有一行。不是每个 index_basic 代码都应在 index_daily 出现。逗号双代码请求返回 0，不能假定它支持 ts_code 多值；无效代码、未来日、周日也均为空。

10-13 全市场单请求仍返回 10,000 行且 has_more=true，本轮没有对该日再次取尽，因此不重复声明旧报告的总量。这说明 10-19 未截断不能消除其他日期的分页需求。

### 5.1 不能统一要求 close 必填

10-19 的 NULL 数量：close 43、open/high/low 各 7,419、pre_close 10、change 43、pct_chg 44、vol 1,871、amount 5,289。没有九个数值字段全部为空的记录；四价齐全样本没有 OHLC 顺序冲突，量额没有负值。

- 10 条 SZSE 记录是 `395xxx.SZ` 成交量/成交额统计，如 `395001.SZ`。其 index_basic.desc 明确描述对应成交统计，价格字段为空、量额有值。独立代码请求复现，不能作为价格坏值丢掉。
- 33 条 CSI 记录 close 为空。独立重查 `H11100.CSI`（香港100）在 10-12–10-19 有 6 行，前五个交易日收盘有值，10-19 为 NULL、pre_close 有值、量额为 0。
- [香港政府 2026 年假期公告](https://www.info.gov.hk/gia/general/202505/16/P2025051300353p.htm)列出 10-19 为重阳节翌日假期；[港交所公告页面](https://www.hkex.com.hk/News/HKEX-Calendar?sc_lang=en)也显示当日香港市场关闭。该样本与香港休市一致；这不是对全部 33 行原因的逐条证明。

建议把所有九个数值字段设为可空 DOUBLE，记录键及日期必填；只对实际有值的字段做数值检查，四价关系只在齐全时检查。不将 NULL 补前收盘或 0，不把 0 量额推导为停牌状态。若业务目标只收“有价格的指数行情”，需要显式确认分类/排除规则，不能在 Adapter 静默删行。

### 5.2 SSE 日历不能直接确定全量指数 Scope

[上交所 2026 年休市通知](https://www.sse.com.cn/disclosure/announcement/general/c/c_20251222_10802507.shtml)确定 10-01–10-07 休市。实际 `id_cross_calendar_all` 在 **10-05 返回 2,681 条、has_more=false**，独立 `H11100.CSI` 查询 close=2108.3807。证据直接说明：SSE 休市不意味着全供应商指数日集合为空，也不能据此排除该日期。

另一个独立反例是 000300.SH 的历史：本轮完整长区间返回 6,011 行，从 2002-01-04 开始，早于 index_basic 的 list_date=2005-04-08。不能用发布日期切掉回填历史。

建议候选键 `(ts_code, trade_date)`；按全供应商日期集合发布是一个可行的 Unit 方向，但**范围需确认**：全量供应商日事实应覆盖显式自然日区间，空日语义另定；如仅接入指定指数/市场，则需明确全集与对应日历。当前成功 index_basic 可用于范围与诊断证据，但不能把当前快照当成历史指数全集，或从它直接生成“一日每指数必有一行”的硬门禁。

## 6. index_weight：月度分片、offset 上限及多资产成分

官方依据：[指数成分和权重](https://tushare.pro/document/2?doc_id=96)。文档为月度数据，建议使用自然月两端请求；输出四列，不规定单次行数上限。

### 6.1 单日全市场无法用 offset 取尽

`trade_date=20260930` 默认返回 7,000 行、has_more=true。改为 limit=7,000 后，offset=0 至 98,000 共 15 页返回 **105,000 条、6,400 个不同 index_code**，每页均 has_more=true，候选三字段键无重复。offset=105,000 返回 `50101`，消息明确要求 offset 不得大于 100,000，需缩小查询范围。

补证 offset=100,000/limit=1,000 返回 1,000 行且仍 has_more=true；offset=100,001 再次返回相同边界错误。因此本报告**没有全市场总量，也没有全市场完整性结论**。这 105,000 行不能生成全市场成功 Coverage，更不能按其中不完整指数分组检查权重和。

### 6.2 指数 × 月份可以形成完整实验分片

| index_code / 月份 | 行数 | 返回日期 |
|---|---:|---|
| 000300.SH / 2026-09 | 300 | 2026-09-30 |
| 000300.SH / 2026-08 | 300 | 2026-08-31 |
| 000300.SH / 2005-04 | 300 | 2005-04-29 |
| 399300.SZ / 2018-09 | 300 | 2018-09-28 |
| 000852.SH / 2026-09 | 1,000 | 2026-09-30 |
| 000300.SH / 2026-10 | 0 | 抓取时尚未到月末，不能推导无成分 |

000300.SH 的 9 月单月、09-30 单日、单月分页 300+0 三者逐行一致；09-29 单日为 0。三份独立完整样本（沪深300本月、中证1000本月、399300历史月）共 1,600 行，权重和均在 99–101 范围内。仅作为样本统计，不设固定 300/1,000 行或精确等于 100 的通用门禁。

日期应保留源值：本轮月末样本不能重新定义官方示例中的月初日期，也不能把所有权重统一移动到月初/月末；“月度”不证明每个指数每月恰有一个日期。

### 6.3 参数、资产与零行

`con_code=600519.SH` 及不存在的 `con_code=999999.XX` 均被忽略，返回的 7,000 行与仅传 trade_date 完整多重集合相同。这与旧报告中“非法参数会报 50101”的观察不同：不能把请求不报错视为过滤有效。

105,000 条部分样本中，成分包含 `.SH/.SZ/.BJ/.HK/.IB/.NQ`，还有 2,399 条不带点号的代码，如 AAPL、6758。不能限制为六位 A 股代码、强制 stock_basic 外键、补造交易所后缀或静默过滤非 A 股。部分样本 weight 为浮点、无 NULL，范围 1.539–100；因为未取到低权重尾部，**不能据此断言全体没有 0 或负值**，也不能直接采用旧建议的 weight>0 硬约束。

建议候选键 `(index_code, con_code, trade_date)`；字符串代码、DATE、DOUBLE weight。优先按“指数 × 月份”完整替换，分片内部仍保留分页及 offset 边界保护；若单片依然过大，需要再验证日期子分片后才可声称获取完整。

开放问题：目标指数集合如何确定、某指数某月份零行是无数据/未发布/不覆盖/合法空的哪一种。index_basic 存在不保证 index_weight 覆盖该指数；本轮不存在代码的空响应不能替这些语义作决定。不能把当月请求为空简单写成永久 OUT_OF_SCOPE，也没有证据保证所有指数都在同一时点发布权重。

## 7. index_member_all：申万行业归属的当前/历史快照

官方依据：[申万行业成分构成（分级）](https://tushare.pro/document/2?doc_id=335)。它提供一级、二级、三级行业和股票的归属，**不是任意指数成分接口**。支持 `l1_code/l2_code/l3_code/ts_code/is_new`，默认 Y。

默认、显式 Y、空字符串 is_new 三者均为 5,918 行，逐行一致；N 为 2,006 行，涉及 1,646 只股票。Y 分页为 1,000×5+918+0，N 为 1,000×2+6+0，与各自独立全量请求逐行一致。实际单次 5,918 行已超过文档的 2,000 行说明，因此不能只靠文档上限推断交付行为。

合法过滤与全量 Y 中对应子集逐行一致：l1=801010.SI 126 行，l2=801053.SI 14 行，l3=850531.SI 13 行，000001.SZ 1 行。股票与不匹配三级分类组合为空，不存在股票/分类也为空。

关键陷阱：

- `index_code=999999.XX` 被静默忽略，仍返回与默认 Y 相同的 5,918 行；不能用它获取中证指数成分。
- 请求 `con_code/con_name` 这两个错误输出名会被忽略，实际只返回请求中其他合法列。正式字段是 **ts_code/name**，不能用缺列补 NULL 掩盖错误。旧研究脚本的初始字段常量包含这两个错误名，本轮没有修改历史脚本，复跑时应使用新脚本。

Y 的 out_date 全 NULL，N 全有值；合并 7,924 行后 `(ts_code,in_date)` 与 `(l3_code,ts_code,in_date)` 在本轮均唯一。9 条 in_date 早于 1990 年，最早 1984-05-09；未见 out_date<in_date。日期异常保留为证据并告警，不强行改成上市日。样本不能独立证明历史分类版本、变更当时可知性或完整 PIT 语义。

建议待确认：完整 Y+N 归属快照作为一个 Unit，两集合都完整取得后原子替换；Record Key 候选采用 `(l3_code,ts_code,in_date)`，但本轮没有证据声称更短候选键已经发生冲突。四组代码/名称、is_new 保留字符串，in_date 为 DATE、out_date 可空 DATE。避免当前股票名单外键，不用最新行业名称覆盖历史行，不把 is_new 当作当前上市状态。

## 8. 对 ingestion V3.2 的契约建议与待决事项

| 接口 | 候选 Record Key | 候选 Publication Unit | 获取策略 |
|---|---|---|---|
| stock_st | ts_code + trade_date | 全市场交易日名单 | 经日历确认后逐日；零行规则待定 |
| index_basic | ts_code | 完整目标快照 | 显式全部字段、全量分页；目标市场集合待确认 |
| index_daily | ts_code + trade_date | 完整目标日期集合 | 全量需包含 SSE 休市自然日；指定全集时另定日历与替换范围 |
| index_weight | index_code + con_code + trade_date | 指数 × 自然月 | 显式指数过滤、月内取尽，避开全市场 offset 上限 |
| index_member_all | l3_code + ts_code + in_date | 完整 Y+N 快照 | 两状态分别取尽，合并后验收 |

正式实现应沿用现有 `fetch → prepare → validate → publish`：获取结果表达 COMPLETE/FAILED/INCOMPLETE；字段/类型/范围解析问题独立形成准备诊断；Validator 决定明确的合格 Unit；发布按 Unit 完整替换并保护旧成功版本。不同阶段不重复实现业务规则，不能从 Candidate 行数反推完整性。

当前无需改变共享接口才能表达上述证据。正式接入前需要确认：

1. index_basic / index_daily 的目标全集，以及跨市场日期的 Scope；不能直接将已有 SSE 共同日历约定外推到境外/期货指数。
2. stock_st 开市日零行、index_daily 空日、index_weight 空月的发布或阻断语义；供应商支持范围尚不足以自动区分这些情况。
3. index_daily 是否保存全供应商事实并允许可空价格，或显式限定一类指数；不得靠静默删行实现范围选择。
4. index_weight 的目标指数范围、weight 合法范围及替换 Unit；现有样本不支持直接冻结所有数值规则。

这些是接入前的契约决策，不是本轮对冻结架构的修改。

## 9. 复跑与验证

研究脚本：[probe_tushare_st_index.py](../../scripts/probe_tushare_st_index.py)。固定日期/代码是实验用例；默认运行整个矩阵，其中包含预期失败用例，因此非零退出码需结合摘要解释，不能解释成所有接口不可用。每次必须指定新证据目录或使用默认时间戳目录。

```bash
# 五接口连通性
uv run --frozen python scripts/probe_tushare_st_index.py \
  --case st_20261019 --case ib_single --case id_recent \
  --case iw_september --case ima_stock

# 权重全市场的受限分页实验（预计在 offset 上限处明确失败）
uv run --frozen python scripts/probe_tushare_st_index.py \
  --case iw_pages --page-size 7000 --max-pages 100

# 单指数月份分页与申万两状态完整分页
uv run --frozen python scripts/probe_tushare_st_index.py \
  --case iw_300_pages --case ima_Y_pages --case ima_N_pages

# 离线重建本次摘要并核对全部有记录尝试的 SHA-256
uv run --frozen python -m scripts.analyze_tushare_st_index \
  data/probes/st-index-20261020-smoke \
  data/probes/st-index-20261020-main \
  data/probes/st-index-20261020-weight-pages \
  data/probes/st-index-20261020-followup \
  data/probes/st-index-20261020-calendar-check \
  --documents data/probes/st-index-20261020-docs/manifest.json \
  --output data/probes/st-index-20261020-rebuilt-summary.json

uv run --frozen python -m unittest tests.test_st_index_probe tests.test_probe -v
```

新增三项必要测试，和既有三项探针测试共六项通过，验证 NULL/空串/缺列、失败/零行、重复保留及多重集合比较。离线分析核验了全部 176 份响应证据摘要；16 组独立多重集合核对通过。

未做：正式 ingestion 接入、全历史批量下载、全市场权重取尽、盘中更新轮询、修订/PIT 保证、积分/限流压力测试、跨市场量额口径独立核验。没有新增生产抽象、共享接口变更或正式数据库写入。
