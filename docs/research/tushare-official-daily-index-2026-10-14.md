# 官方行情/指数接口探查与 Canonical Schema 建议（daily 系与 index 系）

日期：2026-10-14（Asia/Shanghai）
依据：`docs/design/core_design.md` V3.1.1
状态：真实 API 探查完成；下述 Schema 与策略是建议稿，尚未作为正式发布契约冻结。

## 1. 环境与证据

沿用 [上一轮 stock_basic/suspend_d 探查](tushare-official-stock-suspend-2026-10-13.md) 的环境与探针模式：Python 标准库 HTTPS JSON POST、串行、请求间隔 ≥1.3 秒、40 秒超时、token 不落盘。探针脚本为 `scripts/probe_tushare_daily_index.py`（第一轮）与 `scripts/probe_tushare_daily_index_r2.py`（第二轮补查），两轮共 **105 次真实请求**（含 3 次瞬时 SSL 错误），其余全部 HTTP 200 / code=0。

本地原始证据：

- `data/probes/official-daily-index-2026-10-14/`：70 次主矩阵探查。
- `data/probes/official-daily-index-r2-2026-10-14/`：35 次补查（重试、停牌语义、历史起点、分页边界、index_member_all 正确参数）。

可入 Git 的 [请求摘要](tushare-official-daily-index-probe-summary-2026-10-14.json) 含每次请求参数、返回字段、行数、has_more 与原始文件 SHA-256。探查时点为交易日 2026-10-14 收盘后（18 点之后），当日 15–16 点入库的数据均已可见。

官方文档：[daily](https://tushare.pro/document/2?doc_id=27)、[adj_factor](https://tushare.pro/document/2?doc_id=28)、[daily_basic](https://tushare.pro/document/2?doc_id=32)、[stk_limit](https://tushare.pro/document/2?doc_id=183)、[index_basic](https://tushare.pro/document/2?doc_id=94)、[index_daily](https://tushare.pro/document/2?doc_id=95)、[index_weight](https://tushare.pro/document/2?doc_id=96)、[index_member_all](https://tushare.pro/document/2?doc_id=335)（申万行业成分分级，**不是通用指数成分**）、sw_daily（申万行业日线行情，5000 积分门槛）。

## 2. 通用协议新发现

在上一轮信封结论（`fields` 定列序、`count` 恒 0、`has_more` 语义保守）之上，本轮新增：

1. **瞬时传输错误真实存在且可重试**：3 次 `SSL: UNEXPECTED_EOF_WHILE_READING`（daily_basic、index_basic、index_daily 各一次），第二轮原样重试全部成功。正式 Provider Client 必须对网络/传输层错误做有限重试（建议指数退避、上限 3 次左右），且重试记录进 Run Audit。供应商业务错误码（如 `50101 参数校验失败`）不可重试，直接视为请求失败。
2. **limit/offset 在本轮 9 个接口全部实测有效**，包括文档未列出的 index_member_all。分页是消除截断的通用路径。
3. **请求字段被静默裁剪**：index_member_all 显式请求不存在的 `con_code/con_name` 时，响应既不报错也不含必需的 `ts_code/name`，只返回交集字段。Adapter 必须校验"返回 fields ⊇ 必需字段"，缺失即 ERROR。
4. **无效参数被静默忽略**：index_member_all 传文档外的 `index_code/con_code` 时，接口忽略参数并返回默认全量集（5918 行 is_new=Y），五个不同参数的请求返回完全相同。Adapter 必须校验"返回行匹配请求参数域"（如按 l1_code 请求时检查返回 l1_code 一致），否则会把错误参数的结果当作目标数据发布。
5. **实际交付可超过文档标注上限**：index_daily 单日返回 10,000 行（文档 8,000），index_member_all 单请求返回 5,918 行（文档 2,000）。文档上限只能当下限参考，交付判断以 `has_more` + 触顶检测为准。
6. 当日数据可得性（2026-10-14 18 点后）：daily、daily_basic、stk_limit、sw_daily 均已有当日行，与各自文档的更新时点（15–16 点 / 15–17 点 / 9 点 / 盘后）一致；adj_factor 盘前 9:15–20 更新。**本轮未测盘中时点和更新再修订行为，不能声明"几点后数据最终完整"。**

## 3. 各接口实测行为

### 3.1 daily（A股日线行情）

- 单日全市场：20261013 返回 5,561 行、20261014 返回 5,563 行，`has_more=false`。**已逼近 6,000 行上限**，随上市公司增加会突破，获取策略必须预设分页/拆分路径，触顶即视为截断风险。
- 单代码长区间（000001.SZ，1991–2026）返回恰好 6,000 行且 `has_more=true`，只覆盖 20010525 起——长历史必须按时间或分页切片。
- 停牌语义：乐视网 300104.SZ 全天停牌区间（20170417–20170428）返回 **0 行**，证实文档"停牌期间不提供数据"。全天停牌股的缺失是合法缺失，但必须由 suspend_daily + stock_basic 解释，不能由零行反推。
- 未来日、非交易日（周日 20261011）、无效代码均返回合法空。
- 20000104 全市场 913 行，历史可回溯到 2000 年（更早未测）。
- 逗号分隔多代码实测有效。
- 非默认字段 `ah_vol/ah_amount`（盘后量额，2026-07-06 起）可请求，当日 5,561 行中 349 行为 NULL。
- 全部样本 `(ts_code, trade_date)` 无重复；OHLC/量额无空值。

### 3.2 adj_factor（复权因子）

- 单日全市场 5,574 行（两个交易日相同），**多于 daily 的 5,561**：复权因子覆盖当日在市但全天停牌的股票（300104.SZ 停牌区间实测每日有行）。
- 单代码长区间同样恰 6,000 行截断（has_more=true）。
- 20000104 有 925 行（daily 同日 913 行）。
- 因子为 Tushare 自行生产；除权后历史因子会被改写（前复权口径下旧因子整体变化），**这是一个天然存在历史修订的 Dataset**，Write Policy 必须支持按日替换和定期重抓校验。
- 全部样本键无重复，`adj_factor` 无空值。

### 3.3 daily_basic（每日指标）

- 单日行数与 daily 完全一致（5,561 / 5,613 错误重试后 5,563），全天停牌区间 0 行——**覆盖集合与 daily 相同**，可互为交叉验收。
- 大量**合法 NULL**：20261013 的 5,561 行中 pe 空 1,454、pe_ttm 空 1,633、dv_ratio 空 1,803、dv_ttm 空 1,831、pb 空 52、volume_ratio 空 3；2000 年样本还有 free_share 空。pe/pe_ttm 对亏损股为空是文档声明的语义，这些字段必须可空，NULL 率高不是质量问题。
- 非默认字段 `limit_status`（收盘涨跌状态 0–6）可请求，实测返回 int。
- 更新时点 15–17 点，晚于 daily 的 15–16 点：同日两接口先后抓取可能短暂不一致，交叉验收只对已确认完整发布的范围做。

### 3.4 stk_limit（每日涨跌停价格）

- 单日全市场 5,653 行（两日相同），**多于 adj_factor 的 5,574**：文档口径含 A/B 股和基金，且覆盖全天停牌股（300104.SZ 停牌区间每日有行）。**不能用它推断当日交易状态。**
- 文档上限 5,800 行，当前 5,653 已非常接近，触顶风险同 daily。
- 历史起点未精确定位：20100104 有 1,671 行、20150601 有 2,735 行、20190102 有 3,566 行，但 20000104 为 0 行。Scope 不能假设覆盖全部历史，起点属 UNKNOWN，需要进一步探查或接受 2010 年前的覆盖缺口。
- 非默认字段 `pre_close/asset_type/exchange` 可请求（asset_type=STK，exchange=SSE/SZSE/BSE）；如需只保留 A 股，可在 Canonical 侧按 stock_basic 的 ts_code 集合过滤，或保留全量由 Serving 过滤（建议后者：Canonical 存供应商交付事实）。
- 默认字段列序为 `trade_date,ts_code,...`（与其他接口不同），再次确认不能以列序假设代替 fields 解析。

### 3.5 index_basic（指数基本信息）

- **文档"market 默认 SSE"与实测不符**：不传 market 返回 8,000 行混合市场（CSI 5,996 / SW 950 / SZSE 465 / CNI 379 / SSE 208 / BSE 2）且 `has_more=true`——已截断。
- market=CSI 单市场即 8,000 行 `has_more=true`，**必须分页**；SSE=208、SZSE=485、SW=950 不分页可完整。分页（limit/offset）实测有效。
- 这是快照型接口：每天 18 点更新，含终止指数（`exp_date` 非空，CSI 样本中 235 条有终止日期）、含未来 `list_date`（样本见 20261014 当日）。
- 空值现实：`index_type` 全空（字段名存实亡）、`weight_rule` 部分空、`base_date/base_point/list_date` 少量空、`exp_date` 大多数空（存续指数）。
- `ts_code` 无重复，可作快照主键；指数代码形态多样（如 `950238CNY01.CSI` 出现在 index_weight 中），不可限定六位数字。

### 3.6 index_daily（指数日线行情）

- 单日全市场合计 **10,519 行**（分页 8,000 + 2,519 + 0 确认终止）：不带 limit 的单请求返回 10,000 行且 `has_more=true`，已超文档 8,000 上限，**单日全市场必须分页**。
- 单指数全历史（000300.SH）6,008 行 `has_more=false`，未截断；其行情起点 20020104 **早于指数发布日 20050408**（基期回填）——不能用 index_basic.list_date 截断历史 Scope。
- 大量合法 NULL：单日 10,000 行样本中 open/high/low 空 7,783、vol 空 1,665、amount 空 5,023（债券/货币/多资产类指数只有收盘点位）。**OHLC 关系检查只能对四价俱全的行执行**，close 非空可作基本门槛。
- **不包含申万行业指数**（801010.SI 返回 0 行），申万行情必须走 sw_daily；Scope 的指数集合来自 index_basic 时需按 market 区分去向。
- 未来日、无效代码合法空；分页有效。

### 3.7 index_weight（指数成分和权重）

- **月度粒度**：000300.SH 每个自然月恰一条记录日期、300 行，`trade_date` 为**月末交易日**（20260930、20260831、20050531；发布月 2005-04 为 20050429）。但官方文档示例的 `trade_date=20180903` 是**月初**——不同指数/时期的日期口径不统一，Canonical 只能把 trade_date 当供应商原值，不能假设它等于月末。
- **当月权重月末前不可得**：截至 2026-10-14，000300.SH 与 000852.SH 的 10 月请求均为合法空。Scope 的"目标月份"必须排除尚未结束的月份，否则会把未发布当缺失；新月权重发布后，最近月份也应保留重查入口（可能修订）。
- 历史早于文档声明：文档称"数据最早 2005/05"，实测 000300.SH 在 20050429（发布当月）已有权重。
- 单日全市场 trade_date=20260930 直接请求返回 7,000 行 `has_more=true`；按 limit=3,000 分页到 offset=9,000 仍未结束（≥12,000 行）。**按日全市场必须分页，或按指数×月切片**。
- `con_code` 不是合法输入参数（报 50101）；合法入参只有 index_code/trade_date/start_date/end_date。
- weight 无空值；单成分指数 weight=100 合法；样本键 `(index_code, con_code, trade_date)` 无重复。
- 指数代码含货币/衍生形态（`950238CNY01.CSI`），con_code 形态同样不可硬编码规则。

### 3.8 index_member_all（申万行业成分分级）

- 语义边界：**这是申万三级行业分类的成分接口，不是中证指数成分接口**，不能替代 index_weight 的指数成分语义。入参为 `l1_code/l2_code/l3_code/ts_code/is_new`（默认 Y）。
- 无效参数被静默忽略（见 §2.4）——本轮最大交付陷阱。
- 全量快照：is_new=Y 默认集 5,918 行（out_date 全空）；is_new=N 历史集 2,006 行（out_date 有值，1,646 只股票的 2,006 段历史归属，单股票可多次进出，`ts_code` 重复 360 次）。单请求 5,918 行已超文档 2,000 上限，但分页可用。
- 单过滤实测：l1_code=801010.SI 126 行、l3_code=850531.SI 13 行、ts_code=000001.SZ 1 行、600519.SH 的 is_new=N 为 0 行（从未被调出，合法）。
- 数据质量：`in_date` 出现 19840509 这类早于 A 股市场的值；`is_new=Y` 集合含个别已退市股票的可能（5,918 > 在市 5,574，未逐一核对）。名称是当时名称。
- Record Key 证据：is_new=N 集内 `(ts_code, in_date)` 仍可能不足以区分跨行业移动，建议 `(l3_code, ts_code, in_date)`。

### 3.9 sw_daily（申万行业日线行情）

- 单日全市场 439 行（两日相同），远低于 4,000 上限，按日获取安全。
- 5000 积分门槛：当前 token 可调取，低积分 token 会以权限错误失败——环境/权限前置检查应覆盖。
- 历史起点：801010.SI 自 20120801 起有数据（20000101–20120831 区间仅 23 行即 2012 年 8 月），**与 index_basic 中申万指数 1999 基期不一致**，历史 Scope 起点 UNKNOWN，不能假设 2012 前可补齐。
- 合法空值：pe 空（439 行中 2 行）、长区间 total_mv 空 4 行。
- 命名差异：`pct_change`（非 `pct_chg`）、`change`，且默认列序为 `open,low,high,close`——标准化映射要逐接口声明，不能复用 daily 的映射。
- 439 行的构成（申万 50/中小 + 2021 版三级行业）与 index_basic 中 SW 市场 950 个指数不一致：SW 指数全集不等于 sw_daily 覆盖集，Scope 不能简单取 index_basic 的 market=SW 全量，需要实测对齐。

## 4. 对核心设计五方面的建议

### 4.1 完整性检查（Source-side Validation / Adapter）

| 检查 | 依据 | 适用 |
|---|---|---|
| 信封与 `has_more` 保留到交付结果；`has_more=true` 必须继续分页或拆分直至 false | 本轮多个接口实测截断 | 全部 |
| `rows == limit` 或 `rows ≥ 接口文档上限` 即视为截断风险，即使 has_more=false 也复核 | daily/adj 长区间恰 6,000；index_basic 恰 8,000 | 全部 |
| 返回 fields ⊇ 必需字段，否则 ERROR | index_member_all 静默裁剪 | 全部 |
| 返回行匹配请求参数域（参数域字段值一致、日期不越界） | index_member_all 静默忽略参数 | 全部 |
| 单日行数接近上限时主动拆分（daily 5,563/6,000，stk_limit 5,653/5,800） | 实测行数 | daily、stk_limit |
| 单日全市场必须分页获取（10,519 行 / ≥12,000 行） | 实测 | index_daily、index_weight 按日 |
| 零行合法性由 Scope 判定（未来日/非交易日/停牌/当月未发布），不视为交付失败 | 各接口合法空实测 | 全部 |

### 4.2 重试保护（Provider Client）

- 网络/SSL/超时错误：有限重试（建议 3 次、指数退避），本轮 3 次瞬时失败重试全部成功；重试事件进 Run Audit。
- 供应商非零 code（如 50101 参数错误、权限错误）：不重试，直接判定请求失败并归属受影响 Unit。
- 限流：本轮 1.3 秒间隔未触发限流；daily 文档称每分钟 500 次、index_weight 每分钟 200 次（2000 积分档）。正式 Client 以保守间隔 + 失败后退避为准；**限流阈值和积分余量未实测，不声明**。
- sw_daily 的 5000 积分门槛属于环境前置检查项，权限失败应在 Run 开始即失败，而非逐 Unit 报错。

### 4.3 Canonical Schema 建议

日期一律 `YYYYMMDD` 严格解析为 DuckDB DATE（Asia/Shanghai 语义），标识 VARCHAR 原样保留。单位原样保留并在 Schema 注释固定：daily 的 vol=手、amount=千元；daily_basic 股本=万股、市值=万元；sw_daily 的 amount 单位需与样本核对后固定。

**canonical.stock_daily**：Record Key `(ts_code, trade_date)`；Publication Unit = 一个交易日全市场。字段：ts_code、trade_date、open/high/low/close/pre_close/change/pct_chg（DOUBLE）、vol、amount（DOUBLE），另加可空 ah_vol/ah_amount（2026-07-06 起）。未复权事实；停牌日无行属合法稀疏。

**canonical.adj_factor_daily**：Key `(ts_code, trade_date)`；Unit = 交易日。单字段 adj_factor DOUBLE 非空、>0。覆盖含停牌股的在市集合；支持按日替换与历史重抓修订。

**canonical.daily_basic**：Key `(ts_code, trade_date)`；Unit = 交易日。17 个默认字段 + 可选 limit_status（SMALLINT）；pe/pe_ttm/pb/ps/ps_ttm/dv_ratio/dv_ttm/volume_ratio/free_share 必须可空。覆盖集合与 stock_daily 相同。

**canonical.stk_limit_daily**：Key `(ts_code, trade_date)`；Unit = 交易日。up_limit/down_limit DOUBLE 非空；可选 pre_close、asset_type、exchange。集合含 B 股/基金与停牌股，是否过滤留给 Serving。

**canonical.index_basic**：快照型，Key `ts_code`；Unit = 完整快照（按 market 分片 + 分页合并后整表原子替换）。exp_date（终止）、weight_rule、index_type、base_date/base_point/list_date 均可空。

**canonical.index_daily**：Key `(ts_code, trade_date)`；Unit = 交易日。close 非空，open/high/low/pre_close/change/pct_chg/vol/amount 可空。不含申万指数。

**canonical.index_weight**：Key `(index_code, con_code, trade_date)`；Unit = 指数 × 月份。weight DOUBLE 非空、>0（单成分 =100 合法）；trade_date 为供应商原值，不解释口径。

**canonical.sw_industry_member**（对应 index_member_all）：快照型，Key `(l3_code, ts_code, in_date)`；Unit = 完整快照（is_new=Y 与 is_new=N 两集合并验收后原子替换）。out_date 可空；is_new ∈ {Y,N}；in_date 异常早值记 WARNING 不清洗。

**canonical.sw_daily**：Key `(ts_code, trade_date)`；Unit = 交易日。name 保留快照原值；pe/pb/float_mv/total_mv 可空。

### 4.4 Scope Resolve 建议

- stock_daily / daily_basic：交易日 × 当日在市股票，全日停牌股 OUT_OF_SCOPE（依赖 trade_calendar、stock_basic、suspend_daily 上游版本；停牌日内时间段不排除整日获取）。
- adj_factor / stk_limit：交易日 × 当日在市（**含**全天停牌）；stk_limit 另含 B 股/基金，超额部分记 WARNING 不过滤。stk_limit 2010 年前的起点 UNKNOWN，回填历史前先补探查。
- index_daily：交易日 ×（index_basic 中 market ≠ SW 且当日未终止的指数）；指数历史起点不能由 list_date 推定（回填证据），首次全量用长区间按指数切片 + 触顶分页。
- sw_daily：交易日 × 申万行业指数集合；集合不能取 index_basic market=SW 全量（950 vs 439），需以 sw_daily 实际覆盖为准建立清单，差异记 WARNING。
- index_weight：指数 × 自然月；目标月 = 已结束月份（当月未发布属合法空）；指数清单来自 index_basic Scope；新月发布后保留近期重查。历史起点 200504（000300 实测），早于文档声明。
- index_basic / index_member_all：快照型默认 Scope，不做范围裁剪。

### 4.5 Validate（Canonical 侧）建议

ERROR（阻断受影响 Unit）：

- 空键、键重复、非法日期、日期越出 Scope。
- stock_daily：四价非空且 high≥low、open/close 在 [low, high] 内、vol≥0、amount≥0；index_daily 同样规则但只对四价俱全的行执行，close 必须非空。
- adj_factor ≤ 0；weight ≤ 0；up_limit < down_limit 或 ≤ 0。
- 当日在市应交易集合与 stock_daily/daily_basic 行集合的差异无法由停牌解释（依赖 suspend_daily 覆盖已成功发布的同日）。
- index_weight 指数×月行数为零（该指数当月应已发布时）；成分集合与上月全同/全异这类结构性突变可先 WARNING、连续或伴随交付异常时升级。

WARNING（保留事实并记录）：

- daily_basic 各估值字段 NULL 率（本轮 pe 空约 26%、dv 空约 32%）；index_daily OHLC 空值率。
- adj_factor 当日集合 ⊇ daily 集合的差集（应为停牌股）与 suspend_daily 不一致。
- stk_limit 行数超出 A 股在市数（B 股/基金部分）。
- index_weight 单月 trade_date 不等于月末交易日（口径不统一）、权重和明显偏离 100（指数而异）。
- index_member_all 的 in_date 早于 1990、is_new=Y 集合与在市股票数差异。
- 指数代码/成分代码形态超出已知模式（如含 CNY 段）。

## 5. 遗留未验证事项

- 限流阈值、积分余量、错误 token 行为未测；本轮未主动触发。
- 各接口盘中更新时点与盘后修订行为未测，不能声明数据最终性时点。
- stk_limit（2010 前）、sw_daily（2012-08 前）的历史起点未精确定位。
- sw_daily 439 行与申万指数全集的精确对应关系未逐代码核对。
- index_weight 全市场单日分页未取到终止页（offset=9,000 仍 has_more=true），总量未测尽；正式 Adapter 以分页终止为准，不设固定总量。
- 供应商跨请求快照一致性（同一 Run 多请求间数据漂移）未验证。
