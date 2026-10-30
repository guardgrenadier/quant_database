# stock_1min 接入提案（银河分钟行情 + 开盘集合竞价 + 完整研究网格）

日期：2026-10-22 初稿，2026-10-23 修订，2026-10-25 北交所探查与 Pilot 补验结论回写，2026-10-26 完整网格、状态、参考价与日线交叉检查修订，2026-10-27 正式实现及独立库验收完成。状态：**正式实现及独立库真实验收完成**。决议记录见第 10 节，验收结果见第 13 节。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)；研究证据：[银河接口总探查](../research/galaxy-amazingdata-interface-probe-2026-10-19.md)、[银河性能/并行探查](../research/galaxy-perf-probe-2026-10-19.md)、[3 秒快照分离开盘集合竞价](../research/galaxy-snapshot-probe-2026-10-21.md)、[银河分钟本地质量检查](../research/galaxy-local-quality-20260903.md)、[银河 vs Tushare 日线对照](../research/galaxy-vs-tushare-daily-quality-2026-10-22.md)、[历史分钟零量扫描](../research/historical-minute-zero-volume-2026-10-22.md)、[北交所收盘标签研究](../research/historical-minute-bj-close-labels-2026-10-22.md)、[北交所分钟探查](../research/galaxy-bj-minute-probe-2026-10-25.md)、[北交所 Pilot 补验与快照历史深度探查](../research/galaxy-bj-pilot-probe-2026-10-25.md)、[银河快照 pre_close 除权语义探查](../research/galaxy-preclose-exdate-probe-2026-10-26.md)。

## 1. 用户已定需求

1. **单一物化完整网格**：本地只保存一份分钟行情，直接服务批量因子计算；`canonical.stock_1min` 保存项目定义的标准研究分钟行情，不再只是银河稀疏交付事实。银河请求仍用 `auto_complete=0`，完整网格由项目按本提案构造，不使用供应商占位行。
2. **显式区分行状态**：新增 `status`，区分 `TRADED / NO_TRADE / SUSPENDED`；新增 `reference_price` 承载连续参考价格。零成交与停牌分钟均在 Canonical 物化，不另建 Dense Serving 表或实时补网格 View。
3. **请求时间范围 09:00–17:00**：尽可能拉取该范围内全部真实成交 bar。
4. **并行拉取**：沿用性能探查结论，2 或 4 请求并行。
5. **开盘集合竞价 925 bar**：每股票每日附加一次 3 秒快照窄窗拉取，抽取竞价量价，作为固定的 09:25 bar；竞价零成交仍不构造该行。
6. **发布范围 = 开市且当日在市、事实可确定的所有股票**：全天停牌 Unit 也保存完整分钟网格和 Coverage，只从银河 K 线与快照请求集合排除；引入 `suspend_daily` 与 `stock_daily` 依赖。
7. **北交所首版纳入**，原未验证项已于 2026-10-25 探查完毕、不阻断首版（第 6 节结论；920 代码映射与快照历史深度亦于当日补验关闭，见第 6 节第 2 项与第 8 节）。

**已放弃方向**：用 3 秒快照完全重建分钟主体、用 09:30 附近快照 `last` 估算连续竞价 open、以及“稀疏 Canonical + 独立 Dense Serving”双份存储均已放弃。快照只用于开盘竞价抽取和既有校验；分钟主体仍来自 K 线接口。2026-10-26 用户明确修订原“Canonical 不补造记录”的 stock_1min 专属口径，该变更不自动扩展到其他 Dataset。

## 2. 已确认的研究事实（证据基础）

- 分钟接口：底层 `tgw.QueryKline` 异步回调；字段 `open/high/low/close_price, volume_trade, value_trade`，缩放为价格 ÷1,000,000（元）、量 ÷100（股）、额 ÷100,000（元），已经独立复算逐行一致。
- 时间语义：银河为**开始分钟**标签，按 Asia/Shanghai 解释；开盘集合竞价并入首根（沪深 09:30），收盘集合竞价并入末根（沪深 14:59）；沪深连续竞价恰好 240 个槽位（09:30–11:29 + 13:00–14:59），无 15:00 行；15:00–17:00 查询返回空。北交所 242 槽位（09:30–11:30 + 13:00–15:00，15:00 为真实收盘行、独立承载收盘竞价）已经 12 只 × 4 日多样本验证；11:30/15:00 槽位无成交时源交付自然缺行，宽窗口确认 15:00 后无 bar，见第 6 节。
- `auto_complete=1` 补零量槽位（占位行，不表示成交也不表示停牌）；`=0` 只返回真实成交分钟；全天停牌返回 `kDataEmpty(-76)`，不补造 240 行。`kDataEmpty` 与覆盖前/无效代码多义，必须保存底层状态码。
- 并行：单会话并发 2/4 实测 0 失败、与串行逐值一致，吞吐约 2×/4×；并发 2/4 下无需请求间隔；真实约束是周流量配额（`kFlowOverLimit=-82` 等），阈值不可观测，出现即熔断。
- 会话工程：禁用 `ad.login`（失败 exit(0) 且会踢会话）；用 `set_cfg(force_logout=False)+tgw.Login`；登录后必须日历预热；同步 `QueryKline` 无超时，高层异常后锁可能不释放；会话级故障必须销毁进程、间隔 ≥60s 换新进程、最多 2 次；不多进程并发登录同账号。
- 竞价快照：`query_snapshot` 窄窗 09:24:57–09:25:30，取窗口内每证券**第一根 volume>0 行**为竞价锚点；锚点行 O=H=L=last=竞价价、volume/amount 为竞价累计量额；与分钟线量额精确守恒（6/6 差为 0）；竞价零成交证券自然退化为无竞价 bar。200 只窄窗实测 2.875 秒。
- 快照 `pre_close`：5 个沪深北真实除权除息日与 6 个普通交易日对照支持其为交易所除权后的参考昨收，且与 Tushare `daily.pre_close` 逐值一致；快照 OHLC/last 仍是当日未复权价，不能用 `pre_close == 昨日原始 close` 作为 invariant。该结论不外推到未测品种。
- 停牌数据质量：`S + suspend_timing IS NULL` 存在供应商反例（2010 年样本当日仍有成交），因此本项目维持其默认全天停牌判断，但必须用 `stock_daily` 正成交量等可靠成交证据检测冲突，不能明知有成交仍发布全天停牌网格。
- 质量口径：银河分钟线只能按**银河供应商口径**验收（与逐笔权威参考全字段精确一致率仅 19.05%）；分钟聚合与同源银河日线一致（沪深北样本均过）。
- 历史起点：官方声明 2013 年起，2012-12-31 实测 `kDataEmpty`；北交所开市于 2021-11-15，银河 BJ 分钟实测覆盖起点即开市日（更早 `kDataEmpty`，已验证）；快照历史深度已验证与分钟同为 2013 年起点（2012-01-04 及更早不可得；BJ 按代码时代：旧代码自 2021-11-15、920 代码自 2025-10-09）。

## 3. 逐项契约建议

| 接入项 | 项目建议 |
|---|---|
| 业务事实与边界 | 保存沪深北 A 股单一物化完整研究网格：银河真实成交 bar、09:25 开盘竞价、项目生成的零成交/停牌分钟及连续参考价；未复权原始价。09:30 OHLC 保留银河竞价与连续竞价混合口径，量额精确扣除开盘竞价。复权、跨源对齐和收盘竞价拆分不在本 Dataset 内 |
| Dataset / 表 | stock_1min / canonical.stock_1min |
| Record Key | (ts_code, trade_date, bar_time)；同键重复阻断所属 Unit，不去重 |
| Schema | 见第 4 节 DDL 草案；价格为元（DOUBLE），volume 为股（BIGINT），amount 为元（DOUBLE）；新增 `status` 与 `reference_price`，量额允许且仅允许非成交状态为零；不设 bar_type/num_trades/open_source 列 |
| 时间语义 | trade_date 为市场本地业务日（DATE）；bar_time 为开始分钟（TIME）；**09:25:00 恒为开盘集合竞价 bar**；抓取时间留在 Meta 审计。交易阶段按市场和日期解释，不能仅凭 `NO_TRADE` 推断连续可撮合 |
| 输入 | 显式 --start-date/--end-date ASCII YYYYMMDD 闭区间；拒绝未来日期，允许上海当日但不承诺当日终态（分钟数据应在收盘后运行，见第 8 节） |
| 上游依赖 | 同库已发布版本：trade_calendar、suspend_daily、stock_basic、stock_daily（全日成交检查及初始参考价），以及新增 bse_mapping / SNAPSHOT（BJ 时代代码与历史 Universe）；不自动获取上游，所有用于纳入或排除的版本均固定并在提交前复核 |
| Scope | 见第 5 节；Scope.units 为发布集合，新增 Scope.request_units 显式表示银河请求子集。开市 ∧ 当日在市 ∧ 事实可确定为 IN_SCOPE，全天停牌也在本地发布完整网格；仅 request_units 排除全天停牌。休市、未在市为 OUT_OF_SCOPE；冲突或状态无法确定为 UNKNOWN/ERROR。缺少所需 PUBLISHED Coverage 时沿用内核在 Scope 前终止 Run |
| Publication Unit | 一只股票 × 一个交易日；按 Unit 原子 DELETE+INSERT 完整替换；请求分片可覆盖多个 Unit，但验收与发布以 Unit 为界，一次请求失败只阻断其覆盖的 Unit |
| 零行与缺失 | Canonical IN_SCOPE Unit 不发布零行：非全天停牌须取得并核对源交付，全天停牌须依据确定事实生成完整网格。银河空响应不得直接变成全 `NO_TRADE`；竞价零成交仍允许无 925 bar。请求失败、交付不完整或状态未知不得通过补行制造完整表象 |
| 获取与完整性 | 见第 7 节；每次请求尝试落脱敏证据 JSON 与 meta.request_attempt（含底层状态码、耗时、行数）；无分页/总数概念，完整性由保守契约判定。全天停牌 Unit 无外部请求，其本地获取计划依据固定停牌事实完成 |
| 准备与标准化 | Raw 保留原始整数与底层状态码；prepare 通过新增的 con/pins 输入复核并读取固定上游，Candidate 才做缩放、代码/日期时间标准化、竞价构造、930 扣减、完整网格、status 与 reference_price；源日期位于请求闭区间并归属具体 code×date Unit |
| 验收规则 | 见第 9 节；集合式 DuckDB SQL 批量验收，不逐行转 Python 对象；连续交易槽位按市场/日期完整匹配，09:25 仅在有竞价成交时存在；逐状态检查 OHLC、量额与参考价 |
| 更新与修订 | 显式重跑完整替换目标 Unit，接受历史修订并重建派生行；银河修订 SLA 未知，T+N 重叠重取策略列为后续决策项，首版不自动重取 |
| CLI 与证据 | `ingest stock_1min --start-date ... --end-date ... [--parallel 2\|4]`；独立库验收，不写默认权威库；产物/证据生命周期沿用内核 |

## 4. Schema 草案

```sql
CREATE SCHEMA IF NOT EXISTS canonical;

-- 单一物化研究网格：真实成交、项目构造的零成交与停牌分钟通过 status 区分。
-- 价格为元，volume 为股，amount 为元；bar_time 为开始分钟标签。
-- 09:25:00 恒为开盘集合竞价 bar；09:30 OHLC 保留银河混合口径，量额扣除开盘竞价。
CREATE TABLE IF NOT EXISTS canonical.stock_1min (
    ts_code         VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    trade_date      DATE NOT NULL,
    bar_time        TIME_NS NOT NULL,
    status          VARCHAR NOT NULL CHECK (status IN ('TRADED', 'NO_TRADE', 'SUSPENDED')),
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          BIGINT NOT NULL CHECK (volume >= 0),
    amount          DOUBLE NOT NULL CHECK (isfinite(amount) AND amount >= 0),
    -- 连续估值价格：TRADED 等于 close，其余状态取最近有效参考价；无可靠值时允许 NULL。
    reference_price DOUBLE,
    PRIMARY KEY (ts_code, trade_date, bar_time),
    -- OHLC 必须同时有值或同时为空；有值时为有限正数并满足区间关系。
    CHECK ((open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
        OR (open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL)),
    CHECK (open IS NULL OR (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close)
                            AND open > 0 AND high > 0 AND low > 0 AND close > 0
                            AND high >= low AND open BETWEEN low AND high AND close BETWEEN low AND high)),
    CHECK (reference_price IS NULL OR (isfinite(reference_price) AND reference_price > 0)),
    CHECK (
        (status = 'TRADED' AND open IS NOT NULL AND volume > 0 AND amount > 0
             AND reference_price = close)
        OR
        (status = 'NO_TRADE' AND volume = 0 AND amount = 0
             AND ((reference_price IS NULL AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
               OR (reference_price IS NOT NULL AND open = reference_price AND high = reference_price
                   AND low = reference_price AND close = reference_price)))
        OR
        (status = 'SUSPENDED' AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL
             AND volume = 0 AND amount = 0)
    )
);
```

说明：

- `bar_time` 用 TIME 而非 TIMESTAMP，沿用“业务日期存 DATE”约定；跨日时间序列由消费方 `trade_date + bar_time` 合成。
- `status='TRADED'` 表示源接口实际交付的成交 bar；`NO_TRADE` 表示非停牌、源交付完整、该分钟没有项目定义下的成交 bar，包括收盘集合竞价等待期间的无成交槽位；`SUSPENDED` 表示能够确认该分钟整体不可交易。`NO_TRADE` 不单独承诺连续可撮合，订单模拟仍需交易阶段规则。
- `NO_TRADE` 有可靠参考价时 O=H=L=C=`reference_price`，量额为 0；这些 OHLC 是连续价格构造值，不表示发生成交。`SUSPENDED` 的 OHLC 为 NULL、量额为 0，`reference_price` 仅用于连续估值。
- 当日初始参考价统一使用当日 `stock_daily.pre_close`（除权后参考昨收）；当日没有该股票日线行时初始 `reference_price=NULL`，不读取前一 Canonical Unit、不依赖回补顺序，也不直接使用未经调整的昨日 close。日内出现 TRADED 后，参考价从该 close 起按时间顺序向前填充；此前无参考价的非成交行保持 NULL。快照 `pre_close` 可作交叉证据，不单列进分钟表。
- 不设 bar_type/num_trades/open_source 列：09:25 时间标签即竞价标识；不采用快照估算 open。`reference_price` 是统一连续估值列，收益和估值类因子无需逐行拼接 close 与历史价格。
- **930 扣减的口径代价（明示）**：09:30 `TRADED` 行的 volume/amount 为连续竞价净量额，OHLC 仍可能包含开盘竞价影响，open 等于竞价价；OHLC 与量额不描述同一组成交。`amount / volume` 是连续竞价净量额口径的 VWAP。日聚合直接对全表求和即为正确总量，原始 09:30 量额可由 925 与净量额重新相加恢复。
- 首分钟价格形态、开盘执行及 OHLC/量额联合假设的因子必须单独处理 09:30；不能把它当作普通同组成交 OHLCV bar。

## 5. Scope、停牌与日线交叉检查

对目标区间每个自然日 D，在固定上游版本的同一读取事务内：

1. `trade_calendar`：D 非开市日 → 整日 OUT_OF_SCOPE；开市事实缺失 → UNKNOWN 阻断。北交所共用 SSE 日历（已全量验证，见第 6 节第 6 项）。
2. 在市 membership（`stock_basic` 固定快照）：`list_date <= D` 且（`delist_date IS NULL` 或 `delist_date >= D`），交易所为 SSE/SZSE/BSE。不满足 → 该股票当日 OUT_OF_SCOPE。当前快照不是完整 PIT 历史保证，该边界不因完整网格而消失。
3. BJ 时代代码（新增 `bse_mapping / SNAPSHOT` 固定版本）：映射只负责同一证券的新旧代码关系，membership 仍由 stock_basic 的证券在市日期驱动。2025-10-09 前使用 o_code、自该日起使用 n_code；迁移股票旧代码即使不在 stock_basic，也按映射和对应证券 membership 纳入历史 Scope。Canonical ts_code 与当日银河请求代码均为时代代码，不把旧历史改写为 920 新代码，也不另造永久证券 ID。
4. 全天停牌默认判断（`suspend_daily`）：存在 `(ts_code, D, suspend_type='S')` 且 `suspend_timing IS NULL` → 默认全天停牌。**该推导仍只固化为 stock_1min 规则，不改写 suspend_daily 稀疏事实**；`S` 带时段与 `R` 不作为全天停牌排除条件。
5. `stock_daily` 冲突检查：全天停牌默认判断与同日 `vol > 0` 的日线记录冲突时，该 Unit 为 UNKNOWN/ERROR，不请求或发布全天 `SUSPENDED` 网格，记录 `suspend_conflict` 并保护旧数据。无全天停牌记录但日线无行/零量只能形成疑似漏报或全天无成交的待核对情形，不能单独证明停牌或正常零成交。
6. **发布 Scope**：开市 ∧ 当日在市 ∧ 上游事实足以确定状态的 Unit 为 IN_SCOPE，包括无成交冲突的全天停牌 Unit；休市、未在市为 OUT_OF_SCOPE，确定为冲突的范围为 UNKNOWN/ERROR。缺少所需 PUBLISHED Coverage 仍由 deps.check 在 Scope 前阻断整个 Run；只有成功 Coverage 下缺少契约要求的稠密事实时才在 Scope 中形成 UNKNOWN，稀疏 upstream 个股无行按本节业务规则解释。
7. **银河请求集合**：新增 `Scope.request_units` 显式保存 IN_SCOPE 中排除无冲突全天停牌后的子集，Adapter 只请求该子集。全天停牌由固定停牌事实完成本地交付并生成完整网格；不得先标记 OUT_OF_SCOPE 再绕过 Publisher 补行。
8. 非全天停牌 Unit 获取后执行分钟—日线交叉检查：日线正成交量但银河为空，或银河有成交但日线无行/零量，均为跨源冲突并阻断；日线无行/零量、无全天停牌记录且银河也为空时，可能是漏报停牌、合法全天无成交或日线遗漏，保持 UNKNOWN/ERROR，不补全天 `NO_TRADE`。日线正成交量不能证明盘中停牌名单完整。prepare 通过新增 con/pins 输入先复核版本再读取上游行。
9. `suspend_timing` 首版只支持 S 记录的单段 ASCII `HH:MM-HH:MM`，解释为 `[start,end)` 并与交易槽位求交；例如 09:34-10:04 对应 09:34–10:03，跨午休按绝对时间区间处理。R 不独立生成或解除区间。多段、其他分隔符、非法时间、跨日或无法确认边界均阻断受影响 Unit。真实成交行优先；只有无真实 bar 且整分钟落在已解析停牌区间时才标 SUSPENDED，其余缺行才能归为 NO_TRADE。

银河侧日频 `IS_SUSP_SEC` 仍只作异常排查证据，不新增为首版依赖。

## 6. 北交所：首版纳入口径与验证结论

首版将 BSE 纳入发布 Scope，按 242 个连续交易槽位生成完整网格，异常以 Unit 级 ERROR 暴露而不阻断全表。原未验证项已于 2026-10-25 专项探查（12 只在市 920 代码 × 4 个交易日模板矩阵 + 历史/停牌/竞价/收盘专项；证据：[北交所分钟探查](../research/galaxy-bj-minute-probe-2026-10-25.md)），结论回写如下：

1. **分钟时间模板（已验证）**：242 槽位（09:30–11:30 + 13:00–15:00）经 48 个 code×date 验证零越界标签，宽窗口（00:00–23:59）确认 15:00 后无 bar。11:30 槽位真实存在但仅 5/48 有源成交；15:00 源收盘行 47/48（唯一缺失经快照证实为收盘竞价零成交）；首源标签亦不固定。Canonical 将这些缺行按状态补齐，源槽位上界证据转为完整网格模板。
2. **历史覆盖与 920 代码迁移（已探明，映射源已确认）**：银河 BJ 分钟覆盖起点即北交所开市日 2021-11-15（更早 `kDataEmpty`，官方 2013 起点不适用于 BJ）。920 迁移于 2025-10-09 干净切换：旧代码数据止于其前最后交易日，新代码自该日起，跨时代用错代码返回 `kDataEmpty`。**跨迁移回补同一股票须按日期使用时代代码；映射源已确认为 tushare `bse_mapping`（248 条全量，本 token 有权限，证据已落盘），并经 3 对抽样拼接验证（新代码首日快照 pre_close == 旧代码末日 close，3/3 精确）**。BJ 回补下界 = max(2021-11-15, stock_basic 在市日期) 并按代码时代分段；248 条仅覆盖存量切换股票，更早退市股与 920 直接上市新股不在表内，Scope 仍以 stock_basic 在市日期驱动。
3. **全天停牌行为（已验证）**：两个真实长期停牌样本（831396.BJ @ 2024-01、872808.BJ @ 2025-06）停牌日银河均返回 `kDataEmpty(-76)`、复牌日正常交付，与沪深一致；tushare suspend_d 对开市后 BJ 停牌有正常报告，第 5 节默认判断及日线冲突检查同样适用于 BJ。
4. **集合竞价口径（已验证）**：快照窄窗锚点规则适用 BJ：23/24 股票日锚点在 09:25:00 整点，竞价零成交自然退化为无竞价 bar；930 守恒 24/24 差为 0。**BJ 快照相位码恒为 '0'/'1'，无阶段区分能力，锚点必须且只能用“窗口内第一根 volume>0”**；BJ 快照 num_trades 恒 0、存在同秒时间戳重复行（不影响 925 抽取）。BJ 收盘竞价独立承载于 15:00 bar，不似沪深并入 14:59。
5. **收盘标签质量（已验证，正面）**：银河 BJ 分钟无本地第三方文件的 15:01/15:02 晚标、15:30 大宗混入、收盘缺口形态；收盘为干净的 15:00 标签。
6. **日历假设（已验证）**：`get_calendar(market='BJ')` 全量 1,197 天（2021-11-15 至 2026-10-23）与 SSE 日历同区间逐日完全相同；以 SSE 日历驱动 BJ Scope 成立。
7. **零成交形态（已确认常态）**：48 样本源行数/242：最小 0.22、中位 0.75、最大 0.99；auto_complete=0 交付行全部 volume>0。BJ 低流动性下大量分钟天然缺行，正是 Canonical 必须显式构造 `NO_TRADE` 的主要场景之一。

## 7. 获取、网格构造与拉取策略

### 7.1 请求组织

- **分钟 K 线**：每请求 = 一只证券 × 一个日期段，`cyc_type=min1`、`auto_complete=0`、`cq_flag=0`（未复权）、`begin_time=900`、`end_time=1700`（HHMM）。日常增量每股票每日一请求；历史回补可每股票每请求最多 10 个交易日分段（是否放宽分段常数属实现决策）。多日与单日一致性、批量与单只一致性均已逐值验证。Raw 保存请求闭区间，源日期须落在区间内并最终归属具体 code×date Unit。只处理第 5 节定义的银河请求集合，不请求无冲突的全天停牌 Unit。
- **竞价快照**：每交易日按同一银河请求集合窄窗 `begin_time=92457000, end_time=92530000`（HHMMSSmmm），批量 ≤200 只/请求。全天停牌 Unit 不请求快照；其无 925 行是全天停牌网格的自然结果，不记竞价零成交 WARNING。

### 7.2 并行与会话

- 单会话内在途请求数默认 **2**，可选 **4**（CLI `--parallel`），不超过 4；并发 2/4 下**不加请求间隔**；每 Run 统计请求数。
- 熔断：任一请求返回 `kFlowOverLimit(-82)` / `kOverMaxQueryLimit(-78)` / `kOverLoad(-91)` → 终止本次 Run 的剩余获取，不试探阈值。
- 底层异步调用 + 每请求显式回调超时（30s）；请求级失败原样记录并跳过，只阻断受影响 Unit。
- **执行隔离**：Adapter 在独立 worker 子进程内完成登录、预热与全部请求（父进程硬超时监控）；会话级故障 → 销毁进程、间隔 ≥60s 换 ≤2 次新进程，不多进程并发登录同账号，自动化禁用 `force_logout`。worker 进程被销毁后，新 worker 不仅跳过已完成请求，还必须从对应证据恢复原始响应、请求区间及 Unit 归因并核对完整性，才能组成完整 Delivery。这是 Adapter 内部对“主处理路径不读证据文件”的受控例外，不是通用跨 Run 恢复能力；该形态不改变 Dataset/Runner 语义。
- 运行环境必须直连（剔除进程内代理环境变量），代理/TUN 下登录实测失败。

### 7.3 竞价 bar 构造与 930 扣减规则

- **925 bar 构造**：对每股票取快照窗口内第一根 `volume > 0` 行：`bar_time=09:25:00`、`status='TRADED'`，O=H=L=C=该行竞价价，`reference_price=close`，volume/amount 为竞价累计量额。窗口内无 volume>0 行（竞价零成交）→ 不构造 925 bar，Unit 其余部分照常验收并记 WARNING。当前批次意外缺席或响应不完整 ≠ 零成交，阻断受影响 Unit；只有能由底层状态码确认的历史快照不可得，才按第 8 节降级。竞价过程不可得，不构造。
- **930 扣减**：同 Unit 存在 925 bar 时，prepare 阶段将 09:30 原始 volume/amount 减去 925 对应量额（容差为 0）；OHLC 保持银河交付原值，不用快照 `last` 估算连续竞价 open。存在 925 但缺少原始 09:30 行时无法校验或扣减，属于明确数据异常，阻断 Unit。减后 volume 与 amount 均大于 0 时保留 `TRADED` 行，`reference_price=close`。
- **减后边界**：两者同时为 0 时，不保留 09:30 `TRADED` 行，但完整网格仍生成 09:30 `NO_TRADE` 行；存在 925 bar 时以 925 收盘价为 `reference_price`，OHLC 等于该参考价。仅 volume 或 amount 一项为 0 时不猜测状态，阻断 Unit。日总量 = 925 + 09:30 净量额 + 其余各 bar，原始交付值保留在 Raw 证据。
- **守恒前置校验**：扣减前对 Raw 值验证 925 量额 ≤ 09:30 原始量额、竞价价 ∈ 09:30 原始 [low, high]，容差为 0；违反即源数据异常，阻断所属 Unit（ERROR）。

### 7.4 完整网格、状态与参考价构造

1. **网格模板**：SSE/SZSE 每开市 Unit 生成 240 个连续交易槽位（09:30–11:29、13:00–14:59），BSE 生成 242 个（09:30–11:30、13:00–15:00）；925 仅在有开盘竞价成交时附加，不因“完整网格”强制补造。现有上游没有特殊交易时段事实，首版明确不支持半日交易等改变槽位集合的特殊安排，也不声称能从开闭市日历自动识别。
2. **状态归属**：真实成交行（含构造的 925）为 `TRADED`；无真实 bar 且能确认整分钟停牌为 `SUSPENDED`；其余已完整交付、非停牌的缺行为 `NO_TRADE`。真实成交行优先于停牌文本，绝不覆盖成停牌或零成交。
3. **零成交价格**：`NO_TRADE` 量额为 0，有参考价时 O=H=L=C=`reference_price`；`SUSPENDED` 量额为 0、OHLC 为 NULL。无可靠参考价时两者的 `reference_price` 可为 NULL，此时 `NO_TRADE` OHLC 也为 NULL。
4. **参考价顺序**：当日有该股票日线行时以 `stock_daily.pre_close` 初始化，否则初始 `reference_price=NULL`，不读取前一 Canonical Unit。随后按时间顺序推进，TRADED 的参考价等于其 close，非成交行继承当日内此前最近有效参考价；首个有效参考价出现之前保持 NULL。不得使用未来价格反向回填，结果与回补、重跑顺序无关。
5. **收盘阶段解释**：SSE 自 2018-08-20 起、SZSE 按既有收盘集合竞价规则，尾盘无成交槽位也可为 `NO_TRADE`；此前 SSE 相应槽位属于连续竞价。该日期只改变文档和消费者的业务解释，不改变 240 槽位模板或本 Dataset 的行构造，首版不引入 phase 字段或无输出差异的日期分支。BSE 收盘竞价由 15:00 行独立承载。`NO_TRADE` 不等于连续可撮合。
6. **构造前提**：只有获取、停牌与日线交叉证据均满足第 5 节时才生成目标 Unit；失败、冲突或 UNKNOWN 不生成可发布网格。完整网格是本 Dataset 的 Canonical 产物，不再由 Serving 二次补行。

### 7.5 规模估算（日常增量）

非全天停牌股票约 ~5,600 次 K 线请求 + ~28 次快照请求每交易日（实际随请求集合减少）；并发 2 下按单请求 0.1–0.4s 外推约 10–20 分钟。Canonical 每日约 5,600 × 240/242 行，另加实际 925 行。周流量配额未知：每 Run 记录请求数，回补须分阶段执行并遵守熔断；单内存批次仍需按日期区间控制规模。

## 8. 更新与运行时点

- 每日收盘后运行（建议 ≥15:30）：快照为历史接口，09:25 后任意时点可拉，与 K 线同一次 Run 完成。
- 上游日常顺序：trade_calendar → suspend_daily → stock_basic → stock_daily → bse_mapping → stock_1min；不自动触发上游。bse_mapping 是独立 SNAPSHOT Dataset，先完成自身真实接入和发布。
- 当日盘中运行不承诺终态；修订靠显式重跑，重跑完整替换目标 Unit 并重新生成全部派生行和参考价。
- **历史回补**（首版边界内、独立执行）：范围 2013-01-01 起；北交所以 2021-11-15 开市与 stock_basic 在市日期为下界，跨 2025-10-09 迁移依据固定 bse_mapping 按时代代码分段。回补前须发布同区间 stock_daily 和 bse_mapping 等全部依赖；按股票 × ≤10 交易日分段，分阶段执行并遵守熔断。2010–2012 保留本地历史 Parquet，跨源对齐不属于本 Dataset。
- 快照历史深度前提已关闭；若个别历史日期快照仍不可得，Unit 只发连续交易网格并记 WARNING，不整 Unit 阻断。此时没有 925 行，也不执行 930 竞价扣减，保留银河原始 930 量额；该降级口径必须与正常扣减 Unit 一并写入审计。

## 9. 验收规则（集合式 SQL，按 Unit 归因）

**ERROR（阻断所属 Unit，保护旧数据）：**

1. Record Key (ts_code, trade_date, bar_time) 重复；
2. `bar_time` 越出合法槽位，或连续交易槽位相对市场/日期模板缺行、多行；925 以外不得出现竞价性质 bar，925 只能为 `TRADED` 且不得在无竞价成交时补造；
3. 任一状态的字段契约破坏：TRADED 必须有合法正 OHLC、正量额且 `reference_price=close`；NO_TRADE/SUSPENDED 量额必须为 0；NO_TRADE OHLC 必须全等于参考价或全空；SUSPENDED OHLC 必须全空；所有非空价格必须有限且为正；
4. 行 trade_date 越出对应请求闭区间、无法归属具体 code×date Unit，或记录归属越出发布 Scope；
5. 竞价守恒（对 Raw 交付值，见 7.3）：存在 925 但缺少原始 09:30、925 量额 > 09:30 原始量额、竞价价越出 09:30 原始价格区间，或扣减后仅 volume/amount 一项为 0；
6. IN_SCOPE Unit 零行、银河空响应与日线正成交量冲突、银河成交与日线无行/零量冲突，或三方均无成交但状态无法区分；
7. 当前批次快照获取不完整或意外缺席（请求集合中的股票在快照响应中缺席）但同 Unit K 线已交付；由底层状态码确认的历史快照不可得按 WARNING 降级，不重复记 ERROR；
8. 从固定版本的初始 pre_close、逐行 TRADED close 和状态序列重算的 `reference_price` 与 Candidate 不一致，或除权日直接使用未调整昨日 close 冒充参考价；不得以“数值等于某个未来价格”作为判断；
9. 停牌时段解析失败、部分分钟边界被机械覆盖、真实成交行被标为非成交，或全天停牌 Unit 发出本应排除的源请求并造成语义冲突。

**WARNING（仅留审计）：** 竞价零成交无 925 bar 的 Unit；回补期快照不可得及未扣减 930 的降级；其他已定义的非阻断诊断。疑似 `suspend_d` 漏报若导致状态无法确定，必须 ERROR/UNKNOWN，不能仅 WARNING 放行。

**明确不做：** 不做分钟聚合对日线一致性的逐 Unit 数值门禁；stock_daily 在本 Dataset 中用于全日成交冲突/疑似漏报和初始参考价，不承担逐分钟聚合仲裁。不对本地历史 Parquet 做逐分钟对照；不设涨跌停/连续性阈值；不做收盘集合竞价拆分；不再接受“auto_complete=0 缺行所以 Canonical 可缺连续槽位”的旧验收口径。

## 10. 决议记录（2026-10-23 初版；2026-10-26 修订确认）

| # | 事项 | 当前决议 |
|---|---|---|
| 1 | Schema 与单位 | volume 股 / amount 元；不设 num_trades/bar_type/open_source；新增 status/reference_price，09:25 时间标签即竞价标识。**已确认，2026-10-26 修订** |
| 2 | 稀疏获取与完整落地 | 银河请求保持 auto_complete=0；Canonical 改存完整研究网格，逐连续槽位验收。**已确认，取代原“不补行/不精确匹配”** |
| 3 | 全天停牌判断 | 维持 S 且 suspend_timing IS NULL 的默认判断，suspend_daily 本身不固化；增加 stock_daily 成交冲突检查。**已确认，2026-10-26 修订** |
| 4 | 在市 Universe 来源 | stock_basic 快照 list_date/delist_date 驱动 membership；新增正式 bse_mapping / SNAPSHOT 驱动 BJ 时代代码，迁移旧代码可凭映射进入历史 Scope；不声称完整 PIT。**已确认，2026-10-26 修订** |
| 5 | 银河空响应 | 不得直接发布全 NO_TRADE 或合法零行；结合 stock_daily 与 suspend_daily 区分冲突和 UNKNOWN，无法确定即阻断。**已确认，2026-10-26 修订** |
| 6 | 北交所 | 首版纳入，按 242 连续槽位生成完整网格，15:00 独立承载收盘竞价；Canonical 和请求均使用当日时代代码。**已确认，2026-10-26 修订** |
| 7 | 并行度 | 默认 2、可选 4、无请求间隔。**已确认** |
| 8 | 会话执行 | worker 进程隔离 + 硬超时；换进程时从证据恢复已完成响应及 Unit 归因，而非仅跳过请求；属于 Adapter 内部受控例外。**已确认，2026-10-26 补充** |
| 9 | 复权 | 分钟 OHLC 存未复权原始价；reference_price 从除权后 pre_close 初始化；复权因子留给 Serving/独立事实。**已确认，2026-10-26 修订** |
| 10 | 修订策略 | 首版显式重跑并重建网格；T+N 重叠重取待样本确认后另行决策。**已确认** |
| 11 | 零成交与停牌 | 单份 Canonical 直接物化完整网格，status 区分 TRADED/NO_TRADE/SUSPENDED；suspend_timing 仅支持 S 的单段 HH:MM-HH:MM，按 [start,end) 映射，真实成交优先，未知格式/边界阻断。**已确认，2026-10-26 修订** |
| 12 | 925 bar | 3 秒快照窄窗抽取；竞价零成交无 bar；当前批次意外缺席 ERROR；底层状态码确认的历史不可得才降级 WARNING，且不执行 930 扣减。**已确认，2026-10-26 修订** |
| 13 | 收盘集合竞价 | 不拆分；末根保持银河交付口径。尾盘无成交槽位可为 NO_TRADE，SSE 以 2018-08-20 区分文档解释；首版不引入 phase 或无输出差异的日期分支。**已确认，2026-10-26 补充** |
| 14 | 09:30 混合口径 | OHLC 保留银河竞价+连续竞价口径，volume/amount 精确扣除 925；不采用快照估算 open；存在 925 但原始 930 缺失为 ERROR，扣减后仅单项为 0 也为 ERROR。**已确认，2026-10-26 修订** |
| 15 | 单份存储与 Canonical 定义 | 因批量因子读取和本地空间约束，只存一份物化行情；用户明确修订 stock_1min 的 Canonical 不补造约束，不扩展至其他 Dataset。**已确认，2026-10-26 新增** |
| 16 | 全天停牌 Scope | Scope.units 包含全天停牌并保存完整网格/Coverage；Scope.request_units 显式排除全天停牌，不得 OUT_OF_SCOPE 后绕过发布补行。**已确认，2026-10-26 修订** |
| 17 | 非成交行价格 | NO_TRADE OHLC=reference_price（无可靠参考价时全空）；SUSPENDED OHLC=NULL；两者量额均为 0。**已确认，2026-10-26 新增** |
| 18 | reference_price | 当日有日线行则从 stock_daily.pre_close 初始化，否则初始为 NULL，不读取前一 Canonical Unit；TRADED 等于 close，其后在当日内向前继承；验收从固定输入重算，结果不依赖回补顺序。**已确认，2026-10-26 修订** |
| 19 | 日线交叉检查 | 新增 stock_daily 依赖；vol>0 可否定全天停牌，日线无行/零量只能形成疑似漏报，不能单独证明停牌；冲突或未知阻断。**已确认，2026-10-26 新增** |
| 20 | 930 扣减后零量 | 不保留 TRADED 行，但完整网格生成 09:30 NO_TRADE；有 925 时参考价取 925 收盘价，OHLC 等于参考价。**已确认，2026-10-26 新增，取代原“删后无行”** |
| 21 | 特殊交易时段 | 现有上游无特殊时段事实，首版明确不支持半日交易等改变 240/242 槽位集合的安排，不声称自动识别。**已确认，2026-10-26 新增** |
| 22 | 依赖失败语义 | 沿用 deps.check：缺 PUBLISHED Coverage 在 Scope 前终止 Run；成功 Coverage 下缺稠密事实才形成 UNKNOWN；稀疏 upstream 个股无行由业务规则解释。**已确认，2026-10-26 新增** |
| 23 | 多日请求归属 | 源日期位于请求闭区间并可归属具体 code×date Unit，不再要求等于单一请求日。**已确认，2026-10-26 新增** |
| 24 | 共享接口最小扩展 | Scope 增加 request_units，SourceAdapter.prepare 增加 con/pins；prepare 读取上游前复核版本，其他 Adapter 机械适配，不引入隐式数据库旁路。**已确认，2026-10-26 新增** |
| 25 | BJ 映射正式接入 | 新增独立 bse_mapping SNAPSHOT Dataset/Source/CLI，可 pin/recheck；不使用探查 JSON 或硬编码作为生产映射。**已确认，2026-10-26 新增** |
| 26 | 无日线行参考价 | 当日无 stock_daily 行时初始 reference_price=NULL，不新增公司行动依赖，不继承前一已发布 Unit；接受全天停牌网格通常无参考价的首版边界。**已确认，2026-10-26 新增** |
| 27 | 开市日依赖派生 | Dataset.dependencies 最小扩展为 dependencies(params, con)，在同一事务内按已发布日历只声明开市日的 suspend_daily/stock_daily 依赖；现有 Dataset 机械适配，仍统一 pin/recheck。**已确认，2026-10-27 新增** |

## 11. 必要验收计划

1. 独立库发布全部上游依赖（trade_calendar / suspend_daily / stock_basic / stock_daily / bse_mapping 真实数据），验证发布 Scope 与 request_units、IN/OUT/UNKNOWN 边界（含 BJ 时代代码）、全天停牌本地交付，以及所有依赖版本的固定与提交前复核。分别验证缺 PUBLISHED Coverage 时 Scope 前终止 Run，以及成功 Coverage 下缺稠密事实时形成 UNKNOWN；不把稀疏个股无行当作依赖机制错误。
2. 真实样本重放：既有分钟原始 JSON、竞价窄窗证据和日线样本，对照 Raw、Candidate、Canonical；验证 925、930 扣减及混合口径、减后双零转 NO_TRADE、925 有而原始 930 缺失或扣减后仅单项为零时阻断、240/242 完整网格、三类 status、除权日 pre_close 初始化，以及无当日日线行时初始参考价为 NULL 且不同回补顺序结果一致。
3. 停牌专项：全日/盘中/边界分钟、真实成交与停牌文本重叠、S+NULL 与日线正量冲突、无停牌记录且日线/分钟均无行的歧义；严格验证 HH:MM-HH:MM 的 [start,end)、跨午休和 09:34-10:04 边界，多段/未知格式阻断；全天停牌合格 Unit 不请求银河但发布完整网格。BJ 另验证 bse_mapping 版本复核、迁移旧代码进入历史 Scope，以及迁移前后 Canonical/请求时代代码正确。
4. 故障注入离线测试：kDataEmpty、回调超时、会话级挂起、熔断、快照批次缺席与历史不可得状态区分、重复键/错日/越出请求区间/坏值、缺槽/多槽、固定输入重算参考价和状态违约；失败不得被补行掩盖，一股一日失败不污染其他 Unit，旧数据与 Coverage 完整保留。
5. 独立库正式 CLI 小区间（含休市、全天停牌、低流动性、除权日和 2018-08-20 前后 SSE 样本）及重跑：核对按 Unit 替换、Coverage、Publication、失败保护与证据 SHA-256；跨 2018-08-20 只断言统一的 240 槽位、无 15:00 行及正确 status/参考价，不声称验证 phase 识别；并发 2 与 4 结果逐值一致；回归沿用现有机制测试。
6. 受控 Pilot 补查（不阻断首版实现，阻断全量生产发布）：近期 T+1/T+N 修订样本持续观察、沪深新股上市首日抽样，以及完整网格物化后的实际存储和批量因子读取性能。此前北交所模板、920 迁移、历史覆盖、BJ 停牌/竞价、快照历史深度等探查已完成，见第 6 节。

## 12. 实施边界（明确不做）

- 沿用 Runner、Validator、Publisher、Meta，按决议 #24 做最小共享扩展：Scope 增加 request_units，SourceAdapter.prepare 增加 con/pins，现有 Adapter 机械适配；不得用隐式连接或可变状态绕过。新增 bse_mapping 与 stock_1min 的 Dataset/DDL/Source/CLI，`sources/galaxy/` Client（会话/超时/熔断/请求计数）与 stock_1min Adapter 及必要测试。bse_mapping 是新 Dataset，编码前须按接入清单形成独立的 Schema、SNAPSHOT、交付与验收契约，不能只在分钟 Adapter 内隐式接入。stock_1min 的 Canonical 语义和共享接口修订均已获用户确认；实现时同步核心架构与实施契约，其他 Dataset 仍不得自行补造事实。
- 不接指数分钟、不用快照重建分钟主体或估算 930 open、不接复权因子、不做调度/水位线、不引入 PyTables 或高层缓存接口、不为每 Unit 双取 auto_complete 两份、不引入基类/注册器/规则 DSL。
- 不另存稀疏 Canonical + Dense Serving 双份分钟表，不在查询时通过 View 临时生成网格；Raw 证据与审计仍按现有内核保存，不视为第二份业务行情表。
- 银河分钟线按银河供应商口径验收，不承诺等价于逐笔成交聚合；构造的 NO_TRADE/SUSPENDED 行是项目研究口径，不回写或冒充供应商原始交付。

## 13. 实现与验收结果

正式实现包括 `bse_mapping` 独立快照、银河 Client/worker、`stock_1min` Dataset/DDL/Adapter、CLI，以及 `request_units`、`dependencies(params,con)`、`prepare(con/pins)` 共享扩展。完整 122 项测试中 121 项通过、1 项既有股票真实样本缺失跳过；环境烟测、源码编译和差异空白检查通过。

2026-10-27 在 `data/probes/minute-ingestion-validation-20261027T013030/` 完成独立库真实验收：

- `bse_mapping` 两次正式 CLI 均发布 248 条，双键唯一，重跑不累加；映射及上游产物 220 个文件哈希核对通过。
- 因 `stock_basic` 两次真实请求均遇 SSL EOF，股票基础快照改为复制现有库已发布的 5,922 条事实作为上游夹具，原库只读；日历、停复牌、5,549 条当日日线和 BJ 映射仍由正式 CLI 获取。该夹具边界不计为 stock_basic 本轮真实接入验收。
- 2026-09-03 全市场分钟 Pilot 分别以并行 2、并行 4 成功一次：每次 5,555 个 IN_SCOPE Unit 全部 PUBLISHED、5,577 次请求尝试、0 ERROR；仅 50 个正常竞价零成交 WARNING。
- 两次 Canonical 均为 1,339,379 行，包含 5,499 个 09:25 竞价行、1,440 个全天 SUSPENDED 行；完整网格行数符合 240/242 加可选 925。两次 Canonical SHA-256 均为 `490a039caa6156c803d2e659bdb676c31a4af7c1f5f7853e952b8007c4216c49`，验证并发一致与重跑不累加。
- 全部 5,549 个日线 Unit 的分钟成交量精确一致；成交额在 Tushare 日线舍入精度内一致。初始参考价缺失仅存在于无当日日线行的全天停牌网格，符合决议 #26。
- 成功产物按默认生命周期清理，未写默认权威库，未执行全历史回补。首次工具超时、成功回调 `status=None` 和北交所 Unit 后缀模板问题均已修复并有回归测试。

首版之外的 T+N 修订观察、沪深新股首日、特殊交易时段及全历史分阶段回补仍未执行；不得把本次单日全市场成功解释为这些工作已完成。
