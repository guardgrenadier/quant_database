# Tushare 拉取策略（2026-09-19 更新）

状态：保留为网关端点实测知识（仍有效），供未来 dataset（日线、基本面等）实现 adapter 时参考。现行设计决策见 [ingestion-design-v2.md](ingestion-design-v2.md)；[ingestion-framework.md](ingestion-framework.md) 已冻结保留。

本页是策略结论；逐项请求参数、HTTP 响应和按 [ProMax 手册](../research/promax.md) 补测的结果见 [官方示例兼容性抽样](../research/tushare-proxy-official-examples-2026-09-19.md)。官方接口的行数上限与第三方网关的请求估算是两层约束，不能互相替代。

## 当前可用的请求方向

| 数据集 | 推荐切分与更新节奏 | 已验证的网关条件与缺口 |
| --- | --- | --- |
| [stock_basic](https://tushare.pro/document/2?doc_id=25) | 按状态与交易所取快照；超过单页时用 `limit/offset` | `L` 固定六字段分页得到 5000＋565 行、代码无交集；`D` 按 SSE/BSE 得到 147/5 行，SZSE 本轮遇 503，退市覆盖尚未核齐 |
| [trade_cal](https://tushare.pro/document/2?doc_id=26) | 按年拉日历，定期刷新未来区间 | 2018 全年传 `limit=500` 或显式选四字段，均返回 365 行；无二者时网关报 400 |
| [daily](https://tushare.pro/document/2?doc_id=27) | 按 `trade_date` 抓全市场；交易日 17 点后或次晨更新 | 2026-07-03 不限页返回 5516 行，接近官方单次 6000 行上限；显式 `limit=5000` 时要取后续页；停牌日无记录 |
| [adj_factor](https://tushare.pro/document/2?doc_id=28) | 全市场按交易日；当前日因子可在盘前获取 | 2018-07-18 显式列出三字段或传 `limit=5000`，均返回 3708 行；不传二者报 400。近期全市场日期尚未复测 |
| [daily_basic](https://tushare.pro/document/2?doc_id=32) | 全市场按交易日；17 点后或次晨更新 | 2018-07-26 指定六字段或传 `limit=5000`，均返回 3390 行；无二者报 400。官方单次上限 6000 行 |
| [stock_st](https://tushare.pro/document/2?doc_id=397) | 按交易日抓历史与增量 | 2025-08-13 传 `limit=500` 或指定三字段，均返回 175 行；单证券年窗曾返回范围外日期，必须逐行校验。官方单次上限 1000 行 |
| [suspend_d](https://tushare.pro/document/2?doc_id=214) | 按交易日抓历史与增量 | 2020-03-12 传 `limit=500` 或指定三字段，均返回 21 行；官方单次上限 5000 行 |
| [stk_limit](https://tushare.pro/document/2?doc_id=183) | 按交易日抓；触及上限时再为该接口设计代码切分 | 2019-06-25 传 `limit=5000` 或指定四字段，均返回 3621 行；官方单次上限 5800 行，且包含 A/B 股与基金 |
| [index_basic](https://tushare.pro/document/2?doc_id=94) | 按指数市场取列表；低频刷新 | `market='SW'` 指定五字段返回 950 行，传 `limit=2000` 返回 951 行；默认字段或缓存结果需核对一致性 |
| [index_daily](https://tushare.pro/document/2?doc_id=95) | 少量基准指数按代码抓；历史窗口与每日增量分别验证 | `399300.SZ` 不设日期返回 5418 行；指定 2018 区间传 `limit=1000` 或选三字段，均返回 186 行，网关有间歇 503 |
| [index_dailybasic](https://tushare.pro/document/2?doc_id=128) | 需要指数估值时按交易日抓 | 官方示例选四字段返回 14 行 |
| [index_weight](https://tushare.pro/document/2?doc_id=96) | 按指数、按月抓成分权重 | 官方月窗传 `limit=500` 返回 300 行；不设 `limit` 报 400，只设 `fields` 返回 0 行。抓取后核对成分数和权重 |

## 落地规则

1. 每个 dataset 明确传入所需 `fields`，并按 [ProMax 手册](../research/promax.md) 为大响应设置有界 `limit/offset`；记录参数、行数、请求时间和返回日期范围。已多次看到同条件仅增加 `fields` 或 `limit`，网关从 400 变成非空成功。Wrapper 不自动分页，防止一次成功请求被误认为完整数据。
2. 对每行校验证券／指数代码与日期落在请求范围内。HTTP 200、`code=0`、甚至大于官方标称上限的行数，都不能单独证明正确或完整；`stock_st` 的单证券年窗已出现日期过滤失效。
3. 接近或命中官方行数上限时，先验证该接口能否按代码拆分，再做补抓。不能假设所有端点都支持相同的逗号代码或分页参数。
4. HTTP 400 的「预计响应超限」先缩小字段、设置 `limit` 或缩短窗口；超过 366 天的范围是网关曾明确拒绝的请求形态。HTTP 503 `upstream_pool_exhausted` 做有限重试并记录失败，不能把它当成确定的行数上限。单证券不设日期或使用较长区间即使返回 200、0 行，也不能据此确认历史已完整拉取。
5. 退市证券的 SSE/BSE 已有小范围正例，SZSE 本轮仍为 503；指数权重已有单月正例。两者都还未完成历史覆盖和一致性校验，因此暂不宣称无幸存者偏差的股票池或完整指数成分。Client 继续只负责一次请求；请求切分与分页属于各 dataset 的源 adapter，整日完整性属于 pipeline（见 [ingestion-framework.md](ingestion-framework.md)）。
