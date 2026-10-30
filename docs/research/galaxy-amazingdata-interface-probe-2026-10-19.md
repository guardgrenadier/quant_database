# 银河 AmazingData / tgw 接口探查报告（2026-10-19）

## 1. 总结论

本次已验证银河 SDK 可在直连网络下登录并读取股票日线、分钟线、基础证券信息、交易日历、历史状态、复权因子、指数成分和指数权重样本。对当前规划的 `stock_1min`，最合适的接入形态是：**按交易所确认交易日和证券 Universe，以“一只股票 × 一个交易日”为请求与验收 Unit，使用底层异步接口加显式超时，保存原始错误码与未缩放值，沪深按 240 个开始分钟标签验收，北交所暂按单独模板继续验证；未复权行情与原始单次/累计因子分开保存。**

但 `COMPLETE` 目前不能由 SDK 返回、240/242 行或同源日线一致单独证明：

- K 线响应没有总数、`has_more`、日终终态或修订标志。
- `kDataEmpty(-76)` 同时出现在覆盖前日期、无效代码、全天停牌和指数权重空区间；高层还会把它压成 `None`。
- 指数权重范围查询实测出现“重复 99 行且缺 99 个成分、SDK 不报错”的反例，证明供应商批量响应也可能内容损坏。
- 历史 Parquet 与银河在日级总量上可对齐，但逐分钟 OHLC/量额存在大量差异，不能作为银河逐分钟正确性的证明。
- 以用户确认的逐笔聚合权威文件 `20260903-1min.parquet` 严格对照，银河在 2,289 个权威非零分钟中只有 436 行全字段精确一致（19.05%）；详见[逐笔权威参考对照](galaxy-authority-20260903-quality.md)。因此银河分钟线可以按“银河供应商口径”继续设计，但不能宣称为逐笔权威分钟的等价数据。

因此：

- **可以进入接入设计**：沪深 2013 年以来的单股单日日线/分钟线（仅限明确的银河口径）、当前与历史代码表样本、交易日历开市日列表、历史状态、底层复权因子、沪深 300 成分、单日指数权重、北交所近期日线/分钟线样本。
- **仍被阻断**：2010–2012 分钟补齐、全天停牌合法零行的统一语义、北交所历史与模板确认、指数权重范围批量、当前环境下高层复权/成分缓存接口、官方频率/流量/并发/查询时段/修订 SLA、把银河分钟线当作逐笔权威等价数据、全量生产发布。
- **接入前必须用户确认**：复权保存口径、空响应/停牌发布语义、北交所时间模板、2013 前历史缺口策略、历史修订与重取窗口、是否引入 PyTables 或采用底层直连、进程级超时隔离方案。

## 2. 范围、材料与证据级别

本项目约束来自 `docs/design/core_design.md` 与 `docs/design/ingestion_v3.2_implementation.md`：获取成功、标准化成功、验收通过和发布成功必须分开；`stock_1min` 规划 Unit 为“单股单日”；失败不得覆盖旧成功数据。

官方资料：

- 官方 Gitee 仓库固定提交 `f0da75e7ff6b3fdde658184ee5c2795d4a4a47c2`。
- 官方开发手册 V1.1.1，本地证据 `data/probes/amazingdata-full-20261019-112300/static/official-AmazingData-manual.md`，SHA-256 `198427d2d107ba292776320ab26942099f50d085d2036b9a95f8e49b74d9f115`。
- 手册声明股票/指数 K 线起点为 2013 年，支持 1m/3m/5m/10m/15m/30m/60m/120m/日/周/月/季/年；见静态手册第 199–228 行。
- 手册 K 线字段与竞价算法见第 1248–1280、5679–5706 行；缓存语义见第 5709–5748 行。

SDK 证据：

- `AmazingData 1.1.15`、`tgw 1.0.9.2`，运行时 `V4.3.0.260626-rc2.0-YHZQ`，Python 3.13.13。
- Wheel 哈希与 `sdk-static.json` 相符；`AmazingData` 元数据未声明 PyTables，当前环境 `tables` 模块不存在。
- 关键 pyc 反汇编已保存到 `data/probes/amazingdata-full-20261019-112300/static/sdk-disassembly.txt`，SHA-256 `240bb35d44a0d6e5a70a1d7a6716bc9d74a3f91dc83b2d1591f95a36b2fc2766`。
- `tgw` Python 源码可直接检查：登录和查询包装在 `.venv/lib/python3.13/site-packages/tgw/interface.py`，错误码在 `error_code.py`，同步 SPI 在 `tmp_spi.py`。

证据级别在表格中明确区分：**实测**、**官方**、**SDK 实现**、**推断**、**未验证**。样本可用范围不外推为接口最大能力。

## 3. 可复现环境与最小调用

### 3.1 环境条件

1. `uv sync --frozen`，凭据只放在本地 `.env`：`AD_USERNAME`、`AD_PASSWORD`、`AD_HOST`、`AD_PORT`。
2. 需要到服务器的直连路由。实测 Windows/WSL 的系统代理或 TUN 活跃时，SDK 在 WSS 初始化阶段失败；关闭路由层代理且进程内移除代理环境变量后，同一凭据登录成功。
3. 探针只清除自身进程中的 `HTTP(S)_PROXY`、`ALL_PROXY`、`NO_PROXY`，不会修改系统代理；如果系统 TUN 接管路由，仅清进程环境变量不够。
4. 不使用官方 `ad.login()` 作为生产入口：SDK 实现显示其失败时 `exit(0)`，会话数超限后会最多 5 次 `force_logout=True` 重试踢会话。探针使用 `set_cfg(..., force_logout=False)` 加一次 `tgw.Login()`，以返回值判断。

### 3.2 最小调用示例

```python
from dotenv import dotenv_values
import AmazingData as ad
import tgw
from AmazingData.login.tgw_login import set_cfg

cfg_env = dotenv_values(".env")
cfg, mode, log_spi = set_cfg(
    cfg_env["AD_USERNAME"], cfg_env["AD_PASSWORD"],
    cfg_env.get("AD_HOST", "101.230.159.235"), int(cfg_env.get("AD_PORT", "8600")),
    force_logout=False,
)
if not tgw.Login(cfg, mode):
    raise RuntimeError("AmazingData login failed")
try:
    base = ad.BaseData()
    calendar = base.get_calendar(data_type="str", market="SH", date=20261019)
    market = ad.MarketData(calendar)
    result = market.query_kline(
        ["000001.SZ"], begin_date=20240417, end_date=20240417,
        period=ad.constant.Period.min1.value,
    )
    frame = result["000001.SZ"]   # 240 行：code/kline_time/OHLC/volume/amount
finally:
    tgw.Close()
```

该示例用于证明最小链路，不用于正式生产。高层查询内部没有调用级超时；正式执行必须另有进程级超时或隔离。可复跑探针：

```bash
uv run --frozen python scripts/probe_amazingdata.py --group minimal --output data/probes/<new-empty-dir>
uv run --frozen python scripts/probe_amazingdata.py --output data/probes/<new-empty-dir>
uv run --frozen python scripts/compare_amazingdata_evidence.py --evidence data/probes/<main-dir>
uv run --frozen python scripts/analyze_amazingdata_supplementary.py \
  --full data/probes/<main-dir> --recovery data/probes/<recovery-dir> \
  --extended data/probes/<extended-dir> --output data/probes/<main-dir>/supplementary-comparisons.json
```

`--output` 必须是不存在的新目录。探针只写 `data/probes`，不写 Canonical、Coverage 或正式 DuckDB。

## 4. 能力、覆盖与返回结构

| 能力 | 当前结果 | 证据与限制 |
| --- | --- | --- |
| 登录/会话 | 主服务器直连成功；备用地址未在本轮成功会话中复测 | 实测主地址 2.7–5.9 秒；代理/TUN 对照失败。`force_logout=False`，未测试并发会话和踢出策略 |
| 当前代码表 | 沪深 A 股 5,224；北交所 A 股 351 | 实测。当前表不含已退市 `300104.SZ`；不能单独作为历史 Universe |
| 历史代码表 | 2017-04-17 3,183；2024-04-17 5,116 | 2017 含 `300104.SZ`，2024 不含；样本支持 PIT Universe 方向。高层会先读本地 pkl，缺中间日期时有静默跳过的实现风险 |
| 证券基础信息 | 指定 4 只有效/退市代码均返回 | 实测 10 字段。`300104.SZ` 的 `IS_LISTED=0`，而手册示例称终止上市为 3，语义需确认 |
| 每日证券信息 | 5,224 行、7 字段 | 可用于名称、涨跌停、tick；`600519.SH list_day=20080605` 与 stock_basic/Tushare 的 2001-08-27 冲突，不能当上市日期 |
| 交易日历 | SH 8,744、SZ 8,736、BJ 1,193 个开市日 | 只返回开市日期列表，无休市行/`is_open`。差异样本均在 1991–1995；测试日及近期 SH/SZ 相同，BJ 自 2021-11-15 起且样本为 SH 子集 |
| 股票日线 | 沪深历史、近期及北交所近期样本成功 | 与同源分钟聚合在 OHLC、股数、金额上完全一致 |
| 股票 1 分钟 | 沪深 2013 样本与 2024/2026 样本成功；北交所近期成功 | 官方起点 2013；2012-12-31 单样本为 `kDataEmpty`。不能断言所有证券最早日期 |
| 历史证券状态 | 日频涨跌停、ST、全天停牌、除权除息 | 2017 `300104.SZ IS_SUSP_SEC=1`；2020 `300819.SZ` 盘中停牌日为 0，日频状态不能覆盖盘中事件 |
| 复权因子 | 底层 `QueryExFactorTable("600519")` 返回 32 条 | 字段 `ex_factor`、`cum_factor`；高层缓存接口因缺 PyTables 报错。单次因子与除息日前收关系实测通过 |
| 指数成分 | 底层沪深 300 返回 1,227 行 | 当前 300、历史 927；`RN=1..1227` 连续、业务键无重复。高层缓存接口同样因 PyTables 报错；其他约 600 指数未测 |
| 指数权重 | 沪深 300 单日 300 行成功；9 月范围响应异常 | 官方仅声明 5 个指数；范围批量阻断。2026-09-11 后和 10 月样本为 `kDataEmpty`，不能断言这是最大覆盖终点 |
| 其他 InfoData | SDK 提供财务、股东、龙虎榜、ETF、行业等 | 本轮未验证，不列入接入能力 |

## 5. K 线语义与字段

### 5.1 原始与高层字段

底层原始样本字段为：

- `market_type`、`security_code`、`orig_time`、`kline_time`
- `open_price/high_price/low_price/close_price`
- `volume_trade`、`value_trade`、`variety_category`

高层转换为：`code,kline_time,open,high,low,close,volume,amount`。

实测且经独立复算逐行一致的转换规则：

| 高层字段 | 转换 | 实测语义 |
| --- | --- | --- |
| `kline_time` | `%Y%m%d%H%M` → naive datetime | 交易所本地时间；本项目应按 Asia/Shanghai 解释 |
| OHLC | 原始整数 ÷ 1,000,000 | 元；样本股票保留到分位，不要转 float32 |
| `volume` | 原始整数 ÷ 100 后逐根转 int | 股；原始样本均为 100 的整数倍 |
| `amount` | 原始整数 ÷ 100,000 | 元 |
| `orig_time` | 未转换 | 本轮分钟样本均为 0，暂无业务用途 |

### 5.2 时间标签与竞价

官方手册明确：09:30 的 1 分钟线覆盖 09:30:00.000–09:30:59.999；开盘集合竞价进入首根，收盘集合竞价进入末根。

沪深实测：

- 正常交易日为 240 行：`09:30..11:29` 120 行，`13:00..14:59` 120 行。
- 没有独立 15:00 行；14:59 末根包含收盘集合竞价。
- `begin_time=930,end_time=1000` 返回 09:30–10:00 共 31 行，端点按标签包含。
- 查询 15:00–17:00 返回代码对应 `None`，不是 15:00 数据行。

北交所 `920000.BJ` 2026-10-16 实测为 242 行：`09:30..11:30` 121 行、`13:00..15:00` 121 行；`auto_complete=0` 为 102 个非零量行，242 个补全行中新增 140 行全为零量零额，11:30 只在补全结果中出现，15:00 是有真实成交的独立收盘行。该模板目前只有一个证券/日期样本，不能冻结为全北交所规则。

### 5.3 `auto_complete` 与零量语义

| 样本 | auto=1 | auto=0 | 结论 |
| --- | ---: | ---: | --- |
| `000001.SZ` 2024-04-17 | 240，其中 2 行零量 | 238，无零量行 | 补全 14:57/14:58 无成交分钟 |
| `300819.SZ` 2020-03-12 | 240，其中 173 行零量 | 67，无零量行 | 补全盘中停牌/无成交分钟 |
| `920000.BJ` 2026-10-16 | 242，其中 140 行零量 | 102，无零量行 | 补全大量低流动性分钟和 11:30 |
| `300104.SZ` 2017-04-17 全天停牌 | `kDataEmpty` | `kDataEmpty` | 不会补造全天 240 行 |

`auto_complete=1` 适合模板验收，但零量行只表示“补出该分钟槽位”，不表示发生交易，也不单独表示停牌。`auto_complete=0` 可帮助识别真实成交分布，却不能要求 240 行；正式获取不建议为每个 Unit 双取两份，除非用户接受额外流量。

### 5.4 空值与空响应

- 正常 K 线样本 8 列均无 null；非法/覆盖前/全天停牌时底层 `records=null` 且 `callback_status=-76`。
- 非交易日经高层日历过滤后返回空 dict，实际不发服务端请求。
- 有效+无效代码混合批量返回有效代码 240 行、无效代码 `None`，响应没有错误字段。
- `index_weight.FREE_SHARE_RATIO` 本轮 2,301 行全为 null；应保留 NULL，不得改写为 0。
- 指数成分当前记录 `CON_OUTDATE=NULL`；基础信息未退市 `DELISTDATE=NULL`。这些是合法空值，与转换失败不同。

## 6. 批量、拆分与同源核对

以下均为逐字段对照，不是只比行数：

| 对照 | 结果 |
| --- | --- |
| 两股票批量 vs 各自单日请求 | 各 240 行，时间键和 8 个字段全部一致 |
| 2024-04-16..17 合并请求 vs 2024-04-17 单日 | 重叠日 240 行全部一致 |
| 09:30–10:00 时间片 vs 全日过滤 | 31 行全部一致，端点包含 |
| 底层原始 JSON → 独立缩放/时间解析 vs 高层 DataFrame | 240 行全部一致 |
| 分钟聚合 vs 同源日线：000001.SZ/600519.SH 2024-04-17 | OHLC、股数、金额差异均为 0 |
| 分钟聚合 vs 同源日线：000001.SZ 2026-10-16 | 全部一致 |
| 北交所分钟聚合 vs 同源日线 | 全部一致 |
| 指数权重 9 月范围重跑 | 两次高层结果 2,301 行逐字段一致，证明异常是稳定复现而非随机缓存 |

同源日线一致说明 SDK 内部聚合自洽，但日线可能由同一批分钟或同一服务端逻辑生成，不能替代供应商的完整交付标志。

## 7. 历史 Parquet 交叉核对

本地历史文件说明见 `docs/research/historical-parquet-survey-2026-09-29.md`。本轮先纠正时间口径：本地文件是**结束分钟标签**，09:30 单独表示开盘竞价；银河是**开始分钟标签**，开盘竞价并入 09:30 首根。只把 15:00 并入 14:59 的错误/不足假设已保留在 `historical_parquet_close_only_failed_hypothesis`。

正确对齐方法：本地非 09:30 记录减一分钟，09:30 集合竞价与本地 09:31 记录合并到银河 09:30；本地 15:00 自然映射到银河 14:59。

| 样本 | 时间键 | 日级汇总 | 逐分钟差异 |
| --- | --- | --- | --- |
| 000001.SZ 2024-04-17 | 240/240 完全对齐 | 成交股数完全相同；金额差 -5.38 元，符合本地 float32 精度 | open 85、high 4、low 4、close 86、volume/amount 各 237 行不一致 |
| 600519.SH 2024-04-17 | 240/240 完全对齐 | 成交股数完全相同；金额差 -5.81 元 | open 178、high 43、low 61、close 161、volume 203、amount 227 行不一致 |
| 300819.SZ 2020-03-12 | 240/240 完全对齐 | 成交股数完全相同；金额差 0.5234375 元 | open 1、low 1、volume 6、amount 8 行不一致；差异集中在盘中恢复交易附近 |
| 300104.SZ 2017-04-17 | 本地 241 行全零，银河 `kDataEmpty` | 不可逐行比较 | 说明本地补零文件与银河空响应语义不同 |

解释：

- 日级成交股数完全一致，支持两边描述的是同一交易日总量。
- 逐分钟量额和大量 OHLC 不一致，说明历史 Parquet 不能作为银河逐分钟值正确的验收依据；也不能反过来断定银河错误，因为本地文件来源并非银河且没有供应商级权威性。
- 已有 2010–2012 本地历史不能由银河按当前声明起点补齐；跨供应商迁移需要另行定义权威来源和差异处理。

### 7.1 以逐笔权威参考评价银河分钟质量

用户进一步确认 `20260903-1min.parquet` 是逐笔成交聚合的权威参考后，本轮对其 12 只沪深代表样本做了原始整数精度对照。该文件每证券 237 个槽位，覆盖 09:30–14:56，零成交分钟 OHLC 为 NULL，不含 14:57–15:00；银河同日按 `auto_complete=0/1` 获取原始 K 线，双方只在重叠时段比较权威非零分钟。

严格结果：2,289 个权威非零分钟中，银河只有 436 行全字段精确一致（19.05%）；11 行缺失、13 行额外；成交量 1,387 行不同、成交额 1,421 行不同。09:30 银河多 2,573,716 股，主要是开盘集合竞价纳入口径差异；排除该分钟后连续竞价仍只有 19.14% 全字段精确，且存在大量相邻分钟量额抵消。整体平移检验证明不是统一错一分钟。

这意味着银河分钟线只能按“银河供应商口径”设计和验收，不能按该权威逐笔分钟口径通过数据质量验收。完整样本表、日总量、分时段和接入影响见[银河分钟数据与逐笔权威参考对照](galaxy-authority-20260903-quality.md)。

## 8. 失败模式与限制清单

| 项目 | 级别 | 实测 / SDK / 官方 | 接入影响 |
| --- | --- | --- | --- |
| 代理/TUN 导致登录失败 | 实测 | WSS connect fail / Logon Wait timeout；直连成功 | 运行环境必须保证直连；网络失败标 FAILED，不进入业务空数据 |
| 官方 `ad.login()` 失败退出码 0 | SDK 实现 | 且会话超限时最多 5 次 force logout | 正式接入禁用该入口；使用单次、非踢出登录并检查返回值 |
| 同步 `QueryKline` 无超时 | SDK 实现 | `Event.wait()` 无期限 | 必须进程级超时或自管异步回调超时 |
| 高层 `query_kline` 内部并发 | SDK 实现 | 每批最多 500 个底层请求，忽略即时发送失败返回值，等待无超时 | 正式采用单股单日小请求；不要直接调用高层大批量 |
| 高层异常后锁可能不释放 | SDK 实现 | 正常路径才 release，异常路径无 finally | 推荐每个 Run/分片用隔离 worker 进程，避免污染后续请求 |
| 日历偶发 None | 实测 | 登录成功后日历 150.7 秒返回 None，高层转 `TypeError`；新进程重试成功 | 不能无限重试同会话；保留错误并按新进程有限恢复 |
| `kDataEmpty` 多义 | 实测 | 覆盖前、无效代码、全天停牌、指数空区间均 -76 | 必须结合日历、历史 Universe 和交易状态；无效代码不得发布为空 Unit |
| 高层掩盖空错误码 | 实测 | 非交易日 `{}`；无效代码 `None`；指数空区间 `KeyError: TRADE_DATE` | 正式适配层必须保存底层状态，不使用高层形状推断原因 |
| 缺 PyTables | 实测/打包 | 服务端数据下载后保存 HDF5 时 `ImportError`；wheel 元数据未声明依赖 | 高层复权/成分当前阻断；选低层直连或显式补依赖 |
| 指数范围响应内容损坏 | 实测 | 2026-09 原始 2,400 行中 9/7 有 99 个重复键且缺 99 个成分；单日 300 正常 | 禁止直接信任范围批量；按日请求并做业务键/权重和验收 |
| 查询/流量限制阈值 | SDK 错误码存在，阈值未验证 | 有 `kOverLoad/kTimeout/kFlowOverLimit/kOverMaxQueryLimit/kNonQueryTimePeriod/kPermissionError` | 不主动压测额度；正式串行、保守频率，并向券商确认 |
| 历史修订 | 官方明示不保证准确/完整，修订 SLA 未验证 | 本轮只证明即时重跑一致 | 新数据延迟发布并做重叠重取；历史修订策略待确认 |
| 缓存旧数据 | 官方/SDK | `is_local=True` 可能不取最新；历史代码表本地缺失可能继续 | 正式证据获取不用旧缓存；日期参数直连或校验本地完整 |

性能观察只作样本，不构成 SLA：原始分钟请求约 0.1–0.4 秒，日历通常 0.4–0.9 秒但一次 150.7 秒失败，沪深当前代码表约 14 秒，北交所当前代码表约 21.8 秒。未测试最大代码数、最大历史跨度、并发或周流量。

## 9. 单股单日完整性的建议判据

这里区分“供应商可证明绝对完整”和“本项目约定交付合格”。前者目前没有标志；后者可以采用以下保守契约。

### 9.1 请求组织

1. Scope 先由已发布日历确定开市日；休市排除，未知阻断。
2. 用历史 Universe 确认该股票当日应存在；上市前/退市后合理排除，无法确定则 UNKNOWN。
3. 每 Unit 一次请求：一个 `security_code + market_type + trade_date + period + auto_complete + begin/end time`。不按多股票、多月或多年范围直接发布。
4. 沪深使用 SH/SZ 各自日历；北交所使用 BJ 日历。近期三者开市日样本相同，但不能写死永远相同。
5. 底层异步调用保留 `accepted`、即时错误码、回调状态、原始 rows 和耗时；设置回调超时与外层进程超时。
6. 串行小批量执行。一次失败只阻断受影响 Unit，不覆盖旧 Coverage。

### 9.2 沪深分钟 Unit 的必要验收

- 交易日和证券 Universe 均有成功版本；日期不早于 2013 的样本验证起点。
- 回调无错误且返回 list；`kDataEmpty` 不得默认合法。
- 记录键 `(code,kline_time)` 唯一；日期等于请求日；时间排序可验证。
- 时间集合恰好等于 240 个沪深开始分钟标签；无午休、11:30、15:00 或越界时间。
- 8 字段齐全；无非法 NULL；OHLC 有限且 high/low 关系成立；volume/amount 非负。
- 零量行允许，但须保留 volume/amount=0，不得删除或当作停牌事实。
- 分钟聚合与同源日线在 OHLC、股数上一致；金额使用明确表示并只允许最后小数位误差。
- 原始响应与证据文件在发布前保存成功。
- 批量/拆分一致性在当前发布策略中已抽样验证；正式初始阶段保留按日与按小范围的抽样复核。

满足以上条件可以按项目契约标为 `COMPLETE`，但报告和元数据仍应说明：这不是供应商显式日终证明，也不排除后续修订。

### 9.3 空响应与停牌

- 非交易日：OUT_OF_SCOPE，不请求、不写 `stock_1min` Coverage。
- 上市前/退市后：经历史 Universe 确认后 OUT_OF_SCOPE。
- 覆盖前日期：INCOMPLETE，不发布空集合。
- 无效代码：获取前阻断；若进入请求，-76 只能标 FAILED/UNKNOWN，不能标合法空。
- 全天停牌：只有在历史 Universe 证明代码有效，且有可信的**全天停牌正证据**时才可能作为合法零行。本轮 Galaxy 2017 样本 `IS_SUSP_SEC=1` 可作一类证据，但 2020 样本缺失；现有 `suspend_daily` 架构又不推导全天停牌。是否新增 Galaxy 日频状态依赖、如何由 Tushare timing 判定全天停牌，以及发布零行还是零量槽位，需要用户确认。
- 盘中停牌：分钟零量槽位保留，日度 `IS_SUSP_SEC=0` 不能否决；可结合 `suspend_daily` 的 `09:30-10:00` 事实解释，但不要求所有零量段都有停牌记录。

### 9.4 北交所

近期样本支持 242 模板和同源日线一致，但只有单股单日。北交所历史代码迁移、2013–2021 覆盖、全天停牌、11:30 零量边界和 15:00 收盘口径未完成代表验证，正式全市场接入仍阻断。

## 10. 与当前架构的适配建议

### 10.1 可沿用的机制

现有冻结内核可以直接承载：

- Scope、UnitDelivery、Delivery、Check、ValidationResult。
- 单股单日 Unit、部分成功、合法零行讨论、旧成功数据保护。
- 请求证据、准备诊断、统一验收和原子发布。

不需要改变冻结 invariant。需要新增的是 Galaxy Provider Client、Galaxy `stock_1min` Source Adapter 与 Dataset 契约；接口方案应先确认。

### 10.2 Provider Client

建议生产不直接复用高层 `ad.MarketData.query_kline`：

- 用 `set_cfg(force_logout=False) + tgw.Login`；以返回值和日志 SPI 判断，不看进程退出码。
- 用底层异步 SPI，回调带显式超时，原始 JSON 与状态落盘。
- 每个 Run 或较小分片使用独立 worker 进程并设置硬超时；超时后销毁进程。仅在新的会话/进程中做有限重试，不在可能污染的全局 C++ 会话上无限等待。
- 正式串行请求；并发上限、频率和流量阈值未确认前不做并发压测。

是否接受“worker 进程隔离”作为 Provider Client 运维形态，需要用户确认；这属于实现部署方案，不改变 Dataset/Runner 语义。

### 10.3 Source Adapter / 标准化

- Raw 保留原始整数与状态；Candidate 再转换为交易所本地 timestamp、Decimal 或固定缩放整数、股数整数、金额 Decimal/固定精度。不要沿用历史 Parquet 的 float32。
- 保存 `auto_complete`、market、请求日期、原始错误码和 SDK 版本标签。
- 不复权写入分钟事实；将 `ex_factor/cum_factor` 作为独立因子事实或经确认后生成派生复权视图。前复权/后复权公式不能只从字段名猜。
- 交易日状态、历史 Universe、分钟事实分开保存；不用缺失分钟反推停牌，不用日度停牌标志覆盖盘中时段。

### 10.4 Dataset 验收建议

初版 `stock_1min` 建议只承诺沪深 A 股普通完整交易日，北交所另设模板或暂缓：

- Record key：`(security_code, exchange, kline_time)`；Unit key：`exchange:security_code:trade_date`。
- 沪深模板 240 行；时间集合精确匹配。
- OHLC/volume/amount 类型、非负、有限、关系检查。
- 合法零量允许；合法零行 Unit 仅在用户确认全天停牌语义后开放。
- 同源日线聚合一致性作为 ERROR；跨历史 Parquet 只作 WARNING/研究证据，不作分钟级 ERROR。
- 近期数据设延迟发布和重叠重取；检测到同 Unit 内容修订时按新 Publication 处理，不静默覆盖。

## 11. 可以进入设计、仍阻断与补查优先级

### 11.1 可进入接入设计

1. 直连登录、非踢出会话、只读底层查询。
2. 沪深 2013+ 单股单日日线/分钟线，按上述门禁。
3. 当前/历史代码表与证券基础信息作为 Universe 辅助，但历史代码表交付需逐日证据复核。
4. 日频历史状态作为涨跌停、全天停牌、除权除息辅助；不覆盖盘中停牌。
5. 底层复权因子原始表。
6. 沪深 300 指数成分底层样本；其他指数逐个验证。
7. 单日指数权重，且强制业务键、成分数、权重和检查。
8. 北交所近期单股日线/分钟线的独立模板研究。

### 11.2 仍阻断

1. 2010–2012 Galaxy 分钟补齐。
2. 全天停牌零行发布语义。
3. 北交所全历史和模板冻结。
4. 指数权重任何多日范围批量；尤其 2026-09-07 已证明范围响应损坏。
5. 当前环境高层复权因子/指数成分缓存接口。
6. 官方查询时段、频率、并发、流量阈值和历史修订 SLA。
7. 特殊半日交易、全部交易所/品种、全量大规模性能。
8. 以历史 Parquet 逐分钟验收银河数据。

### 11.3 最值得下一步补查

1. 向银河书面确认：K 线交付终止/完整性说明、`auto_complete` 精确规则、BJ 模板、2013 起点适用品种、历史修订、错误码阈值、指数权重范围异常是否为已知问题。
2. 做一个受控 Pilot：覆盖多年份、沪深/北交所、上市首日、长期停牌、零成交、复权日和近期日，按单股单日重取并隔日复核。
3. 对复权因子和指数成分决定技术路线：补 PyTables 使用高层缓存，或正式使用底层只读接口并绕过 HDF5。
4. 对近期数据建立 T+1/T+N 重取样本，确认分钟与日线是否稳定、是否存在供应商修订。

## 12. 需要用户确认的决策

1. **复权口径**：Canonical 保存未复权事实 + 原始因子，Serving 派生前/后复权；还是直接保存复权价格？若派生，需确认精确公式、精度和因子日期对齐。
2. **空数据语义**：全天停牌发布零行、240 个零量槽位，还是不发布分钟 Coverage？接受哪个全天停牌证据源？
3. **分钟模板**：沪深固定 240 是否作为初版范围；北交所 242 样本是否继续按独立 Dataset/模板推进。
4. **历史范围**：Galaxy 初版是否从 2013 开始；2010–2012 保留原 Parquet 还是另找供应商；跨源逐分钟差异以谁为准。
5. **修订策略**：近期数据延迟多久发布，旧 Unit 重叠重取窗口多长，发现修订如何触发新版本。
6. **技术路线**：是否为高层 HDF5 缓存显式引入 PyTables，还是生产使用底层 `tgw` 查询。
7. **执行隔离**：是否接受 Provider Client 使用短命 worker 进程及硬超时，而不是在当前 Runner 进程内直接调用 C++ SDK。
8. **指数范围**：指数权重是否仅限官方 5 个指数；近期 `kDataEmpty` 是权限、覆盖还是数据源问题的确认责任。
9. **分钟权威口径**：是否要求 `stock_1min` 等价于逐笔成交聚合。若是，当前银河分钟样本不能通过质量验收；若接受银河供应商口径，必须在 Dataset 契约中明确集合竞价、零成交补价和逐笔边界规则差异。

## 13. 证据索引

所有证据均在本地、未入 Git；凭据不出现在请求证据或报告中。`summary.json` 对每个用例保存文件 SHA-256；`comparisons.json` 与补充分析还检查了索引哈希和凭据字符串命中，主目录、恢复目录、扩展目录均为 0。

| 目录 | 内容与用途 |
| --- | --- |
| `data/probes/amazingdata-20261019-101140/` | 初始主备连接失败、TLS 探测；用于代理/TUN 对照 |
| `data/probes/amazingdata-20261019-102646/` | 关闭路由代理后的登录和小范围查询成功摘要 |
| `data/probes/amazingdata-tun-20261019-104058/` | TUN 活跃时再次登录失败 |
| `data/probes/amazingdata-20261019-112234/` | 最小复跑：日历 + 2026-10-16 单股 240 行 |
| `data/probes/amazingdata-full-20261019-112300/` | 主矩阵 33 个用例、完整分钟/日线/历史对照、静态官方手册与 SDK 反汇编 |
| `data/probes/amazingdata-recovery-20261019-114000/` | 复权/成分底层恢复的首次实验及高层 PyTables 失败，保留 |
| `data/probes/amazingdata-recovery-20261019-114800/` | 底层复权、成分、指数权重单日/范围/空区间最终证据 |
| `data/probes/amazingdata-extended-20261019-115800/` | 登录成功但日历 150.7 秒 None 的失败实验 |
| `data/probes/amazingdata-extended-20261019-115500/` | 北交所初步样本和混合批量，后续被 120500 完整覆盖，保留 |
| `data/probes/amazingdata-extended-20261019-120100/` | 北交所 auto_complete 中间实验，保留 |
| `data/probes/amazingdata-extended-20261019-120500/` | 最终扩展：混合无效代码、2017 历史 Universe、北交所 0/1 补全、复权日前收 |
| `data/probes/amazingdata-authority-20261019-121500/` | 以逐笔权威 `20260903-1min.parquet` 对照 12 只沪深样本的银河 auto=0/1 原始分钟与日线 |

关键分析文件：

- `data/probes/amazingdata-full-20261019-112300/comparisons.json`，SHA-256 `e1f06a39514b7932804b704974c606a36e9907f47c67ff339d8852942ca18c71`。
- `data/probes/amazingdata-full-20261019-112300/supplementary-comparisons.json`，SHA-256 `fc3cdc85d940331561ad47543efa57b4da8da892f2763a2e1299e67ae49348f0`。
- `data/probes/amazingdata-full-20261019-112300/static/static-evidence-index.json`，记录官方手册、SDK 反汇编和静态元数据哈希。
- `data/probes/amazingdata-authority-20261019-121500/authority-comparisons.json`，SHA-256 `1bb240d3e716d4f454322338c752e16ffa9782dde29438df807b569e44b1db90`。
- 可复跑脚本：`scripts/probe_amazingdata.py`、`scripts/compare_amazingdata_evidence.py`、`scripts/analyze_amazingdata_supplementary.py`、`scripts/analyze_authority_20260903.py`。

失败实验未删除；中间目录也没有被后续成功结果覆盖。正式数据库仅被只读查询用于证券基础信息交叉核对，未写入。
