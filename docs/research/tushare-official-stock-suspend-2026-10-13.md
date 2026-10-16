# 官方 stock_basic / suspend_d 探查与 Canonical Schema 建议

日期：2026-10-13（Asia/Shanghai）\
依据：`docs/design/core_design.md` V3.1.1\
状态：环境和真实 API 探查完成；下述 Schema 是建议稿，尚未作为正式发布契约冻结。

## 1. 环境与证据

采用 Python 3.13 + uv；依赖由 `pyproject.toml` 声明、`uv.lock` 锁定。实际验证版本：Python 3.13.13、DuckDB 1.5.6、PyArrow 24.0.0、dbt-core 1.12.5、dbt-duckdb 1.11.0、python-dotenv 1.2.4。Arrow/Parquet 直接由 PyArrow 提供。

探针直接用 Python 标准库发送官方 HTTPS JSON POST，省去 SDK/DataFrame 中间转换。当前脚本是研究工具：串行请求、40 秒超时、请求间隔至少 1.3 秒，失败留证据，不自动重试。正式 Client 后续需要实现有限重试和运行审计；分页、按交易所拆分和字段映射仍属于 Adapter。

本轮使用项目 `.env` 中的 `TUSHARE_TOKEN`，未使用 `TUSHARE_API_KEY`，共完成 **56 次真实请求，全部 HTTP 200 / code=0**。原始请求记录不含 token；响应落盘也进行 token 替换。记录请求参数、请求时间、HTTP 状态、供应商完整响应和耗时。

本地原始证据位于：

- `data/probes/official-initial-2026-10-13/`：2 次连通性探查。
- `data/probes/official-matrix-2026-10-13/`：31 次主要矩阵探查。
- `data/probes/official-followup-2026-10-13/`：21 次交付及字段补查。
- `data/probes/official-range-check-2026-10-13/`：2 次按日/区间对照。

原始响应仅保留在本地。可入 Git 的 [请求摘要](tushare-official-probe-summary-2026-10-13.json) 包含每次请求的参数、返回字段、行数、空值数、交付标记和原始文件 SHA-256。摘要中的空值统计合并了 JSON null 和空字符串。

已完成的检查：

- `uv pip check`：依赖兼容。
- `scripts/check_environment.py`：临时 DuckDB 建表、业务键重复拒绝、Arrow/Parquet 往返、DuckDB 扫描、dbt 生成并查询 Serving View。
- `tests/test_probe.py`：合法空结果与供应商错误区分、异常响应形态、重复业务键统计、测试用例名称唯一性。
- 在内存 DuckDB 中按推荐 DDL 加载 **5,921 条股票记录**及跨探针去除完全相同响应行后的 **11,435 条停复牌样本**，全部满足约束。样本合并只用于研究验证，不是正式 Adapter 的去重策略；其中截断响应也参与字段/键验证，不能据此发布这些日期范围。

## 2. 通用协议与交付标记

官方 [HTTP 文档](https://tushare.pro/document/1?doc_id=130) 描述请求的 `api_name / token / params / fields` 和响应的 `code / msg / data`。

本轮实测响应形态为：

```json
{
  "code": 0,
  "msg": null,
  "data": {
    "fields": ["ts_code", "trade_date", "suspend_timing", "suspend_type"],
    "items": [],
    "has_more": false,
    "count": 0
  }
}
```

`fields` 确定每行列顺序，不能假定它与请求顺序或文档示例完全相同。空响应仍带字段列表。**所有样本的 `count` 均为 0，包括非空响应，不能作为总行数或完整性依据。** `has_more` 是 HTTP 原始交付信息，标准 SDK 只转成 DataFrame 时可能丢失它。

两个接口均实测支持 `limit / offset`，虽然各自参数表没有列出。`limit=2, offset=0/2` 的两页合并与原查询前四行相同。尾页行为：

| 请求 | 行数 | has_more |
|---|---:|---|
| stock_basic，L，limit=2，offset=5572 | 2 | true |
| stock_basic，L，limit=2，offset=5574 | 0 | false |
| suspend_d，20200312，limit=2，offset=22 | 1 | false |
| suspend_d，20200312，limit=2，offset=23 | 0 | false |

因此 `has_more=true` 可能只是“本页达到 limit”，不证明下一页必有记录；不能把它当成精确总数。正式 Adapter 必须保留信封，检测 true、达到已知上限及重复分页；无法消除截断风险就阻断相关 Publication Unit。没有 `has_more` 时也不能默认数据完整，须结合已验证的拆分策略与行数上限判断。

本轮没有刻意耗尽积分、触发限流或测试错误 token，也没有证明供应商提供事务一致的跨请求快照。

## 3. stock_basic 的真实行为

官方 [stock_basic 文档](https://tushare.pro/document/2?doc_id=25) 标注默认 `list_status=L`，五类状态为 `L / D / P / G / UN`；单次上限 6,000 行，最低 2,000 积分，每分钟 50 次。当前 token 已证明可调用本接口；未查询账户积分。

实测状态与交易所拆分：

| list_status | SSE | SZSE | BSE | 合计 |
|---|---:|---:|---:|---:|
| L | 2,320 | 2,905 | 349 | 5,574 |
| D | 147 | 187 | 5 | 339 |
| P | 0 | 0 | 0 | 0 |
| G | 0 | 0 | 0 | 0 |
| UN | 0 | 3 | 5 | 8 |
| 合计 | 2,467 | 3,095 | 359 | **5,921** |

每种状态的三交易所并集与该状态全市场请求逐行相同，五种状态合并后 `ts_code` 无重复，所有普通全量请求 `has_more=false`。默认参数、`list_status=''` 都只返回 L，不是全状态。不存在的股票代码返回结构明确的合法空结果。

默认返回 10 列，未包含 Scope 必需的 `exchange / list_status / delist_date`。明确请求全部 17 个文档字段后都能返回。因此 Adapter 必须显式声明字段列表。

会影响 Schema 或验收的实际数据：

| 观察 | 实现影响 |
|---|---|
| UN 的 8 条 `list_date` 全空 | `list_date` 可空，并按状态验收；不能全表 NOT NULL。 |
| D 的 339 条上市/退市日期均有值 | 首版可要求 D 的两日期存在；缺失阻断候选并保留 Landing。 |
| L 中 `601198.SH`、`601059.SH` 已带 `delist_date=20261019` | L 与非空退市日期可以共存；不能把 L 的退市日期强行清空或禁止未来日期。 |
| 历史标识 `T600018.SH`，symbol=`T600018` | 标识保留 VARCHAR；不要限定六位数字，也不要转整数。 |
| UN 中 `874396.BJ` 的 symbol=`920238` | 不从 ts_code 重算 symbol，不强制二者数字部分相同。 |
| D 的 area/industry 各有 333 条空值；T600018.SH 的 market 为空 | 描述字段允许空；不能通过当前行业或板块补历史事实。 |
| UN 的 cnspell 有空值；act_name/act_ent_type 多处为空 | 补充属性可空。 |
| is_hs 实测为 N/H/S | 保留枚举字符串；转 BOOLEAN 会丢失沪/深港通区分。 |
| curr_type 本批均为 CNY | 保留供应商货币代码，不靠当前样本永久硬编码。 |

接口给的是抓取时的最新基础信息，**不是历史每日快照，也不是 PIT 数据**。名称、行业、港通状态不能直接解释为某历史日的值。当前 D 列表有历史证券，并不独立证明“所有曾上市证券绝无遗漏”；核心设计中这项供应商确认仍需要与官方范围说明和后续异常案例核对。

建议获取策略：每次完整快照请求 **五状态 × 三交易所，共 15 个请求**，显式字段。每片检查字段、范围、交付标记、6,000 行风险及错误。全部完成后才合并验收并原子替换一个完整快照；不能先发布 L、再补 D。P/G/UN 的空片允许成功，但 L 整体意外为空应阻断；与已有快照的状态/交易所数量突变记 WARNING 并结合已知交付问题判断，首版不设置无依据的固定“股票总数”常量。

## 4. suspend_d 的真实行为

官方 [suspend_d 文档](https://tushare.pro/document/2?doc_id=214) 标注历史起点 2000-01-03、单次 5,000 行、至少 2,000 积分；停牌为 S，复牌为 R，`suspend_timing` 表示日内停牌时间段。

实测单日请求未过滤类型时：

| trade_date | 行数 | S | R | 时间段非空 |
|---|---:|---:|---:|---:|
| 20000103 | 0 | 0 | 0 | 0 |
| 20100104 | 41 | 41 | 0 | 0 |
| 20150708 | 1,348 | 1,343 | 5 | 31 |
| 20200312 | 23 | 22 | 1 | 2 |
| 20260903 | 7 | 6 | 1 | 0 |
| 20261012 | 14 | 12 | 2 | 0 |
| 20261013 | 15 | 13 | 2 | 0 |
| 20261011 | 0 | 0 | 0 | 0 |

这些单日响应均为 `has_more=false`。20000103 的空响应只是该 token 此次请求没有记录，不能单凭日期等于文档历史起点认定当天确无停牌；20261011 的空响应也不能替代交易日历 Scope 判断。

20200312 全部 23 条恰好等于 S 过滤的 22 条与 R 过滤的 1 条并集。20200309–20200313 区间请求返回 100 条，恰好等于五个单日请求的并集，边界包含起止日期。多代码逗号参数实测有效。

`000029.SZ` 在 20200309–20200313 每天各返回一条 S；`300104.SZ` 在 2017 年 4 月样本中返回 20170417–20170428 的 10 个日期。接口确实能够每日列举连续停牌，不能将所有 S 仅理解为“停牌开始事件”。但 `300104.SZ` 的 20200309–20200313 请求为零记录，而官方文档的 20200312 示例列出了它：文档示例与当前返回有差异，不能扩大为供应商历史记录绝对完整的保证。

2015 全年返回恰好 5,000 条，日期只覆盖到 20150129；2020 全年也返回 5,000 条，只覆盖到 20200727，两者均为 `has_more=true`。**长区间返回 code=0 仍可严重截断，不能按这些区间直接发布 Coverage。**

时间段样本共有 33 种非空字符串，包含普通的 `09:30-10:00`、跨午休的 `11:25-13:05` 和多段的 `13:36-13:46,14:53-14:57`。合并样本中有 8 条多段记录。因此首版用一个可空 VARCHAR 保留完整字符串；不要简化为单对 `TIME`，不要拆成多行改变业务键。`suspend_type=R` 且 timing 为空是复牌记录，不是全天停牌。

建议获取策略：以交易日为 Publication Unit，**每个交易日一次全市场请求，不传 suspend_type**，显式请求全部四字段；现有压力样本 20150708 的 1,348 条远低于上限。按日仍须检查 5,000 行风险和 `has_more`；若触顶，专属 Adapter 用已验证的分页补齐并检查重复/范围，无法证明完整就阻断该日。不预先把全市场请求改为逐股票查询，也不要求 stock_basic 外键。

合法空集合必须独立携带成功交付结果、Schema 和合格 Unit；不能由零行反推成功。最终只有完整验收并提交 Coverage 的日期，才可把无记录解释成“供应商没有报告该事件”。当日更新不定期，建议抓取后保留近期重查入口；本轮未测更新时点，不能声明固定几点后数据最终完整。

## 5. 推荐 Canonical Schema

stock_basic 已采用为正式契约，DDL 见 [stock_basic.sql](../../src/quant_data/datasets/stock_basic.sql)；停复牌仍为 [suspend_daily Schema 建议](../design/canonical_suspend_daily_proposal.sql)。保留供应商清晰、已有项目使用的字段名，做类型和空值标准化；日期用 DuckDB DATE，股票标识用 VARCHAR。日期与日内时间段均采用中国市场本地日期/时间语义（Asia/Shanghai），不把 DATE 转成 UTC 时间戳。

### canonical.stock_basic

粒度：抓取时最新的证券基础信息，一条 `ts_code` 一行。\
Record Key：`ts_code`。\
Scope：官方五状态、三交易所的全部交付记录，包含未上市证券。\
Publication Unit：一个完整快照；成功后整表原子替换。

| 字段 | DuckDB 类型 | 空值 | 语义 |
|---|---|---|---|
| ts_code | VARCHAR | 不允许；主键 | 完整证券标识，含交易所后缀，原样保留历史特殊代码。 |
| symbol | VARCHAR | 不允许 | 供应商股票代码字符串，保留前导零，不由 ts_code 派生。 |
| name | VARCHAR | 不允许 | 当前证券简称，保留供应商标记。 |
| exchange | VARCHAR | 不允许 | SSE / SZSE / BSE。 |
| list_status | VARCHAR | 不允许 | L / D / P / G / UN；当前供应商状态，不等同于某历史日可交易。 |
| list_date | DATE | 条件允许 | L/D/P 必须有值；G/UN 可空。 |
| delist_date | DATE | 条件允许 | D 必须有值；其他状态可空或有已提供的日期，包括未来日期。 |
| market | VARCHAR | 允许 | 供应商板块名称，不硬编码板块枚举。 |
| area | VARCHAR | 允许 | 当前地域属性。 |
| industry | VARCHAR | 允许 | 当前供应商行业属性；不承诺历史有效性。 |
| fullname | VARCHAR | 允许 | 当前完整中文名称。 |
| enname | VARCHAR | 允许 | 当前英文名称。 |
| cnspell | VARCHAR | 允许 | 供应商拼音缩写，保留原大小写。 |
| curr_type | VARCHAR | 允许 | 供应商货币代码，例如 CNY；不是金额字段。 |
| is_hs | VARCHAR | 允许 | N / H / S；NULL 表示未提供，不等同于 N。 |
| act_name | VARCHAR | 允许 | 实控人名称原字符串，可能包含多个名字。 |
| act_ent_type | VARCHAR | 允许 | 实控人性质原字符串。 |

### canonical.suspend_daily

粒度：供应商报告的一只证券、一个日期、一个停/复牌类型的日记录。\
Record Key：`(ts_code, trade_date, suspend_type)`。\
Scope / Publication Unit：一个经交易日历确定的目标交易日。\
Write Policy：按日原子替换；合法零记录也替换旧日集合并更新元数据。

| 字段 | DuckDB 类型 | 空值 | 语义 |
|---|---|---|---|
| ts_code | VARCHAR | 不允许 | 供应商证券标识。 |
| trade_date | DATE | 不允许 | 停复牌记录对应的市场日期。 |
| suspend_type | VARCHAR | 不允许 | S=停牌；R=复牌。 |
| suspend_timing | VARCHAR | 允许 | 完整日内停牌时间段字符串，可含逗号分隔多段；NULL 表示未提供日内时间段。 |

本轮全部样本的三字段业务键无重复；甚至两字段键也未重复，但保留类型维度以免 S/R 相互覆盖。同一三字段键未来如出现多行，**先阻断相关日期并保留原始数据**，确认供应商粒度后再调整契约；不静默 last-write-wins，也不把 nullable timing 加入主键。

不增加 `is_full_day / is_tradable / snapshot_date / source / run_id / publication_id` 等列：交易状态便利字段放 Serving；抓取时间、来源和成功版本放 Meta。不补正常交易股票日行，不设指向当前 stock_basic 的外键，以免历史特殊证券和不同供应商交付范围造成无关阻断。

## 6. 标准化与发布门禁建议

**标准化：** 核心标识仅去除首尾空白，保留正文与大小写；描述字符串去首尾空白，空字符串转 NULL；`YYYYMMDD` 用严格格式解析为 DATE，非法非空日期必须报错，不能转成 NULL 掩盖坏数据。timing 去首尾空白、空字符串转 NULL，多段和跨午休原样保留。空 Candidate 必须通过显式 Schema 构造。

| 位置 | ERROR（阻断） | WARNING（保留事实） |
|---|---|---|
| Provider Client | 网络/HTTP/鉴权错误、非零供应商 code、非法信封及行列宽度。 | 可重试错误的有限重试记录。 |
| stock_basic Adapter | 请求片未完成、字段缺失、状态/交易所越界、已知截断未消除。 | 跨请求抓取窗口可能导致状态迁移，记录起止时间。 |
| stock_basic Canonical | 空主键/名称/代码/交易所/状态、重复主键、未知受约束枚举、非法日期、L/D/P 上市日期缺失、D 退市日期缺失、退市早于上市、整体 L 异常空。 | symbol 与 ts_code 前缀不一致、未来日期、属性空值统计、与上个快照数量大幅变化。 |
| suspend_d Adapter | 请求失败、字段缺失、日期或类型越界、截断未消除、分页重复/无法确认终止。 | 更新不定期；记录抓取时点。 |
| suspend_daily Canonical | 空键、非法日期、类型不为 S/R、三字段键重复；日内时间段有值但无法满足后续 Scope 解释时，不可把它当全天停牌。 | 与 stock_basic 标识集合不一致只告警；新增时间段格式保留原文并标记未知。 |

timing 为空且类型 S 时，可按供应商契约视作其报告的非日内停牌；类型 R 或 timing 非空都不能用于排除整日分钟获取。没有事件行不等于股票正常交易，还要有该日成功 Coverage、有效股票日期范围及交易日历。`stock_basic` 日期能帮助 Scope 判定，但退市日是否含当日、转板/换码映射和暂停上市历史边界尚未在本轮核实，应显式留 UNKNOWN，不能自行补连续历史。

两表的正式发布仍应遵守核心设计：验证成功 Unit 显式传给 Publisher；Canonical 和 Coverage/Publication 同一事务更新；失败不修改旧版本；快照分片不是独立 Publication Unit；停复牌零行 Unit 可成功发布。环境烟测仅验证数据类型、键约束与工具协作，本轮尚未实现或验证正式 Publisher 的事务行为。
