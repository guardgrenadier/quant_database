# stock_daily 接入提案

日期：2026-10-22。状态：**用户已确认提案，正式实现及独立库验收完成。** 下列项目建议已作为本 Dataset 的当前契约。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[质量对照报告](../research/galaxy-vs-tushare-daily-quality-2026-10-22.md)、[原日线研究](../research/tushare-official-daily-index-2026-10-14.md)及其[摘要](../research/tushare-official-daily-index-probe-summary-2026-10-14.json)。

## 1. 已确认及证据

- 用户已决定股票日线采用官方 Tushare；不接银河日线，不需要重做供应商选择。
- 质量对照报告只确认选型建议，没有冻结 Schema、Scope、Unit、零行或单位政策。报告 run_manifest/comparison/arbitration 三份本地文件的 SHA-256 已与报告核对一致。
- 官方 [daily 文档](https://tushare.pro/document/2?doc_id=27)声明未复权、停牌期间不提供行、每次 6,000 条，量为手、额为千元；pre_close 是除权昨收，不能强制等于上一行原始 close。15–16 点入库不保证其后绝不修订。
- 本轮只读补证 2026-10-21 全市场 daily：5,568 行，显式 11 列齐全、数值无 NULL、has_more=false。证据为 `data/probes/daily-onboarding-20261022T012318968173/requests/stock_day-attempt-01.json` 及同目录上级 summary.json，未写数据库。
- 原 2026-10-14 摘要仍在 Git，但其引用的旧单日原始文件本地已缺失。可使用本轮新信封及质量对照的七股历史样本，不声称旧单日重放已执行。

## 2. 逐项契约建议

| 接入项 | 项目建议 |
|---|---|
| 业务事实与边界 | 保存 daily 报告的全市场沪深北 A 股未复权日事实；不从当前股票名单筛历史代码，不补停牌日价格或零量行，不派生可交易状态 |
| Dataset / 表 | stock_daily / canonical.stock_daily |
| Record Key | (ts_code, trade_date)，重复和冲突均阻断所属日，不去重 |
| Schema | ts_code 为必填非空白 VARCHAR，trade_date 为必填 DATE；open/high/low/close/pre_close/change/pct_chg/vol/amount 为九列必填 DOUBLE，共 11 列 |
| 单位及时间 | 股票价格为人民币元，pct_chg 为百分数，vol 为手，amount 为千元；trade_date 保留市场业务日期，抓取时间留在审计 |
| 输入 | 显式 start_date/end_date，ASCII YYYYMMDD 闭区间；拒绝未来日期，允许上海当日但不保证终态；首版不加股票、交易所或状态过滤 |
| 上游依赖 | 同库目标区间每个自然日的 trade_calendar / SSE:YYYY-MM-DD 成功版本；不依赖 stock_basic、suspend_daily 或 stock_st，不自动获取上游 |
| Scope | 在固定版本同一事务读取日历：开市 IN_SCOPE、休市 OUT_OF_SCOPE、成功 Coverage 下实际缺日历事实为 UNKNOWN；提交前复核包括休市排除所用版本 |
| Publication Unit | 一个交易日全市场集合，YYYY-MM-DD；按日完整替换、日间可部分成功，合格日按现有一次事务提交 |
| 零行与缺失 | 开市日全市场零行 ERROR，保护旧版；个别股票无行仅表示供应商未报告，不能自动解释为停牌、退市或缺失错误，也不补造事实 |
| 获取与完整性 | 每个开市日一次 daily(trade_date=...)，显式 11 字段；has_more 必须 false、行数低于 6,000；请求失败或触顶阻断该日，继续其他日；不加通用分页框架 |
| 准备 | 严格字符串代码／日期、JSON 数值拒绝 bool/字符串隐式转码；文本清理一次；源日期必须匹配可信请求日再转 DATE；Raw 保留原值与请求标签 |
| ERROR | 空键、缺字段、坏类型／日期、错日、非有限数、四价非正、pre_close<0、vol/amount<0、high/low 与 open/close 顺序不符、重复键、整日零行 |
| WARNING | 允许个别已定义的非阻断诊断，但不从上市状态／停牌数据推导日线硬门禁；不设固定全市场股票数、跨行昨收连续性或涨跌停阈值 |
| 更新与修订 | 显式重跑完整替换目标日，接受历史修订；失败保护旧数据及指针；Coverage / Publication 表示满足供应商交付契约，不独立证明每只应交易股票无遗漏 |
| CLI 与证据 | ingest stock_daily --start-date ... --end-date ...，沿用 db/landing/keep-artifacts 和退出状态；独立库验收，不写默认权威库 |

股票暂停交易可能无行；本提案的 Unit 是全市场整日，**不要求一只股票每天一行**。这与原研究中“按当前在市股票加停牌集确定应交易全集”的建议不同：当前快照不足以可靠还原历史全集，首版不引入该假设。

先实现常规 11 列，不接 adj_factor、不计算前后复权、不增加 ah_vol/ah_amount、daily_basic 或涨跌停 Dataset。它们不是用户已确认的两个 Dataset 范围。

## 3. 证据缺口与实施边界

- 逐日完整交付不证明供应商无个股遗漏；历史当前代码映射、上市状态重建及停牌归因均不是本 Dataset 的验收结论。
- 6,000 行边界接近当前市场规模；首版遇到 has_more/触顶会明确失败，不能发布部分数据。后续真需要取尽时再实现已验证的专属分页，不提前新增框架。
- 不把请求空响应、转换失败和合法 NULL 混为一谈；九个数值必填是本项目建议，未来真实新样本冲突时须报告，不能自行改可空规则。

沿用现有 Dataset / Adapter Protocol、Runner、Validator、Publisher、Meta；新增专属 Dataset/DDL/Adapter、CLI 装配及必要测试，不修改冻结内核。相近实现是 suspend_daily 的按日范围与请求隔离，以及 index_basic 的严格数值边界；不继承它们的零行或股票枚举政策。不引入基类、注册器、规则 DSL 或额外生产依赖。

## 4. 必要验收计划

1. 同库真实发布目标日历，验证开闭市范围、缺依赖阻断、固定版本复核及已排除日修订。
2. 严格 Schema、源错日隔离、缺列／坏值／重复键、OHLC／量额规则与开市日零行；一日失败不污染另一合格日。
3. 重放新全市场日响应和可用历史样本，对照 Raw、Candidate、Canonical；明确原旧文件缺失及新样本来源。
4. 独立库正式 CLI 小区间及重跑，核对替换、Coverage、Publication、失败保护与证据 SHA-256；回归沿用现有机制测试。

## 5. 确认与实现记录

用户在本次会话明确回复“提案确认，开始实现”。**全市场逐交易日、未复权常规 11 列、不加当前股票名单／停牌依赖、开市日整日零行阻断、沿用 SSE 共同日历**均已确认并实现。

正式代码见 [Dataset](../../src/quant_data/datasets/stock_daily.py)、[DDL](../../src/quant_data/datasets/stock_daily.sql)、[Adapter](../../src/quant_data/sources/tushare/stock_daily.py)及[共同测试](../../tests/test_daily.py)。两个日线 Dataset 复用普通字段／日历／数值检查函数和严格解码函数，没有新增控制模型、基类或框架；CLI 已显式装配，内核和 Meta 不变。

## 6. 验收结果

新增 10 项日线链路测试全部通过：覆盖两个 Dataset 的依赖／代码范围／未知范围、闭市排除、重复运行、股票零行阻断、指数合法零行清旧、两种指数来源映射、坏值／错日／重复键、年片失败隔离、universe 及排除日版本修订、事务回滚、CLI 和真实信封重放。重放股票 5,568 行及宽基／申万样本，与独立标准化期望值逐字段一致。

```bash
uv run --frozen python -m unittest tests.test_daily -v
uv run --frozen python -m unittest discover -s tests -v
uv run --frozen python scripts/check_environment.py
```

全套 **82 项：81 项通过、1 项既有 stock_basic 旧真实样本缺失跳过**。新日线真实信封重放未跳过；环境烟测通过，新 DDL 另外由正式初始化及链路测试验证。批量内核 100 / 5,000 Unit SQL 调用数仍为 28。

2026-10-22 在独立库真实发布 2026-10-16–10-19 的 SSE 日历（含两个休市日），再运行正式 stock_daily CLI：两次运行均只请求两个开市日，各发布 **11,131 行**（10-16 为 5,567；10-19 为 5,564），无 ERROR、CLI 0、产物完整。重跑逐字段一致，生成新版本而不累加。

离线注入完整空 daily 响应时，获取 COMPLETE、有明确 Schema 的空 Candidate，由 stock_daily.nonempty 阻断；旧日线事实、Coverage 和 Publication 完整保留。注入明确标为无 HTTP，不作为供应商真实故障证据。

独立库及完整证据位于本地 `data/probes/daily-ingestion-validation-20261022T014518303442/`，包含 warehouse.duckdb、summary.json、verification.py、各 CLI JSON 及 landing/<run_id>。本轮连同指数验证共核对 **476 个 manifest 文件条目的 SHA-256**，全部通过。核对源响应独立标准化值、Candidate、Canonical、成功 Unit、Coverage、Publication、请求与依赖审计。未写默认权威库；原始业务数据只存本地。

机器摘要见 [daily-ingestion-summary-2026-10-22.json](../research/daily-ingestion-summary-2026-10-22.json)。本轮没有未完成的必需验收步骤；旧单日样本缺失、供应商真实遗漏／终态、当前未实现的复权及盘后字段仍按上文限制解释。
