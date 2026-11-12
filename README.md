# quant_data

个人量化数据库，已接入 `stock_basic` 完整快照、SSE `trade_calendar`、`suspend_daily` 停复牌、`stock_st` 名单、`index_basic` 目标指数快照、`index_member_all` 申万行业归属完整快照、`stock_daily` 未复权股票日线、`daily_basic` 每日指标、`adj_factor` 原始复权因子、`index_daily` universe 内指数日线、`index_weight` 银河每日指数权重、`stk_limit` 每日涨跌停价格、`bse_mapping` 北交所代码映射，以及 `stock_1min` 完整分钟研究网格：获取及证据 → 集中准备 Candidate → 统一验收 → Canonical 和 Meta 原子发布。

| 阅读目的 | 文档 |
|---|---|
| 理解核心心智模型、职责与 invariant | [核心架构](docs/design/core_design.md) |
| 理解当前模型、接口、事务与能力边界 | [技术设计与实现边界](docs/design/ingestion_v3.2_implementation.md) |
| 运行日频行业特征及查看环境下载清单 | [dbt Serving](dbt/README.md) |

这两份是当前维护文档：先读核心架构，再按需查技术细节。[V3.1.1 归档](docs/design/archive/core_design_v3.1.1.md)保留设计历史与 invariant 对照，不代表当前实现进度。

## 日频回填状态（2026-11-06）

2025 全年七项日频已完成：243 个开市日、194,157 个成功 Unit、5,436,950 行，年度覆盖、旧数据保护和 Serving 验收通过。行业未覆盖 691 行、8 个证券，按用户决定保留 NULL 并集中汇报。该阶段仅保留一份已校验滚动备份；自 2026-11-12 起，所有新生产备份及临时文件固定写入 `/mnt/d/quant_data_backups/`，不得写入 WSL 内的 `data/backups/`，未生成月度全库副本。审计引用 SQL 已优化，七项请求间隔按用户确认的 10,000 积分降为 0.13 秒；接口和业务语义保持不变。见[2025 年验收与清单](docs/operations/daily_backfill_2025_validation_20261106.md)及[性能优化记录](docs/operations/ingestion_performance_optimization_20261106.md)。2024 年未自动执行。

用户确认的 2026 年 9 月倒序至 1 月回填已完成。连同此前完成的 10 月，七项 Tushare 日频完整覆盖 2026-01-01～2026-10-30 的 198 个开市日，共 158,202 个成功 Unit；逐 Unit 行数、旧事实／版本保护、Serving 和最终备份均已验收。002310.SZ 在 1～6 月的 116 行行业历史缺口保留 NULL 并集中汇报，不阻断回填。1 月 14 日 ST 网络缺口已单日修复，历史失败审计保留。详见[最终验收与行业清单](docs/operations/daily_backfill_2026_completion_20261105.md)。11 月不在本次范围，分钟和指数权重继续暂缓。

## 生产准备状态（2026-11-03）

生产权威库使用 `data/warehouse.duckdb`，与 ingestion CLI 的默认路径一致；任务中仍建议显式传入 `--db`，dbt 使用绝对路径的 `QUANT_DATA_DB`。该文件本地保存，不入 Git，克隆仓库不会带出生产数据。

2026-11-09 已完成生产存储重建：六张持续增长 Canonical 表不物化 ART，分钟为十列且 bar_time=TIME，参考价由 View 提供。库从 38.3 GiB 降至 14.1 GiB，全表/逐日及参考价对账通过；重建完成时 SHA-256 为 `c06abe0caca98feff921a9f76fd00d5e816d86f6dfeb28c4a3d4c3474108401d`。批前旧库和批后新库恢复点当时保留在 `data/backups/`；该路径仅为历史记录，现行新备份一律写入 `/mnt/d/quant_data_backups/`。

本次通过现有 `db.initialize` 和十四个 Dataset 的正式 DDL 新建权威库，已核验 14 张 Canonical 表、7 张 Meta 表、主键及既有约束。建库时全部表为空；现已通过正式 CLI 完成以下基础数据的首次真实获取与发布，没有导入旧库或测试夹具：

| 已发布 Dataset | 范围 | 行数 |
|---|---|---:|
| `trade_calendar` | SSE，2013-01-01～2026-12-31，包含休市日 | 5,113 |
| `stock_basic` | 完整快照 | 5,926 |
| `index_basic` | 目标指数完整快照 | 793 |
| `index_member_all` | 完整当前及历史行业归属 | 7,928 |
| `bse_mapping` | 完整北交所代码映射 | 248 |

五次 Run 均为 SUCCESS，64 次请求全部成功；事实行数、Publication、Coverage 和 Unit 审计核对一致，共 5,117 个 PUBLISHED Unit、零 ERROR。五条 WARNING 分别为未来日历（58 日）、股票可选属性缺失、股票特殊代码映射（2 行）、指数可选属性缺失及行业异常早日期（9 行），按既有契约保留源值。成功运行沿用默认生命周期，Landing 已清理，逐请求和检查审计保留在 Meta。

单日验证 A 阶段已成功发布 2026-10-30 的七项 Tushare 日频数据：

| Dataset | 行数 | 成功 Unit 数 |
|---|---:|---:|
| `suspend_daily` | 7 | 1 |
| `stock_st` | 197 | 1 |
| `stock_daily` | 5,571 | 1 |
| `daily_basic` | 5,571 | 1 |
| `adj_factor` | 5,581 | 1 |
| `stk_limit` | 5,654 | 1 |
| `index_daily` | 438 | 793 |

A 阶段 34 次请求全部成功、零 ERROR，新增 799 个 PUBLISHED Unit；该 A 阶段结束时成功 Coverage 共 5,916 个。指数的 355 个零行代码日仅表示本次供应商完整交付中没有报告记录，按既有契约发布成功版本。两条 WARNING 分别为 daily_basic 的三行股本关系异常，以及申万 Raw 中 25 行范围外记录的显式排除。

银河 `stock_1min` 已完成 B 阶段补跑及 C 阶段交叉验收：2026-10-30 共 5,577 个成功 Unit、1,344,709 行，5,599 次请求全部成功，零 ERROR。分钟状态为 TRADED 1,291,889 行、NO_TRADE 51,380 行、SUSPENDED 1,440 行；50 条 WARNING 均为按契约允许的竞价零成交。沪深每股票日 240 个连续槽位、北交所 242 个连续槽位、可选 09:25 行、状态及参考价规则均通过正式 Dataset 只读复查，五项固定上游版本与审计一致。

C 阶段已成功构建 `serving.stock_daily_with_sw_industry` View，dbt 发布门禁、模型及两项数据测试全部通过；5,571 条日线全部匹配行业，无重复键，原行情字段双向核对一致。构建前后 21 张 Canonical/Meta 表集合摘要不变，未改写来源事实或发布审计。该 C 阶段结束时成功 Coverage 共 11,493 个。

`index_weight` 的五个当日 Unit 仍为 FAILED，表中零行、无成功 Publication；用户确认该接口因供应商问题滞后约一个月。既有失败审计和五条 FAILED Coverage 保留，权重获取规则不变，未用其他日期代替本日权重。本次单日分钟／Serving 验收通过不表示本日十四个 Dataset 均齐备，也不表示 2013 年起历史已回补或分钟全量生产补验门槛已关闭。阶段记录见[交接及验收说明](docs/operations/galaxy_validation_20261030.md)。

旧项目本地归档、`data/probes/`、`data/landing/`、`data/downloads/` 以及三个旧验证／演示／备份库已清理。历史报告中的产物路径描述当时的验收，不表示文件仍在。必要的 48 个真实响应已保存到 [tests/fixtures](tests/fixtures/README.md)，供离线测试使用；原本缺失的 stock_basic walkthrough 仍是既有跳过项。

下文 `data/probes/my-*` 是可新建的隔离验证路径示例，不是已准备好的库；生产任务使用上述权威库。分钟支持银河及网盘本地 Parquet 正式链路，具体市场覆盖仍遵循已确认契约。旧 `data/raw/` 和根目录两个历史 Parquet 样本已于 2026-11-03 经确认永久删除，未导入新权威库；2026-11-05 已通过本地 Parquet 完成 2026-04-17 生产 Pilot，全历史尚未回填。

## stock_basic Ingestion

在仓库根目录设置 `.env` 的 `TUSHARE_TOKEN` 后执行：

```bash
uv sync --frozen
uv run --frozen python -m quant_data ingest stock_basic --db data/warehouse.duckdb
```

默认及生产路径为 `data/warehouse.duckdb`；旧版 Meta 仍明确报不兼容，不自动迁移或删除旧数据。每次请求五种上市状态 × 三交易所，获取全部 17 字段，直接形成 Polars Raw/Candidate 数据表；验收后单事务替换数据、成功版本、Coverage 和 Unit 审计。重跑产生新版本，失败保留旧成功数据。

可指定独立验证库及暂存目录：

```bash
uv run --frozen python -m quant_data ingest stock_basic \
  --db data/probes/my-validation/warehouse.duckdb \
  --landing-dir data/probes/my-validation/landing
```

成功默认清理 Landing；失败或部分成功保留原始信封。CLI 输出具名模型的 JSON 结果及独立的产物状态。请求尝试位于 `meta.request_attempt`，检查位于 `meta.validation_check`，Unit 保存引用和阶段；`meta.run.summary` 仅保存计数。全程使用同一 DuckDB 连接。

添加 `--keep-artifacts` 保留完整 walkthrough 产物：

```bash
uv run --frozen python -m quant_data ingest stock_basic \
  --db data/probes/my-walkthrough/warehouse.duckdb --keep-artifacts
# 或一次生成独立观察库和产物目录
uv run --frozen python scripts/capture_stock_basic_walkthrough.py
```

产物目录的 README 提供阅读顺序；包含原始响应、Raw/Candidate Parquet、Schema、`preparation.json` 准备诊断、统一验收、发布和各阶段 Meta 快照。获取 COMPLETE 不等于转换或验收通过；零行结合获取证据、准备诊断和 Dataset 契约判断。诊断不完整以 `artifacts_complete=false` 和 `export_errors` 表达，成功发布状态独立保留。

运行时保持串行访问：Ingestion、dbt 和外部研究进程应错开访问同一文件。各 Dataset 使用专属获取计划；尚无通用分页或恢复执行。Serving 模型定义已提供，需待生产来源成功发布后构建。

## trade_calendar Ingestion

首版仅保存 SSE 供应商日历，按当前确认口径供沪深北市场共同使用，不复制生成 SZSE/BSE 行。开市与休市均有显式记录，主键及发布 Unit 为 `(exchange, cal_date)`；后续依赖通过 `trade_calendar / SSE:YYYY-MM-DD` 定位成功版本。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20100101 --end-date 20261231 \
  --db data/warehouse.duckdb
```

日期为显式闭区间，包含休市日，不自动猜测起止日期；内部按自然年请求片段、按自然日验收并替换。缺日、空交付或错误不能生成默认休市行，也不能清除旧成功日历；其他合格日期仍可发布，PARTIAL 返回非零。`--landing-dir` / `--keep-artifacts` 与股票入口相同。字段和交付依据见[官方交易日历探查](docs/research/tushare-official-trade-calendar-2026-10-16.md)。

## suspend_daily Ingestion

供应商接口名是 `suspend_d`，内部 Dataset / 表名为 `suspend_daily`。先在**同一数据库**发布整个目标区间的 SSE 日历，再获取停复牌：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20200309 --end-date 20200315 --db data/warehouse.duckdb
uv run --frozen python -m quant_data ingest suspend_daily \
  --start-date 20200309 --end-date 20200315 --db data/warehouse.duckdb --keep-artifacts
```

日历开市日各发一次全市场请求，不过滤股票或 S/R 类型；休市日排除且不生成停复牌 Coverage。缺任一自然日的成功日历时在获取前阻断，不自动运行上游；Scope 与依赖版本在同一快照读取，提交前再次复核。

按交易日原子替换，完整验收的零行日也发布版本并清除旧日集合。缺字段、坏值、重复业务键、请求失败或截断只阻断受影响日期，保护旧成功数据；触及 5,000 行或 has_more 风险时不自动分页。`suspend_timing` 保留完整字符串与 NULL，不拆分时段、不推导全天停牌或可交易状态，也不依赖当前 stock_basic。完整契约和证据见[停复牌落地验证](docs/research/suspend-daily-ingestion-validation-2026-10-16.md)。

## stock_st Ingestion

保存供应商逐交易日报告的全市场 ST 名单，主键 `(ts_code, trade_date)`，按交易日完整替换。先在同一库发布整个目标区间的 SSE 日历：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261016 --end-date 20261019 --db data/warehouse.duckdb
uv run --frozen python -m quant_data ingest stock_st \
  --start-date 20261016 --end-date 20261019 --db data/warehouse.duckdb --keep-artifacts
```

每个开市日一次全市场请求；休市日排除，缺日历依赖在请求前阻断。首版仅接受 type=ST，保留历史名称和标签，name/type_name 可空并告警；不依赖当前 stock_basic，不从名称区分 ST/*ST 或生成风险变更事件。

**开市日零行阻断，不清除旧名单，不发布空版本。** 请求确已完成时仍记 COMPLETE，业务非空规则独立验收。缺列、坏值、错日、重复键、请求失败、has_more 风险或触及 1,000 行只阻断相应日期；合格日期仍可发布，失败保护旧成功数据。拒绝未来日期，早于 2000-01-01 的开市日不请求、不发布；成功不保证历史绝无遗漏或当日已经终态。首版不自动分页。

独立契约与真实验收记录见 [stock_st 文档](docs/design/stock_st_ingestion_proposal.md)，通用设计准备见[新 Dataset 接入清单](docs/design/dataset_onboarding_checklist.md)。

## index_basic Ingestion

保存两组目标：24 个常用宽基白名单，以及 category=行业指数、publisher=上海申银万国证券研究所有限公司对应的申万行业基础记录。暂不纳入国证系列；保留 13 字段及 ts_code 主键，无日历或股票表依赖，不接受任意子集参数。

```bash
uv run --frozen python -m quant_data ingest index_basic \
  --db data/probes/my-index-validation/warehouse.duckdb \
  --landing-dir data/probes/my-index-validation/landing --keep-artifacts
```

每个宽基按代码单独请求，申万按上述类别与发布方一次过滤请求；只调用 index_basic，不获取全量、不查询 index_classify。每次显式 13 字段，has_more=false 且低于 8,000 行才确认交付完成，触顶阻断，不自动分页。Raw 仅多一个请求代码标签，用于核对返回范围。

宽基缺记录、申万过滤结果零行或源交付／准备／验收失败均阻断并保护旧版。合格后单 SNAPSHOT 整表替换，重跑生成新版本、不累加，并删除非目标旧记录。申万保留历史／已终止记录，不再按 SW2021 或一二三级重建目标和检查各层完整性。

market/category/index_type 为开放字符串；日期和基点允许 NULL，0 基点合法，负基点只告警，不要求基期早于发布日期。成功表示符合供应商交付及项目验收契约，不保证跨页同一时点或历史 PIT。完整规则与真实验收见 [index_basic 文档](docs/design/index_basic_ingestion_proposal.md)。

## index_member_all 申万行业归属

完整获取最新 Y 与历史 N 两组 11 字段，以单个 SNAPSHOT 验收并整表原子替换；无需日历、股票或指数基础表，不接受日期、证券或行业过滤。

```bash
uv run --frozen python -m quant_data ingest index_member_all \
  --db data/probes/my-index-member/warehouse.duckdb --keep-artifacts
```

两状态分别按 1,000 行分页，短页且明确 has_more=false 才完成，满页 false 继续取尾页。任一状态请求/分页/准备失败、重复业务键、状态日期矛盾、闭区间重叠或任一状态整组零行均阻断整快照，保护旧事实及成功版本。主键 `(l3_code, ts_code, in_date)`；保留源名称和历史断档，缺名称及早于 1990 年的纳入日期仅告警。

`out_date` **包含当天**，归属区间为 `[in_date,out_date]`，NULL 表示没有报告结束日。每次完整刷新同步分类迁移、历史修订和删除；建议每个交易日盘后运行同一入口，年度及临时调整后可再次刷新。本切片没有自动调度、每日归属物化或 PIT 历史版本。完整规则及端点依据见 [index_member_all 方案](docs/design/index_member_all_ingestion_proposal.md)。

## 股票与指数日线

两者均采用官方 Tushare，先在同一库发布所需自然日区间的 SSE 日历。股票逐交易日获取全市场未复权11列，不按当前股票名单删历史证券；开市日整日零行阻断，停牌个股不补行。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261016 --end-date 20261019 --db data/probes/my-daily/warehouse.duckdb
uv run --frozen python -m quant_data ingest stock_daily \
  --start-date 20261016 --end-date 20261019 --db data/probes/my-daily/warehouse.duckdb \
  --landing-dir data/probes/my-daily/landing --keep-artifacts
```

指数还依赖同库已发布的 index_basic / SNAPSHOT；不传代码时获取当前完整 universe，也可重复 `--ts-code` 选取其中代码。宽基按代码自然年请求 index_daily，申万每天一次 sw_daily(trade_date)。默认单日为1次申万＋24次宽基；申万 Raw 可含范围外代码，保留 Raw 和排除行数审计，仅发布固定 scope 内行情。按代码日完整替换；完整零行可清除该代码日旧行情并生成成功版本，含义仅为本次供应商没有报告。不会按 list_date 或 exp_date 裁历史。

```bash
uv run --frozen python -m quant_data ingest index_basic --db data/probes/my-daily/warehouse.duckdb
uv run --frozen python -m quant_data ingest index_daily \
  --start-date 20261016 --end-date 20261019 --ts-code 000300.SH --ts-code 801010.SI \
  --db data/probes/my-daily/warehouse.duckdb --landing-dir data/probes/my-daily/landing --keep-artifacts
```

两类 Canonical 量额均为手／千元，逻辑键均为 `(ts_code,trade_date)`，由 Dataset 验收而不物化 ART；申万源万股／万元在准备时分别乘100／10，pct_change 映射到 pct_chg，缺源能力的 pre_close 保留NULL。指数允许合法数值NULL，四价齐全才检查区间；股票九个数值必填。请求／转换／验收失败保护旧数据及版本，上游版本变化时提交前阻断。复权因子通过独立 adj_factor 保存，盘后量额、PIT 和自动调度尚未实现。

契约和真实独立库验收见[股票日线](docs/design/stock_daily_ingestion_proposal.md)、[指数日线](docs/design/index_daily_ingestion_proposal.md)。

## index_weight 银河每日指数权重

保存银河 `QueryThirdInfo / A010200005` 报告的九列日权重，主键 `(index_code, con_code, trade_date)`，以指数日为 Unit 完整替换。首版支持上证50、沪深300、中证500、中证800和中证1000；不传 `--index-code` 获取全部五个，也可重复参数选择子集。先在同库发布目标日期的 SSE 日历和 index_basic / SNAPSHOT：

```bash
uv run --frozen python -m quant_data ingest index_weight \
  --start-date 20260907 --end-date 20260907 \
  --index-code 000300.SH --index-code 000905.SH \
  --db data/probes/my-index-weight/warehouse.duckdb \
  --landing-dir data/probes/my-index-weight/landing --keep-artifacts
```

每个指数日一次单指数单日请求，内部串行且不使用多日范围响应。股本为股、比例及权重为百分数，保留源值，不归一化或补造成分；`free_share_ratio` 可为 NULL。空响应、`-76`、重复键、错日、非法数值及权重和超出 `[99.9,100.1]` 阻断所属指数日并保护旧版。未实现自动调度、PIT 或全历史覆盖承诺。契约和五指数真实验收见 [index_weight 文档](docs/design/index_weight_ingestion_proposal.md)。

## daily_basic 每日指标

保存 Tushare 默认 18 字段，逻辑键 `(ts_code, trade_date)` 由 Dataset 验收、不物化 ART，全市场逐日分页并按日完整替换。仅依赖同库已发布的 SSE 日历，不用当前股票名单或日线集合裁剪历史。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261028 --end-date 20261029 --db data/probes/my-daily-basic/warehouse.duckdb
uv run --frozen python -m quant_data ingest daily_basic \
  --start-date 20261028 --end-date 20261029 --db data/probes/my-daily-basic/warehouse.duckdb \
  --landing-dir data/probes/my-daily-basic/landing --keep-artifacts
```

每页 3,000 行，满页 false 仍续取尾页，短页且明确 false 才完成。后续页失败、缺列、坏值、错日、重复键或整日零行阻断所属日，保护旧版，其他合格日仍可发布。

close 必填且为有限正数，其余十五个指标允许合法 NULL。有符号估值、NULL 和零原样保存；股本为万股、市值为万元，比率为百分数，不换算或重算。非负指标出现负值会阻断；股本/市值关系异常按日告警，不改值。limit_status 因已发现历史语义冲突暂缓接入，不提供财务 PIT 或自动调度。完整契约与验收见 [daily_basic 文档](docs/design/daily_basic_ingestion_proposal.md)。

## adj_factor 原始复权因子

保存 Tushare 原始三字段，逻辑键 `(ts_code, trade_date)` 由 Dataset 验收、不物化 ART，全市场逐日分页并按日完整替换。仅依赖同库已发布的 SSE 日历；停牌日因子按源交付保留，不根据当前股票名单或日线集合过滤。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261028 --end-date 20261029 --db data/probes/my-adj-factor/warehouse.duckdb
uv run --frozen python -m quant_data ingest adj_factor \
  --start-date 20261028 --end-date 20261029 --db data/probes/my-adj-factor/warehouse.duckdb \
  --landing-dir data/probes/my-adj-factor/landing --keep-artifacts
```

每页 3,000 行，满页 false 仍续取尾页，短页且明确 false 才完成。坏字段、错日、重复键、非有限/非正因子或整日零行阻断对应日，保护旧数据及版本。

因子无量纲，原值不归一化、不填 1 或前向继承；允许正值小于 1、大因子和相邻日回落。重抓只修订显式目标日，不保证全历史来自同一供应商修订版本。前复权基准变化不等于历史原始因子必然改写；前后复权行情、PIT、自动重抓及全历史下载另行设计。完整契约和验收见 [adj_factor 文档](docs/design/adj_factor_ingestion_proposal.md)。

## stk_limit 每日涨跌停价格

保存 Tushare 报告的全市场七字段，包括 B 股、基金及停牌证券；逻辑键 `(ts_code, trade_date)` 由 Dataset 验收、不物化 ART，按交易日完整替换。只依赖同库已发布的 SSE 日历，不按当前股票名单过滤历史。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261023 --end-date 20261026 --db data/probes/my-limits/warehouse.duckdb
uv run --frozen python -m quant_data ingest stk_limit \
  --start-date 20261023 --end-date 20261026 --db data/probes/my-limits/warehouse.duckdb \
  --landing-dir data/probes/my-limits/landing --keep-artifacts
```

逐开市日按 3,000 行专属分页；满页即使 has_more=false 也续取尾页，短页且明确 false 才完成。分页失败、重复页、缺列、坏值、错日、重复业务键或整日零行均阻断对应日，保护旧数据及版本，其他合格日仍可发布。

pre_close 的 NULL/0 原样保存并告警；asset_type/exchange 为可空开放字符串。两个上下限须为有限数，上限为正、下限非负且顺序正确；零下限原样保留并按日告警，`100000 / 0.01` 等源值同样保留，不推导无限制标志或交易状态。价格保留各证券源报价口径，不统一换算币种，不自动下载全历史。完整契约及独立库验收见 [stk_limit 文档](docs/design/stk_limit_ingestion_proposal.md)。

## 北交所代码映射与股票一分钟

`bse_mapping` 保存官方北交所新旧代码完整快照，用于把来源旧代码统一为当前新代码；分钟 Scope/Canonical 全历史使用新代码，银河改代码前请求可在 Adapter 内路由旧代码。单次全量请求，四字段，`o_code` 主键、`n_code` 唯一，整快照替换，失败保护旧版：

```bash
uv run --frozen python -m quant_data ingest bse_mapping \
  --db data/probes/my-minute/warehouse.duckdb --keep-artifacts
```

`stock_1min` 落地单一物化完整研究网格，而不是稀疏源行副本。`status` 区分 `TRADED / NO_TRADE / SUSPENDED`；Canonical 保存十列事实，`trade_date` 为 DATE、`bar_time` 为 TIME，逻辑键由 Dataset 验收，不创建复合 ART。`reference_price` 由 `serving.stock_1min_with_reference` 按需派生：从当前已发布 `stock_daily.pre_close` 初始化并在当日内前向继承，不固定历史版本。量额为股／元，价格为元。银河源使用分钟 K 线和开盘竞价快照，全天停牌保存完整网格但不请求银河；09:30 OHLC 保留银河混合口径，量额扣除开盘竞价。

先在同库发布上游依赖，再在 `.env` 配置 `AD_USERNAME/AD_PASSWORD` 后运行：

```bash
uv run --frozen python -m quant_data ingest stock_1min \
  --start-date 20260903 --end-date 20260903 --parallel 2 \
  --db data/probes/my-minute/warehouse.duckdb \
  --landing-dir data/probes/my-minute/landing --keep-artifacts
```

默认并行 2，可选 4；worker 进程隔离、请求超时、熔断及当前 Run 证据恢复沿用正式执行层。网盘历史文件可使用同一 Dataset 的本地源，一日一文件一 Run，不需要供应商凭据：

```bash
uv run --frozen python -m quant_data ingest stock_1min \
  --start-date 20260417 --end-date 20260417 \
  --source-parquet /mnt/d/BaiduNetdiskDownload/stock_1min/20260417.parquet \
  --db data/probes/my-local-minute/warehouse.duckdb \
  --landing-dir data/probes/my-local-minute/landing --keep-artifacts
```

本地 Run 复核实际文件 SHA-256、大小和行数，将来源及映射版本写入 `meta.run.params`，`request_attempt` 为空；源行去向与正量记录等价检查进入既有统一验收，不新增 Meta 表。特殊交易时段和分钟复权仍不在范围内。完整契约见[银河分钟提案](docs/design/stock_1min_ingestion_proposal.md)、[本地 Parquet 转换契约](docs/design/stock_1min_local_parquet_contract.md)与[映射提案](docs/design/bse_mapping_ingestion_proposal.md)。连续夜间回填使用 `scripts/run_local_minute_overnight.py`：只通过正式 CLI 发布，提供只读计划、Meta 短摘要、问题台账、熔断恢复和 D 盘备份轮换；执行前阅读[网盘分钟回填执行参考](docs/operations/local_parquet_yearly_backfill_agent_runbook.md)。

## Meta 审计维护

独立 ingestion CLI 在登记新 Run 前自动收尾遗留 RUNNING：仅将 PENDING Unit 标为执行中断，保留已提交发布及原有错误。已有成功 Unit 的中断 Run 为 PARTIAL，否则 FAILED；`reconciled_at` 表示收尾时间，不是准确的进程死亡时间。程序内直接调用 Runner 不触发收尾。

也可独立收尾，或预览／应用历史明细留存；这些命令不需要供应商凭据：

```bash
uv run --frozen python -m quant_data meta reconcile --db data/warehouse.duckdb
uv run --frozen python -m quant_data meta retain --db data/warehouse.duckdb
uv run --frozen python -m quant_data meta retain --db data/warehouse.duckdb --apply
```

`run_unit`、`request_attempt`、`validation_check` 结束满 60 天后按 Run 整体归档为 Parquet，满 365 天后允许过期；其他四张 Meta 表长期保留。默认归档目录为数据库同目录的 `<库名>.audit/`，可用 `--archive-dir` 指定。运行中的记录不清理，ingestion 不自动执行历史归档或删除，Landing 留存政策保持原样。

本轮 Meta 增加 `reconciled_at`、`audit_state`、`audit_archive_path`；已有缺少这些列的 V3.2 验证库会明确报不兼容，不自动迁移或清空，请使用新库路径。详细规则与验证边界见 [Meta 审计 Spec](docs/design/meta_audit_lifecycle_spec.md)。

## dbt Serving：日线申万行业特征

已提供 `serving.stock_daily_with_sw_industry` View，为每条 stock_daily 追加 `sw_l1_code / sw_l2_code / sw_l3_code`。按完整归属历史及包含 out_date 当天的闭区间匹配，未匹配保留日线并输出 NULL，不改 Canonical、不填充行业断档。

在仓库根目录，显式选择已发布两个来源的数据库：

```bash
QUANT_DATA_DB="$PWD/data/warehouse.duckdb" \
  uv run --frozen --no-sync dbt build --project-dir dbt --profiles-dir dbt
```

当前行业 SNAPSHOT 和 2026-10-30 的 stock_daily 已成功发布，C 阶段已完成生产 View 构建与验收，5,571 条日线全部匹配行业。构建前仍检查行业 SNAPSHOT 和日线日期的成功 Coverage；缺发布时门禁明确阻断，不将空业务表视为已接入。结果键/行粒度测试为 ERROR，未匹配行测试为 WARNING。View 随来源修订更新，dbt 测试需要显式运行，测试失败不会自动回滚已创建的 View。

环境复用锁定的 dbt-core 1.12.5、dbt-duckdb 1.11.0、DuckDB 1.5.6，没有新增软件包、dbt 包或扩展。2026-11-02 的真实演示验证是历史记录；该演示库、旧备份及探查产物已于 2026-11-03 清理，同一路径现为新建生产空库，不做旧 Meta 自动迁移。设置及历史验证说明见 [dbt README](dbt/README.md) 和 [Serving 方案](docs/design/stock_daily_sw_industry_serving.md)。

## 开发环境与接口探查

使用 uv 管理 Python 3.13、`.venv` 和锁定依赖（Polars、DuckDB、Arrow/Parquet、dbt-duckdb、python-dotenv）。在仓库根目录执行：

```bash
uv sync --frozen
uv run --frozen python scripts/check_environment.py
uv run --frozen python -m unittest discover -s tests -v
```

环境检查在临时目录验证 DuckDB DDL、Arrow/Parquet 和 dbt View；结束后自动清理。

在本地 `.env` 设置 `TUSHARE_TOKEN`（格式见 [.env.example](.env.example)），然后执行：

```bash
# 两个快速探针；完整矩阵去掉 --case 参数
uv run --frozen python scripts/probe_tushare.py --case stock_L --case suspend_20200312
```

探针优先读取项目 `.env` 的 `TUSHARE_TOKEN`，没有时才读取环境变量；只访问官方 `https://api.tushare.pro`。原始响应和脱敏请求信息保存在 `data/probes/<运行时间>/`，不入 Git。供应商/网络/协议错误会保存证据并返回非零退出码。探针中的日期与分页尾部 offset 是本轮固定测试用例，后续复跑的最新股票快照与尾页行数可能变化。

- [官方接口探查报告与 Canonical 契约建议](docs/research/tushare-official-stock-suspend-2026-10-13.md)
- [日频与指数接口探查报告](docs/research/tushare-official-daily-index-2026-10-14.md)
- [交易日历接口探查与正式契约](docs/research/tushare-official-trade-calendar-2026-10-16.md)
- [停复牌正式 Schema](src/quant_data/datasets/suspend_daily.sql) 与[落地验证](docs/research/suspend-daily-ingestion-validation-2026-10-16.md)
- [银河 vs Tushare 日线质量对照与接入建议](docs/research/galaxy-vs-tushare-daily-quality-2026-10-22.md)（银河接口系列探查见同目录 galaxy-* 报告）

十四个 Dataset 的正式 DDL 位于 [datasets](src/quant_data/datasets)。逐请求摘要由探查报告引用。旧 review、walkthrough 和探查产物已分批清理，2026-11-03 已清空原 `data/probes/` 及 Landing；各文档中的旧证据路径仅作历史来源说明。需要重新探查时，上述入口会生成新产物。

## 数据商接口

2026-10-13 起改用 tushare 官方 API（`.env` 中的 `TUSHARE_TOKEN`）。此前经 ProMax 中转站的全部接口研究（网关手册、分钟 API 实测、stock_basic/suspend_d 探查、原始响应证据与探针脚本）已归档至 Git 分支 `archive/promax-relay-2026-10-13`；那些结论描述的是中转站网关行为，不作为官方 API 的有效依据，官方接口的权限、信封、限流与字段语义需重新探查。

- [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)：网盘历史分钟文件的来源与 Schema，与中转站无关。

## 本地生产数据与旧分钟清理

- `data/warehouse.duckdb`：本地生产权威库，保存已发布事实、Meta 和行业 Serving View，不入 Git。
- `/mnt/d/quant_data_backups/`：所有新生产全库备份及临时文件的唯一目录，不入 Git；禁止在 WSL 内新建备份。早期 WSL 备份及恢复核验仅为历史记录，详见[备份与恢复演练](docs/operations/warehouse_backup_restore_20261103.md)。
- `.env`：保留本地数据商凭据，不提交。

2013-01-04 的历史试点 A 阶段已完成：七项日频共 31 次请求，新增 799 个成功 Unit；当前成功 Coverage 共 12,292 个，原有范围数据和版本保持不变。分钟预检为 2,472 个 Unit、2,422 个请求 Unit，但其中十九只缺少当日日线及停复牌证据，按现有规则无法成功发布，需先核查或由用户明确接受受控 PARTIAL Pilot。B 脚本及资源采样已准备但未执行。2026-11-04 已完成只读核查，十九只目标日缺失与历史暂停上市／终止上市过渡的持续非交易状态相吻合；现有状态依据不足，处理语义待确认，未修改生产库或规则。详见[核查报告](docs/research/p1-listing-status-gap-investigation-2026-11-04.md)、[P1 交接说明](docs/operations/galaxy_pilot_20130104.md)和[回补方案](docs/operations/historical_backfill_plan_20261103.md)。

2026-11-03 已经用户确认永久删除以下旧输入，共 3,957 个文件、约 52.24 GiB：

- `data/raw/`：3,954 个网盘历史分钟 Parquet（2010-01-04～2026-04-17）及 `extraction_manifest.json`。
- 根目录 `20260903-1min.parquet` 和 `20260903-callauction.parquet`。

删除前后生产库 SHA-256 保持一致，48 个固定测试夹具哈希全部通过。银河与网盘本地 Parquet 两条分钟来源链路均已实现；2026-02-02～04-17 受控生产回填已验收，最近 22 日全部 SUCCESS、零失败 Unit。生产分钟共 65,284,067 行，此前批次保留 13 个质量缺口待统一处理，全历史仍未回填。本地输入使用当前 D 盘扁平日文件，不恢复已删除的 `data/raw/`；具体覆盖仍遵循各市场契约。

[原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)等旧研究报告作为历史记录保留。部分 `scripts/` 研究工具仍引用上述默认路径，其旧参考输入现已不存在，不能直接复跑原对照；正式 ingestion 和固定测试夹具不依赖这些旧文件。Git 克隆不包含本地生产数据或凭据。

## 旧项目归档

旧代码、架构、测试、性能实验、依赖配置以及清理前未提交的设计修改和编辑器配置，保存在 Git 分支 [`archive/pre-restart-2026-10-12`](https://github.com/guardgrenadier/quant_database/tree/archive/pre-restart-2026-10-12)，归档提交为 `8f6ebbd`。如需查看，请在项目目录外检出该分支，避免旧实现重新进入开发工作区。

ProMax 中转站研究资料（研究报告、探针脚本、原始响应证据）归档在分支 `archive/promax-relay-2026-10-13`，归档提交为 `f9fd057`。

旧运行环境、数据库、加工数据和其他本地资产曾归档于项目外的 `../quant_data-legacy-local-2026-10-12/`，该目录及其中清单已于 2026-11-03 经确认永久删除。上述 Git 归档分支仍保留。

GitHub 仓库为 [guardgrenadier/quant_database](https://github.com/guardgrenadier/quant_database)。旧代码归档分支已推送；`main` 保存清理后的重新开发起点。历史数据、凭据和项目外本地备份不上传。
