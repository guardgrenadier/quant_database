# quant_data

个人量化数据库，已接入 `stock_basic` 完整快照、SSE `trade_calendar`、`suspend_daily` 停复牌、`stock_st` 名单、`index_basic` 目标指数快照，以及 `stock_daily` 未复权股票日线和 `index_daily` universe 内指数日线：获取及证据 → 集中准备 Candidate → 统一验收 → Canonical 和 Meta 原子发布。

| 阅读目的 | 文档 |
|---|---|
| 理解核心心智模型、职责与 invariant | [核心架构](docs/design/core_design.md) |
| 理解当前模型、接口、事务与能力边界 | [技术设计与实现边界](docs/design/ingestion_v3.2_implementation.md) |

这两份是当前维护文档：先读核心架构，再按需查技术细节。[V3.1.1 归档](docs/design/archive/core_design_v3.1.1.md)保留设计历史与 invariant 对照，不代表当前实现进度。

## stock_basic Ingestion

在仓库根目录设置 `.env` 的 `TUSHARE_TOKEN` 后执行：

```bash
uv sync --frozen
uv run --frozen python -m quant_data ingest stock_basic --db data/warehouse_v3_2.duckdb
```

默认路径仍为 `data/warehouse.duckdb`；已有 V3.1 Meta 的原型库需指定一个新库路径，新版明确报错且不自动迁移或删除旧数据。每次请求五种上市状态 × 三交易所，获取全部 17 字段，直接形成 Polars Raw/Candidate 数据表；验收后单事务替换数据、成功版本、Coverage 和 Unit 审计。重跑产生新版本，失败保留旧成功数据。

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

运行时保持串行访问：Ingestion、dbt 和外部研究进程应错开访问同一文件。各 Dataset 使用专属获取计划；尚无通用分页、恢复执行或正式 Serving 模型。

## trade_calendar Ingestion

首版仅保存 SSE 供应商日历，按当前确认口径供沪深北市场共同使用，不复制生成 SZSE/BSE 行。开市与休市均有显式记录，主键及发布 Unit 为 `(exchange, cal_date)`；后续依赖通过 `trade_calendar / SSE:YYYY-MM-DD` 定位成功版本。

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20100101 --end-date 20261231 \
  --db data/warehouse_v3_2.duckdb
```

日期为显式闭区间，包含休市日，不自动猜测起止日期；内部按自然年请求片段、按自然日验收并替换。缺日、空交付或错误不能生成默认休市行，也不能清除旧成功日历；其他合格日期仍可发布，PARTIAL 返回非零。`--landing-dir` / `--keep-artifacts` 与股票入口相同。字段和交付依据见[官方交易日历探查](docs/research/tushare-official-trade-calendar-2026-10-16.md)。

## suspend_daily Ingestion

供应商接口名是 `suspend_d`，内部 Dataset / 表名为 `suspend_daily`。先在**同一数据库**发布整个目标区间的 SSE 日历，再获取停复牌：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20200309 --end-date 20200315 --db data/warehouse_v3_2.duckdb
uv run --frozen python -m quant_data ingest suspend_daily \
  --start-date 20200309 --end-date 20200315 --db data/warehouse_v3_2.duckdb --keep-artifacts
```

日历开市日各发一次全市场请求，不过滤股票或 S/R 类型；休市日排除且不生成停复牌 Coverage。缺任一自然日的成功日历时在获取前阻断，不自动运行上游；Scope 与依赖版本在同一快照读取，提交前再次复核。

按交易日原子替换，完整验收的零行日也发布版本并清除旧日集合。缺字段、坏值、重复业务键、请求失败或截断只阻断受影响日期，保护旧成功数据；触及 5,000 行或 has_more 风险时不自动分页。`suspend_timing` 保留完整字符串与 NULL，不拆分时段、不推导全天停牌或可交易状态，也不依赖当前 stock_basic。完整契约和证据见[停复牌落地验证](docs/research/suspend-daily-ingestion-validation-2026-10-16.md)。

## stock_st Ingestion

保存供应商逐交易日报告的全市场 ST 名单，主键 `(ts_code, trade_date)`，按交易日完整替换。先在同一库发布整个目标区间的 SSE 日历：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20261016 --end-date 20261019 --db data/warehouse_v3_2.duckdb
uv run --frozen python -m quant_data ingest stock_st \
  --start-date 20261016 --end-date 20261019 --db data/warehouse_v3_2.duckdb --keep-artifacts
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

两类 Canonical 量额均为手／千元；申万源万股／万元在准备时分别乘100／10，pct_change 映射到 pct_chg，缺源能力的 pre_close 保留NULL。指数允许合法数值NULL，四价齐全才检查区间；股票九个数值必填。请求／转换／验收失败保护旧数据及版本，上游版本变化时提交前阻断。未实现复权因子、盘后量额、PIT 或自动调度。

契约和真实独立库验收见[股票日线](docs/design/stock_daily_ingestion_proposal.md)、[指数日线](docs/design/index_daily_ingestion_proposal.md)。

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

七个 Dataset 的正式 DDL 位于 [datasets](src/quant_data/datasets)。逐请求摘要由探查报告引用。早期 review、walkthrough 和探查原始产物已于 2026-10-16 清理，可按上述入口重新生成；当前正式切片的证据路径见各自文档。

## 数据商接口

2026-10-13 起改用 tushare 官方 API（`.env` 中的 `TUSHARE_TOKEN`）。此前经 ProMax 中转站的全部接口研究（网关手册、分钟 API 实测、stock_basic/suspend_d 探查、原始响应证据与探针脚本）已归档至 Git 分支 `archive/promax-relay-2026-10-13`；那些结论描述的是中转站网关行为，不作为官方 API 的有效依据，官方接口的权限、信封、限流与字段语义需重新探查。

- [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)：网盘历史分钟文件的来源与 Schema，与中转站无关。

## 已下载数据（本地，不入 Git）

- `data/raw/stock_1min/year=YYYY/YYYYMMDD.parquet`：3,954 个网盘历史文件，2010-01-04 至 2026-04-17，约 53 GiB；保留 `extraction_manifest.json`。详见 [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)。
- `20260903-1min.parquet`：原有单日历史文件。
- `.env`：保留本地数据商凭据，不提交。

原始数据保持原路径、原内容。Git 克隆不包含这些本地文件。

## 旧项目归档

旧代码、架构、测试、性能实验、依赖配置以及清理前未提交的设计修改和编辑器配置，保存在 Git 分支 [`archive/pre-restart-2026-10-12`](https://github.com/guardgrenadier/quant_database/tree/archive/pre-restart-2026-10-12)，归档提交为 `8f6ebbd`。如需查看，请在项目目录外检出该分支，避免旧实现重新进入开发工作区。

ProMax 中转站研究资料（研究报告、探针脚本、原始响应证据）归档在分支 `archive/promax-relay-2026-10-13`，归档提交为 `f9fd057`。

旧运行环境、数据库、加工数据和其他被移出的本地资产保存在项目外的 `../quant_data-legacy-local-2026-10-12/`；其中 `preserved-data-inventory.json` 记录清理前保留数据的路径、大小与修改时间，用于核对。归档不代表推荐实现。

GitHub 仓库为 [guardgrenadier/quant_database](https://github.com/guardgrenadier/quant_database)。旧代码归档分支已推送；`main` 保存清理后的重新开发起点。历史数据、凭据和项目外本地备份不上传。
