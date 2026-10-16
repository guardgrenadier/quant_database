# quant_data

个人量化数据库，当前已完成 V3.2 `stock_basic` 纵向切片：真实请求 → Polars 标准化与验收 → Canonical 和 Meta 原子发布。

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

产物目录的 README 提供阅读顺序；包含原始响应、Raw/Candidate Parquet、Schema、验收、发布和各阶段 Meta 快照。诊断不完整以 `artifacts_complete=false` 和 `export_errors` 表达，成功发布状态独立保留。

运行时保持串行访问：Ingestion、dbt 和外部研究进程应错开访问同一文件。当前只开放 stock_basic 入口；触及满页/has_more 时明确阻断，未实现自动分页或恢复执行。尚未接入 suspend_daily、交易日历或正式 Serving 模型。

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
- [停复牌 Schema 建议](docs/design/canonical_suspend_daily_proposal.sql)

stock_basic Schema 已作为首片契约采用，正式 DDL 见 [stock_basic.sql](src/quant_data/datasets/stock_basic.sql)；suspend_daily DDL 仍为建议稿。逐请求摘要由探查报告引用。历史 review、walkthrough 和探查原始产物已于 2026-10-16 清理，可按上述入口重新生成。

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
