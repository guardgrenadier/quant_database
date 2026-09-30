# quant-db

单机个人量化研究数据库骨架。Tushare 兼容网关是首个数据源；raw 以 JSON 原文短期保存（30 天），canonical 明细为 Parquet，DuckDB 保存状态账，dbt 负责 canonical 建模。现行设计决策见 [docs/architecture/ingestion-design-v2.md](docs/architecture/ingestion-design-v2.md)（2026-09-25 起；旧架构文档冻结保留）。

V2 重构按 [落地顺序](docs/architecture/ingestion-implementation-plan.md) 分阶段 review；新代码在 `src/quant_ingest/`，阶段 1 的离线检查运行 `uv run python -m unittest discover -s tests_v2`。

分钟链路的逐模块源码导读和 review 清单见 [docs/walkthrough/minute-ingestion-src-walkthrough.md](docs/walkthrough/minute-ingestion-src-walkthrough.md)（对应冻结的 V1 实现）。

## 初始化

在项目根目录运行（需要 `uv`）：

```bash
uv sync
cp dbt/profiles.yml.example dbt/profiles.yml
export QUANT_DB_PATH="$(uv run python -c 'from quant_db.config import DUCKDB_PATH; print(DUCKDB_PATH)')"
```

`QUANT_DB_PATH` 从项目根目录计算，无需在仓库里写个人绝对路径。新终端运行 dbt 前重新执行 `export` 命令。

## dbt smoke test

在项目根目录运行：

```bash
uv run dbt debug --project-dir dbt --profiles-dir dbt
uv run dbt run --project-dir dbt --profiles-dir dbt
uv run dbt test --project-dir dbt --profiles-dir dbt
```

数据库写入 `data/warehouse/quant.duckdb`。实际 raw 数据、数据库文件、本地 dbt profile 和 `.env` 不进入 Git；目录里的 `.gitkeep` 只用于保留结构。

## Tushare 兼容网关

在项目根目录的 `.env` 填写 `TUSHARE_API_KEY`，运行 `set -a; source .env; set +a` 导出环境变量。这个 key 用于 ProMax 网关的 `X-API-Key`；`TUSHARE_TOKEN` 留给原生 Tushare API。项目不会自动加载 `.env`。

```python
from quant_db.sources.tushare.client import TushareClient

result = TushareClient().query("daily", ts_code="000001.SZ", start_date="20260701")
if result.status == "rows":
    rows = result.rows
elif result.status == "empty":
    rows = []
else:
    print("数据仍在补取，建议下次 run 再试", result.retry_after)
```

`query()` 返回 `QueryResult`，状态为 `rows / empty / pending`；硬错误仍抛异常。结果同时暴露 `Retry-After`、`X-RateLimit-Remaining`、`X-Request-ID`，`requests` 保存每次实际 HTTP 尝试的脱敏摘要。wrapper 不分页；临时 HTTP 失败会做有限重试，默认 503 预算覆盖分钟级故障窗口。

全市场或长区间请求应明确传 `fields`、`limit`，下一页传 `offset`；例如：

```python
result = TushareClient().query(
    "stock_basic", list_status="L",
    fields="ts_code,symbol,name,list_date", limit=5000, offset=0,
)
rows = result.rows
```

Wrapper 不自动分页，完整性由对应 ingestion 检查。网关手册使用 `TUSHARE_RELAY_KEY` 作为环境变量示例；本项目沿用 `TUSHARE_API_KEY`，两者最终都用于同一个 `X-API-Key` Header。默认验证 TLS 证书。运行 `uv run python -m unittest discover -s tests` 可离线测试 wrapper。

## V2 基础数据集

导出 `TUSHARE_API_KEY` 后运行 `uv run python scripts/ingest_basic_v2.py --date 2026-09-18`，默认依次处理 `stock_basic`、`trade_cal`、`suspend_d`；`--dataset` 可只跑其中一个。V2 的长期状态账在 `data/warehouse/v2/state.duckdb`，dbt 工作库及可重建视图在独立的 `data/warehouse/v2/dbt.duckdb`；raw 与 canonical 分别位于 `data/raw/v2/` 和 `data/warehouse/v2/canonical/`。`trade_cal` 拉目标年份的 SSE 日历快照；`stock_basic` 使用已实测的五状态单页参数；`suspend_d` 抓目标日的 `S` 停牌记录。离线验证运行 `uv run python -m unittest discover -s tests_v2`。

## V2 分钟 L1

同日基础数据集发布后运行 `uv run python scripts/ingest_minute_v2.py --date 2026-09-18 --request-budget 100`。新分钟计划按已发布日历和证券快照固定全天 `00:00:00–23:59:59` 请求；已有计划续跑仍使用账上原窗口。最多 8 个线程、进程内滚动 60 秒最多 180 次 HTTP 尝试。HTTP 202 的切片留给下一次同命令续抓；完整性摘要中的 `complete=true` 只表示 raw 抓取完成。当前增量链路使用目标日的同日快照；历史回补的快照选择在历史阶段处理。

## V2 分钟发布

raw 完整后运行 `uv run python scripts/publish_minute_v2.py --date 2026-09-18`。dbt 将原始元金额按十进制值转为整数分，结构错误阻断发布；行情语义异常记入 `quality_findings`，canonical 仍保留窗内全部 bar。默认 `minute_bars` view 建在 dbt 工作库，只显示约定的交易时段；账本库仅保存 `plans/slices/publications/quality_findings`。转换逻辑变更且 raw 尚在时可加 `--rebuild` 从 raw 整日替换并留下修订 diff；T+1 重新请求供应商使用下节刷新脚本。每条 dbt 命令的返回码及输出写入 run 级 JSONL。

## V2 刷新、清理与历史

- T+1 观察：`uv run python scripts/refresh_minute_v2.py --date 2026-09-18 --request-budget 6000`。pending 时重跑同命令，所有切片收敛后才替换正式日文件；`uv run python scripts/report_refresh_v2.py --days 14` 查看修订发生率。
- 30 天清理：`uv run python scripts/cleanup_raw_v2.py` 只预览，显式加 `--apply` 才删除已发布、全部切片过期且没有失败候选的 V2 raw 与过程日志；网盘原件和 quarantine 不在清理范围。
- 网盘历史：先看 [勘察报告](docs/research/historical-parquet-survey-2026-09-29.md)。发布当前 `stock_basic` 快照后，用 `uv run python scripts/survey_history_v2.py --day 2026-04-17 --codes 000001.SZ` 做有界 API 重叠抽样与 D6 覆盖报告。`scripts/import_history_v2.py --start 2026-04-17 --end 2026-04-17 --limit-days 1` 默认预览；加入 `--trust-report data/warehouse/v2/history_trust.json --apply` 才导入。批量 `--apply` 要求报告 `trust_level=sampled_match`；`low_or_pending` 仅允许 `--limit-days 1` 单日试点，发布后需人工抽查并重新生成报告。原始网盘文件只读且永久保留。
- gap：`scripts/backfill_gap_v2.py --start 2026-04-18 --end 2026-09-18` 默认预览；`--apply` 后每轮最多处理 7 个连续交易日，逐日使用已验证的单代码请求与发布链路。跨日 HTTP 批次尚未启用，因此不会引入跨日窗外行。

## 股票分钟 ingestion

> 本节描述冻结保留的 V1 实现的用法；新实现按 `docs/architecture/ingestion-design-v2.md` 另起。

指定交易日和代码子集运行完整链路：

```bash
set -a; source .env; set +a
uv run python scripts/ingest_minute.py \
  --trade-date 2026-09-18 \
  --codes 000001.SZ 600000.SH \
  --request-budget 25
```

省略 `--trade-date` 时使用上海时间最近一个已收盘交易日；省略 `--codes` 时使用当日 `stock_basic(list_status='L')` 快照。未完成切片会保留为 `pending/failed`，下一次以完全相同的日期和代码计划续跑：

```bash
uv run python scripts/ingest_minute.py \
  --trade-date 2026-09-18 \
  --codes 000001.SZ 600000.SH \
  --resume --request-budget 25
```

raw 写入 `data/raw/stk_mins/trade_date=YYYYMMDD/`；canonical 按月写入 `data/warehouse/canonical/stock_1min/`。DuckDB 的 `main.minute_bars` 是默认消费 view，过滤 `09:30` 和午间填充时段。
