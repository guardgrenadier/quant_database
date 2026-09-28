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
