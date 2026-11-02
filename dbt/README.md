# dbt Serving

本项目使用仓库锁定的 dbt Core 和 dbt-duckdb，将已发布 Canonical 事实组合为 DuckDB `serving` Schema 下的普通 View。首个模型为 `serving.stock_daily_with_sw_industry`，保留股票日线所有字段，追加 `sw_l1_code / sw_l2_code / sw_l3_code`。

## 环境与运行

在仓库根目录运行。dbt 依赖已列在 `pyproject.toml` 和 `uv.lock` 中；全新机器按根目录 README 使用 `uv sync --frozen` 安装。本次搭建时现有虚拟环境已经完整，不需要安装，也不需要执行 `dbt deps`。

`QUANT_DATA_DB` 必须显式指向已有 DuckDB 文件，建议使用绝对路径。dbt 不自动加载项目 `.env`，不需要 Tushare 或银河凭据。以下命令中的数据库需要已通过 ingestion 发布两个业务来源：

```bash
export QUANT_DATA_DB="$PWD/data/warehouse.duckdb"
test -f "$QUANT_DATA_DB"
uv run --frozen --no-sync dbt debug --project-dir dbt --profiles-dir dbt
uv run --frozen --no-sync dbt build --project-dir dbt --profiles-dir dbt
```

先确认 `test -f` 成功再运行 dbt，防止路径笔误使 DuckDB 创建新的空文件。`debug` 仅检查配置和连接；`build` 才执行发布状态门禁、创建 View 并运行数据测试。`--no-sync` 使用已有环境，缺包时明确失败；首次安装后再运行。

环境搭建阶段检查的 `data/warehouse_v3_2.duckdb` 缺行业表且 Meta 为旧布局，保持原状。用户随后指定向 `data/warehouse.duckdb` 准备演示数据；该旧库同样不兼容，因此先在新库完成实时获取和验证，经用户确认完整备份旧库后，再切换到 `data/warehouse.duckdb`。当前该路径已有日线、行业归属和 Serving View，可直接执行上面的命令。

初次环境验证的独立库也保留，可单独运行：

```bash
QUANT_DATA_DB="$PWD/data/probes/dbt-serving-validation-20261102/warehouse.duckdb" \
  uv run --frozen --no-sync dbt build --project-dir dbt --profiles-dir dbt
```

`profiles.yml` 固定单线程、`serving` Schema、上海时区，关闭 DuckDB 扩展自动安装。项目关闭 dbt 匿名使用统计，没有远程数据库、扩展或外部 dbt 包配置。Ingestion、dbt、外部研究进程应错开访问同一个文件；单线程不提供跨进程锁协调。

## 模型契约

- 基准是实际已发布的 `stock_daily` 行，不生成交易日历网格，也不改 Canonical。
- 按 ts_code 和业务日期匹配完整 Y+N 归属：`in_date <= trade_date`，且结束日为空或 `trade_date <= out_date`。结束日包含当天。
- 三级代码来自同一条归属记录，不筛 is_new，不按当前股票表或最新名称填历史。
- 无归属及历史断档保留原日线，三个行业列均为 NULL，不前向填充。
- 结果键仍为 `(ts_code, trade_date)`；重复匹配必须失败，不能用任选一行或去重掩盖。

构建前的项目 Hook 要求 `index_member_all / SNAPSHOT` 已 PUBLISHED，且现有日线中的每个业务日期都有 PUBLISHED Coverage，stock_daily 至少已有一个成功日。缺表、缺成功版本或错误库路径明确报错，不自动执行 ingestion。

`stock_daily_industry_grain` 检查所有原日线键恰好对应一行结果，ERROR 阻断本次 dbt 成功结局；`stock_daily_industry_unmatched` 按未匹配日线行计数，WARNING 保留 NULL 并允许成功。不要使用三列全量 not-null 检查，也不要将未匹配全部解释为真实没有行业。

查看逐日覆盖及缺失样本：

```sql
SELECT trade_date, count(*) AS rows, count(sw_l3_code) AS matched,
       count(*) - count(sw_l3_code) AS unmatched
FROM serving.stock_daily_with_sw_industry
GROUP BY trade_date ORDER BY trade_date;

SELECT ts_code, trade_date
FROM serving.stock_daily_with_sw_industry
WHERE sw_l3_code IS NULL
ORDER BY trade_date, ts_code LIMIT 20;
```

## View 与测试边界

普通 View 不复制业务行，上游成功修订后，下一次查询使用修订值，无需增量重算。dbt 不自动监听 ingestion；需要重新验证数据时显式运行 `dbt build` 或 `dbt test`。

`build` 是先创建 View、再运行测试；测试失败不会自动回滚 View，也不等于通过 ingestion Publisher 发布。构建前门禁只检查本次运行，不能充当每次研究查询的实时验证。标准 ingestion 负责持续保护来源事实与 Coverage，禁止绕过它手工写入正式事实。

这里不新增 Serving 的 Publication/Coverage，不固定跨查询的来源版本，也不保留历史结果镜像。分类历史修订可能改变过去日期的特征值；本模型不是 PIT。dbt 日志和 manifest/run_results 在 `dbt/logs/`、`dbt/target/`，这些诊断文件不进入 Git，也不代替 ingestion Meta。

## 本次搭建的下载记录（2026-11-02）

**没有执行安装命令，没有新增软件下载或数据下载。** 全程复用现有 `.venv`，执行命令使用 `uv run --frozen --no-sync` 或现有 dbt 可执行文件。

| 类型 | 本次新增下载/安装 |
|---|---|
| Python 运行时与系统软件 | 无 |
| Python 包及其依赖 | 无；原有 77 个 distribution 的名称和版本前后逐项核对 |
| dbt 外部包，如 dbt-utils | 无；没有 packages.yml，也没有运行 dbt deps |
| DuckDB 扩展 | 无；未配置扩展，自动安装关闭 |
| Tushare/银河数据 | 无；独立新库通过正式 Runner 离线重放既有真实日历、日线和行业响应 |
| 官方资料 | 在线查阅 dbt 官方说明，没有将第三方软件或资料包下载进仓库 |

复用的主要版本：dbt-core **1.12.5**、dbt-duckdb **1.11.0**、DuckDB **1.5.6**、dbt-adapters **1.24.5**、dbt-common **1.39.0**。`pyproject.toml` 和 `uv.lock` 保持原样；本地环境前后清单与验收记录位于 `data/probes/dbt-serving-validation-20261102/`。

## 后续实时演示数据（2026-11-02，用户单独授权）

本阶段通过正式 ingestion CLI 向新结构库实时获取数据，共 12 次 Tushare HTTP 尝试：SSE 日历 1 次、stock_daily 2 次、index_member_all 9 次。日线为 2026-10-29 的 5,570 行及 10-30 的 5,571 行；申万 Y 5,922 行、N 2,006 行。dbt build 在最终 `data/warehouse.duckdb` 路径成功，11,141 条日线全部匹配，无重复或行情字段差异。

原库完整备份为 `data/warehouse.before-dbt-20261102T120647218730.duckdb`，其中原有 stock_basic 5,921 行和旧审计保留，备份 SHA-256 与切换前相同。这是用户确认后的整库备份及路径切换，没有迁移或重定义旧审计；新演示库没有复制 stock_basic。候选库和原始响应仍保留于 `data/probes/warehouse-dbt-preview-20261102T120136922070/`。数据库、备份、响应及 dbt 运行产物均不上传 Git。

本阶段新增的是市场数据获取，没有新增软件、dbt 包或 DuckDB 扩展。机器摘要见 [最终库验收](../docs/research/dbt-warehouse-live-validation-2026-11-02.json)。

详细设计与验证记录见 [Serving 方案](../docs/design/stock_daily_sw_industry_serving.md)。
