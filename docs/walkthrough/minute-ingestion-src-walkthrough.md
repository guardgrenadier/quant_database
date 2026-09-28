# 股票分钟 Ingestion 源码 Walkthrough

状态：对应 V1 实现（2026-09-25 起随 V1 冻结保留）。阅读对象：准备 review、维护或排查分钟链路的人。

这份文档回答三个问题：`src/` 中每个模块负责什么；一条分钟数据怎样穿过这些模块；review 时哪些不变量比代码风格更重要。权威设计为 [../architecture/ingestion-design-v2.md](../architecture/ingestion-design-v2.md)，本文只解释 V1 实现。

## 1. 先建立一张心智图

```text
scripts/ingest_minute.py                 composition root
          │
          ├── TushareClient              一次可靠 HTTP 调用
          │       ├── QueryResult        rows / empty / pending
          │       └── RequestRecord      每次真实 HTTP 尝试的脱敏证据
          │
          ├── TushareUniverse            日历、L 列表、停牌交叉信息
          │
          └── TushareMinuteSource        一个证券 × 一个交易日的完整切片
                  └── SliceResult        ok / empty / pending / failed
                           │
                           ▼
                    MinutePipeline
                           │
              ┌────────────┼──────────────┐
              ▼            ▼              ▼
          RawStore       AuditLog       dbt staging/tests
              │            │              │
              │            │              ▼
              │            │        candidate parquet
              │            │              │
              │            │              ▼
              │            └────── publish_candidate
              │                           │
              ▼                           ▼
 manifest + slice parquet          canonical parquet
 + requests.jsonl                         │
                                         ▼
                                  DuckDB minute_bars view
```

两条缝最重要：

- `QueryResult` 是 client 给源代码使用的传输结果；它不知道“一个完整分钟切片”是什么。
- `SliceResult` 是 adapter 给 pipeline 使用的切片结果；pipeline 不知道 offset、字段名和 API 时间格式。

## 2. `src/` 每个模块在做什么

### 2.1 包入口与配置

| 模块 | 当前职责 | Review 要点 |
| --- | --- | --- |
| [`quant_db/__init__.py`](../../src/quant_db/__init__.py) | 包标识和一句项目说明，无运行逻辑。 | 不应在这里初始化数据库、读取凭证或产生导入副作用。 |
| [`quant_db/config.py`](../../src/quant_db/config.py) | 从源码位置计算项目根目录；集中定义 DuckDB、raw、candidate、canonical 路径；保留原生 Tushare token。 | `RAW_STK_MINS_DIR` 必须与历史只读目录 `data/raw/stock_1min` 分开；candidate 必须在 canonical 通配路径之外；凭证只能来自环境变量。 |
| [`quant_db/tushare.py`](../../src/quant_db/tushare.py) | 旧导入路径的兼容转发层。研究脚本仍可 `from quant_db.tushare import TushareClient`。 | 这里不应重新实现 client；所有符号必须直接转发到新模块。 |
| [`quant_db/sources/__init__.py`](../../src/quant_db/sources/__init__.py) | 标识 vendor-specific source 包。 | 保持无副作用。 |
| [`quant_db/sources/tushare/__init__.py`](../../src/quant_db/sources/tushare/__init__.py) | 暴露常用 client 类型与异常。 | 只做稳定导出，不放抓取逻辑。 |
| [`quant_db/ingestion/__init__.py`](../../src/quant_db/ingestion/__init__.py) | 标识 vendor-neutral storage、audit、publish 和 dataset pipeline 包。 | 保持无副作用。 |

### 2.2 供应商侧模块

#### `sources/tushare/client.py`

[`client.py`](../../src/quant_db/sources/tushare/client.py) 只回答：“怎样可靠执行一次 API 调用？”

主要类型：

- `RequestRecord`：一次真实 HTTP 尝试的时间、脱敏参数、状态码、耗时、限流头、request id 和错误摘要。
- `QueryResult`：`rows`、`empty`、`pending` 三态；硬错误仍抛异常。
- `TushareError / AuthenticationError / TransportError`：业务协议、鉴权和传输错误分类，异常也携带已经发生的请求记录。
- `TushareClient`：TLS、API key、timeout、有限重试、硬请求预算、响应 envelope 校验和日志脱敏。

一次 `query()` 的关键顺序：

1. 校验 endpoint 名，删除值为 `None` 的参数并生成脱敏副本。
2. 每次实际发请求前检查全进程 `request_budget`。
3. 连接错误和 `429/502/503/504` 使用有限指数退避。
4. `202 + minute_data_pending` 在错误分类之前返回 `pending`。
5. 解析并校验 `code/data/fields/items`，保证字段唯一、每行长度与字段数一致。
6. 0 行返回 `empty`，非 0 行返回 `rows`。

它明确不做分页、落盘、交易日判断、OHLC 检查或 resume。

Review 时优先验证：

- API key 是否只进入 Header，且参数、异常和 `requests.jsonl` 都不会泄漏它。
- 每条重试路径是否有限；裸 503 是否允许非 JSON body。
- pending 是否永远不会被误判成 empty 或普通异常。
- `request_count` 是否按真实 HTTP attempt 计数，而不是按 `query()` 次数计数。
- response envelope 变形时是否 fail closed，而不是返回部分数据。

#### `sources/tushare/minute.py`

[`minute.py`](../../src/quant_db/sources/tushare/minute.py) 是 `stk_mins` adapter，只回答：“怎样完整取得一个证券的一天？”

它固定：

- 窗口 `09:30:00–15:00:00`；
- `freq=1min`；
- 固定九个 vendor 字段；
- `limit=500`、offset 每页加 500；
- 最多 20 页；
- 显式 raw Arrow schema。

`fetch_slice()` 逐页累积原样行，并检查必需字段、代码、时间格式、请求窗口、页内重复、页间重叠、重复页和页数上限。短页才结束：第一张空页得到 `empty`，有累计行时得到 `ok`。任何页 pending 会让整个切片成为 `pending`；任何硬错误或协议问题成为 `failed`。

Review 时不要在这里要求价格非负或 OHLC 成立——那是 dbt 的 B 类检查。应重点证明：

- 满 500 行后一定继续请求下一页。
- 已取到一部分行后若下一页 pending/failed，不会把部分结果标成 ok。
- 任一重复主键都会阻止切片完成，不会“取第一条”或“取最后一条”。
- `rows` 保持 vendor 字段和值，只有落 raw 时才增加 `extracted_at`。

#### `sources/tushare/universe.py`

[`universe.py`](../../src/quant_db/sources/tushare/universe.py) 是分钟 pipeline 的三个窄依赖：

- `is_trade_day()`：确认指定日开市。
- `latest_closed_trade_day()`：按上海时间，15:30 前不选择当天，再从 45 天日历窗口取最近开市日。
- `listed_stocks()`：分页抓取 `stock_basic(list_status='L')`，检查页内及页间代码重复。
- `suspensions()`：抓取当日 `suspend_d`，只供 manifest 交叉记录。

所有请求记录累积在 `self.requests`，由 pipeline 写入同一个 `requests.jsonl`。

Review 时确认 universe 只决定“问哪些领域问题”，不推断停牌分钟行数，也不把空分钟结果改写成停牌。

### 2.3 Vendor-neutral ingestion 模块

#### `ingestion/raw_store.py`

[`raw_store.py`](../../src/quant_db/ingestion/raw_store.py) 管可恢复的 raw artifact：

```text
data/raw/stk_mins/trade_date=YYYYMMDD/
├── manifest.json
├── requests.jsonl
├── 000001_SZ.parquet
└── 600000_SH.parquet
```

职责包括：

- 首次创建固定计划、universe 快照、停牌交叉信息和 planned 切片。
- `record()` 先追加实际请求摘要，再把 ok/empty 切片按显式 schema 写入临时文件并用 `os.replace()` 落盘。
- pending/failed 只更新 manifest，不伪造 Parquet。
- `terminal()` 同时检查状态、文件存在和物理行数；manifest 写着 ok 但文件损坏时会重新抓取。
- manifest 也通过临时文件替换，避免半写 JSON。
- 所有切片均为 ok/empty 后才能 `mark_fetched()`。
- 只清理按实际抓取时间满 30 天、且 callback 确认已发布的交易日缓存。

Review 的核心问题是“断电在任意一行发生会怎样”：Parquet 和 manifest 都应该要么保持旧版本，要么成为完整新版本；不能出现 manifest 宣称成功而文件不存在的可跳过状态。

#### `ingestion/audit.py`

[`audit.py`](../../src/quant_db/ingestion/audit.py) 在 DuckDB 中维护 `ingestion_audit`：

- `start()` 插入一次处理尝试，初始 stage 为 `started`。
- `advance()` 只推进 stage 并补 raw/canonical 行数。
- `finish()` 无论成功失败都填结束时间，失败通过 `error_message` 表达。
- `is_published()` 为 raw 清理提供发布确认。

Audit 不保存每个 HTTP 请求，也不参与 Parquet 事务。Review 时确认失败只保留“最后成功阶段”：例如 dbt test 失败应停在 `fetched`，view 失败应停在 `published`。

#### `ingestion/publish.py`

[`publish.py`](../../src/quant_db/ingestion/publish.py) 只处理一个交易日候选文件：

- `canonical_path()` 生成 `year=YYYY/month=YYYYMM/YYYYMMDD.parquet`。
- `validate_candidate()` 确认文件可读、行数与 manifest 一致、文件中只有目标交易日。
- `normalize_empty_candidate()` 处理 dbt-duckdb external materialization 为零行关系写入 null sentinel 的行为。
- `publish_candidate()` 先验证，再用同文件系统 `os.replace()` 替换正式日文件，最后复读验证。

Review 时确认 candidate 与 canonical 在同一文件系统、candidate 不被正式 glob 读取，并特别检查“文件已经替换但 audit 尚未登记”时重跑能否安全恢复。

#### `ingestion/minute.py`

[`minute.py`](../../src/quant_db/ingestion/minute.py) 是分钟数据集专用 pipeline，也是整个状态机所在处。

`RunSummary` 是 CLI 输出；`MinutePipeline.run()` 的顺序是：

1. 新 run 创建计划，resume run 读取旧 manifest 并要求显式代码顺序完全一致。
2. 新 run 确认交易日、抓 L 列表、验证代码子集、记录停牌交叉信息。
3. 写 audit `started`。
4. 顺序遍历计划；已验证的终态切片跳过，其余调用 adapter 并立即落 raw 状态。
5. 只要残留 pending/failed，就结束本 run，不调用 dbt。
6. fetched gate 通过后，把 raw 行数登记为 `fetched`。
7. 用 dbt vars 生成 run-id 隔离的 candidate，并运行 `tag:minute_ingestion` 检查。
8. 发布日文件，audit 进入 `published`。
9. 重建默认 `minute_bars` view 并运行 view test。
10. 结束 audit，并保守清理过期 raw。

`_dbt()` 是薄 subprocess wrapper；`_transform_version()` 只在工作树干净时使用 commit，dirty 或无 commit 会明确标记。

Review 这个模块时，最重要的是四个 gate：

```text
计划成立
  → 所有切片 ok/empty
    → dbt B 类检查全部通过
      → candidate 行数/日期复核通过
        → publish
```

任何代码改动都不应让后面的阶段绕过前面的 gate。

## 3. 一次成功运行的 walkthrough

Composition root 是 [`scripts/ingest_minute.py`](../../scripts/ingest_minute.py)。它只负责解析参数、组装一个共享 client、选择默认日期，并把依赖注入 pipeline。

示例：

```bash
set -a; source .env; set +a
uv run python scripts/ingest_minute.py \
  --codes 000001.SZ 600000.SH \
  --request-budget 25
```

仓库中的一次实际 smoke 选择了 `2026-09-21`，流程如下：

1. `latest_closed_trade_day()` 请求一次 45 天交易日窗口。
2. pipeline 再确认目标日开市。
3. `stock_basic` 两页得到 5000 + 533 只 L 股票；显式子集验证通过。
4. `suspend_d` 返回交叉信息；两只样例均不在停牌记录中。
5. 建立 manifest，两切片从 `planned` 开始。
6. `000001.SZ` 单页短页结束，得到 `ok / 241 rows`。
7. `600000.SH` 单页短页结束，得到 `ok / 240 rows`。
8. fetched gate 得到 raw 总行数 481。
9. dbt 转换并检查 candidate：字段转换、amount 舍入、非空、类型、唯一键、日期、代码范围、OHLC、量额非负、输入输出 481 行对账。
10. candidate 替换为 canonical 日文件，audit 记录 `published / 481 / 481`。
11. 默认 view 过滤一个 `09:30` bar，得到 480 行；无 `11:31–12:59` 记录。

最终证据链：

```text
manifest: 241 + 240 = 481
raw parquet physical rows: 481
canonical physical rows:   481
view rows after filtering:  480
audit raw/canonical rows:   481 / 481
```

查看证据而不重新请求 API：

```bash
uv run python - <<'PY'
import json
from pathlib import Path
import duckdb
from quant_db.config import DUCKDB_PATH

raw = Path('data/raw/stk_mins/trade_date=20260921')
manifest = json.loads((raw / 'manifest.json').read_text())
print({code: (item['status'], item['row_count'])
       for code, item in manifest['slices'].items()})

with duckdb.connect(str(DUCKDB_PATH)) as con:
    print(con.execute('''
        select count(*) as rows,
               count(*) filter (where bar_time = time '09:30:00') as at_0930,
               count(*) filter (
                   where bar_time between time '11:31:00' and time '12:59:59'
               ) as midday
        from minute_bars
    ''').fetchone())
PY
```

预期输出的核心内容：

```text
{'000001.SZ': ('ok', 241), '600000.SH': ('ok', 240)}
(480, 0, 0)
```

## 4. pending 和 resume walkthrough

若第二个切片返回 `202 minute_data_pending`：

```text
000001.SZ → ok      → raw parquet 已落盘
600000.SH → pending → manifest 记录 retry_after，不写伪造数据
fetched gate → blocked
dbt / publish → 不运行
audit → stage=started，error_message 说明状态分布
CLI → exit 2
```

下一次运行：

```bash
uv run python scripts/ingest_minute.py \
  --trade-date 2026-09-21 \
  --codes 000001.SZ 600000.SH \
  --resume --request-budget 25
```

`RawStore.terminal('000001.SZ')` 会复核文件和行数后跳过它，只重访 pending 切片。本次实际 smoke 在全部成功后做了一次 resume 验证：HTTP 请求数为 0，仍成功发布 481 行。

## 5. 失败发生在哪一层，应该保留什么

| 失败位置 | Audit 最后阶段 | 保留内容 | 下一步 |
| --- | --- | --- | --- |
| 计划阶段：日历、L 列表或首次 manifest 创建失败 | 当前实现尚未创建 audit 行 | 可能只有请求端异常 | 修参数或服务后重新开始；这是下节的 review 决策点。 |
| 切片 pending/failed | `started` | 已成功切片、manifest、requests.jsonl | 同计划 `--resume`。 |
| raw 写入失败 | `started` | 请求记录和 failed manifest；临时/旧文件供检查 | 修磁盘或 schema 后 resume。 |
| dbt run/test 失败 | `fetched` | 完整 raw 和可能存在的 candidate | 修转换或质量问题；不必重抓成功切片。 |
| candidate 发布前校验失败 | `fetched` | raw、candidate、旧 canonical | 修检查或转换后 resume。 |
| 文件替换后、audit 登记前进程退出 | 旧 audit 可能停在 `fetched` | 新 canonical 已存在 | resume 安全重做 candidate、替换并补登记。 |
| view 重建或 view test 失败 | `published` + error | canonical 已正式发布 | 从 canonical 修 view，不需要重新下载。 |

## 6. 推荐的 review 顺序

不要从文件名顺序开始，也不要先看格式。沿着一个真实请求阅读：

1. **入口和路径**：`scripts/ingest_minute.py` → `config.py`。确认依赖组装、默认日期和三个存储根没有混用。
2. **信任边界**：`client.py`。检查凭证、重试、pending、非 JSON 错误、envelope 和请求预算。
3. **抓取完整性**：source `minute.py`。手工走一遍短页、满页、重复页、pending 第 N 页。
4. **计划完整性**：`universe.py`。确认日历和 L 列表分页结果真正进入 manifest 快照。
5. **恢复证据**：`raw_store.py`。逐个想象在 Parquet 写前、写后、manifest 替换前中断。
6. **发布 gate**：pipeline `minute.py`。寻找任何能够在 pending/failed 或 dbt failure 后继续 publish 的路径。
7. **替换语义**：`publish.py`。确认 candidate 先完整验证、正式路径按交易日唯一、同日只替换不追加。
8. **审计语义**：`audit.py`。核对每种异常的最后成功 stage，而不是只看成功路径。
9. **最后看 dbt**：确认 Python 没有复制 B 类语义检查，dbt 也没有静默删除 raw 行。

### Review checklist

- [ ] 任一计划切片都只能以 ok/empty 通过 fetched gate。
- [ ] pending 不同步等待，不转 empty，不发布。
- [ ] hard error 有请求证据，但没有 API key。
- [ ] 分页短页是唯一正常终止条件，满页必取下一页。
- [ ] raw 写入和 manifest 写入都是替换式，不追加出重复日文件。
- [ ] resume 只跳过“状态终态且 Parquet 存在且行数相符”的切片。
- [ ] raw 与 canonical 行数逐日严格相等。
- [ ] candidate 永远位于 canonical glob 之外。
- [ ] amount 舍入发生在 dbt 且结果是 BIGINT；负值和溢出阻止发布。
- [ ] 默认 view 只过滤时间，不修改 canonical。
- [ ] 文件发布后 audit 未登记的窗口可以靠重跑恢复。
- [ ] 30 天清理同时满足抓取时间、已发布 audit 和 canonical 文件存在。

## 7. Review 时运行哪些检查

先跑完全离线测试：

```bash
uv sync
uv run python -m unittest discover -s tests
```

这些测试覆盖 client 错误分类与重试、pending、响应头、硬预算、adapter 协议失败、raw resume、过期清理和空 candidate。

再验证 dbt 项目本身：

```bash
export QUANT_DB_PATH="$(uv run python -c 'from quant_db.config import DUCKDB_PATH; print(DUCKDB_PATH)')"
uv run dbt debug --project-dir dbt --profiles-dir dbt
uv run dbt run --project-dir dbt --profiles-dir dbt
uv run dbt test --project-dir dbt --profiles-dir dbt
```

只有确实需要端到端验证时才做有界 live smoke：

```bash
set -a; source .env; set +a
uv run python scripts/ingest_minute.py \
  --codes 000001.SZ 600000.SH \
  --request-budget 25
```

review live 结果时，不要只看退出码。至少核对 manifest 行数、raw 物理行数、canonical 行数、view 时间过滤和 audit 行。

## 8. 当前值得 reviewer 明确决定的点

1. **计划阶段审计**：当前 audit 在 manifest 创建完成后才 `start()`。因此日历、证券列表、停牌查询或 manifest 创建失败不会产生 audit 行。优点是 audit 行总有合法 `raw_path`；代价是最早期失败只能从进程日志发现。如果希望“一启动就必有 audit”，需要先决定 raw path 尚未建立时如何表达，而不是只把 `start()` 上移。
2. **接口类型是鸭子类型**：`MinutePipeline` 对 source/universe 使用 `Any`，没有额外 Protocol 文件。这是刻意保持 V1 简单；出现第二个真实 source、且静态接口漂移成为问题时再加 Protocol。
3. **成功 dbt 输出不单独归档**：失败时 pipeline 会把 dbt 尾部输出放入 audit error，成功时只保留 dbt 自身日志和 audit stage。需要长期逐命令审计时再增加 run log artifact。

这三个点都不影响当前单机、单写入任务的正确性，但 review 时应把它们当作明确取舍，而不是无意遗漏。
