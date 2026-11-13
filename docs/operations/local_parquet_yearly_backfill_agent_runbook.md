# 网盘分钟回填执行参考

日期：2026-11-12。状态：**后续网盘分钟回填的唯一执行权威**。业务语义以冻结架构和分钟契约为准，不因执行提效而放宽。

## 1. 适用范围与文档分层

适用于 `/mnt/d/BaiduNetdiskDownload/stock_1min/YYYYMMDD.parquet` 到 `canonical.stock_1min` 的历史回填。生产库固定为 `data/warehouse.duckdb`。

只有本文提供后续操作指令。其他资料的定位如下：

| 层级 | 文档 | 用途 |
|---|---|---|
| 核心架构 | `docs/design/core_design.md`、`docs/design/ingestion_v3.2_implementation.md` | Runner、Validator、Publisher、Meta 和事务 invariant |
| 当前分钟契约 | `docs/design/stock_1min_local_parquet_contract.md` | 网盘输入、时间、竞价、北交所、网格和守恒规则 |
| 银河分钟契约 | `docs/design/stock_1min_ingestion_proposal.md` | 银河获取及后续 Unit 修复 |
| 历史执行记录 | `docs/operations/local_parquet_2024_2013_backfill_log.md` | 2024 实际 Run、失败台账和当时审计结果 |
| 历史交接 | `docs/operations/local_parquet_2024_2013_session_handoff.md` | 上次 session 的停止状态与问题 |
| 修复记录 | `docs/operations/stock_1min_2025_failure_repair.md`、`stock_1min_2025_failure_baseline.json` | 2025 不可变失败基线及修复事实 |
| 银河验证记录 | `docs/operations/galaxy_pilot_20130104.md`、`galaxy_validation_20261030.md` | 既有银河分钟事实和边界验证 |
| 早期计划/演练 | `docs/operations/historical_backfill_plan_20261103.md`、`warehouse_backup_restore_20261103.md` | 历史计划和备份演练 |

研究证据主要包括：

- 网盘文件：`docs/research/historical-parquet-survey-2026-09-29.md`。
- 零量、北交所缺口与收盘标签：`docs/research/historical-minute-zero-volume-2026-10-22.md`、`historical-minute-bj-close-labels-2026-10-22.md`。
- 银河接口、性能和本地质量：`docs/research/galaxy-amazingdata-interface-probe-2026-10-19.md`、`galaxy-perf-probe-2026-10-19.md`、`galaxy-local-quality-20260903.md`、`galaxy-vs-tushare-daily-quality-2026-10-22.md`。
- 快照与分钟边界：`docs/research/galaxy-snapshot-probe-2026-10-21.md`、`snapshot-minute-boundaries-2026-10-23.md`、`snapshot-minute-reconstruction-2026-10-23.md`。
- 北交所与参考价：`docs/research/galaxy-bj-minute-probe-2026-10-25.md`、`galaxy-bj-pilot-probe-2026-10-25.md`、`galaxy-preclose-exdate-probe-2026-10-26.md`。
- 早期上市状态：`docs/research/p1-listing-status-gap-investigation-2026-11-04.md`。

`docs/design/stock_1min_snapshot_reserve.md` 是未采用方案；`docs/design/archive/` 和 V3.1.1 旧稿是历史架构资料。`daily_backfill_*` 文档属于日频任务。以上均不得覆盖本文。

## 2. 不可改变的业务边界

1. **一日一文件一 Run**。不得把多日合并为一个本地 Run，不改变 Unit、Canonical Schema 或 `NETDISK_END_MINUTE_V1`。
2. 使用现有 Dataset、Source Adapter、Runner、Validator 和 Publisher。不得直接 INSERT/DELETE Canonical，不新增任务或问题 Meta 表。
3. 一次只运行一个 ingestion 进程；dbt、研究进程和其他写库任务与回填串行。
4. 本地源不使用 `--parallel` 或 `--ts-code`，不需要 Galaxy/Tushare 凭据。
5. 依赖必须已有 PUBLISHED Coverage，并由正式流程固定和复核：`trade_calendar`、`suspend_daily`、`stock_daily`、`stock_basic`、`bse_mapping`。不得自动补取上游。
6. 生产回填禁止 `--keep-artifacts`，不得导出整张 Canonical 和全阶段 Meta。Landing 不承担历史备份职责。
7. 本地 Run 保留两次正式源身份检查：登记前 `inspect()` 记录路径、大小、SHA-256 和行数；`fetch()` 对实际解码字节再次核对。**SHA-256 不证明源数据完整或准确。**
8. Unit 级 ERROR 仍阻断所属 Unit，WARNING 不阻断。不得补造 NO_TRADE/SUSPENDED、改写源值或放宽 Validator。
9. 不因 PARTIAL 自动重跑；本地源没有股票子集入口，显式重跑会重发全日成功 Unit。全部历史回填期间只登记普通缺口，不自动使用银河修复。

## 3. 核心审计原则：Meta 优先，禁止重复证明

> **Meta 已经能够证明的事项，Agent 不再重复证明；Meta 不能证明的事项，只在明确风险触发时做独立检查。高成本历史 `EXCEPT ALL` 仅在存在较大损坏风险时使用。**

### 3.1 常规执行只做 Meta 汇总

- 正式 CLI、七张 Meta 表和库内检查结果是常规验收依据。
- 每日只生成一次固定摘要：日期、Run ID、状态、Scope/PUBLISHED/FAILED、Publication 行数、ERROR/WARNING 规则、依赖、耗时和资源。
- PARTIAL 返回码为 1；结合 Meta 判断，不把 PARTIAL 当成整日未发布。
- 月度、年度汇总从 Meta 和执行器日志生成。常规验收不重新扫描 Canonical，不重复核对正式流程已经记录的依赖、Publication、Coverage 和请求计数。
- **取消 Agent 的第三次源文件 SHA-256。** 正式两次核对已经证明登记身份与实际读取字节一致。
- stdout、stderr 和资源数据写入固定日志文件，stdout 验收后压缩。Agent 只读取短摘要、停止事件和最终报告。

### 3.2 高成本独立检查的触发条件

Meta 没有历史行镜像或通用 CDC，不能用 Meta 声称旧 Canonical 已逐行未变；但常规回填不再为此执行全历史对账。

只有以下较大损坏风险才触发独立事实对账：

1. 修改共享 Publisher、事务、Schema/存储，执行数据库迁移、恢复或其他可能广泛影响非目标事实的操作；
2. 发现非预期旧 Meta/Coverage 变化、全局执行错误、依赖漂移、外部或手工写库、串行访问被破坏等实际异常；
3. 高风险变更在独立库验证或首次生产 Pilot 时，需要建立新的受信基线。

对账范围必须覆盖风险即可：能限定到目标 Unit、日期或表的，不扩大到全历史。只有影响范围无法可信限定时，才使用全历史双向 `EXCEPT ALL`。不得把整表一次性比较作为默认做法，应使用受控分片和明确资源预算。

正常 PARTIAL、已登记 Unit 级质量问题、北交所集中缺源、常规月度/年度结束、源 SHA-256 一致或单纯重复确认 Meta，均不构成触发条件。

未触发独立历史对账时，年度汇总必须写“未做独立历史事实对账”，不得声称已经逐行证明旧事实无差异。轮换并删除旧恢复点后，不能再借该恢复点补做历史证明。

## 4. D 盘单恢复点政策

1. **后续所有新恢复点，包括临时文件和最终文件，都写入 `/mnt/d/quant_data_backups/`。** 不在 C 盘或 WSL 内创建新全文件备份，不与网盘源目录混放。夜间执行器在参数解析时强制校验该精确路径，传入 `data/backups/`、其他 WSL 路径或其他 D 盘目录都会拒绝启动。
2. 最终只保留一个最新已验证恢复点。旧恢复点保留到新恢复点验证完成后再删除。
3. 默认安全轮换：停止写库并确认无 RUNNING、无 WAL → 检查 D 盘容量 → 写 D 盘临时文件并 `fsync` → 原子命名 → 核对 SHA-256 并以只读方式核验关键 Meta/Canonical → 删除旧恢复点。
4. D 盘必须同时容纳安全轮换期间的旧、新备份，并保留至少 20 GiB 余量。C/D 实际宿主剩余空间低于 20 GiB 时停止新 Run；备份轮换只读主库且不写 C 盘，因此只应用 D 盘容量门槛，但不能借备份绕过新 Run 的 C 盘停止条件。**WSL 内部或未分配膨胀空间不计入宿主可用空间**。
5. 不做每日或月度备份，不保留年度备份链。年度或明确授权窗口开始前复用/建立基线，验收后轮换。
6. 若 D 盘无法容纳安全轮换，停止并报告。先删除旧备份再写新备份会形成零恢复点窗口，必须另行明确授权。
7. 备份验证用于证明副本可恢复，不属于常规旧事实 `EXCEPT ALL` 审计。

## 5. 固定夜间执行器

正式实现：`scripts/run_local_minute_overnight.py`。它只通过正式 CLI 写业务数据；编排器自身只读 Meta、写外部日志并复制备份文件，不实现第二套转换或发布逻辑。

### 5.1 授权与预检

单次手工窗口仍不得自动扩大范围。夜间连续模式必须由用户明确给出起止年份；以下命令授权 2023→2013 自动跨年，年内按交易日升序：

```bash
uv run --frozen python scripts/run_local_minute_overnight.py \
  --start-year 2023 --end-year 2013 \
  --db /home/xindingtian/code/quant_data/data/warehouse.duckdb \
  --source-dir /mnt/d/BaiduNetdiskDownload/stock_1min \
  --backup-dir /mnt/d/quant_data_backups \
  --landing-dir /home/xindingtian/code/quant_data/data/landing \
  --work-dir /home/xindingtian/code/quant_data/data/probes/local_parquet_2023_2013_overnight
```

先生成只读计划：

```bash
uv run --frozen python scripts/run_local_minute_overnight.py \
  --start-year 2023 --end-year 2013 --dry-run
```

执行器每年从已发布 SSE 日历生成清单，检查源文件和依赖，并调用正式 `Stock1MinDataset.dependencies()/resolve_scope()`。新任务首次执行要求主库与唯一 D 盘恢复点 SHA-256 一致；不一致时停止，不擅自覆盖恢复点。`--dry-run` 不运行 CLI、不核对主库完整哈希、不轮换备份。

### 5.2 记录后继续

1. 已登记 Unit 级 ERROR 即使集中或连续出现，也记录后推进：

   ```text
   local.daily_conflict
   local.auction_not_reported
   local.empty_ambiguous
   local.volume_amount
   suspend.timing_format
   suspend.timing_range
   ```

2. 已知 WARNING：`local.zero_auction`、`local.zero_close_auction`、`local.post_close_excluded`、`local.out_of_scope_zero`。
3. 新 Unit 级 ERROR 只有在正式 Meta 证明影响范围明确、ERROR 与 FAILED Unit 集合一致、失败 Unit 无新 Publication、无全局/依赖/发布异常且无新 WARNING 时才可继续。**同一新 ERROR 规则在第二个交易日再次出现时停止。**
4. Unit 级 UNKNOWN 可记录并跳过，前提是其范围明确、没有发布且不影响其他 Unit。本地 Adapter 不再把这类 Unit 的源行二次升级为全局 `local.scope_conflict`；既有 `run_unit` UNKNOWN 原因即为正式问题记录。
5. 单日缺 `stock_daily/suspend_daily` Coverage（包括 FAILED、PENDING 或 UNKNOWN）只记录为 SKIPPED 并继续，**不参与连续日期熔断**；这些日期没有分钟发布，必须进入后续集中修复台账。单日缺源文件也可跳过，但连续两个交易日缺源文件时停止。
6. 连续两个实际执行日零 PUBLISHED 时停止，防止系统性问题下空跑。
7. 任何既有分钟日期均不自动重跑，包括 SUCCESS、PARTIAL、FAILED、本地或银河 Run。2013-01-04 的既有银河 PARTIAL 记录后跳过，不阻塞其他 2013 日期。
8. 北交所集中缺源是已确认的源文件交付缺口，不得通过改代码、补零或推断开盘竞价放行。

### 5.3 必须停止

- 新 WARNING、全局或无法归属的 ERROR、`local.row_accounting`、`local.traded_equivalence`；
- Candidate、Schema、Scope、Publisher、事务或 Run.error 异常；
- 进程异常、RUNNING/PENDING 遗留、CLI 输出与 Meta 不一致；
- 文件读取期间变化、SHA-256 不一致、共享快照或日历依赖异常；
- 非预期 Meta/Coverage 变化、外部写库、锁冲突；
- 连续缺源文件、连续零发布或新 Unit ERROR 跨日重复；日频 Coverage 缺失不再列入连续熔断；
- C/D 实际宿主余量低于 20 GiB、备份无法安全轮换；
- 任何第 3.2 节所述较大损坏风险。

单日 CLI 默认硬超时为 1800 秒；超时只终止该执行器新建的进程组，然后停止。不得自动清库、回滚成功 Unit、重试或修复。遗留 RUNNING/PENDING 只能在进程退出后按第 6 节人工收尾。

### 5.4 日志、进度与恢复

默认工作目录：

```text
data/probes/local_parquet_2023_2013_overnight/
├── plan-<year>.json
├── year-<year>-summary.json
├── campaign-summary.json
├── state.json
├── daily.jsonl
├── issues.jsonl
├── logs/
└── STOPPED.json / stop.traceback.log（仅停止时）
```

- 终端只输出 YEAR_PLAN、每日短摘要、MONTH_DONE、YEAR_DONE 和最终状态。
- 完整 stdout 验收后压缩为 `logs/YYYYMMDD.stdout.json.gz`；stderr 和 `/usr/bin/time -v` 数据分别保存。
- `state.json` 保存新错误首次日期、连续计数、已完成年度和当前备份。`daily.jsonl` 是逐日执行记录，恢复时重放以修正策略状态；已记录日期不重复写台账或执行。
- 存在 `STOPPED.json` 时拒绝直接重启。人工核对停止原因后，显式追加 `--resume`；已有日期仍跳过，不重跑，也不自动清除新规则历史。
- 年度结束先完成 D 盘安全轮换，再把年度标为完成并进入下一年度。未触发独立对账时，年度汇总不得声称旧事实逐行无差异。

## 6. 手工单日命令与异常收尾

需要单独运行一日时使用：

```bash
uv run --frozen python -m quant_data ingest stock_1min \
  --start-date YYYYMMDD --end-date YYYYMMDD \
  --source-parquet /mnt/d/BaiduNetdiskDownload/stock_1min/YYYYMMDD.parquet \
  --db /home/xindingtian/code/quant_data/data/warehouse.duckdb \
  --landing-dir /home/xindingtian/code/quant_data/data/landing
```

进程异常且遗留 RUNNING/PENDING 时，先确认进程已经退出，再由用户明确决定收尾：

```bash
uv run --frozen python -m quant_data meta reconcile --db data/warehouse.duckdb
```

`reconcile` 后仍保持停止，不自动续跑。恢复执行、数据库恢复、生产路径替换、先删旧备份和统一修复均需明确授权。统一修复继续使用 Validator/Publisher；银河可使用既有 `--ts-code` 子集。

## 7. 最小报告内容

每日一行由执行器自动生成。年度只需汇总：

```text
授权范围与完成日期清单
成功/失败/跳过 Unit 与 Publication 行数
按规则、代码和日期汇总的新缺口
停止事件及处置
独立历史对账：未触发 / 触发原因、范围与结果
耗时、最大 RSS、库增长和 C/D 余量
D 盘恢复点路径、大小、SHA-256 与验证结果
遗留问题
```
