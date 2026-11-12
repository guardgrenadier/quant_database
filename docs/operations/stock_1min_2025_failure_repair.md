# stock_1min 2025 失败基线与修复记录

> 文档定位：2025 专项修复基线与历史记录，不是常规回填权威。后续修复的 Meta 优先、高成本独立检查触发条件和 D 盘单恢复点政策，按 [网盘分钟回填执行参考](local_parquet_yearly_backfill_agent_runbook.md) 第 3～4 节执行；本文历史记录中的旧路径和旧对账要求不再作为新操作指令。

日期：2026-11-09。用途：为不了解前情的修复 Agent 提供不可变失败基线、集中事件和安全生产约束。本文持续记录修复经验；基线 JSON 不因后续修复改写。

## 1. 基线文件与当前口径

完整基线：`docs/operations/stock_1min_2025_failure_baseline.json`

- 范围：2025 年最新一次尝试仍为 FAILED 的 `stock_1min` Unit；不包含 2013、2026 或已 PUBLISHED Unit。
- 预期 Unit：1,317,342；当前成功：1,311,322；失败：6,020。
- 2025 Canonical：316,133,992 行。
- 基线逐 Unit 保存代码、日期、名称、规则、诊断、最新 Run ID、源文件路径/SHA-256 和当前 Coverage。
- 基线 failures payload SHA-256：`6cd650f87cf27d53c4a3b26a1005f2174c076ed188ee0fec2e1a8ff98cbfaf07`。
- 基线文件 SHA-256：`215875bdc50f526eefebdf974ebf0a0b35b4ce84312d8daee0201a621f5f6cdd`。

2025 年 243 个开市日已全部执行。年度旧数据/Meta 全表对照曾因 WSL 重载中断，不能视为已完成；后续不因常规回填或修复自动补做，只有执行参考第 3.2 节的较大损坏风险触发时才按最小必要范围对账。

## 2. 失败规则基线

| 规则 | Unit 数 | 当前含义与边界 |
|---|---:|---|
| `local.daily_conflict` | 4,302 | 日线正成交但本地分钟整股无行；不得补成 NO_TRADE |
| `local.auction_not_reported` | 1,611 | 非全天停牌 Unit 缺少源 09:30 开盘边界；不得与显式零量混同 |
| `local.empty_ambiguous` | 89 | 日线/停牌/分钟证据不足，不能区分合法无交易与缺失 |
| `suspend.timing_format` | 15 | 停牌时段不符合当前单段 `HH:MM-HH:MM` 契约；银河同样受此规则阻断 |
| `local.volume_amount` | 3 | 量额组合违反契约，不得置零或重分类放行 |

## 3. 已识别的集中失败事件

### 3.1 北交所 daily_conflict 日期系列

16 个交易日各出现 264～277 个失败：02-07、02-10、02-11、03-26、03-27、03-28、05-16、05-19、05-20、07-03、07-04、07-07、08-19、08-20、08-21、10-13。

这构成明确的日期×北交所集中模式，但尚未证明根因。`302132.SZ`（中航成飞）另有 26 次 `daily_conflict`，应独立分析。

### 3.2 北交所 auction_not_reported 日期系列

| 日期 | Unit 数 |
|---|---:|
| 2025-02-12 | 264 |
| 2025-03-31 | 265 |
| 2025-05-21 | 266 |
| 2025-07-08 | 266 |
| 2025-08-22 | 273 |
| 2025-10-14 | 277 |

这些日期不同于 daily_conflict 集中日，不能合并成同一根因。

### 3.3 证券维度重复问题

- `empty_ambiguous`：920680.BJ 32 次、920808.BJ 10 次、688766.SH 7 次、920305.BJ 6 次、920961.BJ 5 次。
- `volume_amount`：600462.SH 在 07-01、07-08、07-11 各 1 行异常。
- `suspend.timing_format` 较分散，没有单日 20 个以上的集中事件。

## 4. 修复边界

1. 只处理基线内 2025 Unit，不自动扩展到 2013、2024、2023、2026，也不把后续新失败混入基线。
2. 先区分“基线已修复”“仍失败”“出现更新失败”；不能只看 Coverage=PUBLISHED。
3. `daily_conflict` 可用银河做只读探针，合格后使用既有 `--ts-code` 子集、统一 Validator 和 Publisher；不得直接 INSERT/DELETE Canonical。
4. `empty_ambiguous` 逐股核对在市/退市、停牌、日线、本地分钟和银河交付；证据不足继续保留失败。
5. `timing_format`：2026-11-11 已确认并实现多段、非补零和秒级完整分钟子集解析；逆序、零长度及未知格式仍阻断。银河与本地不得绕过共用解析，生产重跑仍须等待独立验证和明确执行。
6. `volume_amount` 保留原始错误，不置零、不修补数值；先确认来源和行级归属。
7. 新 ERROR/WARNING、依赖漂移、全局错误或审计不一致停止；集中性问题当日停止分析，不等月末。
8. 一次只运行一个 ingestion；生产写入前按执行参考创建或复用 D 盘唯一恢复点，分别检查 C/D 宿主余量。WSL 内部 `df /` 和未分配膨胀空间不作为备份容量依据。
9. 常规验收只使用正式 CLI 和 Meta。历史 `EXCEPT ALL` 仅在执行参考第 3.2 节的较大损坏风险触发时使用，并按最小必要范围、分片和资源预算执行。
10. 不保留常规 Landing；临时探针和修复产物验收后清理，库内正式 Meta 不删除。
11. 生产修复禁止 `--keep-artifacts`；它会导出整张 Canonical 和全阶段 Meta，而非仅目标 Unit。需要完整产物时只在规模受控的独立库验证；需要独立对账时使用最小定向范围。

## 5. 修复工作流程

1. 阅读 AGENTS、核心设计、分钟提案、本地 Parquet 契约、年度回填 Runbook 和本文。
2. 只读核对当前库、基线 JSON 哈希、最新 Unit 尝试、依赖和恢复点。
3. 先分析集中事件，再选择最小样本；报告确认的事实、假设和预计影响范围。
4. 独立库/只读探针验证后才申请生产发布；每次只发布明确 Unit。
5. 常规以正式 CLI 和 Meta 验收；只有明确风险触发时，才对修复 Unit 或非目标事实做最小范围独立对账。
6. 在本文第 6 节追加修复记录；不改写基线 JSON，不用新成功 Run 掩盖旧失败。

## 6. 修复经验与生产记录

### 2026-11-09：建立基线

- 从当前 Meta 生成 6,020 个 2025 最新失败 Unit 的不可变基线。
- 尚未修复任何基线 Unit。
- 后续每条记录至少写明：日期、规则/事件、样本与根因证据、实际修复 Unit 数、Run ID、独立验证、旧数据保护、恢复点和遗留风险。

### 2026-11-09：北交所 2025-02-07 集中缺源修复及产物 OOM 中断

- 规则/事件：`local.daily_conflict`。基线源文件 SHA-256 复核一致，文件内北交所行数为 0；这证明本地源集中缺交付，不证明供应商为何缺失。独立库样本 `920000.BJ` 经 Run `873dacff-c256-45d6-816e-4510ca6b1ca8` 使用旧代码 `832000.BJ` 请求、统一新代码发布 243 行，统一 Validator/Publisher、请求审计及 Candidate/Canonical 对照通过。
- 生产修复：Run `8cc74c6d-2424-46fd-97a9-54ed84a7a526` 于 20:16:41 提交 SUCCESS，264 个基线 Unit 全部 PUBLISHED，新增 64,145 行，266 次请求，0 ERROR；7 个 `snapshot.zero_auction` WARNING 均属契约允许的竞价零成交。当前基线分类为 264 个已修复、5,756 个仍失败，无基线外新增 2025 最新失败。
- 验证与数据保护：生产 Candidate 与目标 Canonical 64,145 行双向 `EXCEPT ALL` 为零；同日 1,236,502 条非目标分钟双向差异为零；恢复点中的旧 Run、Run Unit、Dependency、Request、Check、Publication 均无变化，Coverage 仅 264 个目标由 FAILED 改为本次 PUBLISHED；固定依赖无漂移。环境检查通过，未修改生产代码或契约。
- 恢复点：`data/backups/warehouse-20261109T201118+0800-before-2025-failure-repair.duckdb`，SHA-256 `a83cc13d209aa39fef6515f5d3c9739dd6c074cde020dbe7e18dbb017e34433a`，创建时核验 6,020 FAILED / 1,311,322 PUBLISHED 和 316,133,992 条 2025 分钟。
- OOM 事故：首日 `--keep-artifacts` 导出全表 `published/canonical.parquet` 6.3 GiB；20:16:54 内核全局 OOM 杀死 Python，匿名 RSS 14.4 GiB 且 Swap 用尽。直接内存风险为 `core/artifacts.py` manifest 使用 `Path.read_bytes()` 整文件哈希。业务提交早于 OOM，其他日期未执行。
- 遗留风险/停止项：首日 Landing 约 6.7 GiB 保留，manifest 缺失；Meta 的 `artifacts_complete=true` 是中断前已记录状态，不能视为 manifest/全产物验收通过。库内 Meta 不删除或改写，后续续跑前须明确记录该中断并完成产物处置；不得再次在大生产库使用 `--keep-artifacts`。
- 产物处置与 OOM 修复：完成目标及非目标数据对照后，首日不完整 Landing 已按常规政策清理，库内 Meta 保持原状。`core/artifacts.py` 已改用 `hashlib.file_digest()` 流式哈希，新增禁止 `Path.read_bytes()` 的回归测试；全套 217 项测试中 216 通过、1 项既有样本缺失跳过，环境检查通过。该修复不减少全表导出成本，后续生产 Run 均不传 `--keep-artifacts`。

### 2026-11-09：02-10～02-12 续跑成功，03-26 会话故障停止

- 样本与根因证据：02-10/11 本地文件北交所行数为 0，02-12 北交所记录从 11:10 才开始；源文件哈希均与基线一致。生产继续使用统一银河子集、Validator/Publisher，不重跑已完成的 02-07，不传 `--keep-artifacts`。
- 已完成修复：02-10 Run `8e77abca-147f-4ea7-9ea4-729e6dab02af` 发布 264 Unit、64,152 行、0 WARNING；02-11 Run `f7be9fe3-0ca3-4926-90ac-9e459b82f900` 发布 264 Unit、64,147 行、5 个竞价零成交 WARNING；02-12 Run `7f2fb772-119b-4f04-b13c-94da21598816` 发布 264 Unit、64,146 行、6 个竞价零成交 WARNING。三日合计 792 Unit、192,445 行。
- 测试与数据保护：逐日 Scope、成功 Unit、Publication、Canonical 行数、Coverage 指针、请求错误、依赖版本及 Landing 清理均通过；只有契约允许的 WARNING。子进程峰值 RSS 约 1.6 GiB。沿用批前恢复点 `warehouse-20261109T201118+0800-before-2025-failure-repair.duckdb`。
- 停止事件：03-26 Run `da851116-329e-4fea-b604-9f4fb45a4d20` 为 FAILED；266 个请求均为 `Galaxy worker did not complete after 2 respawns`，264 Unit 全部 INCOMPLETE/FAILED，0 Publication、0 新增 Canonical 行。它是会话级执行故障，不能据此推断银河无数据；未自动重试，后续日期未执行。
- 当前状态与遗留风险：累计修复 1,056 Unit；基线中 4,700 个仍为原失败、264 个出现更新失败、无基线外新增 2025 最新失败。获批范围内尚有 4,832 Unit 待处理，另有 132 个无当前修复依据。03-26 的失败请求证据保留；恢复银河前须单独确认会话健康，不能借补取绕过熔断/会话保护。

### 2026-11-09：03-27～03-31 成功，05-16 六个 Unit 补取失败后跳过

- 成功修复：03-27 Run `3bee6db4-7cd2-4955-b664-a525ac6d3f1a` 发布 264 Unit、64,148 行、4 个竞价零成交 WARNING；03-28 Run `a9c44415-6497-49eb-92a6-7ed20cf8a0f3` 发布 265 Unit、64,390 行、5 个竞价零成交 WARNING；03-31 Run `82291e2d-c057-4128-888c-54558a276f86` 发布 265 Unit、64,395 行、0 WARNING。逐日 Scope、Publication、Coverage、依赖、请求及 Landing 审计通过，子进程峰值 RSS 约 1.7 GiB。
- 05-16 部分修复：Run `16c3bc8b-02e0-4ae3-a96f-f875b72366a9` 为 PARTIAL，260 Unit 发布 63,180 行；`920167/920445/920489/920682/920799/920819.BJ` 六个 Unit 的 K 线为 `-76`，快照父批次及单股补取均为 `code entry is None`，保留 `source.request` 与伴生 `source.daily_conflict`，未发布失败 Unit。两条 `source.request_retry` WARNING 与补取路径一致，无熔断、全局错误或依赖漂移。
- 跳过决策：依据用户明确政策，多次尝试仍失败的日期不再重试并继续后续日期；03-26 与 05-16 六个 Unit 均保留失败及证据。该政策不扩展到熔断、全局错误、未知规则或审计不一致。
- 当前累计修复 2,110 Unit，基线仍失败 3,910 Unit；后续从 05-19 开始，剩余计划 14 日、3,508 Unit。沿用唯一批前恢复点，所有生产 Run 不传 `--keep-artifacts`。

### 2026-11-09：05-19～10-14 执行完成与整批验收

- 本批 14 日均使用明确基线代码的银河 `--ts-code` 子集、统一 Validator/Publisher，不传 `--keep-artifacts`。共修复 3,190 Unit、新增 775,124 行，318 Unit 在已定义来源补试后仍失败并按用户政策跳过；逐日审计无范围、Publication、Coverage、依赖、熔断或资源异常，子进程峰值 RSS 约 1.7 GiB。

| 日期 | Run ID | 修复 Unit | 保留失败 | 新增行数 |
|---|---|---:|---:|---:|
| 05-19 | `e5194256-f6e1-4c52-9ac5-a36b87e17a16` | 260 | 6 | 63,180 |
| 05-20 | `b4636c47-f8d0-44b1-8850-ca2b9b7d1155` | 260 | 6 | 63,178 |
| 05-21 | `4ad16710-5e8c-45e2-8e5b-d23a02bceefc` | 260 | 6 | 63,180 |
| 07-01 | `7df9a2c1-8da1-4fe2-b144-2af23a180a35` | 1 | 0 | 241 |
| 07-03 | `c0bc790c-c532-4617-b553-30535a03e427` | 261 | 6 | 63,420 |
| 07-04 | `1d2ee881-0dce-49c4-80cf-ae7a4cc1669a` | 262 | 6 | 63,661 |
| 07-07 | `cc81d15c-8adb-4f49-8ba8-cffd83ce67bc` | 260 | 6 | 63,177 |
| 07-08 | `b2483ba6-3bbb-40ec-a8dc-4a33ff86bdad` | 260 | 6 | 63,164 |
| 08-19 | `4e60c6f8-d948-4f33-97bd-096f7e3f5235` | 265 | 6 | 64,395 |
| 08-20 | `d7f3d1b1-161a-4395-a011-e560b7dedd3f` | 14 | 258 | 3,402 |
| 08-21 | `bd7e2a8b-2a69-4f38-b19c-7f14b2d82544` | 266 | 6 | 64,635 |
| 08-22 | `25120289-b47a-470a-88d3-f3084dd7155b` | 267 | 6 | 64,877 |
| 10-13 | `3332a1d4-d62b-457e-ad9d-ec4bc0e601e7` | 277 | 0 | 67,311 |
| 10-14 | `91f471bf-e6a7-4c2b-aec2-67d3815ac634` | 277 | 0 | 67,303 |

- 失败证据：除 08-20 外的十个 PARTIAL 日各 6 个 Unit 经快照补取仍为 `code entry is None`，并伴有 K 线 `-76` 的日线冲突。08-20 有 252 个 Unit K 线成功但快照无效，另 6 个 K 线 `-76` 且快照无效；只发布 14 个合格 Unit。上述为交付事实，不断言供应商内部根因。失败 Run 证据保留，成功 Run Landing 已清理。
- 整批结果：累计修复 5,300 Unit、新增 1,287,827 行；当前 2025 Canonical 为 317,421,819 行。基线分类为 5,300 已修复、588 更新失败、132 原失败未变，无基线外新增 2025 最新失败；Coverage 为 1,316,622 PUBLISHED / 720 FAILED。
- 最终验证：23 个修复日非目标分钟逐日双向 `EXCEPT ALL` 为零，13 张其他 Canonical 无差异，恢复点中的旧 Run/Run Unit/Dependency/Request/Check/Publication 均保留；Coverage 差异精确限于 5,888 个目标 Unit。6 个 `920000.BJ` 改代码前后样本和 `600462.SH@2025-07-01` 与独立库逐字段一致。全套 217 项测试中 216 通过、1 项既有样本缺失跳过，环境检查通过；未改变业务契约。
- 恢复点轮换：修复后唯一恢复点 `data/backups/warehouse-20261109T231812+0800-after-2025-failure-repair.duckdb`，SHA-256 `b819a1c9a5b5c6911cd45348343e9b2d245668153cad4e89c1b45d21fd180e3b`，16,899,649,536 字节，只读核验无 RUNNING，行数、Coverage 和 341 个分钟 Run 符合当前状态。批前恢复点 `warehouse-20261109T201118+0800-before-2025-failure-repair.duckdb` 在新恢复点验证后删除。
- 遗留 720 Unit：`local.daily_conflict` 596（含 `302132.SZ` 26 和北交所 570）、`local.auction_not_reported` 18、`local.empty_ambiguous` 89、`suspend.timing_format` 15、`local.volume_amount` 2。不得把用户允许的跳过政策解释为这些 Unit 已修复；北交所集中快照无效、03-26 会话故障及其他规则仍需分别调查或契约决策。

### 2026-11-10：daily_conflict / auction_not_reported 最后一轮银河重试

- 范围与前提：仅选择当前仍失败且基线规则为 `local.daily_conflict` / `local.auction_not_reported` 的 614 Unit；不处理 `empty_ambiguous`、停牌格式或两个剩余量额 Unit，不重发已成功 Unit。批前唯一恢复点 `warehouse-20261110T105950+0800-before-2025-final-galaxy-retry.duckdb`，SHA-256 `56264469da1654b001e6db0860cdf829c69e133dbd7807e545c60aef94603cc4`。所有 Run 不传 `--keep-artifacts`。
- 执行记录：03-26 Run `818ead25-49a2-4406-a1f3-9c27ac475fd6` 修复 264 Unit、64,152 行；其余 Run 均未新增业务行：05-16 `82f4ac3c-d444-4409-b978-2cb78fe47406` 6 个失败，05-19 `28e5a5d4-f256-4563-9b50-39cd03179551` 6，05-20 `0283e483-f14f-4126-b079-ecd69cfe865f` 6，05-21 `ecd8225c-c2e8-4ab6-8d56-9d5b50c160b3` 6，07-03 `8f0f8bc9-2fef-477c-b16d-e6f4a98fdc0e` 6，07-04 `f054a72b-5348-481b-a1c0-2bcff00b347b` 6，07-07 `b377020c-0db2-477c-9336-b8e8ac2aab86` 6，07-08 `787f89e9-8f94-4ac4-b3f4-899bbf46aa26` 6，08-19 `9df49a8c-dae6-4f48-ae4e-83f9d724e7ca` 6，08-20 `ed6f2bfd-bc6a-4b17-880a-75195e3d2cb5` 258，08-21 `991c28be-b7eb-4224-9bcb-841742bd92a3` 6，08-22 `40a7c3b6-87e3-4c08-8d27-1762cf7aa41c` 6，`302132.SZ@01-02～02-14` Run `fb02fccb-a748-4e55-a4b5-5196f0d4b003` 26。
- 最终交付事实：仍失败的 350 Unit 中，98 个为银河 K 线 `-76` 且快照无效；252 个为 08-20 K 线成功但快照持续 `code entry is None`。后者不是“银河全部分钟缺数据”，而是缺合格竞价交付。依据用户决策，这些 Unit 不再补取，保留失败和请求证据，不生成 NO_TRADE/SUSPENDED 或绕过验收。
- 审计说明：`302132.SZ` 多日 Run 另含 18 个休日 OUT_OF_SCOPE，26 个目标全部 FAILED，无目标遗漏或额外发布。此前格式错误 Run `ac7bd89f-134f-4a44-a5bf-8eececfc1d04` 在 Scope 前失败，0 请求、0 业务变更，正式审计保留。
- 数据保护：03-26 的 1,238,183 条非目标分钟双向 `EXCEPT ALL` 为零；批前旧 Run/Run Unit/Dependency/Request/Check/Publication 全部保留；Coverage 差异精确限于本轮 614 个目标。逐批 Scope、Publication、行数、依赖、熔断、Landing 和资源检查通过，子进程峰值 RSS 约 1.6 GiB。
- 当前结果：累计修复 5,564 Unit；2025 Canonical 为 317,485,971 行，Coverage 为 1,316,886 PUBLISHED / 456 FAILED。基线分类为 5,564 已修复、350 更新失败、106 原失败未变，无基线外新增 2025 最新失败。
- 修复后唯一恢复点：`data/backups/warehouse-20261110T112846+0800-after-2025-final-galaxy-retry.duckdb`，SHA-256 `6ca5b18ea7cbfecbcf06ff1ea1ff39b0fc497c78ebe0dc9dbdc56e744ae7c72f`，16,950,767,616 字节；只读核验无 RUNNING、356 个分钟 Run，行数和 Coverage 符合当前状态。批前恢复点在验证后删除。
- 剩余 456 Unit：`daily_conflict` 332、`auction_not_reported` 18、`empty_ambiguous` 89、`suspend.timing_format` 15、`volume_amount` 2。其中 350 个 daily/auction Unit 已按用户决策结束补数；其他类别不因本轮结论自动改变处理方式。

### 2026-11-10：600462.SH 两个 volume_amount Unit 银河替换完成

- 根因证据：本地源 `600462.SH@2025-07-08/07-11` 各有一行 `vol=1, amount=0`，违反正量行必须同时正量额的契约；原错误为 ERROR 阻断，不是 WARNING，未置零、修补或删除异常行。
- 生产修复：07-08 Run `f3e2c86d-b827-47aa-bb97-0cc683e9bbff` 通过银河 `--ts-code` 整 Unit 验收和 Publisher 替换，发布 241 行；07-11 Run `fbbbf2ab-f499-477a-b125-0c7ca765ae22` 发布 241 行。两 Run 各 2 次成功请求、0 ERROR/WARNING，Landing 均清理；不使用 `--keep-artifacts`。
- 数据保护：两日分别 1,304,233 和 1,305,494 条非目标分钟双向 `EXCEPT ALL` 为零；批前旧 Run/Run Unit/Dependency/Request/Check/Publication 全部保留，Coverage 差异精确限于这两个 Unit，依赖无漂移。未修改代码或契约。
- 当前结果：累计修复 5,566 Unit；2025 Canonical 为 317,486,453 行，Coverage 为 1,316,888 PUBLISHED / 454 FAILED。`local.volume_amount` 剩余 0；其余为 `daily_conflict` 332、`auction_not_reported` 18、`empty_ambiguous` 89、`suspend.timing_format` 15。
- 恢复点：唯一恢复点 `data/backups/warehouse-20261110T120735+0800-after-volume-amount-repair.duckdb`，SHA-256 `decbd0bd54694e7401ddbb5f0c0a12085535145235ece8a45a76a26d8224f729`，16,948,547,328 字节；只读核验无 RUNNING、358 个分钟 Run，行数和 Coverage 符合当前状态。批前恢复点在验证后删除。

### 2026-11-11：停牌格式契约扩展与独立验证（未生产重跑）

- 已确认支持 S 的多段、非补零 `H:MM[:SS]`；原区间必须正向，开始秒向上、结束秒向下取完整分钟，多段排序取并集，不足一个完整分钟不产生 SUSPENDED。真实成交优先，逆序/零长度/未知格式仍阻断，原始 `suspend_daily` 不改写。
- 实现限于 `sources/stock_1min_common.py` 共用解析，未新增表、Meta 或发布接口。新增多段、乱序、相邻、秒级边界、子分钟空区间、非补零逆序及成交优先测试；全套 219 项测试中 218 通过、1 项既有样本缺失跳过，环境检查通过。
- 独立库真实本地 Run `0272e258-a0e0-4814-8b45-e461d011f824` 的 `001221.SZ@2025-07-30` 和 Run `981d5323-83dd-4848-b2bc-eba05f15b2bd` 的 `600200.SH@2025-12-09` 均发布 241 行，目标 Candidate/Canonical 与 manifest 哈希通过。真实秒级样本 `831152.BJ@2022-09-01` 规范为 `[10:06,10:15)`。07-30 Run 因既有 `920680.BJ empty_ambiguous` 保持 PARTIAL，未借格式扩展放行。
- 当前 15 个 2025 `suspend.timing_format` Unit 尚未生产重跑；是否修复及来源范围仍需明确执行，不能把独立验证视为生产 Coverage 已改变。

### 2026-11-11：2025/2026 停牌格式 Unit 生产重跑完成

- 范围：2025 基线 15 个 `suspend.timing_format` Unit，加 2026 既有失败 `603056.SH@2026-01-09` 和 `300391.SZ@2026-03-20`。全部使用银河 `--ts-code` 子集和统一 Validator/Publisher，不传 `--keep-artifacts`。
- 2025 全部 15 个修复，新增 3,619 行：603072/01-03 `6e8e1020-d033-485d-88f4-31dac59a9107`，301557/03-04 `3b589f05-186b-4969-8377-0d692e3aa811`，603409/03-04 `16bb03fd-6bb2-4802-b4d8-aece260aac20`，603257/04-08 `cd807988-f9e2-4c14-a840-78bfed2d3437`，300630/04-28 `2964cb70-fd21-43eb-aebd-95c2756d09e8`，603382/06-12 `0ca9b63e-5aef-4246-82df-402fe3e78b7a`，603400/06-25 `c16c34e6-54db-4fcb-889d-8619b17c41ac`，600930/07-16 `698b5b50-b22b-46bf-840d-06cd58b272fb`，001221/07-30 `8667f0ab-e76f-4b6d-b594-1d0df3b2a355`，920091/11-21 `d910d454-cfbd-49f0-a04a-d6f291a50491`，600200/12-09 `a9d131d2-29b3-438c-a495-2ce0fc7668d1`，920680/12-11 `a077b0cc-dbd8-465d-83a8-147e2617a1a7`，301449/12-23 `8a3c7933-0b28-42af-869c-39735eb0abe5`，301667/12-23 `a26641c3-97d8-4e4d-8080-a10b5cac642e`，603248/12-23 `d386fe1d-80a5-45d4-8f70-370b45d98975`。
- 2026：`300391.SZ@2026-03-20` Run `2a67a2dd-ee78-4b10-b039-c1506a648de5` 发布 241 行；逆序 `603056.SH@2026-01-09` Run `2b407459-5b83-4372-9f51-b9a5a3bdb1c8` 继续以 `suspend.timing_range` 阻断，0 Publication、0 Canonical 行，符合契约。
- 数据保护：14 个实际日期的非目标分钟逐日双向 `EXCEPT ALL` 为零；批前旧 Run/Run Unit/Dependency/Request/Check/Publication 全部保留，Coverage 差异精确限于 17 个目标；所有成功 Run 0 ERROR/WARNING，依赖无漂移，Landing 清理通过。
- 当前结果：2025 累计修复 5,581 Unit，Coverage 为 1,316,903 PUBLISHED / 439 FAILED，Canonical 为 317,490,072 行；`suspend.timing_format` 剩余 0。剩余 439 Unit 为 `daily_conflict` 332、`auction_not_reported` 18、`empty_ambiguous` 89。2026 逆序 Unit 不在 2025 基线内，继续保留失败。
- 恢复点：唯一恢复点 `data/backups/warehouse-20261111T005508+0800-after-suspension-format-repair.duckdb`，SHA-256 `2bb42b6108ee0a0a63ffb9ddf5b779a4fd26b9f331bbbded1d984f4b88fbd227`，17,833,930,752 字节；只读核验无 RUNNING、375 个分钟 Run，2025 行数/Coverage 及两个 2026 目标行数符合预期。批前恢复点在验证后删除。

## 7. 可直接交给 fresh agent 的 Prompt

```text
你是 quant_data 项目的 stock_1min 2025 失败修复 Agent。请依次阅读：
- AGENTS.md
- docs/design/core_design.md
- docs/design/stock_1min_ingestion_proposal.md
- docs/design/stock_1min_local_parquet_contract.md
- docs/operations/local_parquet_yearly_backfill_agent_runbook.md
- docs/operations/stock_1min_2025_failure_repair.md
- docs/operations/stock_1min_2025_failure_baseline.json

只处理基线中的 2025 失败 Unit，不自动扩大到其他年份或新失败。先核对基线哈希和当前 Meta，
区分已修复、仍失败和更新失败。优先分析文档中的北交所集中事件及重复证券；事实与假设分开报告。

默认先只读调查和独立探针，不写生产库。生产修复必须复用统一 Validator/Publisher；
daily_conflict 可使用银河 --ts-code 子集，其他规则不得擅自放宽或改写事实。任何契约变化先请求确认。

生产写入前按执行参考创建或复用 D 盘唯一恢复点，检查数据库锁、RUNNING、C/D 宿主余量。
常规只使用正式 CLI 和 Meta 验收；历史 EXCEPT ALL 仅在较大损坏风险触发时按最小范围执行。
常规 Landing 不保留，库内 Meta 不删除。新错误、依赖漂移或审计不一致立即停止。

每完成一项修复，将样本、根因证据、修复 Unit、Run ID、测试、数据保护和遗留风险追加到
stock_1min_2025_failure_repair.md 的修复记录；不要修改不可变基线 JSON。
```
