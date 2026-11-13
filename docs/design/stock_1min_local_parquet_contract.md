# stock_1min 网盘本地 Parquet 转换契约

日期：2026-11-05，验收更新至 2026-11-06；2026-11-08 物理存储修订。状态：**转换契约、统一新代码及剩余风险已确认；十列 TIME Schema 已于 2026-11-09 完成生产重建和全历史对账**。

依据：[核心架构](core_design.md)、[技术设计与实现边界](ingestion_v3.2_implementation.md)、[银河分钟契约](stock_1min_ingestion_proposal.md)、[网盘文件勘察](../research/historical-parquet-survey-2026-09-29.md)、[历史零量与占位调查](../research/historical-minute-zero-volume-2026-10-22.md)及[北交所收盘标签调查](../research/historical-minute-bj-close-labels-2026-10-22.md)。

## 1. 目标与已确认边界

本契约定义网盘本地日 Parquet 如何转换为现有 `canonical.stock_1min` 完整研究网格，以及转换阶段必须执行的两组守恒检查。银河链路继续保留；本地文件是同一 Dataset 的另一种 Source Adapter，不新建第二份 Canonical。

已确认的运行与审计边界：

1. 一个 Run 只处理一个交易日、一个本地 Parquet 文件。
2. 输入路径为 `/mnt/d/BaiduNetdiskDownload/stock_1min/YYYYMMDD.parquet`，文件只读，不在 D 盘生成或修改文件。
3. 复用现有 Dataset、Scope、依赖固定、统一 Validator、Publisher、Publication 和 Coverage；不得由 Adapter 直接 INSERT Canonical。
4. 本地 Run 不产生 HTTP 请求，`meta.request_attempt` 保持为空，`request_attempts=0` 是正常状态。
5. 不新增 Meta 表。源文件路径、大小、SHA-256、行数和映射版本进入长期保留的 `meta.run.params`。
6. 两组转换守恒检查属于本地 Adapter 的源专属准备诊断，在 `prepare()` 中执行并返回 `Check`；Adapter 不直接写审计表或决定发布。
7. 源行情准确性不在本轮重新验证。本契约保证的是输入身份、转换完整性、项目业务验收和发布可追溯，不新增跨供应商准确性证明或分钟—日线数值硬门禁。
8. 不改变现有历史起点、开市 Scope、在市 membership、全天停牌、盘中停牌、统一新代码 Unit 或特殊交易时段边界。

契约版本固定为：

```text
NETDISK_END_MINUTE_V1
```

任何时间映射、竞价、北交所、排除或守恒规则变化都必须升级版本，不得同名静默修改。

## 2. 输入身份与 Raw 边界

### 2.1 Run 输入身份

本地入口在登记 Run 前取得并记录以下控制信息：

```json
{
  "start_date": "20200417",
  "end_date": "20200417",
  "source": {
    "type": "LOCAL_PARQUET",
    "path": "/mnt/d/BaiduNetdiskDownload/stock_1min/20200417.parquet",
    "sha256": "...",
    "size_bytes": 123456789,
    "row_count": 1299923,
    "mapping_profile": "NETDISK_END_MINUTE_V1"
  }
}
```

`start_date` 必须等于 `end_date`。`source` 是输入与转换审计信息，不参与业务范围推导。

Adapter 实际读取时必须重新核对文件大小、完整文件 SHA-256 和 Raw 行数。任一不一致均阻断发布。成功发布后的溯源链为：

```text
Coverage.publication_id
  → Publication.run_id
  → Run.params.source
```

Run Unit 通过 `run_id + unit_key` 关联同一输入。若未来允许一个 Run 读取多份文件或一个 Unit 由多份文件拼接，需重新评估结构化输入 lineage；本版不支持这些形态。

### 2.2 必需源列

```text
code, trade_time, date,
open, high, low, close,
vol, amount,
pre_close, change, pct_chg
```

允许存在并明确忽略技术列 `__index_level_0__`。缺少必需列、出现未登记业务列或列类型不符合本契约时，阻断该日文件并要求升级契约，不做自动兼容。

读取时可增加仅用于技术审计的 `_raw_row_number`，表示文件内行号。它不是业务字段，只用于证明每条源行恰好获得一个处理去向。

### 2.3 通用字段规则

- `date`：严格八位日期；所有记录必须等于目标交易日，否则为文件级 ERROR。
- `code`：严格证券代码字符串；沪深代码不得自动改写，北交所按第 5 节归一化。
- `trade_time`：接受 `HH:MM`、`HH:MM:SS` 或实际源使用的 `YYYY-MM-DD HH:MM:SS`，解析为市场本地时间且秒必须为 00；完整时间戳中的日期必须同时等于 `date` 和目标交易日。无法解析、日期不一致或出现未登记标签为 ERROR。
- `vol`：必须有限、非负且数值上为整数，转为 BIGINT；单位已经是股，不做缩放或取整修补。
- `amount`：必须有限、非负，转为 DOUBLE；单位已经是元，不做缩放。
- 正量行必须同时满足 `vol>0, amount>0`；零量行必须同时满足 `vol=0, amount=0`。
- `vol=0, amount>0` 或 `vol>0, amount=0` 为 ERROR，不得置零、舍入或重分类后放行。
- 正量行 OHLC 必须齐全、有限、为正，并满足基本区间关系。
- 零量行 OHLC 不进入最终 Candidate；NO_TRADE/SUSPENDED 价格由项目规则重新构造。
- `pre_close/change/pct_chg` 不进入 Canonical；不得用源 `pre_close` 替代固定版本 `stock_daily.pre_close`。

文件读取、哈希或 Schema 失败属于文件级问题。可可信归属到股票日的坏代码、坏时间、坏数值或状态冲突只阻断受影响 Unit；无法可信归因时阻断整日。

## 3. 沪深时间转换

定义：

- `A`：源 09:30 行，表示开盘竞价；
- `C`：源 09:31 行，表示第一根连续竞价结束分钟。

### 3.1 普通时间映射

| 源时间 | Canonical 时间 |
|---|---|
| 09:31–11:30 | 减一分钟 → 09:30–11:29 |
| 13:01–15:00 | 减一分钟 → 13:00–14:59 |
| 09:30 | 按第 3.2 节作为开盘竞价处理 |
| 其他时间 | ERROR |

普通正量行的 OHLC、volume、amount 保留源值，仅转换时间、代码归属和字段类型。不得去重；映射前后重复业务键均按 Unit 阻断。

### 3.2 09:25 开盘竞价

- `A` 为正量行：生成一条 09:25 TRADED，OHLC、volume、amount 来自 `A`，不执行量额扣减。
- `A` 为显式零量行：不生成 09:25，记录 `local.zero_auction` WARNING。
- 非全天停牌 Unit 完全没有 `A`：不生成 09:25，记录 `local.auction_not_reported` ERROR。显式零量与完全未报告不得合并成同一语义。
- 全天停牌 Unit 不适用上述边界行存在要求；其源行按第 6 节处理。

### 3.3 Canonical 09:30 构造

本地源已经把开盘竞价与连续竞价分开，**不得执行银河链路的 09:30 竞价量额扣减**。

若 `A`、`C` 均为正量行，为保持当前 Canonical 的 09:30 混合 OHLC 语义：

```text
open   = A.open
high   = max(A.high, C.high)
low    = min(A.low,  C.low)
close  = C.close
volume = C.volume
amount = C.amount
status = TRADED
```

若只有 `C` 为正量行，09:30 使用 `C` 的 OHLC、volume、amount。

若 `C` 为零量或不存在，09:30 不生成 TRADED，而由完整网格生成 NO_TRADE 或 SUSPENDED。若 `A` 为正量行，09:25 仍存在，09:30 的参考价继承 09:25 close。

该构造保证 09:25 与 09:30 的量额分别对应源 09:30 与 09:31，不会重复扣除开盘竞价。09:30 OHLC 是按本契约重建的项目语义，不声称与银河逐字段完全相同。

## 4. 北交所统一新代码

**2026-11-05 用户明确修订原先的统一新代码保存决议：Scope 与 Canonical 全历史统一使用 `bse_mapping.n_code` 当前新代码。** `o_code` 只用于识别并转换来源旧代码，不再按交易日期反向生成旧代码 Unit。生产库此前没有改代码前的北交所分钟，因此没有历史行迁移。

使用本次固定版本的 `bse_mapping / SNAPSHOT`：

1. Raw 已为 `n_code` 时直接保留，包括 2025-10-09 之前的网盘历史记录。
2. Raw 为 `o_code` 时，无论行情日期均转换为对应 `n_code`。
3. 同一证券同一日同时出现新旧两套代码时阻断所属 Unit，不拼接、不去重，即使其中一套只有零量行。
4. 映射无法解释的代码进入第 6 节 Scope 冲突处理，不得直接作为范围外数据丢弃。
5. 当前 `stock_basic` 和实际 `stock_daily` 均使用新代码与 Scope 对齐，不得把它们反向映射为旧代码。
6. 银河改代码前如必须使用 `o_code` 请求，该路由只存在于银河 Adapter；响应和 Unit 归属必须转换回 `n_code`，跨迁移日请求分段及单股日补取也遵守该边界。

代码归一化使用固定映射版本并纳入转换审计计数；准备和发布前均沿用现有依赖复核。

## 5. 北交所时间转换

### 5.1 普通时间与派生槽位

| 源时间 | Canonical 处理 |
|---|---|
| 09:30 | 按第 3 节作为开盘竞价，09:30 构造规则相同 |
| 09:31–11:30 | 减一分钟 → 09:30–11:29 |
| 13:01–14:59 | 减一分钟 → 13:00–14:58 |
| 15:00–15:02 | 按第 5.2 节选择收盘记录 |
| 15:03–15:30 | 按第 5.3 节处理盘后记录 |
| 其他时间 | ERROR |

Canonical 11:30 和 14:59 不由本地源正量行填充，由完整网格生成 NO_TRADE 或 SUSPENDED。源 11:30 映射到 Canonical 11:29，不得同时复制到两个槽位。

### 5.2 15:00–15:02 收盘记录

对每股票日的三个候选标签 `15:00/15:01/15:02`：

1. 恰好一根为正量行：将该行映射为 Canonical 15:00 TRADED，其余显式零量行归类为零量占位；不统一减一分钟。
2. 两根或以上为正量行：ERROR，不得选择、合并或去重。
3. 只有显式零量行：不生成 15:00 TRADED，由网格生成相应状态，并记录零收盘竞价类 WARNING。
4. 非全天停牌 Unit 三个标签全部未报告：ERROR，与明确交付零量收盘记录区分。
5. 全天停牌 Unit 不适用上述收盘边界行存在要求。

收盘行 OHLC 保留源值。既有研究证明部分晚标行 OHLC 不一定代表纯集合竞价四价；本契约只转换其项目时间和成交状态，不修补 OHLC，也不声称与银河收盘行逐字段相同。

### 5.3 15:03–15:30 盘后记录

- 15:03–15:29 零量行：显式归类为盘后零量占位并排除。
- 15:03–15:29 正量行：ERROR，本版契约不解释这些记录。
- 15:30 行：作为当前分钟研究网格之外的盘后记录显式排除，不并入 15:00，也不用其价格修补收盘价。
- 15:30 排除按日汇总行数、正量行数、volume 和 amount，并记录 WARNING；零量盘后行保留分类计数。

盘后排除表示“不属于本 Dataset 的业务范围”，不是认定源数据错误，也不是删除源文件记录。

## 6. Scope、状态、网格与参考价

### 6.1 Scope 与排除

本地日文件可以包含发布 Scope 之外的记录，但必须区分：

- **允许排除**：本契约明确规定的北交所盘后记录、可证明的非目标品种，以及固定 `stock_basic/bse_mapping` 能解释但当日不在发布 Scope 的全零占位行。后者记录 `local.out_of_scope_zero` WARNING；只要有正量或证券身份无法解释，仍为 Scope 冲突 ERROR。
- **占位处理**：当前 Unit 的零量行，不属于业务排除。
- **Scope 冲突**：有正量行情但固定 membership 认为当日未在市，或代码无法由固定基础表及映射解释。
- **错误隔离**：坏值、非法时间、重复映射、状态冲突等。

Scope 冲突不得记成普通范围外排除。无法由固定基础表解释的代码不得通过零量或范围外标签静默丢弃；只有契约明确允许的排除原因可以放行。

### 6.2 完整网格与状态

以下沿用现有 `Stock1MinDataset`，不建立第二套业务规则：

1. 沪深生成 240 个连续槽位，北交所生成 242 个；09:25 只在存在合格开盘竞价正量行时附加。
2. 转换后的正量行优先标记为 TRADED。
3. 非全天停牌 Unit 中，无真实成交且落在已解析停牌区间内的槽位为 SUSPENDED，其余缺槽为 NO_TRADE。2026-11-11 起与银河共用多段 `H:MM[:SS]` 解析：原区间必须正向，开始秒向上、结束秒向下取完整分钟，排序后取并集；例如 10:05:50-10:15:50 只覆盖 10:06–10:14 槽位。
4. 全天停牌 Unit 由固定上游事实生成完整 SUSPENDED 网格；源文件零量行作为占位行审计，源文件出现正量行则构成冲突并阻断。2026-11-07 起，R 同日且固定 stock_daily.vol>0 的 Unit 不视为全天停牌；R 单独或无日线不改变原规则。
5. NO_TRADE 的 OHLC 按派生参考价重建，不沿用源零量行价格；SUSPENDED 的 OHLC 为 NULL。
6. 真实成交行优先于盘中停牌文本；全天停牌冲突不得被本地源行覆盖或忽略。
7. IN_SCOPE Unit 不发布零行；空文件、缺行或解析失败不得直接转换成全天 NO_TRADE。

### 6.3 参考价派生与 Serving

- 2026-11-08 起 `reference_price` 不进入 Candidate/Canonical，只由 `serving.stock_1min_with_reference` 按需输出。
- 构造和验收的初始值只能来自固定版本 `stock_daily.pre_close`；无当日日线行时允许 NULL，之后由 TRADED close 在当日内向前继承。
- 不使用源 `pre_close`，不读取前一日 Canonical，不使用未来价格反向回填。
- Dataset 必须从固定输入独立重算并验收 NO_TRADE 的 OHLC；本地 Adapter 不以自身生成结果替代该验收。
- Serving 使用查询时当前已发布的日线，不固定历史 publication_id；日线修订可改变 View 中的派生参考价。

## 7. 两组转换守恒检查

两组检查均位于本地 Adapter 的 `prepare()`，可实现为模块内普通函数。它们使用 Raw、分类结果、标准化预期成交关系和 Candidate 的临时 DuckDB 关系进行集合检查，不逐行转 Python 业务对象。

### 7.1 检查一：源行去向完整性

每条 Raw 行必须且只能获得一个去向。本版去向至少包括：

```text
OPENING_AUCTION
REGULAR_TRADED
BSE_CLOSING_TRADED
ZERO_PLACEHOLDER
FULL_SUSPENSION_PLACEHOLDER
EXPLICITLY_EXCLUDED
REJECTED
```

检查要求：

```text
Raw 总行数 = 各类去向行数之和
```

并同时满足：

1. 不存在未分类行，不存在被分类两次的行。
2. `REJECTED>0` 的 Unit 不得发布。
3. `EXPLICITLY_EXCLUDED` 必须携带本契约规定的原因。
4. 不得使用笼统的 `OTHER_DROPPED` 或未知排除类别。
5. 分类结果按 Unit 汇总；无法归因的分类错误阻断整日。

建议规则名为 `local.row_accounting`，阶段为 `prepare`。该检查发现转换程序静默吞行，不证明源文件本身没有缺行。

### 7.2 检查二：正量记录等价性

先从 Raw 生成 `expected_traded`，字段为：

```text
ts_code, trade_date, bar_time,
open, high, low, close, volume, amount
```

生成前已完成北交所统一新代码归一化、时间映射、09:25 构造、09:30 混合 OHLC 构造、北交所收盘选择和显式排除。

将其与 Candidate 中 `status='TRADED'` 的相同九字段执行双向 `EXCEPT ALL`：

```text
expected_traded EXCEPT ALL candidate_traded = 空
candidate_traded EXCEPT ALL expected_traded = 空
```

该比较同时检查记录数量、重复次数和字段值，不只是比较总量。建议规则名为 `local.traded_equivalence`，阶段为 `prepare`。

reference_price 不进入 Candidate，也不在此比较中；Dataset 独立重算参考价并验收 NO_TRADE 的 OHLC，SUSPENDED 由完整网格和状态规则验收。Raw 解析失败、收盘选择失败等前提不成立时，跳过受影响 Unit 的等价检查并保留根因，避免连带差异错误。

## 8. 检查结果的审计落点

Adapter 只返回 `(Candidate, preparation_checks)`，不直接写 Meta。Runner 将检查交给统一 Validator，由其合并交付、准备、Candidate Schema、Scope 和现有 Dataset 业务检查，形成唯一发布许可。

- WARNING/ERROR 写入 `meta.validation_check`，保存 level、rule、stage、units 和小型 detail JSON。
- `meta.run_unit.check_ids` 关联检查；ERROR 阻断受影响 Unit，WARNING 不阻断。
- `meta.run.summary` 只保存检查计数等汇总。
- `--keep-artifacts` 时可另存 preparation/validation 诊断，但数据库 Meta 是正式审计记录。生产回填禁止该开关：其发布阶段会导出整张 Canonical，而非当前日文件；完整诊断只在规模受控的独立库使用。
- 检查全部通过时不逐规则保存 PASS；检查自身执行异常必须使 Run FAILED/PARTIAL，不得按无问题放行。
- 全量差异行和业务记录不得塞入 Check.detail；必要诊断留在当前 Run 的 Landing 产物。

## 9. 已确认接受的剩余风险与主要关注点

用户已确认接受以下边界；实现和运维报告不得将其隐藏：

1. **源文件缺行可能被完整网格补成 NO_TRADE。** 两组守恒检查只保证文件中已有记录未被转换过程弄丢或改坏，不能证明源文件未交付的行本来没有成交。本地文件没有与银河请求完成状态等价的逐 Unit 完整性证明，本版不新增分钟—日线数值硬门禁。
2. **北交所是代码与时间的复合转换。** 必须覆盖历史 920 代码归一化、15:00/15:01/15:02 选择、15:30 排除及 11:30/14:59 派生槽位；不得统一平移全部分钟。
3. **09:30 是项目语义重建。** 本地源不得再次扣减竞价；重建 OHLC 与银河不保证逐字段一致。
4. **显式零量与完全未报告始终区分。** 本版对非全天停牌 Unit 的 09:30 开盘边界和北交所三个收盘标签全部未报告采取 ERROR，不得为追求回填成功率降为静默 NO_TRADE。
5. **历史 Universe 和持续非交易状态缺口仍按现有契约阻断。** 本地源行不能自动扩大 Scope，不能绕过既有股票日状态问题。
6. **源文件覆盖后的历史可重读性不属于本版保证。** Run 中 SHA-256 长期证明当时输入身份，但若原 Parquet 被覆盖，不能凭哈希恢复旧文件内容；不因此新增 Raw 历史版本库。

## 10. 实现与验收记录

正式实现位于 `sources/local/stock_1min.py`、`sources/stock_1min_common.py`、`datasets/stock_1min.py`、CLI 和 `tests/test_local_stock_1min.py`。本地入口、SourceAdapter/Runner 接口、Canonical Schema 和七张 Meta 表均保持既有结构；银河 09:30 扣减仍留在银河 Adapter，共用函数只接收标准化成交并构造网格、状态和参考价。

2026-11-05 验证结果：

1. 本地 17 项测试通过，覆盖沪深竞价及混合 09:30、完整时间戳日期核对、零量、全天停牌、北交所统一新代码、旧代码映射与新旧代码冲突、15:00/15:01/15:02、盘后排除、重复键、坏值、两组守恒故障注入、旧版保护、重跑和正式 CLI。
2. 全套 208 项测试中 207 项通过，1 项既有股票真实响应样本缺失跳过；环境检查通过。银河 23 项分钟回归全部通过，包括改代码前请求旧代码、响应和两日 Canonical 统一新代码。
3. 真实 `/mnt/d/BaiduNetdiskDownload/stock_1min/20260417.parquet` 在临时独立库通过正式 CLI 发布：5,506 个 Unit、1,327,519 行；TRADED 1,285,719、NO_TRADE 39,640、SUSPENDED 2,160；零 ERROR，32 条零开盘竞价及 307 条零收盘竞价 WARNING，`request_attempts=0`。独立库保留产物的 Candidate/Canonical 双向差异为零。
4. 真实北交所边界验证通过：2022-07-15 的 920169/920223 收盘量分别由 15:02 转为新代码 15:00 的 10,000/57,398 股；2024-04-17 的 920000 由 15:01 转为 15:00 的 63,200 股，15:30 不进入 Canonical；2025-10-09 使用 920000，无旧 832000 Unit。三日北交所均无 ERROR。
5. 2022-07-15 和 2024-04-17 各有一个与北交所无关的 `302132.SZ` 全日源缺失/日线正成交冲突，按契约阻断，两个全日 Run 因此 PARTIAL；2025-10-09 全日 SUCCESS。该缺口未被静默放行，后续全历史执行仍会按现有规则遇到它。
6. 2026-04-17 生产 Pilot 在权威库成功发布，Run 为 `15e5c164-bea6-4e66-ae2f-7be9489bbbd5`：5,506 个 PUBLISHED Unit、1,327,519 行、零 ERROR、339 条已定义 WARNING、零 HTTP Attempt。目标日 Candidate 与生产 Canonical 双向 `EXCEPT ALL` 为零；43 个产物文件哈希全部通过，`artifacts_complete=true`，产物保留于 `data/landing/15e5c164-bea6-4e66-ae2f-7be9489bbbd5/`。
7. 生产前后其他 Canonical、既有分钟及全部旧 Meta 记录双向比较均无差异；五项固定依赖与预检一致。生产 CLI 约 40.6 秒、峰值 RSS 约 3,074 MiB。Pilot 前备份为 `data/backups/warehouse-20261105T214917+0800-before-local-minute-20260417.duckdb`（SHA-256 `23ef6c66edeb4f6b103891e6260524d764482056078a7f960ccc98bfc6e693ec`），成功后备份为 `data/backups/warehouse-20261105T215235+0800-after-local-minute-20260417.duckdb`（SHA-256 `03fa8c07e7d7ae153d83309f8cf7ca38486915aadebe727cbd72c91ad291a894`），均通过只读和哈希核验。

8. 五日批次首日在 2026-04-10 发现五个源缺失 Unit 后暂停；用户确认这些是可统一处理的质量缺口，批次协调政策改为：Unit 级 ERROR 继续阻断所属 Unit，但保留 FAILED 并继续其余日期，批次末统一汇总；全局文件错误、执行异常和审计不一致仍停止。Adapter 验收和发布许可未放宽。
9. 五日批次最终完成：04-10 Run `87b82373-b0e0-4349-8eb0-c457853a8a77` 发布 5,497 Unit、失败 5；04-13 Run `336ef505-957f-4ccf-bdf8-0098367f9c63` 发布 5,502、失败 1；04-14/15/16 分别由 Run `8a5c50e2-ed63-4a81-8c1f-f6ed97be377e`、`012e45b2-45e3-4a72-8083-2fb7262f14ba`、`8948d125-3a8b-4d83-92d0-58d05fb62f68` 发布 5,502/5,502/5,505 Unit，均 SUCCESS。五日合计发布 27,508 Unit、6,632,240 行，保留 6 个失败股票日。
10. 六个统一缺口为：04-10 的 301696.SZ、603459.SH、688811.SH、920011.BJ、920181.BJ（均为日线正成交但 Parquet 完全无行，`local.daily_conflict`），以及 04-13 的 300391.SZ（退市日无日线、停牌和分钟证据，`local.empty_ambiguous`）。所有失败 Unit 均未发布，成功 Unit 的 Candidate/Canonical 对账为零差异，产物 manifest 哈希全部通过。
11. 整批其他 Canonical、既有分钟及旧 Meta 与批前恢复点双向比较均无差异；当前无 RUNNING Run。最终备份为 `data/backups/warehouse-20261106T081952+0800-after-local-minute-20260410-16.duckdb`（SHA-256 `e6a1573f33fa70623cda04bd69de4d6d8fa15e4afb71a6a09bf8f8393d84217c`），只读及哈希核验通过。04-10 初次 PARTIAL 恢复点 `data/backups/warehouse-20261105T231439+0800-after-local-minute-20260410-partial.duckdb` 也保留。

12. 扩大至 2026-03-12～04-09 的二十日生产批次全部完成，逐日全字段、Coverage、依赖和 manifest 验收通过。15 日 SUCCESS、5 日 PARTIAL，合计发布 109,859 Unit、26,486,967 行，保留 7 个失败股票日；无全局执行、审计或产物错误。
13. 二十日缺口为：03-20 的 300391.SZ（`suspend.timing_format`）、03-27 的 002231.SZ 和 03-31 的 603056.SH（均为 `local.empty_ambiguous`）、04-08/09 的 603459.SH 与 920011.BJ（均为源完全无行的 `local.daily_conflict`）。失败 Unit 均未发布，留待统一处理。其他 Canonical、既有分钟和旧 Meta 与批前恢复点比较无差异。
14. 二十日最终备份为 `data/backups/warehouse-20261106T083942+0800-after-local-minute-20260312-20260409.duckdb`（SHA-256 `1f49976be5a78d7ee32823f1511f7fca21b004d16dad8ffffb61cb30dfd5e8fc`），只读及哈希核验通过；备份保存 36,198,119 条分钟、121 个 Run、315,058 条 Coverage，无 RUNNING Run。

15. 2026-02-02～03-11 的二十二日生产批次全部 SUCCESS，包含 2 月完整开市日及 3 月此前剩余日期；发布 120,639 Unit、29,085,948 行，零失败 Unit。逐日全字段、Coverage、依赖和 manifest 均通过验收。
16. 二十二日批次其他 Canonical、既有分钟及旧 Meta 与批前恢复点比较无差异。最终备份为 `data/backups/warehouse-20261106T110024+0800-after-local-minute-20260202-20260311.duckdb`（SHA-256 `65877872df24c507bf3f97a170dfbe619a4f11ef84bd9d81495ad58f39291616`），保存 65,284,067 条分钟、143 个 Run、435,697 条 Coverage，无 RUNNING Run。

17. 2026-01-05～01-30 的二十日本地生产批次完成：17 日 SUCCESS、3 日 PARTIAL，发布 109,438 Unit、26,385,501 行；三个失败 Unit 为 920680.BJ/01-05、603056.SH/01-09、300379.SZ/01-22。逐日 Meta、依赖、Coverage、首末日全字段和旧数据保护验收通过。
18. 2026-11-06 使用经确认的银河 `--ts-code` 子集修复 04-08～04-10 的 9 个 `local.daily_conflict` Unit，全部成功并新增 2,177 行；独立库与生产逐字段一致，非目标旧事实和旧 Meta 不变。剩余 7 个失败 Unit 为 5 个退市日 `empty_ambiguous` 和 2 个 `suspend.timing_format`，未放宽契约。
19. 全部备份、Landing 和本次执行产物在最终验收后经用户授权删除；当前无恢复点，后续生产回填须重新创建批前备份。库内 Run、Publication、Coverage 和检查审计未删除。

20. 2026-11-11 与银河共用停牌解析扩展为多段、非补零和 `H:MM[:SS]` 完整分钟子集。独立库 2025-07-30/12-09 正式本地 Run 的目标 Unit 均发布 241 行，Candidate/Canonical 无差异，manifest 哈希通过；真实秒级样本只保留 10:06–10:14 槽位。全套 219 项测试中 218 通过、1 项既有样本缺失跳过，未写生产库。

21. 2026-11-13 修复 UNKNOWN Unit 的二次全局污染：本地 Adapter 对 Scope 已登记的 UNKNOWN Unit 不再额外生成无法归属的 `local.scope_conflict`；源行分类、停牌冲突语义和 `NETDISK_END_MINUTE_V1` 不变，UNKNOWN Unit 仍无 Candidate/Publication。回归模拟全日停牌、正成交日线及正量分钟冲突，正常 Unit 发布 241 行而冲突 Unit 保持 UNKNOWN；未识别代码和其他源错误仍阻断。全套 231 项测试中 230 通过、1 项既有样本缺失跳过，未写生产库或重跑真实冲突日。

当前完成的是 2026-01-05～04-17 的受控生产回填及既有锚点，不代表全历史覆盖；仍有 7 个失败 Unit，第 9 节剩余风险不因生产回填成功而消失。
