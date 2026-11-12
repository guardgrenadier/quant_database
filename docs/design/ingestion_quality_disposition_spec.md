# Ingestion 质量处置与可追溯发布 Spec

日期：2026-11-12。状态：**本次会话已完成设计，尚未实现、验收或用于生产发布。**

依据：[核心架构](core_design.md)、[技术设计与实现边界](ingestion_v3.2_implementation.md)、[Meta 审计 Spec](meta_audit_lifecycle_spec.md)、[股票日线契约](stock_daily_ingestion_proposal.md)、[每日指标契约](daily_basic_ingestion_proposal.md)、[涨跌停价格契约](stk_limit_ingestion_proposal.md)及 2026-11-12 日频失败 Unit 交接与源端复查。

本 Spec 定义新增的逻辑能力，不声称当前代码已经具备这些能力。正式实施前仍须完成对应 Dataset 契约、DDL、Meta Schema、测试和独立库验收；不得仅凭本文直接修改生产事实或历史 Run 状态。

## 1. 背景与问题

当前 ingestion 已较完整地覆盖获取、标准化、统一验收、原子发布和运行审计。它能够发现供应商交付不完整、源格式错误和业务质量异常，并按 Unit 阻断不合格发布；Client、Adapter 和运维脚本也提供有限重试、分页、故障隔离、定向补缺及延期登记。

但现有质量处理主要只有两种结果：

- `ERROR`：阻断受影响 Unit；
- `WARNING`：保留诊断并允许原 Candidate 发布。

这使部分交付完整、身份可信、仅少数字段不可用的 Unit 永远无法进入 Canonical，并进一步阻断依赖这些 Coverage 的下游 Dataset。延期和重试只能隔离或重新观察故障，不能构成质量问题的最终处置。

2026-11-12 的当前 211 个失败／延期 Unit 集中体现了这一缺口：`stock_daily` 206 日、`daily_basic` 1 日、`stk_limit` 4 日。它们需要明确、可审计的质量处置，而不是无限期保留 FAILED，也不能通过静默删行、补值或把任意 ERROR 改名为 WARNING 来放行。

## 2. 目标

本次新增以下逻辑能力：

1. 在原始值仍可见时发现精确质量问题。
2. 对已批准的精确问题签名执行受控处置。
3. 保留发现、处置动作、受影响记录及原值／结果之间的追溯关系。
4. 对处置后的最终 Candidate 重新执行完整验收，Validator 仍是唯一发布许可来源。
5. 合格 Unit 正常形成 Publication 和 PUBLISHED Coverage，残余质量问题以 WARNING 和处置档案表达。
6. 使处置记录与 Publication 同寿命，能够解释当前及历史发布版本实际采用过的质量政策。

成功发布不再表示“源数据绝对完美”，而表示：**交付满足完整性契约，所有发现的问题均已按明确政策处置或接受，最终 Candidate 通过完整验收，并且残余质量语义可追溯。**

## 3. 非目标与边界

本轮不实现：

- validate 后的通用 ERROR handler、自动修复循环或规则 DSL；
- 任意异常按比例、行数或“小问题时放行”的阈值政策；
- 静默删行、去重、补零、前向继承、跨日推算或未经批准的跨源补值；
- 用 Serving/View 掩盖未发布 Unit 或改写 Canonical 源事实语义；
- 新的 Run、Unit、Coverage 状态，包括 `REPAIRED` 或 `PUBLISHED_WITH_DEFECTS`；
- 通用跨 Run 自动重试、故障工单、调度器或 Agent 生产决策；
- 对历史 FAILED Run 的原地改写；
- 完整原始响应随 Publication 永久保留的新归档体系。

Agent 可以协助研究未知异常和提出新政策，但不是正式运行时的处置决策者，不能临时决定修值或直接改变发布许可。

## 4. 失败分类与处置前提

数据面的失败分为两类：

| 类别 | 含义 | 处理原则 |
|---|---|---|
| 供应商交付完整性 | 请求失败、截断、分页不完整、交付状态未知等 | Client 有限重试、Adapter 专属获取恢复、后续定向重跑；不得靠质量置 NULL 绕过 |
| 数据质量 | 已完整交付记录中的字段缺失、不可用值或业务关系异常 | 按精确问题签名接受或转换，再完整复验 |

数据库、事务、证据写入、进程中断等属于系统执行失败，不混入供应商质量处置。

质量处置必须满足以下共同前提：

1. 对应 Unit 的交付为 `COMPLETE`；不得对 FAILED、INCOMPLETE 或未知交付进行质量放行。
2. 请求和记录身份可信，业务日期、证券代码及 Unit 归属明确。
3. 发现发生在原值转换前，问题签名完整匹配获批政策。
4. Raw 保持不可变；处置只作用于 Candidate。
5. 每条受影响源记录及其字段变化均可解释，不允许静默丢弃。
6. 处置不妨碍其他规则执行；最终仍存在任何 ERROR 时，Unit 不发布。

## 5. 逻辑流程与职责

### 5.1 流程

```text
供应商获取
  → Client 有限重试 / Adapter 专属交付恢复
  → 确认 COMPLETE 与可信身份
  → Raw 原样保存
  → prepare / assess
       发现原始问题
       → 无获批政策：保留 ERROR，不处置
       → 有获批政策：记录发现和处置，生成处置后 Candidate
  → validate
       对最终 Candidate 执行完整验收并产生唯一发布许可
  → publish
       原子写入 Canonical、Coverage、Publication 和实际生效的处置档案
```

### 5.2 不增加物理处置阶段

“处置”是必须明确的逻辑职责，但当前三个政策均可由现有 `prepare → validate → publish` 承载，不新增 Runner 物理阶段。

`SourceAdapter.prepare()` 现有返回值为 `(Candidate, checks)`，可同时承载标准化后的 Candidate 以及发现／处置诊断。prepare 不得静默修值；每项实际处置都必须形成对应审计内容。

Dataset 契约负责定义什么问题可接受、最终字段可空性和值域；Adapter 只执行已批准的确定性源值标准化，不自行取得发布决定权。

### 5.3 Validator 始终是最终确认职责

禁止采用以下流程：

```text
validate → 修改 Candidate → 直接 publish
```

如果未来确实需要在完整检查后实施更复杂的修复，则必须形成新的前置 assess／remediate 设计，并在处置后重新完整执行 validate。当前不预建该机制。

最终 validate 不再发现已被修复的原始值是正常结果，不代表问题被隐藏；原始问题由 prepare 诊断、Raw 证据和 Publication 处置档案共同保存。

## 6. 处置模型

### 6.1 精确问题签名

处置政策不能与宽泛规则名直接一一对应。可处置问题至少由以下信息共同确定：

- Dataset、原问题规则；
- Unit 和受影响记录业务键；
- 受影响字段；
- 原始值满足的精确条件；
- 完整交付、可信身份及其他政策前提。

例如，`quote.values`、`daily_basic.required`、`stk_limit.sign` 本身都不足以唯一确定处置；还必须分别核对 NULL 字段集合、键／日期有效性或三价格同时为零的条件。

### 6.2 首批动作

| 动作 | 语义 | 要求 |
|---|---|---|
| `ACCEPT_NULL` | 接受供应商合法缺失，不改变源值 | Candidate 保留 NULL，记录受影响记录和字段 |
| `NULLIFY_INVALID_GROUP` | 将精确匹配政策的一组不可用价格统一表达为未知 | Raw 保留原值，Candidate 同组字段置 NULL，记录 before/after |

`NULLIFY` 后的 NULL 只表示“未知／不可用”，不自动表示无涨跌停、零涨跌、停牌或其他业务状态。

本轮不实现行删除、任意单字段修值、推算补值或替代来源修补。

## 7. 首批 Dataset 政策

| Dataset | 精确政策 | 最终 Candidate / Canonical | 继续保留的边界 |
|---|---|---|---|
| `stock_daily` | 接受 `pre_close / change / pct_chg` 的源 NULL | 保留完整源行，这三列允许 NULL | 其他六个行情数值仍必填；键、日期、OHLC、有限数、量额和唯一键规则不变 |
| `daily_basic` | 接受 `close` 的源 NULL | 保留完整源行，close 允许 NULL | 非 NULL close 仍须有限且为正；其他指标及唯一键规则不变 |
| `stk_limit` | 仅对 `pre_close=up_limit=down_limit=0` 的记录执行整组置 NULL | 三字段同时为 NULL；Raw 仍保存三个零 | 原始 NULL、负数、非有限数、部分零值及其他不在政策内的异常仍按契约阻断 |

实施时需同步修改 Dataset 验收和 Canonical DDL。对 `stk_limit`，最终字段可空不意味着供应商原始 NULL 自动合法；prepare 必须在转换前区分原始 NULL 与本次政策产生的 NULL。

`stock_daily` 政策按字段语义定义，不硬编码 `.BJ`。当前异常集中于北交所是样本事实，不是特殊放行分支。

这些都是完整源行集合的正式发布，不是删行后的部分发布。失败历史 Run 保持原状；新政策实现后通过新 Run 重跑目标 Unit 并形成新 Publication。

## 8. 下游与 Serving 语义

Coverage 的 `PUBLISHED` 继续表示最终 Candidate 满足修订后的 Dataset 契约，不增加质量状态。残余质量情况通过 WARNING、Canonical 中的合法 NULL 和 Publication 处置档案表达。

下游依赖分为两层：

1. Unit 是否已有成功 Coverage；
2. 下游所需的字段是否可用以及 NULL 如何影响其自身产物。

下游必须显式消费 NULL，不得把缺失默认解释为零。当前分钟参考价链路已有相应基础：当日内 TRADED close 和 `stock_daily.pre_close` 均不可用时，`reference_price` 为 NULL；分钟 Dataset 对参考价不可用时的 NO_TRADE 行也已有 NULL 验收语义。正式实施仍须补充对应回归测试。

Serving/View 可继续承担参考价等消费派生，但不得用于把 FAILED Unit 伪装成成功、绕过 Coverage 依赖或使 Canonical 与正式消费口径不一致。

## 9. Meta 语义分工

| 表 | 本次设计中的职责 |
|---|---|
| `meta.run` | 保持现有运行结局；本轮不要求增加处置明细或新的 Run 状态 |
| `meta.run_unit` | 保持最终 FAILED/PUBLISHED 等结局，通过 `check_ids` 和 `publication_id` 连接诊断与成功版本 |
| `meta.validation_check` | 保存本次发现及最终检查；已批准且不再阻断的问题为 WARNING，未知或残余问题为 ERROR |
| `meta.request_attempt` | 保持原始请求、Attempt 和证据身份，不写处置动作 |
| `meta.publication` | 长期保存本次成功发布实际生效的处置档案 |
| `meta.coverage` | 继续只保存当前成功 Publication 指针，不写质量详情 |
| `meta.run_dependency` | 不变 |

业务校验失败的 Unit 原因仍通过 `run_unit.check_ids → validation_check` 获取；`run_unit.error` 不是所有业务失败的完整原因。依赖解析前失败等 Run 级错误仍可能不存在逐 Unit 明细。

## 10. Meta 最小扩展

### 10.1 只扩展 Publication

逻辑上为 `meta.publication` 增加：

```text
dispositions JSON NOT NULL DEFAULT '[]'
```

不新增 Meta 表。每个成功 Unit 的 Publication 保存零项或多项实际生效的处置；无处置时为 `[]`。该字段随 Publication 长期保留，并进入现有 Publication 审计快照。

处置档案必须与 Canonical、Coverage、Publication 在同一发布事务写入，不允许发布提交后再异步补记。实际写入内容应与 prepare 产生的处置诊断来自同一份事实，避免两处计数或记录集合不一致。

### 10.2 处置记录内容

每项 disposition 至少包含：

- `policy`、`policy_version`、`action`；
- `source_rule`，以及可选的本次 `source_check_id`；
- 全部受影响 `records`，而非仅示例或总计数；
- 每条记录的业务键、受影响字段、`before`、`after`；
- 来源 `request_id`、`attempt`、`evidence_sha256`。

示例：

```json
{
  "policy": "stk_limit.zero_price_group_to_null",
  "policy_version": "v1",
  "action": "NULLIFY_INVALID_GROUP",
  "source_rule": "stk_limit.sign",
  "source_check_id": 0,
  "records": [
    {
      "key": {"ts_code": "000509.SZ", "trade_date": "2014-01-09"},
      "fields": ["pre_close", "up_limit", "down_limit"],
      "before": {"pre_close": 0, "up_limit": 0, "down_limit": 0},
      "after": {"pre_close": null, "up_limit": null, "down_limit": null},
      "request_id": "stk-limit-2014-01-09-0",
      "attempt": 1,
      "evidence_sha256": "..."
    }
  ]
}
```

对于 `ACCEPT_NULL`，`before` 和 `after` 均为 NULL，用于明确表达“源缺失被接受”，而不是发生了填值。

这些必要审计值只用于解释处置，不替代 Canonical 业务事实，也不构成新的业务查询入口。`source_check_id` 只用于回查本次 Run；由于检查和 Attempt 明细存在归档／过期生命周期，长期追溯所需的问题、记录和来源摘要必须直接包含在 Publication 处置档案中。

### 10.3 失败、重跑与修订

- 最终 FAILED 的 Unit 没有 Publication，因此没有 `publication.dispositions`；发现及尝试过的处置保留在本次检查明细和失败证据中。
- 后续成功重跑生成新 Run 和新 Publication，保存该次实际生效的处置；旧 FAILED Run 不改写。
- 后续源值恢复、无需处置时，新 Publication 使用 `[]`；旧 Publication 的处置档案仍保留。
- Coverage 切换后，可通过当前 Publication 查询当前版本处置，也可通过历史 Publication 解释历史版本。

### 10.4 基础留档与完整证据的边界

Publication 处置档案保存全部受影响记录的 before/after、请求身份和证据哈希后，本轮可以沿用成功 Run 的现有 Landing 清理政策，不因基础处置留档而永久保存每次完整原始响应。

这保证长期解释“什么记录按什么政策从什么值变为什么值”，但不承诺完整原始响应永久可重放。若未来需要完整源证据与 Publication 同寿命，应另行设计证据归档，不在本轮隐式扩大保留范围。

当前 JSON 方案面向少量、精确定位的记录级处置。未来若出现大规模处置或频繁按股票／政策检索需求，再评审是否规范化成独立长期表；本轮不预建。

## 11. 一致性与安全不变量

1. Raw 不可变，任何 before/after 不得反写原始请求证据。
2. 只有完整交付且精确匹配获批政策的质量问题才可处置。
3. 未发现政策的问题保持 ERROR，不因新增了处置能力而自动降级。
4. prepare 中实际发生的每项值变化必须有对应处置记录；不得只记录 WARNING 而遗漏行动。
5. 最终 Validator 对全部最终 Candidate 和全部规则形成唯一发布许可。
6. 处置后仍有其他 ERROR 时，保护旧事实和旧 Coverage，不生成成功 Publication。
7. Publication 处置档案、Canonical 和 Coverage 原子提交；事务失败整体回滚。
8. 同一 Run 中其他无 ERROR 的合格 Unit 仍可发布，失败 Unit 不造成无关范围连带失败。
9. 不以处置记录数量代替受影响记录集合；全部受影响业务键必须可核对。
10. 处置政策版本变化不得改写历史 Publication，只影响后续新运行。

## 12. 验收要求

### 12.1 机制测试

至少覆盖：

- 精确问题命中后接受／置 NULL，Raw 不变，记录键、字段、before/after 和来源完整；
- 宽泛规则相同但问题签名不匹配时仍 ERROR，不发生转换；
- 原始 NULL 与政策生成 NULL 的区分；
- 处置后其他规则仍失败时保护旧事实，无成功 Publication；
- 成功发布时处置档案与 Canonical、Coverage 同事务提交及回滚；
- 多 Unit 中一个失败不影响其他合格 Unit；
- 重跑无需处置时新 Publication 为 `[]`，旧 Publication 和旧 FAILED Run 保持不变；
- Meta 明细归档／过期后，Publication 仍能独立提供基础处置追溯；
- 成功 Run 按现有政策清理 Landing 后，基础 before/after 处置档案仍完整。

### 12.2 首批真实证据与下游验收

在独立库重放本轮保存的真实响应，目标覆盖 206 个 `stock_daily`、1 个 `daily_basic` 和 4 个 `stk_limit` 当前失败 Unit。逐 Unit 核对源响应、Raw、发现记录、处置记录、最终 Candidate、Canonical、Publication 和 Coverage。

若任何 Unit 仍有政策外 ERROR，继续保持失败，不得为了满足预期数量强行发布。补充股票分钟参考价及其他已识别消费者的 NULL 回归测试，确认 NULL 不被解释为零或编造价格。

完成独立库验收、正式 Schema 兼容性检查和必要回归后，才可提出生产定向重跑；正式写库前另按运维要求建立并校验恢复点。

## 13. 本次设计新增能力总结

本次会话新增的是一套**逻辑上的质量处置与版本化留档能力**：

- 从“发现后只能阻断或忽略”扩展为“发现后按明确政策接受或置 NULL”；
- 从“validate 前静默转换的风险”明确为“prepare 前置发现与受控处置、validate 最终完整确认”；
- 从“Unit 永远 FAILED、下游无法推进”扩展为“合格处置后的 Unit 正常 PUBLISHED，下游显式消费 NULL”；
- 从“只有规则级失败原因”扩展为“记录键、字段、原值、结果、政策和来源的版本化追溯”；
- 以 `publication.dispositions` 一个长期 JSON 字段完成最小 Meta 扩展，不增加物理处置阶段、通用修复框架或新的 Coverage 状态。

截至本 Spec 编写时，上述内容均为已确认设计，不代表代码、生产 Schema、历史失败 Unit 或下游数据已经完成变更。
