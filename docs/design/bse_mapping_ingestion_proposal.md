# bse_mapping ingestion 契约

日期：2026-10-27。状态：**正式实现及独立库真实验收完成**。

依据：[核心架构](core_design.md)、[当前实施契约](ingestion_v3.2_implementation.md)、[新 Dataset 接入清单](dataset_onboarding_checklist.md)及 [Tushare 官方 bse_mapping 文档](https://tushare.pro/document/2?doc_id=375)。复用现有 Runner、Validator、Publisher、Client 和审计，不改变共享接口或新增框架。

旧探查 JSON 当前本地缺失，本文不声称已重放真实响应，也不把旧探查结果当作本次验收证据。正式接口字段、限量及示例以官方文档为依据。

## 1. 业务事实、范围与 Schema

保存 Tushare 报告的北交所股票代码变更后新旧代码映射。一行表示一个旧代码到新代码的映射及上市日期，不是行情、每日状态或证券主数据扩展。

Dataset / 表为 `bse_mapping` / `canonical.bse_mapping`，完整保留官方四字段：

| 字段 | Canonical 类型 | 约束 |
|---|---|---|
| name | VARCHAR | 必填、非空白，保留供应商名称正文 |
| o_code | VARCHAR | 必填、非空白、以 `.BJ` 结尾；主键 |
| n_code | VARCHAR | 必填、非空白、以 `.BJ` 结尾；全快照唯一 |
| list_date | DATE | 必填；源 `YYYYMMDD` 严格解析 |

`o_code <> n_code`。新旧代码任一重复、名称或代码空白、缺日期、代码后缀错误或新旧相同，都阻断整个快照；不静默去重、不修正代码、不用其他表补齐。

无上游依赖、无日期或代码子集参数。Scope 和 Publication Unit 固定为一个 `SNAPSHOT`，`record_key=("o_code",)`。

## 2. 单次获取与完成性

Adapter 只调用一次 `bse_mapping`，请求参数为 `{}`，显式字段顺序为 `name,o_code,n_code,list_date`。不使用输入过滤、不分页、不追加辅助请求；每次尝试沿用 Client 的重试与即时脱敏证据留存。

响应按字段名重排。Raw 四列严格只接受字符串或 NULL，拒绝数值、布尔等隐式转字符串。缺列报告 `source.fields / prepare`，坏类型报告 `source.types / prepare`，均不能把缺字段补成可发布数据。

只有 `has_more` 明确为 `false` 且行数严格小于官方单次上限 1000 时，Unit 才是 `COMPLETE`。标记缺失、为 true 或触顶报告 `source.truncated / fetch` 并标记 `INCOMPLETE`，不以分页或截断行继续发布。`count` 不参与完整性判断。

请求异常报告 `source.request / fetch` 并标记 `FAILED`。缺列、坏类型或空响应不伪造请求失败：前两者在请求终止条件满足时仍为 COMPLETE，合法空响应还产生明确 Schema 的 Raw / Candidate。

## 3. 准备、验收与发布

`prepare(delivery, scope, con, pins)` 遵循当前共享 SourceAdapter 签名。三个文本字段去首尾空白并将空串转 NULL；`list_date` 分别核对 ASCII 八位格式和真实历法，再转为 DATE。坏格式与坏日期分别报告 `source.date_format`、`source.date_parse`，任一错误阻断整个快照，不将非法值静默变为 NULL。

Dataset 验收规则如下：

| 规则 | 级别 | 含义 |
|---|---|---|
| bse.required | ERROR | 四字段必填，名称和代码非空白 |
| bse.code_suffix | ERROR | 新旧代码均以 `.BJ` 结尾 |
| bse.code_changed | ERROR | 新旧代码不同 |
| bse.o_code_unique | ERROR | 旧代码在全快照唯一 |
| bse.n_code_unique | ERROR | 新代码在全快照唯一 |
| bse.nonempty | ERROR | 空快照不能发布或清旧 |

所有检查通过后，现有 Publisher 在同一事务内整表替换 SNAPSHOT，并更新 Publication 与 Coverage。相同内容重跑生成新版本而不累加；获取、准备、验收或发布失败保护旧 Canonical、成功 Publication 和 Coverage 指针。发布事务失败整体回滚。

## 4. 实现与验收边界

实现文件为 Dataset、DDL、Tushare Adapter、专项测试、CLI 显式装配和本文；共享接口按已确认的 `dependencies(params, con)` 与 `prepare(delivery, scope, con, pins)` 执行。

离线测试以假 Client 和临时 DuckDB 执行正式 Runner / Adapter / Dataset / Publisher，不访问真实 API。覆盖单次无过滤请求、显式字段、成功重跑与整表替换、缺列、坏类型、错日期、双键重复、空快照、触顶、请求失败和发布回滚，并核对失败时旧版本不变。

运行专项测试：

```bash
uv run --frozen python -m unittest tests.test_bse_mapping -v
```

## 5. 独立库真实验收

2026-10-27 通过正式 CLI 在同一独立库连续运行两次 `ingest bse_mapping`，均成功发布 248 条映射，`o_code` 与 `n_code` 分别唯一，重跑生成新 Publication 而不累加。两次产物均完整。独立库及证据位于 `data/probes/minute-ingestion-validation-20261027T013030/`，映射与后续上游产物的 220 个文件哈希均核对通过。

本轮不实现自动分页、历史事件推导、调度或旧探查 JSON 重放。若未来响应触及 1000 行，须重新确认获取契约，不能由本实现自动扩大或绕过完成性门禁。
