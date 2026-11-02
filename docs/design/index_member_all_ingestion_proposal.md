# index_member_all 接入方案

日期：2026-11-02。状态：**用户已确认完整快照方案及 out_date 包含当天；正式实现、回归与独立库验收完成。**

用户已明确要求将 2,006 对相邻归属的日期关系作为 out_date 包含当天的依据，写入提案并开始落地。Dataset 采用完整 Y+N、单 SNAPSHOT 整表替换，日常建议每个交易日盘后刷新；不修改共享接口或通用内核。

依据：[核心架构](core_design.md)、[当前实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[官方接口文档](https://tushare.pro/document/2?doc_id=335)、[2026-10-20 探查](../research/tushare-official-st-index-2026-10-20.md#7-index_member_all申万行业归属的当前历史快照)。

## 1. 数据对象与更新问题

接口提供股票的申万一、二、三级行业归属及纳入/剔除日期，共 11 字段。官方输入有 l1_code、l2_code、l3_code、ts_code、is_new；is_new 默认 Y，文档未提供按修改时间查询的增量游标或按业务日期提取快照的参数。文档声明单次最多 2,000 行，limit/offset 与 has_more 的行为须依据真实交付另行核验。

一行表示“供应商当前报告的一段股票行业归属记录”，完整表同时保存最新与历史归属。表中有历史日期，不代表项目保存了过去每次抓取时的内容，也不保证过去某日当时已经知道该归属。

分类调整可能同时改变多条记录：旧归属由 Y 变为 N、回填 out_date、新归属新增为 Y；供应商也可能更正历史日期、名称或删除错误记录。因此，按最大 in_date 追加或只刷新 Y 都不能完整同步这些变化。年度/临时是业务变化节奏，不直接决定数据库的替换粒度。

## 2. 本轮真实复核

复用既有只读脚本与正式 TushareClient，独立证据目录：

`data/probes/index-member-all-onboarding-20261102/`

原始信封与请求摘要保存在 requests/、summary.json；离线核对结果见 analysis.json。没有调用 Runner，没有写入 Canonical、Publication 或 Coverage。

| 项目 | 直接观察 |
|---|---|
| 最新 Y | 5,922 行，1,000 行分页为五个满页和一个 922 行尾页，随后额外空页确认 |
| 历史 N | 2,006 行，两个满页和一个 6 行尾页，随后额外空页确认 |
| 合计 | 7,928 行、11 个请求；两组分页均正常结束，11 份原始证据 SHA-256 与摘要一致 |
| 字段 | 11 字段齐全；除 out_date 外未见 NULL |
| 状态与日期 | Y 的 out_date 全 NULL，N 全非空；未见 out_date 早于 in_date |
| 候选键 | (l3_code, ts_code, in_date) 无重复；Y 中未见同一股票多行 |
| 异常早日期 | 9 条 in_date 早于 1990 年，保留源值，不能改成上市日 |
| 对比 2026-10-20 分页证据 | 原 7,924 行全部仍存在，新增 4 行 Y，in_date 为 2026-10-23 或 2026-10-28；本窗口未观察到旧行修订或分类迁移 |

本轮有新增记录，说明不能仅依赖一年一次的集中刷新。此观察不能证明这四行的新增原因、供应商更新延迟或全部历史完整性。历史相邻归属之间存在多种日期间隔，不能强制无缝连续；out_date 包含当天的项目口径及用户确认见第 5.1 节。

## 3. 已确认的获取与发布方式

**首版每次完整分页获取 Y 和 N，合并统一验收后，以一个 SNAPSHOT Unit 原子替换整张表。首次初始化与后续刷新使用同一条路径。**

1. 不传股票或行业过滤，分别显式请求 is_new=Y、is_new=N，显式请求全部 11 字段。
2. 专属分页先采用已实测的 limit=1000。满页即使 has_more=false 也续取，短页且明确 false 才确认终止；缺标记、空页 true、超页上限、重复整页或请求失败均阻断。正式终止政策沿用已有日集合 Adapter 的机制；本轮探针额外空页属于验证证据。
3. Raw 保留请求状态与 offset 标签；按返回字段名解码，并核对返回 is_new 与可信请求状态一致。不能把非法字段补 NULL，也不能去重或用当前股票名单裁剪历史。
4. prepare 集中清理文本、严格解析日期，输出完整 Candidate 和明确诊断。供应商行必须进入 Candidate 或关联明确问题。
5. 两组交付作为同一个 Unit 的共同证据。任一组失败、截断或有阻断性准备/业务错误，整快照不可发布，保留旧事实及成功版本。
6. Validator 给出显式许可后，复用 Publisher 单事务完成整表替换、Publication、Coverage 与成功 Unit 审计。相同内容重跑也沿用现有政策产生新版本。

不依赖当前 stock_basic 或 index_basic：本表自带完整行业路径，当前股票/指数快照不能用来删除供应商报告的历史股票与旧行业记录。全快照获取本身也不需要交易日历；更新任务的运行频率与业务依赖分开。

Dataset 与表名为 `index_member_all / canonical.index_member_all`；业务键为 `(l3_code, ts_code, in_date)`，不把可变的 is_new/out_date 放进键。8 个代码/名称字段及 is_new 保存字符串，in_date/out_date 保存 DATE，out_date 可空。

### 3.1 字段与验收规则

- 四个代码字段、in_date、is_new 必填；不限制代码前缀或按当前在市股票数做数量门禁。
- 四个名称字段允许 NULL，空白清理后缺失按快照汇总 WARNING，不使用最新名称替换历史名称。
- is_new 只接受 Y/N，Y 要求 out_date 为 NULL，N 要求非 NULL。请求状态不匹配为 source.range 准备 ERROR。
- in_date/out_date 必须是严格 ASCII 八位日期或合法 NULL。格式和历法失败独立记 prepare ERROR，不将坏日期转 NULL 放行；必填及条件必填由 Dataset 判断。
- 结束日包含当天，out_date>=in_date；单日归属合法。同一股票的归属闭区间重叠为 ERROR，累计最大前序结束日用于识别嵌套和重复当前区间；允许断档，不补事实。
- 跨页及跨状态重复业务键为 ERROR，不去重。最新 Y 与历史 N 各自必须非空；完整获取但任一组零行仍为业务 ERROR，不能清除旧快照。该非空门禁是针对已报告全历史集合的项目政策，不是由 has_more 推出的交付结论。
- in_date 早于 1990-01-01 保留源值并按快照汇总 WARNING，不改成股票上市日。

Schema、业务关系、非空及区间规则归 Dataset；字段解码、请求身份及日期转换归 Adapter。统一 Validator 仍是唯一发布许可入口。

## 4. 推荐更新策略

| 情况 | 建议动作 |
|---|---|
| 初始化 | 完整获取 Y+N，首次验收并发布 |
| 日常 | 每个交易日盘后安排一次完整刷新；这是项目建议频率，不是供应商时效保证 |
| 年度集中调整、已知临时调整 | 仍执行同一完整刷新；必要时在供应商更新后再次执行，无专属合并路径 |
| 供应商延迟或获取失败 | 保留上次成功版，下一次完整重试；当次失败不表示数据没有变化 |
| 历史更正 | 随下一次完整刷新同步，包括旧行改值及删除，无需猜测修订窗口 |

本轮完整数据不到 8,000 行，采用实测小页也只有 11 个请求。当前没有必要引入“平时 Y、每月 N”或“检测股票变化后局部修订”的双路径；前者会延迟历史更正，后者需要额外设计变更检测、证券范围与发布契约。若未来成本实际成为问题，再讨论按股票完整历史作为 Unit 的替代方案，不在本切片预建框架。

首版提供手动刷新 CLI，调度仍遵守项目现有边界：此稿提出运行频率，不新增调度器、公告监听、自动回补或跨 Dataset 失效传播。没有可靠变更信号时，不承诺即时发现偶发调整；发现时间受轮询间隔及供应商延迟共同影响。

## 5. 必须区分的时间与一致性边界

- in_date/out_date 是供应商报告的业务日期；Run 时间是本项目观察到这批数据的时间，不能相互替代。
- Canonical 保存“当前已发布的完整归属历史”。现有 Publication 只记录成功版本与范围，不保存旧行，不能据此还原过去某次抓取结果。
- 如果需求是复现“某个回测日期当时已经公开的分类”，需要额外确认可得时间与历史版本保存方案；当前接口及现有架构没有提供此保证。
- 已确认 out_date 为包含当天的最后有效日，不以抓取日补剔除日，也不对归属间隙前向填充；历史归属查询表达式见第 5.1 节，正式 Serving 不在本切片实现。
- 分页及 Y/N 是多次独立请求，没有供应商快照令牌。数据库原子提交保证发布整体性，不证明远端所有页来自同一时点。统一验收可阻断重复键、状态/日期冲突等已可识别问题，但无法发现所有并发变化造成的遗漏；遇到矛盾应整次重抓，不能在本地猜测补行。

### 5.1 out_date 包含当天：实测证据与用户确认

2026-11-02 根据用户追问，重新核对官方接口页：仍只说明 out_date 为“剔除日期”，没有明确端点是否包含当天。对本轮原始信封按股票、in_date 排序，共得到 2,006 对相邻已报告归属；全部旧 out_date 早于新 in_date，1,564 对相差一个自然日，没有端点相同或倒置的情况。

| 股票 | 旧行业及 out_date | 新行业及 in_date |
|---|---|---|
| 睿创微纳 688002.SH | 军工电子Ⅲ，2026-08-20 | 模拟芯片设计，2026-08-21 |
| 南钢股份 600282.SH | 板材，2026-06-30 | 特钢Ⅲ，2026-07-01 |

用户明确确认以“全部 2,006 对相邻归属中，旧 out_date 都早于新 in_date”为证据，采用 **out_date 包含当天**。本项目据此将归属定义为闭区间 `[in_date, out_date]`；out_date=NULL 表示没有报告结束日。此项不再待决，也不再将向供应商询问作为实现前置条件。

历史归属的已确认表达式为 `in_date <= d AND (out_date IS NULL OR d <= out_date)`。上述两只股票在旧 out_date 当天归旧行业，新 in_date 当天归新行业；正式链路测试验证该边界。

依据性质为“真实响应中的日期关系＋用户确认的项目解释”，没有改写为官方已明确保证。未来真实数据若出现与该契约冲突的情况，记录并报告，不能静默改成排除端点或修正源日期。

## 6. 实现与验证范围

首版落实完整 Y+N、单 SNAPSHOT 整表替换、当前权威历史归属以及包含 out_date 当天的闭区间，不声称 PIT。推荐的每日刷新频率不代表已新增自动调度。

正式改动：新增 datasets/index_member_all.py、同名 DDL、sources/tushare/index_member_all.py、必要链路测试，修改 CLI 显式装配与当前能力文档。复用 Client、Runner、Validator、Publisher、Meta 和现有 Protocol，没有新增重要共享抽象或修改共享接口。

必要测试包括：两状态完整分页；缺列/坏值与 COMPLETE 分离；任一状态失败保护整表；跨页及跨状态重复业务键；闭区间边界、重叠阻断与断档保留；合成旧归属 Y→N 与新 Y 同次替换；旧行更正和删除；整表零行/状态组异常；回滚、证据写入失败、重跑及 CLI。真实验证使用独立库，不将合成分类调整称为真实供应商事件。

实现前基线：`uv run --frozen python -m unittest discover -s tests -q` 共 159 项，158 通过、1 项因旧股票本地真实样本缺失跳过。

正式入口无需上游初始化，不接受日期、股票或行业过滤：

```bash
uv run --frozen python -m quant_data ingest index_member_all \
  --db data/probes/my-index-member/warehouse.duckdb --keep-artifacts
```

## 7. 实现验收记录

正式实现见 [Dataset](../../src/quant_data/datasets/index_member_all.py)、[DDL](../../src/quant_data/datasets/index_member_all.sql)、[Adapter](../../src/quant_data/sources/tushare/index_member_all.py)、[测试](../../tests/test_index_member_all.py) 和 CLI。新增 9 项链路测试通过，包括最新/历史分页、满页 false 和短页 true、任一状态的后页故障、缺列/坏类型/日期/请求状态、重复业务键、整表/单组零行、闭区间端点及重叠、历史断档和名称/早日期告警、迁移/修订/删除、事务回滚、证据失败、子集拒绝及 CLI 结局。真实信封重放 7,928 行，原始响应、Raw、Candidate 与 Canonical 逐字段核对通过。

独立验证库 `data/probes/index-member-all-ingestion-validation-20261102/warehouse.duckdb` 使用正式 CLI 连续执行两次，每次 9 个请求，发布 7,928 行（Y 5,922、N 2,006），均 SUCCESS、无 ERROR、产物完整。每次仅一项 early_date WARNING，计数为 9，源值保持不变。两次事实逐字段相同，成功 Publication 从 1 增至 2，未累加。正式请求在短页 false 时结束，比准备阶段额外空页探查少 2 次请求。

在 `experiments.duckdb` 独立副本完成三次明确无 HTTP 的实验：历史组完整零行和 N 第二页失败均使整快照 FAILED，旧 Canonical、Coverage 与 Publication 完全保持；合成 000001.SZ 分类迁移时旧 Y 更新为截至 2026-10-29 的 N，新 Y 从 2026-10-30 起生效，整快照成功发布 7,929 行。查询验证 10-29 当天属于旧行业、10-30 属于新行业。该事件是故障/边界实验，不是观察到的真实供应商调整，合成值只存在实验副本。

两次 live 运行及三次实验共核对 256 个产物清单文件 SHA-256，全部通过。正式 live 验证库最终仍只含真实源数据，没有写默认权威库。原始响应、阶段产物及可丢弃验证脚本保存在独立目录；机器摘要见 [index-member-all-ingestion-summary-2026-11-02.json](../research/index-member-all-ingestion-summary-2026-11-02.json)。

全套 `uv run --frozen python -m unittest discover -s tests -q` 共 168 项：167 通过，1 项因既有 stock_basic 本地样本缺失跳过；新增真实申万信封重放未跳过。环境检查 `scripts/check_environment.py` 通过，核心批量机制仍为固定 28 次 SQL。未修改共享 Protocol、内核、Meta 或依赖；每日自动调度、PIT、历史行镜像及正式 Serving 按首版边界未实现。
