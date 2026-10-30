# stk_limit 接入提案

日期：2026-10-29。状态：**用户已确认提案及零下限修订，正式实现与独立库验收完成。**

用户已指定接口 `stk_limit`（每日涨跌停价格）。本文提出该 Dataset 的 Schema、Scope、发布粒度与源交付政策，不修改已冻结架构。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[官方接口文档](https://tushare.pro/document/2?doc_id=183)、[既有探查](../research/tushare-official-daily-index-2026-10-14.md)和[本轮机器摘要](../research/stk-limit-onboarding-summary-2026-10-29.json)。

## 1. 官方声明与实测证据

官方文档于本轮重新读取：范围包含 A/B 股和基金，单次最多 5,800 条，约每个交易日 9 点更新；可请求 `ts_code/trade_date/start_date/end_date`，输出七字段，其中 `pre_close/asset_type/exchange` 非默认。文档更新时点不作为数据终态保证；文档未列出的分页参数以本轮探查单独举证。

本轮通过现有正式 `TushareClient` 与证据写入器完成 10 次真实请求，没有调用 Runner、修改数据库或发布 Coverage。原始信封、每次尝试、摘要与 SHA-256 清单位于本地：

`data/probes/stk-limit-onboarding-20261029T213351571183/`

| 实测场景 | 结果与限制 |
|---|---|
| 2026-10-28 全市场、显式七字段 | 5,653 行，has_more=false；无缺列、NULL、重复键或上下限倒置；SSE 2,359 / SZSE 2,941 / BSE 353，asset_type 均为 STK；包含 B 股代码 |
| 同日分页 limit=3000 | offset=0 返回 3,000 行、true；offset=3000 返回 2,653 行、false；offset=6000 返回 0 行、false。拼接与单次响应排序后逐字段一致，无重复键 |
| 2010-01-04 全市场 | 1,671 行；pre_close 有 1 个 NULL、40 个 0，无负数；up_limit/down_limit 均为非空正数 |
| 2000-01-04 全市场 | 完整零行响应；不能由一个日期的空响应推断精确历史起点，也不能发布为已覆盖 |
| 300104.SZ，2017-04-17–28 停牌样本 | 10 行，pre_close 均为 0，上下限为 33.75 / 27.61；有涨跌停价格不表示当天可交易 |
| 688981.SH，2020-07-16–24 上市初期样本 | 7 行；前五个返回日期的 up_limit/down_limit 为 100000 / 0.01，随后为普通数值。保留此源表示，不由此单独建立全市场“无涨跌幅限制”判定规则 |
| 无效代码、2026-10-25 休市日 | 均为 has_more=false 的零行；不能把请求完成等同于业务验收通过 |

2026-10-14 的研究属于未冻结建议，不能继承其中“按当前股票快照重建历史应有证券集合”的假设；旧研究引用的 stk_limit 原始文件本轮未找到，不声称已重放旧文件。新信封作为可用验收样本。

## 2. 已确认的项目契约

| 项目 | 建议 |
|---|---|
| Dataset / 表 | `stk_limit` / `canonical.stk_limit`，与供应商接口同名；早期研究中的 `stk_limit_daily` 尚未落地，不涉及迁移 |
| 一行的含义 | 供应商报告的一个证券在一个业务日期的涨跌停价格及附带属性；保存供应商全市场集合，包含其报告的 B 股、基金与停牌证券 |
| Record Key | `(ts_code, trade_date)`；重复或冲突阻断所属日，不去重 |
| 输入 | 显式 start_date/end_date，ASCII YYYYMMDD 闭区间；拒绝未来日期，允许上海当日；首版不接受股票、交易所或资产类型子集 |
| 上游 | 仅依赖同库目标区间所有自然日的 `trade_calendar / SSE:YYYY-MM-DD` 成功版本；沿用项目共同日历口径，不自动运行上游 |
| Scope | 固定日历版本同快照解析：开市 IN_SCOPE，休市 OUT_OF_SCOPE，成功 Coverage 下缺实际日历事实为 UNKNOWN；保留用于休市排除的版本 |
| Publication Unit | 一个交易日的供应商全市场集合，Unit key 为 YYYY-MM-DD；请求分页不拆分业务 Unit |
| 写入 | 统一验收许可后按日完整替换；合格日按现有一次事务提交，一日失败保护旧版并允许其他合格日发布 |
| 零行 | 开市日全市场零行 ERROR，不清旧数据、不生成成功空版本；休市日不请求、不生成本 Dataset 的 Coverage |
| 个别证券无行 | 只表示本次供应商没有报告；不解释为停牌、无限制或退市，不补行，也不承诺历史所有证券无遗漏 |
| 历史范围 | 不把 2010 年设为推断出的供应商起点；允许显式历史请求，整日空响应明确失败。大规模回填前另确认目标历史区间及其覆盖证据 |
| 更新与修订 | 重跑替换所选完整日并产生新版本；保留历史修订能力，不保证当日终态、跨页同一时点或 PIT |

不依赖 `stock_basic`、`stock_daily`、`suspend_daily` 或 `stock_st`。当前股票快照不能充当历史完整名单；停牌不能作为排除依据；本表的 pre_close 不强制等于股票日线昨收，也不使用其他表补值。

### Schema

Canonical 字段顺序采用下表顺序，源数据按返回字段名重排。

| 字段 | 类型 | 可空与业务规则 |
|---|---|---|
| ts_code | VARCHAR / pl.String | 必填非空白，保留证券完整标识和前导零，不限定六位数字、不筛当前在市证券 |
| trade_date | DATE / pl.Date | 必填，严格匹配可信请求日；业务日期与抓取时间分离 |
| pre_close | DOUBLE / pl.Float64 | 可空，非空须有限且非负；0 与 NULL 分别原样保留，不相互转换 |
| up_limit | DOUBLE / pl.Float64 | 必填，有限且大于 0 |
| down_limit | DOUBLE / pl.Float64 | 必填，有限且大于等于 0；零下限原样保存并告警；up_limit >= down_limit |
| asset_type | VARCHAR / pl.String | 可空的源分类，开放字符串；不把单次样本 STK 固化为全集枚举 |
| exchange | VARCHAR / pl.String | 可空的源交易所属性，开放字符串；不凭代码后缀补值或改写 |

价格保留供应商证券原始报价口径及精度，不统一声称为人民币，不做汇率换算或人为保留两位小数。接口没有币种字段；全市场混合资产的跨证券价格比较需要额外证券/币种语义，不由本 Dataset 推导。

`100000 / 0.01`、`99999.99 / 0` 等合法有限源值原样保存。不按 5% / 10% / 20% / 30% 检查涨跌停幅度，不强制 pre_close 位于上下限内，不由极值或零下限新增 `is_unlimited`。本表提供源价格事实，不独立提供法律交易限制或可交易性判断。

## 3. 获取、准备与统一验收

采用 **Adapter 内专属逐日分页**，复用现有 Client，不引入通用分页框架或修改共享协议。探查实测 5,653 行已接近文档上限；分页成本为通常每天两次请求。

1. 每个开市日固定 `trade_date`，显式请求七字段，以 `limit=3000, offset=0` 开始；请求间隔沿用 1.3 秒，Client 沿用有限重试和逐次留证。
2. `has_more` 必须为显式布尔值。少于请求上限且 has_more=false 才终止；满页即使 false 也续取尾页确认。非空且 has_more=true 时继续，offset 按本页实际返回行数前移。
3. 缺少终止标记、零行但 has_more=true、返回超过请求上限、重复页或明显忽略分页参数均不能声明完整。请求失败/分页不完整阻断整个请求日，已取得部分行不得冒充完整日集合；继续处理其他日。
4. 所有页都保留证据和可信请求日/页标签。各页获取完成与字段/类型可用性分开记录：缺字段、坏类型属于准备诊断，不能把已完成的请求改写成业务成功或忽略问题。跨页重复业务键最终阻断该日。
5. 字符串严格解码，不把数字转成证券代码；数值只接受 JSON int/float/NULL，拒绝 bool、数值字符串。文本集中去首尾空白，空串转 NULL；Raw 原值保持可追溯。
6. 源日期须匹配可信请求日再转 DATE。错日/坏值的影响归到原请求日，不让错误行进入另一个合格日期。任何隔离均有明确诊断，不静默去重、丢行或补造事实。
7. Dataset 在标准 Candidate 上集中检查必填、数值、上下限顺序、键唯一和整日非空；通用 Validator 汇总 Scope、交付证据与准备诊断，独立给出发布许可。

ERROR：请求/分页不完整，缺字段，坏源类型或日期，错日，空键，非有限数，负 pre_close，非正上限、负下限，上下限倒置，重复键，开市日整日空集合。可定位时只阻断对应日，无法可信定位则按现有内核全局阻断。

WARNING：pre_close 为 NULL 或 0、down_limit 为 0，以及 asset_type/exchange 缺失，按日聚合计数并保留原值；这些不影响上下限事实发布。零下限规则名为 `stk_limit.down_limit_zero`，detail 的 `rows_by_unit` 保存逐日计数。不为正常 B 股、基金、停牌证券或合法极端上下限生成“超出股票数”类告警。

## 4. 实现范围与验收计划

实现涉及：

- 新增 `datasets/stk_limit.py` 和 `.sql`：Schema、依赖、Scope、Unit 与业务规则。
- 新增 `sources/tushare/stk_limit.py`：逐日专属分页、获取证据和集中标准化。
- 修改 `__main__.py`：显式装配与日期参数支持；入口为 `ingest stk_limit --start-date ... --end-date ...`。
- 新增 `tests/test_stk_limit.py`：必要正式链路与异常测试；同步 README 和当前实施文档的 Dataset 列表。

依赖/范围可复用 `datasets.daily` 已有日历函数，前提是保持其现有契约；源七字段使用本 Adapter 的明确类型定义。Runner、Validator、Publisher、Meta 与 Dataset/SourceAdapter Protocol 预计无需变更；不新增基类、注册器、控制模型或第三方依赖。

必要验证：

1. 日历缺失时零供应商请求；休市排除与依赖版本变化保护旧版。
2. 多页完整拼接、满页 false 后尾页确认、第二页失败/缺标记/重复页/跨页重复键均阻断该日；另一合格日仍可发布。
3. 七字段 Schema、字段重排、NULL 与 0、极端上下限、错日归属、非法数值和整日零行的正确处理。
4. 新真实信封重放，逐字段比较 Raw/Candidate/Canonical；不能将测试替身作为真实获取验收。
5. 独立库正式 CLI 小区间及重跑，核对原子替换、Coverage/Publication、失败保护与证据哈希；不写默认权威库。
6. 运行新增必要测试及受影响回归，检查 CLI 的 SUCCESS/PARTIAL/FAILED 和产物状态分离。

本轮基线检查已执行：`uv run --frozen python -m unittest tests.test_ingestion tests.test_daily tests.test_bse_mapping -q`，47 项中 46 通过、1 项因旧 stock_basic 本地真实样本缺失跳过。这是既有链路的回归结果，不是新 Dataset 验收。

首版不实现 A 股筛选、证券历史全集重建、无限制标志、涨停/跌停状态、自动调度、全历史下载、PIT 或 Serving 模型。历史回填区间尚未由用户指定；正式开发与独立库小样本验收不要求先确定全历史任务。

## 5. 用户确认与证据边界

用户在本会话明确回复“确认，开始实现”，上述 **全市场七字段、按交易日完整替换、整日零行阻断、保留 pre_close 的 NULL/0 与极端上下限、专属分页** 已作为当前 Dataset 契约。首版边界按本提案执行。

本轮证明了一个近期交易日的分页与单次响应等价，没有证明供应商跨页快照一致、全部历史覆盖或任一日期终态。精确历史起点、其他板块/时期的特殊价格表示及混合资产币种映射未穷尽验证；未来新样本若冲突于确认后的契约，应报告并讨论，不能通过放宽检查静默改变语义。

正式代码见 [Dataset](../../src/quant_data/datasets/stk_limit.py)、[DDL](../../src/quant_data/datasets/stk_limit.sql)、[Adapter](../../src/quant_data/sources/tushare/stk_limit.py) 和 [测试](../../tests/test_stk_limit.py)。现有共享接口、内核、Meta 及依赖均未修改；只复用已有日历函数。

## 6. 验收记录

新增 9 项正式链路测试通过，覆盖逐日分页及终止、满页 false 的空尾页、短页 true 的 offset 前移、后续页失败/缺标记/超页/重复页、跨页重复键、源字段和日期错误归因、NULL/0 和极端价格保留、开放分类、非空和数值门禁、依赖与休市日版本复核、发布回滚、证据失败、CLI 三类结局与七字段真实样本重放。

2026-10-28 的 5,653 行与 2010-01-04 的 1,671 行真实价格信封均经正式 Adapter/Runner 发布到测试临时库，Raw/Candidate/Canonical 逐字段核对通过。该项是离线重放，日历为明确测试输入，不作为实时供应商验收。

初版全套回归 `uv run --frozen python -m unittest discover -s tests -q`：141 项中 140 通过、1 项旧 stock_basic 本地样本缺失跳过；环境检查 `scripts/check_environment.py` 通过。核心批量检查仍为固定 28 次 SQL。零下限修订后的验证另列于下文。

首次真实 CLI 在独立库 `data/probes/stk-limit-validation-20261029/warehouse.duckdb` 获取 2026-10-23–26（含两个休市日）：日历四个 Unit 成功；stk_limit 四个请求均成功，但 10-23 有一行 `920285.BJ / pre_close=20.3 / up_limit=99999.99 / down_limit=0`，触发已确认的严格正数规则。该次 Run 为 PARTIAL，10-23 未发布，10-26 成功发布 5,654 行；没有因原始响应非空就错误放行。91 个产物文件哈希核对通过，原始证据与结论见该目录 `contract-conflict.json`。该次运行不作为完整成功验收。

用户随后明确选择“允许零下限并告警（建议）”。据此将 Dataset 与 DDL 同步修订为 up_limit>0、down_limit>=0；0 原样保留并按日聚合告警，不解释为无限制。负下限、零上限、NULL 上下限及非有限数仍阻断。本节首次 PARTIAL 是修订前的历史验收记录，不代表当前契约。

不依赖该决策的正数样本验收已完成：在另一新库 `data/probes/stk-limit-validation-positive-20261029/warehouse.duckdb`，2026-10-28 的日历由真实 CLI 发布，stk_limit 两次真实 CLI 均成功，每次两页共 5,653 行、无 ERROR，生成新版本且重跑事实逐字段相同。原始信封、Raw/Candidate/Canonical 核对通过，134 个成功运行产物清单文件哈希通过。随后明确标注为无 HTTP 的完整零行注入被阻断，旧事实、Coverage 和 Publication 保持不变。

本轮机器验收摘要见 [stk-limit-ingestion-summary-2026-10-29.json](../research/stk-limit-ingestion-summary-2026-10-29.json)。各数据库均为独立验证库，未修改默认权威库。修订前的验证库保留原样，新契约使用新库验收，不自动迁移旧实验表的约束。

最终修订验收：`uv run --frozen python -m unittest tests.test_stk_limit -v` 的 9 项全部通过。更新后的用例验证零下限成功入库并仅对所属日计数告警，同时验证负下限、零上限和 NULL 上下限仍阻断。

新独立库 `data/probes/stk-limit-validation-final-20261029/warehouse.duckdb` 经真实 CLI 发布 2026-10-23–26 的四个自然日日历，再执行两次 stk_limit：每次四个分页请求，两个交易日各 5,654 行，合计 **11,308 行**，均 SUCCESS、无 ERROR、产物完整。每次仅一条 `stk_limit.down_limit_zero` WARNING，归属 2026-10-23，计数为 1；`920285.BJ` 的 `99999.99 / 0` 原样入库。重跑逐字段一致并生成新成功版本，源响应、Raw、Candidate、Canonical 逐字段核对通过，138 个成功运行产物清单文件哈希通过。随后明确无 HTTP 的完整零行注入仍被阻断，保护两个交易日的旧事实、Coverage 和 Publication。

本轮没有待决契约或未完成的必要验收。未扩展为全历史下载、自动调度、无限制标志或共享接口改造；供应商覆盖、跨页快照一致性与当日终态仍按前述边界解释。
