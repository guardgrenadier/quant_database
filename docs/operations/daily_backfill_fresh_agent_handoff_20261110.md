# 日频回填 fresh agent 交接（2026-11-10）

**2026-11-11 状态更新：** 本交接描述的暂停点已恢复，2019剩余范围及2018→2013年度扫描、验收和滚动备份已完成；整体仍有获批延期缺口。最新状态与恢复点见[连续回填完成记录](daily_backfill_campaign_2013_complete_20261111.md)，以下内容保留为历史交接证据。

本文件是恢复入口，不替代实际Scope/Coverage/Publication、事实表或正式审计。编写时仅核对现有文件，未重新启动回填，也未对最新生产库重新完成交接验收。文中进度是已记录状态；执行前以真实数据库及运行任务为准。

## 目标、授权与边界

接手目录 `/home/xindingtian/code/quant_data`。继续2019剩余范围，然后2018→2013倒序逐年扫描七项日频：suspend_daily、stock_st、stock_daily、daily_basic、adj_factor、stk_limit、index_daily。

用户先前已授权连续逐年推进，不需每年再次确认；后来要求旧session停止，仅整理资料。新agent收到用户明确“按此交接继续回填”的指令后，才恢复其负责的PAUSED_BY_USER调度状态。不能将阅读文件本身当成执行授权。遇新的未批准异常、硬门禁冲突或外部占用仍停止并报告。

先读AGENTS.md。保留其他agent及此前的所有未提交改动，不reset、清理或覆盖；冻结架构、共享接口、业务语义、Dataset和表约束不改。不为继续回填顺手开发正式运维框架或协作式暂停功能，这两项只是后续建议。

不执行分钟、index_weight、Galaxy或其他数据集补取，不扩展2026。分钟修复文档只作为外部已完成变更的交接证据，不授权执行其中的分钟补数Prompt。

## 阅读顺序

1. AGENTS.md；涉及冻结设计时查docs/design/core_design.md。
2. docs/operations/daily_yearly_backfill_plan_20261106.md：授权、范围与最新暂停点，开头更新优先于下文历史状态。
3. docs/operations/daily_backfill_validation_tuning_20261110.md：当前验收策略、实际耗时、dbt选择修正及暂停恢复限制。
4. docs/operations/daily_backfill_2020_rebuilt_scan_20261109.md：2020最新阶段结果。
5. docs/operations/ingestion_performance_optimization_20261106.md：已完成的审计SQL/限频优化，不重复整套性能实验。
6. docs/operations/daily_backfill_rebuilt_resume_20261110.md：重建与历史停止背景，不将旧停止点当当前缺口。
7. docs/operations/stock_1min_2025_failure_repair.md最新记录：重点看暂停后新增的两个volume_amount修复及最新恢复点，只读衔接。

## 当前控制目录与暂停位置

当前控制根目录B：

`/home/xindingtian/code/quant_data/data/landing/daily-backfill-rebuilt-2020-2013-20261109T235516+0800`

先审阅B内resources.py、ops.py、run_year.py、run_all.py、deferred_quality.py、reconcile_external_minute.py、测试及dbt-profile/profiles.yml。脚本有特定年度、Run ID、旧备份和恢复前提，**不能直接重跑，也不能直接套用reconcile_external_minute.py**。

- B/batch-state.json与B/2019/state.json均为PAUSED_BY_USER，暂停时间2026-11-10 11:55:25+08。
- 2025、2024、2023日频全部完成。2026只做过1至10月，不扩展。
- 2022与2021阶段验收/备份完成，分别保留9、12个stock_daily延期日；不称为无缺口。
- 2020已完成年度扫描及阶段验收：目标194,157 Unit，成功194,129 Unit、3,960,160行，stock_daily延期27日，daily_basic延期1日，共28个Unit；状态PASSED_WITH_DEFERRED_GAPS / SCANNED_WITH_GAPS。
- 2019全年指数完成：193,492 Unit、141,523行；12、11、10、9月六项股票日频已通过月度验收。当前登记延期为stock_daily 2019-12-04。
- 2019年8月suspend_daily、stock_st、stock_daily、daily_basic均已成功且控制记录verified。adj_factor也已正式成功，控制条目尚未收敛，详见下节。8月stk_limit尚未执行；7月至1月尚未执行。
- 2018至2014尚未开始；2013-01-04有历史样本，先按实际Scope/Coverage识别并跳过，禁止整月重抓覆盖样本。

## 第一件事：衔接已成功但控制仍STARTING的批次

2019年8月adj_factor正式Run：

`682358cb-354e-488f-bea5-134f3bdf9c37`

正式CLI返回SUCCESS、22个成功Unit、84,854行、artifact_status=CLEANED、error=NULL。原CLI控制输出保存在：

`B/2019/201908-adj-factor-pause-cli.json`

对应B/2019/state.json最后条目仍是STARTING，没有run_id/verified。原因是用户要求暂停时，冻结本session调度器，让当前CLI正常完成后退出调度器；没有中断正式发布。**不能依据STARTING重抓这个成功范围。**

先只读核对Run参数、请求/检查、Coverage/Publication、事实行数、依赖版本和现有CLI输出，再按既有运维结构衔接该条目，保留暂停历史。B/2019/current-rss.txt记录379,332 KiB，可核对其归属；最低可用内存及完整监督耗时未完整落盘，必须如实注明未记录，不能补造、填0或伪称已通过资源观测。不得因缺失监督指标重抓已发布数据；若衔接与现有硬门禁冲突，明确报告最小处理方案，不擅自扩大验收例外。

衔接后接续8月stk_limit，再完成8月验收，之后7月至1月。只按实际未成功Coverage取数。

## 第二件事：重新核对暂停后的外部分钟变更及备份

2020阶段的滚动备份历史摘要是986ceefb8b51c191fc5a58d22b7889c21d37cb85ce1342992f193c1aed587667；它已经由另一分钟任务轮换，**不是当前恢复点预期值**。B/2019/baseline.json、request.json和B/minute-endpoint-baseline.json仍保留此前状态，不得直接覆盖或拿旧哈希检查新副本。

目前目录中观察到唯一副本：

`data/backups/warehouse-20261110T120735+0800-after-volume-amount-repair.duckdb`

分钟修复文档记载：16,948,547,328字节，SHA-256 `decbd0bd54694e7401ddbb5f0c0a12085535145235ece8a45a76a26d8224f729`。执行前重新核验它仍为最新，不能仅凭本文件或路径信任。相比此前交接后的分钟基线，两个新Run各发布241行，共482行：

- f3e2c86d-b827-47aa-bb97-0cc683e9bbff：600462.SH，2025-07-08。
- fbbbf2ab-f499-477a-b125-0c7ca765ae22：600462.SH，2025-07-11。

这两次运行由另一任务完成，不属于日频回填异常，也不授权日频agent再取分钟。核对正式审计、目标/非目标保护证据、分钟增量及已有日频是否保持一致。B/external-minute-handoff.json仅证明更早的264个Unit/64,152行交接，不能覆盖新482行。当前应验证新的2019保护检查点、备份及分钟区段起始锚点，保留原baseline与历史证据；不能只换一个预期哈希绕过差异检查。

若当前生产库与该唯一副本一致且校验合格，可复用并原子改回warehouse.latest.duckdb，无需复制全库。若已经有新的合法变更，先识别归属并核验再处理。已退休副本不可重验时明确引用此前审计及当前复核范围，不宣称做了不存在副本的精确对照。

## 已批准的数据质量策略

- 所有正式WARNING留痕监控后继续，不改原值、等级或正式审计。
- stock_daily：完整成功交付、代码/日期有效、无其他ERROR，且唯一局部ERROR为quote.values、源NULL字段仅来自pre_close/change/pct_chg，允许整日延期。
- daily_basic：完整成功分页、代码/日期有效、仅缺必填close、唯一ERROR为daily_basic.required，允许整日延期。其余15个可空指标沿用正式定义。
- 两类延期都保留整日FAILED、无事实/成功Publication，原PARTIAL/FAILED/ERROR及请求证据保留，不补值、不丢异常行后部分放行。其他ERROR及请求/分页/身份/依赖/审计异常停止。
- 行业未匹配已豁免：保留行情与NULL行业字段，年度输出代码、日期、行数和轻量CSV，不伪造归属；区分原有问题和本年度新增。
- 非豁免检查通过且成功集合与明确延期集合恰好覆盖Scope后，可按SCANNED_WITH_GAPS更新唯一滚动备份并跨年；这不代表历史全部覆盖完整。最终报告必须列出未修复缺口。

## 执行、资源和验收

- 六项股票日频按月串行、月份倒序；index_daily可年度批次，记录耗时、峰值RSS、请求数和审计规模。
- 使用正式CLI、Adapter、Dataset校验和Publisher；不直接写Canonical/Meta，不绕过审计。
- 保持依赖快照与版本钉住规则。原快照stock_basic5114、index_basic5115、index_member_all5116、bse_mapping5117供核对；若实际发生版本变化先报告，不静默更新。
- 账号10,000积分已由用户确认；其他七项路由0.13秒，index_daily内部sw_daily路由0.35秒，现有有限重试和Retry-After不变。不增加并行取数。
- 本轮检查与dbt内部DuckDB配置4线程、memory_limit=6GB；dbt任务threads=1。它不是进程RSS硬上限。不要恢复单线程/2GB，也不擅自继续增大资源。
- 分钟完整摘要仅在连续日频区段首尾检查，中途查结构/计数及历史Meta。合法外部分钟变更先衔接再建立新区段，最终完整摘要仍保留。禁止全分钟EXCEPT ALL或分钟Serving全表派生校验。
- 历史精确比较使用物化输入和双向EXCEPT ALL，减少逐月/32个hash桶重复扫描，保持NULL和重复行语义。按实际内存与计时检查执行情况，不放宽比较规则。
- 月度构建正式行业View并检查受影响月份；dbt build须显式--indirect-selection empty，避免自动附带全历史测试。年度再运行原有完整dbt测试及全历史Serving、Scope/Coverage/事实/依赖/延期证据验收。
- 继续使用audit-progress.json和逐阶段/逐表计时；定期报告月度进度、各环节耗时与异常，不能只看退出码。
- 生产keep-artifacts保持关闭，不传--keep-artifacts；成功暂存按正式流程清理，失败证据保留，不增加全表导出。
- 只保留一份滚动全库备份。年中不建月度副本/验证库。年度全部非豁免验收通过后，临时新副本校验、fsync、源/副本哈希及Canonical/Meta核对，再原子替换；新副本通过前保留旧副本。
- 核查真实数据库占用、进程、RUNNING Run、WAL，以及宿主机C/D磁盘空间，不用WSL内部df替代宿主机余量。不得清理其他任务遗留目录，包括历史warehouse.duckdb.tmp。
- 数据获取、SQL校验、dbt、备份严格串行，确认子进程退出/连接关闭后再进入下一步。外部占用先识别并请用户释放，不杀其他进程。

## 开始工作的顺序

先只读预检，说明当前事实、最后批次如何衔接、两笔外部分钟修复如何核对、准备使用哪份备份。审阅并最小适配暂停恢复控制流，不能直接删除PAUSED状态/标记而不核验。保持原始计划、已发布范围、审计和恢复证据。

用户明确授权按本文件继续后，在已确认范围内衔接控制状态并执行，不再逐年重复请求授权。遇不属于已批准范围的新异常或冻结规则冲突才停止报告。不要把协作式暂停或正式运维工具重构当成恢复前置条件。
