# 重建后日频续跑与新硬错误停止记录（2026-11-10）

状态：**STOPPED，未完成2020→2013。** 用户授权正式WARNING全部监控后继续；ERROR仅允许stock_daily的pre_close/change/pct_chg源NULL整日延期，其余仍停止。本次已从重建后的实际生产库重建控制基线并开始取数，因daily_basic必填close的新ERROR停止。2019→2013尚未启动，原逐年连续授权和已批准例外仍保留。

## 新生产基线、存储与保护

权威库仍为data/warehouse.duckdb。2026-11-09重建后持续增长表不物化ART，分钟为十列、bar_time=TIME、reference_price由View派生。初次只读核实409,093,564行分钟，2013/2025/2026分别406,684 / 317,421,819 / 91,265,061行；2025分钟修复的旧成功/失败Meta均纳入新基线保护，未重抓或改动分钟。

原始唯一恢复点为warehouse-20261109T231812+0800-after-2025-failure-repair.duckdb，与生产库全文件SHA-256一致：b819a1c9a5b5c6911cd45348343e9b2d245668153cad4e89c1b45d21fd180e3b，16,899,649,536字节，核验52.139秒。没有直接套用11月7日的物理哈希、分钟字段布局或控制状态；旧Run ID仅在新Meta实际存在、Coverage和证据重新核实后引用。

全部26个已批准延期日、27条源记录及其原始证据重新验证，未用旧失败掩盖更新失败；1,837条日频正式WARNING重建监控。2023～2025已完成范围、2022/2021已扫描且仍有缺口的状态保留。遗留data/warehouse.duckdb.tmp约44GiB没有清理。

## 等价Serving实现调整

只读执行计划显示行业View的out_date IS NULL OR trade_date<=out_date条件退化为BLOCKWISE_NL_JOIN。将其内部SQL表达式替换为trade_date<=coalesce(out_date,DATE 'infinity')后使用HASH_JOIN：NULL仍表示无上界，纳入/剔除日仍包含当天，未匹配仍保留NULL。

- 全部真实候选日期对两个结束日谓词的布尔差异为零；另验证无上界、结束日当天、结束日后及纳入日前边界。
- 调整前后21张Canonical/Meta计数与集合摘要完全一致，分钟View定义未变。
- 正式dbt仅选择stock_daily_with_sw_industry及其测试：粒度PASS、行业未匹配WARN 42,191，与调整前一致，0 ERROR。
- 11项分片/Serving/告警测试、10项延期边界测试及缺口规划自检通过；未重复审计关联SQL性能实验，core.audit未改。
- 只改dbt/models/stock_daily_with_sw_industry.sql的等价结束日表达式，没有改字段/共享接口/行业语义、Dataset校验或表约束。

两个未进入取数的只读基线尝试已保留于本控制目录的preflight-attempt-*及对应状态/停止文件；只结束本轮自有只读进程，没有结束外部进程。完成等价实现核验后生成临时副本，源/副本哈希、fsync与全部Canonical/Meta计数验证通过，再原子轮换唯一warehouse.latest.duckdb并退休明确复用的旧恢复点。初次View等价调整前后完整核验证据为view-rewrite-before.json、view-rewrite-verified.json。

## keep-artifacts与资源边界

所有生产ingestion命令均**不传--keep-artifacts**，成功Landing按正式流程清理，PARTIAL仅保留必要请求证据，不追加Raw/Candidate/全Canonical/整套Meta导出。SQL检查和本轮独立dbt profile使用单线程、2GB内存与本控制目录专用临时目录；日频逐月、Meta确定性hash分片精确比较，禁止全分钟EXCEPT ALL，分钟只做新基线计数/流式摘要保护。日频dbt不选择分钟View构建或全表参考价查询。

宿主机C/D盘空间均核查，备份临时共存前额外保留库大小加20GiB；WSL内部df不代替宿主机余量。没有添加依赖、架构抽象、配置系统或修改共享数据库连接接口。

## 实际继续执行范围

新控制目录：`/home/xindingtian/code/quant_data/data/landing/daily-backfill-rebuilt-2020-2013-20261109T235516+0800`。2020基线准备182.836秒：目标194,157个Unit，复用193,022个成功Unit与5个既有延期日，只计划56个新批次。

- 全年指数、12/11月及10月前四项已成功范围全部复用，没有重新取数。
- 新执行10月adj_factor：16 Unit、68,186行、32请求、11.629秒；stk_limit：16 Unit、65,415行、32请求、12.227秒，均SUCCESS。
- 10月阶段检查通过后执行9月：suspend_daily 22 Unit/1,041行，stock_st 22 Unit/4,734行；stock_daily成功20 Unit/81,186行，新增2020-09-10与09-21两个已批准延期日。
- daily_basic成功21 Unit/83,566行，失败2020-09-18。9月adj_factor、stk_limit及8月至1月未启动。

本轮新增117个成功Unit、304,128行，174次请求全部成功。当前2020状态：

| Dataset | 当前成功 Unit | 当前FAILED | 其中已批准延期 | 尚未请求 Unit | 当前已发布行 |
|---|---:|---:|---:|---:|---:|
| suspend_daily | 82 | 0 | 0 | 161 | 3,950 |
| stock_st | 82 | 0 | 0 | 161 | 17,755 |
| stock_daily | 75 | 7 | 7 | 161 | 308,877 |
| daily_basic | 81 | 1 | 0 | 161 | 327,484 |
| adj_factor | 60 | 0 | 0 | 183 | 257,748 |
| stk_limit | 60 | 0 | 0 | 183 | 247,279 |
| index_daily | 192,699 | 0 | 0 | 0 | 141,669 |
| 合计 | **193,139** | **8** | **7** | **1,010** | **1,304,762** |

7个已批准日线延期日为09-10、09-21、10-28、11-17、12-14、12-16、12-22；保持FAILED、无事实和成功Publication，原PARTIAL及证据保留。日频WARNING原值保留，统一监控后继续；不是本次停止原因。

## 新硬错误：daily_basic.required

Run `aa67fe51-128d-4784-a042-baec21bbd6ed`，范围20200901～20200930，状态PARTIAL。44个请求全部成功、交付COMPLETE；22个目标交易日中21个已发布，唯一失败日2020-09-18。

原始分页响应共4160行，其中90行close=NULL，代码和日期均有效，触发daily_basic.required（detail.rows=90）。这些记录并非都来自北交所，其他十五项指标允许NULL不代表必填close可空。未推断供应商内部原因，未按当前名称/上市日期过滤，也未补值、丢行、改DDL或修改ERROR等级。

| 代码 | 当前快照名称 | 源日期 | close | volume_ratio样例 |
|---|---|---|---|---:|
| 000029.SZ | 深深房A | 20200918 | NULL | 2.1943 |
| 000670.SZ | 盈方微 | 20200918 | NULL | 1.1241 |
| 000737.SZ | 北方铜业 | 20200918 | NULL | 0.5597 |
| 000792.SZ | 盐湖股份 | 20200918 | NULL | 1.8908 |
| 000959.SZ | 首钢股份 | 20200918 | NULL | 0.6672 |
| 000995.SZ | 皇台酒业 | 20200918 | NULL | 0.8168 |
| 002569.SZ | *ST步森 | 20200918 | NULL | 0.8106 |
| 002819.SZ | 东方中科 | 20200918 | NULL | 1.1386 |

该失败日整日4,160行均未发布，Canonical该日为零，不是合法零行或已覆盖。90行轻量清单：`/home/xindingtian/code/quant_data/data/landing/daily-backfill-rebuilt-2020-2013-20261109T235516+0800/2020/daily-basic-required-20200918.csv`；完整原始证据路径和异常源值为required-error-evidence.json，正式请求证据保留在该Run的Landing requests目录。

当前新增ERROR不属于已批准的stock_daily三字段延期范围。已向用户提出只增加“daily_basic仅缺必填close，身份可信、完整交付且无其他ERROR时整日延期”的选项，尚未收到确认，未实施。

## 停止后保护和当前恢复点

只读核查耗时869.031秒：21张旧表摘要一致，20张旧表分片精确双向差异为零，分钟409,093,564行摘要一致；同年度已有数据和成功指针另行精确保护，全部本轮成功Publication仍是当前指针。已发布子集满足正式Dataset硬约束，实际失败范围严格对应7个日线延期日和1个未豁免每日指标日。

Serving原行情字段双向一致、双方键无重复，当前行业未匹配44,406行：2013样本86、2020部分8,031、2021年21,706、2022年10,478、2023年1,905、2024年1,393、2025年691、2026年116。原有年份数量不变；行业豁免和指标数值ERROR不同，不把失败日数据计作行业未匹配。

当前唯一恢复点仍是本轮开始取数前、已包含等价View SQL的warehouse.latest.duckdb：

- 字节：16,899,649,536。
- SHA-256：`5f45d6b75e82ae5a0e5d94ec632c56a49b0fac28986471d246aa73f36c097431`，停止后重新核对。
- 不包含本轮2020新增304,128行；年度尚未结束，不用未通过阶段门禁的状态覆盖恢复点。

当前无RUNNING Run、WAL、残留取数/dbt/备份进程，核查连接已关闭；未触碰其他任务的临时文件或未提交修改。

## 后续恢复

年份范围授权、WARNING监控继续和stock_daily三字段延期仍有效。需先决定daily_basic必填close缺失的运维延期例外或核实源端能否更正；不得自动扩大白名单、绕过Validator/Publisher或标记成功。恢复只处理未发布范围，保留已发布Unit、原PARTIAL/ERROR和审计。重建后已经生成的2020基线和117个新成功Unit不可当成空白年度重跑。
