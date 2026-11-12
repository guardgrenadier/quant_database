# 七项日频失败和延期 Unit 总表

生成时间：2026-11-12T11:10:28.809789+08:00；交易日期范围：2013-01-01～2026-11-12。

总表 32,511 个不同 Dataset/Unit；其中当前失败或延期 211 个，历史失败但现已发布 32,300 个。

文件：`all_failed_or_deferred_units.csv` 为完整总表；`current_unresolved_units.csv` 仅列当前待处理；`summary.json` 保存统计与口径。CSV 使用 UTF-8 BOM，适合表格软件筛选。

股票六项以交易日为 Unit；指数以代码和交易日为 Unit。按 Dataset/Unit 去重，异常源记录数不等于 Unit 数。正式 WARNING 不作为失败。已获批准的整日延期仍为 FAILED，不代表已补齐。

正式记录依据：`meta.run_unit` 的 IN_SCOPE/FAILED、`meta.run`、`meta.validation_check`、`meta.request_attempt`、`meta.coverage`、`meta.publication`；当前已批准延期与此前运维台账按 Dataset、Unit 和 Run ID 精确衔接。输出保留历史失败 Run、最新尝试与当前 Publication。

范围不包括未请求或未生成失败 Unit 的空白范围、OUT_OF_SCOPE、2013 年之前及 2026-11-12 之后；不能把本表解释为全量 Scope 缺口普查。指数历史失败中 32,298 个代码日当前已发布，不应与待处理缺口相加。

| Dataset | 当前失败／延期 Unit | 历史失败现已发布 Unit |
|---|---:|---:|
| suspend_daily | 0 | 1 |
| stock_st | 0 | 1 |
| stock_daily | 206 | 0 |
| daily_basic | 1 | 0 |
| adj_factor | 0 | 0 |
| stk_limit | 4 | 0 |
| index_daily | 0 | 32298 |

| 年份 | 当前待处理 Unit |
|---|---:|
| 2022 | 9 |
| 2021 | 12 |
| 2020 | 28 |
| 2019 | 6 |
| 2018 | 17 |
| 2017 | 33 |
| 2016 | 45 |
| 2015 | 50 |
| 2014 | 11 |

源证据路径是否存在仅按报告生成时的文件状态检查；Run/Publication 等审计信息均是只读快照。

证据路径核对：211 个当前失败/延期 Unit 的相关证据路径均存在；历史失败中 `stock_st / 2026-01-14` 的一个源请求证据文件不存在，其正式 Run 和错误审计仍在，当前已发布。总表逐行标明路径状态。
