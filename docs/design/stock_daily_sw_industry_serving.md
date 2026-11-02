# 股票日线申万行业特征：首个 dbt Serving 模型

日期：2026-11-02。用户已确认按股票日线逐行匹配申万一、二、三级行业代码，并授权搭建 dbt 环境和更新文档。

## 1. 输入、输出与职责

两个业务输入为已发布的 `canonical.stock_daily` 与 `canonical.index_member_all`，构建前读取 `meta.coverage` 确认成功版本。输出为同一 DuckDB 文件内的普通 View：`serving.stock_daily_with_sw_industry`。

保留原始日线 11 个字段和 `(ts_code,trade_date)` 粒度，只追加 `sw_l1_code / sw_l2_code / sw_l3_code` 三个可空字符串字段，不新增业务行。Canonical 及 ingestion 接口不变，也不为此创建新的 ingestion Dataset。

dbt 使用项目内 profiles、两个 Canonical 来源及 Meta Coverage 来源、一个 View 模型、一个专属发布状态 Hook 和两个 SQL 数据测试。没有分层透传模型、通用特征框架、规则 DSL 或额外 dbt 包。

## 2. 日期匹配与缺失

按证券代码相等及 `in_date <= trade_date AND (out_date IS NULL OR trade_date <= out_date)` 做 LEFT JOIN。用户已确认 out_date 包含当天，依据及历史样本见 [行业归属方案](index_member_all_ingestion_proposal.md#51-out_date-包含当天实测证据与用户确认)。

使用完整 Y+N，不按当前状态筛选历史。不展开交易日历，不补造停牌日线，不使用当前股票名单、名称或其他日期的行业归属补行。未匹配时保留日线并输出三个 NULL；三列都来自同一段归属。

正式行业 ingestion 已阻断归属区间重叠；Serving 数据测试继续验证输出粒度，以发现来源被绕过正式链路手工更改或 SQL 连接错误造成的多重匹配。失败不静默去重。

## 3. 构建与测试语义

项目 `on-run-start` 通过只读 SQL 检查行业 SNAPSHOT 的 PUBLISHED Coverage、现有日线所用每个日期的 PUBLISHED Coverage，并要求日线至少已有一个成功日。不猜测未出现在日线中的历史日期是否缺失，不自动执行上游，不把未接入误作合法空行业。

首版数据测试：

1. `stock_daily_industry_grain`：结果与原日线的键集合相同，每个键恰好一行。丢行、新增键和多匹配均 ERROR。
2. `stock_daily_industry_unmatched`：未匹配行逐条返回，WARNING 数量表示未匹配日线行数，允许成功；具体股票和逐日数量可查询 View。不设置分类全非空或任意缺失率阈值。

普通 View 查询时读取当前已发布事实，上游修订立即反映到后续查询，不需要增量修订传播。dbt 运行与 ingestion 按现有约定串行访问同一文件。构建前门禁不是每次查询的验证，不持有跨查询版本快照。

dbt build 在创建 View 后执行测试。若测试失败，命令失败但 View 可能已经存在；没有套用 ingestion 数据/成功元数据的原子发布保证，也不写 Serving Publication 或 Coverage。数据测试需要显式运行，不监听 ingestion，不声称 PIT 或可恢复历次特征结果。

## 4. 环境与下载范围

保留项目已锁定的 dbt-core 1.12.5、dbt-duckdb 1.11.0 和 DuckDB 1.5.6。profiles 要求显式 `QUANT_DATA_DB`，输出 Schema 为 serving，threads=1，时区 Asia/Shanghai。关闭扩展自动安装，未配置外部扩展或 dbt 包，关闭匿名使用统计。

没有执行安装命令，没有新增 Python/系统包、dbt 包、DuckDB 扩展或外部市场数据下载。使用已安装环境执行项目及测试；离线重放不算本次实时供应商请求。下载清单、主要复用版本和完整命令见 [dbt README](../../dbt/README.md)。

## 5. 验证记录

新增 `tests/test_dbt_serving.py` 的 3 项真实 dbt 子进程测试，通过明确的临时库夹具验证旧结束日/新开始日、无归属和历史断档 NULL、原字段及键保留、两条未匹配告警、View 后续修订可见、缺行业/日线成功版本时建 View 前阻断，以及多重匹配使粒度测试失败。

独立库 `data/probes/dbt-serving-validation-20261102/warehouse.duckdb` 按当前 Meta 从空库初始化，使用正式 Runner 离线重放 2026 年真实 SSE 日历、2026-10-21 日线响应和已保存 Y/N 归属分页响应。没有本次 HTTP 请求，不将离线重放称为实时获取。日线 5,568 行、行业 7,928 行；随后正式 dbt debug/build 均返回 0，产出普通 View 5,568 行，全部匹配，两个数据测试均 PASS。独立相关子查询逐条核对分类结果，dbt 前后的 Canonical 及成功 Coverage/Publication 完全相同。

全套测试 `uv run --frozen --no-sync python -m unittest discover -s tests -q` 共 171 项，170 通过，1 项因既有 stock_basic 本地真实样本缺失跳过。新增 3 项 Serving 测试全部实际执行。

环境前后 77 个已安装 distribution 逐项一致，pyproject.toml 与 uv.lock 的 SHA-256 不变，没有运行安装器、dbt deps 或扩展安装。原始环境清单、日志、manifest/run_results 及可丢弃验收脚本在独立目录，机器摘要见 [dbt-serving-validation-2026-11-02.json](../research/dbt-serving-validation-2026-11-02.json)。

环境验证时检查的 `data/warehouse_v3_2.duckdb` 已有 stock_daily，尚无 index_member_all；复制该库进行初始化检查时，正式内核拒绝其旧 Meta（meta.run 缺 reconciled_at/audit_state/audit_archive_path）。按冻结政策没有自动迁移，也没有绕过检查。环境验收使用新结构独立库，该源库 SHA-256 前后相同，未对其自动获取上游或部署 View。后续用户指定另一数据库的实时演示见第 6 节。

## 6. 用户授权的实时演示与数据库切换

环境搭建后用户要求向 `data/warehouse.duckdb` 下载日线和申万归属，并验证 dbt。该文件原有旧结构 Meta 和成功 stock_basic 5,921 行，正式初始化拒绝兼容；因此先在 `data/probes/warehouse-dbt-preview-20261102T120136922070/warehouse.duckdb` 新库实时获取 2026-10-29～30 日历、股票日线及完整行业快照。

共 12 次 HTTP 尝试，全部成功；日线 11,141 行、行业 7,928 行。正式 dbt build 通过，日线行数、原字段及键均保持，所有行匹配到行业，无未匹配或重复。

用户确认“备份旧库并启用新库”的方案后，将原文件完整备份为 `data/warehouse.before-dbt-20261102T120647218730.duckdb`，原 stock_basic 及审计保留，SHA-256 与切换前相同。启用新库至 `data/warehouse.duckdb`，并在最终路径重新执行 dbt build，两个数据测试及发布状态 Hook 均通过。原候选库仍保留；新库只有本次日历、日线、行业和 Serving，不将旧 stock_basic 数据或旧 Meta 拼接进去。

以上实时数据获取发生在环境搭建验收之后，不改写第 4 节“搭建阶段无下载”的记录。软件与锁定依赖始终保持原样。最终路径、备份与数据量见 [实时验收摘要](../research/dbt-warehouse-live-validation-2026-11-02.json)。
