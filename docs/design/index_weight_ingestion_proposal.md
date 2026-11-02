# index_weight 接入提案

日期：2026-11-02。状态：首版已实现并通过五指数真实独立库验收。

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[银河接口探查](../research/galaxy-amazingdata-interface-probe-2026-10-19.md)。

## 1. 业务契约

- 保存银河 `get_index_weight` 报告的每日指数成分权重，底层使用 `tgw.QueryThirdInfo / A010200005`。
- 首版只支持 `000016.SH`、`000300.SH`、`000905.SH`、`000906.SH`、`000852.SH`，不从成分接口或 `index_basic` 推断其他指数也有权重。
- Canonical 为 `canonical.index_weight`，Record Key 为 `(index_code,con_code,trade_date)`；一行表示一个指数在一个生效日期的一条成分权重事实。
- 输入为显式日期闭区间，可选指数代码子集。依赖同库已发布的 `index_basic / SNAPSHOT` 和区间内逐自然日 SSE `trade_calendar`；开市日进入 Scope，休市日排除。
- Publication Unit 为 `index_code:YYYY-MM-DD`。每个 Unit 对应一次单指数单日请求，验收后按指数日完整替换，失败保护旧事实及成功版本。

## 2. Schema 与获取边界

保留九列：`index_code/con_code` 为字符串，`trade_date` 为 DATE；`total_share/free_share_ratio/calc_share/weight_factor/weight/close` 为 DOUBLE。股本单位为股，比例及权重为百分数，收盘价保留源值；只有 `free_share_ratio` 允许合法 NULL。不做单位换算、权重归一化、成分补全或跨日前向继承。

多日范围响应已实测出现重复及缺失成分，因此正式获取必须逐日，不能直接使用整月或整年响应。`kDataEmpty(-76)`、空列表及转换后零行均不能证明合法空集合，所属指数日不得发布。

## 3. 验收与首版边界

ERROR 包括：缺字段或坏类型、返回代码/日期与请求不符、空键、重复业务键、必填值缺失、非有限数、权重或股本值域错误，以及指数日零行。每个非空指数日的权重和须在 `[99.9,100.1]`；该检查是项目交付门禁，不单独证明供应商完整或终态。

`free_share_ratio` 缺失、零权重及 `calc_share>total_share` 保留原值并按 Unit 汇总 WARNING。首版不按固定成分数验收，也不以当前 `stock_basic` 过滤历史成分。

首版不实现自动调度、PIT 历史镜像、全历史覆盖承诺或其他指数扩展。实现复用现有 Runner、Validator、Publisher、Meta 和银河 worker 证据恢复机制；只扩展银河内部请求类型、专属 Adapter 与 CLI 装配，不改变冻结架构或共享 Dataset Protocol。

## 4. 入口与实现

```bash
uv run --frozen python -m quant_data ingest index_weight \
  --start-date 20260907 --end-date 20260907 \
  --index-code 000300.SH --index-code 000905.SH \
  --db data/probes/my-index-weight/warehouse.duckdb \
  --landing-dir data/probes/my-index-weight/landing --keep-artifacts
```

不传 `--index-code` 时获取全部五个支持指数。实现见 [Dataset](../../src/quant_data/datasets/index_weight.py)、[DDL](../../src/quant_data/datasets/index_weight.sql)、[Adapter](../../src/quant_data/sources/galaxy/index_weight.py)、银河 [Client](../../src/quant_data/sources/galaxy/client.py) 和 [worker](../../src/quant_data/sources/galaxy/worker.py)。权重内部固定串行，分钟 CLI 的2/4并行政策不变；纯权重 worker 不预热高层日历。

## 5. 验收记录

新增银河执行层及权重链路测试覆盖参数与证据恢复、空响应、重复键、缺列、坏类型、错日、权重和、部分成功、上游版本漂移、回滚和 CLI。29 项目标测试通过；全套 **185 项中184通过、1项既有股票真实样本缺失跳过**。保存的沪深300真实300行响应经正式 Runner 重放后与 Canonical 逐字段一致。

独立库 `data/probes/index-weight-ingestion-validation-20261102T151220/warehouse.duckdb` 先以正式 CLI 发布当日 SSE 日历和793条 `index_basic`，再在2026-09-07对五指数各发一次真实请求。两次 Run 均 SUCCESS、产物完整，分别发布50/300/500/800/1000条，共2,650条；重跑逐字段一致、不累加，Coverage 指向第二次 Publication。权重和分别为100.0001、99.9994、100.0007、100.0011、99.9994；仅沪深300的300条 `free_share_ratio` 为 NULL并汇总告警。

源响应、Raw、Candidate、首次发布快照和当前 Canonical 等值核对通过，208个 manifest 文件条目的 SHA-256 全部通过。数据库副本中重放真实响应并明确离线注入沪深300请求失败，Run 为 PARTIAL，其他四指数发布成功，沪深300旧事实、Coverage 和 Publication 完整保留；故障注入没有发起 HTTP/SDK 请求。机器摘要见 [验收摘要](../research/index-weight-ingestion-summary-2026-11-02.json)。未写默认权威库，未执行全历史下载。
