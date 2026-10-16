# 官方交易日历探查与正式契约

日期：2026-10-16（Asia/Shanghai）。状态：真实接口探查完成；用户确认首版仅接入 SSE，正式契约已落地。

## 1. 官方接口与实际交付

官方 [trade_cal 文档](https://tushare.pro/document/2?doc_id=26) 提供 exchange、start_date、end_date、is_open 参数，默认上交所；输出 exchange、cal_date、is_open、pretrade_date。文档说明北交所参考沪深日历，未列出 BSE 请求参数。

本次通过现有 TushareClient，使用项目凭据直接请求官方 HTTPS API，显式选择四字段、不过滤 is_open；10 次请求均 HTTP 200 / code=0，无重试。请求之间等待 1.3 秒。未写入 Canonical 或 Meta。

| 用例 | 请求区间 | 行数 | 结果 |
|---|---|---:|---|
| SSE / SZSE 各一份 | 20260101–20261231 | 各 365 | 开市 242 天、休市 123 天；除 exchange 外逐行一致 |
| BSE | 20260101–20261231 | 0 | 不能解释为北交所全年休市，也不构成 BSE 日历支持证明 |
| SSE | 19900101–19901231 | 13 | 仅返回 19901219–19901231；首日 pretrade_date 为 NULL |
| SSE | 19910101–19910107 | 7 | 日期完整 |
| SZSE | 19910101–19910107 | 0 | 没有交付该范围，不能据此自动补休市 |
| SSE | 20270101–20271231 | 365 | 接口可返回未来日历；不据此保证节假日安排永不修订 |
| SSE | 20240101–20241231 | 366 | 闰年日期完整 |
| SSE | 20200120–20200205 | 17 | 跨春节范围完整 |
| SSE，limit=2，offset=0 | 20260101–20260107 | 2 | 返回 20260106–20260107，has_more=true；不能依赖返回顺序 |

所有非空样本中，exchange / cal_date 为字符串，is_open 为 **JSON 整数 0/1**，pretrade_date 为字符串或 NULL。不能直接复用 stock_basic 的全字符串 Raw 构造。所有 count 均为 0，不用作完整性依据；正常范围 has_more=false，显式两行分页 has_more=true。

样本中的 cal_date 唯一；非空 pretrade_date 均早于 cal_date，在样本可核对的范围内与前一开市日一致。范围首日可能引用请求区间外的前一交易日，不能要求其必须在本批次中。

## 2. 正式契约

内部 Dataset 名称 `trade_calendar`，供应商接口 `trade_cal`。按用户确认的当前研究口径，仅保存 SSE 供应商事实，供沪深北市场共同使用；保留所有自然日，包括明确的休市行，不复制生成 SZSE/BSE 行。探查样本不构成全历史三市场日历一致性证明。

| 字段 | Canonical 类型 | 约束 |
|---|---|---|
| exchange | VARCHAR | 固定 SSE，非空 |
| cal_date | DATE | 非空，市场本地日期 |
| is_open | BOOLEAN | 非空，由严格整数 0/1 转换 |
| pretrade_date | DATE | 可空；有值时必须早于 cal_date，不要求落在请求区间内 |

Record Key 与 Publication Unit 均为 `(exchange, cal_date)`，Unit 标识为 `SSE:2026-01-01`。一个 Unit 必须且只能包含一行；休市用 is_open=false 的事实行表示，空交付不允许发布为成功日历。按 Unit 完整替换，保留现有 Publisher 的事务与旧成功数据保护。

输入为显式闭区间 start_date / end_date，无交易所或开市筛选参数；不自动猜测历史起点或最新可用日期。Scope 为 SSE × 区间内所有自然日，无上游依赖。Adapter 按自然年拆分请求片段，片段与 Unit 解耦；不传 is_open 过滤，不分页。

每个片段要求四字段、has_more=false，返回范围与请求匹配。请求失败、截断或无法定位的坏数据阻断该片段；缺少日期阻断对应日期，其他可独立验收日期仍可发布。重复业务键、非法类型/日期、未知枚举阻断相关范围，不去重、不填造缺失日，不以周末规则推算开市状态。

复用现有 Dataset / SourceAdapter Protocol、Client、Runner、Validator、Publisher 与 Meta。新增 [Dataset](../../src/quant_data/datasets/trade_calendar.py)、[DDL](../../src/quant_data/datasets/trade_calendar.sql)、[专属 Adapter](../../src/quant_data/sources/tushare/trade_calendar.py) 与[测试](../../tests/test_trade_calendar.py)，扩展 CLI 并更新现行文档。未新增共享控制模型或改动其签名，本轮不实现 suspend_daily、调度或自动恢复。

## 3. 证据与复查

- 本地原始证据：`data/probes/trade-calendar-20261016T183117425911/requests/`，仅包含脱敏控制信息与响应，不入 Git。
- 可入 Git 的[逐请求摘要](tushare-official-trade-calendar-summary-2026-10-16.json)记录完整请求参数、字段、行数、类型、空值、样本、原始文件路径及 SHA-256。
- 复查可按摘要参数通过现有 TushareClient 重发请求；原始证据通过 Artifacts.record_attempt 即时保存。未来日历及历史数据可能修订，不把本次行数写成永久业务阈值。

## 4. 正式链路验证

通过公开 CLI 获取 20100101–20261231，在独立新库发布，未修改已有研究库：

```bash
uv run --frozen python -m quant_data ingest trade_calendar \
  --start-date 20100101 --end-date 20261231 \
  --db data/probes/trade-calendar-validation-20261016T184100210220/warehouse.duckdb \
  --landing-dir data/probes/trade-calendar-validation-20261016T184100210220/artifacts \
  --keep-artifacts
```

- Run ID：`54f36ede-43e6-401f-9415-d6641268e1db`；返回码 0、状态 SUCCESS、artifacts_complete=true。
- 17 次真实请求；Canonical 6,209 行（开市 4,128、休市 2,081），6,209 个成功 Publication，6,209 个 PUBLISHED Coverage，每个 Unit 一行。
- ERROR 为 0；未来日期告警影响 76 个 Unit，不将未来安排当作不可修订的承诺。
- 验证了 `SSE:2026-01-01`、`SSE:2026-10-16` 的依赖查询及版本复核；发布后 Canonical 产物与实际库逐列一致，manifest 中 60 个文件 SHA-256 均通过。
- 产物位于上述 artifacts 下的 Run ID 子目录；同级 `cli-result.json` 保存完整 CLI 结果，`verified-summary.json` 保存验证摘要。本地产物不入 Git。

全套 36 项测试中 35 项通过，1 项本地股票真实样本重放因样本缺失跳过。日历新增 9 项场景测试，覆盖跨年、休市、范围修订、缺日/空响应、逐日坏值及重复、请求片段错误、输入边界、事务/证据故障及 CLI；不使用生产 mock 或补造日历实现正式能力。
