# ProMax 网关对 Tushare 股票／指数官方示例的兼容性抽样

测试日期：2026-09-19。把 Tushare 文档中的 `pro.<接口>(**参数)` 映射为网关 `GET /tushare/pro/<接口>?参数`；API Key 仅从本地 `.env` 读取。TLS 校验开启，单次超时 20 秒；遇 HTTP 503 最多重试一次，间隔 2 秒。下表的“成功”指 **HTTP 200、`code=0`、返回非空行，且已检查的日期落在请求范围内**。没有保存响应数据或密钥。单次抽样不代表长期可用性。

> 下表保留官方示例原样映射的**首轮结果**。依据 [ProMax 调用手册](promax.md) 增加 `limit/offset` 后的复测与当前判断见文末；请勿把首轮 400 当作接口不可用。

## 按官方示例参数调用

表内用简写展示参数；实际请求保留官方示例的参数名、取值和空字符串。标明“无 `fields`”表示没有传这个参数。

| 官方接口与示例参数 | 本次网关结果 | 判定 |
| --- | --- | --- |
| [stock_basic](https://tushare.pro/document/2?doc_id=25)：`exchange=''`、`list_status='L'`、`fields=ts_code,symbol,name,area,industry,list_date` | 200，5565 行 | 成功 |
| [trade_cal](https://tushare.pro/document/2?doc_id=26)：`exchange=''`、`20180101–20181231`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [trade_cal 通用调用示例](https://tushare.pro/document/1?doc_id=40)：`exchange=''`、`20180901–20181001`、`is_open='0'`、四个 `fields` | 首次 503；重试 200，12 行 | 重试后成功 |
| [daily 单股区间](https://tushare.pro/document/2?doc_id=27)：`000001.SZ`、`20180701–20180718` | 首次 503；重试 200，13 行，日期在区间内 | 重试后成功 |
| [daily 全市场单日](https://tushare.pro/document/2?doc_id=27)：`trade_date=20180810` | 200，3439 行，均为所请求日期 | 成功 |
| [adj_factor 单股历史](https://tushare.pro/document/2?doc_id=28)：`ts_code=000001.SZ`、`trade_date=''` | 503 `upstream_pool_exhausted`，重试仍 503 | 本次未成功；未证明该形态永久不可用 |
| [adj_factor 全市场单日](https://tushare.pro/document/2?doc_id=28)：`ts_code=''`、`trade_date=20180718`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [daily_basic 全市场单日](https://tushare.pro/document/2?doc_id=32)：`ts_code=''`、`trade_date=20180726`、六个 `fields` | 首次 503；重试 200，3390 行，均为所请求日期 | 重试后成功 |
| [stock_st 全市场单日](https://tushare.pro/document/2?doc_id=397)：`trade_date=20250813`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [suspend_d 全市场单日](https://tushare.pro/document/2?doc_id=214)：`suspend_type='S'`、`trade_date=20200312`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [stk_limit 全市场单日](https://tushare.pro/document/2?doc_id=183)：`trade_date=20190625`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [stk_limit 单股区间](https://tushare.pro/document/2?doc_id=183)：`002149.SZ`、`20190115–20190615` | 503 `upstream_pool_exhausted`，重试仍 503 | 本次未成功 |
| [index_basic](https://tushare.pro/document/2?doc_id=94)：`market='SW'`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [index_daily 单指数历史](https://tushare.pro/document/2?doc_id=95)：`ts_code=399300.SZ`，不设日期 | 200，5418 行，返回日期为 20020301–20260918 | 成功 |
| [index_daily 指定区间](https://tushare.pro/document/2?doc_id=95)：`399300.SZ`、`20180101–20181010`，无 `fields` | 首次 `ReadTimeout`（20 秒，无 HTTP 响应）；重试 503 | 本次未成功 |
| [index_weight](https://tushare.pro/document/2?doc_id=96)：`399300.SZ`、`20180901–20180930`，无 `fields` | 400 `invalid_params`，预计响应超限 | 失败 |
| [index_dailybasic](https://tushare.pro/document/2?doc_id=128)：`trade_date=20181018`、`fields=ts_code,trade_date,turnover_rate,pe` | 首次 503；重试 200，14 行，均为所请求日期 | 重试后成功 |

## 同参数补测：显式指定字段

以下是诊断用变体，**不是上表官方示例的原样结果**。除 `fields` 外保持同一组接口参数；`trade_cal` 年窗与 `daily_basic` 单日做了直接对照。

| 接口与补测字段 | 结果 | 与无 `fields` 的区别 |
| --- | --- | --- |
| `stock_basic(L)`：示例六字段 | 200，5565 行 | 同条件不传字段为 400 |
| `trade_cal(2018 全年)`：`exchange,cal_date,is_open,pretrade_date` | 200，365 行 | 同条件不传字段为 400 |
| `daily_basic(20180726)`：示例六字段 | 200，3390 行 | 同条件不传字段为 400 |
| `adj_factor(20180718)`：`ts_code,trade_date,adj_factor` | 200，3708 行 | 同条件不传字段为 400；即使显式列出全部三个字段，结果也改变 |
| `stock_st(20250813)`：`ts_code,trade_date,type` | 首次 503；重试 200，175 行 | 无字段为 400 |
| `suspend_d(S,20200312)`：`ts_code,trade_date,suspend_type` | 200，21 行 | 无字段为 400 |
| `stk_limit(20190625)`：`trade_date,ts_code,up_limit,down_limit` | 首次 503；重试 200，3621 行 | 无字段为 400 |
| `index_basic(SW)`：`ts_code,name,market,publisher,category` | 首次 503；重试 200，950 行 | 无字段为 400 |
| `index_daily(399300.SZ,20180101–20181010)`：`ts_code,trade_date,close` | 首次 503；重试 200，186 行 | 无字段时超时后 503 |
| `index_weight(399300.SZ,20180901–20180930)`：`index_code,con_code,trade_date,weight` | 200，**0 行** | 无字段为 400；仍未取得官方示例所示的成分数据 |

另外，`stock_basic(list_status='D')` 使用与上市股相同的六字段返回 **200、0 行**。HTTP 成功但空结果不构成退市证券覆盖。此前还观察到 `stock_st` 请求单股票的 2025 年区间，却返回 2021–2026 年的记录；取数程序必须验证每行日期，不能只检查状态码和行数。

## 失败响应原样结构与结论

网关的 400 响应示例：

```json
{"ok":false,"error":"invalid_params","message":"request response estimate exceeds the limit"}
```

网关的 503 响应示例：

```json
{"ok":false,"error":"upstream_pool_exhausted"}
```

`ReadTimeout` 是客户端在 20 秒内未收到响应，没有 HTTP 响应体。多项请求在一次 503 后重试成功，故 503 不等同于接口不支持。

前一日另用单证券两年窗口探查时，还收到 HTTP 400，`error=date_range_too_large`，`message="date range exceeds 366 days; paginate the request"`。它不是上表官方示例的本次结果。

**首轮发现是显式 `fields` 会影响请求是否通过网关估算。** 对多个相同日期、相同筛选条件的请求，它把 400 变成了非空成功响应；具体估算规则未公开。随后按 ProMax 手册增加 `limit`，更多接口也已取得数据。

## 按 ProMax 手册补测：`limit/offset`

手册建议普通请求单页不超过 5000 行，`limit` 与 `offset` 可直接作为 GET 参数传入。`/tushare/capabilities/{api}` 显示下表接口均已启用。以下保持此前失败请求的业务筛选，**不传 `fields`，只增加 `limit`**；503 最多重试一次。每项均返回 HTTP 200、`code=0`、非空行。

| 接口与请求 | 本次返回 |
| --- | --- |
| `stock_basic(list_status='L', limit=5000, offset=0/5000)` | 两页分别 5000、565 行；合并后 5565 个代码，无页间重复 |
| `trade_cal(2018 全年, limit=500)` | 365 行，日期均在 2018 年 |
| `adj_factor(trade_date=20180718, limit=5000)` | 3708 行，均为所请求日期 |
| `daily_basic(trade_date=20180726, limit=5000)` | 3390 行，均为所请求日期 |
| `stock_st(trade_date=20250813, limit=500)` | 175 行，均为所请求日期 |
| `suspend_d(S, trade_date=20200312, limit=500)` | 21 行，均为所请求日期 |
| `stk_limit(trade_date=20190625, limit=5000)` | 3621 行，均为所请求日期 |
| `index_basic(market='SW', limit=2000)` | 951 行 |
| `index_daily(399300.SZ, 20180101–20181010, limit=1000)` | 186 行，日期在区间内 |
| `index_weight(399300.SZ, 20180901–20180930, limit=500)` | **300 行**，交易日为 20180928；此前不传 `limit` 为 400、只传 `fields` 为 200 但 0 行 |

`stock_basic` 的上述两页若省略 `fields`，第一、二页分别返回 17 与 10 个字段；固定为 `ts_code,symbol,name,area,industry,list_date` 后，两页字段一致、代码无交集。因此分页时还要固定字段并检查页间去重。`TushareClient.query()` 已实测能透传 `limit/offset/fields`；它本身不自动获取下一页。

退市股仍需进一步核验：`stock_basic(list_status='D', exchange='SSE', limit=1000)` 返回 147 行，`BSE` 返回 5 行，`SZSE` 本轮重试后仍为 503；不指定交易所曾返回 200、0 行。网关的本地聚合 `get_all_securities(list_status='D', limit=1000)` 返回 339 行，但本次默认字段不含 `delist_date`。这些结果说明应按明确交易所请求并核对历史证券主数据，不能把某次空结果当作无退市股票。

有界分页也不能保证非空：`adj_factor(ts_code='000001.SZ', limit=5000)` 返回 200、0 行，`stk_limit` 官方单股区间示例加 `limit=500` 也返回 200、0 行。前者用明确的一年日期区间曾取得 243 行，后者换成另一只股票的短窗曾取得数据；因此这两种原样长区间仍应列为未验证，不能把 0 行当作完整历史。

**修订结论：**此前多数 400 是网关对预计响应量的拒绝；显式 `fields` 或有界 `limit` 均能让多项接口成功。`index_weight` 已由 0 行补测到 300 行。503 `upstream_pool_exhausted` 仍会间歇出现，应有限重试；即使返回 200，也必须检查请求代码、日期范围、页间重复及接口行数上限。

## 2026-09-23 补测：stock_basic 五种状态单请求

官方当前列出 `L`（上市）、`D`（退市）、`P`（暂停上市）、`G`（过会未交易）、`UN`（未上市），且省略 `list_status` 时默认 `L`。本轮验证一个请求能否取得五种状态全集。

成功的请求形态：

```text
stock_basic(
  list_status="L,D,P,G,UN",
  fields="ts_code,symbol,name,exchange,market,list_status,list_date,delist_date",
  limit=10000,
  offset=0,
)
```

实测成功响应为 5917 行、5917 个唯一 `ts_code`、无重复代码，且 `5917 < limit`；状态分布为 `L=5568 / D=339 / P=0 / G=0 / UN=10`，交易所分布为 `SZSE=3094 / SSE=2467 / BSE=356`，339 个 D 的 `delist_date` 均非空。两次成功响应逐行顺序和值完全一致。全状态响应里的 5568 个 L 代码与独立 `list_status=L, limit=10000` 请求完全相同，10 个 UN 代码也与独立 UN 请求完全相同。`limit=10000` 高于 ProMax 普通请求建议的 5000，但本轮只有该值能在一页容纳 5917 行并已实测成功。

但该端点仍有明显的不稳定与错误形态：

- 同一全状态参数曾先返回 HTTP 200、上游 `code=1`，也多次返回 503 `upstream_pool_exhausted`，之后才成功；正式任务必须有限重试，并且只把 HTTP 200、`code=0`、协议检查通过的响应视为完成。
- 省略 `list_status` 会被网关当作无业务筛选的本地探测（本轮为 `X-Cache: LOCAL`、0 行）；即使强制正式请求，上游官方默认也只是 L，不代表全状态。
- `list_status=D` 不指定交易所再次返回 200、0 行，但全状态响应中有 339 个 D；不能用该空结果反证退市股不存在。
- `list_status=UN, exchange=SSE` 错误返回 3076 行，其中混入 `L=2313、D=147、状态空=616`，且只有 2462 个唯一代码；组合筛选的其他形态不能自动视为可靠参考。
- `list_status=L, limit=5000, offset=5000` 曾返回 6464 行、只有 5551 个唯一代码（913 个重复），并且响应字段与请求字段不符；不要用这个异常分页形态替代已验证的单页请求。

**当前结论：单次拉取五种 `list_status` 在上述精确参数下可行，且比逐状态／逐交易所组合更可靠。**生产使用仍应检查：字段集合固定、`ts_code` 非空且唯一、`list_status ∈ {L,D,P,G,UN}`、返回行数严格小于 limit、状态分布相对上次快照没有异常骤降。当前 5917 行距 10000 上限尚远；未来接近上限时必须重新设计并实测切分，不能沿用本轮已出现异常的 offset 结果。

同日后续用完整八字段复测 `list_status=D`：SSE 首次返回 147 行，SZSE 首次 503、第二次返回 187 行，BSE 首次返回 5 行；三组 `delist_date` 非空率均为 100%，并集 339 行与全状态响应的 D 子集逐行一致。此前“D 不指定交易所返回假空”和“缩减字段后组合过滤异常”仍说明参数形态会影响结果，但完整字段下分交易所 D 已取得闭合对账证据。
