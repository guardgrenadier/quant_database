# Tushare Relay 客户调用说明

> 面向客户和 AI 工具的统一调用手册。当前服务版本为 `0.5.61`， 257个接口已启用文档中的 Key、代码和日期均为示例，请替换为自己的值。

## 1. 服务地址与认证

服务根地址：

```
https://pcd.mobcvb.cn/tushare/pro
```

每次请求都要在 HTTP Header 中加入：

```
X-API-Key: <YOUR_CLIENT_KEY>
```

不要把客户 Key 放在 URL、代码仓库、公开聊天记录或前端页面中。服务使用 HTTPS，正常 Python 请求不需要代理，也不要关闭证书校验。

当前配额（以服务端返回的限流响应头为准）：

- 正常业务：同一共享 Key 每分钟最多 2000 次；同一来源 IP 每分钟最多 200 次。
- AI 探测：同一 Key 每分钟最多 1000 次；同一来源 IP 每分钟最多 60 次；同时最多 4 个探测；每次最多 5 行；单次探测预算 2 秒。
- 超出限额返回 HTTP 429，并带 Retry-After。同一个 IP 下有多人共用时，IP 配额是合计值，请在客户端做队列和退避。

## 2. 最简 Python 调用

下面的函数适用于所有普通数据接口。接口名放在 `api`，参数放在 `params`；返回值保持 Tushare 风格。

```
import os
import requests

BASE = "https://pcd.mobcvb.cn/tushare/pro"
KEY = os.environ["TUSHARE_RELAY_KEY"]

def ts(api: str, **params):
    params = {k: v for k, v in params.items() if v is not None}
    r = requests.get(
        f"{BASE}/{api}",
        params=params,
        headers={"X-API-Key": KEY},
        timeout=30,
    )
    try:
        body = r.json()
    except ValueError:
        r.raise_for_status()
        raise RuntimeError(f"HTTP {r.status_code}: non-JSON response")

    if r.status_code >= 400 or body.get("ok") is False:
        raise RuntimeError(
            f"{body.get('error', 'http_error')}: {body.get('message', '')}"
        )
    if body.get("code", 0) not in (0, None):
        raise RuntimeError(f"upstream code={body['code']}: {body.get('msg', '')}")
    return body

result = ts(
    "daily",
    ts_code="000001.SZ",
    start_date="20260101",
    end_date="20260827",
    fields="ts_code,trade_date,open,high,low,close,vol,amount",
)
print(result)
```

转成 pandas：

```
import pandas as pd

body = ts("daily", ts_code="000001.SZ", start_date="20260101")
data = body.get("data") or {}
df = pd.DataFrame(data.get("items", []), columns=data.get("fields", []))
print(df)
```

## 3. curl 调用

日期参数建议用 `--data-urlencode`，避免空格和特殊字符被 shell 改写：

```
curl --get "https://pcd.mobcvb.cn/tushare/pro/daily" \
  -H "X-API-Key: <YOUR_CLIENT_KEY>" \
  --data-urlencode "ts_code=000001.SZ" \
  --data-urlencode "start_date=20260101" \
  --data-urlencode "end_date=20260827" \
  --data-urlencode "fields=ts_code,trade_date,close"
```

Windows PowerShell：

```
$h = @{ "X-API-Key" = $env:TUSHARE_RELAY_KEY }
Invoke-RestMethod `
  -Uri "https://pcd.mobcvb.cn/tushare/pro/daily?ts_code=000001.SZ&start_date=20260101" `
  -Headers $h -Method Get
```

## 4. 通用参数规则

| 参数 | 写法 | 说明 |
| --- | --- | --- |
| `ts_code` | `000001.SZ`、`600000.SH`、`510300.SH`、`00001.HK`、`AAPL.US` | 股票/基金/ETF/指数等代码带市场后缀；多代码用英文逗号分隔 |
| `trade_date` | `20260827` 或 `2026-08-27` | 单个交易日 |
| `start_date`、`end_date` | `20260101` 或 `2026-01-01` | 日线类接口的闭区间；结束日期不要早于开始日期 |
| 分钟时间 | `2026-08-01 00:00:00` | `stk_mins` 等分钟接口建议同时传完整时间；也可传日期，网关会补全日始/日末 |
| `freq` | `1min`、`5min`、`15min`、`30min`、`60min` | 分钟周期统一用小写更稳妥 |
| `fields` | `ts_code,trade_date,close` | 英文逗号分隔字段；省略表示返回数据源可提供的字段 |
| `limit` | 正整数 | 单页条数；建议普通请求不超过 5000，分钟大结果分段拉取 |
| `offset` | `0`、`5000`… | 从第几条开始；与 `limit` 配合分页 |
| `exchange` | `SSE`、`SZSE`、`CFFEX` 等 | 交易所筛选，按接口要求填写 |
| `src` | `cls` 等 | 新闻来源；新闻接口的来源值必须是数据源支持的值 |
| `period` | `20241231` | 财务报告期，通常为 YYYYMMDD |
| `index_code` | `000300.SH` | 指数成分/权重接口使用 |
| `index_symbol` | `000300.SH` | 本地聚合接口使用 |
| `industry_code` | 例如 `801010.SI` | 本地聚合行业成分接口使用 |

参数名、字段名和大小写按 Tushare 同名接口填写；不要把 `ts_code` 写成 `code`，不要把 `trade_date` 写成 `date`。


## 4.1 接口名称速查

| 名称规律 | 一般用途 | 调用注意 |
| --- | --- | --- |
| `*_basic` | 基础资料/证券列表 | 常用 exchange、market、list_status 等筛选 |
| `*_daily` | 日线行情或日度统计 | 优先传 ts_code 或 trade_date；历史区间用 start_date/end_date |
| `weekly`、`monthly`、`*_weekly`、`*_monthly` | 周线/月线 | 通常传 ts_code 和日期区间 |
| `*_mins`、`*_min`、`rt_*min*` | 分钟或实时分钟 | 传 ts_code、freq、时间区间；大范围拆分 |
| `*_factor`、`*_indicator` | 复权因子/量化或财务指标 | 用 ts_code、trade_date、period 等条件缩小范围 |
| `*_moneyflow`、`moneyflow*` | 资金流数据 | 常用 ts_code 或 trade_date |
| `*_holder*`、`top*holder*` | 股东/持有人数据 | 通常需要 ts_code 或报告日期 |
| `news*`、`*_news`、`major_news` | 新闻、公告或资讯 | 新闻接口优先传来源和时间窗口 |
| `p_list`、`p_get`、`p_save`、`p_delete` | 客户自选组合 | 只能访问当前 Key 的组合；方法见第 8.8 节 |

这是命名速查，不替代接口的官方字段定义；最终以 capabilities 返回的 required、required_any、methods 为准。

## 5. 探测接口与正式业务请求

AI 探测和正式业务查询是两条不同路径：

- 查看完整机器可读目录：`GET /tushare/capabilities`。
- 查看单个接口能力：`GET /tushare/capabilities/{api_name}`。
- 对普通 GET 加 `?__probe=1`：只读本地样本，不请求上游，最多返回 5 行，适合 AI 判断接口是否存在。
- 普通 GET 如果没有任何业务筛选条件，会自动按探测处理；因此正式请求必须传业务参数。确实没有筛选条件的接口，可加 `__probe=0` 明确表示正式请求。
- `news` 是例外：不传日期时会查询最近 24 小时，不会自动变成探测；其他新闻接口仍建议传明确日期。
- 探测返回 `count=0` 只表示本地没有样本或接口需要参数，不代表正式接口不可用；请用本节的业务参数再测试。

示例：

```
GET https://pcd.mobcvb.cn/tushare/pro/daily?__probe=1
GET https://pcd.mobcvb.cn/tushare/capabilities/stk_mins
```

## 6. 返回格式、状态码和响应头

成功响应兼容 Tushare：

```
{
  "request_id": "server-generated-id",
  "code": 0,
  "msg": "ok",
  "data": {
    "fields": ["ts_code", "trade_date", "close"],
    "items": [["000001.SZ", "20260827", 12.34]]
  },
  "api_name": "daily",
  "count": 1
}
```

程序判断成功请检查 HTTP 状态码和 `code == 0`，不要只判断 `data.items` 是否非空。合法的非交易日或没有匹配记录可能是 `count=0` 且 `code=0`。

常见错误：

| HTTP | `error` | 含义 | 客户处理 |
| --- | --- | --- | --- |
| 400 | `invalid_params` | 参数缺失、格式错误或日期范围错误 | 对照接口表补齐参数 |
| 401 | `unauthorized` | Key 缺失或无效 | 检查 Header 名称和值 |
| 404 | `unknown_api` | 路径未注册 | 使用 `capabilities` 检查拼写 |
| 405 | `method_not_allowed` | 方法不匹配 | 普通数据接口用 GET；组合接口按第 8 节 |
| 429 | `rate_limited` / `ip_rate_limited` | Key 或 IP 达到限额 | 读取 `Retry-After`，退避后重试 |
| 503 | `data_source_unavailable` | 该接口当前未配置授权数据源 | 查看接口表“未启用”状态 |
| 503 | `minute_data_pending` | 分钟数据正在后台补数 | 稍后重试，不要并发重复提交 |
| 504 | `upstream_timeout` / `citydata_timeout` | 上游或队列超时 | 缩小日期范围、拆分代码后重试 |

响应头可用于排障：`X-Request-ID`、`X-Cache`（HIT/MISS/LOCAL）、`X-RateLimit-Remaining`、`X-RateLimit-IP-Remaining`。不要把响应头中的内部上游信息当成业务字段。

## 7. 数据源和缓存行为

普通历史接口的调度顺序是：

成功响应会按接口和标准化参数缓存；相同接口、代码、日期、频率和字段的重复请求优先从本地返回。空结果只短暂缓存，因此不要用一个错误日期的空结果判断接口长期无数据。

## 8. 按场景调用模板

### 8.1 A 股日线、基本面和资金流

```
ts("daily", ts_code="000001.SZ", start_date="20260101", end_date="20260827")
ts("daily_basic", ts_code="000001.SZ", trade_date="20260827")
ts("adj_factor", ts_code="000001.SZ", start_date="20260101", end_date="20260827")
ts("moneyflow", ts_code="000001.SZ", start_date="20260101", end_date="20260827")
```

按交易日查全市场时，使用 `trade_date`：

```
ts("daily", trade_date="20260827", limit=5000, offset=0)
```

### 8.2 周线、月线和复权

```
ts("weekly", ts_code="000001.SZ", start_date="20250101", end_date="20260827")
ts("monthly", ts_code="000001.SZ", start_date="20200101", end_date="20260827")
ts("pro_bar", ts_code="000001.SZ", freq="D", asset="E", adj="qfq",
   start_date="20250101", end_date="20260827")
```

`pro_bar` 常用组合：`asset=E/I/FD/FT/O/C/FX/HK/US`，`freq=D/W/M`；分钟周期使用 `E/I/FD/FT` 并传 `1MIN/5MIN/15MIN/30MIN/60MIN`。不支持的资产/周期组合会返回 `invalid_params`。

### 8.3 基础资料、指数、基金、债券

```
ts("stock_basic", exchange="SSE", list_status="L", limit=5000)
ts("trade_cal", exchange="SSE", start_date="20260101", end_date="20260827")
ts("index_basic", market="CSI")
ts("index_daily", ts_code="000300.SH", start_date="20260101", end_date="20260827")
ts("fund_basic", market="E")
ts("fund_daily", ts_code="510300.SH", start_date="20260101", end_date="20260827")
ts("cb_daily", ts_code="110011.SH", start_date="20260101", end_date="20260827")
```

融资融券、指数权重、期货和期权必须至少提供接口表中列出的交易所、代码或日期条件；不要发送空字符串冒充条件。

### 8.4 财务报表和股东数据

```
ts("income", ts_code="600519.SH", period="20241231")
ts("balancesheet", ts_code="600519.SH", period="20241231")
ts("cashflow", ts_code="600519.SH", period="20241231")
ts("fina_indicator", ts_code="600519.SH", period="20241231")
ts("dividend", ts_code="600519.SH")
ts("top10_holders", ts_code="600519.SH")
```

财务类接口通常返回多期记录；用 `period`、`ann_date` 或 `ts_code` 缩小范围，并用分页拉取长结果。

### 8.5 新闻、公告、互动问答

```
ts("news", src="cls",
   start_date="2026-08-26 00:00:00",
   end_date="2026-08-27 23:59:59", limit=500)
ts("major_news", src="cls",
   start_date="2026-08-26 00:00:00",
   end_date="2026-08-27 23:59:59")
ts("express_news", start_date="20260827", end_date="20260827")
ts("anns_d", ann_date="20260827")
ts("report_rc", ts_code="600519.SH", report_date="20250630")
```

`news` 可直接省略参数取最近 24 小时；为了结果稳定，生产程序仍建议显式传 `src/start_date/end_date`。新闻时间可以写成 `YYYYMMDD`、`YYYY-MM-DD` 或完整时间。

### 8.6 历史分钟和实时行情

```
# 本地 ClickHouse 分钟历史
ts("a_share_mins", ts_code="000001.SZ", freq="5MIN",
   start_date="2026-08-01 00:00:00",
   end_date="2026-08-27 23:59:59")

# 兼容 Tushare 的股票分钟接口
ts("stk_mins", ts_code="000001.SZ", freq="1min",
   start_date="2026-08-01 00:00:00",
   end_date="2026-08-27 23:59:59", limit=5000)

# 当日实时分钟/快照
ts("rt_min", ts_code="000001.SZ", freq="1min")
ts("rt_min_daily", ts_code="000001.SZ", freq="1min")
ts("rt_k", ts_code="000001.SZ")
```

分钟请求应遵守以下规则：

1. `ts_code` 必填；多代码请求用英文逗号分隔，建议每批不超过 10 个，服务端最大安全批次为 20 个。
2. `stk_mins` 推荐始终传 `freq/start_date/end_date`；日期范围过大时按交易日或 7 天一段分页。
3. `freq` 只使用 `1min/5min/15min/30min/60min`；服务端可能用已有 5 分钟数据派生高周期。
4. 本地没有完整覆盖时，服务端会受控地后台补数或切换已配置来源；不要让几十个并发请求重复补同一股票区间。
5. 实时接口是短缓存数据，不要用它请求多年历史；历史请用 `stk_mins` 或 `a_share_mins`。

### 8.7 本地聚合接口

```
ts("get_trade_days", start_date="20260101", until="20260827")
ts("get_all_securities", list_status="L", limit=5000)
ts("get_index_stocks", index_symbol="000300.SH", start_date="20260827")
ts("get_index_weights", index_symbol="000300.SH", start_date="20260827")
ts("get_industries", level="L1", src="SW2021")
ts("get_industry_stocks", industry_code="801010.SI", limit=5000)
```

这些接口用于把常用 Tushare 数据组合成便于量化程序消费的结果，优先读本地并在需要时使用基础接口补齐。

### 8.8 组合管理接口

组合接口按客户 Key 隔离，只能访问自己的数据：

```
# 查询组合
ts("p_list")

# 读取组合（GET）
ts("p_get", portfolio_id="demo")

# 保存或更新组合（POST JSON）
r = requests.post(
    f"{BASE}/p_save",
    json={"portfolio_id": "demo", "name": "示例组合",
          "payload": '[{"ts_code":"000001.SZ","weight":0.5}]'},
    headers={"X-API-Key": KEY},
    timeout=30,
)
print(r.json())

# 删除组合（DELETE）
r = requests.delete(
    f"{BASE}/p_delete",
    params={"portfolio_id": "demo"},
    headers={"X-API-Key": KEY},
    timeout=30,
)
print(r.json())
```

每个客户最多保存 200 个组合；`p_save` 的 `payload` 建议传 JSON 字符串，避免表单编码破坏结构。

## 9. 分页、批量和重试建议

- 先用 `limit=5` 验证字段和参数，再逐步增大到 1000/5000。
- 全市场数据按 `trade_date` 循环；单股票历史按日期区间循环。
- 多股票分钟按 10 个代码一批；收到 `429`、`502`、`503`、`504` 时指数退避（1、2、4 秒），最多重试 2 次。
- 不要把 `limit`、`offset`、`fields` 当成业务筛选条件；只有它们时，普通 GET 可能被识别为探测。
- 读取响应头的 `Retry-After`；在窗口结束前不要忙等重试。

## 10. 当前完整接口索引（298 个）

下表直接来自当前注册表。所有数据路由格式均为：

```
GET https://pcd.mobcvb.cn/tushare/pro/{接口名}
```

“参数门槛”只列网关已知的硬性条件；上游接口还可能要求该接口自己的业务字段。状态为“未启用”的接口当前会返回 `503 data_source_unavailable`，不会伪造数据。

| 接口 | 方法 | 状态 | 数据层/路由 | 参数门槛 |

| `a_share_mins` | GET | 可用 | 本地 ClickHouse | 必填：ts_code |
| `adj_factor` | GET | 可用 | ts_code / trade_date / start_date / end_date |
| `anns_d` | GET | 可用 | 官方兼容池 | 必填：ann_date |
| `bak_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `bak_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `balancesheet` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date / period |
| `bc_bestotcqt` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `bc_otcqt` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `block_trade` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `bond_blk` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `bond_blk_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `broker_recommend` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `bse_mapping` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cashflow` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date / period |
| `cb_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_call` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `cb_factor_pro` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_issue` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_price_chg` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_rate` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_rating` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cb_share` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cctv_news` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ci_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ci_index_member` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_cpi` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_gdp` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_m` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_pmi` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_ppi` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cn_schedule` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `concept` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `concept_board` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `concept_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cyq_chips` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `cyq_perf` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `daily_basic` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date |
| `daily_info` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_concept` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_concept_cons` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_hot` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dc_index_prev` | GET | 未启用 | 外部分钟/实时适配 | 
| `dc_member` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `disclosure_date` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `dividend` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date |
| `eco_cal` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `etf_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `etf_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `etf_mins` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `etf_sh_cons` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `etf_share_size` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `etf_sz_cons` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `express` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `express_news` | GET | 可用 | 官方兼容池 | 必填：start_date、end_date |
| `factor_list` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `factor_value` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fina_audit` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date |
| `fina_indicator` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date / period |
| `fina_mainbz` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / period |
| `fina_mainbz_vip` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `forecast` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ft_limit` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ft_mins` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `fund_adj` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `fund_announcement_report_em` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `fund_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fund_company` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fund_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `fund_div` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date |
| `fund_factor_pro` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fund_manager` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fund_min` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `fund_nav` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / nav_date / end_date |
| `fund_portfolio` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date / end_date |
| `fund_share` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_basic` | GET | 可用 | 官方兼容池 | 必填：exchange |
| `fut_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `fut_holding` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_index_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_level2` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `fut_mapping` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_settle` | GET | 可用 | 官方兼容池 | 必填：trade_date |
| `fut_trade_cal` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_weekly_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_weekly_monthly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `fut_wsr` | GET | 可用 | 官方兼容池 | 必填：trade_date |
| `fx_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `fx_mins` | GET | 可用 | 本地 ClickHouse | 必填：ts_code |
| `fx_obasic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `get_all_securities` | GET | 可用 | 本地聚合 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `get_index_stocks` | GET | 可用 | 本地聚合 | 必填：index_symbol |
| `get_index_weights` | GET | 可用 | 本地聚合 | 必填：index_symbol |
| `get_industries` | GET | 可用 | 本地聚合 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `get_industry_stocks` | GET | 可用 | 本地聚合 | 必填：industry_code |
| `get_trade_days` | GET | 可用 | 本地聚合 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `gz_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hibor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_adj_factor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_balancesheet` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_cashflow` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `hk_fina_indicator` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_hold` | GET | 可用 | 官方兼容池 | 至少一组：trade_date / ts_code |
| `hk_income` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_mins` | GET | 可用 | 本地 ClickHouse | 必填：ts_code |
| `hk_monthly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hk_weekly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hm_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hm_list` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `hsgt_top10` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `idx_anns` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `idx_factor_pro` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `idx_mins` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `income` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / ann_date / period |
| `index_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `index_classify` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `index_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `index_dailybasic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `index_global` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `index_member` | GET | 可用 | 官方兼容池 | 必填：index_code |
| `index_member_all` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `index_min` | GET | 未启用 | 外部分钟/实时适配 |
| `index_monthly` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / start_date / end_date |
| `index_weekly` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / start_date / end_date |
| `index_weight` | GET | 可用 | 官方兼容池 | 至少一组：index_code / trade_date |
| `irm_qa_sh` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date |
| `irm_qa_sz` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date |
| `kpl_concept_cons` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `kpl_list` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `libor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `limit_cpt_list` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `limit_list_d` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `limit_list_ths` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `limit_step` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `major_news` | GET | 可用 | 官方兼容池 | 必填：src、start_date、end_date |
| `margin` | GET | 可用 | 官方兼容池 | 至少一组：trade_date / exchange_id |
| `margin_detail` | GET | 可用 | 官方兼容池 | 至少一组：trade_date / ts_code |
| `margin_secs` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `mkt_idx_bmk` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `monetary_policy` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date |
| `moneyflow_cnt_ths` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_dc` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_hsgt` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_ind_dc` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_ind_ths` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_mkt_dc` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `moneyflow_ths` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `monthly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `namechange` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `new_share` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `news` | GET | 可用 | 官方兼容池 | 可省略筛选（默认最近24小时）；或传 src+start_date+end_date |
| `news_cctv` | GET | 可用 |  | 外部分钟/实时适配 | 
| `npr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `opt_basic` | GET | 可用 | 官方兼容池 | 必填：exchange |
| `opt_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / exchange |
| `opt_mins` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `p_delete` | DELETE | 可用 | PostgreSQL 组合管理 | portfolio_id，或 code/name 三选一 |
| `p_get` | GET | 可用 | PostgreSQL 组合管理 | portfolio_id，或 code/name 三选一 |
| `p_list` | GET | 可用 | PostgreSQL 组合管理 | 无参数；只列当前客户自己的组合 |
| `p_save` | POST | 可用 | PostgreSQL 组合管理 | portfolio_id，或 code/name 三选一 |
| `pledge_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `pledge_stat` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `pro_bar` | GET | 可用 | 本地聚合 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `realtime_tick` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `repo_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `report_rc` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / report_date |
| `repurchase` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `research_report` | GET | 可用 | 外部分钟/实时适配 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `rt_etf_k` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_etf_min` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_etf_min_daily` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_etf_sz_iopv` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_fut_level2` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：
| `rt_fut_min` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_fut_min_daily` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：
| `rt_fut_min_health` | GET | 未启用 | 外部分钟/实时适配 | 
| `rt_fut_ticks` | GET | 未启用 | 外部分钟/实时适配 | 
| `rt_hk_k` | GET | 未启用 | 外部分钟/实时适配 | 
| `rt_idx_k` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_idx_min` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_k` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `rt_min` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `rt_min_daily` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `rt_sw_k` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `rt_tick` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `sf_month` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `sge_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `sge_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `share_float` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `shibor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `shibor_lpr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `shibor_quote` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `slb_len` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `slb_len_mm` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `slb_sec` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `slb_sec_detail` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `st` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_account` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_account_old` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_ah_comparison` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_alert` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_auction` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_auction_c` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_auction_o` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `stk_auction_replay` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_factor` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `stk_factor_pro` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_high_shock` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_holdernumber` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_holdertrade` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_limit` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `stk_managers` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_mins` | GET | 可用 | 外部分钟/实时适配 | 必填：ts_code |
| `stk_nineturn` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_premarket` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_rewards` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_shock` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `stk_surv` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_week_month_adj` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stk_weekly_monthly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stock_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stock_company` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stock_hsgt` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `stock_st` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `suspend_d` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `sw_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `sw_mins` | GET | 可用 | 外部分钟/实时适配 | 按数据源传业务条件；分钟建议 ts_code+freq+start_date+end_date |
| `sz_daily_info` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `tdx_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `tdx_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `tdx_member` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `the_member` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ths_daily` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ths_hot` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ths_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `ths_member` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `top_inst` | GET | 可用 | 官方兼容池 | 必填：trade_date |
| `top_list` | GET | 可用 | 官方兼容池 | 必填：trade_date |
| `top10_cb_holders` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `top10_floatholders` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `top10_holders` | GET | 可用 | 官方兼容池 | 必填：ts_code |
| `trade_cal` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_adj_factor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_adjfactor` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_balancesheet` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_basic` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_cashflow` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_daily` | GET | 可用 | 官方兼容池 | 至少一组：ts_code / trade_date / start_date / end_date |
| `us_daily_adj` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_daily_market_cap` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_depth` | GET | 未启用 | 外部分钟/实时适配 | 当前未启用：返回 503 data_source_unavailable |
| `us_fina_indicator` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_income` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_mins` | GET | 可用 | 本地 ClickHouse | 必填：ts_code |
| `us_monthly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_tbr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_tltr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_tradecal` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_trltr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_trycr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_tycr` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `us_weekly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `weekly` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `wz_index` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |
| `yc_cb` | GET | 可用 | 官方兼容池 | 按同名 Tushare 参数；正式请求带业务筛选 |

## 11. 推荐给客户的排错顺序

1. 先访问 `/tushare/capabilities/{接口名}`，确认接口是否拼写正确、是否启用、需要哪些参数。
2. 用 `limit=5` 和一个明确代码/日期发起正式业务请求。
3. 检查 HTTP 状态码、`code`、`count`、`X-Request-ID` 和 `X-Cache`。
4. 如果是 `count=0` 且 `code=0`，先确认交易日、代码后缀、日期格式和数据覆盖范围。
5. 如果是 429，等待 `Retry-After`；如果是 503 未启用，不要循环重试。
6. 分钟接口按小日期段和少量代码重试；新闻接口传明确时间窗口。

## 12. 机器可读目录

客户程序可以直接读取：

```
GET https://pcd.mobcvb.cn/tushare/capabilities
GET https://pcd.mobcvb.cn/tushare/capabilities/{api_name}
```

这两个地址同样需要 `X-API-Key`。返回包含 `provider`、`enabled`、`required`、`required_any`、`methods`、`probe_example` 等字段，适合 AI 在生成调用前自动校验参数。

本手册只描述调用契约；上游是否在某个日期有记录仍以实际响应为准。不要用探测样本替代业务数据验证。
