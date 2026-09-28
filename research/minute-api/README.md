# minute-api 最小可复现实验

结论报告在 `docs/research/minute-api-request-experiments.md`；
据此形成的设计在 `docs/architecture/minute-data-ingestion.md`
（通用架构见 `docs/architecture/ingestion-framework.md`）。

## 文件

- `experiment.py`：分阶段实验脚本（capabilities / 参数契约 / 分页 / 时间边界 / 空结果 / 多代码矩阵与单代码对照 / 热缓存并发基准 / analyze 汇总检查）。普通阶段单次最多 15 次 HTTP；20 代码对照阶段按固定样本放宽到 20–22 次，均不自动重试。
- `retry_pending.py`：对 `minute_data_pending` 请求做 60 秒间隔、最多 12 轮的有限重试。
- `tmp/`：逐请求脱敏日志（`requests.jsonl`）与各步骤响应行（`<step>.json`）。**不入 Git**；密钥只存在于请求 Header，任何文件都不含密钥。

## 复现

```bash
set -a; source .env; set +a   # 需要 TUSHARE_API_KEY
uv run python research/minute-api/experiment.py p0   # capabilities、日历、证券主数据
uv run python research/minute-api/experiment.py p1   # 参数契约
uv run python research/minute-api/experiment.py p2   # limit/fields/多代码
uv run python research/minute-api/experiment.py p3   # 分页终止
uv run python research/minute-api/experiment.py p4   # 时间边界
uv run python research/minute-api/experiment.py p5   # 空结果、停牌、BSE、a_share_mins 对照
uv run python research/minute-api/experiment.py p6   # 固定样本的多代码 2/5/10/20 批量矩阵；重跑即有限重访
uv run python research/minute-api/experiment.py p7   # 同一 20 代码逐代码对照；重跑即有限重访
uv run python research/minute-api/experiment.py p8   # 最近交易日 20 代码批次与逐代码交叉验证
uv run python research/minute-api/experiment.py p9   # p7 已完成切片的热缓存并发 1/2/4/8 基准
uv run python research/minute-api/experiment.py p10  # stock_basic 五种 list_status 单请求并重复核对
uv run python research/minute-api/experiment.py p11a # 退市主数据覆盖与日线最后交易日对照
uv run python research/minute-api/experiment.py p11b # 上市首日／退市边界分钟请求；重跑即有限重访
uv run python research/minute-api/experiment.py analyze
uv run python research/minute-api/retry_pending.py   # 仅当有 pending 需要等待时
```

注意：对未缓存证券/日期的首次请求会触发服务端补数（HTTP 202），p2/p5 的部分步骤需要等补数完成后重跑才能得到 200 响应。

## 结果摘要（2026-09-21 实测，脱敏）

- 网关 `stk_mins` 可用，`ts_code` 必填；`freq=1min/1MIN/省略` 结果一致（省略默认 1 分钟）；`fields` 子集生效且不影响行数。
- 单日（窗口 09:30:00–15:00:00，双端包含）：核心 241 根（09:30–11:30 + 13:01–15:00，无 13:00），含 09:30 与 15:00；另有逐日不规则的午间/盘后填充 bar（重复前 bar 量价），行数 241–281 不等，行数不能作完整性判据；主键 `(ts_code, trade_time)` 无重复。
- 盘前竞价 bar（09:25–09:29）仅在窗口覆盖时返回。
- `vol` 为整数；`amount` 为带浮点噪声的非整数（canonical BIGINT 口径受阻，见报告未解决问题）。
- 排序降序；offset 分页实测可靠：limit=100 四页 100/100/50/0，并集 = 单页大 limit 参考集，页间无重叠，越界页 0 行，重复请求逐行一致；恰好整页需再取一页。终止 = 一页行数 < limit（且 HTTP 200、`code=0`）。
- `minute_data_pending` 实测为 HTTP 202 + `Retry-After: 30`（手册写 503）。补数耗时不可控：热代码单证券约 10 分钟；非交易日请求约 48 分钟后转 200 + `code=0` + 0 行（合法空结果路径验证成立）；多代码批次、上市前请求超过 50 分钟未解决。多代码与单代码是各自的补数任务。
- 停牌日（002731.SZ，2026-09-18）返回 241 根零成交量骨架 bar，非空响应。
- 北交所抽样（920344.BJ）241 根正常返回。
- `a_share_mins` 同日同窗每 bar 重复约 8 次、时间带毫秒后缀，不能作来源。
- 在市证券 5533 只（SH 2310 / SZ 2892 / BJ 331），来自 `stock_basic` L 两页。
- 观察到一次网关级裸 503（无 JSON、无响应头），约 3.5 分钟自愈。
- 全程 120 次 HTTP 请求（32 次 200、81 次 202 含对 3 个补数任务的轮询、7 次 503），顺序执行，未做压力测试。

2026-09-23 多代码专项复测补充：

- 固定 2/5/10/20 代码矩阵中，2/5/10 持续 pending；20 代码批次反复返回 `200 + code=0 + 0 行 + X-Cache:HIT`，但同批逐代码已有 14 只非空，属于可复现的伪空终态。最近交易日 2026-09-22 也复现：批次空命中时逐代码已有 14/20、合计 3241 行。
- 因此当前逗号多代码请求不具备安全终止条件，不能用于正式 ingestion，也不应用作未经证明的预热请求。
- 安全候选仍是单代码切片；14 个热缓存切片并发 1/2/4/8 的小样本墙钟为 13.041/5.626/9.464/1.079 秒。建议后续受控验证“最多 8 个在途 + 启动速率 ≤180 次/分”，全市场单轮理论下限约 30.74 分钟；冷缓存整日收敛时间仍无固定上界。

`stock_basic` 全状态补测：精确传入 `list_status=L,D,P,G,UN`、固定 8 个字段、`limit=10000, offset=0`，单次返回 5917 个唯一代码（L 5568 / D 339 / UN 10，当前 P/G 为 0），连续两次成功响应逐行一致。省略状态、逐状态加交易所和 `limit=5000` 分页均观察到假空、混入其他状态、重复或字段漂移，正式任务只使用该已验证单页形态并做快照校验。

上市／退市边界补测：D 按 SSE/SZSE/BSE 分别为 147/187/5 行，`delist_date` 非空率 100%，并集与全状态 D 子集逐行一致；上市首日分钟正例支持左端包含，公告和行情证明 `delist_date` 是摘牌日而非最后交易日。历史证券集合使用 `list_date <= t < delist_date`；摘牌日／之后分钟请求实测会 pending、返回零量骨架或机械复制非零量价的假数据，必须在计划阶段排除。
