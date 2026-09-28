# Ingestion 架构

状态：已废止（冻结保留，2026-09-25）。现行设计决策见 [ingestion-design-v2.md](ingestion-design-v2.md)；本文保留作讨论素材，与 V2 冲突处以 V2 为准。

## 要解决的问题

Ingestion 把供应商 API 的数据变成可信的本地数据资产。架构必须满足：

1. **pipeline 与具体 API 解耦。** 当前 Tushare 兼容源质量不佳，未来可能整体换源，也可能不同 dataset 用不同源。换源的修改范围必须收敛到可枚举的少数文件。
2. **校验职责清晰。** 抓取层保证"忠实且完整地问与答"，语义层保证"数据内容成立"，互不越权。

## 存储分层与两类检查

```text
API
 ↓
raw temp Parquet（原样字段 + 抓取元信息 + manifest，缓存 30 天）
 ↓ dbt 标准化与检查
canonical Parquet（长期唯一明细）
 ↓
消费 view / dbt 特征表
```

系统里存在两类完全不同的检查：

| | A：抓取保真与完整 | B：语义与质量 |
| --- | --- | --- |
| 回答的问题 | 供应商是否明确回答了我问的每一个问题 | 这些回答的内容是否成立 |
| 位置 | layer 1（client / adapter / pipeline） | layer 2/3（dbt） |
| 失败含义 | raw 不可信，需要重抓 | raw 是忠实的事实记录，阻止 canonical 发布，重抓无意义 |
| 典型项 | envelope 有效、分页按协议终止、计划切片全部有终态、落盘成功 | 主键唯一、类型与单位、OHLC 关系、量额非负 |

两条规则：

- **layer 1 不做语义过滤。** 即使负价格这类明显异常，只要响应协议有效就原样落盘。抓取层没有知识区分"无效行"与"供应商的特殊合法表达"（实测案例：午间填充 bar 机械重复前一根量价，抓取层无法也不应识别它）。
- **layer 1 发布后不再怀疑抓取。** 交易日进入 `fetched` 状态 = 计划中每个切片都有确定性结果（rows / 合法 empty）且已原样落盘。交接载体 = raw 文件 + manifest（请求计划 + 每切片结果）。dbt 的行数核对是与 manifest 对账、验证**转换过程**没有丢行，不是重新检查抓取完整性。

## 组件与边界

```text
Vendor HTTP API
  ↓
Client（传输 wrapper）          —— 换 vendor 必换
  ↓ rows / pending / empty
Source Adapter（源适配器）       —— 换 vendor 或换 dataset 都要换
  ↓ SliceResult
Pipeline（数据集管线）           —— 换 vendor 不动，换 dataset 重写
  ↓
raw temp Parquet + manifest
  ↓ dbt
canonical 候选 → 检查 → 发布
```

归属判别规则：**换 vendor 会改的属于 client/adapter；换 dataset 会改的属于 pipeline；两者都不变的属于共享机器（raw store / publish / audit）。**

### Client（传输 wrapper）

职责：如何可靠执行一次 API 调用。

| 有 | 明确不做 |
| --- | --- |
| base_url / API key / session / timeout / TLS | 分页 |
| envelope 与结构校验（fields/items 对齐、字段名唯一） | dataset 完整性判断 |
| 错误分类：Authentication / Transport / Business | 语义检查（值域、OHLC 关系） |
| 有限重试：429/502/503/504 与连接错误，指数退避 | 落盘、增量状态 |
| 日志脱敏（key 与参数） | checkpoint / resume |
| 把 vendor 特殊状态作为一等公民返回 | 主动 QPS 限流（V1 靠 429 重试被动适应） |

现有 `TushareClient` 需要的改动：

1. **pending 是一等结果，不是异常。** 实测 HTTP 202 `minute_data_pending` 在补抓期间是常态（实验中 81/120 次请求），当前 client 会把它当 `TushareError` 抛出，必须改。`query()` 返回结果对象（rows / pending(retry_after) / empty），硬错误仍走异常。用异常建模 pending 会让上层 catch 逻辑掩盖真正的错误。
2. **暴露响应头**（`Retry-After`、`X-RateLimit-Remaining`、`X-Request-ID`），供等待决策与追溯。
3. **裸 503 的重试预算放大到分钟级。** 实测网关级裸 503（无 JSON、无响应头）约 3.5 分钟自愈，当前 2 次秒级退避扛不住。仍是有限重试，只是预算放大。

### Source Adapter（源适配器）

契约：把一个切片**完整**取回来。pipeline 只用领域词汇（证券代码、交易日）表达计划，切片→请求的翻译（参数名、日期格式、窗口）封在 adapter 内。

```python
class MinuteSource(Protocol):
    def fetch_slice(self, secu_code: str, trade_date: date) -> SliceResult: ...

@dataclass
class SliceResult:
    status: Literal["ok", "empty", "pending", "failed"]
    rows: list[dict]             # vendor 原样字段，不改名不清洗
    requests: list[RequestMeta]  # 实际请求记录，写入 manifest
    retry_after: float | None
    error: str | None
```

- **分页归 adapter（已决策）。** 分页协议是纯 vendor 知识：当前源是 offset + "尾页短于 limit 终止"，下一个源可能是 cursor、时间窗或单请求全天。pipeline 持有分页逻辑意味着换源要改 pipeline，缝就漏了。将来多代码批量等优化同样封在 adapter 内，pipeline 无感知。
- adapter 做**协议级**切片校验（页间无重叠、返回代码/日期在请求范围内、字段齐全），违反则标 `failed`；不做语义判断。
- `empty` 只是"供应商返回了有效的 0 行"；empty 是否**合法**由 pipeline 结合日历与上市信息判断。

### Pipeline（数据集管线）

职责：问什么问题、是否都问完了、答案放在哪。

- 用领域词汇制定请求计划；universe、日历是独立的窄依赖，构造时注入。
- 编排与 resume：切片状态持久化在 manifest，逐切片落盘，中断后按 manifest 续跑。
- 日级完整性 gate：所有切片 ∈ {ok, 合法 empty}、零 failed、零 pending，才进入 `fetched`。
- 调用 dbt 生成候选并检查；负责正式发布与登记；维护审计表；清理到期 raw 缓存。

### dbt

字段重命名、类型与单位转换、日期派生、语义与质量检查、消费 view、特征模型。raw→canonical 的映射天然 vendor 相关（raw 保持 vendor 原样字段），staging model 按源组织（如 `stg_minute__tushare`），汇入 vendor 中立的 canonical schema。

## 多源与换源

两条 vendor 中立缝：**SliceResult 是抓取侧的缝，canonical schema 是存储侧的缝。**

- **换 vendor**：新 Client + 新 Adapter + 新增一个 staging model。pipeline、publish、audit、canonical、view 全部不动。
- **不同 dataset 不同源**：universe、日历等窄依赖各自注入，在 run 脚本（composition root）里组装。不引入 registry / factory / 配置路由框架。
- **接口验证策略（已决策）**：当前按单一实测源（ProMax 网关）设计并先用现源跑通；第二个真实源出现时再用它校验接口，接受小幅修订。

## pending 编排（已决策：run 内不等，多 run 收敛）

- run 内**不做**同步等待与轮询：切片返回 pending 就记录状态、继续下一个。
- 请求顺序：把计划内切片依次各触发一次，让服务端补数任务并行推进，而不是串行死等单个切片。
- run 结束时仍有 pending/failed：当日停在未 `fetched`，manifest 记录现场；下一次运行（手动或 cron 调同一脚本）直接重试未完成切片——服务端补数可能已完成，重访通常是一次缓存命中。
- 理由：理解负担低，没有 run 内等待预算和状态机；实测补数耗时不可控（10 分钟到 50+ 分钟），任何 run 内预算都不合理。

## 批量历史导入

网盘等批量文件是另一种 capture 机制，**不走 adapter**：一次性脚本 → 候选 canonical → 同一套 dbt 检查 → 同一套 publish。共享的是检查与发布机制，不为它扭曲抓取接口。

## 代码结构

```text
src/quant_db/
  sources/
    tushare/
      client.py      # 传输 wrapper（现有 tushare.py 迁入 + 上述三项改动）
      minute.py      # stk_mins adapter：切片→请求、分页、pending/empty
      universe.py    # stock_basic / trade_cal / suspend_d
  ingestion/
    minute.py        # 分钟 pipeline：计划、编排、完整性 gate、调用 dbt
    raw_store.py     # vendor 中立的 raw parquet + manifest 读写
    audit.py         # 审计表
    publish.py       # 候选→正式替换 + 登记（批量导入脚本复用）
```
