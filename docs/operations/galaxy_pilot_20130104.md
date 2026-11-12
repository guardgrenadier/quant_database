# 2013-01-04 历史 Pilot 交接

**当前状态：A 阶段已完成；B 脚本已准备、仅离线验证，尚未执行。存在 19 个已知证据缺口，需用户决定先核查，或明确接受按原契约运行受控 PARTIAL Pilot。不要直接照脚本开始全市场获取。**

2026-11-04 已完成只读取证：41 次 Tushare 请求及逐只公告核查支持十九只处于历史暂停上市／终止上市过渡的持续非交易区间，生产库 SHA-256 未变。后续需要确认历史状态的接入和分钟处理语义；结论与证据见[核查报告](../research/p1-listing-status-gap-investigation-2026-11-04.md)。下文保留 A 阶段和交接准备时的检查记录，B 仍未执行。

## A 阶段实际结果

权威库为 `/home/xindingtian/code/quant_data/data/warehouse.duckdb`，复用四份已发布快照和 SSE 日历，没有导入旧文件或重建库。

| Dataset | 2013-01-04 行数 | 成功 Unit 数 |
|---|---:|---:|
| suspend_daily | 58 | 1 |
| stock_st | 101 | 1 |
| stock_daily | 2,406 | 1 |
| daily_basic | 2,406 | 1 |
| adj_factor | 2,477 | 1 |
| stk_limit | 2,455 | 1 |
| index_daily | 571 | 793 |

七次 Run 均为 SUCCESS，31 次请求全部成功，新增 799 个成功 Unit、零 ERROR；指数另有 222 个合法零行 Unit。三条 WARNING 为申万 Raw 中 34 行范围外记录的显式排除、stk_limit 一行 pre_close=NULL 和五十行 pre_close=0，均沿用既有契约保留。

原有 11,498 个 Coverage 指针／状态与十四张 Canonical 的原范围集合摘要保持不变。成功 Run 暂存默认清理，审计保留。当前成功 Coverage 共 12,292 个，权重五个 FAILED Unit 仍保留；本日分钟尚无 Run 或 Publication，尚未运行本轮 dbt 复测。

## 分钟依赖与已知阻断

五项成功依赖均已满足。现有 Scope 解析产生 2,472 个 IN_SCOPE Unit、2,422 个请求 Unit、50 个本地全天停牌 Unit，Scope UNKNOWN 为零。

**Scope UNKNOWN 为零不等于后续业务验收一定通过。** 请求集合中以下十九只股票既无本日 stock_daily，也无本日 suspend_daily：

```text
000038.SZ 000403.SZ 000498.SZ 000545.SZ 000622.SZ
000657.SZ 000670.SZ 000672.SZ 000681.SZ 000688.SZ
000693.SZ 000757.SZ 000787.SZ 000805.SZ 000820.SZ
000831.SZ 600556.SH 600698.SH 600817.SH
```

按 `GalaxyStock1MinAdapter._source_checks` 的既有规则，银河完整交付有分钟时会触发 `source.daily_conflict`，无分钟时触发 `source.empty_ambiguous`；获取失败则按获取错误阻断。因此在现有事实和规则下，这十九个 Unit 无法发布成功，不能预期完整 SUCCESS。

历史停牌／暂停上市状态或源交付缺失是待核查方向，仅凭现有事实不能断言原因。不能使用当前股票名称／上市状态代替 2013 年状态，不能自动排除这些股票、补造停牌行、允许全 NO_TRADE 或将缺日线规则降为告警。

下一步可选：

1. **推荐：先核查十九只的历史状态和供应商证据。** 查清后报告是否涉及已有 Scope／停牌语义缺口，再由用户确认最小处理方案。
2. 明确接受现有契约下的受控 PARTIAL Pilot，执行全市场单日获取，保留十九个及其他实际失败 Unit，仅发布合格范围，同时取得历史交付与资源样本；不能据此宣称该日已完整回补。

## B 阶段执行材料（待上述决策）

脚本为 `scripts/run_galaxy_pilot_20130104.sh`，固定 2013-01-04 全市场 stock_1min、并行度 2，不接受额外参数、不请求权重、不刷新上游。用户确认后，由可直连执行 agent 关闭系统代理/TUN，并确认权威库无其他连接，再在工作区运行一次：

```bash
cd /home/xindingtian/code/quant_data
bash scripts/run_galaxy_pilot_20130104.sh
```

脚本使用现有环境及权威库绝对路径，默认产物生命周期不变。任一非零结局（包括 PARTIAL）原样交回，不改验收、不重跑、不清库、不运行 dbt 或触及其他日期。

日志目录为 `data/landing/galaxy-pilot-20130104-<上海时间>-<PID>/`：

- `stock_1min.json`、`stock_1min.stderr.log`、`stock_1min.exit-code`：正式 CLI 输出和实际退出码。
- `script.exit-code`：整个脚本退出码；监测失败或中断时，分钟退出码可能尚未生成，不应推断命令成功。
- `resources.csv`：约每两秒记录 CLI 父进程及后代 RSS 总和、系统 MemAvailable、权威库文件大小和整个 Landing 分配大小，单位为字节。

RSS 是采样估计，可能遗漏瞬时峰值且共享页可能重复计入；不包含执行 agent／编辑器。Landing 统计遇到并发清理失败以 -1 明确标记，不能当作零空间占用。仅记录资源，不改变业务规则或按资源数值自动终止任务；手工中断或监测命令失败会收尾脚本自行创建的 CLI 进程组。

交回日志目录、两个退出码及异常摘要，确认所有子进程结束并释放库连接。协调 agent 在 B 期间不访问权威库；交回后按实际失败范围验收，再决定是否推进 C。无新授权不得循环重跑。
