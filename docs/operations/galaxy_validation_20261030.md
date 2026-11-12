# 2026-10-30 银河分钟补跑交接

**当前状态：B 阶段补跑及 C 阶段验收已完成。以下命令保留为操作记录，不应未经新的执行确认再次运行。权重当日缺口仍未关闭。**

本文件交接用户已确认的 B 阶段分钟补跑任务。原权重任务返回空数据后停止，分钟尚未执行；用户现已确认银河权重因供应商问题滞后约一个月，2026-10-30 无权重数据，要求先补齐分钟再进入 C。本轮只验证一个交易日，不是全历史回补或全量生产就绪验收。

## 固定范围与入口

- 当前工作区：`/home/xindingtian/code/quant_data`。
- 权威库：`/home/xindingtian/code/quant_data/data/warehouse.duckdb`。
- 脚本：`scripts/run_galaxy_validation_20261030.sh`，使用本仓库已有 `.venv/bin/python`。
- 业务日期：`20261030`，由已发布 SSE 日历确认开市。
- 仅运行全市场 `stock_1min`，固定 `--parallel 2`；股票日 Scope 由正式 Dataset 和已发布上游确定，不手工删证券。
- 使用银河正式 Adapter/Client；不通过探针、手工 SDK 调用或旧样本替代真实交付。
- 不请求 `index_weight`，不修改其失败审计或五条 FAILED Coverage，不用其他日期的权重填补本日。权重不是分钟 Dataset 的上游，现有依赖与验收契约不变。

## A 阶段已核验状态（2026-11-03）

七项 Tushare 日频 Run 均为 SUCCESS，34 次请求全部成功、零 ERROR，共新增 799 个成功 Unit。股票日线 5,571 行、停复牌 7 行，其余结果见根目录 README。两条 WARNING 为 daily_basic 三行股本关系异常和申万 Raw 中 25 行范围外记录的显式排除，均按既有契约处理。

只读执行正式分钟依赖检查和 Scope 解析，得到 5,577 个 IN_SCOPE 股票日、5,571 个需要银河请求的股票日、零 UNKNOWN；其余六个股票日为无冲突全天停牌，按既有规则本地交付。上述数量是这次上游快照下的预检结果，不作为脚本中的硬编码或供应商验收门槛。补跑准备时 index_weight 已有两次 FAILED Run、五条 FAILED Coverage、零成功 Publication；stock_1min 尚无 Run 或已发布记录，Serving 未构建。

## 执行前

1. 确认协调 agent 已结束 A 阶段，并已释放数据库连接。A 阶段负责成功发布当日 suspend_daily、stock_st、stock_daily、daily_basic、adj_factor、stk_limit 和 index_daily，以及核对原有五项基础数据；其中分钟实际依赖由既有 Dataset 复核。
2. 关闭系统代理／TUN；脚本和 worker 只清理进程环境变量，不能绕过系统路由。执行者须能在关闭代理时继续运行本地批处理。
3. 断开 VS Code 数据库扩展、研究脚本和其他进程对权威库的连接。整个 B 阶段只有本脚本串行访问权威库；协调 agent 在收到完成交接前不访问该库。
4. 沿用项目根目录 `.env` 或已有环境中的银河凭据，不输出、复制或修改凭据；不安装依赖、不修改代码／契约。

## 执行

在终端中运行一次：

```bash
cd /home/xindingtian/code/quant_data
bash scripts/run_galaxy_validation_20261030.sh
```

脚本按自身位置确定工作区，并向分钟正式 CLI 命令传递权威库绝对路径。它不切换系统代理，也不创建或清空数据库。业务范围已写死为本次确认任务，不接受额外参数。

开始时打印本次日志目录：

```text
data/landing/galaxy-minute-validation-20261030-<上海时间>-<PID>/
```

保存 `stock_1min.json`（CLI 标准输出）、`stock_1min.stderr.log` 和 `stock_1min.exit-code`。其中 JSON 需由 C 阶段解析为 RunResult 核对，不能仅凭文件存在判定成功。日志目录仅本用户可访问，不入 Git，之前的失败日志继续保留。

沿用正式 CLI 的默认产物生命周期：SUCCESS 清理该 Run 的请求／阶段暂存；FAILED/PARTIAL 保留该 Run 的证据。上述交接日志目录单独保留。CLI 自身的有限重试与会话恢复沿用既有机制，脚本不额外循环重跑。

## 失败处理与交回

- 分钟命令非零即停止；PARTIAL 同样停止。不要因此手工清库、补造记录、放宽校验或直接重跑。
- 若脚本中断或尚未写出退出码，交回实际情况及已有日志；不要将缺少结果解释为成功。
- 交回本次日志目录绝对路径、脚本退出码及异常摘要，不粘贴凭据或整份供应商响应。
- 确认脚本和子进程均已结束、数据库连接已释放后，再恢复代理并通知协调 agent。若仍有进程运行，明确说明，不同时启动新的读写任务。

C 阶段由协调 agent 只读核对分钟 Run、逐 Unit 状态、Canonical、Publication、Coverage、依赖版本和检查结果。分钟成功交回后再串行运行 dbt，验证当日日线行业 Serving；权重缺口独立保留，不阻断这两项验收。脚本返回零只代表 CLI 业务成功，不代替交叉验收，也不关闭既有 T+N、沪深新股首日及容量／读取性能补验门槛。2026-10-30 权重仍未成功覆盖，不能据分钟或 dbt 成功宣称本日十四个 Dataset 全部齐备。

## B／C 实际验收结果（2026-11-03）

- 分钟 Run ID：`7fe3ff0d-9b52-40c5-8db4-2fc5e51c8af8`，以实际 CLI JSON 和 Meta 为准。
- Run 为 SUCCESS，5,577 个 Unit 全部 PUBLISHED，共 1,344,709 行；CLI 输出、逐 Unit 审计、Publication、Coverage 与实际行数一致。5,571 次 K 线请求及 28 次快照请求全部成功。
- TRADED 1,291,889 行、NO_TRADE 51,380 行、SUSPENDED 1,440 行。50 条告警均为 `snapshot.zero_auction`，无 ERROR。
- 沪深 5,223 个股票日、每个 240 个连续槽位；北交所 354 个股票日、每个 242 个连续槽位；另有 5,521 个可选竞价行。复用正式 Dataset 只读复查完整网格、业务键、状态字段和参考价序列，均通过。
- 五项上游实际 Publication 版本与 Run Dependency 一致；原有 5,916 个成功 Coverage 指针保持不变。成功 Run 暂存已按默认生命周期清理，交接日志保留在 `data/landing/galaxy-minute-validation-20261030-20261103T213042+0800-70002/`。
- 首次 dbt 构建被 VS Code PID 727 的只读文件锁阻挡，未取得写连接；用户断开后按原配置重试成功。发布门禁、View 构建和两项数据测试均通过，无告警或错误。
- `serving.stock_daily_with_sw_industry` 为普通 View，5,571 条日线全部匹配行业，未匹配和重复键均为零；原行情字段双向 EXCEPT ALL 一致。构建前后 21 张 Canonical/Meta 表集合摘要不变。
- 当前 PUBLISHED Coverage 为 11,493 个，权重另有五个 FAILED Unit、零权重事实及成功 Publication；失败审计继续保留。

本次只验收 2026-10-30 单日链路。2013 年起历史回补、分钟生产补验项及权重供应商滞后问题仍需后续处理。

## 后续旧分钟清理（2026-11-03）

C 阶段完成后，用户逐项确认永久删除 `data/raw/`、根目录 `20260903-1min.parquet` 及 `20260903-callauction.parquet`。三项均已删除，共 3,957 个文件、约 52.24 GiB；生产权威库 SHA-256 未改变，48 个固定测试夹具哈希通过。本次清理没有执行数据回补、数据库写入或修改业务契约。生产分钟仍仅覆盖 2026-10-30；删除旧输入不表示已完成 2013 年起银河历史获取。
