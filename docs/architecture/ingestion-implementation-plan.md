# Ingestion V2 落地顺序

每阶段单独提交并 review；旧 `src/quant_db/`、`scripts/ingest_minute.py`、`dbt/`、`tests/` 保持冻结，新实现放在独立目录。以 [V2 决策集](ingestion-design-v2.md) 为验收口径。

后续新增实现统一使用中文函数说明和关键代码块注释，注释解释约束与恢复语义。

| 阶段 | 交付 | 本轮 review 重点 |
| --- | --- | --- |
| 1. 状态账与 raw | 固定计划、切片状态、逐页 JSON 原文 gzip、原子写入和离线恢复检查 | 同计划续跑、不同计划拒绝、原文不变、未决不算完整 |
| 2. 基础数据集 | `stock_basic`、`trade_cal`、`suspend_d` 独立 raw → temp → canonical 链路与编排 | 全状态快照、开市日判定、发布前后文件/账一致 |
| 3. 分钟 L1 | D6 切片计划、单代码分页、≤8 并发与全局限流、pending 跨 run 续抓 | 越窗/冲突拦截、相同重复行、失败恢复、请求预算 |
| 4. 分钟 L2 与发布 | dbt 转换和分级检查、默认 view、`quality_findings`、候选原子替换及崩溃核对 | 金额分口径、负量额告警、行数对账、整日替换 |
| 5. 运维与历史 | T+1 观察刷新、30 天清理、网盘勘察/一次性导入、gap 补抓 | 修订留痕、清理护栏、历史信任级别和抽样对比 |

当前完成阶段 4：分钟 canonical 候选、阻断级 dbt 检查、语义告警、原子发布、默认 view 和从 raw 整日重建已实现。T+1 重新请求、30 天清理与历史导入属于阶段 5；历史导入先产出勘察报告后执行。阶段 2 的 `trade_cal` 快照覆盖目标年份的 SSE 日历，`suspend_d` 只取目标日 `suspend_type=S`；三个 dataset 各自按目标日发布一个 Parquet 文件。
