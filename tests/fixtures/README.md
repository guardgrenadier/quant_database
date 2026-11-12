# 真实响应测试夹具

这 48 个 JSON（18,880,131 字节，约 18 MiB）于 2026-11-03 从本地探查产物复制，
供 8 个测试模块离线回放。文件逐字节保留，未重取、裁剪、转换或补造数据；
不包含验证数据库、Raw/Candidate 导出或其他 debug 产物。它们是固定历史样本，
不代表实时供应商可用性、生产数据覆盖或当日数据终态。

来源均位于原 `data/probes/` 下；原目录后续可以清理，测试不再依赖其存在。

| 夹具目录 | 文件数 | 原探查目录／文件 |
|---|---:|---|
| `daily/` | 3 | `daily-onboarding-20261022T012318968173/requests/` |
| `daily_basic/` | 5 | `daily-basic-onboarding-20261030T081207214252/requests/` |
| `adj_factor/` | 8 | `adj-factor-onboarding-20261030T083849971994/requests/` |
| `index_basic/` | 14 | `st-index-20261021T220900218316/requests/` |
| `index_member_all/` | 11 | `index-member-all-onboarding-20261102/requests/` |
| `index_weight/` | 1 | `amazingdata-recovery-20261019-114800/index_weight_raw_000300_202609_single.json` |
| `stk_limit/` | 4 | `stk-limit-onboarding-20261029T213351571183/requests/` |
| `stock_st/` | 2 | `st-index-20261020-main/requests/` |

迁移前检查未发现本机 Tushare token、银河用户名／密码，亦未发现未脱敏的常见凭据字段。

`SHA256SUMS` 记录每个源文件迁移时的 SHA-256；从本目录执行
`sha256sum -c SHA256SUMS` 可检查副本是否保持一致。后续修改样本需明确说明来源和原因。
这些夹具随仓库保存，缺失时对应测试必须失败，不能按可选本地样本跳过。

`test_ingestion.py` 原有的 stock_basic 回放仍指向一个迁移前就已缺失的 walkthrough，
不属于本次 48 个文件；其既有跳过保留，不以其他样本或合成数据冒充原基线。
