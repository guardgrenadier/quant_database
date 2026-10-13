# quant_data：重新开发起点

2026-10-12 已清理旧项目。当前仓库保留已下载的历史数据；没有应用源码、下载入口、dbt 模型、测试框架或旧架构方案。重建时重新决定实现与数据处理口径。

## 数据商接口

2026-10-13 起改用 tushare 官方 API（`.env` 中的 `TUSHARE_TOKEN`）。此前经 ProMax 中转站的全部接口研究（网关手册、分钟 API 实测、stock_basic/suspend_d 探查、原始响应证据与探针脚本）已归档至 Git 分支 `archive/promax-relay-2026-10-13`；那些结论描述的是中转站网关行为，不作为官方 API 的有效依据，官方接口的权限、信封、限流与字段语义需重新探查。

- [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)：网盘历史分钟文件的来源与 Schema，与中转站无关。

## 已下载数据（本地，不入 Git）

- `data/raw/stock_1min/year=YYYY/YYYYMMDD.parquet`：3,954 个网盘历史文件，2010-01-04 至 2026-04-17，约 53 GiB；保留 `extraction_manifest.json`。详见 [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)。
- `20260903-1min.parquet`：原有单日历史文件。
- `.env`：保留本地数据商凭据，不提交。

原始数据保持原路径、原内容。Git 克隆不包含这些本地文件。

## 旧项目归档

旧代码、架构、测试、性能实验、依赖配置以及清理前未提交的设计修改和编辑器配置，保存在 Git 分支 [`archive/pre-restart-2026-10-12`](https://github.com/guardgrenadier/quant_database/tree/archive/pre-restart-2026-10-12)，归档提交为 `8f6ebbd`。如需查看，请在项目目录外检出该分支，避免旧实现重新进入开发工作区。

ProMax 中转站研究资料（研究报告、探针脚本、原始响应证据）归档在分支 `archive/promax-relay-2026-10-13`，归档提交为 `f9fd057`。

旧运行环境、数据库、加工数据和其他被移出的本地资产保存在项目外的 `../quant_data-legacy-local-2026-10-12/`；其中 `preserved-data-inventory.json` 记录清理前保留数据的路径、大小与修改时间，用于核对。归档不代表推荐实现。

GitHub 仓库为 [guardgrenadier/quant_database](https://github.com/guardgrenadier/quant_database)。旧代码归档分支已推送；`main` 保存清理后的重新开发起点。历史数据、凭据和项目外本地备份不上传。
