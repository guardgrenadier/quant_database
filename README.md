# quant_data：重新开发起点

2026-10-12 已清理旧项目。当前仓库只保留数据商接口经验、原始响应证据和已下载的历史数据；没有应用源码、下载入口、dbt 模型、测试框架或旧架构方案。重建时重新决定实现与数据处理口径。

## 数据商接口经验

- [ProMax 调用手册](docs/research/promax.md)：供应商提供的历史版本手册，含供应商调用示例。
- [官方示例兼容性抽样](docs/research/tushare-proxy-official-examples-2026-09-19.md)：2026-09-19 的参数和响应观察。
- [端点拉取经验](docs/research/tushare-pull-strategy.md)：2026-09-19 的接口经验；部分结论已被后续复测更新。
- [分钟 API 实测报告](docs/research/minute-api-request-experiments.md)：分钟、主数据、分页、pending、假空结果及上市退市边界实验。
- [实验响应与日志说明](research/minute-api/README.md)：本地原始证据位于 `research/minute-api/tmp/`。

以上文档中的接口行为以实验日期、样本和供应商手册版本为界，不代表当前承诺。旧方案中的 canonical、dbt、刷新和发布建议不作为重建约束。

## 已下载数据（本地，不入 Git）

- `data/raw/stock_1min/year=YYYY/YYYYMMDD.parquet`：3,954 个网盘历史文件，2010-01-04 至 2026-04-17，约 53 GiB；保留 `extraction_manifest.json`。详见 [原始文件勘察](docs/research/historical-parquet-survey-2026-09-29.md)。
- `20260903-1min.parquet`：原有单日历史文件。
- `data/raw/stk_mins/`、`data/raw/v2/`、`data/raw/v2_isolated_20260929/`：保留原有 API 下载响应及伴随请求日志。目录名只是来源标识，不能据此认为数据已验证完整。
- `.env`：保留本地数据商凭据，不提交。

原始数据保持原路径、原内容。Git 克隆不包含这些本地文件。

## 旧项目归档

旧代码、架构、测试、性能实验、依赖配置以及清理前未提交的设计修改和编辑器配置，保存在 Git 分支 [`archive/pre-restart-2026-10-12`](https://github.com/guardgrenadier/quant_database/tree/archive/pre-restart-2026-10-12)，归档提交为 `8f6ebbd`。如需查看，请在项目目录外检出该分支，避免旧实现重新进入开发工作区。

旧运行环境、数据库、加工数据和其他被移出的本地资产保存在项目外的 `../quant_data-legacy-local-2026-10-12/`；其中 `preserved-data-inventory.json` 记录清理前保留数据的路径、大小与修改时间，用于核对。归档不代表推荐实现。

GitHub 仓库为 [guardgrenadier/quant_database](https://github.com/guardgrenadier/quant_database)。旧代码归档分支已推送；`main` 保存清理后的重新开发起点。历史数据、凭据和项目外本地备份不上传。
