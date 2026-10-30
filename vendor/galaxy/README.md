# 银河证券星耀数智 SDK

安装包来自券商提供的仓库 https://gitee.com/cgs2026/xysz ，固定提交：
`f0da75e7ff6b3fdde658184ee5c2795d4a4a47c2`。

| 安装包 | 仓库原始路径 | SHA-256 |
| --- | --- | --- |
| tgw-1.0.9.2-py3-none-any.whl | xysz/xysz_tools/tgw-1.0.9.2-py3-none-any.whl | cbc30194e2d3923c87e5d40ce469b79575758001f9d5f7481d46c29c9667e21d |
| AmazingData-1.1.15-cp313-none-any.whl | xysz/xysz_tools/AmazingData/AmazingData-1.1.15-cp313-none-any.whl | 092f9d857f78170ac1cc0365f9ab761f07364a6733035ed043455e3b8be20c98 |

Gitee 的 tgw 原始文件下载地址返回 HTTP 403，因此保留原始 wheel，使用
`pyproject.toml` 中的本地来源和 `uv.lock` 复现安装；安装包合计约 67 MiB。
在项目根目录运行 `uv sync --frozen` 即可安装，无需手动分步安装。
AmazingData 安装包对应 CPython 3.13；底层 tgw 包含本地动态库，其他系统需另行验证。

在项目根目录的 `.env` 填写 `AD_USERNAME` 和 `AD_PASSWORD`；
`AD_HOST` 默认 `101.230.159.235`，备用地址 `140.206.44.235`，`AD_PORT=8600`。
凭据不提交 Git。调用前需显式加载 `.env` 并执行 `AmazingData.login()`；
环境配置本身不会登录、查询或接入正式 Ingestion。
