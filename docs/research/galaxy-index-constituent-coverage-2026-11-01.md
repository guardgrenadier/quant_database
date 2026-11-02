# 银河 `get_index_constituent` 支持名单探查

日期：2026-11-01

## 结论

银河高层接口 `AmazingData.InfoData.get_index_constituent` 对应底层 `tgw.QueryThirdInfo`，功能号为 `A010200002`。本次使用底层接口完成只读实测：

- 官方 `EXTRA_INDEX_A` 候选池共 **636** 个指数，全部取得响应。
- 其中 **533** 个返回成分记录，**103** 个返回 `kDataEmpty(-76)`，**0** 个查询失败。
- 额外实测项目中证宽基 `932000.CSI`、`930050.CSI`、`000985.CSI`，均返回成分记录。
- 合计确认 **536 个指数有成分数据**。完整代码及名称见 `observed_supported.csv`。

沪深300、中证500、中证1000和中证2000的成分接口均已实测支持；中证2000没有每日权重接口支持，不代表没有成分数据。

## 方法

1. 通过 `BaseData.get_code_list(security_type="EXTRA_INDEX_A")` 获取官方候选池，并保存 `get_code_info` 返回的指数资料。
2. 逐指数调用底层 `QueryThirdInfo`，显式设置 `function_id=A010200002` 和 `index_code`；串行查询，单次回调超时30秒，请求间隔0.2秒。
3. 仅把有成分记录且返回 `INDEX_CODE` 与请求一致的结果列入支持名单；`kDataEmpty` 单列为本次空返回，不与客户端或传输错误混同。
4. 检查业务键 `(INDEX_CODE, CON_CODE, CON_INDATE)` 重复、`CURRENT_SIGN` 取值及返回代码错配。
5. 对候选池外的项目中证宽基做补充查询，单独标注证据范围。

本次536个支持项均有指数名称，未发现业务键重复、异常 `CURRENT_SIGN` 或返回指数代码错配。

## 目标指数结果

| 指数 | 代码 | 总记录 | 当前成分 | 历史成分 |
|---|---|---:|---:|---:|
| 沪深300 | `000300.SH` | 1,227 | 300 | 927 |
| 中证500 | `000905.SH` | 2,470 | 500 | 1,970 |
| 中证1000 | `000852.SH` | 3,441 | 1,000 | 2,441 |
| 中证2000 | `932000.CSI` | 3,257 | 2,000 | 1,257 |
| 中证A50 | `930050.CSI` | 71 | 50 | 21 |
| 中证全指 | `000985.CSI` | 6,521 | 5,117 | 1,404 |

中证2000必须使用 `932000.CSI`；补充查询 `932000.SH` 返回 `kDataEmpty(-76)`。

## 证据与产物

证据目录：`data/probes/galaxy-index-constituent-coverage-20261101-full/`

- `observed_supported.csv`：本次确认的536个支持项，区分官方候选池与补充查询。
- `supported.csv`：官方候选池内533个支持项。
- `no_data.csv`：官方候选池内103个空返回项。
- `errors.csv`：空文件，仅含表头。
- `summary.json`：数量、目标指数和质量检查汇总。
- `responses/`：636个候选指数的完整原始响应；补充CSI响应单独保存为 `supplemental_*.json`。

可恢复探针为 `scripts/probe_galaxy_index_constituent_coverage.py`。本次未修改正式 Dataset、共享接口或数据库。

## 边界

`observed_supported.csv` 是“636个官方候选池指数 + 3个项目CSI宽基”的完整实测名单，**不是所有可能指数代码的绝对穷举**。候选池外三只CSI指数均成功，已经证明官方候选池不是接口支持全集；本次未继续查询另一供应商名单中的9,053个CSI代码。

空返回只代表本次查询结果，不单独证明永久不支持。高层成分接口在当前环境存在 PyTables 缓存依赖问题，因此本探查采用已验证可用的底层只读接口。
