# index_basic ingestion 契约与验收

更新：2026-10-22。状态：**按用户要求简化为宽基白名单逐代码拉取、申万按类别及发布方拉取。**

依据：[核心架构](core_design.md)、[实施契约](ingestion_v3.2_implementation.md)、[接入清单](dataset_onboarding_checklist.md)、[协作规则](../../AGENTS.md)。沿用内核，不新增迁移、兼容、共享模型或注册框架。

## 1. 已确认的范围及事实

用户要求暂不纳入国证系列，宽基用代码白名单显式获取，申万用 category/publisher 获取；另明确确认移除原白名单中的国证1000、国证2000、国证A指，宽基剩 24 个。

| 集合 | 请求参数 | 范围 |
|---|---|---|
| 常用宽基 | 每个代码分别传 ts_code | Dataset 的 BROAD_CODES 白名单，24 个代码 |
| 申万行业 | category=行业指数，publisher=上海申银万国证券研究所有限公司 | 供应商当前按这两个条件报告的全部基础记录 |

本轮不获取国证风格／策略。宽基名单包含沪深300、中证500／800／1000／2000、A50／A100／A500、全指，上证、深证、科创、创业板常用基准及北证50；一个指数使用已核对的一个代码，不自动合并同名代码。完整名单见 [Dataset](../../src/quant_data/datasets/index_basic.py)。

一行仍表示该代码的当前基础属性，不是行情、成分、权重或历史每日快照。申万包含符合过滤条件的历史／已终止记录，不自行排除 exp_date 非空行。**不再按 SW2021 或 L1/L2/L3 重建目标，也不保证各层级独立完整。** 特钢Ⅲ之类不在基础列表的代码不会由外部分类补入。

原全量和分类关联方案已被本次明确修改取代，不保留兼容模式。Dataset 不接收任意子集参数；Scope / Publication Unit 为一个固定目标 SNAPSHOT。无日历或股票表依赖，无已发布上游版本 pin。

## 2. Schema、键及标准化

Canonical 仍是 canonical.index_basic，13 列及 ts_code 主键不变，完整约束见 [DDL](../../src/quant_data/datasets/index_basic.sql)。

- ts_code/name/market/publisher/category：必填非空白 VARCHAR；代码保留字符串，分类与发布方不新增枚举或拆分模型。
- fullname/index_type/weight_rule/desc：可空 VARCHAR；index_type 的正常 NULL 不单独告警，其余描述缺失汇总告警。
- base_date/list_date/exp_date：可空 DATE；前两列缺失告警，exp_date 的正常 NULL 不单独告警。保留供应商业务日期，不转换成上海交易日。
- base_point：可空 DOUBLE，缺失告警；零合法、负值告警、非有限值 ERROR，不换算或舍入。

Raw 为 13 个源字段加 `_requested_code`：宽基为可信请求代码，NULL 表示申万过滤请求；不再有混合来源分类行或 `_sw_` 列。日期在 Raw 仍是字符串，原响应及空白原值保留。

prepare 集中清理文本首尾空白、空串转 NULL，然后核对请求身份：宽基返回代码必须等于请求代码；申万必须同时匹配请求类别和完整发布方。错误过滤结果为 source.range / prepare ERROR，不靠本地过滤静默丢弃。

三列日期各解析一次，分别检查 ASCII 八位和真实历法；非法非空值不能变成合法 NULL。Candidate 只移除可信请求标签，保留 13 列，不再关联分类、选择并集或去重。

## 3. 获取、交付及缺失政策

[Adapter](../../src/quant_data/sources/tushare/index_basic.py) 仅调用 index_basic：24 个代码请求，加一个申万过滤请求。全部显式请求 13 字段；按返回字段名重排，不依赖列序。请求起始间隔至少 1.3 秒，超时及有限重试沿用 Client，每次尝试即时留脱敏证据。

每个响应须 has_more=false 且少于官方单次 8,000 行上限；缺标记、true 或触顶为 INCOMPLETE / ERROR，**不自动分页或分片 fallback**。这足以覆盖本次每宽基 1 行、申万 769 行的实际规模；若以后触顶，须显式修订获取方式。count 不参与完整性判断。

缺字段不能补 NULL；源文本只接受字符串／NULL，基点只接受 JSON int/float/NULL，拒绝布尔或字符串数值。缺列／类型问题作为 prepare 诊断，不改写确已完成的获取。请求异常或不完整交付停止后续请求，整个快照不能部分发布。

| 级别 | 规则 |
|---|---|
| ERROR | 白名单任一码缺记录，index.broad_missing，validation 阶段 |
| ERROR | 申万过滤集合零行，index.sw_nonempty，不以仅宽基的新表清除旧行业集合 |
| ERROR | 请求／截断、缺列／坏类型、返回范围错误、非法日期、必填属性缺失、重复代码、非有限基点 |
| WARNING | 约定的可选属性缺失、负基点、未来业务日期、exp_date 早于 base_date/list_date |

不检查固定申万数量或层级；不再有分类查询失败、三级缺失告警或集合选择审计。整个基础响应零行时，仍有明确 Schema 的 Raw/Candidate，COMPLETE 与必需宽基／申万非空验收失败分别表达。基期晚于发布日期保留，不告警或阻断。

## 4. 发布、消费与证据边界

所有请求和验收通过后，单 SNAPSHOT 整表原子替换，移除旧国证记录、非白名单宽基及不再满足申万过滤的记录；相同内容重跑生成新 Publication，不累加。失败保护旧 Canonical、Coverage 指针和成功 Publication。范围变化已由用户授权，不增加迁移逻辑。

消费者结合 index_basic / SNAPSHOT 的 PUBLISHED Coverage 判断当前这两个集合是否成功发布。成功只保证约定请求与规则，不保证供应商现实全集、跨请求同一时点、历史 PIT 或申万各层独立完整；未知代码的缺失不能解释为历史从未存在。

官方参数和上限见 [index_basic 文档](https://tushare.pro/document/2?doc_id=94)，历史来源证据见[接口研究](../research/tushare-official-st-index-2026-10-20.md)。本轮只读补证 `data/probes/index-basic-simple-scope-20261022T001039227148/` 验证申万组合过滤为 769 行，独立宽基请求为 1 行，均验证后续空页；生产实现按已知单次边界获取，不保留研究分页循环。

## 5. 运行与验收

CLI 和内核接口不变，未加入新依赖包、共享抽象或接口；不实现 index_daily / sw_daily、调度、历史版本库或 Serving。修改限于 Dataset、Adapter、测试及相关文档，DDL 仅更新范围注释。

```bash
uv run --frozen python -m quant_data ingest index_basic \
  --db data/probes/my-index-basic/warehouse.duckdb \
  --landing-dir data/probes/my-index-basic/landing --keep-artifacts
uv run --frozen python -m unittest discover -s tests -v
```

7 项必要测试通过，覆盖显式请求计划、重复运行与旧非目标删除、宽基缺失／申万零行、请求／字段／类型／范围／截断错误、业务日期／数值／重复键、Raw／证据／回滚、CLI 及真实源记录的过滤重放。全套 **72 项中 71 项通过、1 项既有股票真实样本缺失跳过**；环境烟测通过。

独立目录 `data/probes/index-basic-simple-validation-20261022T001501794025/` 保存 warehouse.duckdb、验证驱动、首跑／重跑 CLI 结果及 landing/<run_id>。两次正式 CLI 各发 25 个真实请求（24 个 ts_code＋1 个申万组合过滤），均 COMPLETE / SUCCESS / CLI 0，各发布 **793 行＝24 个宽基＋769 条申万行业记录**，Publication 依次为 1、2；重跑逐字段一致、不累加。

两个成功 Run 的源响应独立标准化结果、Candidate、Canonical、请求审计、Coverage 与 Publication 均核对一致，artifacts_complete=true。明确标注为无 HTTP 的三次离线注入（白名单宽基缺失、申万过滤零行、请求失败）均 FAILED 且保护原数据与版本指针，证据保留。**340 个 manifest 文件条目的 SHA-256 全部核对通过。** 未写默认权威库；源业务行、数据库和原始信封只存本地。

Dataset / Adapter 从 129＋164＝293 行缩至 96＋110＝206 行，减少 87 行；测试从 362 行缩至 209 行，减少 153 行。删除三层辅助请求、分类校验／关联、选择函数、混合 Raw 与对应测试，没有新增生产抽象。机器可读结果见 [index-basic-simple-summary-2026-10-22.json](../research/index-basic-simple-summary-2026-10-22.json)。
