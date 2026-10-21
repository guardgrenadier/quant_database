"""官方 stock_st 适配：逐交易日获取全市场完整集合，失败按日期隔离。"""

import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.stock_st import StockStDataset

# 交付边界：触及已知单次上限即阻断；本切片不自动分页补齐。
ROW_LIMIT = 1000
# 供应商覆盖边界：官方只声明此日起的历史，之前的空响应不能证明完整日集合。
HISTORY_START = "2000-01-01"
REQUEST_INTERVAL = 1.3


class TushareStockStAdapter:
    """复用通用 Client，不自行判断开市日，不按股票子集发布。"""

    def __init__(self, client):
        """显式请求全部五字段，原始日期保持字符串以供交付范围检查。"""
        self.client = client
        # 业务交付：默认字段或返回顺序不能替代正式契约字段列表。
        self.fields = tuple(StockStDataset.schema.names())
        # 业务类型：Raw 全部字符串或 NULL；请求日期标签只供归属及追溯，不进入 Canonical。
        self.raw_schema = pl.Schema({c: pl.String for c in (*self.fields, "_requested_date")})

    def fetch(self, scope, artifacts):
        """只请求 Scope 已确认的开市日，空交付保留 Schema 和 COMPLETE，是否发布由 Dataset 裁决。"""
        frames, attempts, checks, deliveries = [], [], [], []
        last_request = None

        def record(attempt, payload):
            """即时留存每次请求证据；证据写失败阻断该请求而非冒充交付完成。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for unit in scope.units:
            # 业务检查：超出已知历史覆盖的开市日不请求、不发布空集合；不影响其他可获取日期。
            if unit < HISTORY_START:
                checks.append(Check("ERROR", "source.history_start", "fetch", (unit,), {"earliest_date": HISTORY_START}))
                deliveries.append(UnitDelivery(unit, "INCOMPLETE"))
                continue
            # 业务转换：Scope 的 ISO 交易日转为供应商 YYYYMMDD，不传股票或类型筛选。
            params = {"trade_date": unit.replace("-", "")}
            request_id = f"stock-st-{unit}"
            # 交付策略：请求串行限频；超时与有限重试继续由 Client 负责。
            if last_request is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last_request)))
            last_request = time.monotonic()
            state = "COMPLETE"
            try:
                body = self.client.query("stock_st", params, self.fields,
                                         request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                # 业务影响：只阻断该交易日，仍继续获取其他日期，已有成功事实由 Publisher 保护。
                checks.append(Check("ERROR", "source.request", "fetch", (unit,), {"error": str(exc)}))
                state = "FAILED"
            else:
                data = body["data"]
                missing = sorted(set(self.fields) - set(data["fields"]))
                # 准备诊断：描述可空但不可缺列；保留获取完成事实，不补 NULL 冒充可用数据。
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", (unit,), {"missing": missing}))
                # 业务检查：has_more 缺失/为 true 或行数达到 1000 都不能证明完整；count 不可靠。
                if data.get("has_more") is not False or len(data["items"]) >= ROW_LIMIT:
                    checks.append(Check("ERROR", "source.truncated", "fetch", (unit,),
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                # 业务门禁：仅完整字段与交付标记通过的请求片段进入 Raw。
                if state == "COMPLETE" and not missing:
                    try:
                        # 业务映射：按字段名重排；严格字符串拒绝数值代码、日期或嵌套值隐式转码。
                        indices = [data["fields"].index(c) for c in self.fields]
                        arrays = [pa.array((row[i] for row in data["items"]), type=pa.string()) for i in indices]
                        raw = pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields))
                        # 业务追溯：标签使用请求目标日，即使供应商返回错日期也能定位受影响 Unit。
                        frames.append(raw.with_columns(pl.lit(unit).alias("_requested_date")))
                    except (pa.ArrowException, ValueError, TypeError) as exc:
                        # 准备诊断：字段类型非法阻断该日，获取状态仍独立保留；信封保留原值。
                        checks.append(Check("ERROR", "source.types", "prepare", (unit,), {"error": str(exc)}))
            # 业务交付：COMPLETE 独立于行数，完整零行也声明获取完成，Dataset 的非空政策仍会阻断发布。
            deliveries.append(UnitDelivery(unit, state, (request_id,)))
        # 业务批次：多个交易日共享 Raw；完整空响应仍有六列 Schema，无可解码响应则为 None。
        raw = pl.concat(frames) if frames else None
        return Delivery(raw, tuple(deliveries), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        """一次清理与日期匹配，保留历史名称和标签正文，集中返回标准化事实和诊断。"""
        # 业务输入：没有 Raw 与完整空响应不同，不以新建空表抹掉请求或解码失败。
        if delivery.frame is None:
            return None, ()
        # 业务转换：五字段文本清理一次，保留请求标签；空描述转 NULL，不去重或派生风险事件。
        clean = delivery.frame.with_columns(
            pl.col(c).str.strip_chars().replace("", None).alias(c) for c in self.fields)
        # 业务检查：去首尾空白后必须等于请求的合法 YYYYMMDD，NULL、七位和非法日期均失败。
        # 即便错日期恰好是本次另一个目标日，也不能把记录归给那个 Unit。
        clean = clean.with_columns((pl.col("trade_date")
                                    == pl.col("_requested_date").str.replace_all("-", "")).fill_null(False).alias("_date_matches"))
        invalid = clean.filter(~pl.col("_date_matches"))
        checks = ()
        if invalid.height:
            units = tuple(invalid["_requested_date"].unique().to_list())
            checks = (Check("ERROR", "source.range", "prepare", units, {"rows": invalid.height}),)
        # 业务隔离：复用匹配掩码，错日期行有明确 ERROR 且留在 Raw，不得污染另一个合格日期。
        clean = clean.filter(pl.col("_date_matches")).select(self.fields)
        # 业务转换：与合法请求日相等已保证 ASCII 八位真实日期，再严格转为本地 DATE。
        # 名称与标签保留正文；描述缺失由 Dataset 告警，不据名称推导 ST/*ST 或回填默认标签。
        candidate = clean.with_columns(pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d", strict=True))
        return candidate, checks
