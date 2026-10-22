"""官方 daily：逐交易日获取全市场未复权事实，不补停牌行。"""

import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.sources.tushare.daily import raw_frame

ROW_LIMIT = 6000
REQUEST_INTERVAL = 1.3


class TushareStockDailyAdapter:
    def __init__(self, client):
        self.client = client
        self.fields = tuple(StockDailyDataset.schema.names())

    def fetch(self, scope, artifacts):
        frames, attempts, checks, units = [], [], [], []
        last = None

        def record(attempt, payload):
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for unit in scope.units:
            if last is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last)))
            last = time.monotonic()
            request_id, state = f"stock-daily-{unit}", "COMPLETE"
            try:
                # 业务范围：只传交易日获取整个供应商日集合，不借当前股票快照筛历史。
                body = self.client.query("daily", {"trade_date": unit.replace("-", "")}, self.fields,
                                         request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                checks.append(Check("ERROR", "source.request", "fetch", (unit,), {"error": str(exc)}))
                state = "FAILED"
            else:
                data = body["data"]
                # 业务交付：明确 false、低于上限才完整；不以 count 或有行反推成功。
                if data.get("has_more") is not False or len(data["items"]) >= ROW_LIMIT:
                    checks.append(Check("ERROR", "source.truncated", "fetch", (unit,),
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                missing = sorted(set(self.fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", (unit,), {"missing": missing}))
                if state == "COMPLETE" and not missing:
                    try:
                        # 明确空表 Schema 同样保留；类型解码错误是准备诊断，不改写 COMPLETE。
                        frames.append(raw_frame(data, self.fields).with_columns(pl.lit(unit).alias("_requested_date")))
                    except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                        checks.append(Check("ERROR", "source.types", "prepare", (unit,), {"error": str(exc)}))
            units.append(UnitDelivery(unit, state, (request_id,)))
        return Delivery(pl.concat(frames) if frames else None, tuple(units), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        if delivery.frame is None:
            return None, ()
        # 业务转换：文本只清理一次，Raw 不改写；日期匹配同时保证格式、历法与可信归属。
        clean = delivery.frame.with_columns(pl.col(c).str.strip_chars().replace("", None) for c in ("ts_code", "trade_date"))
        clean = clean.with_columns((pl.col("trade_date") == pl.col("_requested_date").str.replace_all("-", ""))
                                   .fill_null(False).alias("_matches"))
        invalid = clean.filter(~pl.col("_matches"))
        checks = (Check("ERROR", "source.range", "prepare", tuple(invalid["_requested_date"].unique().to_list()),
                        {"rows": invalid.height}),) if invalid.height else ()
        # 业务隔离：错日行保留 Raw 和诊断，不污染另一合格日期；只解析已经匹配的合法日期。
        candidate = clean.filter(pl.col("_matches")).select(self.fields).with_columns(
            pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d", strict=True))
        return candidate, checks
