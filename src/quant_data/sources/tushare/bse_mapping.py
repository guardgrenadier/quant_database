"""Tushare 北交所新旧代码映射：单次全量请求，不分页。"""

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.bse_mapping import BseMappingDataset


ROW_LIMIT = 1000


class TushareBseMappingAdapter:
    """交付官方四字段完整快照，集中完成文本清理和日期标准化。"""

    def __init__(self, client):
        self.client = client
        # 业务交付：显式请求全部四字段，不依赖供应商默认列或返回顺序。
        self.fields = tuple(BseMappingDataset.schema.names())

    def _frame(self, data):
        """按字段名重排响应，严格保留字符串及 NULL，拒绝数值隐式转码。"""
        indices = [data["fields"].index(c) for c in self.fields]
        arrays = [pa.array((row[i] for row in data["items"]), type=pa.string()) for i in indices]
        return pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields))

    def fetch(self, scope, artifacts):
        """只发一个无过滤请求；明确未截断且低于单次上限才声明 COMPLETE。"""
        if scope.units != (BseMappingDataset.unit,):
            raise ValueError("bse_mapping adapter requires the complete snapshot scope")
        attempts, checks = [], []

        def record(attempt, payload):
            """每次尝试即时留证，证据写失败不能继续发布。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        request_id = "bse_mapping-snapshot"
        state = "COMPLETE"
        frame = None
        try:
            # 业务范围：不传 o_code/n_code 或日期参数，一次请求供应商当前完整映射表。
            body = self.client.query("bse_mapping", {}, self.fields, request_id=request_id, on_attempt=record)
        except (RuntimeError, ValueError, OSError) as exc:
            # 业务交付：请求失败没有可发布快照，旧版本由统一发布流程保护。
            checks.append(Check("ERROR", "source.request", "fetch", scope.units, {"error": str(exc)}))
            state = "FAILED"
        else:
            data = body["data"]
            # 业务检查：has_more 缺失/true 或触及 1000 行上限均不能确认完整，不自动分页。
            if data.get("has_more") is not False or len(data["items"]) >= ROW_LIMIT:
                checks.append(Check("ERROR", "source.truncated", "fetch", scope.units,
                                    {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                state = "INCOMPLETE"
            # 准备诊断：缺列不能补 NULL；获取完成事实与数据可准备性分别报告。
            missing = sorted(set(self.fields) - set(data["fields"]))
            if missing:
                checks.append(Check("ERROR", "source.fields", "prepare", scope.units, {"missing": missing}))
            if state == "COMPLETE" and not missing:
                try:
                    # 完整空响应也构造明确四列 Raw；是否允许发布由 Dataset 决定。
                    frame = self._frame(data)
                except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                    # 准备诊断：严格类型失败不改写获取状态，原值仍保留在响应证据中。
                    checks.append(Check("ERROR", "source.types", "prepare", scope.units, {"error": str(exc)}))
        return Delivery(frame, (UnitDelivery(BseMappingDataset.unit, state, (request_id,)),),
                        tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        """一次清理文本并解析上市日期，非法日期不静默转为 NULL。"""
        if delivery.frame is None:
            return None, ()
        # 业务转换：三个文本字段去首尾空白、空串转 NULL，保留 Raw 原值，不去重或修正代码。
        clean = delivery.frame.with_columns(
            pl.col(c).str.strip_chars().replace("", None).alias(c) for c in ("name", "o_code", "n_code"))
        # 业务转换：分别核对 ASCII 八位格式和真实历法，暂存解析结果仅用于诊断。
        parsed = clean.with_columns(
            pl.col("list_date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias("_list_date"),
            pl.col("list_date").str.contains(r"^[0-9]{8}$").alias("_list_date_format"))
        checks = []
        for rule, mask in (
            ("source.date_format", pl.col("list_date").is_not_null() & ~pl.col("_list_date_format")),
            ("source.date_parse", pl.col("_list_date_format") & pl.col("_list_date").is_null()),
        ):
            count = parsed.filter(mask).height
            if count:
                checks.append(Check("ERROR", rule, "prepare", scope.units, {"field": "list_date", "rows": count}))
        if checks:
            # 业务影响：任意坏日期阻断整个快照，不发布部分行或合法化 NULL。
            return None, tuple(checks)
        return parsed.select("name", "o_code", "n_code", pl.col("_list_date").alias("list_date")), ()
