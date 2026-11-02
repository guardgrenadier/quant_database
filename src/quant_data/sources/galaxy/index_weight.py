"""银河单指数单日权重交付与九列集中标准化。"""

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.index_weight import IndexWeightDataset
from quant_data.sources.galaxy.client import GalaxyRequest

RAW_FIELDS = ("INDEX_CODE", "CON_CODE", "TRADE_DATE", "TOTAL_SHR", "FREE_SHR_RATIO",
              "CALC_SHR", "WEIGHT_FACTOR", "WEIGHT", "CLOSE")
FIELD_MAP = {
    "INDEX_CODE": "index_code", "CON_CODE": "con_code", "TRADE_DATE": "trade_date",
    "TOTAL_SHR": "total_share", "FREE_SHR_RATIO": "free_share_ratio", "CALC_SHR": "calc_share",
    "WEIGHT_FACTOR": "weight_factor", "WEIGHT": "weight", "CLOSE": "close",
}
TEXT_FIELDS = ("INDEX_CODE", "CON_CODE", "TRADE_DATE")


def _raw_frame(rows):
    """按明确 Arrow 类型解码；数值拒绝 bool 和字符串，代码不隐式数值化。"""
    arrays = []
    for field in RAW_FIELDS:
        values = [row.get(field) for row in rows]
        if field in TEXT_FIELDS:
            arrays.append(pa.array(values, type=pa.string()))
        else:
            # 外部数值边界：源整数可转为 DOUBLE，其他类型不得被静默修复。
            if any(value is not None and type(value) not in (int, float) for value in values):
                raise ValueError(f"{field} must be a number or NULL, excluding bool")
            arrays.append(pa.array(values, type=pa.float64()))
    return pl.from_arrow(pa.Table.from_arrays(arrays, names=RAW_FIELDS))


class GalaxyIndexWeightAdapter:
    def __init__(self, client):
        self.client = client
        self.dataset = IndexWeightDataset()
        self._request_units = {}

    def fetch(self, scope, artifacts):
        """一请求只对应一个指数日，避免已实测的多日范围响应损坏。"""
        requests = []
        for unit in scope.units:
            code, day = unit.rsplit(":", 1)
            number = int(day.replace("-", ""))
            requests.append(GalaxyRequest(f"index-weight-{unit}", "index_weight", (code,),
                                          number, number, 0, 0))
        responses, attempts = self.client.fetch(requests, artifacts) if requests else ((), ())
        self._request_units = {request.request_id: unit
                               for request, unit in zip(requests, scope.units, strict=True)}
        frames, checks, deliveries = [], [], []
        for response in responses:
            request_id = response.request.request_id
            unit = self._request_units[request_id]
            state = response.state
            if state != "COMPLETE":
                checks.append(Check("ERROR", "source.request", "fetch", (unit,),
                                    {"request_id": request_id, "error": response.error,
                                     "provider_code": response.provider_code}))
            elif response.provider_code == -76:
                # 银河空交付可能表示未发布或不覆盖，不是可以清除旧版的成功空集合。
                state = "INCOMPLETE"
                checks.append(Check("ERROR", "source.empty", "fetch", (unit,),
                                    {"request_id": request_id, "provider_code": -76}))
            deliveries.append(UnitDelivery(unit, state, (request_id,)))
            if state != "COMPLETE":
                continue
            missing = sorted(field for field in RAW_FIELDS
                             if any(field not in row for row in response.rows))
            if response.rows and missing:
                checks.append(Check("ERROR", "source.fields", "prepare", (unit,),
                                    {"request_id": request_id, "missing": missing}))
                continue
            try:
                frame = _raw_frame(response.rows).with_columns(
                    pl.lit(request_id).alias("_request_id"),
                    pl.lit(response.request.codes[0]).alias("_requested_index_code"),
                    pl.lit(str(response.request.start_date)).alias("_requested_date"),
                )
            except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                # 获取完成与 Raw 解码分别表达；错误只阻断可信请求对应的指数日。
                checks.append(Check("ERROR", "source.types", "prepare", (unit,),
                                    {"request_id": request_id, "error": str(exc)}))
            else:
                frames.append(frame)
        raw = pl.concat(frames) if frames else None
        return Delivery(raw, tuple(deliveries), attempts, tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        """核对可信请求身份并严格解析日期，不去重、不归一化或补造成分。"""
        if delivery.frame is None:
            return None, ()
        clean = delivery.frame.with_columns(
            pl.col(field).str.strip_chars().replace("", None) for field in TEXT_FIELDS
        )
        clean = clean.with_columns(
            ((pl.col("INDEX_CODE") == pl.col("_requested_index_code"))
             & (pl.col("TRADE_DATE") == pl.col("_requested_date"))).fill_null(False).alias("_matches")
        )
        invalid = clean.filter(~pl.col("_matches"))
        checks = []
        if invalid.height:
            units = tuple(dict.fromkeys(self._request_units[request_id]
                                        for request_id in invalid["_request_id"].unique().to_list()))
            checks.append(Check("ERROR", "source.range", "prepare", units, {"rows": invalid.height}))
        # 只有与合法请求日完全匹配的八位日期才进入 DATE 解析，坏行不会污染其他 Unit。
        matched = clean.filter(pl.col("_matches"))
        expressions = []
        for field in RAW_FIELDS:
            column = pl.col(field)
            if field == "TRADE_DATE":
                column = column.str.strptime(pl.Date, "%Y%m%d", strict=True)
            expressions.append(column.alias(FIELD_MAP[field]))
        return matched.select(expressions), tuple(checks)
