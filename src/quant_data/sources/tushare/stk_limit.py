"""官方 stk_limit：全市场逐交易日分页，集中准备七字段源价格事实。"""

import hashlib
import json
import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.stk_limit import StkLimitDataset
from quant_data.sources.tushare.disposition import disposition_check

PAGE_SIZE = 3000
# 当前账号 10000 积分，常规接口 500 次/分钟；0.13 秒约 462 次/分钟，保留频次余量。
# 官方依据：https://tushare.pro/document/1?doc_id=290（2026-11-06 核对）。
REQUEST_INTERVAL = 0.13
TEXT_FIELDS = ("ts_code", "trade_date", "asset_type", "exchange")


class TushareStkLimitAdapter:
    def __init__(self, client):
        self.client = client
        self.fields = tuple(StkLimitDataset.schema.names())

    def _frame(self, data):
        """严格源解码，保持明确空表类型；不将 bool/字符串隐式转换为价格。"""
        arrays = []
        for field in self.fields:
            index = data["fields"].index(field)
            values = [row[index] for row in data["items"]]
            if field in TEXT_FIELDS:
                arrays.append(pa.array(values, type=pa.string()))
            else:
                # 外部数值边界：NULL 合法性与非有限数交给 Dataset，不在转换中修复。
                if any(v is not None and type(v) not in (int, float) for v in values):
                    raise ValueError(f"{field} must be a JSON number or NULL, excluding bool")
                arrays.append(pa.array(values, type=pa.float64()))
        return pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields))

    def fetch(self, scope, artifacts):
        """页属于可信请求日；部分页失败使整日不可发布，其他日期继续获取。"""
        frames, attempts, checks, units = [], [], [], []
        last = None

        def record(attempt, payload):
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for unit in scope.units:
            offset, state, request_ids, seen = 0, "COMPLETE", [], set()
            while True:
                if last is not None:
                    time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last)))
                last = time.monotonic()
                request_id = f"stk-limit-{unit}-{offset}"
                request_ids.append(request_id)
                try:
                    # 业务范围：分页只分请求，不按当前股票快照缩小供应商全市场日集合。
                    body = self.client.query("stk_limit", {"trade_date": unit.replace("-", ""),
                                                          "limit": PAGE_SIZE, "offset": offset},
                                             self.fields, request_id=request_id, on_attempt=record)
                except (RuntimeError, ValueError, OSError) as exc:
                    checks.append(Check("ERROR", "source.request", "fetch", (unit,),
                                        {"offset": offset, "error": str(exc)}))
                    state = "FAILED"
                    break
                data = body["data"]
                count, more = len(data["items"]), data.get("has_more")
                # 准备诊断与交付状态分离；完整请求缺列仍不能发布，不用 NULL 补列。
                missing = sorted(set(self.fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", (unit,),
                                        {"offset": offset, "missing": missing}))
                else:
                    try:
                        # Raw 保留可解码的部分页及请求标签，即使该日后续失败也供验收诊断。
                        frames.append(self._frame(data).with_columns(
                            pl.lit(unit).alias("_requested_date"), pl.lit(offset, dtype=pl.Int64).alias("_offset")))
                    except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                        checks.append(Check("ERROR", "source.types", "prepare", (unit,),
                                            {"offset": offset, "error": str(exc)}))
                # 分页进度：按字段名和无序行内容识别重复页，防止 offset 被忽略后无限请求。
                # 这里只检查整页重复；跨页部分重复业务键由 Dataset 统一验收。
                signature = hashlib.sha256(json.dumps(sorted(
                    json.dumps(dict(zip(data["fields"], row)), sort_keys=True) for row in data["items"]
                )).encode()).hexdigest()
                if type(more) is not bool or count > PAGE_SIZE or (not count and more) or (count and signature in seen):
                    checks.append(Check("ERROR", "source.pagination", "fetch", (unit,),
                                        {"offset": offset, "rows": count, "has_more": more,
                                         "repeated_page": bool(count and signature in seen)}))
                    state = "INCOMPLETE"
                    break
                seen.add(signature)
                # 业务终止证据：满页即使 false 也续取尾页；短页 true 按实际行数继续。
                if not more and count < PAGE_SIZE:
                    break
                offset += count
            units.append(UnitDelivery(unit, state, tuple(request_ids)))
        return Delivery(pl.concat(frames) if frames else None, tuple(units), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        """核对可信请求日；仅将精确三零价格组置 NULL，并保留原始诊断。"""
        if delivery.frame is None:
            return None, ()
        # 业务转换：空白文本变 NULL，代码和分类不修正；Raw 始终保持源价格。
        clean = delivery.frame.with_columns(pl.col(c).str.strip_chars().replace("", None) for c in TEXT_FIELDS)
        clean = clean.with_columns((pl.col("trade_date") == pl.col("_requested_date").str.replace_all("-", ""))
                                   .fill_null(False).alias("_matches"))
        invalid = clean.filter(~pl.col("_matches"))
        checks = [Check("ERROR", "source.range", "prepare", tuple(invalid["_requested_date"].unique().to_list()),
                        {"rows": invalid.height})] if invalid.height else []
        # 原始上下限 NULL 不是三零处置政策；最终 Schema 可空后仍须在转换前阻断。
        source_invalid = clean.filter(pl.col("_matches")
                                      & (pl.col("up_limit").is_null() | pl.col("down_limit").is_null()))
        if source_invalid.height:
            units = tuple(sorted(source_invalid["_requested_date"].unique().to_list()))
            checks.append(Check("ERROR", "stk_limit.required", "prepare", units,
                                {"rows": source_invalid.height, "fields": ["up_limit", "down_limit"]}))
        trusted = pl.col("ts_code").is_not_null() & (pl.col("ts_code") != "")
        zero_group = (trusted & (pl.col("pre_close") == 0)
                      & (pl.col("up_limit") == 0) & (pl.col("down_limit") == 0))
        # 质量处置：只对日期和代码可信的完整三零组生效，其他符号问题不变。
        accepted = clean.filter(pl.col("_matches") & zero_group)
        for unit in sorted(accepted["_requested_date"].unique().to_list()):
            records = []
            for row in accepted.filter(pl.col("_requested_date") == unit).sort("ts_code").iter_rows(named=True):
                fields = ["pre_close", "up_limit", "down_limit"]
                records.append({"key": {"ts_code": row["ts_code"], "trade_date": unit}, "fields": fields,
                                "before": {f: row[f] for f in fields}, "after": dict.fromkeys(fields),
                                "request_id": f"stk-limit-{unit}-{row['_offset']}"})
            checks.append(disposition_check(
                delivery, rule="stk_limit.zero_price_group_nullified", unit=unit,
                policy="stk_limit.zero_price_group_to_null", action="NULLIFY_INVALID_GROUP",
                source_rule="stk_limit.sign", records=records))
        # 业务隔离：错日行不进入 Candidate；只有获批三零组在 Candidate 中转为未知价格。
        candidate = clean.filter(pl.col("_matches")).with_columns(
            pl.when(zero_group).then(pl.lit(None)).otherwise(pl.col(f)).alias(f)
            for f in ("pre_close", "up_limit", "down_limit")
        ).select(self.fields).with_columns(
            pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d", strict=True))
        return candidate, tuple(checks)
