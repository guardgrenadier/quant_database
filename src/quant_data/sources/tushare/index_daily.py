"""宽基按代码年片获取，申万按日合并获取；仅发布固定 scope 内行情。"""

import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.index_daily import IndexDailyDataset
from quant_data.sources.tushare.daily import raw_frame

REQUEST_INTERVAL = 1.3


class TushareIndexDailyAdapter:
    def __init__(self, client):
        self.client = client
        self.fields = tuple(IndexDailyDataset.schema.names())

    def fetch(self, scope, artifacts):
        groups = {}
        for unit in scope.units:
            code, day = unit.rsplit(":", 1)
            # 业务请求粒度：申万同一天的所有目标共用一次请求，宽基继续按代码自然年拆片。
            key = (None, day) if code.endswith(".SI") else (code, day[:4])
            groups.setdefault(key, []).append(unit)
        frames, attempts, checks, units = [], [], [], []
        last = None

        def record(attempt, payload):
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for (code, period), members in groups.items():
            affected = tuple(members)
            days = [unit.rsplit(":", 1)[1] for unit in members]
            # 申万允许 Raw 超集，准备阶段明确排除 scope 外记录；宽基不拉全市场或逗号代码。
            api = "index_daily" if code is not None else "sw_daily"
            fields = tuple("pct_change" if api == "sw_daily" and c == "pct_chg" else c
                           for c in self.fields if api != "sw_daily" or c != "pre_close")
            start, end = min(days).replace("-", ""), max(days).replace("-", "")
            params = {"ts_code": code, "start_date": start, "end_date": end} if code is not None else {"trade_date": start}
            request_id, state = f"index-daily-{code or 'sw'}-{period}", "COMPLETE"
            if last is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last)))
            last = time.monotonic()
            try:
                body = self.client.query(api, params, fields, request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                checks.append(Check("ERROR", "source.request", "fetch", affected, {"error": str(exc)}))
                state = "FAILED"
            else:
                data = body["data"]
                # 宽基年片/申万日表须明确完整；标记异常或触顶阻断关联 Unit，不发布截断部分。
                if data.get("has_more") is not False or len(data["items"]) >= (4000 if api == "sw_daily" else 8000):
                    checks.append(Check("ERROR", "source.truncated", "fetch", affected,
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                missing = sorted(set(fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", affected, {"missing": missing}))
                if state == "COMPLETE" and not missing:
                    try:
                        # 源字段/单位原样保留，可信标签供集中准备核对代码、区间和影响片段。
                        frames.append(raw_frame(data, fields).with_columns(
                            pl.lit(api).alias("_api"), pl.lit(code, dtype=pl.String).alias("_code"),
                            pl.lit(start).alias("_start"), pl.lit(end).alias("_end"), pl.lit(request_id).alias("_request_id")))
                    except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                        checks.append(Check("ERROR", "source.types", "prepare", affected, {"error": str(exc)}))
            units.extend(UnitDelivery(unit, state, (request_id,)) for unit in affected)
        return Delivery(pl.concat(frames, how="diagonal") if frames else None, tuple(units), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        if delivery.frame is None:
            return None, ()
        clean = delivery.frame.with_columns(pl.col(c).str.strip_chars().replace("", None) for c in ("ts_code", "trade_date"))
        # 业务转换：日期各解析一次；非法非空日期有明确诊断，不能转为 NULL 后取得零行许可。
        clean = clean.with_columns(pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias("_date"),
                                   pl.col("trade_date").str.contains(r"^[0-9]{8}$").alias("_format"))
        identity = pl.concat_str(pl.col("ts_code"), pl.col("_date").dt.strftime("%Y-%m-%d"), separator=":")
        clean = clean.with_columns(identity.is_in(scope.units).alias("_selected"))
        sw = pl.col("_api") == "sw_daily"
        # 申万日表可含未选中的 .SI 代码；非法代码/日期仍阻断整日，不能借排除掩盖交付错误。
        code_matches = pl.when(sw).then(pl.col("ts_code").str.ends_with(".SI")).otherwise(pl.col("ts_code") == pl.col("_code"))
        clean = clean.with_columns((code_matches & pl.col("_format")
            & pl.col("_date").is_not_null() & (pl.col("trade_date") >= pl.col("_start"))
            & (pl.col("trade_date") <= pl.col("_end")) & (sw | pl.col("_selected"))).fill_null(False).alias("_matches"))
        # 业务隔离：源身份不可信时阻断可信请求片段，不把错代码/日期记录归入别的合格 Unit。
        affected = {}
        for unit in delivery.units:
            for request_id in unit.request_ids:
                affected.setdefault(request_id, []).append(unit.unit_key)
        checks = [Check("ERROR", "source.range", "prepare", tuple(affected[r]))
                  for r in clean.filter(~pl.col("_matches"))["_request_id"].unique().to_list()]
        # 明确记录合法 Raw 超集的排除依据和行数；子集运行时也按固定 scope，而非所有 .SI 发布。
        excluded = clean.filter(pl.col("_matches") & ~pl.col("_selected"))
        for request_id, count in excluded.group_by("_request_id").len().iter_rows():
            checks.append(Check("WARNING", "source.scope_excluded", "prepare", tuple(affected[request_id]),
                                {"request_id": request_id, "rows": count, "reason": "Outside selected code-day scope"}))
        clean = clean.filter(pl.col("_matches") & pl.col("_selected"))
        # 表示对齐仅针对两种已知且已校验字段的源协议；缺必需交付字段已在 fetch 明确阻断。
        clean = clean.with_columns(pl.lit(None, dtype=pl.Float64).alias(c)
                                   for c in ("pre_close", "pct_chg", "pct_change") if c not in clean.columns)
        sw = pl.col("_api") == "sw_daily"
        # 业务单位：SW 万股→手×100、万元→千元×10；其 pre_close 缺源能力，不推导补值。
        candidate = clean.with_columns(
            pl.col("_date").alias("trade_date"),
            pl.when(sw).then(None).otherwise(pl.col("pre_close")).cast(pl.Float64).alias("pre_close"),
            pl.when(sw).then(pl.col("pct_change")).otherwise(pl.col("pct_chg")).alias("pct_chg"),
            pl.when(sw).then(pl.col("vol") * 100).otherwise(pl.col("vol")).alias("vol"),
            pl.when(sw).then(pl.col("amount") * 10).otherwise(pl.col("amount")).alias("amount"),
        ).select(self.fields)
        return candidate, tuple(checks)
