"""官方申万行业归属：Y/N 独立分页，共同交付一个完整快照。"""

import hashlib
import json
import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.index_member_all import IndexMemberAllDataset

PAGE_SIZE = 1000
REQUEST_INTERVAL = 1.3


class TushareIndexMemberAllAdapter:
    def __init__(self, client):
        self.client = client
        self.fields = tuple(IndexMemberAllDataset.schema.names())

    def fetch(self, scope, artifacts):
        """只接受完整快照；任一状态分页失败均不能发布部分集合。"""
        if scope.units != (IndexMemberAllDataset.unit,):
            raise ValueError("index_member_all adapter requires the complete snapshot scope")
        frames, attempts, checks, request_ids = [], [], [], []
        state, last = "COMPLETE", None

        def record(attempt, payload):
            # 原始尝试即时留证，证据写失败在发布前阻断。
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for status in ("Y", "N"):
            offset, seen = 0, set()
            while True:
                if last is not None:
                    time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last)))
                last = time.monotonic()
                request_id = f"index-member-all-{status}-{offset}"
                request_ids.append(request_id)
                detail = {"is_new": status, "offset": offset}
                try:
                    # 业务范围：不传行业或证券过滤，也不将纳入日期当作增量修改游标。
                    body = self.client.query("index_member_all", {"is_new": status, "limit": PAGE_SIZE, "offset": offset},
                                             self.fields, request_id=request_id, on_attempt=record)
                except (RuntimeError, ValueError, OSError) as exc:
                    checks.append(Check("ERROR", "source.request", "fetch", scope.units, {**detail, "error": str(exc)}))
                    state = "FAILED"
                    break
                data = body["data"]
                count, more = len(data["items"]), data.get("has_more")
                # 准备诊断：缺列或字符串类型错误与请求完成状态分开，不补列或隐式转码。
                missing = sorted(set(self.fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", scope.units, {**detail, "missing": missing}))
                else:
                    try:
                        arrays = [pa.array((r[data["fields"].index(f)] for r in data["items"]), type=pa.string())
                                  for f in self.fields]
                        frame = pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields))
                        # Raw 保留可信请求标签，后续核对过滤状态，原始日期与名称保持原样。
                        frames.append(frame.with_columns(pl.lit(status).alias("_requested_is_new"),
                                                         pl.lit(offset, dtype=pl.Int64).alias("_offset")))
                    except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                        checks.append(Check("ERROR", "source.types", "prepare", scope.units, {**detail, "error": str(exc)}))
                # 交付进度：按字段名及无序行内容识别重复整页；部分重复由业务键验收发现。
                signature = hashlib.sha256(json.dumps(sorted(
                    json.dumps(dict(zip(data["fields"], r)), sort_keys=True) for r in data["items"]
                )).encode()).hexdigest()
                if type(more) is not bool or count > PAGE_SIZE or (not count and more) or (count and signature in seen):
                    checks.append(Check("ERROR", "source.pagination", "fetch", scope.units,
                                        {**detail, "rows": count, "has_more": more, "repeated_page": bool(count and signature in seen)}))
                    # FAILED 优先保留，不被另一状态的不完整结果降级。
                    if state != "FAILED":
                        state = "INCOMPLETE"
                    break
                seen.add(signature)
                # 明确终止：短页 false 才完成；满页 false 仍续取，短页 true 按实际行数前移。
                if not more and count < PAGE_SIZE:
                    break
                offset += count
        return Delivery(pl.concat(frames) if frames else None,
                        (UnitDelivery(IndexMemberAllDataset.unit, state, tuple(request_ids)),), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        """集中清理文本与解析日期；任一坏行保留诊断并阻断整个快照。"""
        if delivery.frame is None:
            return None, ()
        # 业务转换：首尾空白清理、空串转 NULL；不修正证券或行业代码，不重写历史名称。
        clean = delivery.frame.with_columns(pl.col(f).str.strip_chars().replace("", None) for f in self.fields)
        checks = []
        # 请求身份：供应商不得忽略 Y/N 过滤后以默认当前集合冒充完整历史。
        count = clean.filter((pl.col("is_new") != pl.col("_requested_is_new")).fill_null(True)).height
        if count:
            checks.append(Check("ERROR", "source.range", "prepare", scope.units, {"rows": count}))
        for field in ("in_date", "out_date"):
            # 日期转换：格式与真实历法分开诊断，合法 NULL 保留给 Dataset 的条件必填规则。
            clean = clean.with_columns(
                pl.col(field).str.contains(r"^[0-9]{8}$").alias("_format"),
                pl.col(field).str.strptime(pl.Date, "%Y%m%d", strict=False).alias("_parsed"))
            for rule, mask in (
                ("source.date_format", pl.col(field).is_not_null() & ~pl.col("_format")),
                ("source.date_parse", pl.col("_format") & pl.col("_parsed").is_null()),
            ):
                count = clean.filter(mask).height
                if count:
                    checks.append(Check("ERROR", rule, "prepare", scope.units, {"field": field, "rows": count}))
            clean = clean.with_columns(pl.col("_parsed").alias(field))
        # 非法日期不以解析后的 NULL 冒充合法空值；整快照无法部分发布。
        if checks:
            return None, tuple(checks)
        return clean.select(self.fields), ()
