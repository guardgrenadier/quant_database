"""Tushare 指数快照：专属分页交付、集中标准化与独立准备诊断。"""

import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.index_basic import IndexBasicDataset

PAGE_SIZE = 1000
MAX_PAGES = 100
REQUEST_INTERVAL = 1.3
DATES = ("base_date", "list_date", "exp_date")


class TushareIndexBasicAdapter:
    """全部页面共同交付一个 SNAPSHOT；不引入通用分页或市场枚举。"""

    def __init__(self, client):
        self.client = client
        self.fields = tuple(IndexBasicDataset.schema.names())

    def _frame(self, data, offset):
        """在 JSON 类型边界严格解码，日期仍保留源字符串。"""
        arrays = []
        for field in self.fields:
            # 业务映射：按字段名重排；列序变化不能改变属性含义。
            index = data["fields"].index(field)
            values = [row[index] for row in data["items"]]
            if field == "base_point":
                # 业务检查：数值仅接受 JSON int/float/NULL，拒绝 bool 和字符串隐式转换。
                if any(v is not None and type(v) not in (int, float) for v in values):
                    raise ValueError("base_point must be a JSON number or NULL, excluding bool")
                arrays.append(pa.array(values, type=pa.float64()))
            else:
                # 业务检查：Arrow String 拒绝数字标识等非法源类型，避免丢失前导零。
                arrays.append(pa.array(values, type=pa.string()))
        # 业务归属：可信请求页标签留在 Raw，供转换诊断定位；不进入 Canonical。
        return pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields)).with_columns(
            pl.lit(offset, dtype=pl.Int64).alias("_requested_offset"))

    def fetch(self, scope, artifacts):
        """固定页大小取尽并验证后续空页，所有请求和失败均即时留证。"""
        # 业务检查：只接受已确认的完整快照目标，不允许页面或市场作为独立成功范围。
        if scope.units != (IndexBasicDataset.unit,):
            raise ValueError("index_basic adapter requires the complete snapshot scope")
        frames, attempts, checks, request_ids = [], [], [], []
        state, terminal, last_request = "INCOMPLETE", False, None

        def record(attempt, payload):
            """记录每次 HTTP 尝试；证据保存异常直接阻断获取。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for page in range(MAX_PAGES):
            if last_request is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last_request)))
            last_request = time.monotonic()
            offset = page * PAGE_SIZE
            request_id = f"index_basic-p{page:03d}"
            request_ids.append(request_id)
            detail = {"request_id": request_id, "offset": offset}
            try:
                # 业务范围：只传分页参数，不依赖不完整的文档市场枚举拼接全量。
                response = self.client.query("index_basic", {"limit": PAGE_SIZE, "offset": offset},
                                             self.fields, request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                checks.append(Check("ERROR", "source.request", "fetch", scope.units,
                                    {**detail, "error": str(exc)}))
                state = "FAILED"
                break
            data = response["data"]
            # 准备诊断：缺列不能补 NULL；HTTP/分页完成性与解码错误分别表达。
            missing = sorted(set(self.fields) - set(data["fields"]))
            if missing:
                checks.append(Check("ERROR", "source.fields", "prepare", scope.units,
                                    {**detail, "missing": missing}))
            rows, more = len(data["items"]), data.get("has_more")
            # 业务交付：满页 true 才继续；短页 false 后必须再取得空验证页，count 不参与判断。
            valid = (rows == 0 and more is False) if terminal else (
                (rows == PAGE_SIZE and more is True) or (rows < PAGE_SIZE and more is False))
            if not valid:
                checks.append(Check("ERROR", "source.pagination", "fetch", scope.units,
                                    {**detail, "rows": rows, "has_more": more, "verifying_end": terminal}))
                break
            if not missing:
                try:
                    # 业务转换：包括合法空页在内都构造明确 Raw Schema，不静默去重。
                    frames.append(self._frame(data, offset))
                except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                    checks.append(Check("ERROR", "source.types", "prepare", scope.units,
                                        {**detail, "error": str(exc)}))
            if terminal:
                state = "COMPLETE"
                break
            terminal = rows < PAGE_SIZE
        else:
            # 业务交付：100 次请求预算包含验证页；耗尽不能宣称已完整获取。
            checks.append(Check("ERROR", "source.page_budget", "fetch", scope.units, {"max_pages": MAX_PAGES}))
        # 业务数据：只要存在可解码页便保留 Raw；全页解码失败时不补造空 Candidate。
        return Delivery(pl.concat(frames) if frames else None,
                        (UnitDelivery(IndexBasicDataset.unit, state, tuple(request_ids)),),
                        tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        """文本清理和日期解析集中执行，原始值与页归属不因转换而丢失。"""
        # 业务输入：没有可解码 Raw 时不制造合法空 Candidate，已有诊断送入统一验收。
        if delivery.frame is None:
            return None, ()
        # 业务转换：文本去首尾空白、空串转 NULL；数值不舍入，Raw 不修改。
        clean = delivery.frame.with_columns(
            pl.col(c).str.strip_chars().replace("", None).alias(c)
            for c in self.fields if c != "base_point")
        # 业务转换：三个日期分别只解析一次；暂存 NULL 用于检查，不把坏日期作为合法空值。
        parsed = clean.with_columns(
            [pl.col(c).str.strptime(pl.Date, "%Y%m%d", strict=False).alias(f"_{c}") for c in DATES]
            + [pl.col(c).str.contains(r"^[0-9]{8}$").alias(f"_{c}_format") for c in DATES])
        checks = []
        for field in DATES:
            for rule, mask in (
                # 业务检查：非空日期必须为 ASCII 八位且为有效历法，NULL 可保留。
                ("source.date_format", pl.col(field).is_not_null() & ~pl.col(f"_{field}_format")),
                ("source.date_parse", pl.col(f"_{field}_format") & pl.col(f"_{field}").is_null()),
            ):
                invalid = parsed.filter(mask)
                if invalid.height:
                    checks.append(Check("ERROR", rule, "prepare", scope.units,
                                        {"field": field, "rows": invalid.height,
                                         "offsets": invalid["_requested_offset"].unique().sort().to_list()}))
        # 业务影响：单快照任意转换 ERROR 都阻断；不发布安全子集或用 NULL 隐藏非法值。
        if checks:
            return None, tuple(checks)
        return parsed.select(pl.col(f"_{c}").alias(c) if c in DATES else pl.col(c) for c in self.fields), ()
