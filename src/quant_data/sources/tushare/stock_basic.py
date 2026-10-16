"""Tushare 股票快照适配：原始响应直接构造列式表，文件仅旁路留证据。"""

import time
import pyarrow as pa
import polars as pl

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.stock_basic import StockBasicDataset

# 获取契约：五种上市状态 × 三个交易所串行覆盖；它们是请求分片，不是发布 Unit。
STATUSES = ("L", "D", "P", "G", "UN")
EXCHANGES = ("SSE", "SZSE", "BSE")
ROW_LIMIT = 6000
REQUEST_INTERVAL = 1.3


class TushareStockBasicAdapter:
    """通过组合使用 Client，明确区分交付、标准化与发布许可。"""

    def __init__(self, client):
        """仅持有供应商 Client；字段映射来自当前已确认的 Dataset 契约。"""
        self.client = client
        self.fields = tuple(StockBasicDataset.schema.names())
        # 原始日期也先保留字符串；两个旁路标签记录每行来自哪个请求范围。
        self.raw_schema = pl.Schema({c: pl.String for c in (
            *self.fields, "_requested_status", "_requested_exchange")})

    def _frame(self, data, status, exchange):
        """在协议类型边界严格构造 Arrow，再转 Polars；不会数字化证券代码。"""
        # 按字段名重排供应商行，不能假定接口返回顺序与显式请求字段顺序一致。
        indices = [data["fields"].index(c) for c in self.fields]
        # Arrow String 拒绝整数；Polars String 构造的 strict=True 会将整数转字符串。
        # 业务检查：代码、日期等源字段只能是字符串或 NULL，禁止数值隐式转码丢失前导零。
        arrays = [pa.array((row[i] for row in data["items"]), type=pa.string()) for i in indices]
        table = pa.Table.from_arrays(arrays, names=self.fields)
        # 业务转换：附加请求范围标签，供后续逐行核对供应商是否交付了请求范围内的数据。
        return pl.from_arrow(table).with_columns(
            pl.lit(status).alias("_requested_status"), pl.lit(exchange).alias("_requested_exchange"))

    def fetch(self, scope, artifacts):
        """获取十五片并在内存组表，任一失败或截断阻断完整快照。"""
        # 业务检查：该 Adapter 只接受完整 SNAPSHOT，不接受部分股票或部分交易所范围。
        if scope.units != (StockBasicDataset.unit,):
            raise ValueError("stock_basic adapter requires the complete snapshot scope")
        frames, attempts, checks, request_ids = [], [], [], []
        state, last_request = "COMPLETE", None

        def record(attempt, payload):
            """每次尝试即时旁路留证据，控制记录稍后由 Runner 批量写入。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for status in STATUSES:
            for exchange in EXCHANGES:
                # 控制串行请求起始间隔，降低触发供应商频率限制的概率；重试由 Client 管理。
                if last_request is not None:
                    time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last_request)))
                last_request = time.monotonic()
                request_id = f"request-{len(request_ids):04d}"
                request_ids.append(request_id)
                try:
                    response = self.client.query("stock_basic", {"list_status": status, "exchange": exchange},
                                                 self.fields, request_id=request_id, on_attempt=record)
                except (RuntimeError, ValueError, OSError) as exc:
                    # 业务交付：任一分片请求失败，整个快照交付失败，已取回的片段不能单独发布。
                    checks.append(Check("ERROR", "source.request", "fetch", scope.units, {"error": str(exc)}))
                    state = "FAILED"
                    break
                data = response["data"]
                # 业务检查：17 个约定字段必须全部出现；缺列属于交付不完整，不能补 NULL 冒充成功。
                missing = sorted(set(self.fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "fetch", scope.units, {"missing": missing}))
                    state = "INCOMPLETE"
                # 业务检查：必须明确 has_more=False，且行数不能碰到单次上限；否则无法确认完整性。
                # 本切片不以分页或截断后的数据继续替换全快照。
                if data.get("has_more") is not False or len(data["items"]) >= ROW_LIMIT:
                    checks.append(Check("ERROR", "source.truncated", "fetch", scope.units,
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                if state != "COMPLETE":
                    break
                try:
                    # 仅通过交付检查的分片进入内存 Raw 表；JSON 仅留证据，不参与组表。
                    frames.append(self._frame(data, status, exchange))
                except (pa.ArrowException, ValueError, TypeError) as exc:
                    checks.append(Check("ERROR", "source.types", "fetch", scope.units, {"error": str(exc)}))
                    state = "INCOMPLETE"
                    break
            if state != "COMPLETE":
                break
        # 完整空交付仍有 19 列 Schema，失败的部分数据不伪装成空成功。
        frame = pl.concat(frames) if frames else (pl.DataFrame(schema=self.raw_schema) if state == "COMPLETE" else None)
        return Delivery(frame, (UnitDelivery(scope.units[0], state, tuple(request_ids)),),
                        tuple(attempts), tuple(checks))

    def validate_source(self, delivery, scope):
        """用 Polars 检查交付记录的状态和交易所是否与各自请求一致。"""
        if delivery.frame is None:
            return ()
        frame = delivery.frame
        # 业务检查：每行的上市状态、交易所须匹配其请求标签；NULL 也视为不匹配并阻断快照。
        invalid = frame.filter(
            (pl.col("list_status") != pl.col("_requested_status")).fill_null(True)
            | (pl.col("exchange") != pl.col("_requested_exchange")).fill_null(True))
        return (Check("ERROR", "source.range", "source_validation", scope.units,
                      {"rows": invalid.height}),) if invalid.height else ()

    def normalize(self, delivery, scope):
        """列式标准化为 17 列 Candidate，格式或日期失败返回阻断检查与 None。"""
        if delivery.frame is None:
            return None, ()
        # 业务转换：选回 17 个业务字段、去除两端空白、将空字符串统一为 NULL；不去重或修正代码。
        clean = delivery.frame.select(
            pl.col(c).str.strip_chars().replace("", None).alias(c) for c in self.fields)
        for field in ("list_date", "delist_date"):
            # 业务检查：非空日期必须是 ASCII 八位 YYYYMMDD；先挡住解析器可能接受的七位日期。
            # NULL 是否允许由 Dataset 按上市状态判断，此处不把缺失日期误判为格式错误。
            invalid = clean.filter(pl.col(field).is_not_null() & ~pl.col(field).str.contains(r"^[0-9]{8}$"))
            if invalid.height:
                return None, (Check("ERROR", "source.date_format", "normalize", scope.units,
                                    {"field": field, "rows": invalid.height}),)
        try:
            # 业务转换：严格解析真实日历日期；如 20260230 必须阻断，不转正常 NULL、不丢弃记录。
            candidate = clean.with_columns(
                pl.col(c).str.strptime(pl.Date, "%Y%m%d", strict=True) for c in ("list_date", "delist_date"))
        except pl.exceptions.PolarsError as exc:
            return None, (Check("ERROR", "source.date_parse", "normalize", scope.units, {"error": str(exc)}),)
        return candidate, ()
