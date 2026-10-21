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
                # 请求标识直接表达接口及分片范围，便于审计与原始证据文件相互定位。
                request_id = f"stock_basic-{status}-{exchange}"
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
                # 准备诊断：缺列不能补 NULL；请求已完成的事实独立保留，最终由统一验收阻断。
                missing = sorted(set(self.fields) - set(data["fields"]))
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", scope.units, {"missing": missing}))
                # 业务检查：必须明确 has_more=False，且行数不能碰到单次上限；否则无法确认完整性。
                # 本切片不以分页或截断后的数据继续替换全快照。
                if data.get("has_more") is not False or len(data["items"]) >= ROW_LIMIT:
                    checks.append(Check("ERROR", "source.truncated", "fetch", scope.units,
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                if state != "COMPLETE":
                    break
                if missing:
                    # 业务影响：该片无法构造完整 Raw，但仍获取剩余计划请求；错误不会被空表掩盖。
                    continue
                try:
                    # 仅通过交付检查的分片进入内存 Raw 表；JSON 仅留证据，不参与组表。
                    frames.append(self._frame(data, status, exchange))
                except (pa.ArrowException, ValueError, TypeError) as exc:
                    # 准备诊断：严格 Raw 类型构造失败不改写获取状态，原始值已经保存在信封中。
                    checks.append(Check("ERROR", "source.types", "prepare", scope.units, {"error": str(exc)}))
            if state != "COMPLETE":
                break
        # 完整空交付仍有 19 列 Schema，失败的部分数据不伪装成空成功。
        frame = pl.concat(frames) if frames else None
        return Delivery(frame, (UnitDelivery(scope.units[0], state, tuple(request_ids)),),
                        tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        """一次清理、核对请求范围并解析日期，返回 Candidate 和明确诊断。"""
        # 业务输入：无 Raw 不补造空 Candidate，获取或解码问题由 Delivery 送入统一验收。
        if delivery.frame is None:
            return None, ()
        # 业务转换：文本只清理一次，保留请求标签核对源身份；Raw 保持原始内容。
        clean = delivery.frame.with_columns(
            pl.col(c).str.strip_chars().replace("", None).alias(c) for c in self.fields)
        # 业务检查：规范化后的状态/交易所须匹配请求；NULL 或跨分片返回都阻断完整快照。
        invalid = clean.filter(
            (pl.col("list_status") != pl.col("_requested_status")).fill_null(True)
            | (pl.col("exchange") != pl.col("_requested_exchange")).fill_null(True))
        if invalid.height:
            return None, (Check("ERROR", "source.range", "prepare", scope.units, {"rows": invalid.height}),)
        # 业务转换：每列日期只解析一次；暂存 NULL 仅用于识别失败，不代表非法原值可作为空值接受。
        dates = ("list_date", "delist_date")
        parsed = clean.with_columns(
            [pl.col(c).str.strptime(pl.Date, "%Y%m%d", strict=False).alias(f"_{c}") for c in dates]
            + [pl.col(c).str.contains(r"^[0-9]{8}$").alias(f"_{c}_format") for c in dates])
        checks = []
        for field in dates:
            # 业务检查：可空日期不误报；非空值分别校验 ASCII 八位格式和真实历法。
            invalid = parsed.filter(pl.col(field).is_not_null() & ~pl.col(f"_{field}_format"))
            if invalid.height:
                checks.append(Check("ERROR", "source.date_format", "prepare", scope.units,
                                    {"field": field, "rows": invalid.height}))
            invalid = parsed.filter(pl.col(f"_{field}_format") & pl.col(f"_{field}").is_null())
            if invalid.height:
                checks.append(Check("ERROR", "source.date_parse", "prepare", scope.units,
                                    {"field": field, "rows": invalid.height}))
        # 业务影响：股票只有一个 Unit，任意转换失败都阻断整快照，不制造 NULL 日期或部分成功。
        if checks:
            return None, tuple(checks)
        # 业务转换：复用已验证日期、去掉临时列与请求标签；不去重、不修正代码或填属性。
        return parsed.select(pl.col(f"_{c}").alias(c) if c in dates else pl.col(c) for c in self.fields), ()
