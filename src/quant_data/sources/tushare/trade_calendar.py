"""官方 trade_cal 适配：按自然年请求 SSE，按自然日表达交付与发布影响。"""

from collections import defaultdict
import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.trade_calendar import TradeCalendarDataset

# 交付策略：逻辑请求串行并保持起始间隔，降低触发供应商频率限制的概率。
REQUEST_INTERVAL = 1.3


class TushareTradeCalendarAdapter:
    """复用通用 Client；请求分片可以失败，其他日期仍可独立验收。"""

    def __init__(self, client):
        """注入 Client，Raw 保留供应商整数开市标记及字符串日期。"""
        self.client = client
        # 业务交付：显式要求四个契约字段，不能依赖接口默认返回列。
        self.fields = tuple(TradeCalendarDataset.schema.names())
        # 业务类型：Raw 暂不把整数 0/1 转成布尔、字符串日期转成 DATE，保留验收依据。
        # 请求标签仅用于追溯，标准化时移除，不进入 Canonical。
        self.raw_schema = pl.Schema({"exchange": pl.String, "cal_date": pl.String,
                                     "is_open": pl.Int64, "pretrade_date": pl.String,
                                     "_request_id": pl.String})

    def fetch(self, scope, artifacts):
        """按年切请求范围，保存每次尝试证据并将结果映射回目标日期。"""
        groups = defaultdict(list)
        for unit in scope.units:
            # 业务检查：Scope 由 Dataset 产生；此处核对 SSE 前缀及日期标识长度。
            if not unit.startswith("SSE:") or len(unit) != 14:
                raise ValueError("trade_calendar adapter requires SSE:YYYY-MM-DD units")
            # 业务分片：同一年合为一次请求，Unit 仍是单日，不改成年度发布。
            groups[unit[4:8]].append(unit)
        frames, attempts, checks, deliveries = [], [], [], []
        last_request = None

        def record(attempt, payload):
            """每次尝试即时留证；写盘失败不能继续发布本次请求片段。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        for year, names in sorted(groups.items()):
            units = tuple(sorted(names))
            # 业务转换：ISO Unit 日期转为供应商 YYYYMMDD；首尾年只请求目标子区间。
            start, end = units[0][4:].replace("-", ""), units[-1][4:].replace("-", "")
            request_id = f"calendar-{year}"
            # 业务范围：固定 SSE 且不传 is_open 过滤，明确休市记录也必须完整获取。
            params = {"exchange": "SSE", "start_date": start, "end_date": end}
            # 交付限频：只约束逻辑请求起始间隔；单次请求的超时与重试由 Client 处理。
            if last_request is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last_request)))
            last_request = time.monotonic()
            state = "COMPLETE"
            try:
                body = self.client.query("trade_cal", params, self.fields,
                                         request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                # 业务影响：本请求涉及的全部日期交付失败；其他年度仍继续获取和验收。
                checks.append(Check("ERROR", "source.request", "fetch", units, {"error": str(exc)}))
                state = "FAILED"
            else:
                data = body["data"]
                missing = sorted(set(self.fields) - set(data["fields"]))
                # 准备诊断：pretrade_date 可空不等于可以缺列；获取完成与能否构表分别表达。
                if missing:
                    checks.append(Check("ERROR", "source.fields", "prepare", units, {"missing": missing}))
                # 业务检查：必须明确没有后续页；标记缺失或为 true 都阻断本请求片段。
                # 本切片不自动分页，也不使用不可靠的 count 作为完整性证明。
                if data.get("has_more") is not False:
                    checks.append(Check("ERROR", "source.truncated", "fetch", units,
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                # 业务门禁：只解码完整且字段齐全的响应，业务日期与范围留给 prepare 集中处理。
                if state == "COMPLETE" and not missing:
                    try:
                        # 业务映射：按返回字段名对齐契约顺序，不能假定供应商返回列序固定。
                        indices = [data["fields"].index(c) for c in self.fields]
                        # 业务检查：is_open 只接受 JSON 整数或 NULL，拒绝字符串/浮点/布尔隐式转码。
                        # NULL 暂保留以定位出错日；是否为合法 0/1 在标准化时逐日检查。
                        if any(row[indices[2]] is not None and type(row[indices[2]]) is not int for row in data["items"]):
                            raise ValueError("is_open must be a JSON integer or NULL")
                        # 业务类型边界：交易所和日期严格按字符串、开市标记按整数构造；不先转字符串掩盖坏值。
                        arrays = [pa.array((row[i] for row in data["items"]), type=kind)
                                  for i, kind in zip(indices, (pa.string(), pa.string(), pa.int64(), pa.string()), strict=True)]
                        raw = pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields))
                        # 业务追溯：不清理或转换原始日期；请求标签用于准备阶段定位错误片段。
                        frames.append(raw.with_columns(pl.lit(request_id).alias("_request_id")))
                    except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                        # 准备诊断：构表失败不伪造空成功，也不改写已完成请求；原值仍在信封中。
                        checks.append(Check("ERROR", "source.types", "prepare", units, {"error": str(exc)}))
            # COMPLETE 仅表示获取完成；日期能否转换、是否恰好一行由准备诊断与统一验收决定。
            # 业务映射：同一请求结果关联片段内每个 Unit，发布许可仍由后续验收逐日决定。
            deliveries.extend(UnitDelivery(u, state, (request_id,)) for u in units)
        # 业务批次：合并请求片段而非逐日建表；完整空响应仍有明确 Schema，无可解码片段则为 None。
        # 获取状态和准备诊断独立保留，不能将没有 Raw 当作合法空日历。
        raw = pl.concat(frames) if frames else None
        return Delivery(raw, tuple(deliveries), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        """集中清理、解析与定位错误；缺日/重复日只由 Dataset 的一日一行规则验收。"""
        # 业务输入：无可处理批次不能当成合法空日历，原因与获取状态保留在 Delivery 中。
        if delivery.frame is None:
            return None, ()
        # 业务转换：文本清理一次，两个日期各解析一次并保留临时结果；Raw 与整数开市标记不变。
        parsed = delivery.frame.with_columns(
            pl.col(c).str.strip_chars().replace("", None) for c in self.fields if c != "is_open"
        ).with_columns(
            pl.col("cal_date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias("_date"),
            pl.col("pretrade_date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias("_previous"),
        ).with_columns(
            pl.concat_str([pl.col("exchange"), pl.col("_date").dt.to_string("%Y-%m-%d")], separator=":").alias("_unit"))
        # 业务归属：同时匹配请求标签与 Unit，防止错年度记录被归入另一个合格请求。
        expected = pl.DataFrame([(r, u.unit_key, True) for u in delivery.units for r in u.request_ids],
                                schema={"_request_id": pl.String, "_unit": pl.String, "_requested": pl.Boolean}, orient="row")
        parsed = parsed.join(expected, on=["_request_id", "_unit"], how="left")
        # 业务检查：SSE、ASCII 八位有效日期且属于对应请求；NULL 或解析失败同样拒绝。
        valid_identity = (pl.col("cal_date").str.contains(r"^[0-9]{8}$")
                          & pl.col("_date").is_not_null() & pl.col("_requested")).fill_null(False)
        invalid = parsed.filter(~valid_identity)
        checks = []
        if invalid.height:
            # 业务影响：日期身份不可信时阻断整个请求片段，保持年度隔离；Raw/信封保留所有原值。
            requests = set(invalid["_request_id"].unique().to_list())
            units = tuple(u.unit_key for u in delivery.units if set(u.request_ids) & requests)
            rejected = parsed.filter(pl.col("_request_id").is_in(requests)).height
            checks.append(Check("ERROR", "source.range", "prepare", units,
                                {"rows": invalid.height, "rejected_rows": rejected}))
            parsed = parsed.filter(~pl.col("_request_id").is_in(requests))
        # 业务检查：合法性掩码只算一次；非法整数不能转成 true，非法日期不能伪装成合法 NULL。
        parsed = parsed.with_columns(
            pl.col("is_open").is_in([0, 1]).fill_null(False).alias("_open_ok"),
            (pl.col("pretrade_date").is_null() | (pl.col("pretrade_date").str.contains(r"^[0-9]{8}$")
                                                & pl.col("_previous").is_not_null())).alias("_previous_ok"))
        for rule, mask in (("source.is_open", "_open_ok"), ("source.pretrade_date", "_previous_ok")):
            invalid = parsed.filter(~pl.col(mask))
            if invalid.height:
                # 业务影响：身份已经可靠定位，转换失败只阻断对应自然日，不用默认值补造事实。
                checks.append(Check("ERROR", rule, "prepare", tuple(invalid["_unit"].unique().to_list()),
                                    {"rows": invalid.height}))
        # 业务转换：只将可可靠表达的行放入 Candidate；排除的记录都有 ERROR，不能解释为合法空日。
        # 复用日期结果，保留合法空前交易日；0/1 分别转 false/true，不再次解析或校验同一枚举。
        candidate = parsed.filter(pl.col("_open_ok") & pl.col("_previous_ok")).select(
            "exchange", pl.col("_date").alias("cal_date"),
            (pl.col("is_open") == 1).alias("is_open"), pl.col("_previous").alias("pretrade_date"))
        return candidate, tuple(checks)
