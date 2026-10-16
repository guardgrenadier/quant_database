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
                # 业务检查：四个字段必须全部交付；pretrade_date 可空不等于可以缺少该列。
                if missing:
                    checks.append(Check("ERROR", "source.fields", "fetch", units, {"missing": missing}))
                    state = "INCOMPLETE"
                # 业务检查：必须明确没有后续页；标记缺失或为 true 都阻断本请求片段。
                # 本切片不自动分页，也不使用不可靠的 count 作为完整性证明。
                if data.get("has_more") is not False:
                    checks.append(Check("ERROR", "source.truncated", "fetch", units,
                                        {"rows": len(data["items"]), "has_more": data.get("has_more")}))
                    state = "INCOMPLETE"
                # 业务门禁：只有字段和交付标记都通过的片段，才构造可处理的 Raw。
                if state == "COMPLETE":
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
                        # 业务转换：身份校验只忽略日期两端空白，Raw 仍保留原始文本。
                        dates = raw["cal_date"].str.strip_chars()
                        # 业务检查：暂将非法日期解析为 NULL 以批量识别，后续 valid 必须拒绝它。
                        # 不能将解析失败当作有效空日期，也不能因解析器接受七位日期而放宽格式。
                        parsed = dates.str.strptime(pl.Date, "%Y%m%d", strict=False)
                        # 业务检查：每行须为 SSE、ASCII 八位有效日期且属于本请求目标 Unit。
                        # 身份或比较结果为 NULL 同样判失败，不默认归入某一天。
                        valid = (raw["exchange"].str.strip_chars().eq("SSE")
                                 & dates.str.contains(r"^[0-9]{8}$")
                                 & parsed.dt.to_string("%Y-%m-%d").is_in([u[4:] for u in units])).fill_null(False)
                        if not valid.all():
                            # 业务影响：身份/范围出错阻断整个请求片段，不能只删坏行后宣称完整。
                            checks.append(Check("ERROR", "source.range", "fetch", units,
                                                {"rows": int((~valid).sum())}))
                            state = "INCOMPLETE"
                        else:
                            # 业务转换：仅接受通过身份验收的片段，并附请求标签供 Raw 追溯。
                            frames.append(raw.with_columns(pl.lit(request_id).alias("_request_id")))
                    except (pa.ArrowException, pl.exceptions.PolarsError, ValueError, TypeError, OverflowError) as exc:
                        # 业务影响：类型构造失败不能冒充空交付；阻断该片段并保留原始信封。
                        checks.append(Check("ERROR", "source.types", "fetch", units, {"error": str(exc)}))
                        state = "INCOMPLETE"
            # COMPLETE 表示请求交付完成；是否每个日期都有唯一合法事实由源及业务验收决定。
            # 业务映射：同一请求结果关联片段内每个 Unit，发布许可仍由后续验收逐日决定。
            deliveries.extend(UnitDelivery(u, state, (request_id,)) for u in units)
        # 业务批次：合并请求片段而非为每个 Unit 建表；无可用片段时仍保留明确 Raw Schema。
        # 空表本身不代表成功，失败状态和后续缺日检查共同阻止其被发布。
        raw = pl.concat(frames) if frames else pl.DataFrame(schema=self.raw_schema)
        return Delivery(raw, tuple(deliveries), tuple(attempts), tuple(checks))

    def validate_source(self, delivery, scope):
        """缺少自然日不能视作休市，阻断对应日期且保留其他日期的发布机会。"""
        # 业务转换：把已通过身份检查的源日期映射回 SSE Unit；这里只比较交付覆盖，不去重业务行。
        present = set("SSE:" + value for value in delivery.frame["cal_date"].str.strip_chars()
                      .str.strptime(pl.Date, "%Y%m%d", strict=True).dt.to_string("%Y-%m-%d").to_list())
        # 业务检查：仅核对请求已完成的日期；FAILED/INCOMPLETE 的日期已有独立交付阻断。
        complete = tuple(u.unit_key for u in delivery.units if u.state == "COMPLETE")
        missing = tuple(u for u in complete if u not in present)
        # 业务影响：缺行只阻断缺少的自然日，不能推断为休市；重复行另由 Dataset 的一日一行规则阻断。
        return (Check("ERROR", "source.calendar_missing", "source_validation", missing),) if missing else ()

    def normalize(self, delivery, scope):
        """显式检查开市标记与可空前交易日，坏值只阻断能够定位的日期。"""
        # 业务转换：选回四个正式字段，移除请求标签；文本去首尾空白、空串转 NULL。
        # is_open 保留原始整数供下方枚举验收，不把空值或未知值默认成休市。
        clean = delivery.frame.select(
            pl.col(c).str.strip_chars().replace("", None) if c != "is_open" else pl.col(c)
            for c in self.fields)
        # 业务映射：cal_date 已通过获取阶段身份检查，严格解析并生成出错行所属 Unit。
        key = pl.concat_str([pl.col("exchange"),
                             pl.col("cal_date").str.strptime(pl.Date, "%Y%m%d", strict=True).dt.to_string("%Y-%m-%d")], separator=":")
        checks = []
        # 业务转换：可空前交易日先尝试解析；非空非法值必须由下方 ERROR 阻断，不能静默丢失。
        parsed_previous = pl.col("pretrade_date").str.strptime(pl.Date, "%Y%m%d", strict=False)
        for rule, invalid in (
            # 业务检查：仅整数 0/1 有开闭市含义；NULL、负数及其他整数都阻断对应日。
            ("source.is_open", ~pl.col("is_open").is_in([0, 1]).fill_null(False)),
            # 业务检查：前交易日允许缺失；非空时必须是 ASCII 八位且为有效历法日期。
            ("source.pretrade_date", pl.col("pretrade_date").is_not_null()
             & (~pl.col("pretrade_date").str.contains(r"^[0-9]{8}$") | parsed_previous.is_null())),
        ):
            # 业务影响：汇总出错行对应日期，不删除业务记录；ERROR 只阻断这些 Unit。
            units = tuple(clean.filter(invalid).select(key.alias("unit_key"))["unit_key"].unique().to_list())
            if units:
                checks.append(Check("ERROR", rule, "normalize", units))
        # 非法值已显式关联阻断 Unit；转换中的 NULL 不会成为可发布的默认日历事实。
        candidate = clean.select(
            # 业务转换：保留真实交易所，将已验证的自然日严格转为 DATE。
            "exchange", pl.col("cal_date").str.strptime(pl.Date, "%Y%m%d", strict=True),
            # 业务转换：0→false（休市）、1→true（开市）；非法值仅暂存为 NULL，所属日已阻断。
            pl.when(pl.col("is_open").is_in([0, 1])).then(pl.col("is_open") == 1).otherwise(None).alias("is_open"),
            # 业务转换：前交易日转为可空 DATE；与当日先后关系由 Dataset 再验收。
            parsed_previous.alias("pretrade_date"))
        return candidate, tuple(checks)
