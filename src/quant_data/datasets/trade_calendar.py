"""SSE 交易日历契约：包含休市日，以交易所 × 自然日独立发布。"""

from datetime import date, timedelta
from pathlib import Path
import re

import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import relation


class TradeCalendarDataset:
    """定义 SSE 日历范围与业务规则，复用既有 Dataset 接口及集合发布机制。"""

    name = "trade_calendar"
    table = "canonical.trade_calendar"
    # 业务粒度：同一交易所的同一自然日只有一条事实，开市与休市不拆成不同记录。
    record_key = ("exchange", "cal_date")
    # 业务类型：日期是市场本地 DATE；开市标记是布尔值，前交易日允许缺失。
    schema = pl.Schema({"exchange": pl.String, "cal_date": pl.Date,
                        "is_open": pl.Boolean, "pretrade_date": pl.Date})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params):
        """日历是独立供应商事实，不依赖股票或停复牌数据。"""
        return ()

    def resolve_scope(self, params, pins, con):
        """严格解析显式闭区间，为每个自然日生成一个 SSE Unit。"""
        # 业务检查：只接受显式起止日期；禁止股票、开市或交易所筛选改变完整自然日范围。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("trade_calendar requires only start_date and end_date (YYYYMMDD)")
        dates = []
        for field in ("start_date", "end_date"):
            value = params[field]
            # 业务检查：边界日期必须是 ASCII 八位 YYYYMMDD，不接受七位日期或其他类型。
            if not isinstance(value, str) or re.fullmatch(r"[0-9]{8}", value) is None:
                raise ValueError(f"{field} must be an ASCII YYYYMMDD date")
            # 业务转换：严格转为本地日期；格式正确但历法非法（如 20260230）仍明确失败。
            dates.append(date.fromisoformat(value))
        start, end = dates
        # 业务检查：闭区间不能倒置，避免错误参数被解释成成功的空 Scope。
        if start > end:
            raise ValueError("start_date must not be after end_date")
        # 业务转换：枚举包含两端的所有自然日，周末/节假日也需获取供应商事实。
        # 首版统一保存 SSE；ISO 日期与后续记录归属表达式共同构成稳定的 Unit 身份。
        return Scope(tuple(f"SSE:{(start + timedelta(days=i)).isoformat()}"
                           for i in range((end - start).days + 1)))

    def unit_key_sql(self, alias):
        """以交易所及本地日期定位同一个验收、替换和 Coverage 范围。"""
        # 业务转换：记录映射为与 Scope 相同的 SSE:YYYY-MM-DD，不以是否开市决定归属。
        return f"{alias}.exchange || ':' || strftime({alias}.cal_date, '%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """集合验收单日事实，缺行及重复行都不能发布为成功日历。"""
        table = relation(candidate_relation)
        key = self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务检查：交易所、自然日和开市结论必填；NULL 开市标记不能当成休市。
            ("calendar.required", "c.exchange IS NULL OR c.cal_date IS NULL OR c.is_open IS NULL"),
            # 业务检查：首版只接收真实 SSE 行，不能混入 SZSE/BSE 或未知交易所。
            ("calendar.exchange", "c.exchange <> 'SSE'"),
            # 业务检查：非空前交易日必须严格早于当日；允许引用本次区间外日期。
            ("calendar.pretrade_order", "c.pretrade_date >= c.cal_date"),
        ):
            rows = con.execute(f"SELECT DISTINCT ({key}) FROM {table} c WHERE {where}").fetchall()
            if rows:
                # 业务影响：可定位时只阻断出错日期；缺少身份无法定位时按全局 ERROR 阻断。
                units = None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)
                checks.append(Check("ERROR", rule, "validation", units))
        # 核心业务检查：显式目标 Unit 必须有且仅有一行，包括 is_open=false 的休市日。
        rows = con.execute(
            f"SELECT s.unit_key,count(c.cal_date) FROM _scope_units s LEFT JOIN {table} c "
            f"ON s.unit_key=({key}) GROUP BY s.unit_key HAVING count(c.cal_date)<>1").fetchall()
        if rows:
            # 业务影响：缺行和重复行均阻断对应 Unit，不补默认休市行，也不静默去重。
            checks.append(Check("ERROR", "calendar.one_row", "validation", tuple(r[0] for r in rows),
                                {"missing_units": sum(n == 0 for _, n in rows),
                                 "duplicate_units": sum(n > 1 for _, n in rows)}))
        for rule, where in (
            # 业务告警：历史首日可能没有前交易日；保留 NULL，不伪造日期、不阻断发布。
            ("calendar.pretrade_missing", "c.pretrade_date IS NULL"),
            # 业务告警：未来日历保留供应商安排，但提醒可能修订；以连接的上海日期为基准。
            ("calendar.future_date", "c.cal_date > current_date"),
        ):
            rows = con.execute(f"SELECT DISTINCT ({key}) FROM {table} c WHERE {where}").fetchall()
            if rows:
                # 业务影响：告警关联相应日期；即便无法定位而记为全局，WARNING 也不阻断。
                units = None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)
                checks.append(Check("WARNING", rule, "validation", units))
        return tuple(checks)
