"""停复牌稀疏日集合：依据已发布 SSE 日历确定交易日，允许合法零行发布。"""

from datetime import date, timedelta
from pathlib import Path
import re

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.db import registered, relation, strings_frame


class SuspendDailyDataset:
    """组织停复牌事实契约及日历 Scope，不派生股票交易状态。"""

    name = "suspend_daily"
    table = "canonical.suspend_daily"
    # 业务粒度：保留 S/R 类型，不用可空或多段 timing 扩充业务键。
    record_key = ("ts_code", "trade_date", "suspend_type")
    # 业务类型：代码、类型、时段保留字符串，交易日为市场本地 DATE。
    schema = pl.Schema({"ts_code": pl.String, "trade_date": pl.Date,
                        "suspend_type": pl.String, "suspend_timing": pl.String})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params):
        """要求闭区间内每个自然日都有成功 SSE 日历，缺任一依赖时不请求供应商。"""
        # 业务检查：只允许完整日集合的起止日期，禁止股票、停复牌类型或其他子集筛选。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("suspend_daily requires only start_date and end_date (YYYYMMDD)")
        dates = []
        for field in ("start_date", "end_date"):
            value = params[field]
            # 业务检查：ASCII 八位日期为唯一输入格式，不接受七位日期或数值隐式转码。
            if not isinstance(value, str) or re.fullmatch(r"[0-9]{8}", value) is None:
                raise ValueError(f"{field} must be an ASCII YYYYMMDD date")
            # 业务转换：严格验证真实历法日期；无效日期不能被解释为休市或空范围。
            dates.append(date.fromisoformat(value))
        start, end = dates
        # 业务检查：闭区间必须有序，不能把倒置输入当作成功的空集合。
        if start > end:
            raise ValueError("start_date must not be after end_date")
        # 业务依赖：逐自然日固定 SSE 成功版本，包括待判断是否休市的日期；不依赖 stock_basic。
        return tuple(DependencyRef("trade_calendar", f"SSE:{(start + timedelta(days=i)).isoformat()}")
                     for i in range((end - start).days + 1))

    def resolve_scope(self, params, pins, con):
        """在 Runner 固定版本的同一事务快照中读取实际日历，区分开市、休市和未知。"""
        # 业务读取：以实际固定的日历范围为左表，不能丢掉缺事实的日期后默认为休市。
        frame = strings_frame([(p.ref.unit_key,) for p in pins], ("calendar_unit",))
        with registered(con, "_suspend_calendar", frame) as refs:
            rows = con.execute(
                f"SELECT substr(r.calendar_unit,5),c.is_open FROM {refs} r "
                "LEFT JOIN canonical.trade_calendar c ON c.exchange='SSE' "
                "AND c.cal_date=substr(r.calendar_unit,5)::DATE ORDER BY 1").fetchall()
        # 业务决策：开市日获取全市场停复牌集合；明确休市日排除并审计，不发布零行 Coverage。
        units = tuple(day for day, opened in rows if opened is True)
        excluded = tuple(ScopeReason(day, "Published SSE calendar is closed") for day, opened in rows if opened is False)
        # 业务检查：成功 Coverage 下仍找不到日历事实是 UNKNOWN，不能由空响应或周末规则补判。
        unknown = tuple(ScopeReason(day, "Published SSE calendar fact is missing") for day, opened in rows if opened is None)
        return Scope(units, excluded, unknown)

    def unit_key_sql(self, alias):
        """每条事实归属一个全市场交易日，不按证券或停复牌类型分别发布。"""
        # 业务映射：与 Scope 的 ISO 日期一致，合法零行日的发布目标由显式许可集合给出。
        return f"strftime({alias}.trade_date, '%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """逐日定位键、类型及唯一性错误；稀疏日不设最低记录数或股票外键。"""
        table = relation(candidate_relation)
        key = self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务检查：证券标识、交易日、停复牌类型必填；空代码不代表合法空日集合。
            ("suspend.required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL OR c.suspend_type IS NULL"),
            # 业务检查：只接受 S/R；时段可空且不参与类型推断。
            ("suspend.type", "c.suspend_type NOT IN ('S','R')"),
            # 业务检查：非空时段保留正文，但空白必须已标准化为 NULL。
            ("suspend.timing", "c.suspend_timing IS NOT NULL AND length(trim(c.suspend_timing))=0"),
        ):
            rows = con.execute(f"SELECT DISTINCT ({key}) FROM {table} c WHERE {where}").fetchall()
            if rows:
                # 业务影响：能定位日期时只阻断该日；日期身份缺失时全局阻断，不能猜测归属。
                units = None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)
                checks.append(Check("ERROR", rule, "validation", units))
        # 业务检查：同一三字段键多行即阻断整日，不因 timing 不同而拆键或静默去重。
        rows = con.execute(
            f"SELECT DISTINCT strftime(trade_date,'%Y-%m-%d') FROM {table} "
            "GROUP BY ts_code,trade_date,suspend_type HAVING count(*)>1").fetchall()
        if rows:
            units = None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)
            checks.append(Check("ERROR", "suspend.key_unique", "validation", units))
        # 业务语义：没有事件行可以是完整供应商交付，不要求补正常交易股票行或至少一条 S/R。
        return tuple(checks)
