"""ST 每日名单：依赖成功 SSE 日历，按全市场交易日替换，首版阻断零行。"""

from datetime import date, datetime, timedelta
from pathlib import Path
import re
from zoneinfo import ZoneInfo

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.db import registered, relation, strings_frame


class StockStDataset:
    """定义供应商每日名单契约，不派生 ST/*ST 类别或新增/撤销事件。"""

    name = "stock_st"
    table = "canonical.stock_st"
    # 业务粒度：同一证券在同一交易日只有一条名单事实，类型和描述不扩充键。
    record_key = ("ts_code", "trade_date")
    schema = pl.Schema({"ts_code": pl.String, "name": pl.String, "trade_date": pl.Date,
                        "type": pl.String, "type_name": pl.String})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        """验证显式日期闭区间，并要求每个自然日的成功 SSE 日历版本。"""
        # 业务边界：只接受完整日名单区间，拒绝证券、类型或交易所子集替换。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("stock_st requires only start_date and end_date (YYYYMMDD)")
        dates = []
        for field in ("start_date", "end_date"):
            value = params[field]
            # 业务检查：代码日期不做数值隐式转码，仅接受 ASCII 八位真实日期。
            if not isinstance(value, str) or re.fullmatch(r"[0-9]{8}", value) is None:
                raise ValueError(f"{field} must be an ASCII YYYYMMDD date")
            dates.append(date.fromisoformat(value))
        start, end = dates
        # 业务边界：倒置区间不能成为成功空 Scope；未来名单不具备发布含义。
        if start > end:
            raise ValueError("start_date must not be after end_date")
        if end > datetime.now(ZoneInfo("Asia/Shanghai")).date():
            raise ValueError("stock_st does not accept future dates (Asia/Shanghai)")
        # 业务依赖：休市排除也必须有成功日历依据，不使用周末规则或当前股票快照。
        return tuple(DependencyRef("trade_calendar", f"SSE:{(start + timedelta(days=i)).isoformat()}")
                     for i in range((end - start).days + 1))

    def resolve_scope(self, params, pins, con):
        """在固定版本的同一读取快照中解析开市、休市与缺事实日期。"""
        # 业务读取：固定范围作左表，保留有成功 Coverage 但实际事实缺失的日期。
        frame = strings_frame([(p.ref.unit_key,) for p in pins], ("calendar_unit",))
        with registered(con, "_st_calendar", frame) as refs:
            rows = con.execute(
                f"SELECT substr(r.calendar_unit,5),c.is_open FROM {refs} r "
                "LEFT JOIN canonical.trade_calendar c ON c.exchange='SSE' "
                "AND c.cal_date=substr(r.calendar_unit,5)::DATE ORDER BY 1").fetchall()
        # 业务决策：只获取开市日；明确休市排除，无事实为 UNKNOWN，不发布空名单代替。
        return Scope(tuple(day for day, opened in rows if opened is True),
                     tuple(ScopeReason(day, "Published SSE calendar is closed") for day, opened in rows if opened is False),
                     tuple(ScopeReason(day, "Published SSE calendar fact is missing") for day, opened in rows if opened is None))

    def unit_key_sql(self, alias):
        """记录归属全市场交易日，供统一验收和完整替换使用。"""
        return f"strftime({alias}.trade_date, '%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """按日验收名单身份、类型、唯一性、零行与可选描述。"""
        table = relation(candidate_relation)
        key = self.unit_key_sql("c")
        checks = []
        for level, rule, where in (
            # 业务硬约束：代码、日期和类型构成名单事实，缺失不能作为成功数据。
            ("ERROR", "st.required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL OR c.type IS NULL"),
            # 业务枚举：首版只接已确认的 ST，新类型留证并阻断，不由名称推导类型。
            ("ERROR", "st.type", "c.type <> 'ST'"),
            # 业务告警：描述不决定名单归属；可空但不回填当前名称或固定中文标签。
            ("WARNING", "st.description_missing", "c.name IS NULL OR c.type_name IS NULL"),
        ):
            rows = con.execute(f"SELECT DISTINCT ({key}) FROM {table} c WHERE {where}").fetchall()
            if rows:
                # 业务影响：无法定位日期的错误按全局处理；可信日期只影响该日。
                units = None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)
                checks.append(Check(level, rule, "validation", units))
        # 业务检查：同键不同名称、标签或类型仍是冲突，不去重或增加键字段掩盖。
        duplicates = con.execute(
            f"SELECT DISTINCT strftime(trade_date,'%Y-%m-%d') FROM {table} "
            "GROUP BY ts_code,trade_date HAVING count(*)>1").fetchall()
        if duplicates:
            units = None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)
            checks.append(Check("ERROR", "st.key_unique", "validation", units))
        # 首版发布政策：完整零行也不能清除旧名单；这不声称每个开市日必然存在 ST。
        # 显式 Scope 作左表，避免仅扫描 Candidate 而漏掉没有任何记录的目标日。
        empty = con.execute(
            f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {table} c ON s.unit_key=({key}) "
            "GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "st.nonempty", "validation", tuple(r[0] for r in empty)))
        return tuple(checks)
