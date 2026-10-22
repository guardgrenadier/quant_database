"""两种日线共用的字段、日期边界及数值规则；范围和零行政策仍由各 Dataset 决定。"""

from datetime import date, datetime, timedelta
import re
from zoneinfo import ZoneInfo

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.db import registered, relation, strings_frame

NUMERIC = ("open", "high", "low", "close", "pre_close", "change", "pct_chg", "vol", "amount")
SCHEMA = pl.Schema({"ts_code": pl.String, "trade_date": pl.Date, **dict.fromkeys(NUMERIC, pl.Float64)})


def calendar_dependencies(params):
    """校验日线日期边界并声明逐自然日日历依赖，不决定股票或指数范围。"""
    dates = []
    for field in ("start_date", "end_date"):
        value = params.get(field)
        # 业务边界：只接受有效 ASCII 八位日期，不猜测范围或隐式转换。
        if not isinstance(value, str) or re.fullmatch(r"[0-9]{8}", value) is None:
            raise ValueError(f"{field} must be an ASCII YYYYMMDD date")
        dates.append(date.fromisoformat(value))
    start, end = dates
    # 业务时间：拒绝未来，允许上海当日但不承诺当日终态。
    if start > end or end > datetime.now(ZoneInfo("Asia/Shanghai")).date():
        raise ValueError("Daily range must be ordered and not later than today in Asia/Shanghai")
    return tuple(DependencyRef("trade_calendar", f"SSE:{(start + timedelta(days=i)).isoformat()}")
                 for i in range((end - start).days + 1))


def calendar_scope(pins, con):
    """从实际固定的日历事实区分开闭市和未知，保留排除日的版本。"""
    refs = strings_frame([(p.ref.unit_key,) for p in pins if p.ref.dataset == "trade_calendar"], ("calendar_unit",))
    with registered(con, "_daily_calendar", refs) as source:
        rows = con.execute(f"SELECT substr(r.calendar_unit,5),c.is_open FROM {source} r "
                           "LEFT JOIN canonical.trade_calendar c ON c.exchange='SSE' "
                           "AND c.cal_date=substr(r.calendar_unit,5)::DATE ORDER BY 1").fetchall()
    # 业务语义：休市是合理排除，缺实际事实是 UNKNOWN，不由空行情反推。
    return Scope(tuple(day for day, opened in rows if opened is True),
                 tuple(ScopeReason(day, "Published SSE calendar is closed") for day, opened in rows if opened is False),
                 tuple(ScopeReason(day, "Published SSE calendar fact is missing") for day, opened in rows if opened is None))


def validate_quotes(con, candidate, key_sql, *, nullable):
    """批量验收共同键与价格规则；可空政策由调用 Dataset 显式选择。"""
    table = relation(candidate)
    checks = []
    for rule, where in (
        # 业务键必须存在，不能把 NULL/空白键当成合法零行。
        ("quote.required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL"),
        # 股票九数值必填；指数允许部分 NULL，但不能是一条完全无数值的伪行情。
        ("quote.values", (" AND " if nullable else " OR ").join(f"c.{c} IS NULL" for c in NUMERIC)),
        ("quote.finite", " OR ".join(f"NOT isfinite(c.{c})" for c in NUMERIC)),
        # 非空价格为正，除权昨收允许零；量额非负，不对涨跌幅/涨跌额设符号限制。
        ("quote.sign", " OR ".join(f"c.{c}<=0" for c in ("open", "high", "low", "close"))
         + " OR c.pre_close<0 OR c.vol<0 OR c.amount<0"),
        # 四价齐全才检查 OHLC，合法缺失不通过补值掩盖。
        ("quote.ohlc", "c.open IS NOT NULL AND c.high IS NOT NULL AND c.low IS NOT NULL AND c.close IS NOT NULL AND "
         "(c.high<c.low OR c.open<c.low OR c.open>c.high OR c.close<c.low OR c.close>c.high)"),
    ):
        rows = con.execute(f"SELECT DISTINCT ({key_sql}) FROM {table} c WHERE {where}").fetchall()
        if rows:
            checks.append(Check("ERROR", rule, "validation", None if any(r[0] is None for r in rows) else tuple(r[0] for r in rows)))
    # 业务唯一键不随源接口或请求分片变化，重复及冲突都阻断对应 Unit。
    duplicates = con.execute(f"SELECT DISTINCT ({key_sql}) FROM {table} c "
                             "GROUP BY ts_code,trade_date HAVING count(*)>1").fetchall()
    if duplicates:
        checks.append(Check("ERROR", "quote.key_unique", "validation",
                            None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)))
    return tuple(checks)
