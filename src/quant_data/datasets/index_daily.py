"""固定版本 universe 内的代码日行情；允许完整交付的零记录 Unit。"""

from pathlib import Path

from quant_data.core.models import DependencyRef, Scope, ScopeReason
from quant_data.datasets.daily import SCHEMA, calendar_dependencies, calendar_scope, validate_quotes
from quant_data.datasets.index_basic import BROAD_CODES


class IndexDailyDataset:
    name, table = "index_daily", "canonical.index_daily"
    record_key = ("ts_code", "trade_date")
    schema = SCHEMA
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：代码子集不能绕过已发布 universe；先验证输入结构，成员检查在同快照读取时完成。
        if not {"start_date", "end_date"} <= set(params) <= {"start_date", "end_date", "ts_codes"}:
            raise ValueError("index_daily requires start_date/end_date and optional ts_codes")
        if "ts_codes" in params:
            codes = params["ts_codes"]
            if (not isinstance(codes, (list, tuple)) or not codes
                    or any(not isinstance(c, str) or not c.strip() for c in codes) or len(set(codes)) != len(codes)):
                raise ValueError("ts_codes must be a nonempty sequence of unique codes")
        return (DependencyRef("index_basic", "SNAPSHOT"), *calendar_dependencies(params))

    def resolve_scope(self, params, pins, con):
        # 业务读取：Runner 已在同一事务固定版本；不由 Adapter 再读另一份 universe。
        universe = dict(con.execute("SELECT ts_code,market FROM canonical.index_basic ORDER BY ts_code").fetchall())
        if not universe:
            raise ValueError("Published index universe has no facts")
        codes = params.get("ts_codes", tuple(universe))
        outside = sorted(set(codes) - universe.keys())
        if outside:
            raise ValueError(f"Index codes outside published universe: {outside}")
        calendar = calendar_scope(pins, con)
        units, excluded, unknown = [], [], []
        for code in sorted(codes):
            # 来源契约：当前只支持已有境内宽基和申万，核对 market 与后缀路由，不静默扩充指数族。
            supported = ((universe[code] == "SW" and code.endswith(".SI"))
                         or (code in BROAD_CODES and universe[code] in ("SSE", "SZSE", "CSI", "BSE")))
            for day in calendar.units:
                unit = f"{code}:{day}"
                if supported:
                    units.append(unit)
                else:
                    unknown.append(ScopeReason(unit, "Unsupported index universe route"))
            excluded.extend(ScopeReason(f"{code}:{r.unit_key}", r.reason) for r in calendar.out_of_scope)
            unknown.extend(ScopeReason(f"{code}:{r.unit_key}", r.reason) for r in calendar.unknown)
        # 业务范围：不按 list_date/base_date/exp_date 裁历史，排除日也保留依赖版本审计。
        return Scope(tuple(units), tuple(excluded), tuple(unknown))

    def unit_key_sql(self, alias):
        return f"{alias}.ts_code||':'||strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        # 零行政策：完整代码日可发布空集合，含义仅为本次供应商没有报告该日行情。
        return validate_quotes(con, candidate_relation, self.unit_key_sql("c"), nullable=True)
