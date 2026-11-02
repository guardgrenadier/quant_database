"""银河宽基指数每日成分权重；按指数日验收完整集合。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation

WEIGHT_CODES = ("000016.SH", "000300.SH", "000905.SH", "000906.SH", "000852.SH")
NUMERIC = ("total_share", "free_share_ratio", "calc_share", "weight_factor", "weight", "close")


class IndexWeightDataset:
    name, table = "index_weight", "canonical.index_weight"
    record_key = ("index_code", "con_code", "trade_date")
    # 业务类型：股本为股、比例和权重为百分数，均保留银河源值及合法 NULL。
    schema = pl.Schema({
        "index_code": pl.String, "con_code": pl.String, "trade_date": pl.Date,
        **dict.fromkeys(NUMERIC, pl.Float64),
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：只在明确支持的五个指数内选择，不用其他指数代码试探空响应语义。
        if not {"start_date", "end_date"} <= set(params) <= {"start_date", "end_date", "index_codes"}:
            raise ValueError("index_weight requires start_date/end_date and optional index_codes")
        if "index_codes" in params:
            codes = params["index_codes"]
            if (not isinstance(codes, (list, tuple)) or not codes
                    or any(not isinstance(c, str) or not c.strip() for c in codes)
                    or len(set(codes)) != len(codes)):
                raise ValueError("index_codes must be a nonempty sequence of unique codes")
            unsupported = sorted(set(codes) - set(WEIGHT_CODES))
            if unsupported:
                raise ValueError(f"Unsupported Galaxy index weight codes: {unsupported}")
        return (DependencyRef("index_basic", "SNAPSHOT"), *calendar_dependencies(params))

    def resolve_scope(self, params, pins, con):
        codes = sorted(params.get("index_codes", WEIGHT_CODES))
        # 业务读取：支持名单还必须在同快照固定的项目指数 universe 中，但不按上市日期裁历史。
        published = {r[0] for r in con.execute(
            "SELECT ts_code FROM canonical.index_basic WHERE ts_code IN (SELECT unnest(?::VARCHAR[]))",
            [codes],
        ).fetchall()}
        missing = sorted(set(codes) - published)
        if missing:
            raise ValueError(f"Index codes outside published universe: {missing}")
        calendar = calendar_scope(pins, con)
        units = tuple(f"{code}:{day}" for code in codes for day in calendar.units)
        excluded = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                         for code in codes for r in calendar.out_of_scope)
        unknown = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                        for code in codes for r in calendar.unknown)
        return Scope(units, excluded, unknown)

    def unit_key_sql(self, alias):
        return f"{alias}.index_code||':'||strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """验收单指数日完整集合；权重和与键检查共同防止静默缺行。"""
        table, key = relation(candidate_relation), self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务必填：自由流通比例是已确认的合法 NULL，其余键和数值不得以空值放行。
            ("required", "c.index_code IS NULL OR length(trim(c.index_code))=0 "
             "OR c.con_code IS NULL OR length(trim(c.con_code))=0 OR c.trade_date IS NULL OR "
             + " OR ".join(f"c.{c} IS NULL" for c in NUMERIC if c != "free_share_ratio")),
            # 业务检查：所有非空数值必须有限，不能用 NaN/Inf 参与权重和或价格事实。
            ("finite", " OR ".join(f"NOT isfinite(c.{c})" for c in NUMERIC)),
            # 业务检查：零权重允许；总股本和收盘价为正，其余计算数值不得为负。
            ("sign", "c.total_share<=0 OR c.calc_share<0 OR c.weight_factor<0 OR c.weight<0 OR c.close<=0"),
            ("free_share_ratio", "c.free_share_ratio<0 OR c.free_share_ratio>100"),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("ERROR", f"index_weight.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows": sum(n for _, n in rows)}))
        # 业务唯一键：完全重复和同键冲突都阻断，不能去重后掩盖源集合损坏。
        duplicates = con.execute(f"SELECT DISTINCT {key} FROM {table} c "
                                 "GROUP BY index_code,con_code,trade_date HAVING count(*)>1").fetchall()
        if duplicates:
            checks.append(Check("ERROR", "index_weight.key_unique", "validation",
                                None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)))
        # 业务非空：银河空响应可能表示未发布或不覆盖，不能清除旧指数日权重。
        empty = con.execute(f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {table} c "
                            f"ON s.unit_key={key} GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "index_weight.nonempty", "validation", tuple(r[0] for r in empty)))
        # 集合检查：容差覆盖千只成分的四位小数舍入并留有余量，但不据权重和单独放行。
        sums = con.execute(f"SELECT s.unit_key,sum(c.weight) FROM _scope_units s JOIN {table} c "
                           f"ON s.unit_key={key} GROUP BY s.unit_key "
                           "HAVING sum(c.weight)<99.9 OR sum(c.weight)>100.1").fetchall()
        if sums:
            checks.append(Check("ERROR", "index_weight.weight_sum", "validation",
                                tuple(r[0] for r in sums), {"weight_sums": dict(sums)}))
        for rule, where in (
            # 业务告警：合法缺失和特殊源值原样保留，按 Unit 汇总而不逐行制造诊断。
            ("free_share_ratio_null", "c.free_share_ratio IS NULL"),
            ("zero_weight", "c.weight=0"),
            ("calc_share_above_total", "c.calc_share>c.total_share"),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("WARNING", f"index_weight.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows_by_unit": dict(rows)}))
        return tuple(checks)
