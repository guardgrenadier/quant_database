"""股票每日指标：保留供应商估值与合法缺失，按交易日完整替换。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation

NUMERIC = ("close", "turnover_rate", "turnover_rate_f", "volume_ratio", "pe", "pe_ttm", "pb", "ps", "ps_ttm",
           "dv_ratio", "dv_ttm", "total_share", "float_share", "free_share", "total_mv", "circ_mv")
NONNEGATIVE = ("turnover_rate", "turnover_rate_f", "volume_ratio", "dv_ratio", "dv_ttm",
               "total_share", "float_share", "free_share", "total_mv", "circ_mv")


class DailyBasicDataset:
    name, table = "daily_basic", "canonical.daily_basic"
    record_key = ("ts_code", "trade_date")
    # 业务类型：股本万股、市值万元、比率百分数原样保存；只有 close 为必填数值。
    schema = pl.Schema({"ts_code": pl.String, "trade_date": pl.Date, **dict.fromkeys(NUMERIC, pl.Float64)})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：完整日集合不接受证券或市场子集，防止以子集替换整日事实。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("daily_basic requires only start_date and end_date")
        return calendar_dependencies(params)

    def resolve_scope(self, params, pins, con):
        return calendar_scope(pins, con)

    def unit_key_sql(self, alias):
        return f"strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """指标自身规则决定阻断，股本/市值关系只保留逐日计数告警。"""
        table, key = relation(candidate_relation), self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务必填：键与 close 不能缺失，其余十五个指标允许合法 NULL。
            ("required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL OR c.close IS NULL"),
            # 非有限值不能作为业务指标；估值保留有符号有限值，不做正数校验。
            ("finite", " OR ".join(f"NOT isfinite(c.{f})" for f in NUMERIC)),
            ("close_positive", "c.close<=0"),
            # 业务非负指标不设上限；换手率可超过 100，零与 NULL 含义不同。
            ("nonnegative", " OR ".join(f"c.{f}<0" for f in NONNEGATIVE)),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("ERROR", f"daily_basic.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows": sum(n for _, n in rows)}))
        # 业务唯一键：分页不扩充记录身份，跨页重复或冲突都阻断所属交易日。
        duplicates = con.execute(f"SELECT DISTINCT {key} FROM {table} c "
                                 "GROUP BY ts_code,trade_date HAVING count(*)>1").fetchall()
        if duplicates:
            checks.append(Check("ERROR", "daily_basic.key_unique", "validation",
                                None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)))
        # 业务非空：开市日全市场零行不清除旧事实，不产生已覆盖的成功空版本。
        empty = con.execute(f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {table} c "
                            f"ON s.unit_key={key} GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "daily_basic.nonempty", "validation", tuple(r[0] for r in empty)))
        for left, right in (("free_share", "float_share"), ("float_share", "total_share"), ("circ_mv", "total_mv")):
            # 业务告警：仅两侧均非空时比较；异常源值不改写，也不阻断本次日集合发布。
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c "
                               f"WHERE c.{left}>c.{right} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("WARNING", f"daily_basic.{left}_above_{right}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows_by_unit": dict(rows)}))
        return tuple(checks)
