"""供应商原始复权因子：按交易日替换，不归一化或推导复权行情。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation


class AdjFactorDataset:
    name, table = "adj_factor", "canonical.adj_factor"
    record_key = ("ts_code", "trade_date")
    # 业务类型：因子无量纲，保留源数值，不强制初值为 1 或按基准归一化。
    schema = pl.Schema({"ts_code": pl.String, "trade_date": pl.Date, "adj_factor": pl.Float64})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：完整交易日集合不接受证券子集，不能以单股历史替换整日因子。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("adj_factor requires only start_date and end_date")
        return calendar_dependencies(params)

    def resolve_scope(self, params, pins, con):
        return calendar_scope(pins, con)

    def unit_key_sql(self, alias):
        return f"strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """仅验收因子自身事实；停牌覆盖和相邻日变化不构成阻断或告警。"""
        table, key = relation(candidate_relation), self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务必填：代码、日期和原始因子不能缺失，不以 1 或其他日期代替。
            ("required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL OR c.adj_factor IS NULL"),
            # 因子必须是有限正数，合法小于 1 的值、大因子及相邻日回落均保留。
            ("finite", "NOT isfinite(c.adj_factor)"),
            ("positive", "c.adj_factor<=0"),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("ERROR", f"adj_factor.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows": sum(n for _, n in rows)}))
        # 业务唯一键：跨页重复和同键冲突均阻断所属日，不能去重后冒充完整交付。
        duplicates = con.execute(f"SELECT DISTINCT {key} FROM {table} c "
                                 "GROUP BY ts_code,trade_date HAVING count(*)>1").fetchall()
        if duplicates:
            checks.append(Check("ERROR", "adj_factor.key_unique", "validation",
                                None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)))
        # 业务非空：开市日全市场零行不能清除旧数据或生成成功空版本。
        empty = con.execute(f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {table} c "
                            f"ON s.unit_key={key} GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "adj_factor.nonempty", "validation", tuple(r[0] for r in empty)))
        return tuple(checks)
