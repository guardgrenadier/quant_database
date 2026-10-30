"""供应商全市场每日涨跌停价格，不推导交易状态或无限制标志。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation


class StkLimitDataset:
    name, table = "stk_limit", "canonical.stk_limit"
    record_key = ("ts_code", "trade_date")
    # 业务类型：价格保留源报价口径；pre_close 的 NULL/0 和开放分类均不补值。
    schema = pl.Schema({"ts_code": pl.String, "trade_date": pl.Date, "pre_close": pl.Float64,
                        "up_limit": pl.Float64, "down_limit": pl.Float64,
                        "asset_type": pl.String, "exchange": pl.String})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：仅接受完整日集合，不能把代码或资产类型子集用于整日替换。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("stk_limit requires only start_date and end_date")
        return calendar_dependencies(params)

    def resolve_scope(self, params, pins, con):
        return calendar_scope(pins, con)

    def unit_key_sql(self, alias):
        return f"strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """按日验收源价格事实；诊断按可信业务日期聚合，不筛证券集合。"""
        table, key = relation(candidate_relation), self.unit_key_sql("c")
        checks = []
        for rule, where in (
            # 业务检查：证券/日期和两个上下限必填；源分类和昨收允许 NULL。
            ("required", "c.ts_code IS NULL OR length(trim(c.ts_code))=0 OR c.trade_date IS NULL "
             "OR c.up_limit IS NULL OR c.down_limit IS NULL"),
            # 业务检查：非有限数不能成为价格；昨收和下限可为零，上限必须为正。
            ("finite", "NOT isfinite(c.pre_close) OR NOT isfinite(c.up_limit) OR NOT isfinite(c.down_limit)"),
            ("sign", "c.pre_close<0 OR c.up_limit<=0 OR c.down_limit<0"),
            # 不设固定涨跌幅，不把 100000/0.01 等源值改写为无限制。
            ("order", "c.up_limit<c.down_limit"),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("ERROR", f"stk_limit.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows": sum(n for _, n in rows)}))
        # 业务唯一键：分页及源分类不扩充记录身份，重复和冲突均阻断所属日。
        duplicates = con.execute(f"SELECT DISTINCT {key} FROM {table} c "
                                 "GROUP BY ts_code,trade_date HAVING count(*)>1").fetchall()
        if duplicates:
            checks.append(Check("ERROR", "stk_limit.key_unique", "validation",
                                None if any(r[0] is None for r in duplicates) else tuple(r[0] for r in duplicates)))
        # 业务非空：完整零行请求不能证明全市场已覆盖，也不能清除旧成功日集合。
        empty = con.execute(f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {table} c "
                            f"ON s.unit_key={key} GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "stk_limit.nonempty", "validation", tuple(r[0] for r in empty)))
        for rule, where in (
            # 业务告警：NULL 与零分开计数，保留原值，不影响上下限事实发布。
            ("pre_close_null", "c.pre_close IS NULL"),
            ("pre_close_zero", "c.pre_close=0"),
            # 已确认零下限原样保存并告警，不将供应商的 0 推导为无限制标志。
            ("down_limit_zero", "c.down_limit=0"),
            ("asset_type_missing", "c.asset_type IS NULL OR length(trim(c.asset_type))=0"),
            ("exchange_missing", "c.exchange IS NULL OR length(trim(c.exchange))=0"),
        ):
            rows = con.execute(f"SELECT {key},count(*) FROM {table} c WHERE {where} GROUP BY 1").fetchall()
            if rows:
                checks.append(Check("WARNING", f"stk_limit.{rule}", "validation",
                                    None if any(u is None for u, _ in rows) else tuple(u for u, _ in rows),
                                    {"rows_by_unit": dict(rows)}))
        return tuple(checks)
