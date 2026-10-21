"""指数基础信息契约：全供应商当前快照，无上游依赖。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import identifier, relation


class IndexBasicDataset:
    """定义指数事实、完整替换范围和业务验收，不管理分页通信。"""

    name = "index_basic"
    table = "canonical.index_basic"
    unit = "SNAPSHOT"
    record_key = ("ts_code",)
    # 业务类型：跨市场标识和分类保留字符串；三个业务日期可空，基点不换算。
    schema = pl.Schema({
        "ts_code": pl.String, "name": pl.String, "fullname": pl.String,
        "market": pl.String, "publisher": pl.String, "index_type": pl.String,
        "category": pl.String, "base_date": pl.Date, "base_point": pl.Float64,
        "list_date": pl.Date, "weight_rule": pl.String, "desc": pl.String,
        "exp_date": pl.Date,
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params):
        """完整当前快照无依赖；拒绝会改变替换全集的任何参数。"""
        # 业务检查：日期、市场或代码子集均不能冒充完整快照。
        if params:
            raise ValueError("index_basic only supports a complete current snapshot; no subset parameters")
        return ()

    def resolve_scope(self, params, pins, con):
        """分页和市场均不是发布 Unit；全部记录属于一个快照。"""
        return Scope((self.unit,))

    def unit_key_sql(self, alias):
        """当前表的所有行共用完整快照替换边界。"""
        return "'SNAPSHOT'"

    def validate_candidate(self, con, candidate_relation, scope):
        """业务 ERROR 阻断整个快照；已确认的稀疏属性和异常值只告警。"""
        table = relation(candidate_relation)
        checks = []
        for level, rule, where in (
            # 业务检查：五个必填文本非空；分类是开放字符串，不限定市场/类别枚举。
            ("ERROR", "index.required", " OR ".join(
                f"{identifier(c)} IS NULL OR length(trim({identifier(c)}))=0"
                for c in ("ts_code", "name", "market", "publisher", "category"))),
            # 业务检查：基点可空、可为零或负数，但非有限数不能作为标准数值发布。
            ("ERROR", "index.base_point_finite", "base_point IS NOT NULL AND NOT isfinite(base_point)"),
            # 业务告警：负基点保留源值；零基点为已知合法样本，不告警。
            ("WARNING", "index.negative_base_point", "base_point<0"),
            # 业务告警：三个日期均保留供应商日期；上海 current_date 由连接初始化设置。
            ("WARNING", "index.future_date", "base_date>current_date OR list_date>current_date OR exp_date>current_date"),
            # 业务告警：终止逆序保留；不约束 base_date<=list_date，真实样本存在相反情况。
            ("WARNING", "index.date_order", "exp_date<list_date OR exp_date<base_date"),
        ):
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check(level, rule, "validation", scope.units, {"rows": count}))
        # 业务检查：重复或冲突都不能通过去重隐藏，ts_code 是全快照业务键。
        duplicate = con.execute(
            f"SELECT count(*) FROM (SELECT ts_code FROM {table} GROUP BY ts_code HAVING count(*)>1)"
        ).fetchone()[0]
        if duplicate:
            checks.append(Check("ERROR", "index.key_unique", "validation", scope.units, {"keys": duplicate}))
        # 业务检查：完整零行也不能清空旧快照；获取 COMPLETE 与该业务阻断分别审计。
        if con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0:
            checks.append(Check("ERROR", "index.nonempty", "validation", scope.units))
        # 业务告警：只汇总约定的可选属性；index_type/exp_date 的正常 NULL 不告警。
        optional = ("fullname", "base_date", "base_point", "list_date", "weight_rule", "desc")
        counts = con.execute(f"SELECT {','.join(f'count(*) FILTER (WHERE {identifier(c)} IS NULL)' for c in optional)} FROM {table}").fetchone()
        nulls = {c: n for c, n in zip(optional, counts, strict=True) if n}
        if nulls:
            checks.append(Check("WARNING", "index.optional_nulls", "validation", scope.units, nulls))
        return tuple(checks)
