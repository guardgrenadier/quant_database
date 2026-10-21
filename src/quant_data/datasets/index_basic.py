"""目标指数基础快照：宽基白名单及供应商报告的申万行业集合。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import identifier, relation


# 业务清单：每个宽基选择一个已核对代码；不是供应商全量代码或测试样本路径。
BROAD_CODES = (
    "000001.SH", "399001.SZ", "000300.SH", "000905.SH", "000906.SH", "000852.SH",
    "932000.CSI", "930050.CSI", "000903.SH", "000510.SH", "000985.CSI", "000016.SH",
    "000010.SH", "000688.SH", "000698.SH", "000699.SH", "000680.SH", "399006.SZ",
    "399673.SZ", "399102.SZ", "399850.SZ", "399330.SZ", "399009.SZ", "899050.BJ",
)
SW_CATEGORY = "行业指数"
SW_PUBLISHER = "上海申银万国证券研究所有限公司"


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
        """固定目标快照无已发布上游依赖；拒绝任意子集参数。"""
        # 业务检查：日期、市场或代码子集均不能冒充完整快照。
        if params:
            raise ValueError("index_basic only supports the complete target universe; no subset parameters")
        return ()

    def resolve_scope(self, params, pins, con):
        """请求分片和过滤条件不作为发布 Unit；全部目标记录属于一个快照。"""
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
        # 业务检查：每个白名单宽基必须有事实；缺失不能用其他记录或旧版补足。
        missing = con.execute(f"SELECT unnest(?::VARCHAR[]) EXCEPT SELECT ts_code FROM {table}", [list(BROAD_CODES)]).fetchall()
        if missing:
            checks.append(Check("ERROR", "index.broad_missing", "validation", scope.units,
                                {"codes": sorted(r[0] for r in missing)}))
        # 业务检查：申万过滤集合零行无法确认覆盖，阻断而不是清除旧名单；不检查固定层级数量。
        if con.execute(f"SELECT count(*) FROM {table} WHERE category=? AND publisher=?",
                       [SW_CATEGORY, SW_PUBLISHER]).fetchone()[0] == 0:
            checks.append(Check("ERROR", "index.sw_nonempty", "validation", scope.units))
        # 业务检查：重复或冲突都不能通过去重隐藏，ts_code 是全快照业务键。
        duplicate = con.execute(
            f"SELECT count(*) FROM (SELECT ts_code FROM {table} GROUP BY ts_code HAVING count(*)>1)"
        ).fetchone()[0]
        if duplicate:
            checks.append(Check("ERROR", "index.key_unique", "validation", scope.units, {"keys": duplicate}))
        # 业务告警：只汇总约定的可选属性；index_type/exp_date 的正常 NULL 不告警。
        optional = ("fullname", "base_date", "base_point", "list_date", "weight_rule", "desc")
        counts = con.execute(f"SELECT {','.join(f'count(*) FILTER (WHERE {identifier(c)} IS NULL)' for c in optional)} FROM {table}").fetchone()
        nulls = {c: n for c, n in zip(optional, counts, strict=True) if n}
        if nulls:
            checks.append(Check("WARNING", "index.optional_nulls", "validation", scope.units, nulls))
        return tuple(checks)
