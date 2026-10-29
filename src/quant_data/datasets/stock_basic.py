"""股票基础信息业务契约：完整快照，无上游依赖。"""

from pathlib import Path
import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import identifier, relation


class StockBasicDataset:
    """组织标准字段、范围与 SQL 验收，不管理供应商通信或发布事务。"""
    name = "stock_basic"
    table = "canonical.stock_basic"
    # 业务范围：所有股票共用一个完整快照 Unit，不能按交易所或状态分别发布。
    unit = "SNAPSHOT"
    record_key = ("ts_code",)
    # 业务类型：代码保留字符串及前导零；仅上市、退市日期转为 DATE。
    schema = pl.Schema({
        "ts_code": pl.String, "symbol": pl.String, "name": pl.String,
        "exchange": pl.String, "list_status": pl.String, "list_date": pl.Date,
        "delist_date": pl.Date, "market": pl.String, "area": pl.String,
        "industry": pl.String, "fullname": pl.String, "enname": pl.String,
        "cnspell": pl.String, "curr_type": pl.String, "is_hs": pl.String,
        "act_name": pl.String, "act_ent_type": pl.String,
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        """拒绝子集参数，声明完整当前快照无上游依赖。"""
        # 业务检查：带筛选条件的子集不能替换整个当前快照，因此直接拒绝参数。
        if params:
            raise ValueError("stock_basic only supports a complete current snapshot; no subset parameters")
        return ()

    def resolve_scope(self, params, pins, con):
        """返回完整快照范围，获取和验收共享业务目标。"""
        return Scope((self.unit,))

    def unit_key_sql(self, alias):
        """每条股票事实属于同一快照；表达式供批量 JOIN 与范围替换共用。"""
        return "'SNAPSHOT'"

    def validate_candidate(self, con, candidate_relation, scope):
        """集合式验收业务事实；合法差异和可空属性只告警。"""
        table = relation(candidate_relation)
        checks = []
        rules = (
            # 业务检查：主键、代码、名称必须非空，交易所与上市状态必须存在。
            ("stock.required", "ts_code IS NULL OR ts_code='' OR symbol IS NULL OR symbol='' OR name IS NULL OR name='' OR exchange IS NULL OR list_status IS NULL"),
            # 业务检查：交易所、上市状态和非空的沪深港通标识必须属于约定枚举。
            # SQL 的 NULL 结果不会命中 WHERE，因此允许可选 is_hs 为空。
            ("stock.enums", "exchange NOT IN ('SSE','SZSE','BSE') OR list_status NOT IN ('L','D','P','G','UN') OR is_hs NOT IN ('N','H','S')"),
            # 业务检查：L（上市）、D（退市）、P（暂停上市）必须有上市日期；G/UN 可空。
            ("stock.list_date", "list_status IN ('L','D','P') AND list_date IS NULL"),
            # 业务检查：D 状态必须有退市日期；其他状态允许提供退市日期。
            ("stock.delist_date", "list_status='D' AND delist_date IS NULL"),
            # 业务检查：两个日期均存在时，退市日期不得早于上市日期。
            ("stock.date_order", "delist_date<list_date"),
        )
        # 快照只有一个 Unit；任意硬规则失败都阻断本次完整快照替换。
        for rule, where in rules:
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check("ERROR", rule, "validation", scope.units, {"rows": count}))
        # 业务检查：ts_code 必须全快照唯一；不静默去重，避免掩盖跨请求重复或冲突。
        duplicate = con.execute(f"SELECT count(*) FROM (SELECT ts_code FROM {table} GROUP BY ts_code HAVING count(*)>1)").fetchone()[0]
        if duplicate:
            checks.append(Check("ERROR", "stock.key_unique", "validation", scope.units, {"keys": duplicate}))
        # 业务检查：股票全量快照必须包含正常上市股票，空快照不能清空现有股票表。
        if con.execute(f"SELECT count(*) FROM {table} WHERE list_status='L'").fetchone()[0] == 0:
            checks.append(Check("ERROR", "stock.listed_nonempty", "validation", scope.units))
        for rule, where in (
            # 业务告警：未来上市/退市日期保留原值；例如已公告的未来退市不构成硬错误。
            ("stock.future_date", "list_date>current_date OR delist_date>current_date"),
            # 业务告警：特殊历史代码可能不等于 symbol；保留两个原始标识，不强制改写。
            ("stock.symbol_mapping", "split_part(ts_code,'.',1)<>symbol"),
        ):
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check("WARNING", rule, "validation", scope.units, {"rows": count}))
        # 业务告警：可选属性的缺失只按列统计，不删行、不填造默认值；日期已有条件必填规则。
        optional = [c for c in self.schema.names() if c not in (
            "ts_code", "symbol", "name", "exchange", "list_status", "list_date", "delist_date")]
        counts = con.execute(f"SELECT {','.join(f'count(*) FILTER (WHERE {identifier(c)} IS NULL)' for c in optional)} FROM {table}").fetchone()
        nulls = {c: n for c, n in zip(optional, counts, strict=True) if n}
        if nulls:
            checks.append(Check("WARNING", "stock.optional_nulls", "validation", scope.units, nulls))
        return tuple(checks)
