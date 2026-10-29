"""北交所新旧代码映射：官方完整快照，无上游依赖。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import relation


class BseMappingDataset:
    """北交所新旧代码完整快照及其业务验收。

    o_code/n_code 是同一证券在不同时代的代码，不是两条可任意拼接的行情；
    list_date 保留证券原始上市日期，不表示 920 切换日。整个映射表只有一个
    SNAPSHOT Unit，因此任一记录错误都会阻断整表替换并保护旧版本。
    """

    name = "bse_mapping"
    table = "canonical.bse_mapping"
    unit = "SNAPSHOT"
    record_key = ("o_code",)
    # 业务类型：名称和新旧代码保留字符串，上市日期转为本地 DATE。
    schema = pl.Schema({"name": pl.String, "o_code": pl.String, "n_code": pl.String, "list_date": pl.Date})
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        """完整快照无上游依赖，也不接受日期或代码子集。"""
        # 业务检查：任何过滤条件都不能把子集冒充完整快照。
        if params:
            raise ValueError("bse_mapping only supports the complete snapshot; no subset parameters")
        return ()

    def resolve_scope(self, params, pins, con):
        """全部映射记录属于唯一 SNAPSHOT 发布单元。"""
        return Scope((self.unit,))

    def unit_key_sql(self, alias):
        """所有记录共用完整快照替换边界。"""
        return "'SNAPSHOT'"

    def validate_candidate(self, con, candidate_relation, scope):
        """集合式检查映射完整性，任一失败均阻断整个 SNAPSHOT。

        除四字段必填外，o_code 和 n_code 都必须带 BJ 后缀、彼此不同且分别
        全快照唯一。空快照也不能清除旧映射；不去重、不用 stock_basic 补行。
        """
        table = relation(candidate_relation)
        checks = []
        for rule, where in (
            # 业务检查：四字段均必填，文本不能只含空白。
            ("bse.required", "name IS NULL OR length(trim(name))=0 OR o_code IS NULL OR length(trim(o_code))=0 "
             "OR n_code IS NULL OR length(trim(n_code))=0 OR list_date IS NULL"),
            # 业务检查：该接口只交付北交所新旧代码，完整标识均以 .BJ 结尾。
            ("bse.code_suffix", "o_code NOT LIKE '%.BJ' OR n_code NOT LIKE '%.BJ'"),
            # 业务检查：映射必须表示代码变化，同一新旧代码不能构成有效映射。
            ("bse.code_changed", "o_code=n_code"),
        ):
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check("ERROR", rule, "validation", scope.units, {"rows": count}))
        # 业务检查：旧代码是主键，新代码也必须在整个快照唯一；冲突不能靠去重隐藏。
        for field in ("o_code", "n_code"):
            duplicate = con.execute(
                f"SELECT count(*) FROM (SELECT {field} FROM {table} GROUP BY {field} HAVING count(*)>1)"
            ).fetchone()[0]
            if duplicate:
                checks.append(Check("ERROR", f"bse.{field}_unique", "validation", scope.units, {"keys": duplicate}))
        # 业务检查：空响应不能清除已发布映射，也不能生成首个成功空版本。
        if con.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0:
            checks.append(Check("ERROR", "bse.nonempty", "validation", scope.units))
        return tuple(checks)
