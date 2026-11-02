"""申万行业归属完整快照：最新与历史共同验收，out_date 包含当天。"""

from pathlib import Path

import polars as pl

from quant_data.core.models import Check, Scope
from quant_data.db import relation


class IndexMemberAllDataset:
    name, table, unit = "index_member_all", "canonical.index_member_all", "SNAPSHOT"
    record_key = ("l3_code", "ts_code", "in_date")
    # 业务类型：保留源行业路径和名称，日期为闭区间，不增加抓取日期或分类版本字段。
    schema = pl.Schema({
        "l1_code": pl.String, "l1_name": pl.String,
        "l2_code": pl.String, "l2_name": pl.String,
        "l3_code": pl.String, "l3_name": pl.String,
        "ts_code": pl.String, "name": pl.String,
        "in_date": pl.Date, "out_date": pl.Date, "is_new": pl.String,
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：整快照不能接受股票、行业或日期子集，不以当前基础表裁剪历史。
        if params:
            raise ValueError("index_member_all only supports the complete Y+N snapshot; no subset parameters")
        return ()

    def resolve_scope(self, params, pins, con):
        return Scope((self.unit,))

    def unit_key_sql(self, alias):
        return "'SNAPSHOT'"

    def validate_candidate(self, con, candidate_relation, scope):
        """业务错误阻断完整快照；名称缺失及异常早日期保留并告警。"""
        table = relation(candidate_relation)
        checks = []
        for rule, where in (
            # 业务必填：行业与证券标识、纳入日期和状态不能缺失，名称不作为身份字段。
            ("required", " OR ".join(f"{c} IS NULL OR length(trim({c}))=0"
                                    for c in ("l1_code", "l2_code", "l3_code", "ts_code", "is_new"))
             + " OR in_date IS NULL"),
            # 业务状态：Y 是未结束归属，N 是已结束归属，不代表证券是否在市。
            ("status", "is_new NOT IN ('Y','N') OR (is_new='Y' AND out_date IS NOT NULL) "
             "OR (is_new='N' AND out_date IS NULL)"),
            # 已确认闭区间：同日纳入/剔除合法，结束日期不能早于开始日期。
            ("date_order", "out_date<in_date"),
        ):
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check("ERROR", f"index_member_all.{rule}", "validation", scope.units, {"rows": count}))
        # 业务唯一键：跨页或跨状态重复均阻断，不通过去重隐藏源冲突。
        duplicate = con.execute(f"SELECT count(*) FROM (SELECT l3_code,ts_code,in_date FROM {table} "
                                "GROUP BY l3_code,ts_code,in_date HAVING count(*)>1)").fetchone()[0]
        if duplicate:
            checks.append(Check("ERROR", "index_member_all.key_unique", "validation", scope.units, {"keys": duplicate}))
        # 业务覆盖：完整最新和历史均须非空，不能用单状态集合清除整表的另一部分。
        counts = dict(con.execute(f"SELECT is_new,count(*) FROM {table} GROUP BY is_new").fetchall())
        missing = [s for s in ("Y", "N") if not counts.get(s)]
        if missing:
            checks.append(Check("ERROR", "index_member_all.nonempty", "validation", scope.units, {"states": missing}))
        # 业务区间：同一股票的闭区间不得重叠；累计最大结束日也能识别嵌套区间。
        # 允许历史断档，不以相邻归属补齐供应商没有报告的日期。
        overlap = con.execute(f"""
            SELECT count(*) FROM (
                SELECT in_date, max(coalesce(out_date,DATE '9999-12-31')) OVER (
                    PARTITION BY ts_code ORDER BY in_date,l3_code
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ) AS previous_end FROM {table}
            ) WHERE in_date<=previous_end
        """).fetchone()[0]
        if overlap:
            checks.append(Check("ERROR", "index_member_all.interval_overlap", "validation", scope.units, {"rows": overlap}))
        for rule, where in (
            # 描述可空：保留历史名称，不用最新快照覆盖缺失；仅按快照汇总告警。
            ("name_missing", " OR ".join(f"{c} IS NULL" for c in ("l1_name", "l2_name", "l3_name", "name"))),
            # 历史早日期已有真实样本，不按上市日清洗或拒绝。
            ("early_date", "in_date<DATE '1990-01-01'"),
        ):
            count = con.execute(f"SELECT count(*) FROM {table} WHERE {where}").fetchone()[0]
            if count:
                checks.append(Check("WARNING", f"index_member_all.{rule}", "validation", scope.units, {"rows": count}))
        return tuple(checks)
