"""集合式替换合格范围，成功数据与元数据同事务提交。"""

import polars as pl

from quant_data.core import deps
from quant_data.core.models import Publication
from quant_data.db import identifier, registered, relation, transaction


def publish(con, dataset, candidate, passed_units, pins, run_id):
    """按合格 Unit 关系批量替换；合法零行范围也会删除旧数据并生成版本。"""
    # 没有许可 Unit 时不触碰业务数据，也不生成“成功但无范围”的 Publication。
    if not passed_units:
        return ()
    # 业务检查：发布必须有显式 Candidate（允许零行）和不重复的 Unit 许可集合。
    if candidate is None or len(set(passed_units)) != len(passed_units):
        raise ValueError("An explicit candidate and unique publication units are required")
    accepted = pl.DataFrame({"unit_key": passed_units}, schema={"unit_key": pl.String})
    columns = ",".join(identifier(c) for c in dataset.schema.names())
    candidate_key = dataset.unit_key_sql("c")
    target_key = dataset.unit_key_sql("t")
    with registered(con, "_accepted_units", accepted) as units:
        # 业务检查：验收许可不得越过 Scope；Publisher 再检查边界，防止直接调用时误删其他范围。
        missing = con.execute(f"SELECT count(*) FROM {units} a LEFT JOIN _scope_units s USING(unit_key) WHERE s.unit_key IS NULL").fetchone()[0]
        if missing:
            raise ValueError("Publication units outside resolved scope")
        with transaction(con):
            # 业务检查：事务内复核 Scope 使用的上游版本，变化时在任何业务删除前中止。
            deps.recheck(con, pins)
            # 集合替换：固定次数的 SQL，不按 Unit 循环或重复扫描整批 Candidate。
            # 业务转换：先删许可范围的旧记录，再仅插入这些 Unit 的新记录；阻断范围保留旧数据。
            con.execute(f"DELETE FROM {relation(dataset.table)} AS t USING {units} a WHERE ({target_key})=a.unit_key")
            con.execute(f"INSERT INTO {relation(dataset.table)} ({columns}) SELECT "
                        f"{','.join('c.' + identifier(c) for c in dataset.schema.names())} "
                        f"FROM {relation(candidate)} c JOIN {units} a ON ({candidate_key})=a.unit_key")
            # 业务转换：以许可 Unit 为左表统计行数；零行 Unit 也生成版本，表示权威空范围已发布。
            # stock_basic 的空快照已被 Dataset 规则阻断；通用机制仍支持其他 Dataset 的合法空交付。
            rows = con.execute(
                "INSERT INTO meta.publication(dataset,unit_key,row_count,run_id) "
                f"SELECT ?,a.unit_key,COALESCE(n.rows,0),? FROM {units} a LEFT JOIN "
                f"(SELECT ({candidate_key}) AS unit_key,count(*) AS rows FROM {relation(candidate)} c GROUP BY 1) n USING(unit_key) "
                "RETURNING unit_key,publication_id,row_count", [dataset.name, run_id]).fetchall()
            # 用真实 RETURNING 结果关联版本，不假定编号连续或返回顺序。
            versions = pl.DataFrame(rows, schema={"unit_key": pl.String, "pid": pl.Int64, "rows": pl.Int64}, orient="row")
            with registered(con, "_published_units", versions) as published:
                # 同事务切换当前权威版本并写成功审计；任一步失败均回滚数据、版本和两个状态更新。
                # Publication 序列号允许因回滚留下空隙，不用编号连续性判断发布成功。
                con.execute(f"INSERT INTO meta.coverage SELECT ?,unit_key,'PUBLISHED',pid,current_timestamp,NULL FROM {published} "
                            "ON CONFLICT(dataset,unit_key) DO UPDATE SET status=excluded.status,publication_id=excluded.publication_id,"
                            "updated_at=excluded.updated_at,note=NULL", [dataset.name])
                con.execute(f"UPDATE meta.run_unit AS u SET status='PUBLISHED',stage='publish',publication_id=p.pid,row_count=p.rows,error=NULL "
                            f"FROM {published} p WHERE u.run_id=? AND u.dataset=? AND u.unit_key=p.unit_key", [run_id, dataset.name])
    return tuple(Publication(*row) for row in rows)
