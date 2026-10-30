"""集合式替换合格范围，成功数据与元数据同事务提交。"""

import polars as pl

from quant_data.core import deps
from quant_data.core.models import Publication
from quant_data.db import identifier, registered, relation, transaction


def publish(con, dataset, candidate, passed_units, pins, run_id):
    """按许可范围原子替换，返回已提交的 Publication，包含合法零行版本。

    调用前须在同一连接准备 _scope_units，以及已验收的 candidate 关系。
    candidate 是关系名；Publisher 信任 Validator 的发布许可，不重复验收业务质量。
    """
    # 1. 检查发布参数：无许可范围时不写业务数据，也不生成 Publication。
    if not passed_units:
        return ()
    # 业务检查：发布必须有显式 Candidate（允许零行）和不重复的 Unit 许可集合。
    if candidate is None or len(set(passed_units)) != len(passed_units):
        raise ValueError("An explicit candidate and unique publication units are required")

    # 2. 准备发布范围：许可集合转为列式关系，后续 SQL 均按此范围批量操作。
    accepted_frame = pl.DataFrame({"unit_key": passed_units}, schema={"unit_key": pl.String})
    target_table = relation(dataset.table)
    candidate_relation = relation(candidate)
    columns = ",".join(identifier(c) for c in dataset.schema.names())
    candidate_columns = ",".join("c." + identifier(c) for c in dataset.schema.names())
    candidate_key = dataset.unit_key_sql("c")
    target_key = dataset.unit_key_sql("t")
    with registered(con, "_accepted_units", accepted_frame) as accepted_relation:
        # 业务检查：许可不得越出 Scope，防止发布调用误删其他范围。
        outside_scope_count = con.execute(f"""
            SELECT count(*)
            FROM {accepted_relation} AS a
            LEFT JOIN _scope_units AS s USING (unit_key)
            WHERE s.unit_key IS NULL
        """).fetchone()[0]
        if outside_scope_count:
            raise ValueError("Publication units outside resolved scope")

        # 3. 开始发布事务：版本复核、业务替换和成功元数据写入整体提交或回滚。
        with transaction(con):
            # 业务检查：事务内复核 Scope 使用的上游版本，变化时在任何业务删除前中止。
            deps.recheck(con, pins)

            # 4. 替换业务数据：先删许可范围，再插入其新记录；阻断范围保留旧数据。
            # 固定次数的集合 SQL，不为每个 Unit 单独执行替换。
            con.execute(f"""
                DELETE FROM {target_table} AS t
                USING {accepted_relation} AS a
                WHERE ({target_key}) = a.unit_key
            """)
            con.execute(f"""
                INSERT INTO {target_table} ({columns})
                SELECT {candidate_columns}
                FROM {candidate_relation} AS c
                JOIN {accepted_relation} AS a ON ({candidate_key}) = a.unit_key
            """)

            # 5. 记录成功发布：版本、Coverage 和成功 Unit 审计与业务替换处于同一事务。
            # 业务转换：以许可 Unit 为左表统计，完整零行也生成版本并清除旧范围。
            # 是否允许零行由 Dataset 验收决定；Publisher 不从 Candidate 有无记录反推许可。
            publication_rows = con.execute(f"""
                INSERT INTO meta.publication (dataset, unit_key, row_count, run_id)
                SELECT ?, a.unit_key, COALESCE(n.rows, 0), ?
                FROM {accepted_relation} AS a
                LEFT JOIN (
                    SELECT ({candidate_key}) AS unit_key, count(*) AS rows
                    FROM {candidate_relation} AS c
                    GROUP BY 1
                ) AS n USING (unit_key)
                RETURNING unit_key, publication_id, row_count
            """, [dataset.name, run_id]).fetchall()

            # 业务关联：使用真实 RETURNING 的 Unit 和版本，不假定序号连续或结果顺序。
            publication_frame = pl.DataFrame(
                publication_rows, schema={"unit_key": pl.String, "pid": pl.Int64, "rows": pl.Int64}, orient="row")
            with registered(con, "_published_units", publication_frame) as publication_relation:
                # 业务更新：Coverage 切换到刚创建的成功版本；回滚时该指针与业务数据一同恢复。
                con.execute(f"""
                    INSERT INTO meta.coverage
                    SELECT ?, unit_key, 'PUBLISHED', pid, current_timestamp, NULL
                    FROM {publication_relation}
                    ON CONFLICT (dataset, unit_key) DO UPDATE SET
                        status = excluded.status,
                        publication_id = excluded.publication_id,
                        updated_at = excluded.updated_at,
                        note = NULL
                """, [dataset.name])

                # 业务更新：成功 Unit 审计仅在发布事务内确认，不能先于实际提交成为成功事实。
                con.execute(f"""
                    UPDATE meta.run_unit AS u
                    SET status = 'PUBLISHED',
                        stage = 'publish',
                        publication_id = p.pid,
                        row_count = p.rows,
                        error = NULL
                    FROM {publication_relation} AS p
                    WHERE u.run_id = ? AND u.dataset = ? AND u.unit_key = p.unit_key
                """, [run_id, dataset.name])

    # 6. 返回已提交的版本；事务异常时由 transaction 回滚，不会返回成功结果。
    return tuple(Publication(*row) for row in publication_rows)
