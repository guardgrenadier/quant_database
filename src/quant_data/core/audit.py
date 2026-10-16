"""按阶段批量持久化控制信息，Unit 引用共享请求和检查，业务行不进入审计。"""

from collections import Counter
import json
from uuid import uuid4
import polars as pl

from quant_data.core.models import Publication, RunResult, UnitResult, control_data
from quant_data.db import registered, strings_frame


def encode(value):
    """明确序列化控制信息，避免 DataFrame 被递归展开。"""
    return json.dumps(control_data(value), ensure_ascii=False)


def begin(con, dataset, params, landing_root):
    """登记运行和产物位置，时间由数据库默认值填写。"""
    run_id = str(uuid4())
    path = (landing_root / run_id).resolve()
    # 生命周期：插入时省略 started_at 使用数据库默认时间；finished_at 保持 NULL，finish() 再填写。
    con.execute("INSERT INTO meta.run(run_id,dataset,params,status,artifact_dir) VALUES (?,?,?,'RUNNING',?)",
                [run_id, dataset, encode(params), str(path)])
    return run_id, path


def mark_unpublished(con, run_id):
    """从本次 Unit 关系批量更新未发布范围，已有成功 Coverage 保持原状。"""
    # 业务转换：初次执行可登记 PENDING/UNKNOWN/FAILED；OUT_OF_SCOPE 不改变当前权威覆盖。
    # 冲突更新仅作用于非 PUBLISHED Coverage，修订失败不能抹掉历史成功版本指针。
    con.execute(
        "INSERT INTO meta.coverage SELECT dataset,unit_key,status,NULL,current_timestamp,error "
        "FROM meta.run_unit WHERE run_id=? AND scope_decision<>'OUT_OF_SCOPE' AND status<>'PUBLISHED' "
        "ON CONFLICT(dataset,unit_key) DO UPDATE SET status=excluded.status,publication_id=NULL,"
        "updated_at=excluded.updated_at,note=excluded.note WHERE meta.coverage.status<>'PUBLISHED'", [run_id])


def scope(con, run_id, dataset, resolved, pins):
    """一次登记所有 Scope 决策，并创建后续归属 JOIN 使用的 IN_SCOPE 关系。"""
    # 业务转换：范围决策与执行状态分开保存；纳入范围初始待执行，排除和未知保留各自理由。
    rows = [(u, "IN_SCOPE", None, "PENDING") for u in resolved.units]
    rows += [(x.unit_key, "OUT_OF_SCOPE", x.reason, "OUT_OF_SCOPE") for x in resolved.out_of_scope]
    rows += [(x.unit_key, "UNKNOWN", x.reason, "UNKNOWN") for x in resolved.unknown]
    frame = strings_frame(rows, ("unit_key", "decision", "reason", "status"))
    with registered(con, "_scope_records", frame) as source:
        # 仅 IN_SCOPE 进入归属关系，后续 Candidate 检查与发布均复用同一范围边界。
        con.execute(f"CREATE OR REPLACE TEMP TABLE _scope_units AS SELECT unit_key FROM {source} WHERE decision='IN_SCOPE'")
        con.execute(f"INSERT INTO meta.run_unit(run_id,dataset,unit_key,scope_decision,reason,status,stage) "
                    f"SELECT ?,?,unit_key,decision,reason,status,'scope' FROM {source}", [run_id, dataset])
    mark_unpublished(con, run_id)
    if pins:
        # 记录本次实际读取的上游版本，不以运行结束时的最新版本回填。
        frame = strings_frame([(p.ref.dataset, p.ref.unit_key, str(p.publication_id)) for p in pins],
                              ("dataset", "unit_key", "pid"))
        with registered(con, "_audit_pins", frame) as source:
            con.execute(f"INSERT INTO meta.run_dependency SELECT ?,dataset,unit_key,pid::BIGINT FROM {source}", [run_id])


def record_delivery(con, run_id, delivery):
    """批量写入尝试信息和 Unit 交付状态；每次请求只保存一份控制信息。"""
    if delivery.attempts:
        # 控制转换：列式暂存使用字符串承载时间、计数及 JSON，再由 SQL 一次恢复 Meta 类型。
        # HTTP 尝试（含重试）各有一行；业务响应只在 evidence_path 指向的证据文件中保存。
        rows = [(r.request_id, str(r.attempt), r.api_name, encode(r.params), encode(r.fields),
                 r.requested_at.isoformat(), str(r.elapsed_seconds),
                 str(r.http_status) if r.http_status is not None else None,
                 str(r.provider_code) if r.provider_code is not None else None, encode(r.returned_fields),
                 str(r.row_count) if r.row_count is not None else None,
                 str(r.has_more) if r.has_more is not None else None, r.error, r.evidence_path)
                for r in delivery.attempts]
        names = ("request_id", "attempt", "api_name", "params", "fields", "requested_at", "elapsed",
                 "http_status", "provider_code", "returned_fields", "rows", "has_more", "error", "path")
        with registered(con, "_attempts", strings_frame(rows, names)) as source:
            con.execute(f"INSERT INTO meta.request_attempt SELECT ?,request_id,attempt::INTEGER,api_name,"
                        "params::JSON,fields::JSON,requested_at::TIMESTAMPTZ,elapsed::DOUBLE,"
                        "http_status::INTEGER,provider_code::INTEGER,returned_fields::JSON,rows::BIGINT,"
                        f"has_more::BOOLEAN,error,path FROM {source}", [run_id])
    if delivery.units:
        # 交付状态独立于执行状态：COMPLETE 只表示获取完成，不直接标记 PUBLISHED。
        # Unit 仅关联逻辑 request_id，重试序号在 request_attempt 中展开。
        frame = strings_frame([(u.unit_key, u.state, encode(u.request_ids)) for u in delivery.units],
                              ("unit_key", "state", "request_ids"))
        with registered(con, "_delivery_units", frame) as source:
            con.execute(f"UPDATE meta.run_unit AS u SET delivery_state=d.state,request_ids=d.request_ids::JSON,stage='fetch' "
                        f"FROM {source} d WHERE u.run_id=? AND u.unit_key=d.unit_key", [run_id])


def record_checks(con, run_id, checks):
    """每项检查存一次，集合式建立 Unit 的检查引用，不复制详情。"""
    if not checks:
        return
    # 每项检查分配本轮 check_id 后仅存一份；None 影响范围写为 SQL NULL，代表全局。
    frame = strings_frame([(str(i), c.level, c.rule, c.stage,
                            encode(c.units) if c.units is not None else None, encode(c.detail or {}))
                           for i, c in enumerate(checks)],
                          ("id", "level", "rule", "stage", "units", "detail"))
    with registered(con, "_checks", frame) as source:
        con.execute(f"INSERT INTO meta.validation_check SELECT ?,id::INTEGER,level,rule,stage,units::JSON,detail::JSON FROM {source}", [run_id])
    # 业务关联：全局检查关联所有 IN_SCOPE Unit，局部检查只关联列出的 Unit，详情不重复复制。
    con.execute(
        "UPDATE meta.run_unit AS u SET check_ids=x.ids FROM "
        "(SELECT u.unit_key,to_json(list(c.check_id ORDER BY c.check_id)) AS ids "
        "FROM meta.run_unit u JOIN meta.validation_check c ON u.run_id=c.run_id "
        "AND (c.units IS NULL OR list_contains(c.units::VARCHAR[],u.unit_key)) "
        "WHERE u.run_id=? AND u.scope_decision='IN_SCOPE' GROUP BY u.unit_key) x "
        "WHERE u.run_id=? AND u.unit_key=x.unit_key", [run_id, run_id])


def failure(con, run_id, units, stage, error=None):
    """批量标记本次未成功 Unit，保留既有成功范围及已提交的 Unit。"""
    if not units:
        return
    # 业务转换：仅修改本次尚未发布的执行状态，防止后续异常将已提交的 Unit 反写为失败。
    with registered(con, "_failed_units", pl.DataFrame({"unit_key": units}, schema={"unit_key": pl.String})) as source:
        con.execute(f"UPDATE meta.run_unit AS u SET status='FAILED',stage=?,error=? FROM {source} f "
                    "WHERE u.run_id=? AND u.unit_key=f.unit_key AND u.status<>'PUBLISHED'",
                    [stage, error, run_id])
    mark_unpublished(con, run_id)


def finish(con, run_id, error=None, failed_stage=None):
    """以已持久化 Unit 结局结束运行，只保存计数摘要并返回控制结果。"""
    records = con.execute("SELECT unit_key,status,stage,scope_decision FROM meta.run_unit WHERE run_id=? ORDER BY unit_key", [run_id]).fetchall()
    units = tuple(UnitResult(u, s, stage) for u, s, stage, _ in records)
    status_counts = Counter(u.status for u in units)
    # 业务检查：全局异常或 FAILED/UNKNOWN/PENDING 都表示本轮未完全成功；OUT_OF_SCOPE 不算失败。
    failed = bool(error) or any(status_counts[s] for s in ("FAILED", "UNKNOWN", "PENDING"))
    # 业务转换：失败且已有成功 Unit 为 PARTIAL；失败且无成功 Unit 为 FAILED；其余为 SUCCESS。
    status = ("PARTIAL" if status_counts["PUBLISHED"] else "FAILED") if failed else "SUCCESS"
    stats = con.execute(
        "SELECT (SELECT count(*) FROM meta.request_attempt WHERE run_id=?),"
        "(SELECT count(*) FROM meta.validation_check WHERE run_id=? AND level='ERROR'),"
        "(SELECT count(*) FROM meta.validation_check WHERE run_id=? AND level='WARNING')",
        [run_id, run_id, run_id]).fetchone()
    # Run 摘要只保存范围/结局/尝试/检查计数；完整状态和证据引用分别留在所属 Meta 表。
    summary = {"scope_counts": dict(Counter(r[3] for r in records)), "unit_counts": dict(status_counts),
               "request_attempts": stats[0], "check_counts": {"ERROR": stats[1], "WARNING": stats[2]}}
    # 生命周期：结束时间在汇总结局时写入；产物清理和最终诊断导出发生在这之后。
    con.execute("UPDATE meta.run SET status=?,finished_at=current_timestamp,summary=?,error=?,failed_stage=? WHERE run_id=?",
                [status, encode(summary), error, failed_stage, run_id])
    pubs = tuple(Publication(*r) for r in con.execute(
        "SELECT unit_key,publication_id,row_count FROM meta.publication WHERE run_id=? ORDER BY publication_id", [run_id]).fetchall())
    return RunResult(run_id, status, units, pubs, error)


def record_artifacts(con, run_id, artifacts):
    """记录清理或留存结果，不改已提交的业务结局。"""
    con.execute("UPDATE meta.run SET artifact_status=?,artifacts_complete=?,export_errors=? WHERE run_id=?",
                [artifacts.status, artifacts.complete, encode(artifacts.errors), run_id])
