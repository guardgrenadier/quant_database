"""按 Run 归档和过期执行明细；独立维护入口持有数据库读写连接。"""

import hashlib
import json
from pathlib import Path
import shutil

from quant_data.db import transaction

DETAIL_TABLES = ("run_unit", "request_attempt", "validation_check")


def plan(con):
    """预览到期 Run；不改变数据库、文件或仍在运行的记录。"""
    rows = con.execute("""
        SELECT run_id,audit_state,audit_archive_path,
               finished_at <= current_timestamp - INTERVAL '365 days' AS expired
        FROM meta.run WHERE status<>'RUNNING' AND (
            (audit_state='ONLINE' AND finished_at <= current_timestamp - INTERVAL '60 days')
            OR audit_state IN ('ARCHIVED','EXPIRED'))
        ORDER BY finished_at,run_id
    """).fetchall()
    actions = []
    for run_id, state, path, expired in rows:
        # ARCHIVED 承诺文件可用；缺失应明确报错，不能误当成已经正常过期。
        if state == "ARCHIVED":
            for name in ("manifest.json", *(f"{t}.parquet" for t in DETAIL_TABLES)):
                if not (Path(path) / name).is_file():
                    raise ValueError(f"Archived audit file missing: {Path(path) / name}")
        if state == "ONLINE":
            counts = {t: con.execute(f"SELECT count(*) FROM meta.{t} WHERE run_id=?", [run_id]).fetchone()[0]
                      for t in DETAIL_TABLES}
            action = "archive_and_expire" if expired else "archive"
        elif state == "ARCHIVED" and expired:
            counts, action = None, "expire"
        elif state == "EXPIRED" and Path(path).exists():
            counts, action = None, "cleanup"
        else:
            continue
        actions.append({"run_id": run_id, "action": action, "counts": counts, "archive_path": path})
    return actions


def _digest(path):
    """流式计算归档摘要，避免把整个文件复制到 Python 内存。"""
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _schema(con, query, params=()):
    """取列名和类型用于 Parquet 往返校验；文件不承载数据库主键约束。"""
    return [[row[0], row[1]] for row in con.execute(f"DESCRIBE {query}", params).fetchall()]


def _verify_archive(con, run_id, folder):
    """删除在线明细前校验归档；也用于复用提交前中断留下的完整目录。"""
    manifest = json.loads((folder / "manifest.json").read_text())
    # 归档身份、三表内容清单必须对应本 Run，不能把其他目录误作可删除依据。
    if manifest["run_id"] != run_id or set(manifest["tables"]) != set(DETAIL_TABLES):
        raise ValueError(f"Audit archive identity mismatch: {folder}")
    for table in DETAIL_TABLES:
        path = folder / f"{table}.parquet"
        info = manifest["tables"][table]
        count = con.execute(f"SELECT count(*) FROM meta.{table} WHERE run_id=?", [run_id]).fetchone()[0]
        file_count, foreign_count = con.execute(
            "SELECT count(*),count(*) FILTER (WHERE run_id IS DISTINCT FROM ?) FROM read_parquet(?)",
            [run_id, str(path)]).fetchone()
        schema = _schema(con, f"SELECT * FROM meta.{table}")
        # 类型、数量、归属及摘要一起确认；空表同样必须保有完整 Schema 和文件。
        if (count != info["rows"] or count != file_count or foreign_count
                or schema != info["schema"] or schema != _schema(con, "SELECT * FROM read_parquet(?)", [str(path)])
                or _digest(path) != info["sha256"]):
            raise ValueError(f"Audit archive verification failed: {path}")


def _archive(con, run_id, root):
    """先形成完整 Parquet 归档，再原子删除该 Run 的三表在线明细。"""
    root.mkdir(parents=True, exist_ok=True)
    folder = root / run_id
    if not folder.exists():
        # 固定临时目录可重做未完成导出；最终目录只有在三张表全部校验后才出现。
        temporary = root / f".{run_id}.tmp"
        temporary.mkdir(exist_ok=True)
        manifest = {"run_id": run_id, "tables": {}}
        for table in DETAIL_TABLES:
            path = temporary / f"{table}.parquet"
            con.execute(f"COPY (SELECT * FROM meta.{table} WHERE run_id=$run) TO $path (FORMAT PARQUET)",
                        {"run": run_id, "path": str(path)})
            manifest["tables"][table] = {
                "rows": con.execute(f"SELECT count(*) FROM meta.{table} WHERE run_id=?", [run_id]).fetchone()[0],
                "schema": _schema(con, f"SELECT * FROM meta.{table}"), "sha256": _digest(path),
            }
        (temporary / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
        _verify_archive(con, run_id, temporary)
        temporary.rename(folder)
    else:
        # 文件已就位而数据库事务未提交的中断，不重新覆盖；核对后继续提交。
        _verify_archive(con, run_id, folder)
    with transaction(con):
        for table in DETAIL_TABLES:
            con.execute(f"DELETE FROM meta.{table} WHERE run_id=?", [run_id])
        con.execute("UPDATE meta.run SET audit_state='ARCHIVED',audit_archive_path=? WHERE run_id=?",
                    [str(folder), run_id])
    return folder


def retain(con, archive_root, *, apply=False):
    """执行 60 天归档、365 天过期；默认仅预览，异常保留已完成的 Run 结果。"""
    actions = plan(con)
    root = Path(archive_root).resolve()
    for item in actions:
        # ONLINE 的目标位置尚未入库；预览也展示确定的归档位置。
        path = Path(item["archive_path"]) if item["archive_path"] else root / item["run_id"]
        item["archive_path"] = str(path)
        if not apply:
            continue
        if item["action"] in ("archive", "archive_and_expire"):
            path = _archive(con, item["run_id"], root)
        if item["action"] in ("expire", "archive_and_expire", "cleanup"):
            # 先停止明细可用承诺，再删除文件；删除失败时保留 EXPIRED + 路径供下次重试。
            with transaction(con):
                con.execute("UPDATE meta.run SET audit_state='EXPIRED' WHERE run_id=?", [item["run_id"]])
            shutil.rmtree(path)
    return {"applied": apply, "runs": actions}
