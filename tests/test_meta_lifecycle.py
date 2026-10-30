"""审计生命周期：真实进程中断、归档事务及独立 CLI 的必要验收。"""

from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
import io
import json
from pathlib import Path
import select
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import duckdb

from quant_data import db
from quant_data.core import audit, maintenance
from quant_data.core.models import Check, Delivery, RequestAttempt, Scope, ScopeReason, UnitDelivery
from test_ingestion import FixtureDataset


class MetaLifecycleTest(unittest.TestCase):
    """所有数据库和归档均位于临时目录，不修改本地验证库。"""

    def setUp(self):
        """创建采用正式 Meta DDL 的隔离库。"""
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        db.initialize(self.con, FixtureDataset())
        self.archive = self.root / "archive"

    def pending(self, scope=None):
        """通过正式审计函数登记范围，不运行获取或发布。"""
        run_id, _ = audit.begin(self.con, "fixture", {}, self.root / "landing")
        audit.scope(self.con, run_id, "fixture", scope or Scope(("a",)), ())
        return run_id

    def completed(self, days=60):
        """形成有请求、检查和失败结局的真实审计明细，并指定留存年龄。"""
        run_id = self.pending()
        attempt = RequestAttempt("request", 1, "fixture", {}, (), datetime.now(timezone.utc), 0.1)
        audit.record_delivery(self.con, run_id, Delivery(None, (UnitDelivery("a", "FAILED", ("request",)),), (attempt,)))
        audit.record_checks(self.con, run_id, (Check("ERROR", "source.request", "fetch", ("a",)),))
        audit.failure(self.con, run_id, ("a",), "fetch", "provider failure")
        audit.finish(self.con, run_id)
        self.con.execute("UPDATE meta.run SET finished_at=current_timestamp-?*INTERVAL '1 day' WHERE run_id=?", [days, run_id])
        return run_id

    def test_reconcile_preserves_reasons_and_newer_coverage_and_is_atomic(self):
        """收尾只修改 PENDING，旧运行不覆盖更新的 UNKNOWN；事务失败可重试。"""
        scope = Scope(("a", "b", "c"), (ScopeReason("out", "closed"),), (ScopeReason("unknown", "missing"),))
        old = self.pending(scope)
        audit.failure(self.con, old, ("b",), "prepare", "original error")
        newer = self.pending(Scope((), unknown=(ScopeReason("a", "newer decision"),)))
        audit.finish(self.con, newer)
        # 模拟在汇总前再次中断，Unit 和 Coverage 的修改必须整体回滚。
        with patch.object(audit, "finish", side_effect=RuntimeError("interrupted again")):
            with self.assertRaisesRegex(RuntimeError, "interrupted again"):
                audit.reconcile(self.con)
        self.assertEqual(self.con.execute("SELECT status FROM meta.coverage WHERE unit_key='c'").fetchone()[0], "PENDING")
        self.assertEqual(audit.reconcile(self.con), [{"run_id": old, "status": "FAILED"}])
        units = {u: (s, stage, error) for u, s, stage, error in self.con.execute(
            "SELECT unit_key,status,stage,error FROM meta.run_unit WHERE run_id=?", [old]).fetchall()}
        self.assertEqual(units["a"][:2], ("FAILED", "scope"))
        self.assertEqual(units["b"], ("FAILED", "prepare", "original error"))
        self.assertEqual(units["out"][0], "OUT_OF_SCOPE")
        self.assertEqual(units["unknown"][0], "UNKNOWN")
        self.assertEqual(self.con.execute("SELECT status FROM meta.coverage WHERE unit_key='a'").fetchone()[0], "UNKNOWN")
        self.assertEqual(self.con.execute("SELECT status FROM meta.coverage WHERE unit_key='c'").fetchone()[0], "FAILED")
        closed = self.con.execute("SELECT finished_at,reconciled_at FROM meta.run WHERE run_id=?", [old]).fetchone()
        self.assertEqual(closed[0], closed[1])
        self.assertEqual(audit.reconcile(self.con), [])
        self.assertEqual(self.con.execute("SELECT reconciled_at FROM meta.run WHERE run_id=?", [newer]).fetchone(), (None,))

    def test_kill_before_and_after_publication_commit(self):
        """真实杀死子进程，验证 WAL 恢复后旧版保护和提交后中断收尾。"""
        script = '''
import sys
from pathlib import Path
from unittest.mock import Mock, patch
import duckdb
import polars as pl
sys.path.insert(0, "tests")
from test_ingestion import FixtureDataset, FixtureAdapter
from quant_data import db
from quant_data.core import audit, runner
from quant_data.core.models import RunOptions
path, boundary = sys.argv[1:]
con = duckdb.connect(path)
dataset = FixtureDataset()
db.initialize(con, dataset)
options = RunOptions(Path(path).with_suffix(".landing"))
def ingest(connection, value):
    """用真实发布事务写入可对照的两个 Unit。"""
    frame = pl.DataFrame({"unit_key": ["a", "b"], "value": [value, value]}, schema=dataset.schema)
    return runner.run(connection, dataset, FixtureAdapter(frame), {}, options)
def pause(*args, **kwargs):
    """通知父进程已到达指定事务边界，等待被强制终止。"""
    print("READY", flush=True)
    sys.stdin.read(1)
ingest(con, 1)
if boundary == "before_commit":
    proxy = Mock(wraps=con)
    def execute(sql, *args):
        """在成功元数据已写入但事务尚未 COMMIT 时暂停。"""
        result = con.execute(sql, *args)
        if " ".join(sql.split()).startswith("UPDATE meta.run_unit AS u SET status = 'PUBLISHED'"):
            pause()
        return result
    proxy.execute.side_effect = execute
    ingest(proxy, 2)
else:
    with patch.object(audit, "finish", side_effect=pause):
        ingest(con, 2)
'''
        for boundary in ("before_commit", "after_commit"):
            with self.subTest(boundary=boundary):
                path = self.root / f"{boundary}.duckdb"
                with subprocess.Popen([sys.executable, "-c", script, str(path), boundary],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True) as child:
                    try:
                        self.assertTrue(select.select([child.stdout], [], [], 30)[0], "child did not reach boundary")
                        line = child.stdout.readline()
                        self.assertEqual(line.strip(), "READY")
                        # 活动写库进程持有文件锁，新 CLI 无法取得用于收尾的读写连接。
                        with self.assertRaises(duckdb.IOException):
                            duckdb.connect(str(path))
                    finally:
                        child.kill()
                        child.wait(timeout=10)
                # 重新打开真实磁盘库，先由 DuckDB 恢复事务，再关闭遗留审计。
                with duckdb.connect(str(path)) as con:
                    run_id = con.execute("SELECT run_id FROM meta.run WHERE status='RUNNING'").fetchone()[0]
                    result = audit.reconcile(con)
                    expected = "FAILED" if boundary == "before_commit" else "PARTIAL"
                    self.assertEqual(result, [{"run_id": run_id, "status": expected}])
                    value = 1 if boundary == "before_commit" else 2
                    self.assertEqual(con.execute("SELECT value FROM canonical.fixture ORDER BY unit_key").fetchall(), [(value,), (value,)])
                    self.assertEqual(con.execute("SELECT count(*) FROM meta.publication").fetchone()[0], value * 2)
                    self.assertEqual(con.execute("SELECT DISTINCT status FROM meta.coverage").fetchall(), [("PUBLISHED",)])
                    unit_status = "FAILED" if boundary == "before_commit" else "PUBLISHED"
                    self.assertEqual(con.execute("SELECT DISTINCT status FROM meta.run_unit WHERE run_id=?", [run_id]).fetchall(), [(unit_status,)])
                    self.assertEqual(audit.reconcile(con), [])

    def test_retention_boundaries_and_long_lived_records(self):
        """60/365 天边界、三表整体退出、长期表和原始计数保持。"""
        running = self.pending()
        recent, archive, expire = (self.completed(days) for days in (59, 60, 365))
        # 添加长期发布和依赖记录，验证明细退出不拆除版本骨架。
        self.con.execute("INSERT INTO meta.publication(dataset,unit_key,row_count,run_id) VALUES ('fixture','a',0,?)", [archive])
        self.con.execute("INSERT INTO meta.run_dependency VALUES (?,'upstream','a',1)", [archive])
        before = {t: self.con.execute(f"SELECT * FROM meta.{t}").fetchall()
                  for t in ("publication", "run_dependency", "coverage")}
        summaries = self.con.execute("SELECT run_id,summary FROM meta.run ORDER BY run_id").fetchall()
        # 同一事务固定 current_timestamp，精确核对阈值前后一个微秒。
        self.con.execute("BEGIN")
        now = self.con.execute("SELECT current_timestamp").fetchone()[0]
        self.con.execute("UPDATE meta.run SET finished_at=? WHERE run_id=?", [now - timedelta(days=60) + timedelta(microseconds=1), recent])
        self.con.execute("UPDATE meta.run SET finished_at=? WHERE run_id=?", [now - timedelta(days=60), archive])
        self.con.execute("UPDATE meta.run SET finished_at=? WHERE run_id=?", [now - timedelta(days=365), expire])
        self.con.execute("UPDATE meta.run SET finished_at=? WHERE run_id=?", [now - timedelta(days=400), running])
        preview = maintenance.retain(self.con, self.archive)
        self.assertEqual({r["run_id"]: r["action"] for r in preview["runs"]}, {archive: "archive", expire: "archive_and_expire"})
        self.assertFalse(self.archive.exists())
        self.con.execute("COMMIT")
        self.con.execute("UPDATE meta.run SET finished_at=current_timestamp WHERE run_id=?", [recent])
        maintenance.retain(self.con, self.archive, apply=True)
        states = dict(self.con.execute("SELECT run_id,audit_state FROM meta.run").fetchall())
        self.assertEqual(states, {running: "ONLINE", recent: "ONLINE", archive: "ARCHIVED", expire: "EXPIRED"})
        for table in maintenance.DETAIL_TABLES:
            self.assertEqual(self.con.execute(f"SELECT count(*) FROM meta.{table} WHERE run_id IN (?,?)", [archive, expire]).fetchone()[0], 0)
            self.assertEqual(self.con.execute("SELECT count(*) FROM read_parquet(?)", [str(self.archive / archive / f"{table}.parquet")]).fetchone()[0], 1)
        self.assertFalse((self.archive / expire).exists())
        self.assertEqual(self.con.execute("SELECT run_id,summary FROM meta.run ORDER BY run_id").fetchall(), summaries)
        for table, rows in before.items():
            self.assertEqual(self.con.execute(f"SELECT * FROM meta.{table}").fetchall(), rows)

    def test_archive_failure_and_commit_retry(self):
        """文件或数据库提交失败时在线明细仍完整，重试可复用最终归档。"""
        run_id = self.completed()
        with patch.object(maintenance, "_verify_archive", side_effect=ValueError("bad archive")):
            with self.assertRaisesRegex(ValueError, "bad archive"):
                maintenance.retain(self.con, self.archive, apply=True)
        self.assertEqual(self.con.execute("SELECT audit_state FROM meta.run WHERE run_id=?", [run_id]).fetchone()[0], "ONLINE")
        # 故障发生于文件就位之后、数据库提交之前；事务回滚而完整归档留在最终路径。
        from contextlib import contextmanager
        @contextmanager
        def fail_commit(con):
            """在三表删除之后抛错，使用真实事务验证回滚。"""
            with db.transaction(con):
                yield
                raise RuntimeError("commit interrupted")
        with patch.object(maintenance, "transaction", fail_commit):
            with self.assertRaisesRegex(RuntimeError, "commit interrupted"):
                maintenance.retain(self.con, self.archive, apply=True)
        self.assertTrue((self.archive / run_id / "manifest.json").is_file())
        for table in maintenance.DETAIL_TABLES:
            self.assertEqual(self.con.execute(f"SELECT count(*) FROM meta.{table} WHERE run_id=?", [run_id]).fetchone()[0], 1)
        # 最终目录被改动时不可继续删除在线明细，真实执行摘要校验而非只检查目录存在。
        manifest_path = self.archive / run_id / "manifest.json"
        original = manifest_path.read_text()
        manifest = json.loads(original)
        manifest["tables"]["run_unit"]["sha256"] = "0" * 64
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "verification failed"):
            maintenance.retain(self.con, self.archive, apply=True)
        self.assertEqual(self.con.execute("SELECT audit_state FROM meta.run WHERE run_id=?", [run_id]).fetchone()[0], "ONLINE")
        manifest_path.write_text(original)
        maintenance.retain(self.con, self.archive, apply=True)
        self.assertEqual(maintenance.retain(self.con, self.archive, apply=True)["runs"], [])

    def test_early_failure_archives_empty_detail_tables(self):
        """Scope 前失败没有明细，也必须生成可读且保有 Schema 的三表归档。"""
        run_id, _ = audit.begin(self.con, "fixture", {}, self.root / "landing")
        audit.finish(self.con, run_id, "missing dependency", "dependencies")
        self.con.execute("UPDATE meta.run SET finished_at=current_timestamp-INTERVAL '60 days' WHERE run_id=?", [run_id])
        maintenance.retain(self.con, self.archive, apply=True)
        for table in maintenance.DETAIL_TABLES:
            self.assertEqual(self.con.execute("SELECT count(*) FROM read_parquet(?)",
                [str(self.archive / run_id / f"{table}.parquet")]).fetchone()[0], 0)

    def test_expiry_delete_retry_and_missing_archive(self):
        """EXPIRED 删除失败可重试；ARCHIVED 缺失必须报告。"""
        run_id = self.completed(365)
        with patch.object(maintenance.shutil, "rmtree", side_effect=OSError("delete failed")):
            with self.assertRaisesRegex(OSError, "delete failed"):
                maintenance.retain(self.con, self.archive, apply=True)
        self.assertEqual(self.con.execute("SELECT audit_state FROM meta.run WHERE run_id=?", [run_id]).fetchone()[0], "EXPIRED")
        self.assertEqual(maintenance.retain(self.con, self.archive)["runs"][0]["action"], "cleanup")
        maintenance.retain(self.con, self.archive, apply=True)
        self.assertFalse((self.archive / run_id).exists())
        missing = self.completed(60)
        maintenance.retain(self.con, self.archive, apply=True)
        (self.archive / missing / "request_attempt.parquet").unlink()
        with self.assertRaisesRegex(ValueError, "Archived audit file missing"):
            maintenance.retain(self.con, self.archive, apply=True)

    def test_cli_needs_no_credentials_and_rejects_old_schema(self):
        """独立入口无供应商依赖，默认只预览；旧布局报错且原库不改写。"""
        from quant_data.__main__ import main
        run_id = self.pending()
        for command, expected in (("retain", "runs"), ("reconcile", "reconciled_runs")):
            output = io.StringIO()
            with patch("sys.argv", ["quant_data", "meta", command, "--db", str(self.path)]), \
                 patch("quant_data.__main__.dotenv_values", side_effect=AssertionError("credentials read")), redirect_stdout(output):
                self.assertEqual(main(), 0)
            result = json.loads(output.getvalue())
            self.assertIn(expected, result)
        self.assertEqual(self.con.execute("SELECT status FROM meta.run WHERE run_id=?", [run_id]).fetchone()[0], "FAILED")
        old_path = self.root / "old.duckdb"
        with duckdb.connect(str(old_path)) as old:
            old.execute("CREATE SCHEMA meta; CREATE TABLE meta.run(run_id VARCHAR)")
        output = io.StringIO()
        with patch("sys.argv", ["quant_data", "meta", "reconcile", "--db", str(old_path)]), redirect_stdout(output):
            self.assertEqual(main(), 1)
        self.assertIn("no automatic migration", output.getvalue())
        with duckdb.connect(str(old_path), read_only=True) as old:
            self.assertEqual(old.execute("SHOW TABLES FROM meta").fetchall(), [("run",)])
