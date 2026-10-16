"""正式链路的故障、等价性和批量机制测试；供应商替身只存在于测试边界。"""

from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import time
import unittest
from unittest.mock import MagicMock, Mock, patch
from urllib.error import HTTPError

import duckdb
import polars as pl

from quant_data import db
from quant_data.core import audit, deps, publisher, runner, validation
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import (
    Check, Delivery, DependencyRef, RequestAttempt, RunOptions, Scope, ScopeReason,
    UnitDelivery, control_data,
)
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.sources.tushare import client
from quant_data.sources.tushare.stock_basic import TushareStockBasicAdapter

BASELINE = Path("data/landing/walkthrough-stock_basic-20261014T173405534873")
FIELDS = tuple(StockBasicDataset.schema.names())


def stock_row(**changes):
    """供应商测试记录保持字符串类型，覆盖字段以注入边界异常。"""
    row = dict.fromkeys(FIELDS)
    row.update(ts_code="000001.SZ", symbol="000001", name="example", exchange="SZSE",
               list_status="L", list_date="19910403", curr_type="CNY", is_hs="N")
    row.update(changes)
    return row


class FakeClient:
    """仅替换通信边界，仍走真实 Adapter、Validator、Publisher 和证据路径。"""

    def __init__(self, rows, transform=None):
        self.rows, self.transform = rows, transform

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        """按请求生成信封；类型、缺字段、范围等故障由真实适配器识别。"""
        selected = [r for r in self.rows if r["list_status"] == params["list_status"] and r["exchange"] == params["exchange"]]
        data = {"fields": list(fields), "items": [[r[f] for f in fields] for r in selected], "has_more": False, "count": 0}
        if self.transform:
            self.transform(params, data)
        body = {"code": 0, "msg": None, "data": data}
        attempt = RequestAttempt(request_id, 1, api_name, params, tuple(fields), datetime.now(timezone.utc), 0.01,
                                 200, 0, tuple(data["fields"]), len(data["items"]), data.get("has_more"))
        on_attempt(attempt, body)
        return body


class FixtureDataset:
    """只在测试中使用的小型业务契约，不作为生产注册的 Dataset。"""

    name, table, record_key = "fixture", "canonical.fixture", ("unit_key",)

    def __init__(self, units=("a", "b"), large=False):
        self.scope = Scope(tuple(units))
        self.required = ()
        self.checks = ()
        self.schema = pl.Schema({"unit_key": pl.String, **({"seq": pl.Int64} if large else {}), "value": pl.Int64})
        self.ddl = "CREATE SCHEMA IF NOT EXISTS canonical; CREATE TABLE canonical.fixture(unit_key VARCHAR," + (
            "seq BIGINT,value BIGINT,PRIMARY KEY(unit_key,seq))" if large else "value BIGINT,PRIMARY KEY(unit_key))")

    def dependencies(self, params):
        """允许测试缺失与版本变化的依赖要求。"""
        return self.required

    def resolve_scope(self, params, pins, con):
        """返回测试指定的真实控制范围。"""
        return self.scope

    def unit_key_sql(self, alias):
        """业务 Unit 归属在 Dataset 内定义，内核只使用该表达式 JOIN。"""
        return f"{alias}.unit_key"

    def validate_candidate(self, con, candidate_relation, scope):
        """注入影响明确的检查以验证部分成功。"""
        return self.checks


class FixtureAdapter:
    """机制测试的列式交付，避免假正式 Dataset 或分钟接口。"""

    def __init__(self, frame, checks=()):
        self.frame, self.checks = frame, checks
        self.called = False

    def fetch(self, scope, artifacts):
        """明确交付所有目标 Unit，包括没有任何记录的 Unit。"""
        self.called = True
        return Delivery(self.frame, tuple(UnitDelivery(u, "COMPLETE") for u in scope.units), checks=self.checks)

    def validate_source(self, delivery, scope):
        """该替身的字段直接由测试契约生成，不额外伪造验收。"""
        return ()

    def normalize(self, delivery, scope):
        """返回已按契约构造的测试业务表。"""
        return delivery.frame, ()


class StockIngestionTest(unittest.TestCase):
    """正式股票链路、兼容边界和诊断产物验收。"""

    def setUp(self):
        """隔离数据库与暂存目录，仅在测试中关闭请求等待。"""
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.dataset = StockBasicDataset()
        db.initialize(self.con, self.dataset)
        interval = patch("quant_data.sources.tushare.stock_basic.REQUEST_INTERVAL", 0)
        interval.start()
        self.addCleanup(interval.stop)

    def ingest(self, rows, transform=None, con=None, keep=False):
        """运行实际 Runner 和适配器；可选择保留模式或故障连接。"""
        adapter = TushareStockBasicAdapter(FakeClient(rows, transform))
        return runner.run(con or self.con, self.dataset, adapter, {}, RunOptions(self.root / "landing", keep))

    def current(self):
        """仅比较当前权威事实和成功版本，失败运行的审计允许增加。"""
        return tuple(self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.stock_basic ORDER BY ts_code",
            "SELECT * FROM meta.coverage ORDER BY unit_key",
            "SELECT * FROM meta.publication ORDER BY publication_id"))

    def assert_failed_preserving(self, action):
        """失败后权威状态不变，原始证据仍保留。"""
        before = self.current()
        result = action()
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(before, self.current())
        self.assertTrue(Path(result.artifact_dir).is_dir())
        return result

    def test_success_replacement_and_metadata(self):
        """验证特殊代码、条件日期、共享检查和重跑的范围替换。"""
        rows = [stock_row(), stock_row(ts_code="T600018.SH", symbol="T600018", exchange="SSE", list_status="D", delist_date="20061020"),
                stock_row(ts_code="874396.BJ", symbol="920238", exchange="BSE", list_status="UN", list_date=None),
                stock_row(ts_code="601198.SH", symbol="601198", exchange="SSE", delist_date="20991019")]
        result = self.ingest(rows)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(result.publications[0].row_count, 4)
        self.assertEqual(result.artifact_status, "CLEANED")
        self.assertFalse(Path(result.artifact_dir).exists())
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.request_attempt").fetchone()[0], 15)
        self.assertEqual({r[0] for r in self.con.execute("SELECT rule FROM meta.validation_check").fetchall()},
                         {"stock.future_date", "stock.symbol_mapping", "stock.optional_nulls"})
        summary = json.loads(self.con.execute("SELECT summary FROM meta.run WHERE run_id=?", [result.run_id]).fetchone()[0])
        self.assertNotIn("units", summary)
        self.assertEqual(summary["request_attempts"], 15)
        repeat = self.ingest(rows)
        self.assertGreater(repeat.publications[0].publication_id, result.publications[0].publication_id)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_basic").fetchone()[0], 4)
        final = self.ingest([stock_row(name="changed")])
        self.assertEqual(final.status, "SUCCESS")
        self.assertEqual(self.con.execute("SELECT ts_code,name FROM canonical.stock_basic").fetchall(), [("000001.SZ", "changed")])
        self.assertEqual(self.con.execute("SELECT status,publication_id FROM meta.coverage").fetchone(),
                         ("PUBLISHED", final.publications[0].publication_id))

    def test_validation_failures_preserve_previous_publication(self):
        """空集、键冲突、非法日期和业务约束异常保留旧版。"""
        self.ingest([stock_row()])
        cases = [[], [stock_row(), stock_row()], [stock_row(ts_code=None)], [stock_row(name=" ")],
                 [stock_row(list_date="20260230")], [stock_row(list_date="2026111")],
                 [stock_row(list_date=None)], [stock_row(is_hs="X")], [stock_row(delist_date="19900101")],
                 [stock_row(), stock_row(ts_code="600001.SH", exchange="SSE", list_status="D", delist_date=None)]]
        for rows in cases:
            with self.subTest(rows=rows):
                self.assert_failed_preserving(lambda: self.ingest(rows))

    def test_source_failures_preserve_previous_publication(self):
        """请求、类型、范围、缺字段与截断失败阻断快照。"""
        self.ingest([stock_row()])
        def missing(params, data):
            """保持行宽与信封一致，仅删除必需字段。"""
            index = data["fields"].index("delist_date")
            data["fields"].pop(index)
            data["items"] = [r[:index] + r[index + 1:] for r in data["items"]]
        def full(params, data):
            """标记还有未交付记录。"""
            data["has_more"] = True
        def missing_marker(params, data):
            """省略交付标记不能默认成功。"""
            del data["has_more"]
        def wrong_range(params, data):
            """有行时改成与请求不同的交易所。"""
            if data["items"]:
                data["items"][0][data["fields"].index("exchange")] = "SSE"
        def numeric(params, data):
            """数字不能隐式转换成字符串代码。"""
            if data["items"]:
                data["items"][0][data["fields"].index("symbol")] = 1
        def failure(params, data):
            """模拟终态通信异常。"""
            raise RuntimeError("injected request failure")
        for transform in (missing, full, missing_marker, wrong_range, numeric, failure):
            with self.subTest(transform=transform.__name__):
                self.assert_failed_preserving(lambda: self.ingest([stock_row()], transform))
        self.assert_failed_preserving(lambda: self.ingest([stock_row()] * 6000))

    def test_first_failure_has_coverage_and_retained_evidence(self):
        """首次失败可追溯，原始信封没有被清理。"""
        result = self.ingest([stock_row(list_date="invalid")])
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(self.con.execute("SELECT status,publication_id FROM meta.coverage").fetchone(), ("FAILED", None))
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.publication").fetchone()[0], 0)
        self.assertEqual(len(list(Path(result.artifact_dir).rglob("request-*.json"))), 15)

    def test_transaction_rolls_back_data_and_success_metadata(self):
        """在 DELETE 后和成功元数据写入后失败，重新打开库仍为旧版。"""
        self.ingest([stock_row()])
        for prefix in ('INSERT INTO "canonical"."stock_basic"', "UPDATE meta.run_unit AS u SET status='PUBLISHED'"):
            proxy = Mock(wraps=self.con)
            def execute(sql, *args):
                """在指定发布语句前故障，其他 SQL 走真实连接。"""
                if sql.startswith(prefix):
                    raise RuntimeError("injected publication failure")
                return self.con.execute(sql, *args)
            proxy.execute.side_effect = execute
            self.assert_failed_preserving(lambda: self.ingest([stock_row(name="new")], con=proxy))
        self.con.close()
        with duckdb.connect(str(self.path), read_only=True) as reopened:
            self.assertEqual(reopened.execute("SELECT name FROM canonical.stock_basic").fetchone()[0], "example")
            self.assertEqual(reopened.execute("SELECT count(*) FROM meta.publication").fetchone()[0], 1)

    def test_existing_wrong_schema_is_not_migrated(self):
        """列不匹配和旧 Meta 都明确失败，不改写原型库。"""
        self.con.execute("ALTER TABLE canonical.stock_basic ADD COLUMN unexpected INTEGER")
        with self.assertRaisesRegex(ValueError, "no automatic migration"):
            db.initialize(self.con, self.dataset)
        with duckdb.connect(":memory:") as old:
            old.execute("CREATE SCHEMA meta; CREATE TABLE meta.run(run_id VARCHAR)")
            with self.assertRaisesRegex(ValueError, "new --db"):
                db.initialize(old, self.dataset)
            self.assertEqual(old.execute("SHOW TABLES FROM meta").fetchall(), [("run",)])

    def test_keep_artifacts_and_manifest(self):
        """完整产物明确导出，Manifest 每项文件哈希一致。"""
        result = self.ingest([stock_row()], keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertTrue(result.artifacts_complete, result)
        root = Path(result.artifact_dir)
        manifest = json.loads((root / "manifest.json").read_text())
        self.assertTrue(manifest["complete"])
        for item in manifest["files"]:
            self.assertEqual(hashlib.sha256((root / item["path"]).read_bytes()).hexdigest(), item["sha256"])
        raw = pl.read_parquet(root / "landing.parquet")
        candidate = pl.read_parquet(root / "candidate.parquet")
        self.assertEqual(raw.width, 19)
        self.assertEqual(candidate.schema, self.dataset.schema)
        self.assertEqual(candidate.to_dicts()[0]["symbol"], "000001")

    def test_export_failure_does_not_revoke_publication(self):
        """诊断不完整独立报告，业务状态和成功版本保留。"""
        with patch.object(pl.DataFrame, "write_parquet", side_effect=OSError("export injected")):
            result = self.ingest([stock_row()], keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertFalse(result.artifacts_complete)
        self.assertTrue(result.export_errors)
        self.assertEqual(self.con.execute("SELECT status FROM meta.coverage").fetchone()[0], "PUBLISHED")

    def test_raw_evidence_failure_blocks_before_publication(self):
        """原始证据落盘故障明确阻断，而非伪造完整交付。"""
        self.ingest([stock_row()])
        with patch.object(Artifacts, "record_attempt", side_effect=OSError("disk injected")):
            self.assert_failed_preserving(lambda: self.ingest([stock_row()]))

    def test_cleanup_failure_is_separate_from_success(self):
        """清理失败留独立告警，成功数据与版本仍然有效。"""
        with patch("quant_data.core.artifacts.shutil.rmtree", side_effect=OSError("cleanup injected")):
            result = self.ingest([stock_row()])
        self.assertEqual(result.status, "SUCCESS")
        self.assertEqual(result.artifact_status, "CLEANUP_FAILED")
        self.assertTrue(result.export_errors)
        self.assertEqual(self.con.execute("SELECT artifact_status FROM meta.run").fetchone()[0], "CLEANUP_FAILED")

    @unittest.skipUnless(BASELINE.exists(), "本地真实响应样本不随 Git 分发")
    def test_saved_real_responses_match_candidate(self):
        """重放真实响应，在内存得到与旧 Candidate 逐列一致的业务数据。"""
        bodies = {}
        for path in (BASELINE / "landing/e474d6df-3c32-439a-8e23-4bf744f584ee").glob("*.json"):
            evidence = json.loads(path.read_text())
            bodies[(evidence["params"]["list_status"], evidence["params"]["exchange"])] = evidence["response"]
        class ReplayClient:
            """仅测试离线等价性，不作为正式 Source。"""
            def query(self, api, params, fields, *, request_id, on_attempt):
                """返回真实样本并记录真实交付结构。"""
                body = bodies[(params["list_status"], params["exchange"])]
                data = body["data"]
                on_attempt(RequestAttempt(request_id, 1, api, params, fields, datetime.now(timezone.utc), 0.0,
                                          200, 0, tuple(data["fields"]), len(data["items"]), data["has_more"]), body)
                return body
        result = runner.run(self.con, self.dataset, TushareStockBasicAdapter(ReplayClient()), {},
                            RunOptions(self.root / "landing", True))
        self.assertEqual(result.status, "SUCCESS", result)
        actual = pl.read_parquet(Path(result.artifact_dir) / "candidate.parquet").sort("ts_code")
        expected = pl.read_parquet(BASELINE / "tables/candidate_stock_basic.parquet").sort("ts_code")
        self.assertEqual(actual.schema, expected.schema)
        self.assertTrue(actual.equals(expected))


class MechanismTest(unittest.TestCase):
    """通用机制不依赖真实股票字段，也不建立新的生产 Dataset。"""

    def setUp(self):
        """明确 Schema 的空 Candidate 可代表合法零行范围。"""
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.con = duckdb.connect(":memory:")
        self.addCleanup(self.con.close)
        self.dataset = FixtureDataset()
        db.initialize(self.con, self.dataset)
        self.con.execute("INSERT INTO canonical.fixture VALUES ('a',1),('b',2)")
        self.frame = pl.DataFrame(schema=self.dataset.schema)

    def run_frame(self, frame=None, checks=()):
        """经实际内核处理测试批次和控制范围。"""
        return runner.run(self.con, self.dataset, FixtureAdapter(self.frame if frame is None else frame, checks), {},
                          RunOptions(self.root / "landing"))

    def test_zero_row_replacement_and_explicit_units(self):
        """验收阻断 a，零行 b 仍替换并发布，未合格 Candidate 行不写入。"""
        frame = pl.DataFrame({"unit_key": ["a"], "value": [99]}, schema=self.dataset.schema)
        result = self.run_frame(frame, (Check("ERROR", "fixture.blocked", "source_validation", ("a",)),))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(result.publications[0].row_count, 0)
        self.assertEqual(self.con.execute("SELECT * FROM canonical.fixture").fetchall(), [("a", 1)])

    def test_runner_partial_success_from_source_error(self):
        """局部交付 ERROR 不妨碍另一个 Unit 发布。"""
        frame = pl.DataFrame({"unit_key": ["a", "b"], "value": [99, 77]}, schema=self.dataset.schema)
        result = self.run_frame(frame, (Check("ERROR", "fixture.delivery", "fetch", ("a",)),))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(self.con.execute("SELECT * FROM canonical.fixture ORDER BY unit_key").fetchall(), [("a", 1), ("b", 77)])

    def test_unlocatable_and_out_of_scope_errors_block_publication(self):
        """范围外记录与全局 ERROR 必须阻断整个范围。"""
        frame = pl.DataFrame({"unit_key": ["outside"], "value": [3]}, schema=self.dataset.schema)
        self.assertEqual(self.run_frame(frame).status, "FAILED")
        self.assertEqual(validation.blocked_units(("a", "b"), (Check("ERROR", "global", "validation"),)), {"a", "b"})

    def test_dependency_missing_pin_audit_and_changed_version(self):
        """批量依赖核对、审计以及提交前版本漂移保护。"""
        ref = DependencyRef("upstream", "T")
        with self.assertRaisesRegex(ValueError, "not published"):
            deps.check(self.con, (ref,))
        self.con.execute("INSERT INTO meta.coverage VALUES ('upstream','T','PUBLISHED',1,current_timestamp,NULL)")
        pins = deps.check(self.con, (ref,))
        deps.recheck(self.con, pins)
        rid, _ = audit.begin(self.con, self.dataset.name, {}, self.root)
        audit.scope(self.con, rid, self.dataset.name, self.dataset.scope, pins)
        self.assertEqual(self.con.execute("SELECT publication_id FROM meta.run_dependency").fetchone()[0], 1)
        self.con.execute("CREATE TEMP TABLE candidate AS SELECT * FROM canonical.fixture WHERE FALSE")
        self.con.execute("UPDATE meta.coverage SET publication_id=2 WHERE dataset='upstream'")
        before = self.con.execute("SELECT * FROM canonical.fixture").fetchall()
        with self.assertRaisesRegex(ValueError, "Dependency changed"):
            publisher.publish(self.con, self.dataset, "candidate", ("a",), pins, rid)
        self.assertEqual(before, self.con.execute("SELECT * FROM canonical.fixture").fetchall())

    def test_missing_dependency_blocks_runner_before_requests(self):
        """上游缺失时不会调用 Adapter。"""
        self.dataset.required = (DependencyRef("missing", "T"),)
        adapter = FixtureAdapter(self.frame)
        result = runner.run(self.con, self.dataset, adapter, {}, RunOptions(self.root))
        self.assertEqual(result.status, "FAILED")
        self.assertFalse(adapter.called)

    def test_scope_excluded_and_unknown_do_not_become_successful_coverage(self):
        """UNKNOWN 留当前解释，OUT_OF_SCOPE 仅留审计。"""
        self.dataset.scope = Scope((), (ScopeReason("a", "excluded"),), (ScopeReason("b", "unresolved"),))
        result = self.run_frame()
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(self.con.execute("SELECT unit_key,status FROM meta.coverage").fetchall(), [("b", "UNKNOWN")])

    def test_scope_and_serialization_contracts(self):
        """范围结构互斥，DataFrame 不被通用 JSON 序列化。"""
        with self.assertRaises(ValueError):
            Scope(("a", "a"))
        with self.assertRaises(ValueError):
            Scope(("a",), unknown=(ScopeReason("a", "unknown"),))
        with self.assertRaises(TypeError):
            control_data(self.frame)
        with self.assertRaises(ValueError):
            Check("ERROR", "ambiguous", "validation", ())

    def test_explicit_empty_schema(self):
        """空关系仍需匹配 Canonical Schema，不能由零行推断列类型。"""
        self.assertEqual(self.run_frame(pl.DataFrame({"wrong": []})).status, "FAILED")
        self.assertEqual(self.run_frame().status, "SUCCESS")


class ClientTest(unittest.TestCase):
    """真实协议函数的重试预算与脱敏验证。"""

    def response(self, body):
        """使用标准 HTTP 上下文形态返回测试信封。"""
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.read.return_value = json.dumps(body).encode()
        return response

    def query(self, records):
        """回调分离控制记录和原始证据，验证两个部分都脱敏。"""
        return client.TushareClient("secret").query("endpoint", {}, ("x",), request_id="r1",
                                                   on_attempt=lambda r, p: records.append((control_data(r), p)))

    def test_retry_and_preserve_delivery_markers(self):
        """重试后交付标记完整保留，每次尝试分别记录。"""
        body = {"code": 0, "data": {"fields": ["x"], "items": [], "has_more": False, "count": 0}}
        records = []
        with patch.object(client, "urlopen", side_effect=[TimeoutError("timeout"), self.response(body)]) as opening, patch.object(client.time, "sleep"):
            self.assertEqual(self.query(records), body)
        self.assertEqual(opening.call_count, 2)
        self.assertEqual(len(records), 2)
        self.assertNotIn("secret", json.dumps(records))

    def test_permission_and_malformed_envelopes_are_not_empty_success(self):
        """权限、坏信封和错误标记立即失败，不当作零行成功。"""
        bad = [{"code": 2002, "msg": "permission secret", "data": None}, {"code": 0, "data": None},
               {"code": 0, "data": {"fields": ["x"], "items": [[1, 2]]}},
               {"code": 0, "data": {"fields": ["x"], "items": [], "has_more": "false"}}]
        for body in bad:
            records = []
            with self.subTest(body=body), patch.object(client, "urlopen", return_value=self.response(body)) as opening:
                with self.assertRaises(RuntimeError) as error:
                    self.query(records)
                self.assertNotIn("secret", str(error.exception))
                self.assertNotIn("secret", json.dumps(records))
                self.assertEqual(opening.call_count, 1)

    def test_http_retry_budget_and_auth_failure(self):
        """429/5xx 有限重试，权限或超预算等待不重试。"""
        for code, header, expected in [(429, "0", 3), (503, "0", 3), (401, None, 1), (429, "61", 1), (429, "nan", 1)]:
            records = []
            def failure(*args, **kwargs):
                """每次使用独立可读取 HTTP 错误响应。"""
                raise HTTPError(client.ENDPOINT, code, "error", {"Retry-After": header} if header else {}, io.BytesIO(b"error"))
            with self.subTest(code=code, header=header), patch.object(client, "urlopen", side_effect=failure) as opening, patch.object(client.time, "sleep"):
                with self.assertRaises(RuntimeError):
                    self.query(records)
                self.assertEqual(opening.call_count, expected)
                self.assertEqual(len(records), expected)

    def test_retry_evidence_files_are_distinct(self):
        """一个逻辑请求重试时，各次尝试都有独立原始证据。"""
        body = {"code": 0, "data": {"fields": ["x"], "items": [], "has_more": False}}
        with TemporaryDirectory() as directory:
            store = Artifacts(Path(directory) / "run", False)
            with patch.object(client, "urlopen", side_effect=[TimeoutError("timeout"), self.response(body)]), patch.object(client.time, "sleep"):
                client.TushareClient("secret").query("endpoint", {}, ("x",), request_id="r1", on_attempt=store.record_attempt)
            files = sorted((store.root / "requests").glob("*.json"))
            self.assertEqual([p.name for p in files], ["r1-attempt-01.json", "r1-attempt-02.json"])
            self.assertTrue(json.loads(files[0].read_text())["attempt"]["error"])
            self.assertEqual(json.loads(files[1].read_text())["response"], body)


class BatchScaleTest(unittest.TestCase):
    """SQL 调用数必须与 Unit 数量解耦；约 120 万条业务行不转 Python 对象。"""

    def exercise(self, n):
        """同一真实 Runner 验证 Unit 初始审计、空替换和批量发布。"""
        with TemporaryDirectory() as directory, duckdb.connect(":memory:") as con:
            dataset = FixtureDataset(tuple(f"u{i}" for i in range(n)), large=True)
            db.initialize(con, dataset)
            con.execute("INSERT INTO canonical.fixture SELECT 'u'||i::VARCHAR,j,1 FROM range(?) t(i) CROSS JOIN range(240) s(j)", [n])
            frame = con.execute("SELECT 'u'||i::VARCHAR AS unit_key,j::BIGINT AS seq,2::BIGINT AS value FROM range(?) t(i) CROSS JOIN range(240) s(j) WHERE i<>0", [n]).pl()
            proxy = Mock(wraps=con)
            started = time.monotonic()
            result = runner.run(proxy, dataset, FixtureAdapter(frame), {}, RunOptions(Path(directory)))
            elapsed = time.monotonic() - started
            self.assertEqual(result.status, "SUCCESS", result)
            self.assertEqual(len(result.publications), n)
            self.assertEqual(con.execute("SELECT row_count FROM meta.publication WHERE unit_key='u0'").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT count(*) FROM canonical.fixture WHERE unit_key='u0'").fetchone()[0], 0)
            self.assertEqual(con.execute("SELECT count(*) FROM canonical.fixture").fetchone()[0], (n - 1) * 240)
            self.assertLessEqual(proxy.execute.call_count, 60)
            print(f"\nBATCH units={n} rows={frame.height} sql={proxy.execute.call_count} seconds={elapsed:.3f}", flush=True)
            return proxy.execute.call_count

    def test_5000_units_constant_sql_calls(self):
        """规模扩大时保持固定 SQL 调用数，耗时只记录而不设机器相关阈值。"""
        small = self.exercise(100)
        large = self.exercise(5000)
        self.assertLessEqual(abs(large - small), 5)


if __name__ == "__main__":
    unittest.main()
