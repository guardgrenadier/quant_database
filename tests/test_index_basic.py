"""指数当前快照正式链路：分页证据、完整替换与旧成功版本保护。"""

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timedelta, timezone
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

from quant_data import db
from quant_data.__main__ import main
from quant_data.core import runner
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import RequestAttempt, RunOptions, Scope
from quant_data.datasets.index_basic import IndexBasicDataset
from quant_data.sources.tushare.index_basic import TushareIndexBasicAdapter

FIELDS = tuple(IndexBasicDataset.schema.names())
BASELINE = Path("data/probes/st-index-20261020-main/requests")


def index_row(**changes):
    """保留完整 13 字段，便于在通信边界注入真实形态的异常。"""
    row = dict(ts_code="000300.SH", name=" 沪深300 ", fullname="沪深300指数", market="CSI",
               publisher="中证指数有限公司", index_type=None, category="规模指数", base_date="20041231",
               base_point=1000.0, list_date="20050408", weight_rule="调整自由流通市值加权",
               desc="供应商描述", exp_date=None)
    return {**row, **changes}


class ProviderClient:
    """只替换通信；正式 Adapter、统一验收、发布、审计和证据均实际执行。"""

    def __init__(self, rows=(), transform=None):
        self.rows, self.transform, self.calls = rows, transform, []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        self.calls.append((api_name, dict(params), tuple(fields)))
        # 交替列序并逆序交付每页，防止实现偶然依赖供应商行列排序。
        returned = list(reversed(fields)) if len(self.calls) % 2 else list(fields)
        offset, limit = params["offset"], params["limit"]
        rows = self.rows[offset:offset + limit]
        data = {"fields": returned, "items": [[r[f] for f in returned] for r in reversed(rows)],
                "has_more": len(rows) == limit, "count": 0}
        if self.transform:
            self.transform(params, data)
        body = {"code": 0, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api_name, params, tuple(fields), datetime.now(timezone.utc),
                                  0.01, 200, 0, tuple(data["fields"]), len(data["items"]), data.get("has_more")), body)
        return body


class IndexBasicTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.dataset = IndexBasicDataset()
        db.initialize(self.con, self.dataset)
        # 小页只加速边界测试；真实重放单独恢复正式页大小，不改变生产参数。
        for name, value in (("REQUEST_INTERVAL", 0), ("PAGE_SIZE", 2)):
            setting = patch(f"quant_data.sources.tushare.index_basic.{name}", value)
            setting.start()
            self.addCleanup(setting.stop)

    def ingest(self, rows=(), transform=None, keep=False, params=None, con=None):
        self.client = ProviderClient(rows, transform)
        return runner.run(con or self.con, self.dataset, TushareIndexBasicAdapter(self.client),
                          {} if params is None else params, RunOptions(self.root / "landing", keep))

    def current(self):
        """失败审计允许增长，当前事实和成功版本不得被失败替换。"""
        return tuple(self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.index_basic ORDER BY ts_code",
            "SELECT * FROM meta.coverage ORDER BY dataset,unit_key",
            "SELECT * FROM meta.publication ORDER BY publication_id"))

    def failed_preserving(self, action):
        before = self.current()
        result = action()
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)
        self.assertTrue(Path(result.artifact_dir).is_dir())
        return result

    def rules(self, result, level="ERROR"):
        return self.con.execute(
            "SELECT rule,stage FROM meta.validation_check WHERE run_id=? AND level=? ORDER BY rule",
            [result.run_id, level]).fetchall()

    def test_pages_replacement_rerun_and_metadata(self):
        """全量分页、开放市场、终止记录、无依赖和整表删除消失代码。"""
        rows = [index_row(), index_row(ts_code="A.NH", market="NH", index_type="期货指数"),
                index_row(ts_code="new", market="NEW_MARKET", category="新类别", exp_date="20200101")]
        first = self.ingest(rows)
        self.assertEqual(first.status, "SUCCESS", first)
        self.assertEqual(first.publications[0].row_count, 3)
        self.assertEqual([c[1] for c in self.client.calls],
                         [{"limit": 2, "offset": n} for n in (0, 2, 4)])
        self.assertTrue(all(api == "index_basic" and fields == FIELDS for api, _, fields in self.client.calls))
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.run_dependency").fetchone()[0], 0)
        second = self.ingest(rows)
        self.assertGreater(second.publications[0].publication_id, first.publications[0].publication_id)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_basic").fetchone()[0], 3)
        third = self.ingest([index_row(name="changed")])
        self.assertEqual(self.con.execute("SELECT ts_code,name FROM canonical.index_basic").fetchall(), [("000300.SH", "changed")])
        self.assertEqual(self.con.execute("SELECT unit_key,status,publication_id FROM meta.coverage").fetchone(),
                         ("SNAPSHOT", "PUBLISHED", third.publications[0].publication_id))

    def test_empty_complete_first_failure_and_old_version_protection(self):
        """空响应可 COMPLETE，但有 Schema 的空 Candidate 由非空业务规则阻断。"""
        empty = self.ingest(keep=True)
        self.assertEqual(empty.status, "FAILED", empty)
        self.assertEqual(self.rules(empty), [("index.nonempty", "validation")])
        self.assertEqual(pl.read_parquet(Path(empty.artifact_dir) / "candidate.parquet").schema, self.dataset.schema)
        self.assertEqual(self.con.execute("SELECT status,publication_id FROM meta.coverage").fetchone(), ("FAILED", None))
        self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit").fetchone()[0], "COMPLETE")
        self.assertEqual(len(self.client.calls), 2)
        self.ingest([index_row()])
        self.failed_preserving(lambda: self.ingest())

    def test_business_errors_preserve_snapshot(self):
        """必填属性、重复键和非有限数均阻断，不借新增键字段或去重放行。"""
        self.ingest([index_row()])
        cases = [([index_row(**{f: " "})], "index.required")
                 for f in ("ts_code", "name", "market", "publisher", "category")]
        cases += [([index_row(), index_row(name="conflict")], "index.key_unique")]
        cases += [([index_row(base_point=v)], "index.base_point_finite") for v in (float("nan"), float("inf"), -float("inf"))]
        for rows, rule in cases:
            with self.subTest(rule=rule, rows=rows):
                result = self.failed_preserving(lambda: self.ingest(rows))
                self.assertIn((rule, "validation"), self.rules(result))

    def test_raw_types_and_dates_are_preparation_errors_with_complete_delivery(self):
        """坏类型/日期留原信封，已完成请求不改记 FAILED，也不能发布部分可转换页。"""
        self.ingest([index_row()])
        cases = [(index_row(ts_code=123), "source.types"),
                 (index_row(base_point=True), "source.types"),
                 (index_row(base_point="1000"), "source.types")]
        cases += [(index_row(**{f: value}), rule) for f in ("base_date", "list_date", "exp_date")
                  for value, rule in (("20260230", "source.date_parse"), ("2026101", "source.date_format"),
                                      ("２０２６１０１６", "source.date_format"))]
        for row, rule in cases:
            with self.subTest(row=row):
                result = self.failed_preserving(lambda: self.ingest([row], keep=True))
                self.assertEqual(self.rules(result), [(rule, "prepare")])
                state = self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchone()[0]
                self.assertEqual(state, "COMPLETE")
                self.assertEqual(len(self.client.calls), 2)
                body = json.loads(next((Path(result.artifact_dir) / "requests").glob("*p000*")).read_text())["response"]["data"]
                self.assertEqual(dict(zip(body["fields"], body["items"][0])), row)

    def test_missing_column_on_data_or_verification_page_blocks_snapshot(self):
        """缺列不是合法 NULL，连空验证页也必须交付全部字段。"""
        self.ingest([index_row()])
        for target_offset in (0, 2):
            def missing(params, data):
                if params["offset"] == target_offset:
                    i = data["fields"].index("exp_date")
                    data["fields"].pop(i)
                    data["items"] = [r[:i] + r[i + 1:] for r in data["items"]]
            result = self.failed_preserving(lambda: self.ingest([index_row()], missing))
            self.assertEqual(self.rules(result), [("source.fields", "prepare")])
            self.assertEqual(len(self.client.calls), 2)

    def test_pagination_markers_boundaries_request_failure_and_budget(self):
        """模糊或矛盾的终止、重复满页和请求异常均不能修改旧成功范围。"""
        self.ingest([index_row()])
        def missing_marker(params, data):
            data.pop("has_more")
        def short_true(params, data):
            data["has_more"] = True
        def full_false(params, data):
            if params["offset"] == 0:
                data["items"] *= 2
                data["has_more"] = False
        def oversized(params, data):
            data["items"] *= 3
        def nonempty_verifier(params, data):
            if params["offset"]:
                data["items"] = [[index_row()[f] for f in data["fields"]]]
        def failed_verifier(params, data):
            if params["offset"]:
                raise RuntimeError("injected verification request failure")
        for change in (missing_marker, short_true, full_false, oversized, nonempty_verifier, failed_verifier):
            with self.subTest(change=change.__name__):
                result = self.failed_preserving(lambda: self.ingest([index_row()], change))
                rule = "source.request" if change is failed_verifier else "source.pagination"
                self.assertEqual(self.rules(result), [(rule, "fetch")])
        def repeated(params, data):
            data["items"] = [[index_row()[f] for f in data["fields"]]] * 2
            data["has_more"] = True
        with patch("quant_data.sources.tushare.index_basic.MAX_PAGES", 3):
            result = self.failed_preserving(lambda: self.ingest([index_row()], repeated))
            self.assertEqual(len(self.client.calls), 3)
            self.assertEqual(self.rules(result), [("source.page_budget", "fetch")])
        # 最后一页恰为候选终止页、没有剩余验证预算时，也必须判不完整。
        with patch("quant_data.sources.tushare.index_basic.MAX_PAGES", 1):
            self.assertEqual(self.rules(self.failed_preserving(lambda: self.ingest([index_row()]))),
                             [("source.page_budget", "fetch")])

    def test_nulls_zero_date_order_and_warning_only_values(self):
        """允许稀疏属性、零基点及基期晚于发布日期；异常告警保持源值。"""
        tomorrow = (datetime.now(ZoneInfo("Asia/Shanghai")).date() + timedelta(days=1)).strftime("%Y%m%d")
        rows = [index_row(ts_code="zero", base_point=0, base_date="20120427"),
                index_row(ts_code="null", fullname=" ", base_date=None, base_point=None,
                          list_date="", weight_rule=None, desc=None),
                index_row(ts_code="warning", base_point=-1.5, base_date=tomorrow, exp_date="19990101")]
        result = self.ingest(rows)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.rules(result), [])
        self.assertEqual({r[0] for r in self.rules(result, "WARNING")},
                         {"index.negative_base_point", "index.future_date", "index.date_order", "index.optional_nulls"})
        self.assertEqual(self.con.execute("SELECT base_point,base_date,list_date FROM canonical.index_basic WHERE ts_code='zero'").fetchone(),
                         (0, date(2012, 4, 27), date(2005, 4, 8)))
        self.assertEqual(self.con.execute("SELECT base_point FROM canonical.index_basic WHERE ts_code='warning'").fetchone()[0], -1.5)
        optional = json.loads(self.con.execute("SELECT detail FROM meta.validation_check WHERE run_id=? AND rule='index.optional_nulls'", [result.run_id]).fetchone()[0])
        self.assertNotIn("index_type", optional)
        self.assertNotIn("exp_date", optional)

    def test_retained_raw_candidate_manifest_and_evidence_failure(self):
        """标准化不改 Raw，证据完整；原始证据写失败在发布前保护旧版。"""
        result = self.ingest([index_row()], keep=True)
        self.assertTrue(result.artifacts_complete, result)
        root = Path(result.artifact_dir)
        raw, candidate = pl.read_parquet(root / "landing.parquet"), pl.read_parquet(root / "candidate.parquet")
        self.assertEqual(raw["name"].to_list(), [" 沪深300 "])
        self.assertEqual(raw["_requested_offset"].to_list(), [0])
        self.assertEqual(candidate.schema, self.dataset.schema)
        self.assertEqual(candidate["name"].to_list(), ["沪深300"])
        for item in json.loads((root / "manifest.json").read_text())["files"]:
            self.assertEqual(hashlib.sha256((root / item["path"]).read_bytes()).hexdigest(), item["sha256"])
        with patch.object(Artifacts, "record_attempt", side_effect=OSError("injected evidence failure")):
            self.failed_preserving(lambda: self.ingest([index_row(name="changed")]))

    def test_publication_rollback_and_ddl_constraints(self):
        """发布删除之后故障仍保护旧版；数据库最终防线落实键、必填及有限数。"""
        self.ingest([index_row()])
        proxy = Mock(wraps=self.con)
        def execute(sql, *args):
            if " ".join(sql.split()).startswith('INSERT INTO "canonical"."index_basic"'):
                raise RuntimeError("injected publication failure")
            return self.con.execute(sql, *args)
        proxy.execute.side_effect = execute
        self.failed_preserving(lambda: self.ingest([index_row(name="changed")], con=proxy))
        for values in (("new", " ", "M", "P", "C", 1), ("new", "N", "M", "P", "C", float("inf")),
                       ("000300.SH", "N", "M", "P", "C", 1)):
            with self.assertRaises(duckdb.ConstraintException):
                self.con.execute("INSERT INTO canonical.index_basic(ts_code,name,market,publisher,category,base_point) VALUES (?,?,?,?,?,?)", values)

    def test_subset_parameters_scope_and_cli(self):
        """Dataset 和 CLI 均拒绝子集；实际装配无日历依赖，成功/失败退出状态明确。"""
        for params in ({"market": "SSE"}, {"ts_code": "000300.SH"}, {"start_date": "20261019"}):
            result = self.ingest([index_row()], params=params)
            self.assertEqual(result.status, "FAILED")
            self.assertEqual(self.client.calls, [])
        with self.assertRaises(ValueError):
            TushareIndexBasicAdapter(ProviderClient()).fetch(Scope(("SSE",)), Artifacts(self.root / "bad-scope", False))
        argv = ["quant_data", "ingest", "index_basic", "--db", str(self.path), "--landing-dir", str(self.root / "cli")]
        for rows, status, code in (([index_row()], "SUCCESS", 0), ([], "FAILED", 1)):
            output = io.StringIO()
            with patch("sys.argv", argv), patch("quant_data.__main__.dotenv_values", return_value={"TUSHARE_TOKEN": "test-secret"}), \
                    patch("quant_data.__main__.TushareClient", return_value=ProviderClient(rows)), redirect_stdout(output):
                self.assertEqual(main(), code)
            self.assertEqual(json.loads(output.getvalue())["status"], status)
        for flag in ("--start-date", "--end-date"):
            with patch("sys.argv", argv + [flag, "20261019"]), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main()
                self.assertEqual(error.exception.code, 2)

    @unittest.skipUnless((BASELINE / "ib_pages-p013-attempt-01.json").exists(), "本地真实响应不随 Git 分发")
    def test_saved_full_snapshot_replay(self):
        """重放全部真实页并独立标准化对照，验证 0 基点及真实日期顺序不被丢弃。"""
        bodies, expected_rows = {}, []
        for page in range(14):
            evidence = json.loads((BASELINE / f"ib_pages-p{page:03d}-attempt-01.json").read_text())
            data = evidence["response"]["data"]
            bodies[evidence["attempt"]["params"]["offset"]] = evidence["response"]
            for row in data["items"]:
                # 测试独立依据契约解释源值，不调用正式 prepare 生成期望数据。
                record = {f: (v.strip() or None) if isinstance(v, str) else v for f, v in zip(data["fields"], row)}
                for field in ("base_date", "list_date", "exp_date"):
                    if record[field] is not None:
                        record[field] = datetime.strptime(record[field], "%Y%m%d").date()
                expected_rows.append(record)
        class ReplayClient:
            """只存在测试边界的已保存官方响应重放。"""
            def query(self, api_name, params, fields, *, request_id, on_attempt):
                body = bodies[params["offset"]]
                data = body["data"]
                on_attempt(RequestAttempt(request_id, 1, api_name, params, fields, datetime.now(timezone.utc),
                                          0.0, 200, 0, tuple(data["fields"]), len(data["items"]), data["has_more"]), body)
                return body
        with patch("quant_data.sources.tushare.index_basic.PAGE_SIZE", 1000):
            result = runner.run(self.con, self.dataset, TushareIndexBasicAdapter(ReplayClient()), {}, RunOptions(self.root / "replay"))
        self.assertEqual(result.status, "SUCCESS", result)
        actual = self.con.execute("SELECT * FROM canonical.index_basic ORDER BY ts_code").pl()
        expected = pl.DataFrame(expected_rows, schema=self.dataset.schema).sort("ts_code")
        self.assertTrue(actual.equals(expected))
        self.assertEqual(actual.height, 12599)
