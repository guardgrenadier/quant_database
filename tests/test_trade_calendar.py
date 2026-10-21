"""SSE 日历正式链路测试；仅在供应商通信边界使用替身。"""

from copy import deepcopy
from datetime import date, datetime, timezone
from contextlib import redirect_stderr, redirect_stdout
import hashlib
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.__main__ import main
from quant_data.core import deps, runner
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import DependencyRef, RequestAttempt, RunOptions
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter


def calendar_rows():
    """明确构造跨年、开市与连续休市记录，不在生产代码推算日历。"""
    return [dict(exchange="SSE", cal_date=day, is_open=opened, pretrade_date=previous)
            for day, opened, previous in (
                ("20251231", 1, "20251230"),
                ("20260101", 0, "20251231"),
                ("20260102", 0, "20251231"),
                ("20260103", 0, "20251231"),
                ("20260104", 0, "20251231"),
                ("20260105", 1, "20251231"),
                ("20260106", 1, "20260105"),
            )]


class CalendarClient:
    """只替换 HTTP 交付，保留真实 Adapter、证据、验收和事务发布。"""

    def __init__(self, rows, transform=None):
        """保留请求便于核对按年分片、不传开市过滤等获取契约。"""
        self.rows, self.transform = rows, transform
        self.calls = []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        """返回倒序、列重排的信封，避免实现依赖供应商顺序。"""
        self.calls.append((api_name, dict(params)))
        fields = tuple(reversed(fields))
        selected = [r for r in self.rows if params["start_date"] <= r["cal_date"] <= params["end_date"]]
        data = {"fields": list(fields), "items": [[r[f] for f in fields] for r in reversed(selected)],
                "has_more": False, "count": 0}
        if self.transform:
            self.transform(params, data)
        body = {"code": 0, "msg": None, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api_name, params, fields, datetime.now(timezone.utc),
                                  0.01, 200, 0, fields, len(data["items"]), data.get("has_more")), body)
        return body


class TradeCalendarTest(unittest.TestCase):
    """按日修订与错误影响必须保持已确认的日历契约。"""

    def setUp(self):
        """隔离磁盘库、证据目录与请求等待，并共存已接入的股票表。"""
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        db.initialize(self.con, StockBasicDataset())
        self.dataset = TradeCalendarDataset()
        db.initialize(self.con, self.dataset)
        self.rows = calendar_rows()
        self.params = {"start_date": "20251231", "end_date": "20260106"}
        interval = patch("quant_data.sources.tushare.trade_calendar.REQUEST_INTERVAL", 0)
        interval.start()
        self.addCleanup(interval.stop)

    def ingest(self, rows=None, transform=None, params=None, keep=False, con=None):
        """使用正式 Runner；测试仅注入供应商响应或发布故障。"""
        self.client = CalendarClient(self.rows if rows is None else rows, transform)
        return runner.run(con or self.con, self.dataset, TushareTradeCalendarAdapter(self.client),
                          self.params if params is None else params, RunOptions(self.root / "landing", keep))

    def facts(self):
        """读取当前事实用于修订前后核对。"""
        return self.con.execute("SELECT * FROM canonical.trade_calendar ORDER BY cal_date").fetchall()

    def current(self):
        """权威状态与成功版本应在发布故障后整体保持。"""
        return tuple(self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.trade_calendar ORDER BY cal_date",
            "SELECT * FROM meta.coverage ORDER BY unit_key",
            "SELECT * FROM meta.publication ORDER BY publication_id"))

    def test_success_types_requests_and_artifacts(self):
        """跨年切请求、按日发布，休市为显式假值，原始整数与日期转换可追溯。"""
        result = self.ingest(keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 7)
        self.assertTrue(all(p.row_count == 1 for p in result.publications))
        self.assertEqual(self.client.calls, [
            ("trade_cal", {"exchange": "SSE", "start_date": "20251231", "end_date": "20251231"}),
            ("trade_cal", {"exchange": "SSE", "start_date": "20260101", "end_date": "20260106"})])
        self.assertEqual(self.facts()[1], ("SSE", date(2026, 1, 1), False, date(2025, 12, 31)))
        ref = DependencyRef("trade_calendar", "SSE:2026-01-01")
        self.assertEqual(deps.check(self.con, (ref,))[0].ref, ref)
        root = Path(result.artifact_dir)
        raw = pl.read_parquet(root / "landing.parquet")
        self.assertEqual(raw.schema["is_open"], pl.Int64)
        self.assertEqual(pl.read_parquet(root / "candidate.parquet").schema, self.dataset.schema)
        self.assertTrue(result.artifacts_complete, result)
        self.assertNotIn("stock_basic", (root / "README.md").read_text())
        manifest = json.loads((root / "manifest.json").read_text())
        for entry in manifest["files"]:
            self.assertEqual(hashlib.sha256((root / entry["path"]).read_bytes()).hexdigest(), entry["sha256"])
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_basic").fetchone()[0], 0)

    def test_subset_replacement_preserves_other_dates(self):
        """只替换给定日期，重复执行产生新版本，不删范围外日历。"""
        self.ingest()
        before = self.facts()
        rows = deepcopy(self.rows)
        rows[1]["is_open"] = 1
        result = self.ingest(rows, params={"start_date": "20260101", "end_date": "20260102"})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 2)
        after = self.facts()
        self.assertTrue(after[1][2])
        self.assertEqual(before[:1] + before[3:], after[:1] + after[3:])
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.publication").fetchone()[0], 9)

    def test_missing_and_empty_delivery_preserve_previous_day(self):
        """缺日不反推休市；空交付不能清空已有范围或取得成功 Coverage。"""
        self.ingest()
        old = self.facts()[2]
        old_pin = deps.check(self.con, (DependencyRef("trade_calendar", "SSE:2026-01-02"),))
        rows = [r for r in self.rows if r["cal_date"] != "20260102"]
        result = self.ingest(rows)
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(len(result.publications), 6)
        self.assertEqual(self.facts()[2], old)
        self.assertEqual(deps.check(self.con, (old_pin[0].ref,)), old_pin)
        before = self.current()
        result = self.ingest([])
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)
        self.assertTrue(Path(result.artifact_dir).exists())

    def test_first_empty_does_not_publish(self):
        """首次空响应明确失败，不制造成功日历或默认 false 行。"""
        result = self.ingest([])
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(self.facts(), [])
        self.assertEqual(self.con.execute("SELECT DISTINCT status,publication_id FROM meta.coverage").fetchall(), [("FAILED", None)])
        self.assertEqual(result.publications, ())
        # 获取已完成但业务缺日；仅由 Dataset 的一日一行规则产生一次验收诊断。
        self.assertEqual(self.con.execute("SELECT DISTINCT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchall(), [("COMPLETE",)])
        self.assertEqual(self.con.execute("SELECT rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall(), [("calendar.one_row",)])

    def test_preparation_keeps_legal_null_distinct_from_conversion_error(self):
        """可空前交易日和非法日期不能经 NULL 混同，Raw 保留原值，错误日保护旧版本。"""
        self.ingest()
        before = self.current()
        rows = deepcopy(self.rows)
        rows[2]["pretrade_date"] = "20260230"
        params = {"start_date": "20260102", "end_date": "20260102"}
        bad = self.ingest(rows, params=params, keep=True)
        self.assertEqual(bad.status, "FAILED", bad)
        self.assertEqual(self.current(), before)
        self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [bad.run_id]).fetchone()[0], "COMPLETE")
        self.assertEqual(self.con.execute("SELECT rule,stage FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [bad.run_id]).fetchall(), [("source.pretrade_date", "prepare")])
        folder = Path(bad.artifact_dir)
        self.assertEqual(pl.read_parquet(folder / "landing.parquet")["pretrade_date"].to_list(), ["20260230"])
        self.assertEqual(pl.read_parquet(folder / "candidate.parquet").height, 0)
        self.assertTrue((folder / "preparation.json").exists())
        self.assertFalse((folder / "source_checks.json").exists())
        rows[2]["pretrade_date"] = None
        good = self.ingest(rows, params=params)
        self.assertEqual(good.status, "SUCCESS", good)
        self.assertEqual(good.publications[0].row_count, 1)
        self.assertIsNone(self.con.execute("SELECT pretrade_date FROM canonical.trade_calendar WHERE cal_date='2026-01-02'").fetchone()[0])

    def test_per_day_invalid_values_and_duplicates(self):
        """定位到单日的错误只阻断该日，既有数据和版本保持可用。"""
        self.ingest()
        params = {"start_date": "20260101", "end_date": "20260103"}
        ref = DependencyRef("trade_calendar", "SSE:2026-01-02")
        old = self.facts()[2]
        cases = [("is_open", 2), ("is_open", -1), ("is_open", None),
                 ("pretrade_date", "20260230"), ("pretrade_date", "2026111"),
                 ("pretrade_date", "２０２６０１０１"), ("pretrade_date", "20260102"),
                 ("pretrade_date", "20260103"), ("duplicate", None)]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                before_pin = deps.check(self.con, (ref,))
                rows = deepcopy(self.rows)
                if field == "duplicate":
                    rows.append(dict(rows[2]))
                else:
                    rows[2][field] = value
                result = self.ingest(rows, params=params)
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual({p.unit_key for p in result.publications}, {"SSE:2026-01-01", "SSE:2026-01-03"})
                self.assertEqual(self.facts()[2], old)
                self.assertEqual(deps.check(self.con, (ref,)), before_pin)

    def test_failed_request_and_bad_identity_block_only_request_year(self):
        """字段、协议、类型或无法归属的日期错误阻断请求片段，不污染其他年度。"""
        self.ingest()
        def transform_case(field, value):
            """在 2026 请求里制造错误，2025 请求仍正常完成。"""
            def transform(params, data):
                """只改供应商响应，不绕过正式数据处理和事务。"""
                if params["start_date"][:4] != "2026":
                    return
                if field == "failure":
                    raise RuntimeError("injected request failure")
                if field == "missing_field":
                    idx = data["fields"].index("pretrade_date")
                    data["fields"].pop(idx)
                    data["items"] = [r[:idx] + r[idx + 1:] for r in data["items"]]
                elif field == "has_more":
                    if value is None:
                        del data["has_more"]
                    else:
                        data["has_more"] = value
                else:
                    data["items"][0][data["fields"].index(field)] = value
            return transform
        cases = [("failure", None), ("missing_field", None), ("has_more", True), ("has_more", None),
                 ("exchange", "SZSE"), ("exchange", None), ("cal_date", "2026111"),
                 ("cal_date", "20260230"), ("cal_date", "20260107"), ("cal_date", None),
                 ("cal_date", 20260106), ("pretrade_date", 20260105),
                 ("is_open", "1"), ("is_open", True), ("is_open", 1.5)]
        old = self.facts()
        for field, value in cases:
            with self.subTest(field=field, value=value):
                result = self.ingest(transform=transform_case(field, value))
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual([p.unit_key for p in result.publications], ["SSE:2025-12-31"])
                self.assertEqual(self.facts(), old)

    def test_scope_dates_and_nullable_pretrade(self):
        """闰日、历史首日空前交易日合法；错误输入不发请求。"""
        scope = self.dataset.resolve_scope({"start_date": "20240228", "end_date": "20240301"}, (), self.con)
        self.assertEqual(scope.units, ("SSE:2024-02-28", "SSE:2024-02-29", "SSE:2024-03-01"))
        cases = [{}, {"start_date": "20260230", "end_date": "20260301"},
                 {"start_date": "2026111", "end_date": "20261201"},
                 {"start_date": "２０２６０１０１", "end_date": "20260103"},
                 {"start_date": "20260103", "end_date": "20260101"},
                 {"start_date": None, "end_date": "20260103"},
                 {**self.params, "exchange": "SZSE"}, {**self.params, "is_open": "1"}]
        for params in cases:
            with self.subTest(params=params):
                result = self.ingest(params=params)
                self.assertEqual(result.status, "FAILED", result)
                self.assertEqual(self.client.calls, [])
        rows = [dict(exchange="SSE", cal_date="19901219", is_open=1, pretrade_date=None)]
        result = self.ingest(rows, params={"start_date": "19901219", "end_date": "19901219"})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.facts(), [("SSE", date(1990, 12, 19), True, None)])
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.validation_check WHERE rule='calendar.pretrade_missing'").fetchone()[0], 1)

    def test_publication_rollback_and_evidence_failure(self):
        """删除后、成功审计前和原始证据故障均不能改变旧成功数据。"""
        self.ingest()
        before = self.current()
        for prefix in ('INSERT INTO "canonical"."trade_calendar"', "UPDATE meta.run_unit AS u SET status = 'PUBLISHED'"):
            proxy = Mock(wraps=self.con)
            def execute(sql, *args):
                """在指定发布语句注入故障，其余操作使用真实连接。"""
                if " ".join(sql.split()).startswith(prefix):
                    raise RuntimeError("injected publication failure")
                return self.con.execute(sql, *args)
            proxy.execute.side_effect = execute
            result = self.ingest(con=proxy)
            self.assertEqual(result.status, "FAILED", result)
            self.assertEqual(self.current(), before)
        with patch.object(Artifacts, "record_attempt", side_effect=OSError("disk unavailable")):
            result = self.ingest()
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)
        self.con.close()
        with duckdb.connect(str(self.path), read_only=True) as reopened:
            self.assertEqual(reopened.execute("SELECT count(*) FROM canonical.trade_calendar").fetchone()[0], 7)

    def test_cli_calendar_and_stock_date_boundary(self):
        """公开 CLI 确实装配新 Dataset，且拒绝缺区间与股票日期过滤。"""
        argv = ["quant_data", "ingest", "trade_calendar", "--db", str(self.root / "cli.duckdb"),
                "--landing-dir", str(self.root / "cli-landing"), "--start-date", "20251231", "--end-date", "20260106"]
        output = io.StringIO()
        with patch("sys.argv", argv), patch("quant_data.__main__.dotenv_values", return_value={"TUSHARE_TOKEN": "test-secret"}), \
                patch("quant_data.__main__.TushareClient", return_value=CalendarClient(self.rows)), redirect_stdout(output):
            self.assertEqual(main(), 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS")
        for args in (["ingest", "trade_calendar"], ["ingest", "stock_basic", "--start-date", "20260101"]):
            with self.subTest(args=args), patch("sys.argv", ["quant_data", *args]), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main()
                self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
