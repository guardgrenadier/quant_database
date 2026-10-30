"""停复牌正式链路：稀疏日、SSE 依赖快照、范围隔离及旧成功数据保护。"""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import date, datetime, timezone
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
from quant_data.core import runner
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import RequestAttempt, RunOptions, Scope
from quant_data.datasets.suspend_daily import SuspendDailyDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.suspend_daily import TushareSuspendDailyAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter


def suspension_rows():
    """显式测试事实含同日 S/R、特殊证券及多段时段，不以当前股票集合补数据。"""
    return [dict(ts_code=code, trade_date=day, suspend_type=kind, suspend_timing=timing)
            for code, day, kind, timing in (
                ("000001.SZ", "20200312", "S", " 09:30-10:00,11:25-13:05 "),
                ("000001.SZ", "20200312", "R", " "),
                ("874396.BJ", "20200312", "S", "provider-unknown-format"),
                ("T600018.SH", "20200313", "S", None),
            )]


class ProviderClient:
    """只替换供应商通信；上游日历也经真实 Adapter、Runner 和 Publisher 发布。"""

    def __init__(self, rows=(), transform=None):
        """仅停复牌响应允许注入故障，日历使用明确的测试开闭市记录。"""
        self.rows, self.transform, self.calls = rows, transform, []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        """按请求生成列重排信封；正式代码须按字段名映射，不依赖返回顺序。"""
        self.calls.append((api_name, dict(params)))
        fields = tuple(reversed(fields))
        if api_name == "trade_cal":
            # 测试日历明确包含一个周末，不以停复牌空响应反推休市。
            rows = [dict(exchange="SSE", cal_date=day, is_open=opened, pretrade_date=previous)
                    for day, opened, previous in (
                        ("20200312", 1, "20200311"), ("20200313", 1, "20200312"),
                        ("20200314", 0, "20200313"), ("20200315", 0, "20200313"),
                        ("20200316", 1, "20200313"))
                    if params["start_date"] <= day <= params["end_date"]]
        else:
            rows = [r for r in self.rows if r["trade_date"] == params["trade_date"]]
        data = {"fields": list(fields), "items": [[r[f] for f in fields] for r in reversed(rows)],
                "has_more": False, "count": 0}
        if self.transform and api_name == "suspend_d":
            self.transform(params, data)
        body = {"code": 0, "msg": None, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api_name, params, fields, datetime.now(timezone.utc),
                                  0.01, 200, 0, fields, len(data["items"]), data.get("has_more")), body)
        return body


class SuspendDailyTest(unittest.TestCase):
    """跨正式 Dataset 的依赖、范围决策、零行修订及失败边界。"""

    def setUp(self):
        """隔离数据库和证据；先真实发布五个 SSE 日历 Unit，不建立 stock_basic 表。"""
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.params = {"start_date": "20200312", "end_date": "20200316"}
        self.rows = suspension_rows()
        self.dataset = SuspendDailyDataset()
        for name in ("suspend_daily", "trade_calendar"):
            interval = patch(f"quant_data.sources.tushare.{name}.REQUEST_INTERVAL", 0)
            interval.start()
            self.addCleanup(interval.stop)
        calendar = TradeCalendarDataset()
        db.initialize(self.con, calendar)
        result = runner.run(self.con, calendar, TushareTradeCalendarAdapter(ProviderClient()),
                            self.params, RunOptions(self.root / "landing"))
        self.assertEqual(result.status, "SUCCESS", result)
        db.initialize(self.con, self.dataset)

    def ingest(self, rows=None, transform=None, params=None, keep=False, con=None):
        """只替换 HTTP 边界，可在真实连接代理上注入发布故障。"""
        self.client = ProviderClient(self.rows if rows is None else rows, transform)
        return runner.run(con or self.con, self.dataset, TushareSuspendDailyAdapter(self.client),
                          self.params if params is None else params, RunOptions(self.root / "landing", keep))

    def current(self):
        """权威事实、Coverage 及成功变更一起比较，允许失败审计增加。"""
        return tuple(self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.suspend_daily ORDER BY trade_date,ts_code,suspend_type",
            "SELECT * FROM meta.coverage WHERE dataset='suspend_daily' ORDER BY unit_key",
            "SELECT * FROM meta.publication WHERE dataset='suspend_daily' ORDER BY publication_id"))

    def test_success_scope_pins_and_sparse_facts(self):
        """仅开市日请求全市场，S/R 同日合法，多段及未知 timing 保留，零行日也发布。"""
        result = self.ingest(keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual({p.unit_key: p.row_count for p in result.publications},
                         {"2020-03-12": 3, "2020-03-13": 1, "2020-03-16": 0})
        self.assertEqual(self.client.calls, [("suspend_d", {"trade_date": day}) for day in ("20200312", "20200313", "20200316")])
        states = {u.unit_key: u.status for u in result.units}
        self.assertEqual(states["2020-03-14"], "OUT_OF_SCOPE")
        self.assertEqual(states["2020-03-15"], "OUT_OF_SCOPE")
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.run_dependency WHERE run_id=?", [result.run_id]).fetchone()[0], 5)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.coverage WHERE dataset='suspend_daily'").fetchone()[0], 3)
        self.assertEqual(self.con.execute("SELECT suspend_timing FROM canonical.suspend_daily WHERE ts_code='000001.SZ' AND suspend_type='S'").fetchone()[0], "09:30-10:00,11:25-13:05")
        self.assertIsNone(self.con.execute("SELECT suspend_timing FROM canonical.suspend_daily WHERE suspend_type='R'").fetchone()[0])
        self.assertEqual(self.con.execute("SELECT suspend_timing FROM canonical.suspend_daily WHERE ts_code='874396.BJ'").fetchone()[0], "provider-unknown-format")
        self.assertEqual(self.con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='canonical' AND table_name='stock_basic'").fetchone()[0], 0)
        root = Path(result.artifact_dir)
        self.assertTrue(result.artifacts_complete, result)
        self.assertEqual(pl.read_parquet(root / "landing.parquet").schema["trade_date"], pl.String)
        self.assertEqual(pl.read_parquet(root / "candidate.parquet").schema, self.dataset.schema)

    def test_zero_row_replacement_and_rerun(self):
        """合法零行发布删除旧日集合，范围外事实保留，相同零行重跑生成新版本。"""
        self.ingest()
        params = {"start_date": "20200312", "end_date": "20200312"}
        result = self.ingest([], params=params)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(result.publications[0].row_count, 0)
        self.assertEqual(self.con.execute("SELECT ts_code,trade_date FROM canonical.suspend_daily").fetchall(), [("T600018.SH", date(2020, 3, 13))])
        again = self.ingest([], params=params, keep=True)
        self.assertEqual(again.status, "SUCCESS", again)
        self.assertTrue(again.artifacts_complete, again)
        self.assertGreater(again.publications[0].publication_id, result.publications[0].publication_id)
        self.assertEqual(pl.read_parquet(Path(again.artifact_dir) / "candidate.parquet").schema, self.dataset.schema)

    def test_all_conversion_failures_cannot_become_legal_empty(self):
        """同样的零行 Candidate，坏日期全部被隔离时阻断，完整空响应才能替换旧集合。"""
        self.ingest()
        before = self.current()
        params = {"start_date": "20200312", "end_date": "20200312"}
        def invalid_dates(params, data):
            """响应成功但每条日期非法，保留原始响应供诊断。"""
            for row in data["items"]:
                row[data["fields"].index("trade_date")] = "20200230"
        bad = self.ingest(transform=invalid_dates, params=params, keep=True)
        self.assertEqual(bad.status, "FAILED", bad)
        self.assertEqual(self.current(), before)
        self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [bad.run_id]).fetchone()[0], "COMPLETE")
        self.assertEqual(self.con.execute("SELECT rule,stage FROM meta.validation_check WHERE run_id=?", [bad.run_id]).fetchall(), [("source.range", "prepare")])
        bad_candidate = pl.read_parquet(Path(bad.artifact_dir) / "candidate.parquet")
        self.assertEqual(bad_candidate.height, 0)
        good = self.ingest([], params=params, keep=True)
        good_candidate = pl.read_parquet(Path(good.artifact_dir) / "candidate.parquet")
        self.assertTrue(bad_candidate.equals(good_candidate))
        self.assertEqual(good.status, "SUCCESS", good)
        self.assertEqual(good.publications[0].row_count, 0)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.suspend_daily WHERE trade_date='2020-03-12'").fetchone()[0], 0)

    def test_closed_only_scope_never_requests_or_publishes(self):
        """休市不是停复牌零行成功，全部排除时无请求、无 Coverage、无 Publication。"""
        result = self.ingest(params={"start_date": "20200314", "end_date": "20200315"})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(result.publications, ())
        self.assertTrue(all(u.status == "OUT_OF_SCOPE" for u in result.units))
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.coverage WHERE dataset='suspend_daily'").fetchone()[0], 0)

    def test_missing_calendar_dependency_blocks_before_requests(self):
        """即使缺的是待判休市日，未发布的日历也不能跳过或默认关闭。"""
        self.con.execute("DELETE FROM meta.coverage WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'")
        result = self.ingest()
        self.assertEqual(result.status, "FAILED", result)
        self.assertIn("Dependency not published", result.error)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.current(), ([], [], []))

    def test_missing_calendar_fact_is_unknown(self):
        """已固定成功版本却缺少实际事实时留 UNKNOWN，不把缺失当休市或正常零行。"""
        self.con.execute("DELETE FROM canonical.trade_calendar WHERE cal_date='2020-03-13'")
        result = self.ingest()
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual({u.unit_key: u.status for u in result.units}["2020-03-13"], "UNKNOWN")
        self.assertEqual(self.client.calls, [("suspend_d", {"trade_date": day}) for day in ("20200312", "20200316")])

    def test_failed_calendar_refresh_keeps_old_success_usable(self):
        """上游新运行失败仍可使用旧成功数据，不要求上游最近一次 Run 成功。"""
        calendar = TradeCalendarDataset()
        failed_client = Mock()
        failed_client.query.side_effect = RuntimeError("injected calendar failure")
        failed = runner.run(self.con, calendar, TushareTradeCalendarAdapter(failed_client),
                            self.params, RunOptions(self.root / "landing"))
        self.assertEqual(failed.status, "FAILED")
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)

    def test_source_errors_preserve_bad_day_and_allow_other_days(self):
        """错日期（含另一个目标日）、坏字段/类型/交付与触顶只阻断请求所属日。"""
        self.ingest()
        old = self.current()[0]
        def mutate(field, value):
            """只对 3 月 12 日返回坏交付，其余日期仍走正式发布。"""
            def transform(params, data):
                """保持协议行列对齐，在供应商边界注入明确故障。"""
                if params["trade_date"] != "20200312":
                    return
                if field == "failure":
                    raise RuntimeError("injected request failure")
                if field == "missing_field":
                    i = data["fields"].index("suspend_timing")
                    data["fields"].pop(i)
                    data["items"] = [r[:i] + r[i + 1:] for r in data["items"]]
                elif field == "has_more":
                    if value is None:
                        del data["has_more"]
                    else:
                        data["has_more"] = value
                elif field == "full":
                    data["items"] = [data["items"][0]] * 5000
                else:
                    data["items"][0][data["fields"].index(field)] = value
            return transform
        for field, value in (("failure", None), ("missing_field", None), ("has_more", True),
                             ("has_more", None), ("full", None), ("trade_date", "20200313"),
                             ("trade_date", "20200314"), ("trade_date", "2020312"),
                             ("trade_date", "20200230"), ("trade_date", None),
                             ("trade_date", 20200312), ("ts_code", 1), ("suspend_type", False)):
            with self.subTest(field=field, value=value):
                result = self.ingest(transform=mutate(field, value))
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual({p.unit_key for p in result.publications}, {"2020-03-13", "2020-03-16"})
                self.assertEqual(self.current()[0], old)

    def test_business_failures_and_duplicate_timing_preserve_day(self):
        """空键、未知类型及同键不同 timing 都阻断整日，不去重、不补枚举。"""
        self.ingest()
        old = self.current()[0]
        for field, value in (("ts_code", None), ("ts_code", " "), ("suspend_type", None),
                             ("suspend_type", "X"), ("suspend_type", " "), ("duplicate", None)):
            with self.subTest(field=field, value=value):
                rows = deepcopy(self.rows)
                if field == "duplicate":
                    rows.append({**rows[0], "suspend_timing": "different timing"})
                else:
                    rows[0][field] = value
                result = self.ingest(rows)
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual({p.unit_key for p in result.publications}, {"2020-03-13", "2020-03-16"})
                self.assertEqual(self.current()[0], old)

    def test_calendar_revision_after_scope_blocks_publication(self):
        """获取期间将已排除日修订为开市也需重算 Scope，不能继续发布旧范围。"""
        self.ingest()
        before = self.current()
        revised = False
        def change_calendar(params, data):
            """模拟已完成上游修订；不使用嵌套 Runner 破坏同连接临时关系。"""
            nonlocal revised
            if revised:
                return
            revised = True
            with db.transaction(self.con):
                pid = self.con.execute("INSERT INTO meta.publication(dataset,unit_key,row_count,run_id) VALUES ('trade_calendar','SSE:2020-03-14',1,'test-calendar-revision') RETURNING publication_id").fetchone()[0]
                self.con.execute("UPDATE canonical.trade_calendar SET is_open=true WHERE cal_date='2020-03-14'")
                self.con.execute("UPDATE meta.coverage SET publication_id=? WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'", [pid])
        result = self.ingest([], transform=change_calendar)
        self.assertEqual(result.status, "FAILED", result)
        self.assertIn("Dependency changed", result.error)
        self.assertEqual(self.current(), before)
        used = self.con.execute("SELECT publication_id FROM meta.run_dependency WHERE run_id=? AND upstream_unit='SSE:2020-03-14'", [result.run_id]).fetchone()[0]
        current = self.con.execute("SELECT publication_id FROM meta.coverage WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'").fetchone()[0]
        self.assertNotEqual(used, current)

    def test_transaction_and_evidence_failures_preserve_old_zero_target(self):
        """合法空修订在事务或证据阶段失败时，旧事件和成功版本也必须保留。"""
        self.ingest()
        before = self.current()
        for prefix in ('INSERT INTO "canonical"."suspend_daily"', "UPDATE meta.run_unit AS u SET status = 'PUBLISHED'"):
            proxy = Mock(wraps=self.con)
            def execute(sql, *args):
                """实际删除后或成功元数据阶段注入故障，其他 SQL 使用真实连接。"""
                if " ".join(sql.split()).startswith(prefix):
                    raise RuntimeError("injected publication failure")
                return self.con.execute(sql, *args)
            proxy.execute.side_effect = execute
            result = self.ingest([], con=proxy)
            self.assertEqual(result.status, "FAILED", result)
            self.assertEqual(self.current(), before)
        with patch.object(Artifacts, "record_attempt", side_effect=OSError("disk unavailable")):
            result = self.ingest([])
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)

    def test_parameters_and_cli(self):
        """输入只允许有效日期闭区间，公开入口须使用同库日历且输出真实发布结果。"""
        for params in ({}, {"start_date": "20200230", "end_date": "20200316"},
                       {"start_date": "2020312", "end_date": "20200316"},
                       {"start_date": "２０２００３１２", "end_date": "20200316"},
                       {"start_date": "20200316", "end_date": "20200312"},
                       {**self.params, "ts_code": "000001.SZ"}, {**self.params, "suspend_type": "S"}):
            with self.subTest(params=params):
                result = self.ingest(params=params)
                self.assertEqual(result.status, "FAILED", result)
                self.assertEqual(self.client.calls, [])
        # 供应商边界：历史起点之前不能因 API 可能返回零行而取得完整交付状态。
        client = ProviderClient()
        delivery = TushareSuspendDailyAdapter(client).fetch(
            Scope(("1999-12-30", "2020-03-16")), Artifacts(self.root / "history-evidence", False))
        self.assertEqual({u.unit_key: u.state for u in delivery.units},
                         {"1999-12-30": "INCOMPLETE", "2020-03-16": "COMPLETE"})
        self.assertEqual(client.calls, [("suspend_d", {"trade_date": "20200316"})])
        self.assertEqual(delivery.checks[0].rule, "source.history_start")
        argv = ["quant_data", "ingest", "suspend_daily", "--db", str(self.path),
                "--landing-dir", str(self.root / "cli-landing"), "--start-date", "20200312", "--end-date", "20200316"]
        output = io.StringIO()
        with patch("sys.argv", argv), patch("quant_data.__main__.dotenv_values", return_value={"TUSHARE_TOKEN": "test-secret"}), \
                patch("quant_data.__main__.TushareClient", return_value=ProviderClient(self.rows)), redirect_stdout(output):
            self.assertEqual(main(), 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS")
        with patch("sys.argv", ["quant_data", "ingest", "suspend_daily"]), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
