"""ST 日名单正式链路：非空政策、按日替换、范围隔离及日历版本保护。"""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from zoneinfo import ZoneInfo

import duckdb
import polars as pl

from quant_data import db
from quant_data.__main__ import main
from quant_data.core import runner
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import RequestAttempt, RunOptions, Scope
from quant_data.datasets.stock_st import StockStDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.stock_st import TushareStockStAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter


def st_rows():
    """测试名单包含历史特殊标识及无 ST 字样名称，不能借名称推导或过滤身份。"""
    return [dict(ts_code=code, name=name, trade_date=day, type="ST", type_name=label)
            for code, name, day, label in (
                ("000010.SZ", " *ST示例 ", "20261016", " 风险警示板 "),
                ("T600018.SH", "历史名称", "20261016", "供应商标签"),
                ("874396.BJ", "ST示例", "20261019", "风险警示板"),
            )]


class ProviderClient:
    """仅替换供应商通信，日历和 ST 均走正式 Adapter 与发布链路。"""

    def __init__(self, rows=(), transform=None):
        self.rows, self.transform, self.calls = rows, transform, []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        self.calls.append((api_name, dict(params)))
        fields = tuple(reversed(fields))
        if api_name == "trade_cal":
            # 明确日历事实，包含排除用的休市日，不从 ST 返回行数推算日历。
            rows = [dict(exchange="SSE", cal_date=day, is_open=opened, pretrade_date=previous)
                    for day, opened, previous in (("20261016", 1, "20261015"), ("20261017", 0, "20261016"),
                                                  ("20261018", 0, "20261016"), ("20261019", 1, "20261016"))
                    if params["start_date"] <= day <= params["end_date"]]
        else:
            rows = [r for r in self.rows if r["trade_date"] == params["trade_date"]]
        data = {"fields": list(fields), "items": [[r[f] for f in fields] for r in reversed(rows)],
                "has_more": False, "count": 0}
        if self.transform and api_name == "stock_st":
            self.transform(params, data)
        body = {"code": 0, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api_name, params, fields, datetime.now(timezone.utc),
                                  0.01, 200, 0, tuple(data["fields"]), len(data["items"]), data.get("has_more")), body)
        return body


class StockStTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.params = {"start_date": "20261016", "end_date": "20261019"}
        for name in ("stock_st", "trade_calendar"):
            interval = patch(f"quant_data.sources.tushare.{name}.REQUEST_INTERVAL", 0)
            interval.start()
            self.addCleanup(interval.stop)
        calendar = TradeCalendarDataset()
        db.initialize(self.con, calendar)
        result = runner.run(self.con, calendar, TushareTradeCalendarAdapter(ProviderClient()),
                            self.params, RunOptions(self.root / "landing"))
        self.assertEqual(result.status, "SUCCESS", result)
        self.dataset = StockStDataset()
        db.initialize(self.con, self.dataset)

    def ingest(self, rows=None, transform=None, params=None, keep=False):
        self.client = ProviderClient(st_rows() if rows is None else rows, transform)
        return runner.run(self.con, self.dataset, TushareStockStAdapter(self.client),
                          self.params if params is None else params, RunOptions(self.root / "landing", keep))

    def current(self, day=None):
        """同时比较当前事实和成功版本，允许失败审计增加。"""
        day_filter = f" AND unit_key='{day}'" if day else ""
        return (self.con.execute("SELECT * FROM canonical.stock_st" + (f" WHERE trade_date='{day}'" if day else "") +
                                 " ORDER BY trade_date,ts_code").fetchall(),
                self.con.execute("SELECT * FROM meta.coverage WHERE dataset='stock_st'" + day_filter + " ORDER BY unit_key").fetchall(),
                self.con.execute("SELECT * FROM meta.publication WHERE dataset='stock_st'" + day_filter + " ORDER BY publication_id").fetchall())

    def test_scope_schema_artifacts_and_daily_replacement(self):
        """五字段、历史名称、开闭市 Scope、固定版本及日范围替换形成完整切片。"""
        first = self.ingest(keep=True)
        self.assertEqual(first.status, "SUCCESS", first)
        self.assertTrue(first.artifacts_complete, first)
        self.assertEqual({p.unit_key: p.row_count for p in first.publications}, {"2026-10-16": 2, "2026-10-19": 1})
        self.assertEqual(self.client.calls, [("stock_st", {"trade_date": day}) for day in ("20261016", "20261019")])
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.run_dependency WHERE run_id=?", [first.run_id]).fetchone()[0], 4)
        self.assertEqual(sum(u.status == "OUT_OF_SCOPE" for u in first.units), 2)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.coverage WHERE dataset='stock_st'").fetchone()[0], 2)
        self.assertEqual(self.con.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='canonical' AND table_name='stock_basic'").fetchone()[0], 0)
        raw = pl.read_parquet(Path(first.artifact_dir) / "landing.parquet")
        self.assertEqual(raw.schema["trade_date"], pl.String)
        self.assertIn(" *ST示例 ", raw["name"].to_list())
        self.assertEqual(pl.read_parquet(Path(first.artifact_dir) / "candidate.parquet").schema, self.dataset.schema)
        old_other = self.current("2026-10-19")
        updated = [{**st_rows()[0], "name": "改名"}]
        second = self.ingest(updated, params={"start_date": "20261016", "end_date": "20261016"})
        self.assertEqual(second.status, "SUCCESS", second)
        self.assertEqual(second.publications[0].row_count, 1)
        self.assertGreater(second.publications[0].publication_id, min(p.publication_id for p in first.publications))
        self.assertEqual(self.current("2026-10-19"), old_other)
        self.assertEqual(self.con.execute("SELECT name FROM canonical.stock_st WHERE trade_date='2026-10-16'").fetchall(), [("改名",)])

    def test_complete_empty_is_blocked_before_and_after_success(self):
        """完整零行仍记 COMPLETE 并产生空 Candidate，但首版不得发布或清除旧名单。"""
        for seeded in (False, True):
            if seeded:
                self.assertEqual(self.ingest().status, "SUCCESS")
            before = self.current()
            result = self.ingest([], keep=True)
            self.assertEqual(result.status, "FAILED", result)
            self.assertEqual(result.publications, ())
            self.assertEqual(self.current()[0], before[0])
            self.assertEqual(self.current()[2], before[2])
            if seeded:
                self.assertEqual(self.current(), before)
            self.assertEqual({r[0] for r in self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=? AND scope_decision='IN_SCOPE'", [result.run_id]).fetchall()}, {"COMPLETE"})
            self.assertEqual(self.con.execute("SELECT rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall(), [("st.nonempty",)])
            candidate = pl.read_parquet(Path(result.artifact_dir) / "candidate.parquet")
            self.assertEqual(candidate.height, 0)
            self.assertEqual(candidate.schema, self.dataset.schema)

    def test_business_errors_are_isolated_and_descriptions_are_optional(self):
        """键、类型错误保护对应日；名称/标签可空且不使用默认值回填。"""
        self.ingest()
        before = self.current("2026-10-16")
        for field, value in (("ts_code", None), ("ts_code", " "), ("type", None), ("type", "PT"), ("duplicate", None)):
            with self.subTest(field=field, value=value):
                rows = st_rows()
                if field == "duplicate":
                    rows.append({**rows[0], "name": "不同描述"})
                else:
                    rows[0][field] = value
                result = self.ingest(rows)
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual({p.unit_key for p in result.publications}, {"2026-10-19"})
                self.assertEqual(self.current("2026-10-16"), before)
        rows = st_rows()
        rows[0].update(name=" ", type_name=None)
        result = self.ingest(rows)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.con.execute("SELECT name,type_name FROM canonical.stock_st WHERE ts_code='000010.SZ'").fetchone(), (None, None))
        self.assertIn(("WARNING", "st.description_missing"), self.con.execute("SELECT level,rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall())

    def test_source_failures_and_wrong_dates_protect_the_requested_day(self):
        """列、类型、截断与错日分别报告，不能把另一目标日的错行归给那个日期。"""
        self.ingest()
        before = self.current("2026-10-16")
        cases = [("request", None), ("missing", None), ("has_more", True), ("has_more", None),
                 ("full", None), ("trade_date", "20261019"), ("trade_date", "20261017"),
                 ("trade_date", "20260230"), ("trade_date", "2026106"), ("trade_date", None),
                 ("trade_date", 20261016), ("ts_code", 10), ("name", False)]
        for field, value in cases:
            def transform(params, data):
                if params["trade_date"] != "20261016":
                    return
                if field == "request":
                    raise RuntimeError("injected request failure")
                if field == "missing":
                    index = data["fields"].index("name")
                    data["fields"].pop(index)
                    data["items"] = [r[:index] + r[index + 1:] for r in data["items"]]
                elif field == "has_more":
                    if value is None:
                        del data["has_more"]
                    else:
                        data["has_more"] = value
                elif field == "full":
                    data["items"] = [data["items"][0]] * 1000
                else:
                    data["items"][0][data["fields"].index(field)] = value
            with self.subTest(field=field, value=value):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual({p.unit_key for p in result.publications}, {"2026-10-19"})
                self.assertEqual(self.current("2026-10-16"), before)

    def test_dependencies_closed_scope_and_missing_facts(self):
        """休市不是零行成功；缺少休市日版本也阻断；缺实际日历事实留 UNKNOWN。"""
        closed = {"start_date": "20261017", "end_date": "20261018"}
        result = self.ingest(params=closed)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.current(), ([], [], []))
        self.con.execute("DELETE FROM canonical.trade_calendar WHERE cal_date='2026-10-19'")
        result = self.ingest()
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual({u.unit_key: u.status for u in result.units}["2026-10-19"], "UNKNOWN")
        self.assertEqual(self.client.calls, [("stock_st", {"trade_date": "20261016"})])
        self.con.execute("DELETE FROM meta.coverage WHERE dataset='trade_calendar' AND unit_key='SSE:2026-10-17'")
        before = self.current()
        result = self.ingest()
        self.assertEqual(result.status, "FAILED", result)
        self.assertIn("Dependency not published", result.error)
        self.assertEqual(self.client.calls, [])
        self.assertEqual(self.current(), before)

    def test_calendar_revision_of_excluded_day_blocks_publication(self):
        """获取后已排除日期的上游版本变化也必须阻断整批合格日，保护旧名单。"""
        self.ingest()
        before = self.current()
        revised = False
        def revise(params, data):
            nonlocal revised
            if revised:
                return
            revised = True
            with db.transaction(self.con):
                pid = self.con.execute("INSERT INTO meta.publication(dataset,unit_key,row_count,run_id) VALUES ('trade_calendar','SSE:2026-10-17',1,'revision-test') RETURNING publication_id").fetchone()[0]
                self.con.execute("UPDATE canonical.trade_calendar SET is_open=true WHERE cal_date='2026-10-17'")
                self.con.execute("UPDATE meta.coverage SET publication_id=? WHERE dataset='trade_calendar' AND unit_key='SSE:2026-10-17'", [pid])
        result = self.ingest(transform=revise)
        self.assertEqual(result.status, "FAILED", result)
        self.assertIn("Dependency changed", result.error)
        self.assertEqual(self.current(), before)

    def test_saved_real_responses_publish_the_same_facts(self):
        """离线重放已存真实信封，经正式 Runner 发布后与源五字段逐条核对。"""
        paths = [Path(f"data/probes/st-index-20261020-main/requests/st_{day}-p000-attempt-01.json")
                 for day in ("20261016", "20261019")]
        if not all(p.is_file() for p in paths):
            self.skipTest("本地真实响应样本不随 Git 分发")
        bodies = {day: json.loads(path.read_text())["response"] for day, path in zip(("20261016", "20261019"), paths)}
        expected = []
        for body in bodies.values():
            data = body["data"]
            for item in data["items"]:
                row = dict(zip(data["fields"], item, strict=True))
                expected.append((row["ts_code"], row["name"], date.fromisoformat(row["trade_date"]), row["type"], row["type_name"]))

        class ReplayClient:
            """只存在于离线测试，生产代码仍使用官方 Client。"""
            def query(self, api_name, params, fields, *, request_id, on_attempt):
                body = deepcopy(bodies[params["trade_date"]])
                data = body["data"]
                on_attempt(RequestAttempt(request_id, 1, api_name, params, fields, datetime.now(timezone.utc),
                                          0.0, 200, 0, tuple(data["fields"]), len(data["items"]), data["has_more"]), body)
                return body

        result = runner.run(self.con, self.dataset, TushareStockStAdapter(ReplayClient()),
                            self.params, RunOptions(self.root / "replay", True))
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertTrue(result.artifacts_complete, result)
        self.assertEqual(sorted(self.current()[0]), sorted(expected))
        self.assertEqual(sum(p.row_count for p in result.publications), len(expected))

    def test_all_conversion_errors_keep_raw_and_block_the_day(self):
        """原始日期全错时得到零行 Candidate，也不能掩盖准备失败或清除旧名单。"""
        self.ingest()
        before = self.current()
        def wrong_dates(params, data):
            for row in data["items"]:
                row[data["fields"].index("trade_date")] = "20260230"
        result = self.ingest(transform=wrong_dates, keep=True)
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)
        root = Path(result.artifact_dir)
        self.assertEqual(set(pl.read_parquet(root / "landing.parquet")["trade_date"]), {"20260230"})
        self.assertEqual(pl.read_parquet(root / "candidate.parquet").height, 0)
        self.assertEqual(self.con.execute("SELECT rule,stage FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall(), [("source.range", "prepare")])

    def test_parameter_time_boundaries_history_and_cli(self):
        """只接受显式合法闭区间，上海当日可请求、未来拒绝；更早历史不请求。"""
        tomorrow = (datetime.now(ZoneInfo("Asia/Shanghai")).date() + timedelta(days=1)).strftime("%Y%m%d")
        for params in ({}, {"start_date": "20260230", "end_date": "20261019"},
                       {"start_date": "2026106", "end_date": "20261019"},
                       {"start_date": "２０２６１０１６", "end_date": "20261019"},
                       {"start_date": "20261019", "end_date": "20261016"},
                       {"start_date": tomorrow, "end_date": tomorrow}, {**self.params, "ts_code": "000010.SZ"}):
            result = self.ingest(params=params)
            self.assertEqual(result.status, "FAILED", result)
            self.assertEqual(self.client.calls, [])
        # 上海日期基准：UTC 尚为前一天时仍允许上海当天，不取主机本地 date.today。
        with patch("quant_data.datasets.stock_st.datetime") as clock:
            clock.now.return_value = datetime(2026, 10, 21, 0, 30, tzinfo=ZoneInfo("Asia/Shanghai"))
            self.assertEqual(len(self.dataset.dependencies({"start_date": "20261021", "end_date": "20261021"}, self.con)), 1)
        client = ProviderClient(st_rows())
        delivery = TushareStockStAdapter(client).fetch(Scope(("1999-12-30", "2026-10-16")), Artifacts(self.root / "history", False))
        self.assertEqual([u.state for u in delivery.units], ["INCOMPLETE", "COMPLETE"])
        self.assertEqual(len(client.calls), 1)
        argv = ["quant_data", "ingest", "stock_st", "--db", str(self.path), "--landing-dir", str(self.root / "cli"),
                "--start-date", "20261016", "--end-date", "20261019"]
        for rows, status, code in ((st_rows(), "SUCCESS", 0), (st_rows()[:2], "PARTIAL", 1), ([], "FAILED", 1)):
            output = io.StringIO()
            with patch("sys.argv", argv), patch("quant_data.__main__.dotenv_values", return_value={"TUSHARE_TOKEN": "test-secret"}), \
                    patch("quant_data.__main__.TushareClient", return_value=ProviderClient(rows)), redirect_stdout(output):
                self.assertEqual(main(), code)
            self.assertEqual(json.loads(output.getvalue())["status"], status)
        with patch("sys.argv", ["quant_data", "ingest", "stock_st"]), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as error:
                main()
            self.assertEqual(error.exception.code, 2)
