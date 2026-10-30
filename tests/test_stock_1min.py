"""分钟完整网格正式链路：固定上游、银河交付、竞价扣减与状态构造。"""

from datetime import datetime, timezone
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import duckdb

from quant_data import db
from quant_data.core import audit, runner
from quant_data.core.models import RunOptions
from quant_data.datasets.bse_mapping import BseMappingDataset
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.suspend_daily import SuspendDailyDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.galaxy.client import GalaxyResponse
from quant_data.sources.galaxy.stock_1min import GalaxyStock1MinAdapter


DAYS = ("2020-03-12", "2020-03-13")


def kline(code, day, minute, *, volume=1000, amount=10000):
    prices = {"open_price": 10_000_000, "high_price": 11_000_000,
              "low_price": 9_000_000, "close_price": 10_500_000}
    return {"security_code": code.split(".")[0], "kline_time": int(day.replace("-", "")) * 10000 + minute,
            **prices, "volume_trade": volume * 100, "value_trade": int(amount * 100000)}


def snapshot(code, day, *, volume=200, amount=2000):
    return {"ts_code": code, "trade_time": f"{day}T01:25:00+00:00",
            "open": 10.0, "high": 10.0, "low": 10.0, "last": 10.0,
            "volume": volume, "amount": amount}


class MinuteClient:
    def __init__(self, transform=None):
        self.transform = transform
        self.requests = []

    def fetch(self, requests, artifacts):
        self.requests.extend(requests)
        responses = []
        for request in requests:
            rows = []
            for code in request.codes:
                days = (DAYS if request.kind == "kline" and request.start_date == 20200312
                        else (str(request.start_date),))
                for day in days:
                    day = day if isinstance(day, str) else str(day)
                    if request.kind == "kline":
                        rows.extend((kline(code, day, 930), kline(code, day, 931, volume=100, amount=1050)))
                    else:
                        rows.append(snapshot(code, day))
            response = GalaxyResponse(request, "COMPLETE", tuple(rows), None, 0)
            if self.transform:
                response = self.transform(request, response)
            responses.append(response)
        return tuple(responses), ()


class Stock1MinTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.con = duckdb.connect(str(self.root / "warehouse.duckdb"))
        self.addCleanup(self.con.close)
        self.datasets = (TradeCalendarDataset(), SuspendDailyDataset(), StockBasicDataset(),
                         StockDailyDataset(), BseMappingDataset(), Stock1MinDataset())
        for dataset in self.datasets:
            db.initialize(self.con, dataset)
        for day in (*DAYS, "2020-03-14"):
            opened = day != "2020-03-14"
            self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,?,NULL)", [day, opened])
            self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                             [f"SSE:{day}"])
            if opened:
                for dataset in ("suspend_daily", "stock_daily"):
                    self.con.execute("INSERT INTO meta.coverage VALUES (?,?, 'PUBLISHED',1,current_timestamp,NULL)",
                                     [dataset, day])
        for code in ("000001.SZ", "000002.SZ"):
            self.con.execute("""
                INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
                VALUES (?,?,?,'SZSE','L','1991-04-03')
            """, [code, code.split(".")[0], code])
        self.con.execute("INSERT INTO meta.coverage VALUES ('stock_basic','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
        self.con.execute("INSERT INTO canonical.bse_mapping VALUES ('映射','832000.BJ','920000.BJ','2017-03-29')")
        self.con.execute("INSERT INTO meta.coverage VALUES ('bse_mapping','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
        for day in DAYS:
            self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000002.SZ',?,'S',NULL)", [day])
            self.con.execute("""
                INSERT INTO canonical.stock_daily VALUES ('000001.SZ',?,10,11,9,10.5,9.5,1,10,10,100)
            """, [day])
        self.dataset = self.datasets[-1]

    def ingest(self, client=None, params=None):
        self.client = client or MinuteClient()
        return runner.run(self.con, self.dataset, GalaxyStock1MinAdapter(self.client),
                          params or {"start_date": "20200312", "end_date": "20200314", "parallel": 2},
                          RunOptions(self.root / "landing"))

    def test_complete_grid_auction_net_930_and_local_suspension(self):
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 4)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 962)
        first = self.con.execute("""
            SELECT bar_time,status,open,close,volume,amount,reference_price FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12' AND bar_time IN ('09:25','09:30','09:31','09:32')
            ORDER BY bar_time
        """).fetchall()
        self.assertEqual([r[1] for r in first], ["TRADED", "TRADED", "TRADED", "NO_TRADE"])
        self.assertEqual(first[0][4:], (200, 2000.0, 10.0))
        self.assertEqual(first[1][4:], (800, 8000.0, 10.5))
        self.assertEqual(first[3][2], first[3][3])
        self.assertEqual(first[3][6], 10.5)
        suspended = self.con.execute("""
            SELECT status,count(*),count(reference_price),sum(volume) FROM canonical.stock_1min
            WHERE ts_code='000002.SZ' GROUP BY status
        """).fetchall()
        self.assertEqual(suspended, [("SUSPENDED", 480, 0, 0)])
        kline_requests = [r for r in self.client.requests if r.kind == "kline"]
        self.assertEqual({r.codes[0] for r in kline_requests}, {"000001.SZ"})
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM meta.run_unit WHERE scope_decision='OUT_OF_SCOPE'
        """).fetchone()[0], 2)

    def test_zero_continuous_930_becomes_no_trade(self):
        def transform(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "volume": 1000, "amount": 10000.0} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(transform))
        self.assertEqual(result.status, "SUCCESS", result)
        row = self.con.execute("""
            SELECT status,open,close,volume,amount,reference_price FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12' AND bar_time='09:30'
        """).fetchone()
        self.assertEqual(row, ("NO_TRADE", 10.0, 10.0, 0, 0.0, 10.0))

    def test_auction_without_raw_930_blocks_unit(self):
        def transform(request, response):
            if request.kind == "kline":
                return GalaxyResponse(request, "COMPLETE",
                    tuple(r for r in response.rows if r["kline_time"] % 10000 != 930), None, 0)
            return response
        result = self.ingest(MinuteClient(transform))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 480)
        self.assertIn("auction.conservation", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})

    def test_intraday_suspension_interval_and_real_bar_priority(self):
        self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000001.SZ','2020-03-12','S','09:34-10:04')")
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        rows = self.con.execute("""
            SELECT bar_time,status FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12'
              AND bar_time IN ('09:30','09:31','09:33','09:34','10:03','10:04') ORDER BY bar_time
        """).fetchall()
        self.assertEqual([r[1] for r in rows], ["TRADED", "TRADED", "NO_TRADE", "SUSPENDED", "SUSPENDED", "NO_TRADE"])

    def test_full_day_conflict_is_unknown_and_does_not_request(self):
        self.con.execute("""
            INSERT INTO canonical.stock_daily VALUES ('000002.SZ','2020-03-12',10,11,9,10.5,9.5,1,10,10,100)
        """)
        result = self.ingest()
        self.assertEqual(result.status, "PARTIAL", result)
        unknown = self.con.execute("""
            SELECT unit_key FROM meta.run_unit WHERE scope_decision='UNKNOWN'
        """).fetchall()
        self.assertEqual(unknown, [("000002.SZ:2020-03-12",)])
        self.assertFalse(any(r.codes == ("000002.SZ",) for r in self.client.requests))

    def test_snapshot_absence_and_history_unavailable_are_distinct(self):
        def missing(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "INCOMPLETE", (), "missing codes", None)
            return response
        result = self.ingest(MinuteClient(missing))
        self.assertEqual(result.status, "PARTIAL", result)
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id]).fetchall()}
        self.assertEqual(rules, {"source.request"})

        def unavailable(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE", (), None, -76)
            return response
        result = self.ingest(MinuteClient(unavailable))
        self.assertEqual(result.status, "SUCCESS", result)
        row = self.con.execute("""
            SELECT volume,amount FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12' AND bar_time='09:30'
        """).fetchone()
        self.assertEqual(row, (1000, 10000.0))
        self.assertIn("snapshot.history_unavailable", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='WARNING'").fetchall()})

    def test_empty_snapshot_delivery_is_zero_auction_not_missing(self):
        def empty(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE", (), None, 0)
            return response
        result = self.ingest(MinuteClient(empty))
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM canonical.stock_1min WHERE bar_time='09:25'
        """).fetchone()[0], 0)
        self.assertIn("snapshot.zero_auction", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='WARNING'").fetchall()})

    def test_failed_request_preserves_previous_success(self):
        self.ingest()
        before = self.con.execute("SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall()
        def fail(request, response):
            if request.kind == "kline":
                return GalaxyResponse(request, "FAILED", (), "injected request failure", -1)
            return response
        result = self.ingest(MinuteClient(fail))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(before, self.con.execute(
            "SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall())
        self.assertIn("source.request", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id]).fetchall()}
        self.assertEqual(rules, {"source.request"})

    def test_complete_empty_kline_still_reports_real_conflict(self):
        """完整空交付具备比较前提，不能随失败获取一起抑制真实冲突。"""
        def empty(request, response):
            """只清空成功 K 线，保留真实交付状态。"""
            return GalaxyResponse(request, "COMPLETE", (), None, 0) if request.kind == "kline" else response
        result = self.ingest(MinuteClient(empty))
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall()}
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("source.daily_conflict", rules)
        self.assertIn("auction.conservation", rules)

    def test_input_failure_does_not_hide_independent_checks(self):
        """快照失败仍检查 K 线坏槽；快照解析失败不制造零竞价告警。"""
        def invalid(request, response):
            """独立制造 K 线坏槽与快照类型问题。"""
            if request.kind == "kline":
                rows = tuple({**r, "kline_time": r["kline_time"] // 10000 * 10000 + 1600} for r in response.rows)
            else:
                rows = tuple({**r, "volume": "invalid"} for r in response.rows)
            return GalaxyResponse(request, "COMPLETE", rows, None, 0)
        result = self.ingest(MinuteClient(invalid))
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall()}
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("source.time_slot", rules)
        self.assertIn("snapshot.types", rules)
        self.assertFalse(rules & {"snapshot.zero_auction", "source.daily_conflict", "auction.conservation"})

        def failed(request, response):
            """改为快照通信失败，K 线独立坏槽仍须保留。"""
            if request.kind == "snapshot":
                return GalaxyResponse(request, "FAILED", (), "network failure", -1)
            return invalid(request, response)
        result = self.ingest(MinuteClient(failed))
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=?", [result.run_id]).fetchall()}
        self.assertEqual(rules, {"source.request", "source.time_slot"})

    def test_kline_parse_failure_keeps_root_cause(self):
        """K 线不能解析时保留类型错误，不派生空交付和竞价冲突。"""
        def invalid(request, response):
            """只损坏 K 线字段，快照正常返回。"""
            if request.kind == "kline":
                return GalaxyResponse(request, "COMPLETE", tuple({**r, "kline_time": "invalid"} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(invalid))
        rules = {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id]).fetchall()}
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(rules, {"source.types"})

    def test_invalid_raw_slot_and_snapshot_time_are_errors(self):
        def bad_slot(request, response):
            if request.kind == "kline":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "kline_time": r["kline_time"] // 10000 * 10000 + 1600}
                          if r["kline_time"] % 10000 == 931 else r for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(bad_slot))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("source.time_slot", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})

        def bad_snapshot_day(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "trade_time": "2020-03-15T01:25:00+00:00"} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(bad_snapshot_day))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("snapshot.range", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})

    def test_out_of_scope_suspend_text_does_not_block_and_type_error_is_located(self):
        self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000999.SZ','2020-03-12','S','bad')")
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)

        def bad_type(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "volume": "bad"} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(bad_type))
        self.assertEqual(result.status, "PARTIAL", result)
        rows = self.con.execute("""
            SELECT units FROM meta.validation_check WHERE rule='snapshot.types'
        """).fetchall()
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(json.loads(r[0]) in (["000001.SZ:2020-03-12"], ["000001.SZ:2020-03-13"]) for r in rows))

    def test_dependency_change_during_fetch_blocks_prepare(self):
        self.ingest()
        before = self.con.execute("SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall()
        class ChangingClient(MinuteClient):
            def fetch(self, requests, artifacts):
                self.con.execute("UPDATE meta.coverage SET publication_id=publication_id+1000 WHERE dataset='stock_daily'")
                return super().fetch(requests, artifacts)
        client = ChangingClient()
        client.con = self.con
        result = self.ingest(client)
        self.assertEqual(result.status, "FAILED", result)
        self.assertIn("Dependency changed", result.error)
        self.assertEqual(before, self.con.execute(
            "SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall())

    def test_one_sided_zero_remainder_and_out_of_range_are_errors(self):
        def one_zero(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "volume": 1000, "amount": 0.0} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(one_zero))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("auction.remainder", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})

        def wrong_day(request, response):
            if request.kind == "kline" and response.rows:
                rows = list(response.rows)
                rows[0] = {**rows[0], "kline_time": 202003150930}
                return GalaxyResponse(request, "COMPLETE", tuple(rows), None, 0)
            return response
        result = self.ingest(MinuteClient(wrong_day))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("source.range", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})

    def test_rerun_replaces_units_without_accumulating(self):
        first = self.ingest()
        before = self.con.execute("SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall()
        second = self.ingest()
        after = self.con.execute("SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall()
        self.assertEqual(second.status, "SUCCESS", second)
        self.assertEqual(before, after)
        self.assertEqual(len(second.publications), 4)
        self.assertGreater(min(p.publication_id for p in second.publications),
                           max(p.publication_id for p in first.publications))

    def test_invalid_suspend_timing_blocks_only_affected_units(self):
        self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000001.SZ','2020-03-12','S','09:30/10:00')")
        result = self.ingest()
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("suspend.timing_format", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000001.SZ' AND trade_date='2020-03-12'
        """).fetchone()[0], 0)

    def test_bse_mapping_drives_era_codes(self):
        self.con.execute("UPDATE canonical.stock_basic SET delist_date='2020-03-14'")
        self.con.execute("""
            INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
            VALUES ('920000.BJ','920000','映射','BSE','L','2017-03-29')
        """)
        for day in ("2025-10-01", "2025-10-02", "2025-10-03", "2025-10-04",
                    "2025-10-05", "2025-10-06", "2025-10-07", "2025-10-08"):
            self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,false,NULL)", [day])
            self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                             [f"SSE:{day}"])
        for code, day in (("832000.BJ", "2025-09-30"), ("920000.BJ", "2025-10-09")):
            self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,true,NULL)", [day])
            self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                             [f"SSE:{day}"])
            for dataset in ("suspend_daily", "stock_daily"):
                self.con.execute("INSERT INTO meta.coverage VALUES (?,?,'PUBLISHED',1,current_timestamp,NULL)", [dataset, day])
            self.con.execute("""
                INSERT INTO canonical.stock_daily VALUES (?,?,10,11,9,10.5,9.5,1,10,10,100)
            """, [code, day])
        def with_bse_close(request, response):
            if request.kind == "kline" and request.codes[0].endswith(".BJ"):
                day = str(request.start_date)
                return GalaxyResponse(request, "COMPLETE",
                    (*response.rows, kline(request.codes[0], day, 1500, volume=100, amount=1000)), None, 0)
            return response
        result = self.ingest(MinuteClient(with_bse_close),
                             params={"start_date": "20250930", "end_date": "20251009", "parallel": 2})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual({p.unit_key for p in result.publications}, {"832000.BJ:2025-09-30", "920000.BJ:2025-10-09"})
        self.assertEqual(self.con.execute("""
            SELECT ts_code,count(*) FROM canonical.stock_1min GROUP BY ts_code ORDER BY ts_code
        """).fetchall(), [("832000.BJ", 243), ("920000.BJ", 243)])
        self.assertEqual({r.codes[0] for r in self.client.requests if r.kind == "kline"}, {"832000.BJ", "920000.BJ"})

    def test_cli_assembles_galaxy_and_parallel(self):
        """CLI 装配前收尾旧 Run，并在结果中明确报告；新执行保持正常发布。"""
        from quant_data.__main__ import main
        interrupted, _ = audit.begin(self.con, "stock_1min", {}, self.root / "old-landing")
        argv = ["quant_data", "ingest", "stock_1min", "--db", str(self.root / "warehouse.duckdb"),
                "--landing-dir", str(self.root / "cli-landing"), "--start-date", "20200312",
                "--end-date", "20200314", "--parallel", "4"]
        client = MinuteClient()
        output = io.StringIO()
        with patch("sys.argv", argv), \
             patch("quant_data.__main__.dotenv_values", return_value={"AD_USERNAME": "user", "AD_PASSWORD": "secret"}), \
             patch("quant_data.__main__.GalaxyClient", return_value=client) as factory, redirect_stdout(output):
            self.assertEqual(main(), 0)
        factory.assert_called_once_with(parallel=4)
        self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS")
        self.assertEqual(json.loads(output.getvalue())["reconciled_runs"], [{"run_id": interrupted, "status": "FAILED"}])


if __name__ == "__main__":
    unittest.main()
