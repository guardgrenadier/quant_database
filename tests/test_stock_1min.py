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
                        and request.end_date != 20200312 else (str(request.start_date),))
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


class RetryClient(MinuteClient):
    """主波次和补取波次分别注入响应；正常子请求沿用完整样本。"""

    def __init__(self, primary_transform, retry_transform=None):
        super().__init__()
        self.primary_transform = primary_transform
        self.retry_transform = retry_transform
        self.retry_calls = 0

    def fetch(self, requests, artifacts, allow_unrelated_evidence=False):
        # 只有分钟 Adapter 的补取波次允许多波次证据，测试同时核对调用方向。
        self.transform = self.retry_transform if allow_unrelated_evidence else self.primary_transform
        self.retry_calls += int(allow_unrelated_evidence)
        return super().fetch(requests, artifacts)


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
        mapping = dict(self.con.execute("SELECT n_code,o_code FROM canonical.bse_mapping").fetchall())
        return runner.run(self.con, self.dataset, GalaxyStock1MinAdapter(self.client, mapping),
                          params or {"start_date": "20200312", "end_date": "20200314", "parallel": 2},
                          RunOptions(self.root / "landing"))

    def test_complete_grid_auction_net_930_and_local_suspension(self):
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 4)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 962)
        # 物理存储只保留十列分钟事实；逻辑键由验收保证，不再创建 ART。
        storage = self.con.execute("""
            SELECT data_type FROM duckdb_columns()
            WHERE schema_name='canonical' AND table_name='stock_1min' AND column_name='bar_time'
        """).fetchone()[0]
        self.assertEqual(storage, "TIME")
        self.assertEqual(self.con.execute("SELECT index_count,has_primary_key FROM duckdb_tables() "
                                          "WHERE table_name='stock_1min'").fetchone(), (0, False))
        canonical_columns = [r[0] for r in self.con.execute(
            "SELECT column_name FROM duckdb_columns() WHERE table_name='stock_1min'").fetchall()]
        self.assertNotIn("reference_price", canonical_columns)
        self.assertEqual(len(canonical_columns), 10)
        first = self.con.execute("""
            SELECT bar_time,status,open,close,volume,amount FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12' AND bar_time IN ('09:25','09:30','09:31','09:32')
            ORDER BY bar_time
        """).fetchall()
        self.assertEqual([r[1] for r in first], ["TRADED", "TRADED", "TRADED", "NO_TRADE"])
        self.assertEqual(first[0][4:], (200, 2000.0))
        self.assertEqual(first[1][4:], (800, 8000.0))
        self.assertEqual(first[3][2], first[3][3])
        self.assertEqual(first[3][2], 10.5)
        suspended = self.con.execute("""
            SELECT status,count(*),count(close),sum(volume) FROM canonical.stock_1min
            WHERE ts_code='000002.SZ' GROUP BY status
        """).fetchall()
        self.assertEqual(suspended, [("SUSPENDED", 480, 0, 0)])
        kline_requests = [r for r in self.client.requests if r.kind == "kline"]
        self.assertEqual({r.codes[0] for r in kline_requests}, {"000001.SZ"})
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM meta.run_unit WHERE scope_decision='OUT_OF_SCOPE'
        """).fetchone()[0], 2)

    def test_ts_code_subset_limits_requests_publication_and_audit(self):
        result = self.ingest(params={"start_date": "20200312", "end_date": "20200314", "parallel": 2,
                                     "ts_codes": ["000001.SZ"]})
        self.assertEqual(result.status, "SUCCESS", result)
        # 业务边界：子集只包含选中代码；周末保留一条排除审计，其他股票不产生请求、发布或本次审计。
        self.assertEqual({p.unit_key for p in result.publications},
                         {"000001.SZ:2020-03-12", "000001.SZ:2020-03-13"})
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 482)
        self.assertEqual(self.con.execute("SELECT scope_decision,count(*) FROM meta.run_unit WHERE run_id=? GROUP BY ALL ORDER BY ALL",
                                          [result.run_id]).fetchall(), [("IN_SCOPE", 2), ("OUT_OF_SCOPE", 1)])
        self.assertTrue(self.client.requests)
        self.assertTrue(all(set(r.codes) == {"000001.SZ"} for r in self.client.requests))

    def test_ts_code_subset_rejects_invalid_or_unresolved_codes(self):
        for codes in ([], ["000001.SZ", "000001.SZ"]):
            with self.subTest(codes=codes), self.assertRaises(ValueError):
                self.dataset.dependencies({"start_date": "20200312", "end_date": "20200314",
                                           "parallel": 2, "ts_codes": codes}, self.con)
        with self.assertRaises(ValueError):
            self.dataset.dependencies({"start_date": "20200312", "end_date": "20200312",
                                       "source": {"type": "LOCAL_PARQUET"}, "ts_codes": ["000001.SZ"]}, self.con)
        result = self.ingest(params={"start_date": "20200312", "end_date": "20200314", "parallel": 2,
                                     "ts_codes": ["000003.SZ"]})
        self.assertEqual(result.status, "FAILED")
        self.assertIn("outside resolved membership", result.error)
        self.assertEqual(self.client.requests, [])

    def test_zero_continuous_930_becomes_no_trade(self):
        def transform(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "COMPLETE",
                    tuple({**r, "volume": 1000, "amount": 10000.0} for r in response.rows), None, 0)
            return response
        result = self.ingest(MinuteClient(transform))
        self.assertEqual(result.status, "SUCCESS", result)
        row = self.con.execute("""
            SELECT status,open,close,volume,amount FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12' AND bar_time='09:30'
        """).fetchone()
        self.assertEqual(row, ("NO_TRADE", 10.0, 10.0, 0, 0.0))

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

    def test_multisegment_seconds_and_unordered_suspension_use_full_minutes(self):
        self.con.execute("""
            INSERT INTO canonical.suspend_daily VALUES (
                '000001.SZ','2020-03-12','S',
                '10:05:50-10:15:50,9:34-9:35,09:35-09:36,09:31-09:32,10:16-10:17,11:00:10-11:00:50'
            )
        """)
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        rows = self.con.execute("""
            SELECT bar_time,status FROM canonical.stock_1min
            WHERE ts_code='000001.SZ' AND trade_date='2020-03-12'
              AND bar_time IN ('09:31','09:33','09:34','09:35','09:36','10:05','10:06','10:14','10:15','10:16','10:17','11:00')
            ORDER BY bar_time
        """).fetchall()
        self.assertEqual([r[1] for r in rows], [
            "TRADED", "NO_TRADE", "SUSPENDED", "SUSPENDED", "NO_TRADE",
            "NO_TRADE", "SUSPENDED", "SUSPENDED", "NO_TRADE", "SUSPENDED", "NO_TRADE", "NO_TRADE",
        ])

    def test_reverse_nonpadded_suspension_remains_blocked(self):
        self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000001.SZ','2020-03-12','S','13:00-9:30')")
        result = self.ingest()
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertIn("suspend.timing_range", {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE level='ERROR'").fetchall()})
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000001.SZ' AND trade_date='2020-03-12'
        """).fetchone()[0], 0)

    def test_same_day_resume_with_positive_daily_allows_requests(self):
        for day in DAYS:
            self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000002.SZ',?,'R',NULL)", [day])
            self.con.execute("INSERT INTO canonical.stock_daily VALUES ('000002.SZ',?,10,11,9,10.5,9.5,1,10,10,100)", [day])
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        # 业务边界：R 与正成交日线共同解除全天停牌，两个代码都正常请求和发布。
        self.assertEqual(len(result.publications), 4)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 964)
        self.assertEqual({r.codes[0] for r in self.client.requests if r.kind == "kline"}, {"000001.SZ", "000002.SZ"})

    def test_same_day_resume_without_daily_keeps_full_suspension(self):
        for day in DAYS:
            self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000002.SZ',?,'R',NULL)", [day])
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        # 业务边界：R 单独不能解除全天停牌，无日线时仍只发布本地 SUSPENDED 网格。
        self.assertEqual({r.codes[0] for r in self.client.requests if r.kind == "kline"}, {"000001.SZ"})
        self.assertEqual(self.con.execute("""
            SELECT status,count(*) FROM canonical.stock_1min WHERE ts_code='000002.SZ' GROUP BY status
        """).fetchall(), [("SUSPENDED", 480)])

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

    def test_kline_request_retry_recovers_each_stock_day(self):
        def fail_parent(request, response):
            if request.kind == "kline":
                # 父请求携带部分行，最终只能消费逐日子响应，不能重复进入 Raw。
                return GalaxyResponse(request, "FAILED", response.rows, "multi-day failure", -1, True)
            return response
        client = RetryClient(fail_parent)
        result = self.ingest(client)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 4)
        self.assertEqual(client.retry_calls, 1)
        children = [r for r in client.requests if "-retry-" in r.request_id]
        self.assertEqual(len(children), 2)
        self.assertTrue(all(r.kind == "kline" and len(r.codes) == 1 and r.start_date == r.end_date
                            for r in children))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 962)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id],
        ).fetchone()[0], 0)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM meta.validation_check WHERE run_id=? AND rule='source.request_retry'",
            [result.run_id],
        ).fetchone()[0], 1)
        lineage = dict(self.con.execute(
            "SELECT unit_key,request_ids FROM meta.run_unit WHERE run_id=? AND scope_decision='IN_SCOPE' "
            "AND unit_key LIKE '000001.SZ:%'",
            [result.run_id],
        ).fetchall())
        self.assertEqual(len(lineage), 2)
        for encoded in lineage.values():
            request_ids = json.loads(encoded)
            self.assertEqual(len(request_ids), 3)
            self.assertEqual(sum("-retry-" in request_id for request_id in request_ids), 1)

    def test_snapshot_request_retry_recovers_complete_units(self):
        self.con.execute("""
            INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
            VALUES ('000003.SZ','000003','第三股票','SZSE','L','1991-04-03')
        """)
        for day in DAYS:
            self.con.execute("INSERT INTO canonical.stock_daily VALUES ('000003.SZ',?,10,11,9,10.5,9.5,1,10,10,100)", [day])

        def missing_parent(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "INCOMPLETE", response.rows[:1], "missing codes", None, True)
            return response
        client = RetryClient(missing_parent)
        result = self.ingest(client)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(len(result.publications), 6)
        children = [r for r in client.requests if "-retry-" in r.request_id]
        self.assertEqual(len(children), 4)
        self.assertTrue(all(r.kind == "snapshot" and len(r.codes) == 1 and r.start_date == r.end_date
                            for r in children))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 1444)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id],
        ).fetchone()[0], 0)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM meta.validation_check WHERE run_id=? AND rule='source.request_retry'",
            [result.run_id],
        ).fetchone()[0], 2)

    def test_retry_failure_blocks_only_child_unit_and_preserves_old_data(self):
        self.con.execute("""
            INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
            VALUES ('000003.SZ','000003','第三股票','SZSE','L','1991-04-03')
        """)
        for day in DAYS:
            self.con.execute("INSERT INTO canonical.stock_daily VALUES ('000003.SZ',?,10,11,9,10.5,9.5,1,10,10,100)", [day])
        self.ingest()
        failed_unit = "000003.SZ:2020-03-13"
        before = self.con.execute("""
            SELECT * FROM canonical.stock_1min WHERE ts_code='000003.SZ' AND trade_date='2020-03-13'
            ORDER BY bar_time
        """).fetchall()

        def missing_parent(request, response):
            if request.kind == "snapshot":
                return GalaxyResponse(request, "INCOMPLETE", response.rows[:1], "missing codes", None, True)
            return response

        def fail_one_child(request, response):
            if request.kind == "snapshot" and request.codes == ("000003.SZ",) and request.start_date == 20200313:
                return GalaxyResponse(request, "FAILED", (), "child failure", -1, True)
            return response
        result = self.ingest(RetryClient(missing_parent, fail_one_child))
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(len(result.publications), 5)
        self.assertEqual(self.con.execute(
            "SELECT status FROM meta.run_unit WHERE run_id=? AND unit_key=?", [result.run_id, failed_unit],
        ).fetchone()[0], "FAILED")
        errors = self.con.execute(
            "SELECT units FROM meta.validation_check WHERE run_id=? AND rule='source.request' AND level='ERROR'",
            [result.run_id],
        ).fetchall()
        self.assertEqual(len(errors), 1)
        self.assertEqual(json.loads(errors[0][0]), [failed_unit])
        after = self.con.execute("""
            SELECT * FROM canonical.stock_1min WHERE ts_code='000003.SZ' AND trade_date='2020-03-13'
            ORDER BY bar_time
        """).fetchall()
        self.assertEqual(before, after)

    def test_nonretryable_primary_failure_suppresses_retry_wave(self):
        def mixed_failures(request, response):
            if request.kind == "kline":
                return GalaxyResponse(request, "FAILED", (), "session failure", -1, False)
            return GalaxyResponse(request, "INCOMPLETE", (), "missing codes", None, True)
        client = RetryClient(mixed_failures)
        result = self.ingest(client)
        self.assertEqual(result.status, "PARTIAL", result)
        self.assertEqual(client.retry_calls, 0)
        self.assertFalse(any("-retry-" in r.request_id for r in client.requests))

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

    def test_bse_old_requests_publish_under_current_code(self):
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
        for day in ("2025-09-30", "2025-10-09"):
            self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,true,NULL)", [day])
            self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                             [f"SSE:{day}"])
            for dataset in ("suspend_daily", "stock_daily"):
                self.con.execute("INSERT INTO meta.coverage VALUES (?,?,'PUBLISHED',1,current_timestamp,NULL)", [dataset, day])
            self.con.execute("""
                INSERT INTO canonical.stock_daily VALUES ('920000.BJ',?,10,11,9,10.5,9.5,1,10,10,100)
            """, [day])
        def with_bse_close(request, response):
            if request.kind == "kline" and request.codes[0].endswith(".BJ"):
                day = str(request.start_date)
                return GalaxyResponse(request, "COMPLETE",
                    (*response.rows, kline(request.codes[0], day, 1500, volume=100, amount=1000)), None, 0)
            return response
        result = self.ingest(MinuteClient(with_bse_close),
                             params={"start_date": "20250930", "end_date": "20251009", "parallel": 2})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual({p.unit_key for p in result.publications}, {"920000.BJ:2025-09-30", "920000.BJ:2025-10-09"})
        self.assertEqual(self.con.execute("""
            SELECT ts_code,count(*) FROM canonical.stock_1min GROUP BY ts_code ORDER BY ts_code
        """).fetchall(), [("920000.BJ", 486)])
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
