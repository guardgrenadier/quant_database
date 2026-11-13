"""夜间本地分钟编排政策、真实 PARTIAL 连续执行和备份轮换测试。"""

import json
from pathlib import Path
import shutil
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.datasets.bse_mapping import BseMappingDataset
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.suspend_daily import SuspendDailyDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from scripts import run_local_minute_overnight as overnight

SOURCE_SCHEMA = pl.Schema({
    "code": pl.String, "trade_time": pl.String, "date": pl.String,
    "open": pl.Float32, "high": pl.Float32, "low": pl.Float32, "close": pl.Float32,
    "vol": pl.Float32, "amount": pl.Float32,
    "pre_close": pl.Float32, "change": pl.Float32, "pct_chg": pl.Float32,
})


def source_row(code, label, day, volume=100, amount=1000):
    return {
        "code": code, "trade_time": label, "date": day.replace("-", ""),
        "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0,
        "vol": volume, "amount": amount, "pre_close": 9.5, "change": 0.5, "pct_chg": 5.0,
    }


def result_with(error_rules=None, warning_rules=None):
    return {
        "problems": [], "new_error_rules": error_rules or [],
        "new_warning_rules": warning_rules or [], "conservation_rules": [],
    }


class OvernightPolicyTest(unittest.TestCase):
    def test_known_unit_errors_and_warnings_continue(self):
        result = result_with(warning_rules=[])
        result["new_error_rules"] = []
        self.assertEqual(overnight.rule_policy("2024-01-02", result, {"new_error_rules": {}}), [])

    def test_new_unit_error_stops_only_when_repeated_on_another_day(self):
        state = {"new_error_rules": {}}
        result = result_with(error_rules=["local.new_unit_error"])
        self.assertEqual(overnight.rule_policy("2024-01-02", result, state), [])
        state["new_error_rules"]["local.new_unit_error"] = "2024-01-02"
        self.assertEqual(overnight.rule_policy("2024-01-02", result, state), [])
        self.assertIn("repeated", overnight.rule_policy("2024-01-03", result, state)[0])

    def test_new_warning_and_conservation_failure_stop(self):
        state = {"new_error_rules": {}}
        self.assertIn("WARNING", overnight.rule_policy(
            "2024-01-02", result_with(warning_rules=["local.new_warning"]), state)[0])
        result = result_with()
        result["conservation_rules"] = ["local.traded_equivalence"]
        self.assertIn("Conservation", overnight.rule_policy("2024-01-02", result, state)[0])

    def test_consecutive_missing_and_zero_published_breakers(self):
        state = {}
        self.assertIsNone(overnight.update_skip_policy(state, "MISSING_SOURCE"))
        self.assertEqual(state["consecutive_missing_source"], 1)
        self.assertIsNotNone(overnight.update_skip_policy(state, "MISSING_SOURCE"))
        state = {}
        self.assertIsNone(overnight.update_execution_policy(state, 0))
        self.assertIsNotNone(overnight.update_execution_policy(state, 0))
        state = {}
        overnight.update_execution_policy(state, 1)
        self.assertEqual(state["consecutive_zero_published"], 0)

    def test_missing_dependency_is_record_only_even_after_previous_streak(self):
        state = {"consecutive_missing_dependency": 2, "consecutive_missing_source": 1}
        for _ in range(3):
            self.assertIsNone(overnight.update_skip_policy(state, "MISSING_DEPENDENCY"))
        self.assertEqual(state["consecutive_missing_dependency"], 0)
        self.assertEqual(state["consecutive_missing_source"], 0)

    def test_backup_directory_cannot_point_to_wsl(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as raised:
            overnight.parse_args([
                "--start-year", "2023", "--end-year", "2013",
                "--backup-dir", "data/backups",
            ])
        self.assertEqual(raised.exception.code, 2)

    def test_backup_rotation_does_not_use_ingestion_resource_gate(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            database = root / "warehouse.duckdb"
            backup_dir = root / "backups"
            backup_dir.mkdir()
            with duckdb.connect(str(database)) as con:
                db.initialize(con, Stock1MinDataset())
            backup = backup_dir / "warehouse-before.duckdb"
            shutil.copyfile(database, backup)
            args = SimpleNamespace(db=database, backup_dir=backup_dir, min_free_gib=0)
            state = {"backup": {"path": str(backup)}}
            with patch.object(overnight, "assert_no_lock", lambda _: None), \
                 patch.object(overnight, "assert_resources", side_effect=AssertionError("unexpected C gate")):
                result = overnight.rotate_backup(args, state, 2020)
            self.assertTrue(Path(result["path"]).is_file())
            self.assertFalse(backup.exists())
            self.assertEqual(result["sha256"], overnight.file_sha256(database))


class OvernightIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.database = self.root / "warehouse.duckdb"
        self.source_dir = self.root / "source"
        self.backup_dir = self.root / "backups"
        self.work_dir = self.root / "work"
        self.source_dir.mkdir()
        self.backup_dir.mkdir()
        datasets = (TradeCalendarDataset(), SuspendDailyDataset(), StockBasicDataset(),
                    StockDailyDataset(), BseMappingDataset(), Stock1MinDataset())
        with duckdb.connect(str(self.database)) as con:
            for dataset in datasets:
                db.initialize(con, dataset)
            for day in ("2020-03-12", "2020-03-13", "2019-12-31"):
                con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,true,NULL)", [day])
                con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                            [f"SSE:{day}"])
                for dataset in ("suspend_daily", "stock_daily"):
                    con.execute("INSERT INTO meta.coverage VALUES (?,?,'PUBLISHED',1,current_timestamp,NULL)",
                                [dataset, day])
                con.execute("INSERT INTO canonical.suspend_daily VALUES ('000002.SZ',?,'S',NULL)", [day])
                con.execute("INSERT INTO canonical.stock_daily VALUES ('000001.SZ',?,10,11,9,10.5,9.5,1,10,10,100)", [day])
            for code in ("000001.SZ", "000002.SZ"):
                con.execute("""
                    INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
                    VALUES (?,?,?,'SZSE','L','1991-04-03')
                """, [code, code.split(".")[0], code])
            con.execute("INSERT INTO meta.coverage VALUES ('stock_basic','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
            con.execute("INSERT INTO canonical.bse_mapping VALUES ('映射','832000.BJ','920000.BJ','2017-03-29')")
            con.execute("INSERT INTO meta.coverage VALUES ('bse_mapping','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
        self.write_source("2020-03-12", opening=False)
        self.write_source("2020-03-13", opening=True)
        self.write_source("2019-12-31", opening=True)
        shutil.copyfile(self.database, self.backup_dir / "warehouse-before-2020.duckdb")

    def write_source(self, day, opening):
        rows = []
        if opening:
            rows.append(source_row("000001.SZ", f"{day} 09:30:00", day, volume=200, amount=2000))
        rows.extend([
            source_row("000001.SZ", "09:31", day),
            source_row("000001.SZ", "09:32", day, volume=0, amount=0),
            source_row("000001.SZ", "15:00", day),
            source_row("000002.SZ", "09:30", day, volume=0, amount=0),
        ])
        pl.DataFrame(rows, schema=SOURCE_SCHEMA, infer_schema_length=None).write_parquet(
            self.source_dir / f"{day.replace('-', '')}.parquet")

    def test_real_partial_continuous_years_and_backup_rotation(self):
        argv = [
            "--start-year", "2020", "--end-year", "2019",
            "--db", str(self.database), "--source-dir", str(self.source_dir),
            "--backup-dir", str(self.backup_dir), "--landing-dir", str(self.root / "landing"),
            "--work-dir", str(self.work_dir), "--min-free-gib", "0",
            "--day-timeout-seconds", "60",
        ]
        with patch.object(overnight, "CLI_PREFIX", [sys.executable, "-m", "quant_data"]), \
             patch.object(overnight, "HOST_MOUNTS", ()), \
             patch.object(overnight, "REQUIRED_BACKUP_DIR", self.backup_dir), \
             patch.object(overnight, "assert_no_lock", lambda _: None):
            returncodes = (overnight.main(argv), overnight.main(argv))
        if returncodes != (0, 0):
            stop = self.work_dir / "stop.traceback.log"
            self.fail(f"Overnight executor failed: {returncodes}\n{stop.read_text() if stop.exists() else 'no traceback'}")
        with duckdb.connect(str(self.database), read_only=True) as con:
            statuses = con.execute("""
                SELECT status FROM meta.run WHERE dataset='stock_1min' ORDER BY started_at
            """).fetchall()
            self.assertEqual(statuses, [("PARTIAL",), ("SUCCESS",), ("SUCCESS",)])
            self.assertEqual(con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 1202)
        daily = [json.loads(line) for line in (self.work_dir / "daily.jsonl").read_text().splitlines()]
        self.assertEqual([r["status"] for r in daily], ["PARTIAL", "SUCCESS", "SUCCESS"])
        self.assertEqual(daily[0]["failed"], 1)
        self.assertIn("local.auction_not_reported", daily[0]["error_rules"])
        self.assertEqual(len(list(self.work_dir.glob("logs/*.stdout.json.gz"))), 3)
        self.assertFalse(list(self.work_dir.glob("logs/*.stdout.json")))
        backups = list(self.backup_dir.glob("warehouse-*.duckdb"))
        self.assertEqual(len(backups), 1)
        self.assertIn("after-local-minute-2019", backups[0].name)
        self.assertEqual(overnight.file_sha256(backups[0]), overnight.file_sha256(self.database))
        state = json.loads((self.work_dir / "state.json").read_text())
        self.assertEqual(state["completed_years"], [2020, 2019])
        summary = json.loads((self.work_dir / "year-2020-summary.json").read_text())
        self.assertEqual(summary["executed_days"], 2)
        self.assertEqual(summary["published_units"], 3)
        self.assertEqual(summary["failed_units"], 1)
        self.assertEqual(summary["independent_historical_audit"], "NOT_TRIGGERED")
        self.assertEqual(summary["error_rules"], {"local.auction_not_reported": 1})
        self.assertGreater(summary["max_rss_kib"], 0)
        previous = json.loads((self.work_dir / "year-2019-summary.json").read_text())
        self.assertEqual(previous["executed_days"], 1)
        self.assertEqual(previous["published_units"], 2)
        campaign = json.loads((self.work_dir / "campaign-summary.json").read_text())
        self.assertEqual(campaign["completed_years"], [2020, 2019])
        self.assertEqual(campaign["executed_days"], 3)


if __name__ == "__main__":
    unittest.main()
