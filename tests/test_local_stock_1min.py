"""本地日 Parquet 分钟转换、守恒检查、CLI 与发布保护。"""

from contextlib import redirect_stdout
from datetime import time
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.core import runner
from quant_data.core.models import RunOptions
from quant_data.datasets.bse_mapping import BseMappingDataset
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.suspend_daily import SuspendDailyDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.local.stock_1min import LocalParquetStock1MinAdapter

DAY = "2020-03-12"
DAY_NUMBER = "20200312"
SOURCE_SCHEMA = pl.Schema({
    "code": pl.String, "trade_time": pl.String, "date": pl.String,
    "open": pl.Float32, "high": pl.Float32, "low": pl.Float32, "close": pl.Float32,
    "vol": pl.Float32, "amount": pl.Float32,
    "pre_close": pl.Float32, "change": pl.Float32, "pct_chg": pl.Float32,
})


def source_row(code, label, *, day=DAY_NUMBER, volume=100, amount=1000,
               open=10.0, high=10.0, low=10.0, close=10.0):
    return {
        "code": code, "trade_time": label, "date": day,
        "open": open, "high": high, "low": low, "close": close,
        "vol": volume, "amount": amount,
        "pre_close": 9.5, "change": 0.5, "pct_chg": 5.0,
    }


class LocalStock1MinTest(unittest.TestCase):
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
        self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,true,NULL)", [DAY])
        self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar',?,'PUBLISHED',1,current_timestamp,NULL)",
                         [f"SSE:{DAY}"])
        for dataset in ("suspend_daily", "stock_daily"):
            self.con.execute("INSERT INTO meta.coverage VALUES (?,?,'PUBLISHED',1,current_timestamp,NULL)",
                             [dataset, DAY])
        for code in ("000001.SZ", "000002.SZ"):
            self.con.execute("""
                INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
                VALUES (?,?,?,'SZSE','L','1991-04-03')
            """, [code, code.split(".")[0], code])
        self.con.execute("INSERT INTO meta.coverage VALUES ('stock_basic','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
        self.con.execute("INSERT INTO canonical.bse_mapping VALUES ('映射','832000.BJ','920000.BJ','2017-03-29')")
        self.con.execute("INSERT INTO meta.coverage VALUES ('bse_mapping','SNAPSHOT','PUBLISHED',1,current_timestamp,NULL)")
        self.con.execute("INSERT INTO canonical.suspend_daily VALUES ('000002.SZ',?,'S',NULL)", [DAY])
        self.con.execute("INSERT INTO canonical.stock_daily VALUES ('000001.SZ',?,10,11,9,10.5,9.5,1,10,10,100)", [DAY])
        self.dataset = self.datasets[-1]

    def write_source(self, rows, day=DAY_NUMBER):
        path = self.root / f"{day}.parquet"
        pl.DataFrame(rows, schema=SOURCE_SCHEMA, infer_schema_length=None).write_parquet(path)
        return path

    @staticmethod
    def normal_rows():
        return [
            source_row("000001.SZ", "2020-03-12 09:30:00", volume=200, amount=2000),
            source_row("000001.SZ", "09:31", volume=100, amount=1050,
                       open=10.0, high=11.0, low=9.0, close=10.5),
            source_row("000001.SZ", "09:32", volume=0, amount=0, close=10.5),
            source_row("000001.SZ", "15:00", volume=100, amount=1000),
            source_row("000002.SZ", "09:30", volume=0, amount=0),
        ]

    def ingest(self, rows=None, *, day=DAY_NUMBER):
        path = self.write_source(self.normal_rows() if rows is None else rows, day)
        source = LocalParquetStock1MinAdapter.inspect(path)
        adapter = LocalParquetStock1MinAdapter(source)
        params = {"start_date": day, "end_date": day, "source": source}
        return runner.run(self.con, self.dataset, adapter, params, RunOptions(self.root / "landing"))

    def rules(self, result, level=None):
        query = "SELECT rule FROM meta.validation_check WHERE run_id=?"
        params = [result.run_id]
        if level:
            query += " AND level=?"
            params.append(level)
        return {r[0] for r in self.con.execute(query, params).fetchall()}

    def use_bse_universe(self):
        day = "2024-04-17"
        self.con.execute("UPDATE canonical.trade_calendar SET cal_date=?", [day])
        self.con.execute("UPDATE meta.coverage SET unit_key='SSE:2024-04-17' WHERE dataset='trade_calendar'")
        self.con.execute("UPDATE meta.coverage SET unit_key='2024-04-17' WHERE dataset IN ('suspend_daily','stock_daily')")
        self.con.execute("DELETE FROM canonical.stock_daily")
        self.con.execute("DELETE FROM canonical.suspend_daily")
        self.con.execute("DELETE FROM canonical.stock_basic")
        self.con.execute("""
            INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
            VALUES ('920000.BJ','920000','映射','BSE','L','2017-03-29')
        """)
        self.con.execute("INSERT INTO canonical.stock_daily VALUES ('920000.BJ',?,10,11,9,10.5,9.5,1,10,10,100)", [day])
        return "20240417"

    def test_complete_grid_auction_and_existing_meta_lineage(self):
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result.error)
        self.assertEqual(len(result.publications), 2)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 481)
        rows = self.con.execute("""
            SELECT bar_time,status,open,high,low,close,volume,amount
            FROM canonical.stock_1min WHERE ts_code='000001.SZ' AND trade_date=?
              AND bar_time IN ('09:25','09:30','09:31','14:59') ORDER BY bar_time
        """, [DAY]).fetchall()
        self.assertEqual([r[1] for r in rows], ["TRADED", "TRADED", "NO_TRADE", "TRADED"])
        self.assertEqual(rows[0][6:8], (200, 2000.0))
        self.assertEqual(rows[1][2:8], (10.0, 11.0, 9.0, 10.5, 100, 1050.0))
        self.assertEqual(rows[2][2], 10.5)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.request_attempt WHERE run_id=?",
                                          [result.run_id]).fetchone()[0], 0)
        params = json.loads(self.con.execute("SELECT params FROM meta.run WHERE run_id=?",
                                             [result.run_id]).fetchone()[0])
        self.assertEqual(params["source"]["mapping_profile"], "NETDISK_END_MINUTE_V1")
        self.assertEqual(params["source"]["row_count"], 5)
        self.assertEqual(self.con.execute("""
            SELECT count(*) FROM meta.coverage c JOIN meta.publication p
              ON c.publication_id=p.publication_id
            WHERE c.dataset='stock_1min' AND p.run_id=?
        """, [result.run_id]).fetchone()[0], 2)

    def test_zero_auction_and_rerun_do_not_accumulate(self):
        rows = self.normal_rows()
        rows[0] = source_row("000001.SZ", "09:30", volume=0, amount=0)
        first = self.ingest(rows)
        self.assertEqual(first.status, "SUCCESS", first.error)
        self.assertIn("local.zero_auction", self.rules(first, "WARNING"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min WHERE bar_time='09:25'").fetchone()[0], 0)
        before = self.con.execute("SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall()
        second = self.ingest(rows)
        self.assertEqual(second.status, "SUCCESS", second.error)
        self.assertEqual(before, self.con.execute(
            "SELECT * FROM canonical.stock_1min ORDER BY ts_code,trade_date,bar_time").fetchall())
        self.assertGreater(min(p.publication_id for p in second.publications),
                           max(p.publication_id for p in first.publications))

    def test_missing_auction_blocks_and_preserves_previous_unit(self):
        first = self.ingest()
        before = self.con.execute("SELECT * FROM canonical.stock_1min WHERE ts_code='000001.SZ' ORDER BY bar_time").fetchall()
        old_publication = self.con.execute("SELECT publication_id FROM meta.coverage WHERE dataset='stock_1min' AND unit_key='000001.SZ:2020-03-12'").fetchone()[0]
        rows = [r for r in self.normal_rows() if not (r["code"] == "000001.SZ" and r["trade_time"].endswith("09:30:00"))]
        second = self.ingest(rows)
        self.assertEqual(second.status, "PARTIAL")
        self.assertIn("local.auction_not_reported", self.rules(second, "ERROR"))
        self.assertEqual(before, self.con.execute(
            "SELECT * FROM canonical.stock_1min WHERE ts_code='000001.SZ' ORDER BY bar_time").fetchall())
        self.assertEqual(old_publication, self.con.execute(
            "SELECT publication_id FROM meta.coverage WHERE dataset='stock_1min' AND unit_key='000001.SZ:2020-03-12'").fetchone()[0])

    def test_full_suspension_positive_source_is_rejected(self):
        rows = self.normal_rows()
        rows[-1] = source_row("000002.SZ", "09:31", volume=100, amount=1000)
        result = self.ingest(rows)
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("local.full_suspension_conflict", self.rules(result, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000002.SZ'").fetchone()[0], 0)

    def test_bse_current_code_close_label_and_post_close_exclusion(self):
        day = self.use_bse_universe()
        rows = [
            source_row("920000.BJ", "09:30", day=day, volume=200, amount=2000),
            source_row("920000.BJ", "09:31", day=day, volume=100, amount=1000),
            source_row("920000.BJ", "15:00", day=day, volume=0, amount=0),
            source_row("920000.BJ", "15:01", day=day, volume=300, amount=3000, close=10.0),
            source_row("920000.BJ", "15:30", day=day, volume=100, amount=385,
                       open=5.75, high=5.75, low=5.75, close=5.75),
        ]
        result = self.ingest(rows, day=day)
        self.assertEqual(result.status, "SUCCESS", result.error)
        self.assertIn("local.post_close_excluded", self.rules(result, "WARNING"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 243)
        self.assertEqual(self.con.execute("SELECT DISTINCT ts_code FROM canonical.stock_1min").fetchall(), [("920000.BJ",)])
        closing = self.con.execute("""
            SELECT status,volume,amount FROM canonical.stock_1min WHERE trade_date='2024-04-17' AND bar_time='15:00'
        """).fetchone()
        self.assertEqual(closing, ("TRADED", 300, 3000.0))
        generated = self.con.execute("""
            SELECT status,count(*) FROM canonical.stock_1min
            WHERE trade_date='2024-04-17' AND bar_time IN ('11:30','14:59') GROUP BY status
        """).fetchall()
        self.assertEqual(generated, [("NO_TRADE", 2)])

    def test_bse_multiple_positive_close_rows_block_unit(self):
        day = self.use_bse_universe()
        rows = [source_row("920000.BJ", "09:30", day=day), source_row("920000.BJ", "09:31", day=day),
                source_row("920000.BJ", "15:00", day=day), source_row("920000.BJ", "15:01", day=day)]
        result = self.ingest(rows, day=day)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.close_conflict", self.rules(result, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 0)

    def test_invalid_volume_amount_combination_blocks_unit(self):
        rows = self.normal_rows()
        rows[1] = source_row("000001.SZ", "09:31", volume=0, amount=1050)
        result = self.ingest(rows)
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("local.volume_amount", self.rules(result, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000001.SZ'").fetchone()[0], 0)

    def test_duplicate_zero_source_key_is_not_silently_ignored(self):
        rows = self.normal_rows()
        rows.append(dict(rows[2]))
        result = self.ingest(rows)
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("local.source_key_unique", self.rules(result, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000001.SZ'").fetchone()[0], 0)

    def test_bse_old_source_code_maps_to_current_code(self):
        day = self.use_bse_universe()
        rows = [source_row("832000.BJ", label, day=day) for label in ("09:30", "09:31", "15:00")]
        result = self.ingest(rows, day=day)
        self.assertEqual(result.status, "SUCCESS", result.error)
        self.assertEqual(self.con.execute("SELECT DISTINCT ts_code FROM canonical.stock_1min").fetchall(),
                         [("920000.BJ",)])
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 243)

    def test_bse_old_and_new_codes_together_block_unit(self):
        day = self.use_bse_universe()
        rows = [source_row("920000.BJ", "09:30", day=day), source_row("920000.BJ", "09:31", day=day),
                source_row("920000.BJ", "15:00", day=day), source_row("832000.BJ", "10:00", day=day)]
        result = self.ingest(rows, day=day)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.code_era_conflict", self.rules(result, "ERROR"))

    def test_bse_zero_close_differs_from_close_not_reported(self):
        day = self.use_bse_universe()
        rows = [source_row("920000.BJ", "09:30", day=day), source_row("920000.BJ", "09:31", day=day)]
        rows.extend(source_row("920000.BJ", label, day=day, volume=0, amount=0)
                    for label in ("15:00", "15:01", "15:02"))
        first = self.ingest(rows, day=day)
        self.assertEqual(first.status, "SUCCESS", first.error)
        self.assertIn("local.zero_close_auction", self.rules(first, "WARNING"))
        self.assertEqual(self.con.execute("""
            SELECT status FROM canonical.stock_1min WHERE trade_date='2024-04-17' AND bar_time='15:00'
        """).fetchone()[0], "NO_TRADE")
        second = self.ingest(rows[:2], day=day)
        self.assertEqual(second.status, "FAILED")
        self.assertIn("local.close_not_reported", self.rules(second, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 243)

    def test_known_not_listed_zero_rows_are_explicitly_excluded(self):
        self.con.execute("""
            INSERT INTO canonical.stock_basic(ts_code,symbol,name,exchange,list_status,list_date)
            VALUES ('000003.SZ','000003','未来上市','SZSE','L','2021-01-01')
        """)
        rows = self.normal_rows() + [source_row("000003.SZ", "09:30", volume=0, amount=0)]
        result = self.ingest(rows)
        self.assertEqual(result.status, "SUCCESS", result.error)
        self.assertIn("local.out_of_scope_zero", self.rules(result, "WARNING"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min WHERE ts_code='000003.SZ'").fetchone()[0], 0)

    def test_unresolved_positive_or_zero_code_cannot_be_silently_excluded(self):
        rows = self.normal_rows() + [source_row("000003.SZ", "10:00")]
        result = self.ingest(rows)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.scope_conflict", self.rules(result, "ERROR"))
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.stock_1min").fetchone()[0], 0)
        rows = self.normal_rows() + [source_row("000003.SZ", "10:00", volume=0, amount=0)]
        result = self.ingest(rows)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.scope_conflict", self.rules(result, "ERROR"))

    def test_row_accounting_detects_duplicate_classification(self):
        path = self.write_source(self.normal_rows())
        source = LocalParquetStock1MinAdapter.inspect(path)
        adapter = LocalParquetStock1MinAdapter(source)
        original = adapter._classify

        def duplicate(con, scope, full_suspended, target_day):
            original(con, scope, full_suspended, target_day)
            con.execute("INSERT INTO _local_classified SELECT * FROM _local_classified LIMIT 1")

        params = {"start_date": DAY_NUMBER, "end_date": DAY_NUMBER, "source": source}
        with patch.object(adapter, "_classify", side_effect=duplicate):
            result = runner.run(self.con, self.dataset, adapter, params, RunOptions(self.root / "landing"))
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.row_accounting", self.rules(result, "ERROR"))

    def test_traded_equivalence_detects_candidate_loss(self):
        from quant_data.sources.local import stock_1min as local_module
        original = local_module.build_candidate

        def drop_traded(con, full_suspended):
            frame = original(con, full_suspended)
            return frame.filter(~((pl.col("ts_code") == "000001.SZ") & (pl.col("bar_time") == time(14, 59))))

        with patch.object(local_module, "build_candidate", side_effect=drop_traded):
            result = self.ingest()
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("local.traded_equivalence", self.rules(result, "ERROR"))

    def test_hash_change_and_wrong_internal_date_are_blocked(self):
        path = self.write_source(self.normal_rows())
        source = LocalParquetStock1MinAdapter.inspect(path)
        self.write_source(self.normal_rows() + [source_row("000001.SZ", "10:00")])
        adapter = LocalParquetStock1MinAdapter(source)
        params = {"start_date": DAY_NUMBER, "end_date": DAY_NUMBER, "source": source}
        changed = runner.run(self.con, self.dataset, adapter, params, RunOptions(self.root / "landing"))
        self.assertEqual(changed.status, "FAILED")
        self.assertIn("local.input", self.rules(changed, "ERROR"))
        wrong_date = [dict(r, date="20200313") for r in self.normal_rows()]
        result = self.ingest(wrong_date)
        self.assertEqual(result.status, "FAILED")
        self.assertIn("local.date", self.rules(result, "ERROR"))
        wrong_timestamp = self.normal_rows()
        wrong_timestamp[0]["trade_time"] = "2020-03-13 09:30:00"
        result = self.ingest(wrong_timestamp)
        self.assertEqual(result.status, "PARTIAL")
        self.assertIn("local.date", self.rules(result, "ERROR"))

    def test_cli_local_source_needs_no_supplier_credentials(self):
        from quant_data.__main__ import main
        path = self.write_source(self.normal_rows())
        argv = ["quant_data", "ingest", "stock_1min", "--db", str(self.root / "warehouse.duckdb"),
                "--landing-dir", str(self.root / "cli-landing"), "--start-date", DAY_NUMBER,
                "--end-date", DAY_NUMBER, "--source-parquet", str(path)]
        output = io.StringIO()
        with patch("sys.argv", argv), patch("quant_data.__main__.dotenv_values", return_value={}), \
             patch("quant_data.__main__.GalaxyClient") as galaxy, redirect_stdout(output):
            self.assertEqual(main(), 0)
        galaxy.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS")


if __name__ == "__main__":
    unittest.main()
