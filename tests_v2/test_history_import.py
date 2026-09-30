"""用小型网盘文件验证历史 temp 映射与入场护栏。"""

from __future__ import annotations

import json
import hashlib
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.history_import import history_path, import_history_day, prepare_history_temp, verify_trust_report
from quant_ingest.history_trust import build_trust_report
from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore
from tests_v2.test_minute_pipeline import _result


DAY = date(2026, 4, 17)


def _source(root: Path) -> Path:
    """生成两根紧窗 bar 与一根盘后 bar 的网盘日文件。"""

    path = history_path(root, DAY)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {"code": "000001.SZ", "trade_time": f"{DAY} 09:30:00", "date": "20260417",
         "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "vol": 100.0, "amount": 1000.0},
        {"code": "000001.SZ", "trade_time": f"{DAY} 09:31:00", "date": "20260417",
         "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "vol": 200.0, "amount": 2000.0},
        {"code": "000001.SZ", "trade_time": f"{DAY} 15:30:00", "date": "20260417",
         "open": 10.0, "high": 10.0, "low": 10.0, "close": 10.0, "vol": 0.0, "amount": 0.0},
    ]
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


class HistoryImportTest(unittest.TestCase):
    """历史文件只读，紧窗裁剪发生在 temp 入场。"""

    def test_temp_mapping_and_window_trim(self):
        """映射 code、补 freq、空 extracted_at，盘后行不进入候选。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = _source(root / "history")
            temp, inspection = prepare_history_temp(source, DAY, root / "work")
            self.assertEqual((inspection.source_rows, inspection.selected_rows, inspection.outside_rows), (3, 2, 1))
            with duckdb.connect() as conn:
                rows = conn.execute(
                    "select ts_code, trade_time, freq, vol, extracted_at from read_parquet(?) order by trade_time",
                    [str(temp.data_path)],
                ).fetchall()
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0], ("000001.SZ", "2026-04-17 09:30:00", "1MIN", "100", None))
            self.assertTrue(source.is_file())

    def test_trust_report_requires_both_checks(self):
        """缺少 API 重叠抽样时只能 dry-run，不能批量入库。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = root / "report.json"
            report.write_text(json.dumps({"source_root": str(root), "coverage_checked": True,
                                          "trust_level": "sampled_match"}))
            with self.assertRaisesRegex(ValueError, "API overlap"):
                verify_trust_report(report, root)

    def test_low_trust_requires_one_day_pilot(self):
        """低信任报告阻断批量，保留人工抽查的单日试点入口。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            snapshot = root / "universe.parquet"
            snapshot.write_bytes(b"unchanged")
            report_path = root / "report.json"
            report = {
                "source_root": str(root), "coverage_checked": True,
                "api_overlap_checked": False, "trust_level": "low_or_pending",
                "coverage": {"universe_path": str(snapshot),
                             "universe_sha256": hashlib.sha256(snapshot.read_bytes()).hexdigest()},
            }
            report_path.write_text(json.dumps(report))
            with self.assertRaisesRegex(ValueError, "wait for pending API data"):
                verify_trust_report(report_path, root)
            with self.assertRaisesRegex(ValueError, "--limit-days 1"):
                verify_trust_report(report_path, root, pilot_days=2)
            self.assertEqual(verify_trust_report(report_path, root, pilot_days=1)["trust_level"],
                             "low_or_pending")

    def test_historical_day_uses_same_dbt_and_records_netdisk_source(self):
        """网盘文件跳过 API raw，但经过相同 dbt 候选与发布机制。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "history"
            source = _source(source_root)
            warehouse = root / "warehouse"
            ledger = Ledger(warehouse / "state.duckdb")
            universe = root / "universe.parquet"
            pq.write_table(pa.Table.from_pylist([
                {"ts_code": code, "list_date": date(1991, 4, 3),
                 "delist_date": None, "list_status": "L"}
                for code in ("000001.SZ", "000002.SZ")
            ]), universe)
            target = import_history_day(ledger, RawStore(root / "raw" / "v2"),
                                        warehouse, source_root, DAY, universe_path=universe)
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                rows = conn.execute(
                    "select count(*), count(*) filter (where extracted_at is null) from read_parquet(?)",
                    [str(target)],
                ).fetchone()
                origin = conn.execute(
                    "select source from publications where dataset = 'stk_mins' and trade_date = ?",
                    [DAY],
                ).fetchone()[0]
                coverage = conn.execute(
                    """select finding_count from quality_findings
                       where check_name = 'historical_universe_coverage'"""
                ).fetchone()
            self.assertEqual(rows, (2, 2))
            self.assertEqual(origin, "netdisk")
            self.assertEqual(coverage, (1,))
            self.assertTrue(source.is_file())

    def test_overlap_and_d6_coverage_report(self):
        """抽样 API 与源文件对齐，并用已发布快照计算证券覆盖。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source_root = root / "history"
            _source(source_root)
            ledger = Ledger(root / "state.duckdb")
            snapshot = root / "stock_basic.parquet"
            pq.write_table(pa.Table.from_pylist([{
                "ts_code": "000001.SZ", "list_date": date(1991, 4, 3),
                "delist_date": None, "list_status": "L",
            }]), snapshot)
            with duckdb.connect(str(ledger.database)) as conn:
                conn.execute(
                    """insert into publications (dataset, trade_date, stage, canonical_path, canonical_sha256)
                       values ('stock_basic', ?, 'published', ?, ?)""",
                    [DAY, str(snapshot), hashlib.sha256(snapshot.read_bytes()).hexdigest()],
                )
            class Source:
                """返回与网盘紧窗样本一致的两根分钟 bar。"""

                def fetch_slice(self, code, day):
                    """模拟一次已收敛的 API 单代码请求。"""

                    rows = [
                        [code, f"{day} 09:25:00", "1MIN", 10, 10, 10, 10, 100, 1000],
                        [code, f"{day} 09:30:00", "1MIN", 10, 10, 10, 10, 100, 1000],
                        [code, f"{day} 09:31:00", "1MIN", 10, 10, 10, 10, 200, 2000],
                        [code, f"{day} 15:30:00", "1MIN", 10, 10, 10, 10, 0, 0],
                    ]
                    return _result(code, rows)
            report = build_trust_report(ledger, source_root, DAY, ["000001.SZ"], Source())
            self.assertEqual(report["trust_level"], "sampled_match")
            self.assertTrue(report["api_overlap_checked"])
            self.assertTrue(report["coverage_checked"])
            self.assertEqual(report["coverage"]["missing_count"], 0)
            path = root / "trust.json"
            path.write_text(json.dumps(report))
            self.assertEqual(verify_trust_report(path, source_root)["trust_level"], "sampled_match")
            snapshot.unlink()
            with self.assertRaisesRegex(ValueError, "snapshot is missing"):
                verify_trust_report(path, source_root)


if __name__ == "__main__":
    unittest.main()
