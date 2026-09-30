"""验证 D23 三种观察模式在 dbt 中按日×证券产生 warn。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.ledger import Ledger
from quant_ingest.minute_candidate import MinuteCandidate
from quant_ingest.minute_quality import record_minute_quality
from quant_ingest.minute_temp import MinuteTemp
from quant_ingest.raw import RawStore


class ObservationWarningTest(unittest.TestCase):
    """观察事实不改变 canonical 行，也不阻断 dbt warning 流程。"""

    def test_close_only_boundary_and_out_of_core_details(self):
        """空字段和时刻清单按股票分别写入长期账。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2026, 9, 18)
            code, other = "000001.SZ", "600000.SH"
            stamp = datetime(2026, 9, 18, 10, tzinfo=timezone.utc)
            def bar(code: str, time: str, **changes) -> dict:
                """构造一个可指定缺失字段的候选 bar。"""

                value = {"ts_code": code, "trade_time": datetime.fromisoformat(f"{day} {time}"),
                         "trade_date": day, "open": 10.0, "high": 10.0,
                         "low": 10.0, "close": 10.0, "vol": 100, "amount": 1000,
                         "extracted_at": stamp}
                value.update(changes)
                return value
            bars = [
                bar(code, "09:30:00", open=None, high=None, low=None, vol=None, amount=None),
                bar(code, "15:00:00", high=None),
                bar(code, "12:00:00"), bar(code, "09:25:00"), bar(code, "15:30:00"),
                bar(other, "12:30:00"),
            ]
            candidate_path = root / "work" / "candidate.parquet"
            candidate_path.parent.mkdir()
            pq.write_table(pa.Table.from_pylist(bars), candidate_path)
            temp_path = root / "work" / "temp.parquet"
            pq.write_table(pa.Table.from_pylist([
                {"ts_code": item["ts_code"], "trade_time": item["trade_time"].strftime("%Y-%m-%d %H:%M:%S"),
                 "amount": None if item["amount"] is None else str(item["amount"])}
                for item in bars
            ]), temp_path)
            plan_path = root / "work" / "plan.parquet"
            pq.write_table(pa.Table.from_pylist([{"ts_code": name, "trade_date": day}
                                                   for name in (code, other)]), plan_path)
            ledger = Ledger(root / "warehouse" / "state.duckdb")
            raw = RawStore(root / "raw")
            candidate = MinuteCandidate(candidate_path, MinuteTemp(temp_path, plan_path, 6, 6))
            record_minute_quality(ledger, raw, root / "warehouse", day, "observe", candidate)
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                rows = conn.execute(
                    """select check_name, ts_code, severity, finding_count, details_json
                       from quality_findings where check_name in
                       ('close_only_bars', 'boundary_bar_incomplete', 'out_of_core_bars')"""
                ).fetchall()
            found = {(check, secu): (severity, count, json.loads(details))
                     for check, secu, severity, count, details in rows}
            self.assertEqual(found[("close_only_bars", code)][:2], ("warn", 1))
            self.assertEqual(found[("close_only_bars", code)][2]["null_fields"],
                             ["amount", "high", "low", "open", "vol"])
            self.assertEqual(found[("boundary_bar_incomplete", code)][1], 2)
            self.assertEqual(found[("boundary_bar_incomplete", code)][2]["bar_times"],
                             ["09:30:00", "15:00:00"])
            self.assertEqual(found[("out_of_core_bars", code)][1], 3)
            self.assertEqual(found[("out_of_core_bars", other)][1], 1)


if __name__ == "__main__":
    unittest.main()
