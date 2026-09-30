"""验证 gap 使用当前快照规划并按连续交易日分段。"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.gap_backfill import latest_snapshot_day, next_gap_segment, open_gap_days
from quant_ingest.ledger import Ledger
from quant_ingest.minute_plan import plan_day
from quant_ingest.minute_source import TushareMinuteSource


SNAPSHOT = date(2026, 9, 29)


def _publication(ledger: Ledger, root: Path, dataset: str, rows: list[dict], day: date) -> None:
    """生成有正确哈希的同日基础快照。"""

    path = root / "canonical" / dataset / f"date={day:%Y%m%d}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    with duckdb.connect(str(ledger.database)) as conn:
        conn.execute(
            """insert into publications (dataset, trade_date, stage, canonical_path, canonical_sha256)
               values (?, ?, 'published', ?, ?)""",
            [dataset, day, str(path), hashlib.sha256(path.read_bytes()).hexdigest()],
        )


class GapBackfillTest(unittest.TestCase):
    """确认历史目标日无需伪造同日证券快照。"""

    def test_current_snapshot_and_bounded_contiguous_segment(self):
        """最新快照提供 D6 集合，已发布日切断下一段补抓。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root / "state.duckdb")
            days = [date(2026, 4, n) for n in (20, 21, 22, 23, 24)]
            _publication(ledger, root, "trade_cal", [
                {"exchange": "SSE", "cal_date": day, "is_open": 0 if day.day == 22 else 1}
                for day in days
            ], SNAPSHOT)
            _publication(ledger, root, "stock_basic", [
                {"ts_code": "000001.SZ", "list_date": date(1991, 4, 3),
                 "delist_date": None, "list_status": "L"},
            ], SNAPSHOT)
            self.assertEqual(latest_snapshot_day(ledger), SNAPSHOT)
            trading = open_gap_days(ledger, root, SNAPSHOT, days[0], days[-1])
            self.assertEqual(trading, [days[0], days[1], days[3], days[4]])
            with self.assertRaisesRegex(ValueError, "target year"):
                open_gap_days(ledger, root, SNAPSHOT, date(2025, 12, 31), days[-1])
            self.assertEqual(next_gap_segment(ledger, trading, 2), [days[0], days[1]])
            source = TushareMinuteSource(api_key="offline")
            self.assertEqual(plan_day(ledger, root, days[0], source.parameters, snapshot_day=SNAPSHOT),
                             ["000001.SZ"])
            planned = ledger.saved_plan("stk_mins", days[0])[1]["000001.SZ"]
            self.assertEqual(planned["start_date"], "2026-04-20 00:00:00")
            self.assertEqual(planned["end_date"], "2026-04-20 23:59:59")
            _publication(ledger, root, "stk_mins", [{"ts_code": "000001.SZ"}], days[3])
            self.assertEqual(next_gap_segment(ledger, trading, 7), [days[0], days[1]])


if __name__ == "__main__":
    unittest.main()
