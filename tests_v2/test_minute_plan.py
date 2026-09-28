"""离线验证 D6 股票集合、闭市跳过和固定计划续跑。"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.ledger import Ledger
from quant_ingest.minute_plan import plan_day


DAY = date(2026, 9, 18)


def _publish_fixture(ledger: Ledger, root: Path, dataset: str, rows: list[dict]) -> Path:
    """构造已发布的基础数据集文件与匹配的状态账。"""

    path = root / "canonical" / dataset / "date=20260918.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    checksum = hashlib.sha256(path.read_bytes()).hexdigest()
    with duckdb.connect(str(ledger.database)) as conn:
        conn.execute(
            """insert into publications (dataset, trade_date, stage, canonical_path, canonical_sha256)
               values (?, ?, 'published', ?, ?)""", [dataset, DAY, str(path), checksum]
        )
    return path


class MinutePlanTest(unittest.TestCase):
    """检查 universe 日期左右边界和当前 P 状态排除规则。"""

    def test_membership_boundaries_and_resume_do_not_drift(self):
        """IPO 当日可入选，摘牌日与 P 被排除，续跑不重新解释快照。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root / "state.duckdb")
            _publish_fixture(ledger, root, "trade_cal", [
                {"exchange": "SSE", "cal_date": DAY, "is_open": 1},
            ])
            _publish_fixture(ledger, root, "stock_basic", [
                {"ts_code": "000001.SZ", "list_date": DAY, "delist_date": None, "list_status": "L"},
                {"ts_code": "000002.SZ", "list_date": date(2000, 1, 1), "delist_date": DAY, "list_status": "D"},
                {"ts_code": "000003.SZ", "list_date": date(2000, 1, 1), "delist_date": None, "list_status": "P"},
                {"ts_code": "000004.SZ", "list_date": date(2000, 1, 1), "delist_date": date(2026, 9, 19), "list_status": "D"},
                {"ts_code": "000005.SZ", "list_date": date(2026, 9, 19), "delist_date": None, "list_status": "L"},
            ])
            params = lambda code, day: {"ts_code": code, "trade_date": day.isoformat()}
            self.assertEqual(plan_day(ledger, root, DAY, params), ["000001.SZ", "000004.SZ"])
            # 已固定计划优先于后来变化的证券快照。
            (root / "canonical" / "stock_basic" / "date=20260918.parquet").unlink()
            self.assertEqual(plan_day(ledger, root, DAY, params), ["000001.SZ", "000004.SZ"])

    def test_closed_day_produces_no_plan(self):
        """日历判为闭市时不调用证券请求计划。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root / "state.duckdb")
            _publish_fixture(ledger, root, "trade_cal", [
                {"exchange": "SSE", "cal_date": DAY, "is_open": 0},
            ])
            _publish_fixture(ledger, root, "stock_basic", [
                {"ts_code": "000001.SZ", "list_date": DAY, "delist_date": None, "list_status": "L"},
            ])
            self.assertEqual(plan_day(ledger, root, DAY, lambda code, day: {}), [])
            self.assertIsNone(ledger.saved_plan("stk_mins", DAY))


if __name__ == "__main__":
    unittest.main()
