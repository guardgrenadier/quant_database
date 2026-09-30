"""验证 dbt 行级结果按检查与证券聚合且样例有界。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.finding_store import record_finding_rows
from quant_ingest.ledger import Ledger


class FindingStoreTest(unittest.TestCase):
    """同检查的不同证券分行落账，dataset 级代码保持 NULL。"""

    def test_per_code_detail_and_three_samples(self):
        """异常计数完整保留，样例最多三条，重放同 run 幂等。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root / "state.duckdb")
            rows = [
                {"check_name": "negative_value", "ts_code": "000001.SZ",
                 "trade_time": f"2026-09-18 09:3{i}:00", "detail": "negative vol"}
                for i in range(4)
            ]
            rows += [
                {"check_name": "negative_value", "ts_code": "600000.SH",
                 "trade_time": "2026-09-18 09:31:00", "detail": "negative amount"},
                {"check_name": "row_reconciliation", "ts_code": None,
                 "trade_time": None, "detail": "candidate count mismatch"},
            ]
            path = root / "findings.parquet"
            pq.write_table(pa.Table.from_pylist(rows), path)
            for _ in range(2):
                self.assertEqual(record_finding_rows(ledger, "run", "stk_mins", date(2026, 9, 18),
                                                     path, "warn"), 6)
            with duckdb.connect(str(ledger.database)) as conn:
                found = conn.execute(
                    """select check_name, ts_code, finding_count, details_json, samples_json
                       from quality_findings order by check_name, ts_code"""
                ).fetchall()
            self.assertEqual(len(found), 3)
            first = next(row for row in found if row[1] == "000001.SZ")
            self.assertEqual(first[2], 4)
            self.assertEqual(len(json.loads(first[4])), 3)
            self.assertEqual(len(json.loads(first[3])["bar_times"]), 4)
            self.assertIsNone(next(row for row in found if row[0] == "row_reconciliation")[1])


if __name__ == "__main__":
    unittest.main()
