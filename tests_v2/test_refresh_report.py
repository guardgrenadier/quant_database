"""验证观察期报告只统计已发布完成的 T+1 日期。"""

from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from quant_ingest.ledger import Ledger
from quant_ingest.refresh_report import summarize_refresh


class RefreshReportTest(unittest.TestCase):
    """按日期计算发生率，不把未完成刷新混入分母。"""

    def test_changed_day_rate(self):
        """两个完成日中只有一个有修订，发生率为 50%。"""

        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / "state.duckdb")
            for day, status, changes in ((date(2026, 9, 18), "refreshed", 3),
                                         (date(2026, 9, 19), "refreshed", 0),
                                         (date(2026, 9, 20), "refreshing", 9)):
                with duckdb.connect(str(ledger.database)) as conn:
                    conn.execute(
                        """insert into plans (dataset, trade_date, parameters_json, status)
                           values ('stk_mins', ?, '{}', ?)""", [day, status]
                    )
                ledger.record_finding(f"run-{day}", "stk_mins", day, "revision_diff", "info", changes, [])
            report = summarize_refresh(ledger, date(2026, 9, 1))
            self.assertEqual(report["observed_days"], 2)
            self.assertEqual(report["changed_days"], 1)
            self.assertEqual(report["changed_day_rate"], 0.5)


if __name__ == "__main__":
    unittest.main()
