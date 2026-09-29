"""验证隔离证据在重跑自愈与坠机恢复后仍进入长期质量账。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

import duckdb

from quant_ingest.ledger import Ledger
from quant_ingest.quarantine_findings import record_quarantine_self_healed
from quant_ingest.raw import RawStore


class QuarantineFindingTest(unittest.TestCase):
    """证据路径与原违约原因保留，重复发布不会重复计入。"""

    def test_self_healed_slice_is_recorded_once(self):
        """切片先隔离、再成功，形成包含代码、日期和证据路径的 warn。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2026, 9, 18)
            code = "000001.SZ"
            ledger = Ledger(root / "state.duckdb")
            raw = RawStore(root / "raw" / "v2")
            ledger.plan("stk_mins", day, {}, {code: {}})
            evidence = raw.quarantine("run-000001_SZ", "stk_mins", day, b'{"code":0}',
                                      slice_key=code, reason="minute key has conflicting values")
            ledger.record("stk_mins", day, code, "quarantined")
            self.assertEqual(record_quarantine_self_healed(ledger, raw, "stk_mins", day), 0)
            artifact = raw.write("stk_mins", day, code, [b'{"code":0,"data":{"items":[]}}'])
            ledger.record("stk_mins", day, code, "empty", artifact=artifact, row_count=0)
            self.assertEqual(record_quarantine_self_healed(ledger, raw, "stk_mins", day), 1)
            self.assertEqual(record_quarantine_self_healed(ledger, raw, "stk_mins", day), 1)
            with duckdb.connect(str(ledger.database)) as conn:
                rows = conn.execute(
                    """select severity, finding_count, samples_json from quality_findings
                       where check_name = 'quarantine_self_healed'"""
                ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][:2], ("warn", 1))
            sample = json.loads(rows[0][2])[0]
            self.assertEqual((sample["ts_code"], sample["trade_date"]), (code, day.isoformat()))
            self.assertEqual(sample["reason"], "minute key has conflicting values")
            self.assertEqual(sample["evidence_path"], str(evidence))

    def test_evidence_survives_crash_before_slice_ledger_update(self):
        """证据先落盘、状态尚未写 quarantined 时，自愈发布仍能发现它。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2026, 9, 18)
            ledger = Ledger(root / "state.duckdb")
            raw = RawStore(root / "raw" / "v2")
            ledger.plan("stock_basic", day, {}, {"day": {}})
            raw.quarantine("orphan", "stock_basic", day, b'{"code":1}',
                           slice_key="day", reason="supplier response code is not zero")
            artifact = raw.write("stock_basic", day, "day", [b'{"code":0,"data":{"items":[]}}'])
            ledger.record("stock_basic", day, "day", "empty", artifact=artifact, row_count=0)
            self.assertEqual(record_quarantine_self_healed(ledger, raw, "stock_basic", day), 1)


if __name__ == "__main__":
    unittest.main()
