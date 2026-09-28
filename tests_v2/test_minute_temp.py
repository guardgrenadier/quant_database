"""验证分钟 raw 到临时 Parquet 的无损读取与去重边界。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

from quant_ingest.ledger import Ledger
from quant_ingest.minute_source import FIELDS
from quant_ingest.minute_temp import build_minute_temp
from quant_ingest.raw import RawStore


class MinuteTempTest(unittest.TestCase):
    """覆盖源响应原文保留与 temp 中相同重复行去重。"""

    def test_identical_duplicate_is_removed_only_in_temp(self):
        """raw 两行原样保留，temp 一行且金额浮点文本未丢失。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2026, 9, 18)
            code = "000001.SZ"
            ledger = Ledger(root / "state.duckdb")
            raw = RawStore(root / "raw")
            ledger.plan("stk_mins", day, {"freq": "1min"}, {code: {"ts_code": code}})
            row = [code, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 55929077.10000002]
            body = json.dumps({"code": 0, "data": {"fields": FIELDS.split(","), "items": [row, row]}}).encode()
            artifact = raw.write("stk_mins", day, code, [body])
            ledger.record("stk_mins", day, code, "ok", artifact=artifact, row_count=2,
                          extracted_at=datetime(2026, 9, 18, 10, tzinfo=timezone.utc))
            temp = build_minute_temp(ledger, day, root / "work")
            self.assertEqual((temp.raw_rows, temp.unique_rows), (2, 1))
            self.assertEqual(pq.read_table(temp.data_path).num_rows, 1)
            self.assertEqual(pq.read_table(temp.data_path)["amount"][0].as_py(), "55929077.10000002")
            self.assertIn(b"55929077.10000002", body)

    def test_same_key_with_different_number_representation_is_rejected(self):
        """主键相同但源 JSON 数字表示不同，不当作完全相同的重复行。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            day = date(2026, 9, 18)
            code = "000001.SZ"
            ledger = Ledger(root / "state.duckdb")
            raw = RawStore(root / "raw")
            ledger.plan("stk_mins", day, {"freq": "1min"}, {code: {"ts_code": code}})
            row = [code, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 10]
            body = json.dumps({"code": 0, "data": {"fields": FIELDS.split(","),
                                                    "items": [row, [*row[:-1], 10.0]]}}).encode()
            artifact = raw.write("stk_mins", day, code, [body])
            ledger.record("stk_mins", day, code, "ok", artifact=artifact, row_count=2)
            with self.assertRaisesRegex(ValueError, "conflicting"):
                build_minute_temp(ledger, day, root / "work")


if __name__ == "__main__":
    unittest.main()
