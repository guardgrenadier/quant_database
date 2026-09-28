"""用离线 raw 验证 dbt 分钟候选转换与阻断级检查。"""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from quant_ingest.ledger import Ledger
from quant_ingest.minute_candidate import build_minute_candidate
from quant_ingest.minute_source import FIELDS
from quant_ingest.raw import RawStore


DAY = date(2026, 9, 18)
CODE = "000001.SZ"


def _setup(root: Path, rows: list[list]) -> tuple[Ledger, RawStore]:
    """把供应商原文和完成态切片账写入隔离目录。"""

    ledger = Ledger(root / "warehouse" / "state.duckdb")
    raw = RawStore(root / "raw")
    ledger.plan("stk_mins", DAY, {"freq": "1min"}, {CODE: {"ts_code": CODE}})
    body = json.dumps({"code": 0, "data": {"fields": FIELDS.split(","), "items": rows}}).encode()
    artifact = raw.write("stk_mins", DAY, CODE, [body])
    ledger.record("stk_mins", DAY, CODE, "ok" if rows else "empty", artifact=artifact,
                  row_count=len(rows), extracted_at=datetime(2026, 9, 18, 10, tzinfo=timezone.utc))
    ledger.mark_fetched("stk_mins", DAY)
    return ledger, raw


class MinuteCandidateTest(unittest.TestCase):
    """检查金额按分舍入及非法频率的发布阻断。"""

    def test_decimal_amount_to_cents_and_negative_source_is_allowed(self):
        """半分进一、浮点噪声不偏移，微小负额转换为零分但不阻断。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [
                [CODE, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 0.005],
                [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 100, 55929077.10000002],
                [CODE, "2026-09-18 09:32:00", "1MIN", 10, 10, 10, 10, -1, -0.001],
            ]
            ledger, raw = _setup(root, rows)
            candidate = build_minute_candidate(ledger, raw, root / "warehouse", DAY, "success")
            with duckdb.connect() as conn:
                amounts = [value[0] for value in conn.execute(
                    "select amount from read_parquet(?) order by trade_time", [str(candidate.path)]
                ).fetchall()]
            self.assertEqual(amounts, [1, 5592907710, 0])
            self.assertEqual(candidate.temp.unique_rows, 3)
            self.assertIsNone(ledger.publication("stk_mins", DAY))

    def test_invalid_frequency_blocks_candidate(self):
        """dbt 契约测试失败时抛异常，保留 raw 且不登记发布。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:30:00", "5MIN", 10, 10, 10, 10, 100, 1000]
            ledger, raw = _setup(root, [row])
            with self.assertRaisesRegex(RuntimeError, "dbt test failed"):
                build_minute_candidate(ledger, raw, root / "warehouse", DAY, "failure")
            self.assertIsNone(ledger.publication("stk_mins", DAY))
            self.assertTrue(raw.path("stk_mins", DAY, CODE).is_file())

    def test_amount_cent_overflow_blocks_candidate(self):
        """源成交额能解析但换算为分后超出 BIGINT 时不得发布。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 1e17]
            ledger, raw = _setup(root, [row])
            with self.assertRaisesRegex(RuntimeError, "dbt test failed"):
                build_minute_candidate(ledger, raw, root / "warehouse", DAY, "overflow")
            self.assertIsNone(ledger.publication("stk_mins", DAY))

    def test_all_empty_slices_produce_zero_row_candidate(self):
        """合法空切片经过 dbt 后仍得到类型明确的零行文件。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, raw = _setup(root, [])
            candidate = build_minute_candidate(ledger, raw, root / "warehouse", DAY, "empty")
            with duckdb.connect() as conn:
                count = conn.execute("select count(*) from read_parquet(?)", [str(candidate.path)]).fetchone()[0]
            self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main()
