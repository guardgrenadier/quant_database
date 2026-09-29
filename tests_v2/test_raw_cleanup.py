"""验证 30 天 raw 清理的发布、失败候选与隔离区护栏。"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore
from quant_ingest.raw_cleanup import cleanup_expired_raw


NOW = datetime(2026, 9, 29, tzinfo=timezone.utc)


def _day(ledger: Ledger, raw: RawStore, warehouse: Path, day: date, extracted_at: datetime,
         *, publish: bool = True) -> Path:
    """构造一日完成态 raw，按需登记对应的 canonical 发布。"""

    ledger.plan("stock_basic", day, {}, {"day": {}})
    artifact = raw.write("stock_basic", day, "day", [b'{"code":0,"data":{"items":[]}}'])
    ledger.record("stock_basic", day, "day", "empty", artifact=artifact, row_count=0,
                  extracted_at=extracted_at)
    ledger.mark_fetched("stock_basic", day)
    target = warehouse / "canonical" / "stock_basic" / f"date={day:%Y%m%d}.parquet"
    if publish:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"published-canonical")
        ledger.prepare_publish("stock_basic", day, target, hashlib.sha256(target.read_bytes()).hexdigest(), 0, "test")
        ledger.finish_publish("stock_basic", day)
    return artifact.path.parent


class RawCleanupTest(unittest.TestCase):
    """只允许删除与成功发布日配对的过期 V2 raw。"""

    def test_dry_run_then_delete_preserves_failure_and_quarantine(self):
        """预览无副作用；执行时只删过期成功日的 raw 与 JSONL。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            warehouse = root / "warehouse"
            raw = RawStore(root / "raw" / "v2")
            ledger = Ledger(warehouse / "state.duckdb")
            old = _day(ledger, raw, warehouse, date(2026, 8, 1),
                       datetime(2026, 8, 20, tzinfo=timezone.utc))
            raw.log("run", "stock_basic", date(2026, 8, 1), "test")
            recent = _day(ledger, raw, warehouse, date(2026, 9, 10),
                          datetime(2026, 9, 10, tzinfo=timezone.utc))
            failed = _day(ledger, raw, warehouse, date(2026, 8, 2),
                          datetime(2026, 8, 20, tzinfo=timezone.utc))
            candidate = warehouse / "candidates" / "stock_basic" / "20260802" / "failed-run"
            candidate.mkdir(parents=True)
            (candidate / "error.txt").write_text("dbt failed")
            unpublished = _day(ledger, raw, warehouse, date(2026, 8, 3),
                               datetime(2026, 8, 20, tzinfo=timezone.utc), publish=False)
            quarantine = raw.root.parent / "quarantine" / "evidence.response"
            quarantine.parent.mkdir(parents=True)
            quarantine.write_bytes(b"evidence")
            history = root / "raw" / "stock_1min" / "source.parquet"
            history.parent.mkdir(parents=True)
            history.write_bytes(b"keep forever")

            self.assertEqual(cleanup_expired_raw(ledger, raw, warehouse, now=NOW), [old])
            self.assertTrue(old.is_dir())
            self.assertEqual(cleanup_expired_raw(ledger, raw, warehouse, now=NOW, apply=True), [old])
            self.assertFalse(old.exists())
            self.assertTrue(recent.exists())
            self.assertTrue(failed.exists())
            self.assertTrue(unpublished.exists())
            self.assertTrue(quarantine.exists())
            self.assertTrue(history.exists())
            with duckdb.connect(str(ledger.database)) as conn:
                deleted_at = conn.execute(
                    "select raw_deleted_at from plans where dataset = 'stock_basic' and trade_date = '2026-08-01'"
                ).fetchone()[0]
            self.assertIsNotNone(deleted_at)


if __name__ == "__main__":
    unittest.main()
