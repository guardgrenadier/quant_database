"""验证发布后整日重抓、pending 续跑与 raw 替换坠机恢复。"""

from __future__ import annotations

import hashlib
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_source import MinuteFetch
from quant_ingest.raw import RawStore
from tests_v2.test_minute_pipeline import FakeMinuteSource, _body, _published, _result


DAY = date(2026, 9, 18)
CODE = "000001.SZ"


def _published_minute(root: Path) -> tuple[Ledger, RawStore, Path]:
    """构造一份已有 L1 与 canonical 的分钟日，供刷新测试。"""

    warehouse = root / "warehouse"
    ledger = Ledger(warehouse / "state.duckdb")
    raw = RawStore(root / "raw")
    _published(ledger, warehouse, "trade_cal", [{"exchange": "SSE", "cal_date": DAY, "is_open": 1}])
    _published(ledger, warehouse, "stock_basic", [
        {"ts_code": CODE, "list_date": DAY, "delist_date": None, "list_status": "L"},
    ])
    source = FakeMinuteSource({CODE: []})
    ledger.plan("stk_mins", DAY, {}, {CODE: source.parameters(CODE, DAY)})
    old_row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 100, 1000]
    artifact = raw.write("stk_mins", DAY, CODE, [_body(CODE, [old_row])])
    ledger.record("stk_mins", DAY, CODE, "ok", artifact=artifact, row_count=1,
                  extracted_at=datetime(2026, 9, 18, 10, tzinfo=timezone.utc))
    ledger.mark_fetched("stk_mins", DAY)
    target = warehouse / "canonical" / "stk_mins" / "date=20260918.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([{"ts_code": CODE, "trade_time": "2026-09-18 09:31:00"}]), target)
    checksum = hashlib.sha256(target.read_bytes()).hexdigest()
    ledger.prepare_publish("stk_mins", DAY, target, checksum, 1, "test")
    ledger.finish_publish("stk_mins", DAY)
    return ledger, raw, target


class MinuteRefreshTest(unittest.TestCase):
    """确认刷新状态不会把旧 raw 误判为新一轮完成。"""

    def test_pending_then_completion_keeps_old_canonical_until_publish(self):
        """首轮 pending 保留旧 canonical；下轮只补未决刷新切片。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, raw, target = _published_minute(root)
            old_sha = hashlib.sha256(raw.path("stk_mins", DAY, CODE).read_bytes()).hexdigest()
            canonical_sha = hashlib.sha256(target.read_bytes()).hexdigest()
            new_row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 11, 100, 1100]
            source = FakeMinuteSource({CODE: [MinuteFetch("pending", retry_after=30), _result(CODE, [new_row])]})
            pipeline = MinutePipeline(source, ledger, raw, root / "warehouse")
            self.assertFalse(pipeline.run(DAY, refresh=True).complete)
            self.assertEqual(ledger.plan_status("stk_mins", DAY), "refreshing")
            self.assertEqual(hashlib.sha256(raw.path("stk_mins", DAY, CODE).read_bytes()).hexdigest(), old_sha)
            self.assertTrue(pipeline.run(DAY, refresh=True).complete)
            self.assertEqual(ledger.plan_status("stk_mins", DAY), "refresh_ready")
            self.assertNotEqual(hashlib.sha256(raw.path("stk_mins", DAY, CODE).read_bytes()).hexdigest(), old_sha)
            self.assertEqual(hashlib.sha256(target.read_bytes()).hexdigest(), canonical_sha)
            self.assertEqual(source.calls, [CODE, CODE])

    def test_new_raw_orphan_after_replace_is_recovered(self):
        """raw 已替换但切片账仍为旧哈希时，从新文件补记而不重抓。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, raw, _ = _published_minute(root)
            ledger.start_refresh("stk_mins", DAY)
            new_row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 12, 100, 1200]
            raw.write("stk_mins", DAY, CODE, [_body(CODE, [new_row])], replace=True)
            source = FakeMinuteSource({CODE: []})
            summary = MinutePipeline(source, ledger, raw, root / "warehouse").run(DAY, refresh=True)
            self.assertTrue(summary.complete)
            self.assertEqual(source.calls, [])
            self.assertEqual(ledger.plan_status("stk_mins", DAY), "refresh_ready")


if __name__ == "__main__":
    unittest.main()
