import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow.parquet as pq

from quant_db.ingestion.publish import normalize_empty_candidate, validate_candidate
from quant_db.ingestion.raw_store import RawStore
from quant_db.sources.tushare.client import QueryResult
from quant_db.sources.tushare.minute import FIELDS, RAW_SCHEMA, SliceResult, TushareMinuteSource


def minute_row(code="000001.SZ", trade_time="2026-09-18 15:00:00"):
    return {
        "ts_code": code,
        "trade_time": trade_time,
        "freq": "1MIN",
        "open": 10.0,
        "high": 11.0,
        "low": 9.0,
        "close": 10.5,
        "vol": 100,
        "amount": 1050.0000001,
    }


class FakeClient:
    def __init__(self, *results):
        self.results = list(results)

    def query(self, api_name, **params):
        return self.results.pop(0)


class MinuteAdapterTests(unittest.TestCase):
    def test_short_page_completes_slice(self):
        source = TushareMinuteSource(FakeClient(QueryResult("rows", [minute_row()])))
        result = source.fetch_slice("000001.SZ", date(2026, 9, 18))
        self.assertEqual(result.status, "ok")
        self.assertEqual(len(result.rows), 1)

    def test_pending_is_not_failed(self):
        source = TushareMinuteSource(FakeClient(QueryResult("pending", retry_after=30)))
        result = source.fetch_slice("000001.SZ", date(2026, 9, 18))
        self.assertEqual(result.status, "pending")
        self.assertEqual(result.retry_after, 30)

    def test_out_of_slice_row_fails(self):
        source = TushareMinuteSource(FakeClient(QueryResult("rows", [minute_row(code="600000.SH")])))
        result = source.fetch_slice("000001.SZ", date(2026, 9, 18))
        self.assertEqual(result.status, "failed")
        self.assertIn("outside requested slice", result.error)

    def test_duplicate_key_within_page_fails(self):
        row = minute_row()
        source = TushareMinuteSource(FakeClient(QueryResult("rows", [row, row.copy()])))
        result = source.fetch_slice("000001.SZ", date(2026, 9, 18))
        self.assertEqual(result.status, "failed")
        self.assertIn("duplicate minute keys", result.error)

    def test_full_page_requires_another_page_and_overlap_fails(self):
        start = datetime(2026, 9, 18, 9, 30)
        rows = [minute_row(trade_time=(start + timedelta(seconds=i)).strftime("%Y-%m-%d %H:%M:%S")) for i in range(500)]
        source = TushareMinuteSource(FakeClient(QueryResult("rows", rows), QueryResult("rows", rows)))
        result = source.fetch_slice("000001.SZ", date(2026, 9, 18))
        self.assertEqual(result.status, "failed")


class RawStoreTests(unittest.TestCase):
    def test_per_slice_write_and_resume(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            day = date(2026, 9, 18)
            store = RawStore.create(
                root,
                day,
                [{"ts_code": "000001.SZ"}],
                [],
                RAW_SCHEMA,
                "tushare",
            )
            store.record("000001.SZ", SliceResult("ok", [minute_row()]))
            self.assertTrue(store.complete)
            self.assertEqual(store.raw_rows, 1)
            path = store.directory / "000001_SZ.parquet"
            self.assertEqual(pq.read_table(path).num_rows, 1)
            store.mark_fetched()
            resumed = RawStore.resume(root, day, RAW_SCHEMA)
            self.assertTrue(resumed.terminal("000001.SZ"))
            self.assertEqual(json.loads((store.directory / "manifest.json").read_text())["stage"], "fetched")

    def test_cleanup_requires_age_and_published_confirmation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            day = date(2026, 7, 1)
            store = RawStore.create(root, day, [{"ts_code": "000001.SZ"}], [], RAW_SCHEMA, "tushare")
            store.record("000001.SZ", SliceResult("empty"))
            store.mark_fetched()
            store.manifest["fetched_at"] = "2026-07-01T00:00:00+00:00"
            store._save()
            now = datetime(2026, 8, 1, tzinfo=timezone.utc)
            self.assertEqual(RawStore.cleanup_expired(root, lambda _: False, now=now), [])
            self.assertTrue(store.directory.exists())
            self.assertEqual(RawStore.cleanup_expired(root, lambda _: True, now=now), [store.directory])
            self.assertFalse(store.directory.exists())


class PublishTests(unittest.TestCase):
    def test_empty_candidate_has_zero_physical_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.parquet"
            normalize_empty_candidate(path, 0)
            self.assertEqual(validate_candidate(path, date(2026, 9, 18), 0), 0)


if __name__ == "__main__":
    unittest.main()
