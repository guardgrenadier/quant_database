"""离线验证 V2 计划固定与 raw 持久化边界。"""

import gzip
import tempfile
import unittest
from datetime import date
from pathlib import Path

from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore


class FoundationTest(unittest.TestCase):
    """覆盖首阶段最关键的续跑和文件完整性行为。"""

    def test_plan_is_fixed_and_incomplete_until_every_slice_finishes(self):
        """同计划可续跑，参数漂移被拒绝，未决切片阻止完整判定。"""

        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / "state.duckdb")
            day = date(2026, 9, 18)
            params = {"window": ["09:30:00", "15:00:00"], "limit": 500}
            slices = {"000001.SZ": {"ts_code": "000001.SZ"}, "600000.SH": {"ts_code": "600000.SH"}}
            ledger.plan("stk_mins", day, params, slices)
            ledger.plan("stk_mins", day, params, slices)
            with self.assertRaisesRegex(ValueError, "plan changed"):
                ledger.plan("stk_mins", day, {**params, "limit": 100}, slices)
            self.assertEqual(ledger.unresolved("stk_mins", day), ["000001.SZ", "600000.SH"])
            self.assertFalse(ledger.complete("stk_mins", day))

            raw = RawStore(Path(directory) / "raw")
            page = b'{ "code":0,"data":{"fields":[],"items":[]} }'
            artifact = raw.write("stk_mins", day, "000001.SZ", [page])
            ledger.record("stk_mins", day, "000001.SZ", "empty", artifact=artifact, row_count=0)
            ledger.record("stk_mins", day, "600000.SH", "pending")
            self.assertFalse(ledger.complete("stk_mins", day))
            with self.assertRaisesRegex(ValueError, "incomplete"):
                ledger.mark_fetched("stk_mins", day)
            self.assertEqual(ledger.unresolved("stk_mins", day), ["600000.SH"])
            with self.assertRaisesRegex(ValueError, "immutable"):
                ledger.record("stk_mins", day, "000001.SZ", "pending")
            second = raw.write("stk_mins", day, "600000.SH", [page])
            ledger.record("stk_mins", day, "600000.SH", "empty", artifact=second, row_count=0)
            self.assertTrue(ledger.complete("stk_mins", day))
            ledger.mark_fetched("stk_mins", day)

    def test_raw_keeps_each_page_unchanged_and_recovers_after_crash(self):
        """raw 保留逐页原文，并允许从写文件后、记账前的坠机恢复。"""

        with tempfile.TemporaryDirectory() as directory:
            raw = RawStore(Path(directory))
            ledger = Ledger(Path(directory) / "state.duckdb")
            day = date(2026, 9, 18)
            ledger.plan("stk_mins", day, {"limit": 500}, {"000001.SZ": {"ts_code": "000001.SZ"}})
            pages = [b'{"code":0, "data":{"items":[[1]]}}', b' {"code":0,"data":{"items":[]}}\n']
            artifact = raw.write("stk_mins", day, "000001.SZ", pages)
            # 模拟文件已原子落盘、账本尚未更新时进程坠机。
            self.assertFalse(ledger.complete("stk_mins", day))
            self.assertEqual(gzip.decompress(artifact.path.read_bytes()), b"[" + b",".join(pages) + b"]")
            recovered = raw.inspect("stk_mins", day, "000001.SZ")
            self.assertEqual(recovered, artifact)
            ledger.record("stk_mins", day, "000001.SZ", "ok", artifact=recovered, row_count=1)
            self.assertTrue(ledger.complete("stk_mins", day))
            self.assertTrue(raw.verify(artifact))
            with self.assertRaises(FileExistsError):
                raw.write("stk_mins", day, "000001.SZ", [pages[0]])
            self.assertEqual(raw.inspect("stk_mins", day, "000001.SZ"), artifact)
            self.assertFalse(list(artifact.path.parent.glob(".raw-*")))


if __name__ == "__main__":
    unittest.main()
