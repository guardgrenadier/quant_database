"""离线验证分钟 L1 的 pending 收敛、raw 续用和隔离区。"""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.errors import SourceContractError
from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_source import FIELDS, MinuteFetch, MinutePage, RequestLimiter, TushareMinuteSource
from quant_ingest.raw import RawStore


DAY = date(2026, 9, 18)


def _published(ledger: Ledger, root: Path, dataset: str, rows: list[dict]) -> None:
    """写出可信的同日基础数据集产物供分钟计划读取。"""

    path = root / "canonical" / dataset / "date=20260918.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), path)
    sha = hashlib.sha256(path.read_bytes()).hexdigest()
    with duckdb.connect(str(ledger.database)) as conn:
        conn.execute(
            """insert into publications (dataset, trade_date, stage, canonical_path, canonical_sha256)
               values (?, ?, 'published', ?, ?)""", [dataset, DAY, str(path), sha]
        )


def _body(code: str, rows: list[list]) -> bytes:
    """构造与分钟接口固定字段一致的成功响应。"""

    return json.dumps({"code": 0, "data": {"fields": FIELDS.split(","), "items": rows}}).encode()


class FakeMinuteSource(TushareMinuteSource):
    """返回逐 run 脚本化结果，同时复用真实 raw 重放检查。"""

    def __init__(self, replies: dict[str, list[object]]) -> None:
        """为每只证券绑定按调用顺序弹出的结果。"""

        super().__init__(api_key="offline")
        self.replies = replies
        self.calls: list[str] = []
        self.requested_params: list[dict | None] = []

    def fetch_slice(self, code: str, day: date, *, parameters: dict | None = None) -> MinuteFetch:
        """返回预制结果或抛出预制契约异常。"""

        self.calls.append(code)
        self.requested_params.append(parameters)
        value = self.replies[code].pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def _result(code: str, rows: list[list]) -> MinuteFetch:
    """构造完成态切片，保留源响应原文。"""

    body = _body(code, rows)
    fields = FIELDS.split(",")
    page = MinutePage(body, [dict(zip(fields, row)) for row in rows])
    return MinuteFetch("ok" if rows else "empty", [page], [], datetime.now(timezone.utc))


class MinutePipelineTest(unittest.TestCase):
    """覆盖多轮收敛以及 raw 文件优先于重新请求的规则。"""

    def _setup(self, root: Path, codes: list[str]) -> tuple[Ledger, RawStore]:
        """建立两份同日已发布上游产物。"""

        ledger = Ledger(root / "state.duckdb")
        _published(ledger, root, "trade_cal", [{"exchange": "SSE", "cal_date": DAY, "is_open": 1}])
        _published(ledger, root, "stock_basic", [
            {"ts_code": code, "list_date": DAY, "delist_date": None, "list_status": "L"}
            for code in codes
        ])
        return ledger, RawStore(root.parent / "raw")

    def test_pending_converges_next_run_without_refetching_ok_slice(self):
        """pending 不阻塞其他证券，下一轮仅补 pending，完整后仍不发布 canonical。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            a, b = "000001.SZ", "600000.SH"
            ledger, raw = self._setup(root, [a, b])
            row = [b, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 1000]
            source = FakeMinuteSource({
                a: [MinuteFetch("pending", retry_after=30), _result(a, [])],
                b: [_result(b, [row])],
            })
            pipeline = MinutePipeline(source, ledger, raw, root, workers=2)
            first = pipeline.run(DAY)
            self.assertFalse(first.complete)
            self.assertEqual(first.statuses, {"pending": 1, "ok": 1})
            planned = ledger.saved_plan("stk_mins", DAY)[1][a]
            self.assertEqual((planned["start_date"], planned["end_date"]),
                             ("2026-09-18 00:00:00", "2026-09-18 23:59:59"))
            self.assertFalse(raw.path("stk_mins", DAY, a).exists())
            self.assertTrue(raw.path("stk_mins", DAY, b).exists())
            second = pipeline.run(DAY)
            self.assertTrue(second.complete)
            self.assertEqual(second.statuses, {"empty": 1, "ok": 1})
            self.assertEqual(source.calls.count(b), 1)
            self.assertIsNone(ledger.publication("stk_mins", DAY))

    def test_orphan_raw_recovers_without_http_and_conflict_quarantines(self):
        """孤儿 raw 复核后补账，冲突响应留证且不能判日级完整。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            a, b = "000001.SZ", "600000.SH"
            ledger, raw = self._setup(root, [a, b])
            row = [a, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 1000]
            raw.write("stk_mins", DAY, a, [_body(a, [row])])
            bad = b'{"code":0,"data":{"items":"conflict"}}'
            source = FakeMinuteSource({b: [SourceContractError("conflicting minute key", bad)]})
            result = MinutePipeline(source, ledger, raw, root, workers=2).run(DAY)
            self.assertFalse(result.complete)
            self.assertEqual(result.statuses, {"ok": 1, "quarantined": 1})
            self.assertEqual(source.calls, [b])
            evidence = list((raw.root.parent / "quarantine" / "stk_mins" / "trade_date=20260918").glob("*.response"))
            self.assertEqual(len(evidence), 1)
            self.assertEqual(evidence[0].read_bytes(), bad)

    def test_invalid_orphan_raw_is_moved_to_quarantine(self):
        """损坏的孤儿文件不会在下一轮反复挡住切片。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            code = "000001.SZ"
            ledger, raw = self._setup(root, [code])
            path = raw.path("stk_mins", DAY, code)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not gzip")
            source = FakeMinuteSource({code: []})
            result = MinutePipeline(source, ledger, raw, root).run(DAY)
            self.assertFalse(result.complete)
            self.assertEqual(result.statuses, {"quarantined": 1})
            self.assertFalse(path.exists())
            self.assertEqual(source.calls, [])

    def test_request_budget_leaves_unscheduled_slices_planned(self):
        """请求预算小于切片数时只安排可触发的切片，其他保留 planned。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            a, b = "000001.SZ", "600000.SH"
            ledger, raw = self._setup(root, [a, b])
            source = FakeMinuteSource({a: [_result(a, [])], b: [_result(b, [])]})
            source.limiter = RequestLimiter(budget=1)
            summary = MinutePipeline(source, ledger, raw, root, workers=2).run(DAY)
            self.assertFalse(summary.complete)
            self.assertEqual(len(source.calls), 1)
            self.assertEqual(summary.statuses.get("planned"), 1)

    def test_repeated_contract_violation_stays_unpublished(self):
        """自动重抓再次违约时仍为未决，不允许进入发布。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            code = "000001.SZ"
            ledger, raw = self._setup(root, [code])
            bad = b'{"code":0,"data":"bad"}'
            source = FakeMinuteSource({code: [SourceContractError("bad fields", bad),
                                              SourceContractError("bad fields", bad)]})
            pipeline = MinutePipeline(source, ledger, raw, root)
            self.assertFalse(pipeline.run(DAY).complete)
            self.assertFalse(pipeline.run(DAY).complete)
            self.assertEqual(ledger.status_counts("stk_mins", DAY), {"quarantined": 1})
            self.assertIsNone(ledger.publication("stk_mins", DAY))

    def test_existing_plan_uses_saved_window_parameters(self):
        """代码默认窗口变更后，未完成旧计划仍按账上请求参数执行。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "warehouse"
            code = "000001.SZ"
            ledger, raw = self._setup(root, [code])
            source = FakeMinuteSource({code: [_result(code, [
                [code, "2026-09-18 15:00:00", "1MIN", 10, 10, 10, 10, 100, 1000],
            ])]})
            saved = {**source.parameters(code, DAY), "start_date": "2026-09-18 09:30:00",
                     "end_date": "2026-09-18 15:00:00"}
            ledger.plan("stk_mins", DAY, {}, {code: saved})
            self.assertTrue(MinutePipeline(source, ledger, raw, root).run(DAY).complete)
            self.assertEqual(source.requested_params[0]["start_date"], saved["start_date"])
            self.assertEqual(source.requested_params[0]["end_date"], saved["end_date"])


if __name__ == "__main__":
    unittest.main()
