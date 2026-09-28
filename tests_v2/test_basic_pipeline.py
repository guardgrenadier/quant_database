"""用离线响应验证三个基础数据集的 V2 发布闭环。"""

from __future__ import annotations

import json
import tempfile
import unittest
from unittest.mock import patch
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from quant_ingest.basic_pipeline import BasicPipeline
from quant_ingest.basic_source import FIELDS, FetchResult, Page, TushareBasicsSource
from quant_ingest.errors import SourceContractError
from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore


DAY = date(2026, 9, 18)
ROWS = {
    "stock_basic": [
        ["000001.SZ", "000001", "平安银行", "SZSE", "主板", "L", "19910403", None],
        ["600000.SH", "600000", "浦发银行", "SSE", "主板", "D", "19991110", "20260919"],
    ],
    "trade_cal": [["SSE", "20260918", 1, "20260917"]],
    "suspend_d": [],
}


class FakeSource(TushareBasicsSource):
    """只返回预制的供应商信封，不访问网络。"""

    def __init__(self) -> None:
        super().__init__(api_key="offline")
        self.calls: list[str] = []

    def parameters(self, dataset: str, day: date) -> dict:
        """提供稳定的请求计划供账本校验。"""

        return {"dataset": dataset, "date": day.isoformat(), "fields": FIELDS[dataset]}

    def fetch(self, dataset: str, day: date) -> FetchResult:
        """生成字段顺序与真实网关一致的单页响应。"""

        self.calls.append(dataset)
        body = json.dumps({
            "code": 0, "data": {"fields": FIELDS[dataset].split(","), "items": ROWS[dataset]},
        }, ensure_ascii=False).encode()
        rows = [dict(zip(FIELDS[dataset].split(","), values)) for values in ROWS[dataset]]
        return FetchResult([Page(body, rows, "offline-request")], datetime.now(timezone.utc))


class BasicPipelineTest(unittest.TestCase):
    """覆盖真实 dbt 转换、文件发布和同计划续跑。"""

    def test_three_datasets_publish_and_resume_without_refetch(self):
        """三个数据集各自发布，空停牌日保留零行，续跑不重抓。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = FakeSource()
            ledger = Ledger(root / "warehouse" / "state.duckdb")
            pipeline = BasicPipeline(source, ledger, RawStore(root / "raw"), root / "warehouse")
            for dataset in ROWS:
                path = pipeline.run(dataset, DAY)
                self.assertEqual(ledger.publication(dataset, DAY)[0], "published")
                logs = list((root / "raw" / dataset / "trade_date=20260918" / "runs").glob("*.jsonl"))
                self.assertTrue(any('"event": "published"' in log.read_text() for log in logs))
                with duckdb.connect() as conn:
                    count = conn.execute("select count(*) from read_parquet(?)", [str(path)]).fetchone()[0]
                self.assertEqual(count, len(ROWS[dataset]))
                self.assertEqual(pipeline.run(dataset, DAY), path)
            self.assertEqual(source.calls, list(ROWS))
            self.assertTrue(pipeline.is_open(DAY))

    def test_publish_recovers_after_file_replace_before_ledger_update(self):
        """正式文件已替换时，续跑核对哈希后补登记，不重复抓取。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = FakeSource()
            ledger = Ledger(root / "warehouse" / "state.duckdb")
            pipeline = BasicPipeline(source, ledger, RawStore(root / "raw"), root / "warehouse")
            with patch.object(ledger, "finish_publish", side_effect=RuntimeError("simulated crash")):
                with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                    pipeline.run("trade_cal", DAY)
            self.assertEqual(ledger.publication("trade_cal", DAY)[0], "prepared")
            self.assertTrue(pipeline.canonical_path("trade_cal", DAY).is_file())
            pipeline.run("trade_cal", DAY)
            self.assertEqual(ledger.publication("trade_cal", DAY)[0], "published")
            self.assertEqual(source.calls, ["trade_cal"])

    def test_contract_violation_is_quarantined_without_publication(self):
        """协议违约留下原始响应证据，切片维持未决状态。"""

        class InvalidSource(FakeSource):
            def fetch(self, dataset: str, day: date) -> FetchResult:
                raise SourceContractError("wrong response scope", b'{"code":0,"data":"bad"}')

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = Ledger(root / "warehouse" / "state.duckdb")
            pipeline = BasicPipeline(InvalidSource(), ledger, RawStore(root / "raw"), root / "warehouse")
            with self.assertRaisesRegex(SourceContractError, "wrong response scope"):
                pipeline.run("suspend_d", DAY)
            self.assertEqual(ledger.slice_details("suspend_d", DAY, "day")[0], "quarantined")
            self.assertIsNone(ledger.publication("suspend_d", DAY))
            evidence = list((root / "quarantine" / "suspend_d" / "trade_date=20260918").glob("*.response"))
            self.assertEqual(len(evidence), 1)
            self.assertEqual(evidence[0].read_bytes(), b'{"code":0,"data":"bad"}')


class BasicSourceTest(unittest.TestCase):
    """确认供应商适配器固定全状态快照的请求形态。"""

    def test_stock_basic_uses_verified_full_status_request(self):
        """请求全状态、八字段和单页上限，避免回退到仅 L 状态。"""

        source = TushareBasicsSource(api_key="test-key")
        params = source.parameters("stock_basic", DAY)
        self.assertEqual(params["list_status"], "L,D,P,G,UN")
        self.assertEqual(params["limit"], 10000)
        self.assertEqual(params["fields"], FIELDS["stock_basic"])

    def test_source_fetches_full_status_and_rejects_wrong_calendar(self):
        """真实适配器保留响应原文，且不会把缺少目标日的日历判作完整。"""

        class Response:
            def __init__(self, dataset: str, rows: list[list]):
                self.status_code = 200
                self.headers = {"X-Request-ID": "offline"}
                self.content = json.dumps({
                    "code": 0, "data": {"fields": FIELDS[dataset].split(","), "items": rows},
                }).encode()

        class Session:
            def __init__(self):
                self.calls = []

            def get(self, url, *, params, headers, timeout, verify):
                self.calls.append((url, params.copy()))
                dataset = url.rsplit("/", 1)[-1]
                rows = ROWS[dataset] if dataset != "trade_cal" else [["SSE", "20260917", 1, "20260916"]]
                return Response(dataset, rows)

        session = Session()
        source = TushareBasicsSource(api_key="test-key", session=session)
        result = source.fetch("stock_basic", DAY)
        self.assertEqual(len(result.rows), 2)
        self.assertEqual(result.pages[0].body, Response("stock_basic", ROWS["stock_basic"]).content)
        self.assertEqual(session.calls[0][1]["list_status"], "L,D,P,G,UN")
        with self.assertRaisesRegex(ValueError, "target date"):
            source.fetch("trade_cal", DAY)


if __name__ == "__main__":
    unittest.main()
