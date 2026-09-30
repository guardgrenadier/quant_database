"""离线验证分钟来源的分页、边界、pending 与限流。"""

from __future__ import annotations

import json
import unittest
from datetime import date

from quant_ingest.errors import SourceContractError
from quant_ingest.minute_source import FIELDS, RequestLimiter, TushareMinuteSource


DAY = date(2026, 9, 18)
CODE = "000001.SZ"


def _row(stamp: str, close: float = 10.0) -> list:
    """生成与固定字段顺序一致的供应商分钟行。"""

    return [CODE, stamp, "1MIN", 10.0, 10.1, 9.9, close, 100, 1000.0]


class Response:
    """只实现适配器会读取的 HTTP 响应属性。"""

    def __init__(self, rows: list[list] | None = None, *, status: int = 200, pending: bool = False):
        """构造成功页或 `minute_data_pending` 响应。"""

        self.status_code = status
        self.headers = {"X-Request-ID": "offline", "Retry-After": "30"}
        payload = (
            {"error": "minute_data_pending"}
            if pending else {"code": 0, "data": {"fields": FIELDS.split(","), "items": rows or []}}
        )
        self.content = json.dumps(payload).encode()


class Session:
    """按 offset 返回预制响应并记录实际请求参数。"""

    def __init__(self, pages: dict[int, Response]):
        """绑定各页响应。"""

        self.pages = pages
        self.offsets: list[int] = []

    def get(self, url, *, params, headers, timeout, verify):
        """模拟一次 HTTP GET。"""

        self.offsets.append(params["offset"])
        return self.pages[params["offset"]]


class MinuteSourceTest(unittest.TestCase):
    """覆盖 L1 完成、未决和冲突判定。"""

    def test_full_page_requires_short_final_page(self):
        """整页不能判完成，下一页 0 行才证明分页终止。"""

        rows = [_row(f"2026-09-18 09:{30 + i // 60:02d}:{i % 60:02d}") for i in range(500)]
        session = Session({0: Response(rows), 500: Response([])})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: session)
        result = source.fetch_slice(CODE, DAY)
        self.assertEqual(result.status, "ok")
        self.assertEqual(len(result.rows), 500)
        self.assertEqual(session.offsets, [0, 500])
        self.assertEqual(len(result.pages), 2)

    def test_pending_returns_without_retry_or_wait(self):
        """HTTP 202 在本轮保持未决，只在下轮重新请求。"""

        session = Session({0: Response(status=202, pending=True)})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: session)
        result = source.fetch_slice(CODE, DAY)
        self.assertEqual(result.status, "pending")
        self.assertEqual(result.retry_after, 30)
        self.assertEqual(session.offsets, [0])

    def test_identical_duplicate_retained_but_conflict_is_quarantinable(self):
        """相同重复行保留，主键值冲突抛出带原文的契约异常。"""

        stamp = "2026-09-18 09:30:00"
        duplicate = _row(stamp)
        session = Session({0: Response([duplicate, duplicate])})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: session)
        self.assertEqual(len(source.fetch_slice(CODE, DAY).rows), 2)

        bad = Session({0: Response([duplicate, _row(stamp, 11.0)])})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: bad)
        with self.assertRaisesRegex(SourceContractError, "conflicting") as caught:
            source.fetch_slice(CODE, DAY)
        self.assertEqual(caught.exception.body, bad.pages[0].content)
        self.assertEqual(caught.exception.requests[0]["http_status"], 200)

    def test_rolling_limit_counts_every_http_attempt(self):
        """窗口满时等待到第一个请求离开 60 秒窗口。"""

        class Clock:
            now = 0.0

            def time(self):
                return self.now

            def sleep(self, seconds):
                self.now += seconds

        clock = Clock()
        limiter = RequestLimiter(2, clock=clock.time, sleep=clock.sleep)
        limiter.acquire()
        limiter.acquire()
        limiter.acquire()
        self.assertGreaterEqual(clock.now, 60)
        self.assertEqual(limiter.count, 3)

    def test_deterministic_503_does_not_retry(self):
        """明确的上游不可用错误直接失败，不消耗额外请求预算。"""

        response = Response(status=503)
        response.content = b'{"error":"data_source_unavailable"}'
        session = Session({0: response})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: session)
        with self.assertRaisesRegex(RuntimeError, "unavailable") as caught:
            source.fetch_slice(CODE, DAY)
        self.assertEqual(session.offsets, [0])
        self.assertEqual(caught.exception.requests[0]["http_status"], 503)

    def test_all_day_window_accepts_same_day_edges_and_rejects_next_day(self):
        """新计划覆盖盘前盘后，仍逐行拒绝跨日返回。"""

        rows = [_row(f"2026-09-18 {stamp}") for stamp in
                ("00:00:00", "09:25:00", "15:30:00", "23:59:59")]
        session = Session({0: Response(rows)})
        source = TushareMinuteSource(api_key="offline", session_factory=lambda: session)
        params = source.parameters(CODE, DAY)
        self.assertEqual((params["start_date"], params["end_date"]),
                         ("2026-09-18 00:00:00", "2026-09-18 23:59:59"))
        self.assertEqual(len(source.fetch_slice(CODE, DAY).rows), 4)
        wrong = Session({0: Response([_row("2026-09-19 00:00:00")])})
        with self.assertRaisesRegex(SourceContractError, "outside requested window"):
            TushareMinuteSource(api_key="offline", session_factory=lambda: wrong).fetch_slice(CODE, DAY)

    def test_saved_legacy_window_still_controls_resume(self):
        """旧日计划按原 09:30–15:00 参数续抓，不暗中扩大范围。"""

        source = TushareMinuteSource(api_key="offline",
                                     session_factory=lambda: Session({0: Response([_row("2026-09-18 15:30:00")])}))
        saved = {**source.parameters(CODE, DAY),
                 "start_date": "2026-09-18 09:30:00", "end_date": "2026-09-18 15:00:00"}
        with self.assertRaisesRegex(SourceContractError, "outside requested window"):
            source.fetch_slice(CODE, DAY, parameters=saved)


if __name__ == "__main__":
    unittest.main()
