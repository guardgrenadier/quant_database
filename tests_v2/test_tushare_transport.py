"""离线验证共用传输层的重试、限流和脱敏记录。"""

from __future__ import annotations

import json
import unittest
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

from quant_ingest.errors import SourceRequestError
from quant_ingest.tushare_transport import RequestLimiter, TushareTransport


class Response:
    """只实现传输层读取的 HTTP 属性。"""

    def __init__(self, status: int, body: bytes = b"{}", headers: dict | None = None) -> None:
        """构造一个带状态、原文和响应头的假响应。"""

        self.status_code = status
        self.content = body
        self.headers = headers or {}


class Session:
    """顺序返回假响应，并检查认证信息只进入 HTTP Header。"""

    def __init__(self, responses: list[Response]) -> None:
        """绑定响应序列。"""

        self.responses = iter(responses)
        self.calls: list[tuple] = []

    def get(self, url, *, params, headers, timeout, verify):
        """记录请求形态后返回下一次响应。"""

        self.calls.append((url, params.copy(), headers.copy(), timeout, verify))
        return next(self.responses)


class TransportTest(unittest.TestCase):
    """覆盖共用传输应保证的 HTTP 行为。"""

    def test_retry_records_and_budget_count_real_attempts(self):
        """瞬时 503 消耗预算并重试，记录中不含 API key。"""

        session = Session([Response(503), Response(200, b'{"code":0}', {"X-Request-ID": "ok"})])
        sleeps: list[float] = []
        limiter = RequestLimiter(budget=2)
        transport = TushareTransport(
            api_key="very-secret", session_factory=lambda: session, limiter=limiter,
            retry_delay=2, sleep=sleeps.append,
        )
        result = transport.get("stk_mins", {"offset": 500})
        self.assertEqual(result.body, b'{"code":0}')
        self.assertEqual(result.status_code, 200)
        self.assertEqual([item["http_status"] for item in result.attempts], [503, 200])
        self.assertEqual(limiter.count, 2)
        self.assertEqual(sleeps, [2])
        self.assertNotIn("very-secret", json.dumps(result.attempts))
        self.assertEqual(session.calls[0][2], {"X-API-Key": "very-secret"})
        self.assertTrue(session.calls[0][4])

    def test_retry_after_and_pending_response(self):
        """429 遵守 Retry-After，随后 202 原样交给适配器。"""

        pending = b'{"error":"minute_data_pending"}'
        session = Session([Response(429, headers={"retry-after": "30"}), Response(202, pending)])
        sleeps: list[float] = []
        transport = TushareTransport(api_key="key", session_factory=lambda: session,
                                     retry_delay=1, sleep=sleeps.append)
        result = transport.get("stk_mins", {"offset": 0})
        self.assertEqual(sleeps, [30])
        self.assertEqual(result.status_code, 202)
        self.assertEqual(result.body, pending)
        self.assertEqual(len(result.attempts), 2)

    def test_deterministic_503_stops_with_attempt_record(self):
        """明确不可用的 503 直接抛错，保留该次尝试证据。"""

        session = Session([Response(503, b'{"error":"data_source_unavailable"}')])
        transport = TushareTransport(api_key="key", session_factory=lambda: session)
        with self.assertRaisesRegex(SourceRequestError, "unavailable") as caught:
            transport.get("stock_basic", {"offset": 0})
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(caught.exception.requests[0]["http_status"], 503)

    def test_budget_exhaustion_does_not_send_an_extra_request(self):
        """预算耗尽后拒绝下一次调用，已发请求数保持不变。"""

        session = Session([Response(200)])
        transport = TushareTransport(
            api_key="key", session_factory=lambda: session, limiter=RequestLimiter(budget=1),
        )
        transport.get("stk_mins", {"offset": 0})
        with self.assertRaisesRegex(SourceRequestError, "budget exhausted"):
            transport.get("stk_mins", {"offset": 500})
        self.assertEqual(len(session.calls), 1)

    def test_concurrent_calls_get_separate_sessions(self):
        """两个并发工作线程各自创建 Session，不共享连接对象。"""

        barrier = Barrier(2)
        sessions: list[Session] = []

        def create_session():
            session = Session([Response(200)])
            sessions.append(session)
            return session

        transport = TushareTransport(api_key="key", session_factory=create_session)

        def call():
            barrier.wait()
            return transport.get("stk_mins", {"offset": 0}).status_code

        with ThreadPoolExecutor(max_workers=2) as pool:
            self.assertEqual(list(pool.map(lambda _: call(), range(2))), [200, 200])
        self.assertEqual(len(sessions), 2)
        self.assertIsNot(sessions[0], sessions[1])


if __name__ == "__main__":
    unittest.main()
