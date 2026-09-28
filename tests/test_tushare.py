import os
import unittest
from unittest.mock import patch

import requests

from quant_db.sources.tushare.client import AuthenticationError, TransportError, TushareClient, TushareError


class FakeResponse:
    def __init__(self, payload=None, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        if isinstance(self.payload, ValueError):
            raise self.payload
        return self.payload


class FakeSession:
    def __init__(self, *results):
        self.results = list(results)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def success(fields, items):
    return FakeResponse({"code": 0, "msg": None, "data": {"fields": fields, "items": items}})


class TushareClientTests(unittest.TestCase):
    def test_stock_basic_and_daily_use_one_get_each(self):
        session = FakeSession(
            success(["ts_code", "name"], [["000001.SZ", "平安银行"]]),
            success(["ts_code", "close"], [["000001.SZ", 12.3]]),
        )
        client = TushareClient(api_key="test-key", session=session)
        self.assertEqual(client.query("stock_basic", list_status="L").rows, [{"ts_code": "000001.SZ", "name": "平安银行"}])
        self.assertEqual(client.query("daily", ts_code="000001.SZ").rows, [{"ts_code": "000001.SZ", "close": 12.3}])
        self.assertEqual([url.rsplit("/", 1)[-1] for url, _ in session.calls], ["stock_basic", "daily"])
        self.assertEqual(session.calls[1][1]["params"], {"ts_code": "000001.SZ"})
        self.assertEqual(session.calls[1][1]["headers"], {"X-API-Key": "test-key"})
        self.assertEqual(session.calls[1][1]["timeout"], 30)
        self.assertTrue(session.calls[1][1]["verify"])

    def test_authentication_errors(self):
        with patch.dict(os.environ, {"TUSHARE_API_KEY": ""}):
            with self.assertRaises(AuthenticationError):
                TushareClient()
        for response in (
            FakeResponse(status_code=401),
            FakeResponse({"code": 2002, "msg": "permission denied", "data": None}),
        ):
            with self.subTest(response=response):
                with self.assertRaises(AuthenticationError):
                    TushareClient(api_key="test-key", session=FakeSession(response)).query("daily")

    def test_business_and_invalid_responses(self):
        responses = (
            FakeResponse({"code": 123, "msg": "bad request", "data": None}),
            FakeResponse({"ok": False, "error": "invalid_params", "message": "bad request"}),
            FakeResponse(ValueError("not json")),
            FakeResponse(["not", "an", "envelope"]),
            success(["ts_code", "close"], [["000001.SZ"]]),
        )
        for response in responses:
            with self.subTest(response=response):
                with self.assertRaises(TushareError):
                    TushareClient(api_key="test-key", session=FakeSession(response)).query("daily")

    @patch("quant_db.sources.tushare.client.time.sleep")
    def test_retries_temporary_failures(self, sleep):
        session = FakeSession(requests.Timeout(), FakeResponse(status_code=503), success(["ts_code"], []))
        client = TushareClient(api_key="test-key", session=session)
        self.assertEqual(client.query("daily").status, "empty")
        self.assertEqual(len(session.calls), 3)
        self.assertEqual([call.args[0] for call in sleep.call_args_list], [15.0, 30.0])

    def test_permanent_http_error_is_transport_error(self):
        session = FakeSession(FakeResponse({"ok": False, "error": "invalid_params", "message": "response estimate exceeds limit"}, status_code=400))
        with self.assertRaisesRegex(TransportError, "invalid_params: response estimate exceeds limit"):
            TushareClient(api_key="test-key", session=session).query("daily")
        self.assertEqual(len(session.calls), 1)

    @patch("quant_db.sources.tushare.client.time.sleep")
    def test_retry_after_and_unavailable_source(self, sleep):
        session = FakeSession(
            FakeResponse({"ok": False, "error": "rate_limited"}, status_code=429, headers={"Retry-After": "3"}),
            success(["ts_code"], []),
        )
        self.assertEqual(TushareClient(api_key="test-key", session=session).query("daily").status, "empty")
        sleep.assert_called_once_with(15.0)

        unavailable = FakeSession(FakeResponse({"ok": False, "error": "data_source_unavailable"}, status_code=503))
        with self.assertRaisesRegex(TransportError, "data_source_unavailable"):
            TushareClient(api_key="test-key", session=unavailable).query("daily")
        self.assertEqual(len(unavailable.calls), 1)

    def test_none_is_not_sent_as_query_param(self):
        session = FakeSession(success(["ts_code"], []))
        TushareClient(api_key="test-key", session=session).query("daily", ts_code="000001.SZ", start_date=None, limit=5, offset=0)
        self.assertEqual(session.calls[0][1]["params"], {"ts_code": "000001.SZ", "limit": 5, "offset": 0})

    @patch("quant_db.sources.tushare.client.time.sleep")
    def test_non_json_503_is_retried(self, sleep):
        session = FakeSession(FakeResponse(ValueError("not json"), status_code=503), success(["ts_code"], []))
        self.assertEqual(TushareClient(api_key="test-key", session=session).query("daily").status, "empty")
        self.assertEqual(len(session.calls), 2)
        sleep.assert_called_once_with(15.0)

    def test_pending_and_response_headers_are_first_class(self):
        session = FakeSession(
            FakeResponse(
                {"ok": False, "error": "minute_data_pending"},
                status_code=202,
                headers={"Retry-After": "30", "X-RateLimit-Remaining": "123", "X-Request-ID": "req-1"},
            )
        )
        result = TushareClient(api_key="test-key", session=session).query("stk_mins")
        self.assertEqual(result.status, "pending")
        self.assertEqual(result.retry_after, 30.0)
        self.assertEqual(result.headers["X-RateLimit-Remaining"], "123")
        self.assertEqual(result.headers["X-Request-ID"], "req-1")
        self.assertEqual(result.requests[0].http_status, 202)

    def test_hard_request_budget(self):
        client = TushareClient(api_key="test-key", session=FakeSession(success(["x"], [[1]])), request_budget=1)
        self.assertEqual(client.query("daily").status, "rows")
        with self.assertRaisesRegex(TransportError, "budget 1 exhausted"):
            client.query("daily")


if __name__ == "__main__":
    unittest.main()
