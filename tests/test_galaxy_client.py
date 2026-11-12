import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from quant_data.core.artifacts import Artifacts
from quant_data.core.models import control_data
from quant_data.sources.galaxy import GalaxyClient, GalaxyRequest


def request(request_id="r1", kind="kline", code="000001.SZ"):
    codes = (code,) if isinstance(code, str) else tuple(code)
    return GalaxyRequest(
        request_id, kind, codes, 20260903, 20260903,
        900 if kind == "kline" else 92457000 if kind == "snapshot" else 0,
        1700 if kind == "kline" else 92530000 if kind == "snapshot" else 0,
    )


class ReqKline:
    pass


class FakeTgw:
    MarketType = SimpleNamespace(kSSE=1, kSZSE=2, kNEEQ=3)
    ReqKline = ReqKline

    def __init__(self, state):
        self.state = state

    def Login(self, cfg, api_mode):
        self.state["logins"] += 1
        return True

    def Close(self):
        self.state["closes"] += 1

    def QueryKline(self, req, callback, return_df_format):
        self.state["kline_calls"].append(req.security_code)
        if req.security_code == "HANG":
            return True, 0
        if self.state["delay"]:
            time.sleep(self.state["delay"])
        status = self.state["statuses"].get(req.security_code, 0)
        rows = [] if status else [{
            "security_code": req.security_code, "kline_time": 202609030930,
            "open_price": 10000000, "high_price": 11000000,
            "low_price": 9000000, "close_price": 10500000,
            "volume_trade": 123, "value_trade": 456789,
        }]
        callback(rows, status)
        return True, 0

    def GetTaskID(self):
        return 101

    def SetThirdInfoParam(self, task_id, name, value):
        self.state["third_params"].setdefault(task_id, {})[name] = value
        return self.state["param_status"]

    def QueryThirdInfo(self, task_id, query_spi, return_df_format):
        params = self.state["third_params"][task_id]
        self.state["third_calls"].append(dict(params))
        status = self.state["weight_status"]
        if self.state["weight_rows"] is None:
            rows = [] if status else [
                {"INDEX_CODE": params["index_code"], "CON_CODE": "000001.SZ",
                 "TRADE_DATE": params["start_date"], "TOTAL_SHR": 1000,
                 "FREE_SHR_RATIO": None, "CALC_SHR": 500, "WEIGHT_FACTOR": 1,
                 "WEIGHT": 60.0, "CLOSE": 10.0},
                {"INDEX_CODE": params["index_code"], "CON_CODE": "600000.SH",
                 "TRADE_DATE": params["start_date"], "TOTAL_SHR": 2000,
                 "FREE_SHR_RATIO": None, "CALC_SHR": 1000, "WEIGHT_FACTOR": 1,
                 "WEIGHT": 40.0, "CLOSE": 20.0},
            ]
        else:
            rows = self.state["weight_rows"]
        query_spi(rows, status)
        return True, 0


class FakeBaseData:
    def __init__(self, state):
        self.state = state

    def get_calendar(self, **kwargs):
        self.state["calendar_calls"] += 1
        return [20260903]


class FakeMarketData:
    def __init__(self, state, calendar):
        self.state = state
        self.calendar = calendar

    def query_snapshot(self, codes, start_date, end_date, begin_time, end_time):
        self.state["snapshot_calls"].append((tuple(codes), start_date, end_date, begin_time, end_time))
        if any(code.startswith("HANG.") for code in codes):
            time.sleep(10)
        if self.state["snapshot_custom"] is not None:
            return self.state["snapshot_custom"](codes, start_date)
        from datetime import datetime

        return {start_date: {code: [{
            "trade_time": datetime(2026, 9, 3, 9, 25),
            "last": 10000000, "total_volume_trade": 100,
        }] for code in codes}}


class FakeSdkFactory:
    def __init__(self, state=None):
        self.state = state or {
            "logins": 0, "closes": 0, "calendar_calls": 0, "kline_calls": [],
            "snapshot_calls": [], "statuses": {}, "delay": 0, "snapshot_custom": None,
            "force_logout": [], "third_calls": [], "third_params": {},
            "weight_status": 0, "weight_rows": None, "param_status": 0,
        }

    def __call__(self):
        state = self.state
        ad = SimpleNamespace(
            constant=SimpleNamespace(Period=SimpleNamespace(min1=SimpleNamespace(value=1))),
            BaseData=lambda: FakeBaseData(state), MarketData=lambda calendar: FakeMarketData(state, calendar),
        )

        def set_cfg(username, password, host, port, force_logout):
            state["force_logout"].append(force_logout)
            return object(), 0, None

        return ad, FakeTgw(state), set_cfg


class InlineProcess:
    def __init__(self, target, args):
        self.target, self.args = target, args
        self.exitcode = None
        self.closed = False

    def start(self):
        try:
            self.target(*self.args)
        except SystemExit as exc:
            self.exitcode = int(exc.code or 0)
        else:
            self.exitcode = 0

    def join(self, timeout=None):
        return None

    def is_alive(self):
        return False

    def close(self):
        self.closed = True


class InlineWorkerFactory:
    def __init__(self):
        self.processes = []

    def __call__(self, target, args):
        process = InlineProcess(target, args)
        self.processes.append(process)
        return process


class HangingThenInlineFactory:
    def __init__(self):
        self.calls = 0
        self.terminated = 0

    def __call__(self, target, args):
        self.calls += 1
        if self.calls > 1:
            return InlineProcess(target, args)
        factory = self

        class Hanging:
            def __init__(self):
                self.alive = False

            def start(self):
                self.alive = True

            def join(self, timeout=None):
                return None

            def is_alive(self):
                return self.alive

            def terminate(self):
                factory.terminated += 1
                self.alive = False

            def kill(self):
                self.alive = False

            def close(self):
                pass

        return Hanging()


class AlwaysHangingFactory(HangingThenInlineFactory):
    def __call__(self, target, args):
        self.calls += 1
        return self._hanging()

    def _hanging(self):
        factory = self

        class Hanging:
            alive = False

            def start(self):
                self.alive = True

            def join(self, timeout=None):
                pass

            def is_alive(self):
                return self.alive

            def terminate(self):
                factory.terminated += 1
                self.alive = False

            def kill(self):
                self.alive = False

            def close(self):
                pass

        return Hanging()


class GalaxyClientTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name) / "run"
        self.artifacts = Artifacts(self.root, True)
        self.state = FakeSdkFactory().state
        self.sdk = FakeSdkFactory(self.state)
        self.workers = InlineWorkerFactory()
        credentials = patch.dict(os.environ, {"AD_USERNAME": "offline-user", "AD_PASSWORD": "offline-password"})
        credentials.start()
        self.addCleanup(credentials.stop)
        self.addCleanup(self.directory.cleanup)

    def client(self, **kwargs):
        options = {"sdk_factory": self.sdk, "worker_factory": self.workers, "respawn_wait": 0}
        options.update(kwargs)
        return GalaxyClient(**options)

    def test_kline_contract_raw_rows_and_json(self):
        responses, attempts = self.client().fetch([request()], self.artifacts)
        self.assertEqual(responses[0].state, "COMPLETE")
        self.assertEqual(responses[0].rows[0]["volume_trade"], 123)
        self.assertEqual(responses[0].rows[0]["ts_code"], "000001.SZ")
        self.assertEqual(attempts[0].provider_code, 0)
        self.assertEqual(attempts[0].row_count, 1)
        self.assertIsNotNone(attempts[0].evidence_path)
        self.assertEqual(self.state["force_logout"], [False])
        self.assertEqual(self.state["calendar_calls"], 1)
        self.assertEqual(self.state["logins"], 1)
        self.assertEqual(self.state["closes"], 1)
        json.dumps(control_data((responses, attempts)), allow_nan=False)

    def test_index_weight_contract_and_evidence_recovery(self):
        req = request(kind="index_weight", code="000300.SH")
        responses, attempts = self.client(parallel=1).fetch([req], self.artifacts)
        self.assertEqual(responses[0].state, "COMPLETE")
        self.assertEqual(len(responses[0].rows), 2)
        self.assertNotIn("ts_code", responses[0].rows[0])
        self.assertEqual(attempts[0].api_name, "tgw.QueryThirdInfo")
        self.assertEqual(attempts[0].params, {"function_id": "A010200005", "index_code": "000300.SH",
                                               "start_date": "20260903", "end_date": "20260903"})
        self.assertEqual(self.state["calendar_calls"], 0)
        recovered, recovered_attempts = self.client(parallel=1).fetch([req], self.artifacts)
        self.assertEqual(recovered, responses)
        self.assertEqual(recovered_attempts, attempts)
        self.assertEqual(len(self.state["third_calls"]), 1)

    def test_index_weight_empty_and_parameter_failure(self):
        self.state["weight_status"] = -76
        empty = self.client(parallel=1).fetch(
            [request("empty", "index_weight", "000300.SH")], self.artifacts,
        )[0][0]
        self.assertEqual((empty.state, empty.provider_code, empty.rows), ("COMPLETE", -76, ()))
        self.state["param_status"] = -1
        failed = self.client(parallel=1).fetch(
            [request("bad-param", "index_weight", "000300.SH")],
            Artifacts(Path(self.directory.name) / "bad-param-run", True),
        )[0][0]
        self.assertEqual(failed.state, "FAILED")
        self.assertIn("parameter function_id", failed.error)

    def test_kline_none_callback_status_is_success(self):
        self.state["statuses"]["000001"] = None
        response = self.client().fetch([request()], self.artifacts)[0][0]
        self.assertEqual(response.state, "COMPLETE")
        self.assertEqual(response.provider_code, 0)
        self.assertEqual(len(response.rows), 1)

    def test_default_worker_uses_spawn_with_offline_sdk_factory(self):
        client = GalaxyClient(sdk_factory=FakeSdkFactory(), respawn_wait=0)
        responses, attempts = client.fetch([request()], self.artifacts)
        self.assertEqual(responses[0].state, "COMPLETE")
        self.assertEqual(responses[0].rows[0]["close_price"], 10500000)
        self.assertEqual(len(attempts), 1)

    def test_snapshot_batch_time_attribution_and_utc_iso(self):
        codes = ("000001.SZ", "600519.SH")
        responses, _ = self.client().fetch([request(kind="snapshot", code=codes)], self.artifacts)
        self.assertEqual(responses[0].state, "COMPLETE")
        self.assertEqual({row["ts_code"] for row in responses[0].rows}, set(codes))
        self.assertEqual(responses[0].rows[0]["trade_time"], "2026-09-03T01:25:00+00:00")
        self.assertEqual(self.state["snapshot_calls"], [(codes, 20260903, 20260903, 92457000, 92530000)])

    def test_snapshot_missing_code_is_incomplete_with_partial_raw(self):
        self.state["snapshot_custom"] = lambda codes, day: {day: {codes[0]: []}}
        req = request(kind="snapshot", code=("000001.SZ", "600519.SH"))
        response = self.client().fetch([req], self.artifacts)[0][0]
        self.assertEqual(response.state, "INCOMPLETE")
        self.assertIn("600519.SH", response.error)
        self.assertEqual(response.rows, ())
        self.assertTrue(response.unit_retryable)

    def test_retryable_evidence_and_staged_request_wave(self):
        self.state["snapshot_custom"] = lambda codes, day: {day: {codes[0]: []}}
        parent = request("parent", "snapshot", ("000001.SZ", "600519.SH"))
        original = self.client().fetch([parent], self.artifacts)[0][0]
        self.assertTrue(original.unit_retryable)
        recovered = self.client().fetch([parent], self.artifacts)[0][0]
        self.assertEqual(recovered, original)

        self.state["snapshot_custom"] = None
        children = [request("child1", "snapshot", "000001.SZ"),
                    request("child2", "snapshot", "600519.SH")]
        # 默认模式仍拒绝其他 request_id；只有显式补取波次可以跳过父请求证据。
        with self.assertRaisesRegex(ValueError, "Unknown Galaxy evidence"):
            self.client().fetch(children, self.artifacts)
        responses, attempts = self.client().fetch(
            children, self.artifacts, allow_unrelated_evidence=True,
        )
        self.assertEqual([r.state for r in responses], ["COMPLETE", "COMPLETE"])
        self.assertEqual(len(attempts), 2)
        self.assertEqual(len(self.state["snapshot_calls"]), 3)
        recovered_children, recovered_attempts = self.client().fetch(
            children, self.artifacts, allow_unrelated_evidence=True,
        )
        self.assertEqual(recovered_children, responses)
        self.assertEqual(recovered_attempts, attempts)
        self.assertEqual(len(self.state["snapshot_calls"]), 3)

    def test_snapshot_provider_empty_is_complete_for_history_fallback(self):
        def unavailable(codes, day):
            raise RuntimeError("provider status -76")
        self.state["snapshot_custom"] = unavailable
        response = self.client().fetch([request(kind="snapshot")], self.artifacts)[0][0]
        self.assertEqual(response.state, "COMPLETE")
        self.assertEqual(response.provider_code, -76)
        self.assertEqual(response.rows, ())

    def test_parallel_two_and_four_have_identical_responses(self):
        requests = [request(f"r{i}", code=f"{i:06d}.SZ") for i in range(1, 8)]
        responses2, _ = self.client(parallel=2).fetch(requests, self.artifacts)
        other = Artifacts(Path(self.directory.name) / "run4", True)
        responses4, _ = self.client(parallel=4).fetch(requests, other)
        self.assertEqual(responses2, responses4)
        with self.assertRaises(ValueError):
            self.client(parallel=3)

    def test_kline_callback_timeout_is_failed(self):
        response = self.client(query_timeout=0.01, max_respawns=0).fetch(
            [request(code="HANG.SZ")], self.artifacts,
        )[0][0]
        self.assertEqual(response.state, "FAILED")
        self.assertIn("exceeded", response.error)
        self.assertTrue(response.unit_retryable)

    def test_snapshot_timeout_replaces_session_and_preserves_remaining(self):
        requests = [request("snap", "snapshot", "HANG.SZ"), request("later"), request("pending")]
        responses, attempts = self.client(query_timeout=0.01, max_respawns=0).fetch(requests, self.artifacts)
        self.assertEqual([response.state for response in responses], ["FAILED", "COMPLETE", "INCOMPLETE"])
        self.assertFalse(any(response.unit_retryable for response in responses))
        self.assertEqual(len(attempts), 3)
        self.assertEqual(self.state["kline_calls"], ["000001"])

    def test_circuit_breaker_stops_remaining_scheduling(self):
        self.state["statuses"]["000001"] = -82
        requests = [request(f"r{i}", code=f"{i:06d}.SZ") for i in range(1, 5)]
        responses, attempts = self.client().fetch(requests, self.artifacts)
        self.assertEqual(responses[0].provider_code, -82)
        self.assertEqual([response.state for response in responses[2:]], ["INCOMPLETE", "INCOMPLETE"])
        self.assertFalse(any(response.unit_retryable for response in responses))
        self.assertEqual(self.state["kline_calls"], ["000001", "000002"])
        self.assertEqual(len(attempts), 4)

    def test_new_worker_recovers_rows_from_current_run_evidence(self):
        first, second = request("first"), request("second", code="000002.SZ")
        original = self.client().fetch([first], self.artifacts)[0][0]
        responses, attempts = self.client().fetch([first, second], self.artifacts)
        self.assertEqual(responses[0], original)
        self.assertEqual(responses[0].rows[0]["volume_trade"], 123)
        self.assertEqual(self.state["kline_calls"], ["000001", "000002"])
        self.assertEqual([attempt.request_id for attempt in attempts], ["first", "second"])

    def test_bad_evidence_fails_instead_of_skipping_request(self):
        req = request()
        self.client().fetch([req], self.artifacts)
        path = self.root / "requests" / "r1-attempt-01.json"
        body = json.loads(path.read_text())
        body["response"]["request"]["codes"] = ["600519.SH"]
        path.write_text(json.dumps(body))
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            self.client().fetch([req], self.artifacts)
        self.assertEqual(self.state["kline_calls"], ["000001"])

    def test_hard_timeout_destroys_worker_then_recovers_remaining(self):
        workers = HangingThenInlineFactory()
        client = self.client(worker_factory=workers, hard_timeout=0.001, respawn_wait=7)
        with patch("quant_data.sources.galaxy.client.time.sleep") as sleep:
            responses, _ = client.fetch([request()], self.artifacts)
        self.assertEqual(responses[0].state, "COMPLETE")
        self.assertEqual(workers.calls, 2)
        self.assertEqual(workers.terminated, 1)
        sleep.assert_called_once_with(7)

    def test_respawn_budget_is_two_new_workers(self):
        workers = AlwaysHangingFactory()
        client = self.client(worker_factory=workers, hard_timeout=0.001, max_respawns=2)
        response = client.fetch([request()], self.artifacts)[0][0]
        self.assertEqual(response.state, "INCOMPLETE")
        self.assertEqual(workers.calls, 3)
        self.assertEqual(workers.terminated, 3)

    def test_request_shape_validation(self):
        with self.assertRaises(ValueError):
            self.client().fetch([request(code=("000001.SZ", "600519.SH"))], self.artifacts)
        with self.assertRaises(ValueError):
            self.client().fetch([request(kind="snapshot", code=[f"{i:06d}.SZ" for i in range(201)])], self.artifacts)
        with self.assertRaises(ValueError):
            self.client().fetch([request(kind="index_weight", code=("000300.SH", "000905.SH"))], self.artifacts)
        with self.assertRaises(ValueError):
            self.client().fetch([GalaxyRequest("bad-time", "index_weight", ("000300.SH",),
                                               20260903, 20260903, 900, 1700)], self.artifacts)


if __name__ == "__main__":
    unittest.main()
