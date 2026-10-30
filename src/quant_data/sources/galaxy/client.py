"""银河执行层：独立单会话 worker、证据恢复、并发请求与熔断。"""

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import multiprocessing
from pathlib import Path
import re
import time
from typing import Literal

from quant_data.core.models import RequestAttempt


ResponseState = Literal["COMPLETE", "FAILED", "INCOMPLETE"]


@dataclass(frozen=True, slots=True)
class GalaxyRequest:
    """一个可恢复、可审计的银河逻辑请求。

    K 线 codes 必须为单只，快照最多 200 只且只覆盖单日。日期采用 YYYYMMDD；
    K 线时间为 HHMM，快照窄窗为 HHMMSSmmm，均按市场本地时间解释。
    """
    request_id: str
    kind: Literal["kline", "snapshot"]
    codes: tuple[str, ...]
    start_date: int
    end_date: int
    begin_time: int
    end_time: int


@dataclass(frozen=True, slots=True)
class GalaxyResponse:
    """一个逻辑请求的最终交付及原始行，不直接等于业务验收通过。

    COMPLETE 只表示请求与已知终止条件完成；FAILED/INCOMPLETE 仍保留错误和
    provider_code。rows 保存原始 K 线整数或快照源字段，供证据恢复重建 Raw。
    """
    request: GalaxyRequest
    state: ResponseState
    rows: tuple[dict, ...]
    error: str | None
    provider_code: int | None


def _worker_entry(requests, artifacts, options, sdk_factory):
    """spawn 入口不返回业务行；每条响应先写本 Run 证据，供父进程恢复。"""
    from quant_data.sources.galaxy.worker import run_worker

    run_worker(requests, artifacts, options, sdk_factory)


def _request_params(request, kind=None):
    """审计参数与恢复核对共用同一确定性表示。"""
    selected = kind or request.kind
    if selected == "kline":
        return {
            "codes": list(request.codes), "start_date": request.start_date,
            "end_date": request.end_date, "begin_time": request.begin_time,
            "end_time": request.end_time, "cyc_type": "min1", "auto_complete": 0, "cq_flag": 0,
        }
    return {
        "codes": list(request.codes), "start_date": request.start_date,
        "end_date": request.end_date, "begin_time": request.begin_time, "end_time": request.end_time,
    }


def _api_name(kind):
    return "tgw.QueryKline" if kind == "kline" else "AmazingData.MarketData.query_snapshot"


def _response_data(response):
    return {
        "request": {
            "request_id": response.request.request_id, "kind": response.request.kind,
            "codes": list(response.request.codes), "start_date": response.request.start_date,
            "end_date": response.request.end_date, "begin_time": response.request.begin_time,
            "end_time": response.request.end_time,
        },
        "state": response.state, "rows": response.rows,
        "error": response.error, "provider_code": response.provider_code,
    }


def _request_from_data(value):
    if not isinstance(value, dict):
        raise ValueError("Invalid Galaxy request evidence")
    try:
        request = GalaxyRequest(
            value["request_id"], value["kind"], tuple(value["codes"]),
            value["start_date"], value["end_date"], value["begin_time"], value["end_time"],
        )
    except (KeyError, TypeError) as exc:
        raise ValueError("Invalid Galaxy request evidence") from exc
    # 身份核对不能用字符串化后的近似相等，避免错误请求被恢复成有效 Raw。
    if _response_data(GalaxyResponse(request, "COMPLETE", (), None, None))["request"] != value:
        raise ValueError("Invalid or noncanonical Galaxy request evidence")
    return request


def _response_from_data(value):
    if not isinstance(value, dict) or set(value) != {"request", "state", "rows", "error", "provider_code"}:
        raise ValueError("Invalid Galaxy response evidence")
    request = _request_from_data(value["request"])
    state, rows = value["state"], value["rows"]
    if state not in ("COMPLETE", "FAILED", "INCOMPLETE") or not isinstance(rows, list):
        raise ValueError("Invalid Galaxy response state or rows")
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("Invalid Galaxy row evidence")
    error, code = value["error"], value["provider_code"]
    if error is not None and not isinstance(error, str):
        raise ValueError("Invalid Galaxy error evidence")
    if code is not None and type(code) is not int:
        raise ValueError("Invalid Galaxy provider code evidence")
    if state == "COMPLETE" and error is not None or state != "COMPLETE" and error is None:
        raise ValueError("Inconsistent Galaxy response state and error")
    return GalaxyResponse(request, state, tuple(rows), error, code)


def _datetime_from_evidence(value):
    if not isinstance(value, str):
        raise ValueError("Invalid request timestamp evidence")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("Invalid request timestamp evidence") from exc
    if parsed.tzinfo is None:
        raise ValueError("Galaxy request timestamp must include a timezone")
    return parsed


def _load_evidence(requests, root):
    """严格恢复当前 Run；坏证据和身份不符直接阻断，绝不降级为空响应。"""
    by_id = {request.request_id: request for request in requests}
    found = {request.request_id: [] for request in requests}
    folder = Path(root) / "requests"
    if not folder.exists():
        return {}, []
    for path in sorted(folder.glob("*.json")):
        match = re.fullmatch(r"(.+)-attempt-(\d+)\.json", path.name)
        if match is None or match.group(1) not in by_id:
            raise ValueError(f"Unknown Galaxy evidence filename: {path.name}")
        request_id, number = match.group(1), int(match.group(2))
        try:
            body = json.loads(path.read_text(encoding="utf-8"), parse_constant=_reject_json_constant)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Unreadable Galaxy evidence: {path.name}") from exc
        if not isinstance(body, dict) or set(body) != {"attempt", "response"}:
            raise ValueError(f"Invalid Galaxy evidence envelope: {path.name}")
        attempt_data, response = body["attempt"], _response_from_data(body["response"])
        if not isinstance(attempt_data, dict) or response.request != by_id[request_id]:
            raise ValueError(f"Galaxy evidence request identity mismatch: {path.name}")
        required = {
            "request_id", "attempt", "api_name", "params", "fields", "requested_at",
            "elapsed_seconds", "http_status", "provider_code", "returned_fields",
            "row_count", "has_more", "error", "evidence_path",
        }
        if set(attempt_data) != required or attempt_data["request_id"] != request_id:
            raise ValueError(f"Invalid Galaxy attempt evidence: {path.name}")
        if type(attempt_data["attempt"]) is not int or attempt_data["attempt"] != number or number < 1:
            raise ValueError(f"Invalid Galaxy attempt number: {path.name}")
        if attempt_data["api_name"] != _api_name(response.request.kind):
            raise ValueError(f"Galaxy evidence API mismatch: {path.name}")
        if attempt_data["params"] != _request_params(response.request):
            raise ValueError(f"Galaxy evidence params mismatch: {path.name}")
        if attempt_data["provider_code"] != response.provider_code or attempt_data["error"] != response.error:
            raise ValueError(f"Galaxy evidence response control mismatch: {path.name}")
        if attempt_data["row_count"] != len(response.rows):
            raise ValueError(f"Galaxy evidence row count mismatch: {path.name}")
        evidence_path = attempt_data["evidence_path"]
        if not isinstance(evidence_path, str) or Path(evidence_path).resolve() != path.resolve():
            raise ValueError(f"Galaxy evidence path mismatch: {path.name}")
        fields, returned = attempt_data["fields"], attempt_data["returned_fields"]
        if (not isinstance(fields, list) or any(not isinstance(x, str) for x in fields)
                or not isinstance(returned, list) or any(not isinstance(x, str) for x in returned)):
            raise ValueError(f"Invalid Galaxy fields evidence: {path.name}")
        elapsed = attempt_data["elapsed_seconds"]
        if type(elapsed) not in (int, float) or elapsed < 0:
            raise ValueError(f"Invalid Galaxy elapsed time: {path.name}")
        if attempt_data["http_status"] is not None or attempt_data["has_more"] is not None:
            raise ValueError(f"Unexpected HTTP or pagination state in Galaxy evidence: {path.name}")
        attempt = RequestAttempt(
            request_id, number, attempt_data["api_name"], attempt_data["params"], tuple(fields),
            _datetime_from_evidence(attempt_data["requested_at"]), float(elapsed), None,
            response.provider_code, tuple(returned), len(response.rows), None, response.error, evidence_path,
        )
        found[request_id].append((number, attempt, response))
    responses, attempts = {}, []
    for request in requests:
        records = sorted(found[request.request_id], key=lambda item: item[0])
        if not records:
            continue
        numbers = [item[0] for item in records]
        # 每次换 worker 的 attempt 必须连续，缺文件不能拼成貌似完整的恢复链。
        if numbers != list(range(1, numbers[-1] + 1)):
            raise ValueError(f"Noncontiguous Galaxy attempts: {request.request_id}")
        attempts.extend(item[1] for item in records)
        responses[request.request_id] = records[-1][2]
    return responses, attempts


def _reject_json_constant(value):
    raise ValueError(f"Non-JSON numeric constant: {value}")


class GalaxyClient:
    """分钟 Adapter 的公开获取接口；父进程不登录，默认以 spawn 启动单会话 worker。"""

    def __init__(self, *, parallel=2, query_timeout=30.0, hard_timeout=3600.0,
                 respawn_wait=60.0, shutdown_timeout=5.0, max_respawns=2,
                 sdk_factory=None, worker_factory=None):
        # 并发只开放探查验证过的 2/4；会话故障最多新增两个进程，不能无限登录。
        if parallel not in (2, 4) or max_respawns not in (0, 1, 2):
            raise ValueError("parallel must be 2 or 4; max_respawns must be between 0 and 2")
        if min(query_timeout, hard_timeout, shutdown_timeout) <= 0 or respawn_wait < 0:
            raise ValueError("Timeouts must be positive and respawn_wait nonnegative")
        self.parallel = parallel
        self.query_timeout = float(query_timeout)
        self.hard_timeout = float(hard_timeout)
        self.respawn_wait = float(respawn_wait)
        self.shutdown_timeout = float(shutdown_timeout)
        self.max_respawns = max_respawns
        self.sdk_factory = sdk_factory
        self.worker_factory = worker_factory

    def fetch(self, requests, artifacts):
        """执行请求并返回原始响应与完整尝试序列。

        每次调用先恢复 artifacts.root 当前 Run 已落盘的有效响应，再只调度
        未完成请求。worker 故障时销毁旧会话并有限换新，恢复必须同时包含 Raw、
        请求身份和连续 attempt；预算耗尽则明确 INCOMPLETE，不伪造空成功。
        """
        requests = tuple(requests)
        self._validate_requests(requests)
        if not requests:
            return (), ()
        recovered, old_attempts = _load_evidence(requests, artifacts.root)
        attempts = list(old_attempts)
        next_attempt = {request.request_id: 1 for request in requests}
        for attempt in old_attempts:
            next_attempt[attempt.request_id] = max(next_attempt[attempt.request_id], attempt.attempt + 1)
        options = {
            "parallel": self.parallel, "query_timeout": self.query_timeout,
            "attempts": next_attempt,
        }
        for generation in range(self.max_respawns + 1):
            pending = tuple(request for request in requests if request.request_id not in recovered)
            if not pending:
                break
            if generation:
                # 已销毁旧会话后才允许等待并创建新会话，避免同账号多进程登录。
                time.sleep(self.respawn_wait)
            process = self._start_worker(pending, artifacts, options)
            process.join(self.hard_timeout)
            if process.is_alive():
                self._destroy(process)
            self._close(process)
            latest, loaded = _load_evidence(requests, artifacts.root)
            recovered, attempts = latest, loaded
            next_attempt = {request.request_id: 1 for request in requests}
            for attempt in attempts:
                next_attempt[attempt.request_id] = max(
                    next_attempt[attempt.request_id], attempt.attempt + 1,
                )
            options["attempts"] = next_attempt
        missing = [request for request in requests if request.request_id not in recovered]
        if missing:
            # 换进程预算耗尽仍无响应时留下明确 INCOMPLETE，而不是静默跳过导致 Raw 缺失。
            for request in missing:
                response = self._record_incomplete(
                    request, options["attempts"][request.request_id], artifacts,
                    f"Galaxy worker did not complete after {self.max_respawns} respawns",
                )
                recovered[request.request_id] = response
            _, attempts = _load_evidence(requests, artifacts.root)
        positions = {request.request_id: index for index, request in enumerate(requests)}
        ordered_attempts = sorted(
            attempts, key=lambda attempt: (positions[attempt.request_id], attempt.attempt),
        )
        return tuple(recovered[request.request_id] for request in requests), tuple(ordered_attempts)

    @staticmethod
    def _validate_requests(requests):
        if any(not isinstance(request, GalaxyRequest) for request in requests):
            raise ValueError("Invalid Galaxy request identity")
        ids = [request.request_id for request in requests]
        if len(ids) != len(set(ids)):
            raise ValueError("Galaxy request_id values must be unique")
        for request in requests:
            if (not request.request_id or "/" in request.request_id
                    or "\\" in request.request_id):
                raise ValueError("Invalid Galaxy request identity")
            if request.kind not in ("kline", "snapshot") or not request.codes:
                raise ValueError("Galaxy request kind and codes are required")
            if any(not isinstance(code, str) or code.count(".") != 1 for code in request.codes):
                raise ValueError("Galaxy codes must be full ts_code values")
            if len(set(request.codes)) != len(request.codes):
                raise ValueError("Galaxy request codes must be unique")
            # K 线接口的批量语义未纳入契约；快照单批不得超过供应商已验证的 200 只。
            if request.kind == "kline" and len(request.codes) != 1:
                raise ValueError("A Galaxy kline request must contain exactly one code")
            if request.kind == "snapshot" and len(request.codes) > 200:
                raise ValueError("A Galaxy snapshot request cannot exceed 200 codes")
            if request.start_date > request.end_date:
                raise ValueError("Galaxy start_date cannot exceed end_date")
            # 快照按交易日批量交付；跨日缺席无法在单个响应中可靠区分零成交与漏数。
            if request.kind == "snapshot" and request.start_date != request.end_date:
                raise ValueError("A Galaxy snapshot request must cover exactly one date")

    def _start_worker(self, requests, artifacts, options):
        """以 spawn 创建唯一活动 worker，避免继承父进程锁和 SDK 会话状态。"""
        args = (requests, artifacts, options, self.sdk_factory)
        if self.worker_factory is None:
            process = multiprocessing.get_context("spawn").Process(target=_worker_entry, args=args)
        else:
            process = self.worker_factory(target=_worker_entry, args=args)
        process.start()
        return process

    def _destroy(self, process):
        """先终止再强杀超时 worker，确保不会留下并行登录的旧会话。"""
        process.terminate()
        process.join(self.shutdown_timeout)
        if process.is_alive():
            process.kill()
            process.join(self.shutdown_timeout)

    @staticmethod
    def _close(process):
        close = getattr(process, "close", None)
        if close is not None:
            close()

    @staticmethod
    def _record_incomplete(request, attempt_number, artifacts, error):
        """为恢复预算耗尽的请求留下明确失败证据，不能静默略过该 Unit。"""
        requested_at = datetime.now(timezone.utc)
        response = GalaxyResponse(request, "INCOMPLETE", (), error, None)
        attempt = RequestAttempt(
            request.request_id, attempt_number, _api_name(request.kind), _request_params(request),
            (), requested_at, 0.0, None, None, (), 0, None, error,
        )
        artifacts.record_attempt(attempt, _response_data(response))
        return response
