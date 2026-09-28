"""Tushare 兼容 ProMax 网关的单次调用传输封装。"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal

import requests

LOG = logging.getLogger(__name__)
DEFAULT_BASE_URL = "https://pcd.mobcvb.cn/tushare/pro"
RETRY_STATUSES = {429, 502, 503, 504}


@dataclass(frozen=True)
class RequestRecord:
    """一次真实 HTTP 尝试的脱敏追溯信息。"""

    requested_at: str
    api_name: str
    params: dict[str, Any]
    attempt: int
    http_status: int | None
    elapsed_ms: int
    retry_after: float | None = None
    rate_limit_remaining: str | None = None
    request_id: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class QueryResult:
    """一次 API 调用的业务结果；pending 与空结果都不是异常。"""

    status: Literal["rows", "empty", "pending"]
    rows: list[dict[str, Any]] = field(default_factory=list)
    retry_after: float | None = None
    headers: dict[str, str | None] = field(default_factory=dict)
    requests: list[RequestRecord] = field(default_factory=list)


class TushareError(Exception):
    """网关返回业务错误或结构无效的响应。"""

    def __init__(self, message: str, *, requests: list[RequestRecord] | None = None) -> None:
        """保存错误信息及异常发生前已经完成的 HTTP 尝试。"""

        super().__init__(message)
        self.requests = requests or []


class AuthenticationError(TushareError):
    """API key 缺失或被服务端拒绝。"""


class TransportError(TushareError):
    """HTTP 连接、状态码或传输层响应失败。"""


class TushareClient:
    """执行可靠的单次 API 调用，不负责分页、落盘或数据语义判断。"""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float = 30,
        verify: bool = True,
        session: requests.Session | None = None,
        max_retries: int = 5,
        retry_delay: float = 15.0,
        request_budget: int | None = None,
    ) -> None:
        """配置连接、有限重试和可选的进程级请求硬预算。"""

        if not base_url.startswith("https://"):
            raise ValueError("base_url must use HTTPS")
        if (
            timeout <= 0
            or max_retries < 0
            or retry_delay < 0
            or (request_budget is not None and request_budget <= 0)
        ):
            raise ValueError("timeout, max_retries, or retry_delay is invalid")
        self.api_key = api_key if api_key is not None else os.getenv("TUSHARE_API_KEY")
        if not self.api_key:
            raise AuthenticationError("TUSHARE_API_KEY is required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.verify = verify
        self.session = session if session is not None else requests.Session()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.request_budget = request_budget
        self.request_count = 0

    def query(self, api_name: str, **params: Any) -> QueryResult:
        """调用一个 endpoint，返回 rows、empty 或 pending；硬错误抛异常。"""

        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", api_name):
            raise ValueError("api_name must be a Tushare endpoint name")

        # 请求参数可以完整追溯，但疑似凭证的键和值必须先脱敏。
        params = {name: value for name, value in params.items() if value is not None}
        safe_params = _safe_params(params, self.api_key)
        records: list[RequestRecord] = []
        started = time.monotonic()
        try:
            for attempt in range(self.max_retries + 1):
                requested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
                attempt_started = time.monotonic()
                # 预算按真实 HTTP attempt 计数，重试也必须消耗预算。
                if self.request_budget is not None and self.request_count >= self.request_budget:
                    raise TransportError(f"{api_name}: HTTP request budget {self.request_budget} exhausted", requests=records)
                self.request_count += 1
                try:
                    response = self.session.get(
                        f"{self.base_url}/{api_name}",
                        params=params,
                        headers={"X-API-Key": self.api_key},
                        timeout=self.timeout,
                        verify=self.verify,
                    )
                except (requests.Timeout, requests.ConnectionError) as exc:
                    # 连接类瞬时错误使用同一有限指数退避，不把异常细节写入日志。
                    records.append(
                        RequestRecord(
                            requested_at,
                            api_name,
                            safe_params,
                            attempt + 1,
                            None,
                            round((time.monotonic() - attempt_started) * 1000),
                            error=type(exc).__name__,
                        )
                    )
                    if attempt == self.max_retries:
                        raise TransportError(f"{api_name}: connection failed", requests=records) from exc
                    LOG.warning("Tushare %s retrying after connection error", api_name)
                    time.sleep(self.retry_delay * 2**attempt)
                    continue
                except requests.RequestException as exc:
                    records.append(
                        RequestRecord(
                            requested_at,
                            api_name,
                            safe_params,
                            attempt + 1,
                            None,
                            round((time.monotonic() - attempt_started) * 1000),
                            error=type(exc).__name__,
                        )
                    )
                    raise TransportError(f"{api_name}: request failed", requests=records) from exc

                try:
                    payload = response.json()
                except ValueError:
                    payload = None
                gateway_error = payload.get("error") if isinstance(payload, dict) else None
                headers = _response_headers(response.headers)
                retry_after = _float_or_none(headers["Retry-After"])
                records.append(
                    RequestRecord(
                        requested_at,
                        api_name,
                        safe_params,
                        attempt + 1,
                        response.status_code,
                        round((time.monotonic() - attempt_started) * 1000),
                        retry_after,
                        headers["X-RateLimit-Remaining"],
                        headers["X-Request-ID"],
                        gateway_error,
                    )
                )

                # pending 是正常中间态，必须先于通用 HTTP 错误分类返回。
                if response.status_code == 202 and gateway_error == "minute_data_pending":
                    return QueryResult("pending", retry_after=retry_after, headers=headers, requests=records)

                # 裸 503 允许没有 JSON；data_source_unavailable 则视为确定性硬错误。
                if (
                    response.status_code in RETRY_STATUSES
                    and gateway_error != "data_source_unavailable"
                    and attempt < self.max_retries
                ):
                    delay = self.retry_delay * 2**attempt
                    if response.status_code == 429 and retry_after is not None:
                        delay = max(delay, retry_after)
                    LOG.warning("Tushare %s retrying after HTTP %s", api_name, response.status_code)
                    time.sleep(delay)
                    continue

                # 重试耗尽后再区分鉴权、非 JSON HTTP 错误和无效成功响应。
                if payload is None:
                    if response.status_code in (401, 403):
                        raise AuthenticationError(f"{api_name}: HTTP {response.status_code}", requests=records)
                    if response.status_code >= 400:
                        raise TransportError(
                            f"{api_name}: HTTP {response.status_code}: non-JSON response", requests=records
                        )
                    raise TushareError(f"{api_name}: invalid JSON response", requests=records)

                if response.status_code >= 400 or (isinstance(payload, dict) and payload.get("ok") is False):
                    message_value = payload.get("message") or payload.get("msg") if isinstance(payload, dict) else None
                    detail = ": ".join(
                        str(value).replace(self.api_key, "[redacted]")[:200]
                        for value in (gateway_error, message_value)
                        if value
                    )
                    reason = f"HTTP {response.status_code}" if response.status_code >= 400 else "gateway error"
                    message = f"{api_name}: {reason}" + (f": {detail}" if detail else "")
                    error_type = (
                        AuthenticationError
                        if response.status_code in (401, 403)
                        else TransportError if response.status_code >= 400 else TushareError
                    )
                    raise error_type(message, requests=records)

                # 只有 envelope 完整且字段与每行严格对齐时，才向上层暴露记录。
                rows = _parse_rows(api_name, payload, records)
                LOG.info(
                    "Tushare %s params=%s rows=%d elapsed_ms=%.0f",
                    api_name,
                    safe_params,
                    len(rows),
                    (time.monotonic() - started) * 1000,
                )
                return QueryResult("rows" if rows else "empty", rows, headers=headers, requests=records)
        except TushareError as exc:
            LOG.warning("Tushare %s failed: %s", api_name, exc)
            raise

        raise AssertionError("retry loop exhausted")


def _response_headers(headers: Any) -> dict[str, str | None]:
    """只提取等待、限流和追踪所需的响应头。"""

    return {
        "Retry-After": headers.get("Retry-After"),
        "X-RateLimit-Remaining": headers.get("X-RateLimit-Remaining"),
        "X-Request-ID": headers.get("X-Request-ID"),
    }


def _safe_params(params: dict[str, Any], api_key: str) -> dict[str, Any]:
    """复制请求参数并遮蔽凭证键及值中意外出现的 API key。"""

    safe: dict[str, Any] = {}
    for name, value in params.items():
        if any(marker in name.lower() for marker in ("key", "token", "secret", "password")):
            safe[name] = "[redacted]"
        elif isinstance(value, str):
            safe[name] = value.replace(api_key, "[redacted]")
        else:
            safe[name] = value
    return safe


def _float_or_none(value: Any) -> float | None:
    """把可解析的响应头转为浮点数，缺失或非法值返回 ``None``。"""

    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _parse_rows(api_name: str, payload: Any, records: list[RequestRecord]) -> list[dict[str, Any]]:
    """校验 Tushare envelope，并把 fields/items 无损组装为行字典。"""

    if not isinstance(payload, dict) or "code" not in payload:
        raise TushareError(f"{api_name}: invalid response envelope", requests=records)
    code = str(payload["code"])
    if code != "0":
        error_type = AuthenticationError if code in {"2002", "401", "403"} else TushareError
        raise error_type(
            f"{api_name}: supplier code {code}: {payload.get('msg') or 'unknown error'}", requests=records
        )

    data = payload.get("data")
    if not isinstance(data, dict):
        raise TushareError(f"{api_name}: missing data object", requests=records)
    fields, items = data.get("fields"), data.get("items")
    if (
        not isinstance(fields, list)
        or not all(isinstance(field, str) for field in fields)
        or len(set(fields)) != len(fields)
        or not isinstance(items, list)
    ):
        raise TushareError(f"{api_name}: invalid fields or items", requests=records)
    if any(not isinstance(item, list) or len(item) != len(fields) for item in items):
        raise TushareError(f"{api_name}: row does not match fields", requests=records)
    return [dict(zip(fields, item)) for item in items]
