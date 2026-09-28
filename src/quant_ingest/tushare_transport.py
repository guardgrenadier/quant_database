"""Tushare 共用 HTTP 传输：认证、重试、限流和脱敏请求记录。"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

import requests

from .errors import SourceRequestError


BASE_URL = "https://pcd.mobcvb.cn/tushare/pro"


class RequestLimiter:
    """跨工作线程共享的滚动分钟限流器与进程请求预算。"""

    def __init__(
        self, max_per_minute: int = 180, budget: int | None = None,
        *, clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """设置滚动 60 秒窗口；每次真实 HTTP 尝试都需先 acquire。"""

        if max_per_minute <= 0 or (budget is not None and budget <= 0):
            raise ValueError("invalid request limits")
        self.max_per_minute = max_per_minute
        self.budget = budget
        self.clock = clock
        self.sleep = sleep
        self._lock = threading.Lock()
        self._recent: deque[float] = deque()
        self.count = 0

    def acquire(self) -> None:
        """等待限流窗口腾出位置；预算耗尽时直接拒绝新请求。"""

        while True:
            with self._lock:
                now = self.clock()
                while self._recent and now - self._recent[0] >= 60:
                    self._recent.popleft()
                if self.budget is not None and self.count >= self.budget:
                    raise RuntimeError("HTTP request budget exhausted")
                if len(self._recent) < self.max_per_minute:
                    self._recent.append(now)
                    self.count += 1
                    return
                delay = max(0.001, 60 - (now - self._recent[0]))
            self.sleep(delay)


@dataclass(frozen=True)
class TransportResponse:
    """单次 API 调用完成后的 HTTP 信息，不解释业务信封。"""

    status_code: int
    body: bytes
    headers: dict[str, str]
    attempts: list[dict[str, Any]]


class TushareTransport:
    """执行一个 endpoint 的 HTTP 调用，保留原文并记录每次尝试。"""

    def __init__(
        self, *, api_key: str | None = None,
        session_factory: Callable[[], requests.Session] = requests.Session,
        limiter: RequestLimiter | None = None, base_url: str = BASE_URL,
        retries: int = 2, retry_delay: float = 15,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        """绑定认证和重试参数；每个线程按需创建独立 Session。"""

        self.api_key = api_key or os.getenv("TUSHARE_API_KEY")
        if not self.api_key:
            raise ValueError("TUSHARE_API_KEY is required")
        if not base_url.startswith("https://") or retries < 0 or retry_delay < 0:
            raise ValueError("invalid Tushare transport configuration")
        self.session_factory = session_factory
        self.limiter = limiter
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.retry_delay = retry_delay
        self.sleep = sleep
        self.local = threading.local()

    def get(self, endpoint: str, params: dict[str, Any]) -> TransportResponse:
        """返回 200/202 原文；其他 HTTP 结果在有限重试后报传输错误。"""

        attempts: list[dict[str, Any]] = []
        for attempt in range(self.retries + 1):
            if self.limiter is not None:
                try:
                    self.limiter.acquire()
                except RuntimeError as exc:
                    raise SourceRequestError(f"{endpoint}: {exc}", attempts) from exc
            requested_at = datetime.now(timezone.utc).isoformat()
            record = {"at": requested_at, "endpoint": endpoint, "offset": params.get("offset")}
            try:
                response = self._session().get(
                    f"{self.base_url}/{endpoint}", params=params,
                    headers={"X-API-Key": self.api_key}, timeout=30, verify=True,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                attempts.append({**record, "http_status": None, "error": type(exc).__name__})
                if attempt == self.retries:
                    raise SourceRequestError(f"{endpoint}: transport retry exhausted", attempts) from exc
                self.sleep(self.retry_delay * 2**attempt)
                continue
            except requests.RequestException as exc:
                attempts.append({**record, "http_status": None, "error": type(exc).__name__})
                raise SourceRequestError(f"{endpoint}: request failed", attempts) from exc

            body = response.content
            # HTTP 头大小写不固定；统一小写后交给适配器读取。
            headers = {str(name).lower(): str(value) for name, value in response.headers.items()}
            attempts.append({**record, "http_status": response.status_code,
                             "request_id": headers.get("x-request-id")})
            if response.status_code == 503 and self._unavailable(body):
                raise SourceRequestError(f"{endpoint}: data source is unavailable", attempts)
            if response.status_code in {429, 503} and attempt < self.retries:
                delay = self.retry_delay * 2**attempt
                if response.status_code == 429:
                    try:
                        delay = max(delay, float(headers.get("retry-after", 0)))
                    except ValueError:
                        pass
                self.sleep(delay)
                continue
            if response.status_code not in {200, 202}:
                raise SourceRequestError(f"{endpoint}: HTTP {response.status_code}", attempts)
            return TransportResponse(response.status_code, body, headers, attempts)
        raise AssertionError("retry loop exhausted")

    def _session(self) -> requests.Session:
        """让同一线程复用连接，同时避免多个线程共享 Session。"""

        if not hasattr(self.local, "session"):
            self.local.session = self.session_factory()
        return self.local.session

    @staticmethod
    def _unavailable(body: bytes) -> bool:
        """识别供应商明确宣告的确定性 503，跳过无益的重试。"""

        try:
            payload = json.loads(body)
        except ValueError:
            return False
        return isinstance(payload, dict) and payload.get("error") == "data_source_unavailable"
