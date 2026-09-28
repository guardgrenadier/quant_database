"""Tushare 分钟切片适配器：单代码分页、pending 和全局请求限流。"""

from __future__ import annotations

import json
import os
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Callable, Literal

import requests

from .errors import SourceContractError


BASE_URL = "https://pcd.mobcvb.cn/tushare/pro"
FIELDS = "ts_code,trade_time,freq,open,high,low,close,vol,amount"
LIMIT = 500
MAX_PAGES = 20


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
class MinutePage:
    """HTTP 200 单页的响应原文和解析行。"""

    body: bytes
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class MinuteFetch:
    """完整切片或未收敛 pending 的返回结果。"""

    status: Literal["ok", "empty", "pending"]
    pages: list[MinutePage] = field(default_factory=list)
    requests: list[dict[str, Any]] = field(default_factory=list)
    extracted_at: datetime | None = None
    retry_after: float | None = None

    @property
    def rows(self) -> list[dict[str, Any]]:
        """按原页序返回全部行，不在 L1 过滤相同重复行。"""

        return [row for page in self.pages for row in page.rows]


class TushareMinuteSource:
    """把一只证券的一天转换为固定窗口的分钟请求。"""

    def __init__(
        self, *, api_key: str | None = None, limiter: RequestLimiter | None = None,
        session_factory: Callable[[], requests.Session] = requests.Session,
        base_url: str = BASE_URL, retries: int = 2, retry_delay: float = 15,
    ) -> None:
        """每线程独立 Session，共享进程级限流器。"""

        self.api_key = api_key or os.getenv("TUSHARE_API_KEY")
        if not self.api_key:
            raise ValueError("TUSHARE_API_KEY is required")
        if not base_url.startswith("https://") or retries < 0 or retry_delay < 0:
            raise ValueError("invalid minute source configuration")
        self.limiter = limiter or RequestLimiter()
        self.session_factory = session_factory
        self.local = threading.local()
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.retry_delay = retry_delay

    def parameters(self, code: str, day: date) -> dict[str, Any]:
        """返回计划中固定的代码、窗口和分页参数。"""

        text_day = day.isoformat()
        return {
            "ts_code": code, "freq": "1min", "start_date": f"{text_day} 09:30:00",
            "end_date": f"{text_day} 15:00:00", "fields": FIELDS,
            "limit": LIMIT, "offset": 0,
        }

    def fetch_slice(self, code: str, day: date) -> MinuteFetch:
        """逐页取完单代码切片；遇 pending 立即返回，留给后续 run。"""

        params = self.parameters(code, day)
        pages: list[MinutePage] = []
        records: list[dict[str, Any]] = []
        seen: dict[tuple[str, str], dict] = {}
        previous: set[tuple[str, str]] | None = None
        for index in range(MAX_PAGES):
            params["offset"] = index * LIMIT
            page, pending, retry_after, attempts = self._query(params)
            records.extend(attempts)
            if pending:
                return MinuteFetch("pending", requests=records, retry_after=retry_after)
            assert page is not None
            try:
                previous = self._check_page(code, day, page.rows, seen, previous)
            except ValueError as exc:
                raise SourceContractError(str(exc), page.body) from exc
            pages.append(page)
            if len(page.rows) < LIMIT:
                return MinuteFetch(
                    "ok" if any(item.rows for item in pages) else "empty",
                    pages, records, datetime.now(timezone.utc),
                )
        raise SourceContractError(f"pagination exceeded {MAX_PAGES} pages", pages[-1].body)

    def rows_from_raw(self, code: str, day: date, pages: list[dict]) -> list[dict]:
        """重放 raw 时重新验证分页、范围和主键，不信任孤儿文件。"""

        if not pages or len(pages) > MAX_PAGES:
            raise ValueError("invalid raw page count")
        seen: dict[tuple[str, str], dict] = {}
        previous: set[tuple[str, str]] | None = None
        rows: list[dict] = []
        for index, payload in enumerate(pages):
            body = json.dumps(payload).encode()
            page_rows = self._decode(body)
            if index < len(pages) - 1 and len(page_rows) != LIMIT:
                raise ValueError("raw pagination ended before last page")
            previous = self._check_page(code, day, page_rows, seen, previous)
            rows.extend(page_rows)
        if len(page_rows) >= LIMIT:
            raise ValueError("raw pagination lacks a short final page")
        return rows

    def _session(self) -> requests.Session:
        """懒加载当前线程独享的 HTTP Session。"""

        if not hasattr(self.local, "session"):
            self.local.session = self.session_factory()
        return self.local.session

    def _query(self, params: dict[str, Any]) -> tuple[MinutePage | None, bool, float | None, list[dict]]:
        """执行单页请求，记录每次 HTTP 尝试；202 不在本轮等待。"""

        attempts: list[dict] = []
        for attempt in range(self.retries + 1):
            self.limiter.acquire()
            requested_at = datetime.now(timezone.utc).isoformat()
            try:
                response = self._session().get(
                    f"{self.base_url}/stk_mins", params=params,
                    headers={"X-API-Key": self.api_key}, timeout=30, verify=True,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                attempts.append({"at": requested_at, "offset": params["offset"], "http_status": None,
                                 "error": type(exc).__name__})
                if attempt == self.retries:
                    raise RuntimeError("stk_mins transport retry exhausted") from exc
                time.sleep(self.retry_delay * 2**attempt)
                continue
            attempts.append({"at": requested_at, "offset": params["offset"],
                             "http_status": response.status_code,
                             "request_id": response.headers.get("X-Request-ID")})
            body = response.content
            if response.status_code == 202:
                try:
                    payload = json.loads(body)
                except ValueError as exc:
                    raise SourceContractError("invalid pending response", body) from exc
                if isinstance(payload, dict) and payload.get("error") == "minute_data_pending":
                    try:
                        retry_after = float(response.headers.get("Retry-After", 0))
                    except ValueError:
                        retry_after = None
                    return None, True, retry_after, attempts
                raise SourceContractError("unexpected HTTP 202 response", body)
            if response.status_code == 503:
                try:
                    unavailable = json.loads(body).get("error") == "data_source_unavailable"
                except (ValueError, AttributeError):
                    unavailable = False
                if unavailable:
                    raise RuntimeError("stk_mins data source is unavailable")
            if response.status_code in {429, 503} and attempt < self.retries:
                delay = self.retry_delay * 2**attempt
                if response.status_code == 429:
                    try:
                        delay = max(delay, float(response.headers.get("Retry-After", 0)))
                    except ValueError:
                        pass
                time.sleep(delay)
                continue
            if response.status_code != 200:
                raise RuntimeError(f"stk_mins HTTP {response.status_code}")
            return MinutePage(body, self._decode(body)), False, None, attempts
        raise AssertionError("retry loop exhausted")

    def _decode(self, body: bytes) -> list[dict[str, Any]]:
        """验证 Tushare 成功信封和固定字段，保留源字段名。"""

        try:
            payload = json.loads(body)
        except ValueError as exc:
            raise SourceContractError("invalid minute JSON response", body) from exc
        if not isinstance(payload, dict) or str(payload.get("code")) != "0":
            raise SourceContractError("minute supplier code is not zero", body)
        data = payload.get("data")
        if not isinstance(data, dict) or data.get("fields") != FIELDS.split(","):
            raise SourceContractError("minute response fields differ from request", body)
        items = data.get("items")
        if not isinstance(items, list) or any(not isinstance(item, list) or len(item) != len(FIELDS.split(",")) for item in items):
            raise SourceContractError("minute response rows do not match fields", body)
        return [dict(zip(FIELDS.split(","), item)) for item in items]

    def _check_page(
        self, code: str, day: date, rows: list[dict],
        seen: dict[tuple[str, str], dict], previous: set[tuple[str, str]] | None,
    ) -> set[tuple[str, str]]:
        """检查请求边界和冲突重复行；相同重复行保留到 temp 再去重。"""

        start = f"{day} 09:30:00"
        end = f"{day} 15:00:00"
        keys: set[tuple[str, str]] = set()
        new_count = 0
        for row in rows:
            stamp = row["trade_time"]
            if row["ts_code"] != code or not isinstance(stamp, str):
                raise ValueError("minute row is outside requested code or has invalid time")
            try:
                datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S")
            except ValueError as exc:
                raise ValueError("minute row has invalid time format") from exc
            if not start <= stamp <= end:
                raise ValueError("minute row is outside requested window")
            key = (code, stamp)
            if key in seen:
                if seen[key] != row:
                    raise ValueError("minute key has conflicting values")
            else:
                seen[key] = row
                new_count += 1
            keys.add(key)
        if rows and (keys == previous or (previous is not None and new_count == 0)):
            raise ValueError("pagination repeated a page or cursor did not advance")
        return keys
