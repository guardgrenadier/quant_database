"""Tushare 分钟切片适配器：单代码分页、pending 和全局请求限流。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Callable, Literal

import requests

from .errors import SourceContractError, SourceRequestError
from .tushare_transport import BASE_URL, RequestLimiter, TushareTransport


FIELDS = "ts_code,trade_time,freq,open,high,low,close,vol,amount"
LIMIT = 500
MAX_PAGES = 20


def row_signature(row: dict[str, Any]) -> tuple:
    """逐字段比较源值及类型，避免把不同 JSON 数字表示误判为相同。"""

    return tuple((type(row[name]).__name__, str(row[name])) for name in FIELDS.split(","))


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

        self.transport = TushareTransport(
            api_key=api_key, limiter=limiter or RequestLimiter(), session_factory=session_factory,
            base_url=base_url, retries=retries, retry_delay=retry_delay,
        )

    @property
    def limiter(self) -> RequestLimiter:
        """保留现有调用方使用的限流器访问方式。"""

        assert self.transport.limiter is not None
        return self.transport.limiter

    @limiter.setter
    def limiter(self, value: RequestLimiter) -> None:
        """允许测试或运行入口替换进程级请求预算。"""

        self.transport.limiter = value

    def parameters(self, code: str, day: date) -> dict[str, Any]:
        """返回计划中固定的代码、窗口和分页参数。"""

        text_day = day.isoformat()
        return {
            "ts_code": code, "freq": "1min", "start_date": f"{text_day} 09:30:00",
            "end_date": f"{text_day} 15:00:00", "fields": FIELDS,
            "limit": LIMIT, "offset": 0,
        }

    @property
    def request_count(self) -> int:
        """暴露本进程实际 HTTP 尝试数，供运行摘要使用。"""

        return self.limiter.count

    @property
    def remaining_budget(self) -> int | None:
        """返回尚可触发的请求次数；未设置预算时返回 None。"""

        return None if self.limiter.budget is None else max(0, self.limiter.budget - self.limiter.count)

    def fetch_slice(self, code: str, day: date) -> MinuteFetch:
        """逐页取完单代码切片；遇 pending 立即返回，留给后续 run。"""

        params = self.parameters(code, day)
        pages: list[MinutePage] = []
        records: list[dict[str, Any]] = []
        seen: dict[tuple[str, str], dict] = {}
        previous: set[tuple[str, str]] | None = None
        for index in range(MAX_PAGES):
            params["offset"] = index * LIMIT
            try:
                page, pending, retry_after, attempts = self._query(params)
            except (SourceContractError, SourceRequestError) as exc:
                exc.requests = records + exc.requests
                raise
            records.extend(attempts)
            if pending:
                return MinuteFetch("pending", requests=records, retry_after=retry_after)
            assert page is not None
            try:
                previous = self._check_page(code, day, page.rows, seen, previous)
            except ValueError as exc:
                raise SourceContractError(str(exc), page.body, records) from exc
            pages.append(page)
            if len(page.rows) < LIMIT:
                return MinuteFetch(
                    "ok" if any(item.rows for item in pages) else "empty",
                    pages, records, datetime.now(timezone.utc),
                )
        raise SourceContractError(f"pagination exceeded {MAX_PAGES} pages", pages[-1].body, records)

    def rows_from_raw(self, code: str, day: date, pages: list[dict]) -> list[dict]:
        """重放 raw 时重新验证分页、范围和主键，不信任孤儿文件。"""

        if not pages or len(pages) > MAX_PAGES:
            raise ValueError("invalid raw page count")
        seen: dict[tuple[str, str], dict] = {}
        previous: set[tuple[str, str]] | None = None
        rows: list[dict] = []
        for index, payload in enumerate(pages):
            page_rows = self._decode_payload(payload, json.dumps(payload, default=str).encode())
            if index < len(pages) - 1 and len(page_rows) != LIMIT:
                raise ValueError("raw pagination ended before last page")
            previous = self._check_page(code, day, page_rows, seen, previous)
            rows.extend(page_rows)
        if len(page_rows) >= LIMIT:
            raise ValueError("raw pagination lacks a short final page")
        return rows

    def _query(self, params: dict[str, Any]) -> tuple[MinutePage | None, bool, float | None, list[dict]]:
        """解释 HTTP 202 与成功页；连接、重试和逐次记录交给传输层。"""

        response = self.transport.get("stk_mins", params)
        if response.status_code == 202:
            try:
                payload = json.loads(response.body)
            except ValueError as exc:
                raise SourceContractError("invalid pending response", response.body, response.attempts) from exc
            if isinstance(payload, dict) and payload.get("error") == "minute_data_pending":
                try:
                    retry_after = float(response.headers.get("retry-after", 0))
                except ValueError:
                    retry_after = None
                return None, True, retry_after, response.attempts
            raise SourceContractError("unexpected HTTP 202 response", response.body, response.attempts)
        try:
            rows = self._decode(response.body)
        except SourceContractError as exc:
            exc.requests = response.attempts
            raise
        return MinutePage(response.body, rows), False, None, response.attempts

    def _decode(self, body: bytes) -> list[dict[str, Any]]:
        """验证 Tushare 成功信封和固定字段，保留源字段名。"""

        try:
            payload = json.loads(body, parse_float=Decimal)
        except ValueError as exc:
            raise SourceContractError("invalid minute JSON response", body) from exc
        return self._decode_payload(payload, body)

    def _decode_payload(self, payload: Any, body: bytes) -> list[dict[str, Any]]:
        """共用实时响应与 raw 重放的字段信封检查。"""

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

        if len(rows) > LIMIT:
            raise ValueError("minute page exceeds requested limit")
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
                if row_signature(seen[key]) != row_signature(row):
                    raise ValueError("minute key has conflicting values")
            else:
                seen[key] = row
                new_count += 1
            keys.add(key)
        if rows and (keys == previous or (previous is not None and new_count == 0)):
            raise ValueError("pagination repeated a page or cursor did not advance")
        return keys
