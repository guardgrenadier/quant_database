"""三类基础数据集的 Tushare 请求形态与分页协议。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any

import requests

from .errors import SourceContractError, SourceRequestError
from .tushare_transport import BASE_URL, TushareTransport


FIELDS = {
    "stock_basic": "ts_code,symbol,name,exchange,market,list_status,list_date,delist_date",
    "trade_cal": "exchange,cal_date,is_open,pretrade_date",
    "suspend_d": "ts_code,trade_date,suspend_type",
}
LIMIT = {"stock_basic": 10000, "trade_cal": 500, "suspend_d": 5000}


def decode_page(dataset: str, body: bytes) -> list[dict[str, Any]]:
    """校验成功响应的信封和字段，供实时抓取与 raw 重建共用。"""

    try:
        payload = json.loads(body)
    except ValueError as exc:
        raise ValueError(f"{dataset}: invalid JSON response") from exc
    if not isinstance(payload, dict) or str(payload.get("code")) != "0":
        raise ValueError(f"{dataset}: supplier response code is not zero")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise ValueError(f"{dataset}: missing data object")
    fields, items = data.get("fields"), data.get("items")
    if fields != FIELDS[dataset].split(",") or not isinstance(items, list):
        raise ValueError(f"{dataset}: unexpected response fields or items")
    if any(not isinstance(item, list) or len(item) != len(fields) for item in items):
        raise ValueError(f"{dataset}: response row does not match fields")
    return [dict(zip(fields, item)) for item in items]


def check_page(dataset: str, day: date, rows: list[dict], seen: set[tuple]) -> None:
    """检查代码、日期与分页唯一性；不判断业务值是否合理。"""

    for row in rows:
        if dataset == "stock_basic":
            key = (row["ts_code"],)
            if not row["ts_code"] or row["list_status"] not in {"L", "D", "P", "G", "UN"}:
                raise ValueError("stock_basic code or status violates request contract")
        elif dataset == "trade_cal":
            key = (row["exchange"], row["cal_date"])
            if row["exchange"] != "SSE" or not str(row["cal_date"]).startswith(str(day.year)):
                raise ValueError("trade_cal row is outside request scope")
        else:
            key = (row["ts_code"], row["trade_date"])
            if row["trade_date"] != day.strftime("%Y%m%d") or row["suspend_type"] != "S":
                raise ValueError("suspend_d row is outside request scope")
        if key in seen:
            raise ValueError(f"{dataset}: duplicate key across response pages")
        seen.add(key)


def check_complete(dataset: str, day: date, lengths: list[int], rows: list[dict]) -> None:
    """核对尾页、单页快照和目标日期，供抓取与坠机恢复共用。"""

    limit = LIMIT[dataset]
    if not lengths or len(lengths) > 20 or any(size != limit for size in lengths[:-1]) or lengths[-1] >= limit:
        raise ValueError(f"{dataset}: pagination did not end on a short page")
    if dataset == "stock_basic" and (len(lengths) != 1 or not rows):
        raise ValueError("stock_basic full-status snapshot is empty or paginated")
    if dataset == "trade_cal" and not any(row["cal_date"] == day.strftime("%Y%m%d") for row in rows):
        raise ValueError("trade_cal snapshot does not contain target date")


@dataclass(frozen=True)
class Page:
    """单次成功请求的原文、字段和行数据。"""

    body: bytes
    rows: list[dict[str, Any]]
    request_id: str | None
    attempts: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class FetchResult:
    """一个基础数据集切片完整分页后的结果。"""

    pages: list[Page]
    extracted_at: datetime

    @property
    def rows(self) -> list[dict[str, Any]]:
        """按页序连接响应行，供协议检查和临时 Parquet 转换。"""

        return [row for page in self.pages for row in page.rows]


class TushareBasicsSource:
    """将基础数据集适配为单日切片，向 pipeline 隐藏供应商参数。"""

    def __init__(
        self, *, api_key: str | None = None, session: requests.Session | None = None,
        base_url: str = BASE_URL, retries: int = 2, retry_delay: float = 15,
    ) -> None:
        """绑定网关会话；密钥仅从参数或环境变量读取。"""

        self.transport = TushareTransport(
            api_key=api_key, session_factory=(lambda: session) if session is not None else requests.Session,
            base_url=base_url, retries=retries, retry_delay=retry_delay,
        )

    def parameters(self, dataset: str, day: date) -> dict[str, Any]:
        """返回固定请求参数，供抓取前落账与续跑比对。"""

        if dataset == "stock_basic":
            return {"list_status": "L,D,P,G,UN", "fields": FIELDS[dataset], "limit": LIMIT[dataset], "offset": 0}
        if dataset == "trade_cal":
            return {
                "exchange": "SSE", "start_date": f"{day.year}0101", "end_date": f"{day.year}1231",
                "fields": FIELDS[dataset], "limit": LIMIT[dataset], "offset": 0,
            }
        if dataset == "suspend_d":
            return {
                "trade_date": day.strftime("%Y%m%d"), "suspend_type": "S",
                "fields": FIELDS[dataset], "limit": LIMIT[dataset], "offset": 0,
            }
        raise ValueError(f"unsupported dataset: {dataset}")

    def columns(self, dataset: str) -> list[str]:
        """返回临时 Parquet 需要保留的源字段顺序。"""

        return FIELDS[dataset].split(",")

    def rows_from_raw(self, dataset: str, day: date, pages: list[dict]) -> list[dict]:
        """从已保存的响应页重建行，并复核供应商协议和请求范围。"""

        rows: list[dict] = []
        lengths: list[int] = []
        seen: set[tuple] = set()
        for page in pages:
            page_rows = decode_page(dataset, json.dumps(page).encode())
            check_page(dataset, day, page_rows, seen)
            rows.extend(page_rows)
            lengths.append(len(page_rows))
        check_complete(dataset, day, lengths, rows)
        return rows

    def fetch(self, dataset: str, day: date) -> FetchResult:
        """按固定页宽取完整切片；stock_basic 使用实测可靠的单页形态。"""

        params = self.parameters(dataset, day)
        limit = LIMIT[dataset]
        pages: list[Page] = []
        attempts: list[dict] = []
        seen: set[tuple] = set()
        for index in range(20):
            params["offset"] = index * limit
            try:
                page = self._query(dataset, params)
            except (SourceContractError, SourceRequestError) as exc:
                exc.requests = attempts + exc.requests
                raise
            attempts.extend(page.attempts)
            try:
                check_page(dataset, day, page.rows, seen)
            except ValueError as exc:
                raise SourceContractError(str(exc), page.body, attempts) from exc
            pages.append(page)
            if len(page.rows) < limit:
                try:
                    check_complete(dataset, day, [len(p.rows) for p in pages], [item for p in pages for item in p.rows])
                except ValueError as exc:
                    raise SourceContractError(str(exc), page.body, attempts) from exc
                return FetchResult(pages, datetime.now(timezone.utc))
            if dataset == "stock_basic":
                raise SourceContractError("stock_basic reached its single-page limit; split strategy needs verification", page.body, attempts)
        raise SourceContractError(f"{dataset} exceeded 20 pages", pages[-1].body, attempts)

    def _query(self, dataset: str, params: dict[str, Any]) -> Page:
        """只解释基础数据集的成功响应；HTTP 重试由共用传输层负责。"""

        response = self.transport.get(dataset, params)
        if response.status_code != 200:
            raise SourceRequestError(f"{dataset}: HTTP {response.status_code}", response.attempts)
        try:
            rows = decode_page(dataset, response.body)
        except ValueError as exc:
            raise SourceContractError(str(exc), response.body, response.attempts) from exc
        return Page(response.body, rows, response.headers.get("x-request-id"), response.attempts)
