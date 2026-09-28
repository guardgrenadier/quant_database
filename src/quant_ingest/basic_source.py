"""三类基础数据集的 Tushare 请求形态与分页协议。"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

import requests

from .errors import SourceContractError


BASE_URL = "https://pcd.mobcvb.cn/tushare/pro"
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

        self.api_key = api_key or os.getenv("TUSHARE_API_KEY")
        if not self.api_key:
            raise ValueError("TUSHARE_API_KEY is required")
        if not base_url.startswith("https://") or retries < 0 or retry_delay < 0:
            raise ValueError("invalid source configuration")
        self.session = session or requests.Session()
        self.base_url = base_url.rstrip("/")
        self.retries = retries
        self.retry_delay = retry_delay

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
        seen: set[tuple] = set()
        for index in range(20):
            params["offset"] = index * limit
            page = self._query(dataset, params)
            try:
                check_page(dataset, day, page.rows, seen)
            except ValueError as exc:
                raise SourceContractError(str(exc), page.body) from exc
            pages.append(page)
            if len(page.rows) < limit:
                try:
                    check_complete(dataset, day, [len(p.rows) for p in pages], [item for p in pages for item in p.rows])
                except ValueError as exc:
                    raise SourceContractError(str(exc), page.body) from exc
                return FetchResult(pages, datetime.now(timezone.utc))
            if dataset == "stock_basic":
                raise SourceContractError("stock_basic reached its single-page limit; split strategy needs verification", page.body)
        raise SourceContractError(f"{dataset} exceeded 20 pages", pages[-1].body)

    def _query(self, dataset: str, params: dict[str, Any]) -> Page:
        """执行单页请求，有限重试瞬时 HTTP 错误并保留成功响应原文。"""

        for attempt in range(self.retries + 1):
            try:
                response = self.session.get(
                    f"{self.base_url}/{dataset}", params=params,
                    headers={"X-API-Key": self.api_key}, timeout=30, verify=True,
                )
            except (requests.Timeout, requests.ConnectionError):
                if attempt == self.retries:
                    raise RuntimeError(f"{dataset}: transport retry exhausted") from None
                time.sleep(self.retry_delay * 2**attempt)
                continue
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
                raise RuntimeError(f"{dataset}: HTTP {response.status_code}")
            body = response.content
            try:
                rows = decode_page(dataset, body)
            except ValueError as exc:
                raise SourceContractError(str(exc), body) from exc
            return Page(body, rows, response.headers.get("X-Request-ID"))
        raise AssertionError("retry loop exhausted")
