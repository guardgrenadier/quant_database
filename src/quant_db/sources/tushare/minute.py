"""把 Tushare ``stk_mins`` 适配为“单证券、单交易日”的完整切片。"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Literal

import pyarrow as pa

from .client import RequestRecord, TushareClient, TushareError

FIELDS = "ts_code,trade_time,freq,open,high,low,close,vol,amount"
FIELD_NAMES = set(FIELDS.split(","))
LIMIT = 500
MAX_PAGES = 20
RAW_SCHEMA = pa.schema(
    [
        ("ts_code", pa.string()),
        ("trade_time", pa.string()),
        ("freq", pa.string()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("vol", pa.int64()),
        ("amount", pa.float64()),
        ("extracted_at", pa.timestamp("us", tz="UTC")),
    ]
)


@dataclass
class SliceResult:
    """adapter 交给 pipeline 的切片终态、原始行与请求证据。"""

    status: Literal["ok", "empty", "pending", "failed"]
    rows: list[dict] = field(default_factory=list)
    requests: list[RequestRecord] = field(default_factory=list)
    retry_after: float | None = None
    error: str | None = None
    extracted_at: datetime | None = None


class TushareMinuteSource:
    """封装 ``stk_mins`` 参数、分页协议和切片级协议检查。"""

    def __init__(self, client: TushareClient) -> None:
        """复用注入的传输 client，使 universe 与分钟请求共享预算和会话。"""

        self.client = client

    def fetch_slice(self, secu_code: str, trade_date: date) -> SliceResult:
        """完整抓取一个证券的一天，返回 ok/empty/pending/failed 四态之一。"""

        day = trade_date.isoformat()
        start = f"{day} 09:30:00"
        end = f"{day} 15:00:00"
        rows: list[dict] = []
        requests: list[RequestRecord] = []
        seen: set[tuple[str, str]] = set()
        previous: set[tuple[str, str]] | None = None

        # adapter 独占 vendor 分页知识；pipeline 只看到一个切片结果。
        for page in range(MAX_PAGES):
            try:
                result = self.client.query(
                    "stk_mins",
                    ts_code=secu_code,
                    freq="1min",
                    start_date=start,
                    end_date=end,
                    fields=FIELDS,
                    limit=LIMIT,
                    offset=page * LIMIT,
                )
            except TushareError as exc:
                requests.extend(exc.requests)
                return SliceResult("failed", requests=requests, error=str(exc))
            requests.extend(result.requests)
            # 任一页 pending 都表示切片尚未完整，已取到的部分行不能落成 ok。
            if result.status == "pending":
                return SliceResult("pending", requests=requests, retry_after=result.retry_after)

            # 协议检查发生在合并之前，防止越界或缺字段的数据污染累计结果。
            page_rows = result.rows
            error = _validate_page(page_rows, secu_code, start, end)
            if error:
                return SliceResult("failed", requests=requests, error=error)
            page_keys = {(row["ts_code"], row["trade_time"]) for row in page_rows}
            if len(page_keys) != len(page_rows):
                return SliceResult("failed", requests=requests, error="response contains duplicate minute keys")
            if page_keys and previous == page_keys:
                return SliceResult("failed", requests=requests, error="pagination repeated the previous page")
            if page_keys & seen:
                return SliceResult("failed", requests=requests, error="pagination returned overlapping keys")
            if page_keys and not page_keys - seen:
                return SliceResult("failed", requests=requests, error="pagination cursor did not advance")

            rows.extend(page_rows)
            seen.update(page_keys)
            previous = page_keys
            # 只有有效短页（包括 0 行页）才能证明 offset 分页已经结束。
            if len(page_rows) < LIMIT:
                extracted_at = datetime.now(timezone.utc)
                return SliceResult("ok" if rows else "empty", rows, requests, extracted_at=extracted_at)

        return SliceResult("failed", requests=requests, error=f"pagination exceeded {MAX_PAGES} pages")


def _validate_page(rows: list[dict], secu_code: str, start: str, end: str) -> str | None:
    """校验一页记录属于请求切片；不判断价格和成交量是否合理。"""

    for row in rows:
        if not FIELD_NAMES.issubset(row):
            return "response is missing required minute fields"
        if row["ts_code"] != secu_code:
            return f"response code {row['ts_code']!r} is outside requested slice"
        trade_time = row["trade_time"]
        if not isinstance(trade_time, str):
            return "trade_time is not a string"
        try:
            datetime.strptime(trade_time, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return f"invalid trade_time {trade_time!r}"
        if not start <= trade_time <= end:
            return f"trade_time {trade_time!r} is outside requested slice"
    return None
