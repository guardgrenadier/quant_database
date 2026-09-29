"""以当前已发布快照规划历史 gap 的有界交易日段。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from .ledger import Ledger
from .minute_plan import _published_file


def latest_snapshot_day(ledger: Ledger) -> date:
    """选出证券快照与交易日历都已发布的最新同日版本。"""

    with duckdb.connect(str(ledger.database)) as conn:
        row = conn.execute(
            """select b.trade_date from publications b join publications c
                 on b.trade_date = c.trade_date
               where b.dataset = 'stock_basic' and c.dataset = 'trade_cal'
                 and b.stage = 'published' and c.stage = 'published'
               order by b.trade_date desc limit 1"""
        ).fetchone()
    if row is None:
        raise ValueError("stock_basic and trade_cal have no shared published snapshot date")
    return row[0]


def open_gap_days(
    ledger: Ledger, warehouse: Path, snapshot_day: date, start: date, end: date,
) -> list[date]:
    """从最新日历选出 gap 内全部交易日，保留连续性信息。"""

    if start > end:
        raise ValueError("gap start is after end")
    if start.year != snapshot_day.year or end.year != snapshot_day.year:
        raise ValueError("published trade_cal snapshot covers only its target year")
    calendar, _ = _published_file(ledger, warehouse, "trade_cal", snapshot_day)
    _published_file(ledger, warehouse, "stock_basic", snapshot_day)
    with duckdb.connect() as conn:
        days = [row[0] for row in conn.execute(
            """select cal_date from read_parquet(?) where exchange = 'SSE'
               and is_open = 1 and cal_date between ? and ? order by cal_date""",
            [str(calendar), start, end],
        ).fetchall()]
    return days


def next_gap_segment(ledger: Ledger, days: list[date], max_days: int = 7) -> list[date]:
    """跳过已发布前缀，仅取下一段至多七个连续交易日。"""

    if not 1 <= max_days <= 7:
        raise ValueError("gap segment must contain 1–7 trading days")
    segment: list[date] = []
    for day in days:
        publication = ledger.publication("stk_mins", day)
        published = publication is not None and publication[0] == "published"
        if published and not segment:
            continue
        if published or len(segment) == max_days:
            break
        segment.append(day)
    return segment
