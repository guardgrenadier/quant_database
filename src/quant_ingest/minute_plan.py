"""从已发布的日历和全状态证券快照固定分钟切片计划。"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Callable

import duckdb

from .ledger import Ledger


DATASET = "stk_mins"


def _published_file(ledger: Ledger, warehouse: Path, dataset: str, day: date) -> tuple[Path, str]:
    """只接受已登记且文件哈希一致的同日 canonical 产物。"""

    expected = warehouse / "canonical" / dataset / f"date={day:%Y%m%d}.parquet"
    record = ledger.publication(dataset, day)
    if record is None or record[0] != "published" or record[1] != str(expected):
        raise ValueError(f"{dataset} is not published for {day}")
    if not expected.is_file() or hashlib.sha256(expected.read_bytes()).hexdigest() != record[2]:
        raise ValueError(f"{dataset} published file is missing or changed")
    return expected, record[2]


def plan_day(
    ledger: Ledger, warehouse: Path, day: date,
    slice_parameters: Callable[[str, date], dict],
) -> list[str]:
    """先查已固定计划；首次运行按 D6 谓词生成切片并落账。"""

    saved = ledger.saved_plan(DATASET, day)
    if saved is not None:
        return list(saved[1])

    calendar, calendar_sha = _published_file(ledger, warehouse, "trade_cal", day)
    with duckdb.connect() as conn:
        opening = conn.execute(
            "select is_open from read_parquet(?) where exchange = 'SSE' and cal_date = ?",
            [str(calendar), day],
        ).fetchone()
        if opening is None:
            raise ValueError("trade_cal does not contain target date")
        if opening[0] != 1:
            return []  # 闭市日不生成分钟请求计划。
    universe, universe_sha = _published_file(ledger, warehouse, "stock_basic", day)
    with duckdb.connect() as conn:
        codes = [row[0] for row in conn.execute(
            """select ts_code from read_parquet(?)
               where list_date <= ? and (delist_date is null or ? < delist_date)
                 and list_status != 'P' order by ts_code""",
            [str(universe), day, day],
        ).fetchall()]
    if not codes:
        raise ValueError("open day has no eligible securities")
    slices = {code: slice_parameters(code, day) for code in codes}
    # 将快照哈希一起固定，便于解释当日 S 的来源；后续轮次不再读取变化的快照。
    ledger.plan(DATASET, day, {"calendar_sha256": calendar_sha, "universe_sha256": universe_sha}, slices)
    return codes
