"""历史来源入场前的 API 重叠抽样和 D6 覆盖对照。"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any

import duckdb

from .history_import import history_path
from .ledger import Ledger


def _latest_universe(ledger: Ledger) -> Path | None:
    """读取最新已发布、哈希正确的全状态证券快照。"""

    with duckdb.connect(str(ledger.database)) as conn:
        row = conn.execute(
            """select canonical_path, canonical_sha256 from publications
               where dataset = 'stock_basic' and stage = 'published'
               order by trade_date desc limit 1"""
        ).fetchone()
    if row is None:
        return None
    path = Path(row[0])
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != row[1]:
        return None
    return path


def build_trust_report(
    ledger: Ledger, source_root: Path, day: date, codes: list[str], source: Any,
) -> dict:
    """对样本代码逐行比较 API，并计算历史文件相对 D6 谓词的代码缺口。"""

    if not codes:
        raise ValueError("at least one overlap code is required")
    path = history_path(source_root, day)
    if not path.is_file():
        raise FileNotFoundError(path)
    start, end = f"{day} 09:30:00", f"{day} 15:00:00"
    with duckdb.connect() as conn:
        history_codes = {row[0] for row in conn.execute(
            """select distinct code from read_parquet(?)
               where trade_time between ? and ?""", [str(path), start, end]
        ).fetchall()}
    universe = _latest_universe(ledger)
    coverage: dict = {"checked": False}
    if universe is not None:
        with duckdb.connect() as conn:
            expected = {row[0] for row in conn.execute(
                """select ts_code from read_parquet(?) where list_date <= ?
                   and (delist_date is null or ? < delist_date) and list_status != 'P'""",
                [str(universe), day, day],
            ).fetchall()}
        coverage = {
            "checked": True, "universe_path": str(universe),
            "universe_sha256": hashlib.sha256(universe.read_bytes()).hexdigest(),
            "expected_codes": len(expected), "historical_codes": len(history_codes),
            "missing_count": len(expected - history_codes),
            "extra_count": len(history_codes - expected),
            "missing_examples": sorted(expected - history_codes)[:10],
            "extra_examples": sorted(history_codes - expected)[:10],
        }

    overlaps = []
    for code in codes:
        result = source.fetch_slice(code, day)
        if result.status == "pending":
            overlaps.append({"code": code, "status": "pending"})
            continue
        with duckdb.connect() as conn:
            source_rows = conn.execute(
                """select trade_time, open, high, low, close, vol, amount
                   from read_parquet(?) where code = ? and trade_time between ? and ?""",
                [str(path), code, start, end],
            ).fetchall()
        historical = {row[0]: row[1:] for row in source_rows}
        # D24 API raw 已扩为全天；历史重叠比较仍对齐网盘的 09:30–15:00 紧窗。
        current = {row["trade_time"]: row for row in result.rows
                   if start <= row["trade_time"] <= end}
        shared = historical.keys() & current.keys()
        mismatches = 0
        for stamp in shared:
            h = historical[stamp]
            a = current[stamp]
            prices = ("open", "high", "low", "close")
            if (any(abs(float(h[i]) - float(a[name])) > 0.0001 for i, name in enumerate(prices))
                    or abs(float(h[4]) - float(a["vol"])) > max(1, abs(float(h[4])) * 1e-6)
                    or abs(float(h[5]) - float(a["amount"])) > max(1, abs(float(h[5])) * 1e-6)):
                mismatches += 1
        overlaps.append({
            "code": code, "status": result.status,
            "historical_rows": len(historical), "api_rows": len(current),
            "missing_in_api": len(historical.keys() - current.keys()),
            "missing_in_history": len(current.keys() - historical.keys()),
            "value_mismatches": mismatches,
        })
    complete = all(
        item["status"] != "pending" and item["historical_rows"] > 0 and item["api_rows"] > 0
        for item in overlaps
    )
    matched = complete and all(
        item["missing_in_api"] == item["missing_in_history"] == item["value_mismatches"] == 0
        for item in overlaps
    )
    return {
        "source_root": str(source_root.resolve()), "sample_day": day.isoformat(),
        "sample_codes": codes, "api_overlap_checked": complete,
        "coverage_checked": coverage["checked"],
        "trust_level": "sampled_match" if matched else "low_or_pending",
        "comparison_tolerance": {"price": 0.0001, "vol": "max(1, value*1e-6)",
                                 "amount_yuan": "max(1, value*1e-6)"},
        "api_overlap": overlaps, "coverage": coverage,
    }
