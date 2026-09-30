"""将 dbt 产出的逐行检查结果汇总为长期质量发现。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb

from .ledger import Ledger


def record_finding_rows(
    ledger: Ledger, run_id: str, dataset: str, day: date, path: Path, severity: str,
) -> int:
    """单次扫描结果文件，按检查和证券汇总数量、细节与最多三行样例。"""

    groups: dict[tuple[str, str | None], dict] = {}
    with duckdb.connect() as conn:
        cursor = conn.execute(
            "select check_name, ts_code, trade_time, detail from read_parquet(?)", [str(path)]
        )
        while batch := cursor.fetchmany(10000):
            for check, code, stamp, detail in batch:
                group = groups.setdefault((check, code), {
                    "count": 0, "times": set(), "issues": set(), "samples": [],
                })
                group["count"] += 1
                if stamp:
                    group["times"].add(str(stamp).split(" ")[-1])
                if detail:
                    group["issues"].add(detail)
                if len(group["samples"]) < 3:
                    group["samples"].append({"trade_time": stamp, "detail": detail})
    for (check, code), group in groups.items():
        ledger.record_finding(
            run_id, dataset, day, check, severity, group["count"], group["samples"],
            ts_code=code, details={"issues": sorted(group["issues"]),
                                   "bar_times": sorted(group["times"])},
        )
    return sum(group["count"] for group in groups.values())
