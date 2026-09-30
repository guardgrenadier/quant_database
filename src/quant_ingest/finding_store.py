"""将 dbt 产出的逐行检查结果汇总为长期质量发现。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .ledger import Ledger


def record_finding_rows(
    ledger: Ledger, run_id: str, dataset: str, day: date, path: Path, severity: str,
) -> int:
    """单次扫描结果文件，按检查和证券汇总数量、细节与最多三行样例。"""

    groups: dict[tuple[str, str | None], dict] = {}
    has_null_fields = "null_fields" in pq.read_schema(path).names
    field_expr = "null_fields" if has_null_fields else "cast(null as varchar)"
    with duckdb.connect() as conn:
        cursor = conn.execute(
            f"select check_name, ts_code, trade_time, detail, {field_expr} from read_parquet(?)", [str(path)]
        )
        while batch := cursor.fetchmany(10000):
            for check, code, stamp, detail, null_fields in batch:
                group = groups.setdefault((check, code), {
                    "count": 0, "times": set(), "issues": set(), "null_fields": set(), "samples": [],
                })
                group["count"] += 1
                if stamp:
                    group["times"].add(str(stamp).split(" ")[-1])
                if detail:
                    group["issues"].add(detail)
                fields = [name for name in (null_fields or "").split(",") if name]
                group["null_fields"].update(fields)
                if len(group["samples"]) < 3:
                    group["samples"].append({"trade_time": stamp, "detail": detail,
                                             "null_fields": fields})
    for (check, code), group in groups.items():
        ledger.record_finding(
            run_id, dataset, day, check, severity, group["count"], group["samples"],
            ts_code=code, details={"issues": sorted(group["issues"]),
                                   "bar_times": sorted(group["times"]),
                                   "null_fields": sorted(group["null_fields"])},
        )
    return sum(group["count"] for group in groups.values())
