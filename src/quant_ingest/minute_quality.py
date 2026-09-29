"""运行 dbt 分钟语义告警，并将摘要写入长期质量账。"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from . import config
from .dbt_runner import run_dbt
from .ledger import Ledger
from .minute_candidate import MinuteCandidate
from .raw import RawStore
from .quarantine_findings import record_quarantine_self_healed


QUALITY_SCHEMA = pa.schema([
    ("check_name", pa.string()), ("ts_code", pa.string()),
    ("trade_time", pa.string()), ("detail", pa.string()),
])


def _suspend_path(ledger: Ledger, warehouse: Path, day: date) -> str:
    """只使用哈希与发布账一致的同日停牌文件，否则交由 dbt 告警。"""

    record = ledger.publication("suspend_d", day)
    expected = warehouse / "canonical" / "suspend_d" / f"date={day:%Y%m%d}.parquet"
    if record is None or record[0] != "published" or record[1] != str(expected):
        return ""
    if not expected.is_file() or hashlib.sha256(expected.read_bytes()).hexdigest() != record[2]:
        return ""
    return str(expected)


def record_minute_quality(
    ledger: Ledger, raw: RawStore, warehouse: Path, day: date,
    run_id: str, candidate: MinuteCandidate, *, project: Path = config.DBT_PROJECT,
) -> Path:
    """生成当前日告警行，让 dbt 发 warn，再由 publisher 持久化摘要。"""

    quality = candidate.path.parent / "quality.parquet"
    variables = {
        "dataset": "stk_mins", "temp_path": str(candidate.temp.data_path),
        "plan_path": str(candidate.temp.plan_path),
        "candidate_path": str(candidate.path), "quality_path": str(quality),
        "suspend_path": _suspend_path(ledger, warehouse, day),
        "as_of_date": day.isoformat(),
    }
    run_dbt("run", "minute_quality", variables, project=project, ledger_database=ledger.database,
            raw=raw, dataset="stk_mins", day=day, run_id=run_id)
    table = pq.read_table(quality)
    if table.num_rows == 1 and all(table.column(name).null_count == 1 for name in table.column_names):
        # dbt-duckdb external 的零行哨兵不是实际告警。
        pq.write_table(pa.Table.from_pylist([], schema=QUALITY_SCHEMA), quality, compression="zstd")
    run_dbt("test", "minute_quality_warning", variables, project=project, ledger_database=ledger.database,
            raw=raw, dataset="stk_mins", day=day, run_id=run_id)

    with duckdb.connect() as conn:
        groups = conn.execute(
            "select check_name, count(*) from read_parquet(?) group by check_name",
            [str(quality)],
        ).fetchall()
        for check_name, count in groups:
            rows = conn.execute(
                """select ts_code, trade_time, detail from read_parquet(?)
                   where check_name = ? order by ts_code, trade_time limit 3""",
                [str(quality), check_name],
            ).fetchall()
            samples = [dict(ts_code=code, trade_time=stamp, detail=detail) for code, stamp, detail in rows]
            ledger.record_finding(run_id, "stk_mins", day, check_name, "warn", count, samples)
    counts = ledger.status_counts("stk_mins", day)
    total = sum(counts.values())
    threshold = max(5, (total + 99) // 100)
    if counts.get("empty", 0) > threshold:
        ledger.record_finding(
            run_id, "stk_mins", day, "empty_slice_outlier", "warn", counts["empty"],
            [{"empty": counts["empty"], "planned": total, "threshold": threshold}],
        )
    record_quarantine_self_healed(ledger, raw, "stk_mins", day)
    return quality
