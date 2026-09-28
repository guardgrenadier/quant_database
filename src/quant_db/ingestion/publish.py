"""校验并原子替换单个交易日的 canonical 文件。"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

CANONICAL_SCHEMA = pa.schema(
    [
        ("secu_code", pa.string()),
        ("trade_time", pa.timestamp("us")),
        ("trade_date", pa.date32()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("volume", pa.int64()),
        ("amount", pa.int64()),
        ("extracted_at", pa.timestamp("us", tz="UTC")),
    ]
)


def canonical_path(root: Path, day: date) -> Path:
    """按年/月分区生成唯一的 canonical 日文件路径。"""

    return root / f"year={day:%Y}" / f"month={day:%Y%m}" / f"{day:%Y%m%d}.parquet"


def validate_candidate(path: Path, day: date, expected_rows: int) -> int:
    """确认候选可读、行数对账且所有记录属于目标交易日。"""

    if not path.is_file():
        raise ValueError(f"candidate file does not exist: {path}")
    with duckdb.connect() as conn:
        row_count, wrong_dates = conn.execute(
            "select count(*), count(*) filter (where trade_date != ?) from read_parquet(?)",
            [day, str(path)],
        ).fetchone()
    if row_count != expected_rows:
        raise ValueError(f"candidate rows {row_count} != manifest rows {expected_rows}")
    if wrong_dates:
        raise ValueError(f"candidate contains {wrong_dates} rows outside {day}")
    return row_count


def normalize_empty_candidate(path: Path, expected_rows: int) -> None:
    """移除 dbt-duckdb external materialization 为零行关系写入的 null 哨兵。"""

    if expected_rows == 0:
        pq.write_table(pa.Table.from_pylist([], schema=CANONICAL_SCHEMA), path, compression="zstd")


def publish_candidate(candidate: Path, root: Path, day: date, expected_rows: int) -> tuple[Path, int]:
    """先验证候选，再在同一文件系统内替换正式日文件并复读确认。"""

    rows = validate_candidate(candidate, day, expected_rows)
    target = canonical_path(root, day)
    target.parent.mkdir(parents=True, exist_ok=True)
    # candidate 与 canonical 都位于 warehouse，os.replace 提供同文件系统原子替换。
    os.replace(candidate, target)
    validate_candidate(target, day, expected_rows)
    return target, rows
