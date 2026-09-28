"""把已完成的分钟 raw 切片转换为运行期临时 Parquet。"""

from __future__ import annotations

import gzip
import hashlib
import json
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .ledger import Ledger
from .minute_source import FIELDS, row_signature


DATASET = "stk_mins"
NAMES = FIELDS.split(",")
TEMP_SCHEMA = pa.schema([(name, pa.string()) for name in NAMES] + [("extracted_at", pa.string())])
PLAN_SCHEMA = pa.schema([("ts_code", pa.string()), ("trade_date", pa.date32())])


@dataclass(frozen=True)
class MinuteTemp:
    """临时明细与请求集合路径，以及 raw/去重行数。"""

    data_path: Path
    plan_path: Path
    raw_rows: int
    unique_rows: int


def build_minute_temp(ledger: Ledger, day: date, work: Path) -> MinuteTemp:
    """校验 raw 哈希，按完全相同行去重，并写出 dbt 的两个输入。"""

    work.mkdir(parents=True, exist_ok=True)
    data_path = work / "temp.parquet"
    plan_path = work / "plan.parquet"
    slices = ledger.completed_slices(DATASET, day)
    if not slices:
        raise ValueError("minute plan has no slices")
    pq.write_table(pa.Table.from_pylist(
        [{"ts_code": code, "trade_date": day} for code, *_ in slices], schema=PLAN_SCHEMA
    ), plan_path, compression="zstd")

    raw_rows = 0
    unique_rows = 0
    # 逐切片写入 row group，避免把全市场分钟行同时留在 Python 内存。
    with pq.ParquetWriter(data_path, TEMP_SCHEMA, compression="zstd") as writer:
        for code, raw_path, sha256, expected_rows, extracted_at in slices:
            if raw_path is None or sha256 is None or expected_rows is None or extracted_at is None:
                raise ValueError(f"{code}: completed slice lacks raw evidence")
            content = Path(raw_path).read_bytes()
            if hashlib.sha256(content).hexdigest() != sha256:
                raise ValueError(f"{code}: raw checksum differs from ledger")
            pages = json.loads(gzip.decompress(content), parse_float=Decimal)
            if not isinstance(pages, list) or not pages:
                raise ValueError(f"{code}: raw page array is invalid")
            seen: dict[tuple[str, str], tuple[tuple, dict]] = {}
            count = 0
            for page in pages:
                data = page.get("data") if isinstance(page, dict) else None
                if not isinstance(data, dict) or data.get("fields") != NAMES or not isinstance(data.get("items"), list):
                    raise ValueError(f"{code}: raw response fields differ from plan")
                for values in data["items"]:
                    if not isinstance(values, list) or len(values) != len(NAMES):
                        raise ValueError(f"{code}: raw row does not match fields")
                    row = dict(zip(NAMES, values))
                    key = (row["ts_code"], row["trade_time"])
                    signature = row_signature(row)
                    if key in seen and signature != seen[key][0]:
                        raise ValueError(f"{code}: conflicting duplicate minute key")
                    seen[key] = (signature, row)
                    count += 1
            if count != expected_rows:
                raise ValueError(f"{code}: raw row count differs from ledger")
            raw_rows += count
            unique_rows += len(seen)
            records = [
                {**{name: None if value is None else str(value) for name, value in row.items()},
                 "extracted_at": extracted_at.isoformat()}
                for _, row in seen.values()
            ]
            if records:
                writer.write_table(pa.Table.from_pylist(records, schema=TEMP_SCHEMA))
    return MinuteTemp(data_path, plan_path, raw_rows, unique_rows)
