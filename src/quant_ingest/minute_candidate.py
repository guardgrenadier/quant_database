"""从分钟 raw 生成 dbt 候选文件，并执行阻断级转换检查。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from . import config
from .dbt_runner import run_dbt
from .finding_store import record_finding_rows
from .ledger import Ledger
from .minute_source import FIELDS
from .minute_temp import MinuteTemp, build_minute_temp
from .raw import RawStore


EMPTY_SCHEMA = pa.schema([
    ("ts_code", pa.string()), ("trade_time", pa.timestamp("us")),
    ("trade_date", pa.date32()), ("open", pa.float64()),
    ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("vol", pa.int64()), ("amount", pa.int64()),
    ("extracted_at", pa.timestamp("us", tz="UTC")),
])
CONTRACT_SCHEMA = pa.schema([
    ("check_name", pa.string()), ("ts_code", pa.string()),
    ("trade_time", pa.string()), ("detail", pa.string()),
])


@dataclass(frozen=True)
class MinuteCandidate:
    """通过 dbt 阻断检查的候选文件及其临时输入。"""

    path: Path
    temp: MinuteTemp


def build_minute_candidate(
    ledger: Ledger, raw: RawStore, warehouse: Path, day: date, run_id: str,
    *, project: Path = config.DBT_PROJECT,
) -> MinuteCandidate:
    """只用已完成 raw 建 temp，经 dbt 转换与契约测试后返回候选。"""

    work = warehouse / "candidates" / "stk_mins" / f"{day:%Y%m%d}" / run_id
    try:
        temp = build_minute_temp(ledger, day, work)
    except Exception as exc:
        ledger.record_finding(run_id, "stk_mins", day, "raw_to_temp_error", "error", 1, [],
                              details={"error": str(exc)})
        raise
    return check_minute_temp_candidate(ledger, raw, day, run_id, temp, project=project)


def check_minute_temp_candidate(
    ledger: Ledger, raw: RawStore, day: date, run_id: str, temp: MinuteTemp,
    *, historical: bool = False, project: Path = config.DBT_PROJECT,
) -> MinuteCandidate:
    """让 API 与历史临时输入共用同一 dbt 转换和阻断级契约。"""

    work = temp.data_path.parent
    candidate = work / "candidate.parquet"
    variables = {
        "dataset": "stk_mins", "temp_path": str(temp.data_path),
        "plan_path": str(temp.plan_path), "candidate_path": str(candidate),
        "contract_path": str(work / "contract.parquet"),
        "as_of_date": day.isoformat(), "expected_rows": temp.unique_rows,
        "historical": historical,
    }
    try:
        columns = set(pq.read_schema(temp.data_path).names)
    except Exception as exc:
        ledger.record_finding(run_id, "stk_mins", day, "temp_unreadable", "error", 1, [],
                              details={"error": str(exc)})
        raise
    missing = set(FIELDS.split(",")) | {"extracted_at"}
    missing -= columns
    if missing:
        ledger.record_finding(run_id, "stk_mins", day, "required_columns_missing", "error", 1,
                              [], details={"columns": sorted(missing)})
        raise ValueError(f"minute temp is missing required columns: {sorted(missing)}")
    try:
        run_dbt("run", "stk_mins", variables, project=project, ledger_database=ledger.database,
                raw=raw, dataset="stk_mins", day=day, run_id=run_id)
    except Exception as exc:
        ledger.record_finding(run_id, "stk_mins", day, "dbt_conversion_error", "error", 1, [],
                              details={"error": str(exc)})
        raise
    if temp.unique_rows == 0:
        # external 零行哨兵不属于真实数据，改写为类型明确的空候选。
        pq.write_table(pa.Table.from_pylist([], schema=EMPTY_SCHEMA), candidate, compression="zstd")
    try:
        run_dbt("run", "minute_contract_findings", variables, project=project,
                ledger_database=ledger.database, raw=raw, dataset="stk_mins", day=day, run_id=run_id)
        contract = Path(variables["contract_path"])
        table = pq.read_table(contract)
        if table.num_rows == 1 and all(table.column(name).null_count == 1 for name in table.column_names):
            pq.write_table(pa.Table.from_pylist([], schema=CONTRACT_SCHEMA), contract, compression="zstd")
        errors = record_finding_rows(ledger, run_id, "stk_mins", day, contract, "error")
    except Exception as exc:
        ledger.record_finding(run_id, "stk_mins", day, "contract_report_error", "error", 1, [],
                              details={"error": str(exc)})
        raise
    try:
        run_dbt("test", "minute_contract", variables, project=project, ledger_database=ledger.database,
                raw=raw, dataset="stk_mins", day=day, run_id=run_id)
    except Exception as exc:
        if errors == 0:
            ledger.record_finding(run_id, "stk_mins", day, "contract_test_error", "error", 1, [],
                                  details={"error": str(exc)})
        raise
    if errors:
        raise RuntimeError(f"minute contract found {errors} error rows")
    return MinuteCandidate(candidate, temp)
