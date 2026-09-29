"""从分钟 raw 生成 dbt 候选文件，并执行阻断级转换检查。"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .dbt_runner import ROOT, run_dbt
from .ledger import Ledger
from .minute_temp import MinuteTemp, build_minute_temp
from .raw import RawStore


EMPTY_SCHEMA = pa.schema([
    ("ts_code", pa.string()), ("trade_time", pa.timestamp("us")),
    ("trade_date", pa.date32()), ("open", pa.float64()),
    ("high", pa.float64()), ("low", pa.float64()), ("close", pa.float64()),
    ("vol", pa.int64()), ("amount", pa.int64()),
    ("extracted_at", pa.timestamp("us", tz="UTC")),
])


@dataclass(frozen=True)
class MinuteCandidate:
    """通过 dbt 阻断检查的候选文件及其临时输入。"""

    path: Path
    temp: MinuteTemp


def build_minute_candidate(
    ledger: Ledger, raw: RawStore, warehouse: Path, day: date, run_id: str,
    *, project: Path = ROOT / "dbt_v2",
) -> MinuteCandidate:
    """只用已完成 raw 建 temp，经 dbt 转换与契约测试后返回候选。"""

    work = warehouse / "candidates" / "stk_mins" / f"{day:%Y%m%d}" / run_id
    temp = build_minute_temp(ledger, day, work)
    return check_minute_temp_candidate(ledger, raw, day, run_id, temp, project=project)


def check_minute_temp_candidate(
    ledger: Ledger, raw: RawStore, day: date, run_id: str, temp: MinuteTemp,
    *, historical: bool = False, project: Path = ROOT / "dbt_v2",
) -> MinuteCandidate:
    """让 API 与历史临时输入共用同一 dbt 转换和阻断级契约。"""

    work = temp.data_path.parent
    candidate = work / "candidate.parquet"
    variables = {
        "dataset": "stk_mins", "temp_path": str(temp.data_path),
        "plan_path": str(temp.plan_path), "candidate_path": str(candidate),
        "as_of_date": day.isoformat(), "expected_rows": temp.unique_rows,
        "historical": historical,
    }
    run_dbt("run", "stk_mins", variables, project=project, database=ledger.database,
            raw=raw, dataset="stk_mins", day=day, run_id=run_id)
    if temp.unique_rows == 0:
        # external 零行哨兵不属于真实数据，改写为类型明确的空候选。
        pq.write_table(pa.Table.from_pylist([], schema=EMPTY_SCHEMA), candidate, compression="zstd")
    run_dbt("test", "minute_contract", variables, project=project, database=ledger.database,
            raw=raw, dataset="stk_mins", day=day, run_id=run_id)
    return MinuteCandidate(candidate, temp)
