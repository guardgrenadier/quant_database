"""网盘逐日 Parquet 的一次性 temp 入场与发布。"""

from __future__ import annotations

import json
import hashlib
import uuid
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .ledger import Ledger
from .minute_candidate import check_minute_temp_candidate
from .minute_publish import MinutePublisher
from .minute_temp import MinuteTemp
from .raw import RawStore


SOURCE_COLUMNS = {"code", "trade_time", "open", "high", "low", "close", "vol", "amount", "date"}


@dataclass(frozen=True)
class HistoryInspection:
    """单日源文件行数、紧窗行数和被裁剪的窗外行数。"""

    source_rows: int
    selected_rows: int
    outside_rows: int


def history_path(root: Path, day: date) -> Path:
    """定位按年目录存放的网盘原始日文件。"""

    return root / f"year={day:%Y}" / f"{day:%Y%m%d}.parquet"


def prepare_history_temp(source: Path, day: date, work: Path) -> tuple[MinuteTemp, HistoryInspection]:
    """全文件校验日期与股数，裁紧窗并映射源列到共用 dbt 临时输入。"""

    if not source.is_file() or not SOURCE_COLUMNS.issubset(pq.ParquetFile(source).schema_arrow.names):
        raise ValueError("historical source file is missing required columns")
    work.mkdir(parents=True, exist_ok=True)
    temp = work / "temp.parquet"
    plan = work / "plan.parquet"
    start, end = f"{day} 09:30:00", f"{day} 15:00:00"
    with duckdb.connect() as conn:
        # DuckDB 的 CREATE VIEW 不接受占位参数；仅此处对本地路径做 SQL 字符串转义。
        escaped = str(source).replace("'", "''")
        conn.execute(f"create temp view source as select * from read_parquet('{escaped}')")
        total, wrong_day, invalid, fractional = conn.execute(
            """select count(*),
               count(*) filter (where date is null or date != ?
                 or trade_time is null or left(trade_time, 10) != ?),
               count(*) filter (where code is null or code = '' or
                 try_strptime(trade_time, '%Y-%m-%d %H:%M:%S') is null or
                 open is null or high is null or low is null or close is null or
                 vol is null or amount is null),
               count(*) filter (where vol != floor(vol)) from source""",
            [day.strftime("%Y%m%d"), day.isoformat()],
        ).fetchone()
        if wrong_day or invalid or fractional:
            raise ValueError(
                f"historical source violates date/required/whole-share contract: "
                f"{wrong_day}/{invalid}/{fractional}"
            )
        duplicates = conn.execute(
            """select count(*) from (select code, trade_time, count(*) n from source
               where trade_time between ? and ? group by 1,2 having n > 1)""", [start, end]
        ).fetchone()[0]
        if duplicates:
            raise ValueError(f"historical source has {duplicates} duplicate minute keys")
        selected = conn.execute(
            "select count(*) from source where trade_time between ? and ?", [start, end]
        ).fetchone()[0]
        if selected == 0:
            raise ValueError("historical file has no bars in requested window")
        # DuckDB 流式写 Parquet；不把百万行日文件载入 Python 对象列表。
        conn.execute(
            """copy (select cast(code as varchar) ts_code, cast(trade_time as varchar) trade_time,
               '1MIN' as freq, cast(open as varchar) as "open", cast(high as varchar) as high,
               cast(low as varchar) as low, cast(close as varchar) as "close",
               cast(cast(vol as bigint) as varchar) vol, cast(amount as varchar) amount,
               cast(null as varchar) extracted_at from source
               where trade_time between ? and ?) to ? (format parquet, compression zstd)""",
            [str(temp), start, end],
        )
        conn.execute(
            """copy (select distinct ts_code, cast(? as date) trade_date
               from read_parquet(?)) to ? (format parquet, compression zstd)""",
            [str(plan), day, str(temp)],
        )
    return MinuteTemp(temp, plan, total, selected), HistoryInspection(total, selected, total - selected)


def verify_trust_report(path: Path, source_root: Path, *, pilot_days: int | None = None) -> dict:
    """批量只接受匹配抽样；低信任报告仅允许一日试点。"""

    report = json.loads(path.read_text())
    if (Path(report.get("source_root", "")).resolve() != source_root.resolve()
            or not report.get("coverage_checked")):
        raise ValueError("historical trust report lacks matching source root or D6 coverage check")
    trust = report.get("trust_level")
    if trust == "sampled_match":
        if not report.get("api_overlap_checked"):
            raise ValueError("sampled_match report lacks completed API overlap check; regenerate it")
    elif trust == "low_or_pending":
        if pilot_days != 1:
            raise ValueError(
                "trust_level=low_or_pending blocks batch --apply; wait for pending API data, "
                "regenerate the trust report, or use --limit-days 1 for a manually reviewed pilot"
            )
    else:
        raise ValueError("historical trust report has unknown trust_level; regenerate it")
    universe = Path(report.get("coverage", {}).get("universe_path", ""))
    if (not universe.is_file() or hashlib.sha256(universe.read_bytes()).hexdigest()
            != report.get("coverage", {}).get("universe_sha256")):
        raise ValueError("historical trust report universe snapshot is missing or changed")
    return report


def import_history_day(
    ledger: Ledger, raw: RawStore, warehouse: Path, source_root: Path, day: date,
    *, universe_path: Path | None = None,
) -> Path:
    """一个交易日经同一 dbt 检查与发布门禁入库，来源在发布账记为 netdisk。"""

    source = history_path(source_root, day)
    run_id = uuid.uuid4().hex
    work = warehouse / "candidates" / "stk_mins" / f"{day:%Y%m%d}" / run_id
    temp, inspection = prepare_history_temp(source, day, work)
    candidate = check_minute_temp_candidate(ledger, raw, day, run_id, temp, historical=True)
    coverage: tuple[set[str], set[str]] | None = None
    if universe_path is not None and universe_path.is_file():
        with duckdb.connect() as conn:
            expected = {row[0] for row in conn.execute(
                """select ts_code from read_parquet(?) where list_date <= ?
                   and (delist_date is null or ? < delist_date) and list_status != 'P'""",
                [str(universe_path), day, day],
            ).fetchall()}
            actual = {row[0] for row in conn.execute(
                "select ts_code from read_parquet(?)", [str(temp.plan_path)]
            ).fetchall()}
        coverage = expected - actual, actual - expected

    result = MinutePublisher(ledger, raw, warehouse).publish_history_candidate(day, run_id, candidate)
    if inspection.outside_rows:
        ledger.record_finding(run_id, "stk_mins", day, "historical_window_trim", "info",
                              inspection.outside_rows, [{"source": str(source)}])
    if coverage is not None:
        missing, extra = coverage
        if missing or extra:
            ledger.record_finding(
                run_id, "stk_mins", day, "historical_universe_coverage", "warn",
                len(missing) + len(extra), [{"missing": len(missing), "extra": len(extra),
                                              "missing_examples": sorted(missing)[:5],
                                              "extra_examples": sorted(extra)[:5]}],
            )
    else:
        ledger.record_finding(run_id, "stk_mins", day, "historical_coverage_unavailable", "warn", 1,
                              [{"reason": "published stock_basic snapshot not supplied"}])
    return result
