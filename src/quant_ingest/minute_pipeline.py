"""分钟 L1 编排：固定切片、并发抓取、逐切片落账和跨 run 续抓。"""

from __future__ import annotations

import gzip
import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .errors import SourceContractError
from .ledger import Ledger
from .minute_plan import DATASET, plan_day
from .raw import RawStore


@dataclass(frozen=True)
class MinuteRunSummary:
    """一次 L1 运行的可打印结果，不宣称 canonical 已发布。"""

    trade_date: str
    planned: int
    statuses: dict[str, int]
    request_count: int
    complete: bool
    closed: bool = False

    def to_json(self) -> str:
        """序列化为 CLI 可读、脚本可解析的 JSON。"""

        return json.dumps(asdict(self), ensure_ascii=False, indent=2)


class MinutePipeline:
    """让来源适配器负责 API 细节，主线程串行更新 DuckDB。"""

    def __init__(
        self, source: Any, ledger: Ledger, raw: RawStore, warehouse: Path,
        *, workers: int = 8,
    ) -> None:
        """限定最多八个抓取线程，避免超过设计并发上限。"""

        if not 1 <= workers <= 8:
            raise ValueError("workers must be between 1 and 8")
        self.source = source
        self.ledger = ledger
        self.raw = raw
        self.warehouse = warehouse
        self.workers = workers

    def run(
        self, day: date, *, refresh: bool = False, snapshot_day: date | None = None,
    ) -> MinuteRunSummary:
        """处理未决切片；历史补抓可指定当前快照，刷新可跨 run 收敛。"""

        run_id = uuid.uuid4().hex
        request_start = self.source.request_count
        codes = plan_day(self.ledger, self.warehouse, day, self.source.parameters,
                         snapshot_day=snapshot_day)
        if not codes:
            if refresh:
                raise ValueError("closed day has no minute publication to refresh")
            return MinuteRunSummary(day.isoformat(), 0, {}, 0, complete=True, closed=True)
        status = self.ledger.start_refresh(DATASET, day) if refresh else self.ledger.plan_status(DATASET, day)
        if status in {"refresh_ready", "refreshed"} and refresh:
            return MinuteRunSummary(day.isoformat(), len(codes),
                                    self.ledger.status_counts(DATASET, day), 0, complete=True)
        if status in {"refreshing", "refresh_ready"} and not refresh:
            raise ValueError("minute refresh is in progress; use refresh mode")
        refreshing = status == "refreshing"
        saved = self.ledger.saved_plan(DATASET, day)
        assert saved is not None
        slice_parameters = saved[1]
        self.raw.log(run_id, DATASET, day, "planned", planned=len(codes))
        pending: list[str] = []
        for code in self.ledger.unresolved(DATASET, day):
            path = self.raw.path(DATASET, day, code)
            if path.is_file():
                if refreshing:
                    # 与旧账哈希一致的文件属于已发布版本，必须重新请求。
                    _, _, previous_sha, _, _ = self.ledger.slice_details(DATASET, day, code)
                    if previous_sha == hashlib.sha256(path.read_bytes()).hexdigest():
                        pending.append(code)
                        continue
                try:
                    artifact = self.raw.inspect(DATASET, day, code)
                    rows = self._raw_rows(code, day, path, slice_parameters[code])
                    extracted_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
                    self.ledger.record(
                        DATASET, day, code, "ok" if rows else "empty",
                        artifact=artifact, row_count=len(rows), extracted_at=extracted_at,
                    )
                    self.raw.log(run_id, DATASET, day, "raw_recovered", code=code, row_count=len(rows))
                except Exception as exc:
                    # 孤儿文件不可信时先移入隔离区，避免每轮都卡在同一坏文件上。
                    evidence = self.raw.quarantine(
                        f"{run_id}-{code.replace('.', '_')}", DATASET, day, path.read_bytes(),
                        slice_key=code, reason=str(exc),
                    )
                    path.unlink()
                    self.ledger.record(DATASET, day, code, "quarantined")
                    self.raw.log(run_id, DATASET, day, "raw_recovery_failed", code=code,
                                 evidence=str(evidence), error=str(exc))
            else:
                pending.append(code)

        # 每只新切片至少消耗一次 HTTP；预算小时保留其余 planned，避免批量记成失败。
        remaining = self.source.remaining_budget
        if remaining is not None:
            pending = pending[:remaining]
        # 网络请求并行；future 结果在主线程逐一写 raw 与 DuckDB，避免多写方争锁。
        with ThreadPoolExecutor(max_workers=self.workers) as pool:
            futures = {pool.submit(self.source.fetch_slice, code, day,
                                   parameters=slice_parameters[code]): code for code in pending}
            for future in as_completed(futures):
                code = futures[future]
                try:
                    result = future.result()
                    for attempt in result.requests:
                        self.raw.log(run_id, DATASET, day, "http_attempt", code=code, **attempt)
                    if result.status == "pending":
                        self.ledger.record(DATASET, day, code, "pending")
                        self.raw.log(run_id, DATASET, day, "pending", code=code,
                                     retry_after=result.retry_after)
                        continue
                    if result.extracted_at is None:
                        raise ValueError("completed slice lacks extracted_at")
                    artifact = self.raw.write(
                        DATASET, day, code, [page.body for page in result.pages], replace=refreshing
                    )
                    self.ledger.record(
                        DATASET, day, code, result.status, artifact=artifact,
                        row_count=len(result.rows), extracted_at=result.extracted_at,
                    )
                    self.raw.log(run_id, DATASET, day, "raw_complete", code=code,
                                 row_count=len(result.rows), sha256=artifact.sha256)
                except SourceContractError as exc:
                    for attempt in exc.requests:
                        self.raw.log(run_id, DATASET, day, "http_attempt", code=code, **attempt)
                    evidence = self.raw.quarantine(
                        f"{run_id}-{code.replace('.', '_')}", DATASET, day, exc.body,
                        slice_key=code, reason=str(exc),
                    )
                    self.ledger.record(DATASET, day, code, "quarantined")
                    self.raw.log(run_id, DATASET, day, "contract_violation", code=code,
                                 evidence=str(evidence), error=str(exc))
                except Exception as exc:
                    for attempt in getattr(exc, "requests", []):
                        self.raw.log(run_id, DATASET, day, "http_attempt", code=code, **attempt)
                    self.ledger.record(DATASET, day, code, "failed")
                    self.raw.log(run_id, DATASET, day, "fetch_failed", code=code, error=str(exc))

        complete = self.ledger.complete(DATASET, day)
        if complete:
            self.ledger.mark_fetched(DATASET, day)
        statuses = self.ledger.status_counts(DATASET, day)
        self.raw.log(run_id, DATASET, day, "finished", complete=complete, statuses=statuses)
        return MinuteRunSummary(
            day.isoformat(), len(codes), statuses,
            self.source.request_count - request_start, complete,
        )

    def _raw_rows(self, code: str, day: date, path: Path, parameters: dict) -> list[dict]:
        """让适配器重放原文并复核分页、窗口与重复主键。"""

        pages = json.loads(gzip.decompress(path.read_bytes()), parse_float=Decimal)
        return self.source.rows_from_raw(code, day, pages, parameters=parameters)
