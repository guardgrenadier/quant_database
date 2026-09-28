"""直线式股票分钟 ingestion pipeline。"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import uuid
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any

import pyarrow as pa

from quant_db import config

from .audit import AuditLog
from .publish import canonical_path, normalize_empty_candidate, publish_candidate
from .raw_store import RawStore

LOG = logging.getLogger(__name__)


@dataclass
class RunSummary:
    """一次 pipeline 尝试返回给 CLI 的稳定摘要。"""

    run_id: str
    trade_date: str
    stage: str
    request_count: int
    slice_statuses: dict[str, int]
    raw_rows: int | None = None
    canonical_rows: int | None = None
    raw_path: str | None = None
    canonical_path: str | None = None
    error: str | None = None

    def to_json(self) -> str:
        """把摘要序列化为便于人工阅读和脚本解析的 JSON。"""

        return json.dumps(asdict(self), ensure_ascii=False, indent=2)


class MinutePipeline:
    """编排分钟请求计划、raw gate、dbt 检查、发布与审计。"""

    def __init__(self, source: Any, universe: Any, raw_schema: pa.Schema) -> None:
        """注入数据源、证券集合依赖和供应商 raw schema。"""

        self.source = source
        self.universe = universe
        self.raw_schema = raw_schema
        self.audit = AuditLog(config.DUCKDB_PATH)

    def run(self, day: date, *, codes: list[str] | None = None, resume: bool = False) -> RunSummary:
        """处理一个交易日；未完成切片留给后续 run 收敛，不在 run 内等待。"""

        run_id = uuid.uuid4().hex
        if resume:
            # resume 以旧 manifest 为唯一计划来源，防止证券范围在多次 run 之间漂移。
            store = RawStore.resume(config.RAW_STK_MINS_DIR, day, self.raw_schema)
            if codes is not None and list(codes) != store.codes:
                raise ValueError("--codes must exactly match the saved manifest plan when resuming")
            store.append_requests(self.universe.requests)
        else:
            # 新计划先固定交易日、L 股票快照和停牌交叉信息，再发送分钟请求。
            if not self.universe.is_trade_day(day):
                raise ValueError(f"{day} is not an open trading day")
            listed = self.universe.listed_stocks()
            listed_by_code = {row["ts_code"]: row for row in listed}
            plan_codes = codes or [row["ts_code"] for row in listed]
            missing = [code for code in plan_codes if code not in listed_by_code]
            if missing:
                raise ValueError(f"codes are not in stock_basic L snapshot: {missing}")
            snapshot = [listed_by_code[code] for code in plan_codes]
            suspensions = self.universe.suspensions(day)
            if codes is not None:
                wanted = set(plan_codes)
                suspensions = [row for row in suspensions if row.get("ts_code") in wanted]
            store = RawStore.create(
                config.RAW_STK_MINS_DIR,
                day,
                snapshot,
                suspensions,
                self.raw_schema,
                vendor="tushare",
            )
            store.append_requests(self.universe.requests)

        version = _transform_version()
        self.audit.start(run_id, day, store.directory, version)
        stage = "started"
        try:
            # 逐切片立即落状态；一个 pending/failed 不妨碍继续触发其他切片。
            for code in store.codes:
                if store.terminal(code):
                    continue
                result = self.source.fetch_slice(code, day)
                store.record(code, result)

            statuses = store.status_counts()
            requests = self.source.client.request_count
            # fetched gate 是硬边界：未收敛时不生成 candidate，更不能发布。
            if not store.complete:
                error = f"fetched gate blocked by slice statuses: {statuses}"
                self.audit.finish(run_id, error=error)
                return RunSummary(
                    run_id,
                    day.isoformat(),
                    stage,
                    requests,
                    statuses,
                    raw_path=str(store.directory),
                    error=error,
                )

            store.mark_fetched()
            stage = "fetched"
            raw_rows = store.raw_rows
            self.audit.advance(run_id, stage, raw_rows=raw_rows)
            if statuses.get("empty"):
                LOG.warning("%s has %d empty minute slices", day, statuses["empty"])

            candidate = config.CANDIDATE_STOCK_1MIN_DIR / run_id / f"{day:%Y%m%d}.parquet"
            candidate.parent.mkdir(parents=True, exist_ok=True)
            variables = {
                "minute_raw_glob": str(store.directory / "*.parquet"),
                "minute_candidate_path": str(candidate),
                "minute_trade_date": day.isoformat(),
                "minute_expected_rows": raw_rows,
                "minute_codes": store.codes,
            }
            # dbt 负责字段标准化和 B 类语义检查；失败时 raw 与 candidate 原地保留。
            _dbt("run", "--select", "stg_minute__tushare", variables=variables)
            _dbt("test", "--select", "tag:minute_ingestion", variables=variables)
            normalize_empty_candidate(candidate, raw_rows)

            # 只有 dbt 全绿后，才允许候选替换正式日文件。
            target, canonical_rows = publish_candidate(
                candidate, config.CANONICAL_STOCK_1MIN_DIR, day, raw_rows
            )
            stage = "published"
            self.audit.advance(run_id, stage, canonical_rows=canonical_rows)

            # view 在正式文件发布后重建，避免提前读到未检查的 candidate。
            view_vars = {
                "minute_canonical_glob": str(config.CANONICAL_STOCK_1MIN_DIR / "year=*" / "month=*" / "*.parquet")
            }
            _dbt("run", "--select", "minute_bars", variables=view_vars)
            _dbt("test", "--select", "tag:minute_view", variables=view_vars)
            self.audit.finish(run_id)
            # 清理同时要求 audit 已发布和 canonical 文件仍存在，失败批次不会被删。
            removed = RawStore.cleanup_expired(
                config.RAW_STK_MINS_DIR,
                lambda expired_day: self.audit.is_published(expired_day)
                and canonical_path(config.CANONICAL_STOCK_1MIN_DIR, expired_day).is_file(),
            )
            if removed:
                LOG.info("removed %d expired raw trading-day caches", len(removed))
            return RunSummary(
                run_id,
                day.isoformat(),
                stage,
                requests,
                statuses,
                raw_rows,
                canonical_rows,
                str(store.directory),
                str(target),
            )
        except Exception as exc:
            # 文件发布与审计不是同一事务；保留最后成功 stage 便于重跑核对恢复。
            self.audit.finish(run_id, error=str(exc))
            return RunSummary(
                run_id,
                day.isoformat(),
                stage,
                self.source.client.request_count,
                store.status_counts(),
                store.raw_rows if store.complete else None,
                raw_path=str(store.directory),
                error=str(exc),
            )


def _dbt(command: str, *args: str, variables: dict | None = None) -> None:
    """在项目固定目录执行 dbt，失败时保留末尾输出供 audit 追踪。"""

    executable = shutil.which("dbt") or "dbt"
    cmd = [
        executable,
        command,
        *args,
        "--project-dir",
        str(config.PROJECT_ROOT / "dbt"),
        "--profiles-dir",
        str(config.PROJECT_ROOT / "dbt"),
    ]
    if variables is not None:
        cmd.extend(["--vars", json.dumps(variables)])
    env = {**os.environ, "QUANT_DB_PATH": str(config.DUCKDB_PATH)}
    result = subprocess.run(cmd, cwd=config.PROJECT_ROOT, env=env, text=True, capture_output=True)
    if result.returncode:
        detail = (result.stdout + "\n" + result.stderr)[-6000:]
        raise RuntimeError(f"dbt {command} failed:\n{detail}")


def _transform_version() -> str:
    """返回可追溯代码版本；无 commit 或脏工作树绝不冒充干净提交。"""

    commit = subprocess.run(
        ["git", "rev-parse", "--verify", "HEAD"],
        cwd=config.PROJECT_ROOT,
        text=True,
        capture_output=True,
    )
    if commit.returncode:
        return "uncommitted"
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=config.PROJECT_ROOT,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    return f"{commit.stdout.strip()}{'-dirty' if dirty else ''}"
