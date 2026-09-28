"""持久化 V2 请求计划与切片状态；DuckDB 由单个写方串行更新。"""

from __future__ import annotations

import json
import hashlib
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Mapping

import duckdb

from .raw import RawArtifact


def _json(value: object) -> str:
    """稳定序列化计划参数，避免键顺序差异造成误判。"""

    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Ledger:
    """管理计划、切片、发布和质量发现的长期状态账。"""

    def __init__(self, database: Path) -> None:
        """绑定 DuckDB 文件，并幂等创建 V2 状态表。"""

        database.parent.mkdir(parents=True, exist_ok=True)
        self.database = database
        with duckdb.connect(str(database)) as conn:
            conn.execute("""
                create table if not exists plans (
                    dataset varchar not null, trade_date date not null,
                    parameters_json varchar not null, status varchar not null,
                    created_at timestamptz default current_timestamp,
                    primary key (dataset, trade_date)
                )
            """)
            conn.execute("""
                create table if not exists slices (
                    dataset varchar not null, trade_date date not null, slice_key varchar not null,
                    parameters_json varchar not null, status varchar not null,
                    raw_path varchar, raw_sha256 varchar, row_count bigint,
                    extracted_at timestamptz,
                    updated_at timestamptz default current_timestamp,
                    primary key (dataset, trade_date, slice_key)
                )
            """)
            conn.execute("""
                create table if not exists publications (
                    dataset varchar not null, trade_date date not null,
                    stage varchar not null, source varchar, canonical_path varchar,
                    canonical_sha256 varchar,
                    row_count bigint, transform_version varchar,
                    updated_at timestamptz default current_timestamp,
                    primary key (dataset, trade_date)
                )
            """)
            conn.execute("""
                create table if not exists quality_findings (
                    run_id varchar not null, dataset varchar not null, trade_date date not null,
                    check_name varchar not null, severity varchar not null,
                    finding_count bigint not null, samples_json varchar,
                    created_at timestamptz default current_timestamp,
                    primary key (run_id, check_name)
                )
            """)

    def plan(self, dataset: str, day: date, parameters: dict, slices: Mapping[str, dict]) -> None:
        """抓取前一次性固定计划；仅允许相同计划续跑。"""

        if not dataset or not slices:
            raise ValueError("dataset and slices are required")
        plan_json = _json(parameters)
        rows = [(dataset, day, key, _json(value), "planned") for key, value in slices.items()]
        if not all(key for key in slices):
            raise ValueError("slice keys must be nonempty")
        # 计划和全部 planned 切片同事务提交，避免坠机后只留下半份计划。
        with duckdb.connect(str(self.database)) as conn:
            conn.execute("begin transaction")
            try:
                prior = conn.execute(
                    "select parameters_json from plans where dataset = ? and trade_date = ?",
                    [dataset, day],
                ).fetchone()
                if prior is None:
                    conn.execute(
                        "insert into plans (dataset, trade_date, parameters_json, status) values (?, ?, ?, 'planned')",
                        [dataset, day, plan_json],
                    )
                    conn.executemany(
                        "insert into slices (dataset, trade_date, slice_key, parameters_json, status) values (?, ?, ?, ?, ?)",
                        rows,
                    )
                else:
                    # 续跑不得随着 universe 或请求参数变化而悄悄扩大、缩小切片集合。
                    saved = conn.execute(
                        "select slice_key, parameters_json from slices where dataset = ? and trade_date = ?",
                        [dataset, day],
                    ).fetchall()
                    if prior[0] != plan_json or {key: value for key, value in saved} != {
                        key: _json(value) for key, value in slices.items()
                    }:
                        raise ValueError("request plan changed; refusing to resume")
                conn.execute("commit")
            except Exception:
                conn.execute("rollback")
                raise

    def saved_plan(self, dataset: str, day: date) -> tuple[dict, dict[str, dict]] | None:
        """读取已固定的计划；续跑使用原切片集合，不重算当前 universe。"""

        with duckdb.connect(str(self.database)) as conn:
            plan = conn.execute(
                "select parameters_json from plans where dataset = ? and trade_date = ?", [dataset, day]
            ).fetchone()
            if plan is None:
                return None
            slices = conn.execute(
                """select slice_key, parameters_json from slices
                   where dataset = ? and trade_date = ? order by slice_key""", [dataset, day]
            ).fetchall()
            return json.loads(plan[0]), {key: json.loads(params) for key, params in slices}

    def record(
        self, dataset: str, day: date, slice_key: str, status: str,
        *, artifact: RawArtifact | None = None, row_count: int | None = None,
        extracted_at: datetime | None = None,
    ) -> None:
        """登记切片状态；完成态必须有可校验的 raw，且不得再次修改。"""

        if status not in {"ok", "empty", "pending", "failed", "quarantined"}:
            raise ValueError("invalid slice status")
        completed = status in {"ok", "empty"}
        if completed != (artifact is not None) or completed != (row_count is not None):
            raise ValueError("completed slices require raw artifact and row count")
        if row_count is not None and (row_count < 0 or (status == "empty" and row_count != 0) or (status == "ok" and row_count == 0)):
            raise ValueError("row count does not match slice status")
        if artifact is not None:
            # 先核对文件再更新账本，防止账本指向缺失或损坏的 raw。
            if not artifact.path.is_file():
                raise FileNotFoundError(artifact.path)
            if hashlib.sha256(artifact.path.read_bytes()).hexdigest() != artifact.sha256:
                raise ValueError("raw artifact checksum mismatch")
        with duckdb.connect(str(self.database)) as conn:
            prior = conn.execute(
                "select status from slices where dataset = ? and trade_date = ? and slice_key = ?",
                [dataset, day, slice_key],
            ).fetchone()
            if prior is None:
                raise KeyError((dataset, day, slice_key))
            if prior[0] in {"ok", "empty"}:
                raise ValueError("completed slice is immutable")
            conn.execute(
                """update slices set status = ?, raw_path = ?, raw_sha256 = ?, row_count = ?,
                   extracted_at = ?, updated_at = current_timestamp
                   where dataset = ? and trade_date = ? and slice_key = ?""",
                [status, str(artifact.path) if artifact else None, artifact.sha256 if artifact else None,
                 row_count, extracted_at or (datetime.now(timezone.utc) if completed else None),
                 dataset, day, slice_key],
            )

    def slice_details(self, dataset: str, day: date, slice_key: str) -> tuple[str, str | None, str | None, int | None, datetime | None]:
        """读取切片状态和 raw 证据，供转换与续跑使用。"""

        with duckdb.connect(str(self.database)) as conn:
            row = conn.execute(
                """select status, raw_path, raw_sha256, row_count, extracted_at from slices
                   where dataset = ? and trade_date = ? and slice_key = ?""",
                [dataset, day, slice_key],
            ).fetchone()
            if row is None:
                raise KeyError((dataset, day, slice_key))
            return row

    def unresolved(self, dataset: str, day: date) -> list[str]:
        """返回仍需处理的切片键，供后续 run 续抓。"""

        with duckdb.connect(str(self.database)) as conn:
            return [row[0] for row in conn.execute(
                """select slice_key from slices where dataset = ? and trade_date = ?
                   and status not in ('ok', 'empty') order by slice_key""", [dataset, day]
            ).fetchall()]

    def status_counts(self, dataset: str, day: date) -> dict[str, int]:
        """按状态汇总固定计划中的切片数，供运行摘要使用。"""

        with duckdb.connect(str(self.database)) as conn:
            rows = conn.execute(
                """select status, count(*) from slices where dataset = ? and trade_date = ?
                   group by status""", [dataset, day]
            ).fetchall()
            return {status: count for status, count in rows}

    def complete(self, dataset: str, day: date) -> bool:
        """仅当计划存在且所有切片为 ok/empty 时判定完整。"""

        with duckdb.connect(str(self.database)) as conn:
            row = conn.execute(
                """select exists (select 1 from plans where dataset = ? and trade_date = ?),
                   (select count(*) from slices where dataset = ? and trade_date = ?
                    and status not in ('ok', 'empty'))""",
                [dataset, day, dataset, day],
            ).fetchone()
            return bool(row[0] and row[1] == 0)

    def mark_fetched(self, dataset: str, day: date) -> None:
        """完整性门禁通过后，将计划推进到 fetched 阶段。"""

        if not self.complete(dataset, day):
            raise ValueError("cannot mark an incomplete plan fetched")
        with duckdb.connect(str(self.database)) as conn:
            conn.execute(
                "update plans set status = 'fetched' where dataset = ? and trade_date = ?",
                [dataset, day],
            )

    def publication(self, dataset: str, day: date) -> tuple[str, str, str] | None:
        """返回发布阶段、正式路径和文件校验值，供崩溃恢复核对。"""

        with duckdb.connect(str(self.database)) as conn:
            return conn.execute(
                """select stage, canonical_path, canonical_sha256 from publications
                   where dataset = ? and trade_date = ?""", [dataset, day]
            ).fetchone()

    def prepare_publish(
        self, dataset: str, day: date, target: Path, sha256: str, rows: int, version: str
    ) -> None:
        """替换正式文件前登记候选哈希，供替换后、登记前的坠机恢复。"""

        if not self.complete(dataset, day):
            raise ValueError("cannot publish an incomplete plan")
        with duckdb.connect(str(self.database)) as conn:
            prior = conn.execute(
                "select stage from publications where dataset = ? and trade_date = ?", [dataset, day]
            ).fetchone()
            if prior and prior[0] == "published":
                raise ValueError("published dataset cannot be replaced in this stage")
            if prior:
                conn.execute(
                    """update publications set canonical_path = ?, canonical_sha256 = ?, row_count = ?,
                       transform_version = ?, updated_at = current_timestamp
                       where dataset = ? and trade_date = ?""",
                    [str(target), sha256, rows, version, dataset, day],
                )
            else:
                conn.execute(
                    """insert into publications (dataset, trade_date, stage, source, canonical_path,
                       canonical_sha256, row_count, transform_version)
                       values (?, ?, 'prepared', 'tushare', ?, ?, ?, ?)""",
                    [dataset, day, str(target), sha256, rows, version],
                )

    def finish_publish(self, dataset: str, day: date) -> None:
        """以正式文件实际哈希为准，将 prepared 发布推进到 published。"""

        prior = self.publication(dataset, day)
        if prior is None or prior[0] != "prepared":
            raise ValueError("publication is not prepared")
        target = Path(prior[1])
        if not target.is_file() or hashlib.sha256(target.read_bytes()).hexdigest() != prior[2]:
            raise ValueError("canonical file does not match prepared publication")
        with duckdb.connect(str(self.database)) as conn:
            conn.execute(
                """update publications set stage = 'published', updated_at = current_timestamp
                   where dataset = ? and trade_date = ?""", [dataset, day]
            )
