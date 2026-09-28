"""由 DuckDB 持有的 ingestion 运行审计状态。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb


class AuditLog:
    """维护每个交易日每次处理尝试的最后成功阶段。"""

    def __init__(self, database: Path) -> None:
        """绑定 DuckDB，并幂等创建审计表。"""

        database.parent.mkdir(parents=True, exist_ok=True)
        self.database = database
        with duckdb.connect(str(database)) as conn:
            conn.execute(
                """
                create table if not exists ingestion_audit (
                    run_id varchar primary key,
                    trade_date date not null,
                    raw_path varchar not null,
                    stage varchar not null,
                    started_at timestamptz not null,
                    finished_at timestamptz,
                    raw_rows bigint,
                    canonical_rows bigint,
                    transform_version varchar not null,
                    error_message varchar
                )
                """
            )

    def start(self, run_id: str, day: date, raw_path: Path, transform_version: str) -> None:
        """登记一次新尝试，初始阶段固定为 ``started``。"""

        with duckdb.connect(str(self.database)) as conn:
            conn.execute(
                """insert into ingestion_audit
                   (run_id, trade_date, raw_path, stage, started_at, transform_version)
                   values (?, ?, ?, 'started', current_timestamp, ?)""",
                [run_id, day, str(raw_path), transform_version],
            )

    def advance(
        self,
        run_id: str,
        stage: str,
        *,
        raw_rows: int | None = None,
        canonical_rows: int | None = None,
    ) -> None:
        """推进最后成功阶段，并按需补充 raw/canonical 行数。"""

        # coalesce 保留之前已经登记的行数，阶段推进不主动清空证据。
        with duckdb.connect(str(self.database)) as conn:
            conn.execute(
                """update ingestion_audit set stage = ?, raw_rows = coalesce(?, raw_rows),
                   canonical_rows = coalesce(?, canonical_rows) where run_id = ?""",
                [stage, raw_rows, canonical_rows, run_id],
            )

    def finish(self, run_id: str, *, error: str | None = None) -> None:
        """结束一次尝试；失败信息不覆盖最后成功阶段。"""

        with duckdb.connect(str(self.database)) as conn:
            conn.execute(
                """update ingestion_audit set finished_at = current_timestamp, error_message = ?
                   where run_id = ?""",
                [error, run_id],
            )

    def is_published(self, day: date) -> bool:
        """判断某交易日是否至少存在一次 published/featured 记录。"""

        with duckdb.connect(str(self.database)) as conn:
            return bool(
                conn.execute(
                    """select count(*) > 0 from ingestion_audit
                       where trade_date = ? and stage in ('published', 'featured')""",
                    [day],
                ).fetchone()[0]
            )
