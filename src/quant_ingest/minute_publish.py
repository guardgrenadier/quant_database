"""分钟候选的跨边界对账、质量留痕和原子发布。"""

from __future__ import annotations

import hashlib
import os
import shutil
import uuid
from datetime import date
from pathlib import Path

import duckdb

from .dbt_runner import ROOT, run_dbt, transform_version
from .ledger import Ledger
from .minute_candidate import MinuteCandidate, build_minute_candidate
from .minute_quality import record_minute_quality
from .raw import RawStore


DATASET = "stk_mins"


class MinutePublisher:
    """只有日级 raw 完整且 dbt 检查通过时才替换正式文件。"""

    def __init__(
        self, ledger: Ledger, raw: RawStore, warehouse: Path,
        *, project: Path = ROOT / "dbt_v2",
    ) -> None:
        """绑定状态账、raw、同文件系统候选目录和独立 dbt 项目。"""

        self.ledger = ledger
        self.raw = raw
        self.warehouse = warehouse
        self.project = project

    def target(self, day: date) -> Path:
        """按 dataset × 交易日生成唯一 canonical 路径。"""

        return self.warehouse / "canonical" / DATASET / f"date={day:%Y%m%d}.parquet"

    def publish(self, day: date, *, rebuild: bool = False) -> Path:
        """从 raw 重建候选并发布；rebuild 允许替换已发布的日文件。"""

        run_id = uuid.uuid4().hex
        target = self.target(day)
        prior = self.ledger.publication(DATASET, day)
        if prior is not None:
            stage, path, checksum = prior
            if path != str(target):
                raise ValueError("publication path differs from canonical path")
            matches = target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == checksum
            if stage == "prepared" and matches:
                # 上次在文件替换后、账本登记前坠机，按正式文件实际哈希补登记。
                self.ledger.finish_publish(DATASET, day)
                self._ensure_view(day, run_id)
                return target
            if stage == "published":
                if not matches:
                    raise ValueError("published canonical file is missing or changed")
                if not rebuild:
                    self._ensure_view(day, run_id)
                    return target

        if not self.ledger.complete(DATASET, day):
            raise ValueError("minute raw plan is incomplete")
        candidate = build_minute_candidate(
            self.ledger, self.raw, self.warehouse, day, run_id, project=self.project
        )
        self._validate(day, candidate)
        record_minute_quality(
            self.ledger, self.raw, self.warehouse, day, run_id, candidate, project=self.project
        )
        if prior is not None and prior[0] == "published" and rebuild:
            self._record_diff(day, run_id, target, candidate.path)

        # prepared 先记候选哈希，再原子替换；下一轮用实际文件决定补登记或重建。
        checksum = hashlib.sha256(candidate.path.read_bytes()).hexdigest()
        self.ledger.prepare_publish(
            DATASET, day, target, checksum, candidate.temp.unique_rows,
            transform_version(), replace=rebuild,
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(candidate.path, target)
        self.ledger.finish_publish(DATASET, day)
        self.raw.log(run_id, DATASET, day, "published", path=str(target), sha256=checksum,
                     rows=candidate.temp.unique_rows)
        self._ensure_view(day, run_id)
        shutil.rmtree(candidate.path.parent)
        return target

    def _validate(self, day: date, candidate: MinuteCandidate) -> None:
        """复读待发布文件，核对路径日期、物理行数和目标日。"""

        path = candidate.path
        if not path.is_file() or path.name != "candidate.parquet":
            raise ValueError("minute candidate is missing or has the wrong path")
        with duckdb.connect() as conn:
            count, wrong_date = conn.execute(
                """select count(*), count(*) filter (where trade_date is null or trade_date != ?)
                   from read_parquet(?)""", [day, str(path)]
            ).fetchone()
        if count != candidate.temp.unique_rows or wrong_date:
            raise ValueError("minute candidate rows or date differ from raw plan")

    def _record_diff(self, day: date, run_id: str, old: Path, new: Path) -> None:
        """保留整日替换的行数、主键差集和值修订规模，不保存旧字节。"""

        with duckdb.connect() as conn:
            old_rows = conn.execute("select count(*) from read_parquet(?)", [str(old)]).fetchone()[0]
            new_rows = conn.execute("select count(*) from read_parquet(?)", [str(new)]).fetchone()[0]
            added = conn.execute(
                """select count(*) from read_parquet(?) n
                   anti join read_parquet(?) o using (ts_code, trade_time)""",
                [str(new), str(old)],
            ).fetchone()[0]
            removed = conn.execute(
                """select count(*) from read_parquet(?) o
                   anti join read_parquet(?) n using (ts_code, trade_time)""",
                [str(old), str(new)],
            ).fetchone()[0]
            changed = conn.execute(
                """select count(*) from read_parquet(?) n
                   join read_parquet(?) o using (ts_code, trade_time)
                   where n.open is distinct from o.open or n.high is distinct from o.high
                      or n.low is distinct from o.low or n.close is distinct from o.close
                      or n.vol is distinct from o.vol or n.amount is distinct from o.amount""",
                [str(new), str(old)],
            ).fetchone()[0]
            sample = conn.execute(
                """select n.ts_code, cast(n.trade_time as varchar) from read_parquet(?) n
                   anti join read_parquet(?) o using (ts_code, trade_time) limit 3""",
                [str(new), str(old)],
            ).fetchall()
            removed_sample = conn.execute(
                """select o.ts_code, cast(o.trade_time as varchar) from read_parquet(?) o
                   anti join read_parquet(?) n using (ts_code, trade_time) limit 3""",
                [str(old), str(new)],
            ).fetchall()
            changed_sample = conn.execute(
                """select n.ts_code, cast(n.trade_time as varchar) from read_parquet(?) n
                   join read_parquet(?) o using (ts_code, trade_time)
                   where n.open is distinct from o.open or n.high is distinct from o.high
                      or n.low is distinct from o.low or n.close is distinct from o.close
                      or n.vol is distinct from o.vol or n.amount is distinct from o.amount
                   limit 3""",
                [str(new), str(old)],
            ).fetchall()
        self.ledger.record_finding(
            run_id, DATASET, day, "revision_diff", "info", added + removed + changed,
            [{"old_rows": old_rows, "new_rows": new_rows, "added_keys": added,
              "removed_keys": removed, "changed_values": changed,
              "added_samples": sample, "removed_samples": removed_sample,
              "changed_samples": changed_sample}],
        )

    def _ensure_view(self, day: date, run_id: str) -> None:
        """正式文件发布后重建消费视图，并仅检查本次日期的视图契约。"""

        variables = {
            "minute_canonical_glob": str(self.warehouse / "canonical" / DATASET / "date=*.parquet"),
            "as_of_date": day.isoformat(),
        }
        run_dbt("run", "minute_bars", variables, project=self.project,
                database=self.ledger.database, raw=self.raw, dataset=DATASET, day=day, run_id=run_id)
        run_dbt("test", "minute_view_contract", variables, project=self.project,
                database=self.ledger.database, raw=self.raw, dataset=DATASET, day=day, run_id=run_id)
