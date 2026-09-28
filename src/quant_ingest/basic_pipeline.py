"""三个基础数据集共用的 raw、dbt 转换和发布编排。"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
import uuid
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .errors import SourceContractError
from .ledger import Ledger
from .raw import RawStore


ROOT = Path(__file__).resolve().parents[2]
SLICE_KEY = "day"


class BasicPipeline:
    """逐数据集处理一天，并在文件替换和发布账之间支持安全恢复。"""

    def __init__(
        self, source: Any, ledger: Ledger, raw: RawStore,
        warehouse: Path, dbt_project: Path = ROOT / "dbt_v2",
    ) -> None:
        """注入来源和存储路径，便于离线测试完整链路。"""

        self.source = source
        self.ledger = ledger
        self.raw = raw
        self.warehouse = warehouse
        self.dbt_project = dbt_project

    def canonical_path(self, dataset: str, day: date) -> Path:
        """返回 dataset × 日期的唯一正式 Parquet 路径。"""

        return self.warehouse / "canonical" / dataset / f"date={day:%Y%m%d}.parquet"

    def run(self, dataset: str, day: date) -> Path:
        """固定计划、抓取或恢复 raw、执行 dbt 后原子发布。"""

        run_id = uuid.uuid4().hex
        params = self.source.parameters(dataset, day)
        self.ledger.plan(dataset, day, params, {SLICE_KEY: params})
        self._log(run_id, dataset, day, "planned", parameters=params)
        target = self.canonical_path(dataset, day)
        publication = self.ledger.publication(dataset, day)
        if publication is not None:
            stage, saved_path, checksum = publication
            if saved_path != str(target):
                raise ValueError("publication path differs from canonical path")
            if target.is_file() and hashlib.sha256(target.read_bytes()).hexdigest() == checksum:
                if stage == "prepared":
                    self.ledger.finish_publish(dataset, day)
                self._log(run_id, dataset, day, "published_existing", path=str(target))
                return target
            if stage == "published":
                raise ValueError("published canonical file is missing or changed")

        # raw 已落盘、账本尚未记账时，从文件恢复；否则只抓未决切片。
        if SLICE_KEY in self.ledger.unresolved(dataset, day):
            raw_path = self.raw.path(dataset, day, SLICE_KEY)
            if raw_path.is_file():
                artifact = self.raw.inspect(dataset, day, SLICE_KEY)
                rows = self._raw_rows(dataset, day, raw_path)
                extracted_at = datetime.fromtimestamp(raw_path.stat().st_mtime, timezone.utc)
                self._log(run_id, dataset, day, "raw_recovered", sha256=artifact.sha256)
            else:
                try:
                    fetched = self.source.fetch(dataset, day)
                except Exception as exc:
                    if isinstance(exc, SourceContractError):
                        evidence = self._quarantine(run_id, dataset, day, exc.body)
                        self.ledger.record(dataset, day, SLICE_KEY, "quarantined")
                        self._log(run_id, dataset, day, "contract_violation", evidence=str(evidence), error=str(exc))
                    else:
                        self.ledger.record(dataset, day, SLICE_KEY, "failed")
                    self._log(run_id, dataset, day, "fetch_failed", error=str(exc))
                    raise
                rows = fetched.rows
                for index, page in enumerate(fetched.pages):
                    self._log(
                        run_id, dataset, day, "http_page", page=index,
                        http_status=200, request_id=page.request_id, row_count=len(page.rows),
                    )
                artifact = self.raw.write(dataset, day, SLICE_KEY, [page.body for page in fetched.pages])
                extracted_at = fetched.extracted_at
            self.ledger.record(
                dataset, day, SLICE_KEY, "ok" if rows else "empty",
                artifact=artifact, row_count=len(rows), extracted_at=extracted_at,
            )
            self._log(run_id, dataset, day, "raw_complete", row_count=len(rows), sha256=artifact.sha256)

        if not self.ledger.complete(dataset, day):
            raise ValueError("basic dataset is not complete")
        self.ledger.mark_fetched(dataset, day)
        status, raw_path, raw_sha, expected_rows, extracted_at = self.ledger.slice_details(dataset, day, SLICE_KEY)
        if status not in {"ok", "empty"} or raw_path is None or raw_sha is None or expected_rows is None or extracted_at is None:
            raise ValueError("completed slice is missing raw evidence")
        if hashlib.sha256(Path(raw_path).read_bytes()).hexdigest() != raw_sha:
            raise ValueError("raw file differs from slice ledger")
        rows = self._raw_rows(dataset, day, Path(raw_path))
        if len(rows) != expected_rows:
            raise ValueError("raw row count differs from slice ledger")

        work = self.warehouse / "candidates" / dataset / f"{day:%Y%m%d}" / run_id
        work.mkdir(parents=True, exist_ok=True)
        temp = work / "temp.parquet"
        candidate = work / "candidate.parquet"
        self._write_temp(dataset, rows, extracted_at, temp)
        self._dbt(dataset, day, temp, candidate, expected_rows)
        self._validate_candidate(dataset, day, candidate, expected_rows)
        self._log(run_id, dataset, day, "candidate_checked", row_count=expected_rows)

        # 先登记候选哈希，再替换文件；下一轮可依据实际文件补登记或重建。
        checksum = hashlib.sha256(candidate.read_bytes()).hexdigest()
        self.ledger.prepare_publish(dataset, day, target, checksum, expected_rows, self._version())
        target.parent.mkdir(parents=True, exist_ok=True)
        os.replace(candidate, target)
        self.ledger.finish_publish(dataset, day)
        self._log(run_id, dataset, day, "published", path=str(target), sha256=checksum)
        shutil.rmtree(work)
        return target

    def _log(self, run_id: str, dataset: str, day: date, event: str, **details: object) -> None:
        """把无密钥的过程事件追加到随 raw 一起清理的 JSONL。"""

        directory = self.raw.root / dataset / f"trade_date={day:%Y%m%d}" / "runs"
        directory.mkdir(parents=True, exist_ok=True)
        record = {"at": datetime.now(timezone.utc).isoformat(), "event": event, **details}
        with (directory / f"{run_id}.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def _quarantine(self, run_id: str, dataset: str, day: date, body: bytes) -> Path:
        """把违约响应原文留在 raw 清理范围之外，等待人工检查。"""

        directory = self.raw.root.parent / "quarantine" / dataset / f"trade_date={day:%Y%m%d}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{run_id}.response"
        with path.open("xb") as stream:
            stream.write(body)
        return path

    def is_open(self, day: date) -> bool:
        """仅依据已发布的同日 trade_cal 产物判定目标日是否开市。"""

        target = self.canonical_path("trade_cal", day)
        publication = self.ledger.publication("trade_cal", day)
        if publication is None or publication[0] != "published" or not target.is_file():
            raise ValueError("trade_cal is not published for target date")
        with duckdb.connect() as conn:
            row = conn.execute(
                "select is_open from read_parquet(?) where exchange = 'SSE' and cal_date = ?",
                [str(target), day],
            ).fetchone()
        if row is None:
            raise ValueError("trade_cal does not contain target date")
        return row[0] == 1

    def _raw_rows(self, dataset: str, day: date, path: Path) -> list[dict]:
        """从原文重建响应行，不依赖抓取进程中的内存结果。"""

        pages = json.loads(gzip.decompress(path.read_bytes()))
        if not isinstance(pages, list) or not pages:
            raise ValueError("raw page array is empty or invalid")
        # 供应商适配器负责重放和验证，pipeline 不解释具体 API 的信封与分页字段。
        return self.source.rows_from_raw(dataset, day, pages)

    def _write_temp(self, dataset: str, rows: list[dict], extracted_at: datetime, path: Path) -> None:
        """将源行无筛除地放入临时 Parquet，字段值保留为字符串供 dbt 转换。"""

        fields = self.source.columns(dataset)
        schema = pa.schema([(name, pa.string()) for name in fields] + [("extracted_at", pa.string())])
        values = [
            {**{name: None if row[name] is None else str(row[name]) for name in fields},
             "extracted_at": extracted_at.isoformat()}
            for row in rows
        ]
        pq.write_table(pa.Table.from_pylist(values, schema=schema), path, compression="zstd")

    def _dbt(self, dataset: str, day: date, temp: Path, candidate: Path, expected_rows: int) -> None:
        """调用独立 V2 dbt 项目完成列转换与阻断级契约检查。"""

        vars_json = json.dumps({
            "dataset": dataset, "temp_path": str(temp), "candidate_path": str(candidate),
            "as_of_date": day.isoformat(), "expected_rows": expected_rows,
        })
        with tempfile.TemporaryDirectory() as profile_dir:
            profile = Path(profile_dir) / "profiles.yml"
            profile.write_text((self.dbt_project / "profiles.yml.example").read_text())
            env = {**os.environ, "QUANT_V2_DB_PATH": str(self.ledger.database)}
            for command, selector in (("run", dataset), ("test", "basic_contract")):
                cmd = [
                    "dbt", command, "--select", selector, "--project-dir", str(self.dbt_project),
                    "--profiles-dir", profile_dir, "--vars", vars_json,
                ]
                result = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True)
                if result.returncode:
                    raise RuntimeError(f"dbt {command} failed:\n{(result.stdout + result.stderr)[-5000:]}")
                if command == "run" and expected_rows == 0:
                    # dbt-duckdb external 对零行模型可能生成空值哨兵，改写为类型明确的零行文件。
                    schema = pa.schema([
                        ("ts_code", pa.string()), ("trade_date", pa.date32()),
                        ("suspend_type", pa.string()), ("extracted_at", pa.timestamp("us", tz="UTC")),
                    ])
                    pq.write_table(pa.Table.from_pylist([], schema=schema), candidate, compression="zstd")

    def _validate_candidate(self, dataset: str, day: date, path: Path, expected_rows: int) -> None:
        """发布前复读候选文件，并检查行数与日期边界。"""

        if not path.is_file():
            raise ValueError("dbt did not produce a candidate")
        day_column = "trade_date" if dataset == "suspend_d" else "snapshot_date"
        with duckdb.connect() as conn:
            count, wrong_date = conn.execute(
                f"select count(*), count(*) filter (where {day_column} is null or {day_column} != ?) from read_parquet(?)",
                [day, str(path)],
            ).fetchone()
        if count != expected_rows or wrong_date:
            raise ValueError("candidate rows or dates differ from the fixed plan")

    def _version(self) -> str:
        """返回代码提交及脏工作树标记，不把未提交代码冒充干净版本。"""

        commit = subprocess.run(["git", "rev-parse", "--verify", "HEAD"], cwd=ROOT, text=True, capture_output=True)
        if commit.returncode:
            return "uncommitted"
        dirty = subprocess.run(["git", "status", "--porcelain"], cwd=ROOT, text=True, capture_output=True)
        return commit.stdout.strip() + ("-dirty" if dirty.stdout else "")
