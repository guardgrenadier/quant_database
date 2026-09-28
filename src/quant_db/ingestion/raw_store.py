"""供应商中立、可按切片恢复的 raw artifact 存储。"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq


class RawStore:
    """维护一个交易日的切片 Parquet、manifest 和请求流水。"""

    def __init__(self, directory: Path, schema: pa.Schema, manifest: dict[str, Any]) -> None:
        """绑定日目录、显式 Arrow schema 和内存中的 manifest。"""

        self.directory = directory
        self.schema = schema
        self.manifest = manifest
        self.manifest_path = directory / "manifest.json"
        self.requests_path = directory / "requests.jsonl"

    @classmethod
    def create(
        cls,
        root: Path,
        day: date,
        universe: list[dict],
        suspensions: list[dict],
        schema: pa.Schema,
        vendor: str,
    ) -> "RawStore":
        """创建不可变请求计划和初始 planned 切片状态。"""

        directory = root / f"trade_date={day:%Y%m%d}"
        directory.mkdir(parents=True, exist_ok=True)
        manifest_path = directory / "manifest.json"
        if manifest_path.exists():
            raise FileExistsError(f"raw manifest already exists; use --resume: {manifest_path}")
        now = _now()
        codes = [str(row["ts_code"]) for row in universe]
        # universe 快照与逐代码计划一起保存，后续 resume 不重新解释当日范围。
        manifest = {
            "version": 1,
            "vendor": vendor,
            "trade_date": day.isoformat(),
            "created_at": now,
            "updated_at": now,
            "stage": "started",
            "universe": universe,
            "suspensions": suspensions,
            "plan": codes,
            "slices": {
                code: {
                    "status": "planned",
                    "row_count": None,
                    "fetched_at": None,
                    "retry_after": None,
                    "error": None,
                    "file": None,
                    "request_count": 0,
                    "request_ids": [],
                }
                for code in codes
            },
        }
        store = cls(directory, schema, manifest)
        store._save()
        return store

    @classmethod
    def resume(cls, root: Path, day: date, schema: pa.Schema) -> "RawStore":
        """读取既有 manifest，恢复同一交易日的抓取现场。"""

        directory = root / f"trade_date={day:%Y%m%d}"
        manifest_path = directory / "manifest.json"
        if not manifest_path.exists():
            raise FileNotFoundError(f"no manifest to resume: {manifest_path}")
        manifest = json.loads(manifest_path.read_text())
        if manifest.get("trade_date") != day.isoformat():
            raise ValueError("manifest trade date does not match its directory")
        return cls(directory, schema, manifest)

    @property
    def codes(self) -> list[str]:
        """按 manifest 固定顺序返回计划证券代码。"""

        return list(self.manifest["plan"])

    @property
    def complete(self) -> bool:
        """判断所有计划切片是否都处于 ok 或 empty。"""

        return all(item["status"] in {"ok", "empty"} for item in self.manifest["slices"].values())

    @property
    def raw_rows(self) -> int:
        """汇总所有确定性切片的 manifest 行数。"""

        return sum(int(item["row_count"] or 0) for item in self.manifest["slices"].values())

    def terminal(self, code: str) -> bool:
        """确认切片状态和 Parquet 实体都完整，满足 resume 跳过条件。"""

        entry = self.manifest["slices"][code]
        if entry["status"] not in {"ok", "empty"} or not entry.get("file"):
            return False
        path = self.directory / entry["file"]
        try:
            return path.is_file() and pq.ParquetFile(path).metadata.num_rows == entry["row_count"]
        except Exception:
            return False

    def append_requests(self, records: list, *, secu_code: str | None = None) -> None:
        """把实际 HTTP 尝试的脱敏摘要追加到 ``requests.jsonl``。"""

        if not records:
            return
        self.directory.mkdir(parents=True, exist_ok=True)
        with self.requests_path.open("a", encoding="utf-8") as stream:
            for record in records:
                value = asdict(record)
                if secu_code is not None:
                    value["secu_code"] = secu_code
                stream.write(json.dumps(value, ensure_ascii=False, default=str) + "\n")

    def record(self, code: str, result: Any) -> None:
        """持久化一次切片结果；成功状态必须同时写出显式 schema Parquet。"""

        # 请求证据先追加，即使随后 Parquet 写失败也能追溯供应商响应。
        self.append_requests(result.requests, secu_code=code)
        entry = self.manifest["slices"][code]
        entry.update(
            status=result.status,
            row_count=len(result.rows) if result.status in {"ok", "empty"} else None,
            fetched_at=(result.extracted_at or datetime.now(timezone.utc)).isoformat()
            if result.status in {"ok", "empty"}
            else None,
            last_attempt_at=_now(),
            retry_after=result.retry_after,
            error=result.error,
            request_count=len(result.requests),
            request_ids=[record.request_id for record in result.requests if record.request_id],
            file=None,
        )
        if result.status in {"ok", "empty"}:
            try:
                filename = f"{code.replace('.', '_')}.parquet"
                target = self.directory / filename
                temp = target.with_suffix(".parquet.tmp")
                extracted_at = result.extracted_at or datetime.now(timezone.utc)
                rows = [{**row, "extracted_at": extracted_at} for row in result.rows]
                table = pa.Table.from_pylist(rows, schema=self.schema)
                # 临时文件完整写完后再替换，避免 manifest 指向半写 Parquet。
                pq.write_table(table, temp, compression="zstd")
                os.replace(temp, target)
                entry["file"] = filename
            except Exception as exc:
                entry.update(status="failed", row_count=None, error=f"raw parquet write failed: {exc}")
        self._save()

    def mark_fetched(self) -> None:
        """在全部切片确定后，把日级 raw 状态推进为 fetched。"""

        if not self.complete:
            raise ValueError("cannot mark incomplete raw data fetched")
        self.manifest["stage"] = "fetched"
        self.manifest["raw_rows"] = self.raw_rows
        self.manifest["fetched_at"] = _now()
        self._save()

    def status_counts(self) -> dict[str, int]:
        """汇总 planned/ok/empty/pending/failed 切片数量。"""

        counts: dict[str, int] = {}
        for entry in self.manifest["slices"].values():
            counts[entry["status"]] = counts.get(entry["status"], 0) + 1
        return counts

    @staticmethod
    def cleanup_expired(root: Path, is_published: Any, *, now: datetime | None = None) -> list[Path]:
        """只删除抓取满 30 天且已确认发布的 fetched raw 缓存。"""

        now = now or datetime.now(timezone.utc)
        removed: list[Path] = []
        for path in root.glob("trade_date=*/manifest.json"):
            try:
                manifest = json.loads(path.read_text())
                fetched_at = datetime.fromisoformat(manifest["fetched_at"])
                day = date.fromisoformat(manifest["trade_date"])
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                continue
            if manifest.get("stage") != "fetched" or now - fetched_at < timedelta(days=30):
                continue
            # 是否已经发布由 pipeline 注入判断，RawStore 不依赖 canonical 布局或审计表。
            if is_published(day):
                shutil.rmtree(path.parent)
                removed.append(path.parent)
        return removed

    def _save(self) -> None:
        """通过同目录临时文件原子替换 manifest。"""

        self.manifest["updated_at"] = _now()
        temp = self.manifest_path.with_suffix(".json.tmp")
        temp.write_text(json.dumps(self.manifest, ensure_ascii=False, indent=2, default=str) + "\n")
        os.replace(temp, self.manifest_path)


def _now() -> str:
    """返回秒精度 UTC ISO 时间戳。"""

    return datetime.now(timezone.utc).isoformat(timespec="seconds")
