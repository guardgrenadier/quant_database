"""按切片保存无损的 gzip JSON 原始响应。"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import tempfile
from datetime import datetime, timezone
from dataclasses import dataclass
from datetime import date
from pathlib import Path


_NAME = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]*\Z")


@dataclass(frozen=True)
class RawArtifact:
    """已落盘 raw 文件的路径、校验值和响应页数。"""

    path: Path
    sha256: str
    page_count: int


class RawStore:
    """负责切片 raw 的路径、原子写入和坠机后核验。"""

    def __init__(self, root: Path) -> None:
        """绑定 raw 根目录。"""

        self.root = root

    def path(self, dataset: str, day: date, slice_key: str) -> Path:
        """生成切片路径，并拒绝可能逃出 raw 根目录的名称。"""

        if not all(_NAME.fullmatch(value) and ".." not in value for value in (dataset, slice_key)):
            raise ValueError("invalid dataset or slice key")
        return self.root / dataset / f"trade_date={day:%Y%m%d}" / f"{slice_key}.json.gz"

    def write(
        self, dataset: str, day: date, slice_key: str, pages: list[bytes],
        *, replace: bool = False,
    ) -> RawArtifact:
        """原子写入完整响应；仅已发布日刷新时允许整切片替换。"""

        if not pages:
            raise ValueError("a completed slice needs at least one response page")
        for page in pages:
            if not isinstance(page, bytes) or not isinstance(json.loads(page), dict):
                raise ValueError("each response page must be a JSON object in bytes")

        target = self.path(dataset, day, slice_key)
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temp_name = tempfile.mkstemp(prefix=".raw-", dir=target.parent)
        temp = Path(temp_name)
        try:
            # 同目录临时文件先写完并同步；JSON 数组仅添加页间分隔符，不改响应正文。
            with os.fdopen(fd, "wb") as stream:
                with gzip.GzipFile(fileobj=stream, mode="wb", mtime=0) as zipped:
                    zipped.write(b"[")
                    for index, page in enumerate(pages):
                        if index:
                            zipped.write(b",")
                        zipped.write(page)
                    zipped.write(b"]")
                stream.flush()
                os.fsync(stream.fileno())
            # 常规抓取只允许首次创建；刷新阶段由调用方显式启用原子替换。
            if replace:
                os.replace(temp, target)
            else:
                os.link(temp, target)
            return RawArtifact(target, hashlib.sha256(target.read_bytes()).hexdigest(), len(pages))
        finally:
            temp.unlink(missing_ok=True)

    def verify(self, artifact: RawArtifact) -> bool:
        """核对文件是否存在且字节内容与登记的 SHA-256 一致。"""

        return artifact.path.is_file() and hashlib.sha256(artifact.path.read_bytes()).hexdigest() == artifact.sha256

    def inspect(self, dataset: str, day: date, slice_key: str) -> RawArtifact:
        """读取已写成的文件，供文件落盘后、账本更新前的坠机恢复。"""

        path = self.path(dataset, day, slice_key)
        content = path.read_bytes()
        pages = json.loads(gzip.decompress(content))
        if not isinstance(pages, list) or not pages or not all(isinstance(page, dict) for page in pages):
            raise ValueError("raw artifact does not contain response pages")
        return RawArtifact(path, hashlib.sha256(content).hexdigest(), len(pages))

    def log(self, run_id: str, dataset: str, day: date, event: str, **details: object) -> None:
        """把无密钥过程事件追加到随 raw 清理的 run 级 JSONL。"""

        directory = self.root / dataset / f"trade_date={day:%Y%m%d}" / "runs"
        directory.mkdir(parents=True, exist_ok=True)
        record = {"at": datetime.now(timezone.utc).isoformat(), "event": event, **details}
        with (directory / f"{run_id}.jsonl").open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")

    def quarantine(self, run_id: str, dataset: str, day: date, body: bytes) -> Path:
        """将违约响应原文留在 raw 清理范围之外，等待人工检查。"""

        directory = self.root.parent / "quarantine" / dataset / f"trade_date={day:%Y%m%d}"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{run_id}.response"
        with path.open("xb") as stream:
            stream.write(body)
        return path
