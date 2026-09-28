"""按切片保存无损的 gzip JSON 原始响应。"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import re
import tempfile
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

    def write(self, dataset: str, day: date, slice_key: str, pages: list[bytes]) -> RawArtifact:
        """将各页响应原文字节放入 JSON 数组，完整写入后才公布文件。"""

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
            # 原子创建正式文件；目标已存在时失败，避免覆盖已完成的切片。
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
