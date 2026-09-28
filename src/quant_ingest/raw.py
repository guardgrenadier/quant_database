"""Write one lossless gzip JSON artifact per completed slice."""

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
    path: Path
    sha256: str
    page_count: int


class RawStore:
    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, dataset: str, day: date, slice_key: str) -> Path:
        if not all(_NAME.fullmatch(value) and ".." not in value for value in (dataset, slice_key)):
            raise ValueError("invalid dataset or slice key")
        return self.root / dataset / f"trade_date={day:%Y%m%d}" / f"{slice_key}.json.gz"

    def write(self, dataset: str, day: date, slice_key: str, pages: list[bytes]) -> RawArtifact:
        """Keep response bodies byte-for-byte inside a JSON array; publish only after a full write."""
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
            os.link(temp, target)  # atomic create; an existing successful slice is immutable
            return RawArtifact(target, hashlib.sha256(target.read_bytes()).hexdigest(), len(pages))
        finally:
            temp.unlink(missing_ok=True)

    def verify(self, artifact: RawArtifact) -> bool:
        return artifact.path.is_file() and hashlib.sha256(artifact.path.read_bytes()).hexdigest() == artifact.sha256

    def inspect(self, dataset: str, day: date, slice_key: str) -> RawArtifact:
        """Recover an artifact written just before a process crash or ledger update."""
        path = self.path(dataset, day, slice_key)
        content = path.read_bytes()
        pages = json.loads(gzip.decompress(content))
        if not isinstance(pages, list) or not pages or not all(isinstance(page, dict) for page in pages):
            raise ValueError("raw artifact does not contain response pages")
        return RawArtifact(path, hashlib.sha256(content).hexdigest(), len(pages))
