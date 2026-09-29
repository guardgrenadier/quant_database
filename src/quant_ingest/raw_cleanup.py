"""只清理已发布且抓取满 30 天的 V2 raw 与同目录过程日志。"""

from __future__ import annotations

import hashlib
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .ledger import Ledger
from .raw import RawStore


DATASETS = {"stock_basic", "trade_cal", "suspend_d", "stk_mins"}


def cleanup_expired_raw(
    ledger: Ledger, raw: RawStore, warehouse: Path,
    *, now: datetime | None = None, apply: bool = False,
) -> list[Path]:
    """返回可清理目录；apply 时删除它们并在长期账记录断源时间。"""

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(days=30)
    ready: list[Path] = []
    for dataset, day, published_path, published_sha in ledger.expired_raw_plans(cutoff):
        if dataset not in DATASETS:
            continue
        expected = warehouse / "canonical" / dataset / f"date={day:%Y%m%d}.parquet"
        if published_path != str(expected) or not expected.is_file():
            continue
        if hashlib.sha256(expected.read_bytes()).hexdigest() != published_sha:
            continue
        # 失败转换留下的候选目录豁免清理，供修复后从 raw 重建。
        candidate_day = warehouse / "candidates" / dataset / f"{day:%Y%m%d}"
        if candidate_day.is_dir() and any(candidate_day.iterdir()):
            continue
        directory = raw.root / dataset / f"trade_date={day:%Y%m%d}"
        ready.append(directory)
        if apply:
            if directory.exists():
                shutil.rmtree(directory)
            ledger.mark_raw_deleted(dataset, day)
    return ready
