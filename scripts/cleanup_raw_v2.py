#!/usr/bin/env python3
"""预览或执行 V2 raw 与过程日志的 30 天清理。"""

from __future__ import annotations

import argparse
from pathlib import Path

from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore
from quant_ingest.raw_cleanup import cleanup_expired_raw


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    """默认只列出候选目录；显式 --apply 才删除。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="删除符合全部护栏的 V2 raw 目录")
    args = parser.parse_args()
    warehouse = ROOT / "data" / "warehouse" / "v2"
    paths = cleanup_expired_raw(
        Ledger(warehouse / "state.duckdb"), RawStore(ROOT / "data" / "raw" / "v2"),
        warehouse, apply=args.apply,
    )
    for path in paths:
        print(path)
    print(f"{'removed' if args.apply else 'eligible'}: {len(paths)}")


if __name__ == "__main__":
    main()
