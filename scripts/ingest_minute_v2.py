#!/usr/bin/env python3
"""运行 V2 分钟数据 L1 抓取。"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_source import RequestLimiter, TushareMinuteSource
from quant_ingest.raw import RawStore


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    """按交易日运行最多八线程的切片抓取，并输出完整性摘要。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument("--workers", type=int, default=8, help="1–8")
    parser.add_argument("--request-budget", type=int, help="本进程允许的 HTTP 尝试上限")
    args = parser.parse_args()
    warehouse = ROOT / "data" / "warehouse" / "v2"
    limiter = RequestLimiter(180, budget=args.request_budget)
    pipeline = MinutePipeline(
        TushareMinuteSource(limiter=limiter), Ledger(warehouse / "state.duckdb"),
        RawStore(ROOT / "data" / "raw" / "v2"), warehouse, workers=args.workers,
    )
    summary = pipeline.run(args.date)
    print(summary.to_json())
    return 0 if summary.complete else 2


if __name__ == "__main__":
    raise SystemExit(main())
