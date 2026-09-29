#!/usr/bin/env python3
"""T+1 观察期重抓已发布分钟日，并在收敛后整日替换。"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_publish import MinutePublisher
from quant_ingest.minute_source import RequestLimiter, TushareMinuteSource
from quant_ingest.raw import RawStore


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    """重访全部原计划切片；pending 时留待下轮，完成后才替换 canonical。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="需要 T+1 重访的交易日")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-budget", type=int)
    args = parser.parse_args()
    warehouse = ROOT / "data" / "warehouse" / "v2"
    ledger = Ledger(warehouse / "state.duckdb")
    if ledger.plan_status("stk_mins", args.date) == "refreshed":
        print(f"{args.date}: refresh already published")
        return 0
    raw = RawStore(ROOT / "data" / "raw" / "v2")
    source = TushareMinuteSource(limiter=RequestLimiter(180, budget=args.request_budget))
    summary = MinutePipeline(source, ledger, raw, warehouse, workers=args.workers).run(
        args.date, refresh=True
    )
    print(summary.to_json())
    if not summary.complete:
        return 2
    print(MinutePublisher(ledger, raw, warehouse).publish(args.date, rebuild=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
