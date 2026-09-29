#!/usr/bin/env python3
"""T+1 观察期重抓已发布分钟日，并在收敛后整日替换。"""

from __future__ import annotations

import argparse
from datetime import date

from quant_ingest import config
from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_publish import MinutePublisher
from quant_ingest.minute_source import RequestLimiter, TushareMinuteSource
from quant_ingest.raw import RawStore


def main() -> int:
    """重访全部原计划切片；pending 时留待下轮，完成后才替换 canonical。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="需要 T+1 重访的交易日")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-budget", type=int)
    args = parser.parse_args()
    warehouse = config.WAREHOUSE_ROOT
    ledger = Ledger(config.LEDGER_DB)
    if ledger.plan_status("stk_mins", args.date) == "refreshed":
        print(f"{args.date}: refresh already published")
        return 0
    raw = RawStore(config.RAW_ROOT)
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
