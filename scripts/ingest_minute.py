#!/usr/bin/env python3
"""Composition root for the V1 stock-minute pipeline."""

from __future__ import annotations

import argparse
import logging
from datetime import datetime

from quant_db.ingestion.minute import MinutePipeline
from quant_db.sources.tushare.client import TushareClient
from quant_db.sources.tushare.minute import RAW_SCHEMA, TushareMinuteSource
from quant_db.sources.tushare.universe import TushareUniverse


def parse_day(value: str):
    for pattern in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(value, pattern).date()
        except ValueError:
            pass
    raise argparse.ArgumentTypeError("trade date must be YYYY-MM-DD or YYYYMMDD")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--trade-date", type=parse_day)
    parser.add_argument("--codes", nargs="+", help="explicit security subset, e.g. 000001.SZ 600000.SH")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--request-budget", type=int, help="hard cap on all HTTP attempts in this process")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = TushareClient(request_budget=args.request_budget)
    universe = TushareUniverse(client)
    day = args.trade_date or universe.latest_closed_trade_day()
    summary = MinutePipeline(TushareMinuteSource(client), universe, RAW_SCHEMA).run(
        day, codes=args.codes, resume=args.resume
    )
    print(summary.to_json())
    return 0 if summary.stage == "published" and summary.error is None else 2


if __name__ == "__main__":
    raise SystemExit(main())
