#!/usr/bin/env python3
"""以最多七个连续交易日为一轮规划和补抓历史 gap。"""

from __future__ import annotations

import argparse
from datetime import date

from quant_ingest import config
from quant_ingest.gap_backfill import latest_snapshot_day, next_gap_segment, open_gap_days
from quant_ingest.ledger import Ledger
from quant_ingest.minute_pipeline import MinutePipeline
from quant_ingest.minute_publish import MinutePublisher
from quant_ingest.minute_source import RequestLimiter, TushareMinuteSource
from quant_ingest.raw import RawStore


def main() -> int:
    """默认只预览；显式执行时逐日走现成 L1 与 L2 发布链路。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--snapshot-date", type=date.fromisoformat)
    parser.add_argument("--max-days", type=int, default=7, help="每轮 1–7 个连续交易日")
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--request-budget", type=int, default=6000)
    parser.add_argument("--probe-code", help="先对 start 日期的一只股票小范围复测，不写入数据")
    parser.add_argument("--apply", action="store_true", help="发送真实分钟 API 请求")
    args = parser.parse_args()
    if not 1 <= args.max_days <= 7:
        parser.error("--max-days must be between 1 and 7")
    if args.probe_code:
        probe = TushareMinuteSource(limiter=RequestLimiter(180, budget=min(args.request_budget, 25)))
        result = probe.fetch_slice(args.probe_code, args.start)
        print(f"probe {args.probe_code} {args.start}: status={result.status}, rows={len(result.rows)}")
        return 0 if result.status in {"ok", "empty"} else 2
    warehouse = config.WAREHOUSE_ROOT
    ledger = Ledger(config.LEDGER_DB)
    snapshot_day = args.snapshot_date or latest_snapshot_day(ledger)
    days = next_gap_segment(
        ledger, open_gap_days(ledger, warehouse, snapshot_day, args.start, args.end), args.max_days
    )
    if not args.apply:
        for day in days:
            print(f"{day}: would fetch using snapshot {snapshot_day}")
        print(f"planned trading days: {len(days)}")
        return 0
    raw = RawStore(config.RAW_ROOT)
    source = TushareMinuteSource(limiter=RequestLimiter(180, budget=args.request_budget))
    pipeline = MinutePipeline(source, ledger, raw, warehouse, workers=args.workers)
    publisher = MinutePublisher(ledger, raw, warehouse)
    for day in days:
        summary = pipeline.run(day, snapshot_day=snapshot_day)
        print(summary.to_json())
        if not summary.complete:
            return 2
        print(publisher.publish(day))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
