#!/usr/bin/env python3
"""按日导入网盘历史；默认预览，执行前需要信任报告。"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from quant_ingest import config
from quant_ingest.history_import import import_history_day, verify_trust_report
from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore


def main() -> None:
    """只处理指定区间内现存日文件，已发布日期默认跳过。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--limit-days", type=int, help="先限制本次尝试的日文件数")
    parser.add_argument("--trust-report", type=Path)
    parser.add_argument("--apply", action="store_true", help="真正写入 canonical 与状态账")
    args = parser.parse_args()
    if args.start > args.end or (args.limit_days is not None and args.limit_days <= 0):
        parser.error("invalid date range or --limit-days")
    source_root = config.HISTORY_ROOT
    files = sorted(source_root.glob("year=*/*.parquet"))
    days = [date.fromisoformat(path.stem) for path in files
            if args.start <= date.fromisoformat(path.stem) <= args.end]
    if args.limit_days is not None:
        days = days[:args.limit_days]
    warehouse = config.WAREHOUSE_ROOT
    ledger = Ledger(config.LEDGER_DB)
    report = None
    if args.apply:
        if args.trust_report is None:
            parser.error("--apply requires --trust-report")
        report = verify_trust_report(args.trust_report, source_root, pilot_days=args.limit_days)
        if report["trust_level"] == "low_or_pending":
            print("low-trust single-day pilot: manually inspect this publication before another import")
    universe = Path(report["coverage"]["universe_path"]) if report else None
    raw = RawStore(config.RAW_ROOT)
    for day in days:
        publication = ledger.publication("stk_mins", day)
        if publication is not None and publication[0] == "published":
            print(f"{day}: already published")
            continue
        if not args.apply:
            print(f"{day}: would import {source_root / f'year={day:%Y}' / f'{day:%Y%m%d}.parquet'}")
            continue
        target = import_history_day(ledger, raw, warehouse, source_root, day, universe_path=universe)
        print(f"{day}: published {target}")


if __name__ == "__main__":
    main()
