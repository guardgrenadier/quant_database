#!/usr/bin/env python3
"""查看最近约两周 T+1 重抓的日期差异率。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from quant_ingest import config
from quant_ingest.ledger import Ledger
from quant_ingest.refresh_report import summarize_refresh


def main() -> None:
    """输出可留存的 JSON 汇总，供观察期结束后决定常态刷新策略。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days must be positive")
    since = datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=args.days)
    ledger = Ledger(config.LEDGER_DB)
    print(json.dumps(summarize_refresh(ledger, since), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
