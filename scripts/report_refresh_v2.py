#!/usr/bin/env python3
"""查看最近约两周 T+1 重抓的日期差异率。"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from quant_ingest.ledger import Ledger
from quant_ingest.refresh_report import summarize_refresh


ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    """输出可留存的 JSON 汇总，供观察期结束后决定常态刷新策略。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()
    if args.days <= 0:
        parser.error("--days must be positive")
    since = datetime.now(ZoneInfo("Asia/Shanghai")).date() - timedelta(days=args.days)
    ledger = Ledger(ROOT / "data" / "warehouse" / "v2" / "state.duckdb")
    print(json.dumps(summarize_refresh(ledger, since), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
