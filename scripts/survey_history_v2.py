#!/usr/bin/env python3
"""生成网盘历史的 API 重叠抽样与 D6 覆盖信任报告。"""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from quant_ingest import config
from quant_ingest.history_trust import build_trust_report
from quant_ingest.ledger import Ledger
from quant_ingest.minute_source import RequestLimiter, TushareMinuteSource


def main() -> None:
    """对少量代码发送有界单日 API 请求，并保存可审阅的 JSON 报告。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", type=date.fromisoformat, default=date(2026, 4, 17))
    parser.add_argument("--codes", nargs="+", required=True)
    parser.add_argument("--request-budget", type=int, default=20)
    parser.add_argument("--output", type=Path, default=config.WAREHOUSE_ROOT / "history_trust.json")
    args = parser.parse_args()
    ledger = Ledger(config.LEDGER_DB)
    report = build_trust_report(
        ledger, config.HISTORY_ROOT, args.day, args.codes,
        TushareMinuteSource(limiter=RequestLimiter(180, budget=args.request_budget)),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(args.output)
    print(f"trust_level={report['trust_level']}, api_overlap_checked={report['api_overlap_checked']}, "
          f"coverage_checked={report['coverage_checked']}")


if __name__ == "__main__":
    main()
