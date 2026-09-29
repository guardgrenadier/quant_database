#!/usr/bin/env python3
"""将已完成的 V2 分钟 raw 转换、检查并发布。"""

from __future__ import annotations

import argparse
from datetime import date

from quant_ingest import config
from quant_ingest.ledger import Ledger
from quant_ingest.minute_publish import MinutePublisher
from quant_ingest.raw import RawStore


def main() -> None:
    """按交易日发布；--rebuild 只从现存 raw 重建，不触发供应商请求。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument("--rebuild", action="store_true", help="从现存 raw 整日替换已发布文件")
    args = parser.parse_args()
    warehouse = config.WAREHOUSE_ROOT
    publisher = MinutePublisher(
        Ledger(config.LEDGER_DB), RawStore(config.RAW_ROOT), warehouse,
    )
    print(publisher.publish(args.date, rebuild=args.rebuild))


if __name__ == "__main__":
    main()
