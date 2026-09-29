#!/usr/bin/env python3
"""运行 V2 基础数据集摄取。"""

from __future__ import annotations

import argparse
from datetime import date

from quant_ingest import config
from quant_ingest.basic_pipeline import BasicPipeline
from quant_ingest.basic_source import TushareBasicsSource
from quant_ingest.ledger import Ledger
from quant_ingest.raw import RawStore


def main() -> None:
    """解析日期与数据集，并按独立发布单位逐个运行。"""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, required=True, help="YYYY-MM-DD")
    parser.add_argument(
        "--dataset", choices=["stock_basic", "trade_cal", "suspend_d", "all"], default="all",
    )
    args = parser.parse_args()
    warehouse = config.WAREHOUSE_ROOT
    pipeline = BasicPipeline(
        TushareBasicsSource(), Ledger(config.LEDGER_DB),
        RawStore(config.RAW_ROOT), warehouse,
    )
    datasets = ["stock_basic", "trade_cal", "suspend_d"] if args.dataset == "all" else [args.dataset]
    for dataset in datasets:
        print(f"{dataset}: {pipeline.run(dataset, args.date)}")


if __name__ == "__main__":
    main()
