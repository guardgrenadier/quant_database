"""正式入口：显式装配具体对象，保留既有命令及路径参数。"""

import argparse
import json
import os
from pathlib import Path

import duckdb
from dotenv import dotenv_values

from quant_data import db
from quant_data.core.models import RunOptions, control_data
from quant_data.core.runner import run
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.sources.tushare.client import TushareClient
from quant_data.sources.tushare.stock_basic import TushareStockBasicAdapter


def main():
    """加载私有凭据并运行已选择的 Dataset，成功返回 0，失败或部分成功返回 1。"""
    parser = argparse.ArgumentParser(description="Personal quantitative database ingestion")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("dataset", choices=["stock_basic"])
    ingest.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    ingest.add_argument("--landing-dir", type=Path, default=Path("data/landing"))
    ingest.add_argument("--keep-artifacts", action="store_true", help="保留完整阶段产物供 walkthrough")
    args = parser.parse_args()
    # 凭据优先使用项目 .env，再取环境变量；仅交给 Client，输出错误和结果时再次脱敏。
    token = (dotenv_values(".env").get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN", "")).strip()
    if not token:
        parser.error("Set TUSHARE_TOKEN in the project .env or environment")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    try:
        # 显式组合业务契约、供应商适配器和通信 Client，再用同一连接执行初始化与完整 Run。
        dataset = StockBasicDataset()
        adapter = TushareStockBasicAdapter(TushareClient(token))
        with duckdb.connect(str(args.db)) as con:
            db.initialize(con, dataset)
            result = run(con, dataset, adapter, {}, RunOptions(args.landing_dir, args.keep_artifacts))
    except (ValueError, OSError, duckdb.Error) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc).replace(token, "<redacted>")}, ensure_ascii=False))
        return 1
    print(json.dumps(control_data(result), ensure_ascii=False).replace(token, "<redacted>"))
    # CLI 结局：仅业务 SUCCESS 返回 0；PARTIAL 也返回非零，诊断状态在结果中独立表达。
    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
