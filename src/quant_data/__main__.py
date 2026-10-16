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
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.client import TushareClient
from quant_data.sources.tushare.stock_basic import TushareStockBasicAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter


def main():
    """加载私有凭据并运行已选择的 Dataset，成功返回 0，失败或部分成功返回 1。"""
    parser = argparse.ArgumentParser(description="Personal quantitative database ingestion")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("dataset", choices=["stock_basic", "trade_calendar"])
    ingest.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    ingest.add_argument("--landing-dir", type=Path, default=Path("data/landing"))
    ingest.add_argument("--keep-artifacts", action="store_true", help="保留完整阶段产物供 walkthrough")
    ingest.add_argument("--start-date", help="trade_calendar 开始日期，YYYYMMDD")
    ingest.add_argument("--end-date", help="trade_calendar 结束日期，YYYYMMDD，包含当日")
    args = parser.parse_args()
    # CLI 边界：日历必须显式给出完整区间，股票快照不能接受子集日期。
    params = {}
    if args.dataset == "trade_calendar":
        # 业务检查：日历不猜测历史起点或最新日期，必须同时给出区间两端。
        if args.start_date is None or args.end_date is None:
            parser.error("trade_calendar requires --start-date and --end-date")
        # 业务传递：原样交给 Dataset 校验日期及闭区间顺序，不在入口补默认边界或过滤休市日。
        params = {"start_date": args.start_date, "end_date": args.end_date}
    elif args.start_date is not None or args.end_date is not None:
        # 业务检查：股票表只能整快照替换，禁止将带日期筛选的子集误作完整快照。
        parser.error("stock_basic does not accept date filters")
    # 凭据优先使用项目 .env，再取环境变量；仅交给 Client，输出错误和结果时再次脱敏。
    token = (dotenv_values(".env").get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN", "")).strip()
    if not token:
        parser.error("Set TUSHARE_TOKEN in the project .env or environment")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    try:
        # 显式组合业务契约、供应商适配器和通信 Client，再用同一连接执行初始化与完整 Run。
        if args.dataset == "stock_basic":
            dataset = StockBasicDataset()
            adapter = TushareStockBasicAdapter(TushareClient(token))
        else:
            # 业务装配：trade_calendar 固定采用 SSE 专属契约与适配器，不由用户参数改成其他市场。
            dataset = TradeCalendarDataset()
            adapter = TushareTradeCalendarAdapter(TushareClient(token))
        with duckdb.connect(str(args.db)) as con:
            db.initialize(con, dataset)
            result = run(con, dataset, adapter, params, RunOptions(args.landing_dir, args.keep_artifacts))
    except (ValueError, OSError, duckdb.Error) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc).replace(token, "<redacted>")}, ensure_ascii=False))
        return 1
    print(json.dumps(control_data(result), ensure_ascii=False).replace(token, "<redacted>"))
    # CLI 结局：仅业务 SUCCESS 返回 0；PARTIAL 也返回非零，诊断状态在结果中独立表达。
    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
