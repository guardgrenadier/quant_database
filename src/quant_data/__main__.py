"""正式入口：显式装配具体对象，保留既有命令及路径参数。"""

import argparse
import json
import os
from pathlib import Path

import duckdb
from dotenv import dotenv_values

from quant_data import db
from quant_data.core import audit, maintenance
from quant_data.core.models import RunOptions, control_data
from quant_data.core.runner import run
from quant_data.datasets.stock_basic import StockBasicDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.datasets.suspend_daily import SuspendDailyDataset
from quant_data.datasets.stock_st import StockStDataset
from quant_data.datasets.index_basic import IndexBasicDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.daily_basic import DailyBasicDataset
from quant_data.datasets.stk_limit import StkLimitDataset
from quant_data.datasets.index_daily import IndexDailyDataset
from quant_data.datasets.bse_mapping import BseMappingDataset
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.sources.tushare.client import TushareClient
from quant_data.sources.tushare.stock_basic import TushareStockBasicAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter
from quant_data.sources.tushare.suspend_daily import TushareSuspendDailyAdapter
from quant_data.sources.tushare.stock_st import TushareStockStAdapter
from quant_data.sources.tushare.index_basic import TushareIndexBasicAdapter
from quant_data.sources.tushare.stock_daily import TushareStockDailyAdapter
from quant_data.sources.tushare.daily_basic import TushareDailyBasicAdapter
from quant_data.sources.tushare.stk_limit import TushareStkLimitAdapter
from quant_data.sources.tushare.index_daily import TushareIndexDailyAdapter
from quant_data.sources.tushare.bse_mapping import TushareBseMappingAdapter
from quant_data.sources.galaxy.client import GalaxyClient
from quant_data.sources.galaxy.stock_1min import GalaxyStock1MinAdapter


def _maintain(args):
    """维护现有库，不加载供应商凭据、不创建业务表，也不自动迁移 Meta。"""
    try:
        # 维护必须指向已有文件，防止路径笔误生成一个空数据库。
        if not args.db.is_file():
            raise ValueError(f"Database does not exist: {args.db}")
        with duckdb.connect(str(args.db)) as con:
            if not db.check_meta(con):
                raise ValueError("Database has no Meta schema")
            con.execute("SET TimeZone = 'Asia/Shanghai'")
            # 独立进程取得写连接后才允许收尾；留存预览不隐式修改遗留 Run。
            if args.meta_command == "reconcile":
                result = {"reconciled_runs": audit.reconcile(con)}
            else:
                root = args.archive_dir or args.db.resolve().with_suffix(".audit")
                result = maintenance.retain(con, root, apply=args.apply)
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (ValueError, OSError, duckdb.Error) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, ensure_ascii=False))
        return 1


def main():
    """装配 ingestion 或独立 Meta 维护；失败返回非零，留存默认只预览。"""
    parser = argparse.ArgumentParser(description="Personal quantitative database ingestion")
    commands = parser.add_subparsers(dest="command", required=True)
    ingest = commands.add_parser("ingest")
    ingest.add_argument("dataset", choices=["stock_basic", "trade_calendar", "suspend_daily", "stock_st", "index_basic", "stock_daily", "index_daily", "bse_mapping", "stock_1min", "stk_limit", "daily_basic"])
    ingest.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    ingest.add_argument("--landing-dir", type=Path, default=Path("data/landing"))
    ingest.add_argument("--keep-artifacts", action="store_true", help="保留完整阶段产物供 walkthrough")
    ingest.add_argument("--start-date", help="日期型 Dataset 开始日期，YYYYMMDD")
    ingest.add_argument("--end-date", help="日期型 Dataset 结束日期，YYYYMMDD，包含当日")
    ingest.add_argument("--ts-code", action="append", help="仅 index_daily：选择已发布 universe 内代码，可重复")
    ingest.add_argument("--parallel", type=int, choices=(2, 4), help="仅 stock_1min：银河单会话请求并行度")
    meta = commands.add_parser("meta", help="独立审计收尾与留存维护")
    maintenance_commands = meta.add_subparsers(dest="meta_command", required=True)
    reconcile = maintenance_commands.add_parser("reconcile", help="收尾遗留 RUNNING，不重新获取或发布")
    reconcile.add_argument("--db", type=Path, required=True)
    retain = maintenance_commands.add_parser("retain", help="预览 60 天归档／365 天过期")
    retain.add_argument("--db", type=Path, required=True)
    retain.add_argument("--archive-dir", type=Path, help="默认使用数据库同目录的 <库名>.audit")
    retain.add_argument("--apply", action="store_true", help="实际执行归档、过期及删除重试")
    args = parser.parse_args()
    if args.command == "meta":
        return _maintain(args)
    # CLI 边界：按日期发布的数据必须显式给出完整区间，完整快照不能接受子集日期。
    params = {}
    if args.dataset in ("trade_calendar", "suspend_daily", "stock_st", "stock_daily", "index_daily", "stock_1min", "stk_limit", "daily_basic"):
        # 业务检查：不猜测任何日集合的目标起止日期，必须同时给出区间两端。
        if args.start_date is None or args.end_date is None:
            parser.error(f"{args.dataset} requires --start-date and --end-date")
        # 业务传递：原样交给 Dataset 校验日期及闭区间顺序，不在入口补默认边界或过滤休市日。
        params = {"start_date": args.start_date, "end_date": args.end_date}
    elif args.start_date is not None or args.end_date is not None:
        # 业务检查：快照只能整表替换，禁止将日期子集误作完整快照。
        parser.error(f"{args.dataset} does not accept date filters")
    # 业务边界：指数子集不能改变其他 Dataset 的完整日/快照政策；成员由 Dataset 同快照核对。
    if args.ts_code is not None:
        if args.dataset != "index_daily":
            parser.error("--ts-code is only supported for index_daily")
        params["ts_codes"] = args.ts_code
    if args.parallel is not None and args.dataset != "stock_1min":
        parser.error("--parallel is only supported for stock_1min")
    if args.dataset == "stock_1min":
        params["parallel"] = args.parallel or 2
    # 凭据优先使用项目 .env，再取环境变量；仅交给 Client，输出错误和结果时再次脱敏。
    values = dotenv_values(".env")
    token = (values.get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN", "")).strip()
    ad_username = (values.get("AD_USERNAME") or os.environ.get("AD_USERNAME", "")).strip()
    ad_password = (values.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD", "")).strip()
    if args.dataset == "stock_1min":
        if not ad_username or not ad_password:
            parser.error("Set AD_USERNAME and AD_PASSWORD in the project .env or environment")
        os.environ["AD_USERNAME"], os.environ["AD_PASSWORD"] = ad_username, ad_password
    elif not token:
        parser.error("Set TUSHARE_TOKEN in the project .env or environment")
    args.db.parent.mkdir(parents=True, exist_ok=True)
    try:
        # 显式组合业务契约、供应商适配器和通信 Client，再用同一连接执行初始化与完整 Run。
        if args.dataset == "stock_basic":
            dataset = StockBasicDataset()
            adapter = TushareStockBasicAdapter(TushareClient(token))
        elif args.dataset == "trade_calendar":
            # 业务装配：trade_calendar 固定采用 SSE 专属契约与适配器，不由用户参数改成其他市场。
            dataset = TradeCalendarDataset()
            adapter = TushareTradeCalendarAdapter(TushareClient(token))
        elif args.dataset == "suspend_daily":
            # 业务装配：停复牌自行声明 SSE 日历依赖，不自动触发日历获取或借用当前股票表。
            dataset = SuspendDailyDataset()
            adapter = TushareSuspendDailyAdapter(TushareClient(token))
        elif args.dataset == "stock_st":
            # 业务装配：ST 日名单依赖成功 SSE 日历，零行由自身业务验收阻断。
            dataset = StockStDataset()
            adapter = TushareStockStAdapter(TushareClient(token))
        elif args.dataset == "index_basic":
            # 业务装配：指数快照无日历依赖，固定白名单及申万过滤后整表替换。
            dataset = IndexBasicDataset()
            adapter = TushareIndexBasicAdapter(TushareClient(token))
        elif args.dataset == "stock_daily":
            dataset = StockDailyDataset()
            adapter = TushareStockDailyAdapter(TushareClient(token))
        elif args.dataset == "daily_basic":
            # 业务装配：默认十八字段按交易日完整替换，仅由本 Dataset 声明日历依赖。
            dataset = DailyBasicDataset()
            adapter = TushareDailyBasicAdapter(TushareClient(token))
        elif args.dataset == "stk_limit":
            # 业务装配：全市场价格按日替换，专属分页不改变现有共享接口。
            dataset = StkLimitDataset()
            adapter = TushareStkLimitAdapter(TushareClient(token))
        elif args.dataset == "bse_mapping":
            dataset = BseMappingDataset()
            adapter = TushareBseMappingAdapter(TushareClient(token))
        elif args.dataset == "stock_1min":
            dataset = Stock1MinDataset()
            adapter = GalaxyStock1MinAdapter(GalaxyClient(parallel=params["parallel"]))
        else:
            dataset = IndexDailyDataset()
            adapter = TushareIndexDailyAdapter(TushareClient(token))
        with duckdb.connect(str(args.db)) as con:
            db.initialize(con, dataset)
            # 只在独立 CLI 启动、登记新 Run 前收尾；Runner 的普通调用不扫描活动运行。
            reconciled = audit.reconcile(con)
            result = run(con, dataset, adapter, params, RunOptions(args.landing_dir, args.keep_artifacts))
    except (ValueError, OSError, duckdb.Error) as exc:
        message = str(exc)
        for secret in (token, ad_password):
            if secret:
                message = message.replace(secret, "<redacted>")
        print(json.dumps({"status": "FAILED", "error": message}, ensure_ascii=False))
        return 1
    output_data = control_data(result)
    if reconciled:
        output_data["reconciled_runs"] = reconciled
    output = json.dumps(output_data, ensure_ascii=False)
    for secret in (token, ad_password):
        if secret:
            output = output.replace(secret, "<redacted>")
    print(output)
    # CLI 结局：仅业务 SUCCESS 返回 0；PARTIAL 也返回非零，诊断状态在结果中独立表达。
    return 0 if result.status == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
