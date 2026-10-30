"""探查脚本（可丢弃实验代码）：银河 AmazingData 历史 3 秒快照（query_snapshot）能力验证。

探查目标（服务于"用快照把开盘集合竞价量价从 09:30 首根分钟 bar 中分离"的决策）：
1. 历史快照是否可查（按 begin_date/end_date + begin_time/end_time，时间格式 HHMMSSmmm 九位整数）；
2. 竞价时段 09:15-09:25 是否有快照、时间戳间隔是否约 3 秒、trading_phase_code 如何标记；
3. 快照累计量额（total_volume_trade/total_value_trade，转换后列为 volume/amount）的量纲与缩放；
4. 与同日 1 分钟 K 线（银河口径把集合竞价并入 09:30 首根）交叉验证：
   - 09:25 前后快照累计量是否等于集合竞价成交量；
   - 09:30:00 快照累计量 - 竞价末快照累计量 是否等于"09:30 bar 成交量 - 竞价量"对应的盘中第一分钟量。

只读取与本地落盘到 data/probes/，不写入 warehouse / Canonical / Coverage，不修改任何 ingestion 架构。
登录与预热流程复用 scripts/download_galaxy_min1.py 中已验证的直连流程。
"""

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)

# 默认六只代表证券：沪主板/科创、深主板/创业板，均已在分钟线探查证据中确认可取数。
DEFAULT_CODES = (
    "000001.SZ", "002168.SZ", "300308.SZ",
    "600519.SH", "600365.SH", "688981.SH",
)
# 时间参数为 HHMMSSmmm 九位整数（query_snapshot 反编译确认：默认 90000000 / 170000000）。
DEFAULT_BEGIN_TIME = 90000000    # 09:00:00.000，覆盖集合竞价全程
DEFAULT_END_TIME = 150100000     # 15:01:00.000，覆盖收盘竞价后的最后快照


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def credentials():
    """凭据只从本地 .env / 环境变量读取，绝不写盘或打印。"""
    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or os.environ.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or os.environ.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or os.environ.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required in .env")
    return username, password, host, port


def analyze_code(df, code):
    """单证券快照帧的结构化摘要：时间分布、间隔、竞价窗口、累计量额锚点。"""
    import pandas as pd

    summary = {"code": code, "rows": int(len(df))}
    if df.empty:
        return summary
    df = df.sort_values("trade_time").reset_index(drop=True)
    summary["columns"] = [str(c) for c in df.columns]
    summary["first_time"] = str(df["trade_time"].iloc[0])
    summary["last_time"] = str(df["trade_time"].iloc[-1])
    # 相邻快照间隔分布：验证是否约 3 秒，以及竞价时段的推送节奏。
    deltas = df["trade_time"].diff().dropna().dt.total_seconds()
    if not deltas.empty:
        summary["interval_seconds"] = {
            "min": float(deltas.min()),
            "median": float(deltas.median()),
            "max": float(deltas.max()),
        }
        summary["interval_value_counts_head"] = {
            str(k): int(v) for k, v in deltas.value_counts().head(8).items()
        }
    if "trading_phase_code" in df.columns:
        summary["trading_phase_code_counts"] = {
            str(k): int(v) for k, v in df["trading_phase_code"].astype(str).value_counts().items()
        }
    day = df["trade_time"].dt.normalize().iloc[0]
    auction_end = day + pd.Timedelta(hours=9, minutes=25)
    open_ts = day + pd.Timedelta(hours=9, minutes=30)
    # 竞价时段（09:30 之前）的快照数量与时间范围。
    pre_open = df[df["trade_time"] < open_ts]
    summary["pre_open_rows"] = int(len(pre_open))
    if not pre_open.empty:
        summary["pre_open_first_time"] = str(pre_open["trade_time"].iloc[0])
        summary["pre_open_last_time"] = str(pre_open["trade_time"].iloc[-1])
    # 关键锚点：最后一根 09:30 之前的快照（累计量=集合竞价量，若语义成立），
    # 以及 09:30:00 当刻快照（累计量=竞价量+第一分钟连续竞价量）。
    for label, ts in (("last_before_0930", None), ("at_093000", open_ts)):
        if label == "last_before_0930":
            row = pre_open.iloc[-1] if not pre_open.empty else None
        else:
            exact = df[df["trade_time"] == ts]
            row = exact.iloc[-1] if not exact.empty else None
        if row is None:
            summary[label] = None
            continue
        summary[label] = {
            "trade_time": str(row["trade_time"]),
            "last": row.get("last"),
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "num_trades": row.get("num_trades"),
            "trading_phase_code": str(row.get("trading_phase_code")),
        }
    # 全天最后一根快照的累计量额：与同日分钟线总量守恒核对用。
    last = df.iloc[-1]
    summary["final_row"] = {
        "trade_time": str(last["trade_time"]),
        "volume": last.get("volume"),
        "amount": last.get("amount"),
        "num_trades": last.get("num_trades"),
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=int, default=20260903, help="交易日 YYYYMMDD")
    parser.add_argument("--codes", nargs="+", default=list(DEFAULT_CODES), help="证券代码列表")
    parser.add_argument("--begin-time", type=int, default=DEFAULT_BEGIN_TIME, help="HHMMSSmmm 九位整数")
    parser.add_argument("--end-time", type=int, default=DEFAULT_END_TIME, help="HHMMSSmmm 九位整数")
    parser.add_argument("--output", type=Path, help="输出目录，默认 data/probes/galaxy-snapshot-<date>/")
    parser.add_argument(
        "--force-login", action="store_true",
        help="登录前踢掉同账号旧会话；用于上次进程被强杀、服务端仍占用单会话额度时",
    )
    args = parser.parse_args()

    out_dir = (args.output or ROOT / "data/probes" / f"galaxy-snapshot-{args.date}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        raise SystemExit(f"refuse to overwrite existing probe dir: {out_dir}")

    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    manifest = {
        "started_at": timestamp(),
        "source": "galaxy-amazingdata-tgw",
        "interface": "AmazingData.MarketData.query_snapshot (tgw.QuerySnapshot, level-1 历史快照)",
        "trade_date": args.date,
        "codes": list(args.codes),
        "begin_time": args.begin_time,
        "end_time": args.end_time,
        "host": host,
        "port": port,
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
    }
    work = out_dir / f".work-{int(time.time())}"
    work.mkdir()
    original_cwd = Path.cwd()
    os.chdir(work)
    try:
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=args.force_login)
        manifest["force_login"] = bool(args.force_login)
        if not tgw.Login(cfg, api_mode):
            manifest["max_limitation_observed"] = bool(getattr(log_spi, "max_limitation", False))
            raise RuntimeError("login failed" + (" (账号已有活动会话；可加 --force-login 踢除旧会话)" if not args.force_login else ""))
        manifest["login"] = True
        # 关键：先用日历构造 MarketData 预热 tgw 回包线程（已在分钟线下载中实测必要）。
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=args.date)]
        if not calendar:
            raise RuntimeError("empty trading calendar; cannot initialize market data session")
        md = ad.MarketData(calendar)
        manifest["calendar_rows"] = len(calendar)
        manifest["date_in_calendar"] = args.date in set(calendar)

        started = time.monotonic()
        try:
            result = md.query_snapshot(
                list(args.codes), args.date, args.date,
                begin_time=args.begin_time, end_time=args.end_time,
            )
        except Exception as exc:
            manifest["query_error"] = f"{type(exc).__name__}: {exc}"
            raise
        manifest["query_elapsed_seconds"] = round(time.monotonic() - started, 3)
        manifest["result_type"] = type(result).__name__
        manifest["result_dates"] = [str(d) for d in result.keys()] if isinstance(result, dict) else None
    finally:
        try:
            tgw.Close()
            manifest["session_closed"] = True
        except Exception as exc:
            manifest["session_close_error"] = f"{type(exc).__name__}"
        os.chdir(original_cwd)
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    import pandas as pd

    per_code = []
    for date_key, code_map in (result or {}).items():
        for code, df in (code_map or {}).items():
            if df is None:
                per_code.append({"code": code, "rows": None, "note": "query returned None"})
                print(code, "returned None", flush=True)
                continue
            frame = df.copy()
            parquet_path = out_dir / f"{date_key}_{code.replace('.', '_')}.parquet"
            frame.to_parquet(parquet_path, index=False)
            summary = analyze_code(frame, code)
            summary["parquet"] = str(parquet_path.relative_to(ROOT))
            per_code.append(summary)
            print(code, summary["rows"], "rows", summary.get("first_time"), "->", summary.get("last_time"), flush=True)

    manifest["finished_at"] = timestamp()
    manifest["per_code"] = per_code
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print("wrote manifest", manifest_path, flush=True)


if __name__ == "__main__":
    main()
