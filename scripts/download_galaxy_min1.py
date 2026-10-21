"""用银河 AmazingData/tgw 接口下载指定若干股票的单日一分钟 K 线并保存为 Parquet。

登录与底层异步 K 线请求复用 scripts/probe_amazingdata.py 中已验证的直连流程：
set_cfg + tgw.Login + tgw.QueryKline 回调 + 显式超时。本脚本只做只读取与本地落盘，
不写入 warehouse / Canonical / Coverage，也不修改任何 ingestion 架构。
"""

import argparse
import os
import threading
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

# 默认十只代表性证券：沪市主板/科创、深市主板/创业板混合，均已在探查证据中确认可取到分钟线。
DEFAULT_CODES = (
    "000001.SZ", "000010.SZ", "002168.SZ", "300308.SZ", "300555.SZ",
    "300819.SZ", "600365.SH", "600519.SH", "688184.SH", "688981.SH",
)
SUFFIX_MARKET = {"SH": "kSSE", "SZ": "kSZSE", "BJ": "kNEEQ"}
# 银河原始整数到常规单位的缩放：价格 ×1,000,000、成交量 ×100（股）、成交额 ×100,000（元）。
PRICE_SCALE = 1_000_000
VOLUME_SCALE = 100
VALUE_SCALE = 100_000
RAW_FIELDS = (
    "open_price", "high_price", "low_price", "close_price",
    "volume_trade", "value_trade",
)


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


def query_raw_kline(tgw, code, trade_date, period, auto_complete, timeout):
    """单股单日原始 K 线：显式填充每个请求字段，回调等待加超时。"""
    suffix = code.rsplit(".", 1)[-1]
    if suffix not in SUFFIX_MARKET:
        raise ValueError(f"unsupported code suffix: {code}")
    req = tgw.ReqKline()
    # SWIG 新建对象字段可能带未初始化垃圾值，逐个显式赋值避免污染服务端请求。
    req.security_code = code.split(".", 1)[0]
    req.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[suffix])
    req.cq_flag = 0
    req.cq_date = 0
    req.qj_flag = 0
    req.cyc_type = period
    req.cyc_def = 0
    req.auto_complete = auto_complete
    req.begin_date = trade_date
    req.end_date = trade_date
    req.begin_time = 900
    req.end_time = 1700
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"] = result
        state["status"] = status
        finished.set()

    accepted, immediate_error = tgw.QueryKline(req, callback, return_df_format=False)
    if accepted is not True:
        raise RuntimeError(f"{code}: request not accepted (immediate_error={immediate_error})")
    if not finished.wait(timeout):
        raise TimeoutError(f"{code}: kline callback exceeded {timeout}s")
    status = state.get("status")
    # kDataEmpty 等非零状态保留错误码，不静默当作空表。
    if isinstance(status, int) and status != 0:
        raise RuntimeError(f"{code}: callback status {status} ({tgw.GetErrorMsg(status)})")
    return state.get("result") or []


def to_frame_records(rows, code):
    """把原始整数行转为可读记录：保留未缩放整数（*_raw），另加缩放后的常规单位列。"""
    import pandas as pd

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    # kline_time 形如 202609030930，是银河口径的开始分钟标签（含集合竞价并入口径）。
    df["kline_time"] = pd.to_datetime(df["kline_time"].astype("int64"), format="%Y%m%d%H%M")
    # 原始行的 security_code 是无后缀裸码；保留为 *_bare，另设带交易所后缀的标准代码列。
    df = df.rename(columns={"security_code": "security_code_bare"})
    df.insert(0, "security_code", code)
    # 原始整数列以 _raw 保留，缩放列便于研究读取；二者都落盘，不做有损替换。
    for field in RAW_FIELDS:
        df[field + "_raw"] = df[field].astype("int64")
    df["open"] = df["open_price_raw"] / PRICE_SCALE
    df["high"] = df["high_price_raw"] / PRICE_SCALE
    df["low"] = df["low_price_raw"] / PRICE_SCALE
    df["close"] = df["close_price_raw"] / PRICE_SCALE
    df["volume"] = df["volume_trade_raw"] / VOLUME_SCALE
    df["amount"] = df["value_trade_raw"] / VALUE_SCALE
    df["market_type"] = df["market_type"].astype("int64")
    df["variety_category"] = df["variety_category"].astype("int64")
    return df


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=int, default=20260903, help="交易日 YYYYMMDD")
    parser.add_argument("--codes", nargs="+", default=list(DEFAULT_CODES), help="证券代码列表")
    parser.add_argument(
        "--auto-complete", type=int, choices=(0, 1), default=1,
        help="1=补全 240 根开始分钟（无成交分钟补前价）；0=仅返回有成交的原始分钟",
    )
    parser.add_argument("--output", type=Path, help="输出 Parquet 路径，默认 data/downloads/galaxy/")
    parser.add_argument("--query-timeout", type=int, default=60)
    parser.add_argument(
        "--force-login", action="store_true",
        help="登录前踢掉同账号旧会话；用于上次进程被强杀、服务端仍占用单会话额度时",
    )
    args = parser.parse_args()

    out_dir = (args.output.parent if args.output else ROOT / "data/downloads/galaxy").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = args.output.resolve() if args.output else out_dir / f"{args.date}_min1_auto{args.auto_complete}.parquet"
    manifest_path = parquet_path.with_suffix(".manifest.json")
    if parquet_path.exists():
        raise SystemExit(f"refuse to overwrite existing file: {parquet_path}")

    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    manifest = {
        "started_at": timestamp(),
        "source": "galaxy-amazingdata-tgw",
        "interface": "tgw.QueryKline (min1)",
        "trade_date": args.date,
        "auto_complete": args.auto_complete,
        "codes": list(args.codes),
        "host": host,
        "port": port,
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
        "scaling": {"price": PRICE_SCALE, "volume": VOLUME_SCALE, "amount": VALUE_SCALE},
        "note": "银河口径：开盘集合竞价并入 09:30 首根，收盘竞价并入 14:59；开始分钟标签。",
    }
    work = out_dir / f".work-{int(time.time())}"
    work.mkdir()
    original_cwd = Path.cwd()
    os.chdir(work)
    period = ad.constant.Period.min1.value
    frames = []
    failures = []
    try:
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=args.force_login)
        manifest["force_login"] = bool(args.force_login)
        if not tgw.Login(cfg, api_mode):
            manifest["max_limitation_observed"] = bool(getattr(log_spi, "max_limitation", False))
            raise RuntimeError("login failed" + (" (账号已有活动会话；可加 --force-login 踢除旧会话)" if not args.force_login else ""))
        manifest["login"] = True
        # 关键：先用日历构造 MarketData 预热 tgw 回包线程，否则底层 QueryKline 回调不会触发（已实测）。
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=args.date)]
        if not calendar:
            raise RuntimeError("empty trading calendar; cannot initialize market data session")
        ad.MarketData(calendar)
        manifest["calendar_rows"] = len(calendar)
        manifest["date_in_calendar"] = args.date in set(calendar)
        for code in args.codes:
            started = time.monotonic()
            try:
                rows = query_raw_kline(tgw, code, args.date, period, args.auto_complete, args.query_timeout)
            except Exception as exc:
                # 单只失败不阻断整批：记录原因，继续其余证券，最后如实报告。
                failures.append({"code": code, "error": f"{type(exc).__name__}: {exc}"})
                manifest.setdefault("per_code", []).append({"code": code, "error": failures[-1]["error"]})
                print(code, "FAILED", failures[-1]["error"], flush=True)
                continue
            frame = to_frame_records(rows, code)
            per_code = {"code": code, "rows": int(len(frame)), "elapsed_seconds": round(time.monotonic() - started, 3)}
            if not frame.empty:
                per_code["first_kline_time"] = frame["kline_time"].min().isoformat()
                per_code["last_kline_time"] = frame["kline_time"].max().isoformat()
            manifest.setdefault("per_code", []).append(per_code)
            print(code, per_code["rows"], "rows", flush=True)
            frames.append(frame)
            time.sleep(0.2)  # 保守请求节奏，非已确认的供应商限流值。
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
    import json

    combined = pd.concat([f for f in frames if not f.empty], ignore_index=True) if frames else pd.DataFrame()
    if combined.empty:
        manifest["finished_at"] = timestamp()
        manifest["failures"] = failures
        manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        raise SystemExit(f"no data downloaded; see {manifest_path}")
    combined.to_parquet(parquet_path, index=False)
    manifest["finished_at"] = timestamp()
    manifest["total_rows"] = int(len(combined))
    manifest["codes_ok"] = [p["code"] for p in manifest.get("per_code", []) if "rows" in p]
    manifest["failures"] = failures
    manifest["parquet"] = str(parquet_path.relative_to(ROOT))
    manifest["columns"] = [str(c) for c in combined.columns]
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("wrote", parquet_path, len(combined), "rows;", manifest_path)


if __name__ == "__main__":
    main()
