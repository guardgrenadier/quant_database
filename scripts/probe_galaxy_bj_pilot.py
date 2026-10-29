"""探查脚本（可丢弃实验代码）：北交所 Pilot 遗留项 + 快照历史深度 + T+N 修订。

覆盖 docs/design/stock_1min_ingestion_proposal.md 第 11 节第 5 条中与 BJ/快照相关的遗留项：
1. 快照历史深度：query_snapshot 窄窗沿日期阶梯下探（沪深 + BJ 新旧代码各自时代），定位最早可得日；
2. BJ 新股上市首日：920003.BJ（2025-11-07 上市）首日分钟 + 竞价快照 + 930 守恒，前一日作未上市对照；
3. BJ 涨跌停样本：银河日线筛 30% 涨跌停日，拉该日分钟 + 竞价快照验证形态与守恒；
4. T+N 修订：重拉 2026-09-03 分钟（auto=1，原十只）与全天快照（原六只），与既有 parquet 逐值对比；
5. 底层长跨度请求：单请求多日 min1（920000.BJ @ 2026-09-01..09-30），验证 >10 日跨度。

只读取与本地落盘到 data/probes/，不写 warehouse / Canonical / Coverage。
"""

import argparse
import json
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
SUFFIX_MARKET = {"SH": "kSSE", "SZ": "kSZSE", "BJ": "kNEEQ"}

# 快照深度阶梯：沪深全程用 000001.SZ/600519.SH；BJ 新代码 920000 限迁移后，旧代码 832000 限迁移前。
SNAP_DEPTH_PROBES = [
    ("000001.SZ", [20261009, 20260803, 20260601, 20260401, 20260105, 20251103,
                   20250602, 20241202, 20240102, 20230403, 20230103, 20220104, 20211115]),
    ("600519.SH", [20250602, 20230403, 20211115]),
    ("920000.BJ", [20251010, 20251009]),
    ("832000.BJ", [20250930, 20250602, 20241202, 20230403, 20211115]),
]
IPO_CODE = "920003.BJ"   # 中诚咨询，stock_basic list_date=2025-11-07
IPO_DATE = 20251107
# 涨跌停筛选：前 24 只在市 920 代码的 2026 年日线。
LIMIT_SCAN_CODES = [
    "920000.BJ", "920001.BJ", "920002.BJ", "920003.BJ", "920005.BJ", "920006.BJ",
    "920007.BJ", "920008.BJ", "920009.BJ", "920010.BJ", "920011.BJ", "920012.BJ",
    "920013.BJ", "920014.BJ", "920015.BJ", "920016.BJ", "920017.BJ", "920018.BJ",
    "920019.BJ", "920020.BJ", "920021.BJ", "920022.BJ", "920023.BJ", "920024.BJ",
]
LIMIT_SCAN_RANGE = (20260101, 20261023)
TEN_CODES = [  # download_galaxy_min1.DEFAULT_CODES，T+N 对比用
    "000001.SZ", "000010.SZ", "002168.SZ", "300308.SZ", "300555.SZ",
    "300819.SZ", "600365.SH", "600519.SH", "688184.SH", "688981.SH",
]
SIX_SNAP_CODES = ["000001.SZ", "002168.SZ", "300308.SZ", "600519.SH", "600365.SH", "688981.SH"]
LONG_SPAN = ("920000.BJ", 20260901, 20260930)


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def credentials():
    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or os.environ.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or os.environ.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or os.environ.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required in .env")
    return username, password, host, port


def query_kline(tgw, code, begin_date, end_date, period, auto_complete, begin_time=900, end_time=1700, timeout=60):
    """通用原始 K 线查询：支持跨日跨度；非零状态码照常返回不抛异常。"""
    req = tgw.ReqKline()
    req.security_code = code.split(".", 1)[0]
    req.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[code.rsplit(".", 1)[-1]])
    req.cq_flag = 0
    req.cq_date = 0
    req.qj_flag = 0
    req.cyc_type = period
    req.cyc_def = 0
    req.auto_complete = auto_complete
    req.begin_date = begin_date
    req.end_date = end_date
    req.begin_time = begin_time
    req.end_time = end_time
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"] = result
        state["status"] = status
        finished.set()

    accepted, immediate_error = tgw.QueryKline(req, callback, return_df_format=False)
    if accepted is not True:
        return {"accepted": False, "immediate_error": str(immediate_error)}
    if not finished.wait(timeout):
        return {"accepted": True, "timeout": True}
    status = state.get("status")
    return {
        "accepted": True,
        "status": status if isinstance(status, int) else 0,
        "status_msg": tgw.GetErrorMsg(status) if isinstance(status, int) and status != 0 else None,
        "rows": state.get("result") or [],
    }


def auction_snapshot(md, codes, date, end_time=92530000):
    """竞价窄窗快照；返回 {code: summary}，并给出原始 df 供落盘。"""
    res = md.query_snapshot(list(codes), date, date, begin_time=92457000, end_time=end_time)
    out, frames = {}, {}
    for dkey, code_map in (res or {}).items():
        for code, df in (code_map or {}).items():
            if df is None:
                out[code] = None
                continue
            df = df.sort_values("trade_time")
            frames[code] = df
            pre = df[df["trade_time"].dt.strftime("%H%M%S") < "093000"]
            anc = pre[pre["volume"] > 0]
            out[code] = {
                "rows": int(len(df)),
                "first_time": str(df["trade_time"].iloc[0]),
                "anchor_time": str(anc["trade_time"].iloc[0]) if not anc.empty else None,
                "anchor_volume": float(anc["volume"].iloc[0]) if not anc.empty else None,
                "anchor_amount": float(anc["amount"].iloc[0]) if not anc.empty else None,
            }
    return out, frames


def main():
    import pandas as pd

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--force-login", action="store_true")
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    out_dir = (args.output or ROOT / "data/probes" / f"galaxy-bj-pilot-{today}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        raise SystemExit(f"refuse to overwrite existing probe dir: {out_dir}")
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(exist_ok=True)

    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    manifest = {
        "started_at": timestamp(),
        "source": "galaxy-amazingdata-tgw",
        "probe_goal": "BJ pilot leftovers: snapshot depth, IPO first day, limit samples, T+N revision, long-span request",
        "host": host, "port": port,
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
    }
    work = out_dir / f".work-{int(time.time())}"
    work.mkdir()
    original_cwd = Path.cwd()
    os.chdir(work)
    try:
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=args.force_login)
        if not tgw.Login(cfg, api_mode):
            raise RuntimeError("login failed")
        manifest["login"] = True
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=20261023)]
        md = ad.MarketData(calendar)
        min1 = ad.constant.Period.min1.value
        day1 = ad.constant.Period.day.value

        # 1. 快照历史深度阶梯
        depth = []
        for code, dates in SNAP_DEPTH_PROBES:
            for date in dates:
                started = time.monotonic()
                try:
                    res = md.query_snapshot([code], date, date, begin_time=92457000, end_time=92530000)
                    df = (res.get(date) or {}).get(code)
                    rec = {"code": code, "date": date,
                           "rows": None if df is None else int(len(df)),
                           "elapsed": round(time.monotonic() - started, 3)}
                    if df is not None and not df.empty:
                        df = df.sort_values("trade_time")
                        anc = df[df["volume"] > 0]
                        rec["first_time"] = str(df["trade_time"].iloc[0])
                        rec["anchor_time"] = str(anc["trade_time"].iloc[0]) if not anc.empty else None
                except Exception as exc:
                    rec = {"code": code, "date": date, "error": f"{type(exc).__name__}: {exc}"}
                depth.append(rec)
                print("depth", code, date, rec.get("rows"), rec.get("error", ""), flush=True)
                time.sleep(0.2)
        manifest["snapshot_depth"] = depth

        # 2. BJ 新股首日 + 前一日对照
        ipo = {}
        for date in (IPO_DATE - 1, IPO_DATE):
            rec = query_kline(tgw, IPO_CODE, date, date, min1, 0)
            rows = rec.pop("rows", [])
            rec["n_rows"] = len(rows)
            if rows:
                labels = sorted(r["kline_time"] % 10000 for r in rows)
                rec["first_label"] = labels[0]
                rec["last_label"] = labels[-1]
                pd.DataFrame(rows).to_parquet(raw_dir / f"ipo_kline_{date}.parquet", index=False)
            ipo[f"kline_{date}"] = {k: v for k, v in rec.items()}
            print("ipo kline", date, rec["n_rows"], rec.get("status_msg"), flush=True)
            time.sleep(0.2)
        snap, frames = auction_snapshot(md, [IPO_CODE], IPO_DATE, end_time=93100000)
        ipo["auction_snapshot"] = snap
        for code, df in frames.items():
            df.to_parquet(raw_dir / f"ipo_snapshot_{IPO_DATE}.parquet", index=False)
        # 首日 930 守恒
        kl_path = raw_dir / f"ipo_kline_{IPO_DATE}.parquet"
        if kl_path.exists() and frames.get(IPO_CODE) is not None:
            kdf = pd.read_parquet(kl_path)
            bar930 = kdf[kdf["kline_time"] % 10000 == 930]
            sdf = frames[IPO_CODE]
            last930 = sdf[sdf["trade_time"] <= sdf["trade_time"].dt.normalize() + pd.Timedelta(hours=9, minutes=30, seconds=57)]
            if not bar930.empty and not last930.empty:
                ipo["conservation_930_diff"] = int(bar930["volume_trade"].iloc[0]) - float(last930["volume"].iloc[-1]) * 100
        manifest["ipo_first_day"] = ipo

        # 3. 涨跌停扫描（银河日线）+ 样本分钟验证
        limit_days = []
        for code in LIMIT_SCAN_CODES:
            rec = query_kline(tgw, code, *LIMIT_SCAN_RANGE, day1, 0)
            rows = rec.pop("rows", [])
            rows = sorted(rows, key=lambda r: r["kline_time"])
            prev_close = None
            for r in rows:
                close = r["close_price"] / 1_000_000
                if prev_close and prev_close > 0:
                    pct = close / prev_close - 1
                    if abs(pct) >= 0.295:
                        limit_days.append({
                            "code": code, "date": r["kline_time"], "pct": round(pct, 4),
                            "close": close, "volume_raw": r["volume_trade"],
                        })
                prev_close = close
            time.sleep(0.2)
        manifest["limit_scan"] = {"codes": len(LIMIT_SCAN_CODES), "range": LIMIT_SCAN_RANGE, "hits": limit_days}
        print("limit hits:", len(limit_days), flush=True)
        limit_samples = []
        for hit in limit_days[:4]:
            code, date = hit["code"], hit["date"]
            krec = query_kline(tgw, code, date, date, min1, 0)
            rows = krec.pop("rows", [])
            krec["n_rows"] = len(rows)
            if rows:
                pd.DataFrame(rows).to_parquet(raw_dir / f"limit_kline_{code.replace('.', '_')}_{date}.parquet", index=False)
            snap, frames = auction_snapshot(md, [code], date, end_time=93100000)
            entry = {"kline": krec, "snapshot": snap.get(code)}
            if rows and frames.get(code) is not None:
                sdf = frames[code]
                bar930 = [r for r in rows if r["kline_time"] % 10000 == 930]
                last930 = sdf[sdf["trade_time"] <= sdf["trade_time"].dt.normalize() + pd.Timedelta(hours=9, minutes=30, seconds=57)]
                if bar930 and not last930.empty:
                    entry["conservation_930_diff"] = int(bar930[0]["volume_trade"]) - float(last930["volume"].iloc[-1]) * 100
                frames[code].to_parquet(raw_dir / f"limit_snapshot_{code.replace('.', '_')}_{date}.parquet", index=False)
            limit_samples.append(entry)
            print("limit sample", code, date, krec["n_rows"], entry.get("conservation_930_diff"), flush=True)
            time.sleep(0.2)
        manifest["limit_samples"] = limit_samples

        # 4. T+N 修订：重拉 2026-09-03 分钟（auto=1）与全天快照，与既有 parquet 逐值对比
        import sys as _sys
        _sys.path.insert(0, str(ROOT / "scripts"))
        from download_galaxy_min1 import to_frame_records

        revision = {"minute": [], "snapshot": []}
        for code in TEN_CODES:
            rec = query_kline(tgw, code, 20260903, 20260903, min1, 1)
            rows = rec.pop("rows", [])
            new_df = to_frame_records(rows, code)
            old = pd.read_parquet(ROOT / "data/downloads/galaxy/20260903_min1_auto1.parquet")
            old_code = old[old["security_code"] == code].reset_index(drop=True)
            new_df = new_df.reset_index(drop=True)
            same = old_code.equals(new_df)
            entry = {"code": code, "rows_now": len(new_df), "rows_old": len(old_code), "identical": bool(same)}
            if not same and len(old_code) == len(new_df):
                diff_cols = [c for c in old_code.columns if not old_code[c].equals(new_df[c])]
                entry["diff_columns"] = diff_cols
                for c in diff_cols:
                    mask = old_code[c] != new_df[c]
                    both_na = old_code[c].isna() & new_df[c].isna()
                    changed = mask & ~both_na
                    entry.setdefault("diff_detail", {})[c] = int(changed.sum())
            revision["minute"].append(entry)
            print("revision min1", code, entry["identical"], flush=True)
            time.sleep(0.2)
        res = md.query_snapshot(SIX_SNAP_CODES, 20260903, 20260903, begin_time=90000000, end_time=150100000)
        for dkey, code_map in (res or {}).items():
            for code, df in (code_map or {}).items():
                old_path = ROOT / f"data/probes/galaxy-snapshot-20260903/20260903_{code.replace('.', '_')}.parquet"
                if not old_path.exists():
                    continue
                old_df = pd.read_parquet(old_path).reset_index(drop=True)
                new_df = df.reset_index(drop=True)
                same = old_df.equals(new_df)
                entry = {"code": code, "rows_now": len(new_df), "rows_old": len(old_df), "identical": bool(same)}
                if not same and len(old_df) == len(new_df):
                    entry["diff_columns"] = [c for c in old_df.columns if not old_df[c].equals(new_df[c])]
                revision["snapshot"].append(entry)
                print("revision snap", code, entry["identical"], flush=True)
        manifest["revision_20260903"] = revision

        # 5. 底层长跨度请求：单请求 22 个交易日 min1
        code, b, e = LONG_SPAN
        started = time.monotonic()
        rec = query_kline(tgw, code, b, e, min1, 0, timeout=120)
        rows = rec.pop("rows", [])
        days = sorted({r["kline_time"] // 10000 for r in rows})
        manifest["long_span"] = {
            "code": code, "begin": b, "end": e,
            "n_rows": len(rows), "n_days": len(days),
            "days": days, "elapsed": round(time.monotonic() - started, 3),
            "status": rec.get("status"), "status_msg": rec.get("status_msg"),
        }
        print("long span", len(rows), "rows", len(days), "days", flush=True)
    finally:
        try:
            tgw.Close()
            manifest["session_closed"] = True
        except Exception as exc:
            manifest["session_close_error"] = f"{type(exc).__name__}"
        os.chdir(original_cwd)
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    manifest["finished_at"] = timestamp()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print("wrote manifest", manifest_path, flush=True)


if __name__ == "__main__":
    main()
