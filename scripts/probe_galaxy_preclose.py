"""探查脚本（可丢弃实验代码）：银河快照 pre_close 在除权除息日的语义判定。

问题：银河 query_snapshot 返回的 pre_close，在除权除息日是"未调整昨收"还是"除权除息后的参考昨收"？

方法（区分实测事实与推断）：
- 样本：2 个纯现金分红（600519.SH、000596.SZ）、3 个送转+现金（688256.SH、001298.SZ、920037.BJ），
  另加对照（同股票除权日后一交易日；无分红 000001.SZ 同日）；
- 银河侧：底层 tgw.QuerySnapshot(return_df_format=False) 取原始整数与原始时间戳（不经 SDK 转换层），
  除权日窄窗 09:24:57–09:25:30 的 pre_close；日线接口（cyc_type=day）取上一交易日原始 close；
- tushare 侧：daily 的 close/pre_close、adj_factor、dividend 明细，原始响应留证；
- 判定算术：除权参考价 =（昨收 − 每股现金）÷（1 + 送转比例）；独立复核 = 昨收 × adj_factor(昨) ÷ adj_factor(今)。

只读取与本地落盘 data/probes/，不写 warehouse / Canonical，不改正式代码。
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

# (code, ex_date, 类型标注, 每股现金(税前), 每股送转比例)
EX_SAMPLES = [
    ("600519.SH", 20260626, "cash", 28.02423, 0.0),
    ("000596.SZ", 20260717, "cash", 3.4, 0.0),
    ("688256.SH", 20260508, "stock+cash", 1.5, 0.49),
    ("001298.SZ", 20260417, "stock+cash", 0.07, 0.45),
    ("920037.BJ", 20260508, "stock+cash", 0.656, 0.4),
]
CLEAN_CONTROLS = [("000001.SZ", 20260626)]  # 无分红股票普通日对照
SNAP_BEGIN, SNAP_END = 92457000, 92530000


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def tushare_query(token, api, params, fields=""):
    from urllib.request import Request, urlopen
    body = json.dumps({"api_name": api, "token": token, "params": params, "fields": fields}).encode()
    req = Request("https://api.tushare.pro", data=body, headers={"Content-Type": "application/json"})
    with urlopen(req, timeout=40) as r:
        return json.loads(r.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--force-login", action="store_true")
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    out_dir = (args.output or ROOT / "data/probes" / f"galaxy-preclose-{today}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        raise SystemExit(f"refuse to overwrite existing probe dir: {out_dir}")

    manifest = {
        "started_at": timestamp(),
        "probe_goal": "galaxy snapshot pre_close semantics on ex-dividend dates",
        "samples": [
            {"code": c, "ex_date": d, "kind": k, "cash_div_per_share": cash, "stk_div_ratio": stk}
            for c, d, k, cash, stk in EX_SAMPLES
        ],
        "clean_controls": CLEAN_CONTROLS,
    }

    # ---- Phase 0: 交易日历（tushare trade_cal 线上只读），计算 prev/next 交易日 ----
    vals = dotenv_values(ROOT / ".env")
    token = next(v for k, v in vals.items() if "TOKEN" in k.upper())
    resp = tushare_query(token, "trade_cal", {"exchange": "SSE", "start_date": "20260401", "end_date": "20260831", "is_open": "1"}, "exchange,cal_date,is_open")
    cal_items = (resp.get("data") or {}).get("items") or []
    cal = sorted(r[1] for r in cal_items)
    calset = set(cal)
    for c, d, *_ in EX_SAMPLES + CLEAN_CONTROLS:
        ds = str(d)
        if ds not in calset:
            raise SystemExit(f"{d} not a trading day per tushare trade_cal")
    (out_dir / "tushare_trade_cal_2026.json").write_text(json.dumps(cal_items, ensure_ascii=False) + "\n", encoding="utf-8")
    def prev_day(d):
        i = cal.index(str(d))
        return int(cal[i - 1])
    def next_day(d):
        i = cal.index(str(d))
        return int(cal[i + 1])

    # 每个采样日做：当日快照 + 前一日/当日日线。对照日只做当日快照与前一日日线。
    sample_days = []  # (code, date, role, kind)
    for c, d, k, cash, stk in EX_SAMPLES:
        sample_days.append((c, d, "ex_date", k))
        sample_days.append((c, next_day(d), "control_next", k))
    for c, d in CLEAN_CONTROLS:
        sample_days.append((c, d, "control_clean", "none"))
    manifest["calendar_note"] = "prev/next trading days from tushare trade_cal (SSE, 2026-04..08, is_open=1)"

    # ---- Phase 1: tushare 证据（daily / adj_factor / dividend）----
    ts_evidence = {}
    codes = sorted({c for c, _, _, _ in sample_days})
    for code in codes:
        entry = {}
        ex_dates = [d for c, d, role, _ in sample_days if c == code and role == "ex_date"]
        resp = tushare_query(token, "adj_factor", {"ts_code": code}, "ts_code,trade_date,adj_factor")
        items = (resp.get("data") or {}).get("items") or []
        entry["adj_factor_raw_rows"] = len(items)
        factors = {r[1]: r[2] for r in items}
        relevant = sorted({d for c, d, _, _ in sample_days if c == code} | {prev_day(d) for c, d, _, _ in sample_days if c == code})
        entry["adj_factor"] = {str(d): factors.get(str(d)) for d in relevant}
        resp = tushare_query(token, "daily", {"ts_code": code, "start_date": str(min(relevant)), "end_date": str(max(relevant))},
                             "ts_code,trade_date,open,high,low,close,pre_close,vol,amount")
        entry["daily"] = [dict(zip((resp.get("data") or {}).get("fields") or [], r)) for r in (resp.get("data") or {}).get("items") or []]
        if ex_dates:
            resp = tushare_query(token, "dividend", {"ts_code": code}, "")
            entry["dividend_all"] = [dict(zip((resp.get("data") or {}).get("fields") or [], r)) for r in (resp.get("data") or {}).get("items") or []]
        ts_evidence[code] = entry
        time.sleep(0.2)
    (out_dir / "tushare_evidence.json").write_text(json.dumps(ts_evidence, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("tushare evidence saved for", len(codes), "codes", flush=True)

    # ---- Phase 2: 银河原始快照 + 日线 ----
    username = (vals.get("AD_USERNAME") or os.environ.get("AD_USERNAME") or "").strip()
    password = (vals.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD") or "").strip()
    host = (vals.get("AD_HOST") or os.environ.get("AD_HOST") or "101.230.159.235").strip()
    port = int(vals.get("AD_PORT") or os.environ.get("AD_PORT") or "8600")
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]
    manifest["proxy_environment_removed"] = removed_proxy

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg
    manifest["sdk"] = {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()}

    work = out_dir / f".work-{int(time.time())}"
    work.mkdir()
    original_cwd = Path.cwd()
    os.chdir(work)

    galaxy_rows = []
    day_klines = {}
    try:
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=args.force_login)
        if not tgw.Login(cfg, api_mode):
            raise RuntimeError("login failed")
        manifest["login"] = True
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=20261023)]
        ad.MarketData(calendar)
        day_period = ad.constant.Period.day.value

        def raw_snapshot(code, date):
            """底层 tgw.QuerySnapshot，return_df_format=False，拿未缩放原始字段。"""
            req = tgw.ReqDefault()
            req.security_code = code.split(".", 1)[0]
            req.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[code.rsplit(".", 1)[-1]])
            req.date = date
            req.begin_time = SNAP_BEGIN
            req.end_time = SNAP_END
            req.data_type = 0
            req.level_type = 0
            finished = threading.Event()
            state = {}

            def callback(result, status):
                state["result"] = result
                state["status"] = status
                finished.set()

            accepted, err = tgw.QuerySnapshot(req, callback, return_df_format=False)
            if accepted is not True:
                return {"accepted": False, "immediate_error": str(err)}
            if not finished.wait(60):
                return {"accepted": True, "timeout": True}
            return {"accepted": True, "status": state.get("status"), "result": state.get("result")}

        def day_kline(code, date):
            req = tgw.ReqKline()
            req.security_code = code.split(".", 1)[0]
            req.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[code.rsplit(".", 1)[-1]])
            req.cq_flag = 0
            req.cq_date = 0
            req.qj_flag = 0
            req.cyc_type = day_period
            req.cyc_def = 0
            req.auto_complete = 0
            req.begin_date = date
            req.end_date = date
            req.begin_time = 0
            req.end_time = 2359
            finished = threading.Event()
            state = {}

            def callback(result, status):
                state["result"] = result
                state["status"] = status
                finished.set()

            accepted, err = tgw.QueryKline(req, callback, return_df_format=False)
            if accepted is not True:
                return {"accepted": False, "immediate_error": str(err)}
            if not finished.wait(60):
                return {"accepted": True, "timeout": True}
            status = state.get("status")
            return {"accepted": True,
                    "status": status if isinstance(status, int) else 0,
                    "status_msg": tgw.GetErrorMsg(status) if isinstance(status, int) and status != 0 else None,
                    "rows": state.get("result") or []}

        for code, date, role, kind in sample_days:
            snap = raw_snapshot(code, date)
            rows = snap.get("result") or []
            for r in rows:
                galaxy_rows.append({"code": code, "date": date, "role": role, "kind": kind, "raw": r})
            print("snap", code, date, role, "rows:", len(rows), "status:", snap.get("status"), flush=True)
            time.sleep(0.2)
            for kd in sorted({prev_day(date), date}):
                key = (code, kd)
                if key in day_klines:
                    continue
                rec = day_kline(code, kd)
                day_klines[f"{code}|{kd}"] = {k: v for k, v in rec.items()}
                print("dayk", code, kd, "rows:", len(rec.get("rows") or []), "status:", rec.get("status_msg"), flush=True)
                time.sleep(0.2)
    finally:
        try:
            tgw.Close()
            manifest["session_closed"] = True
        except Exception as exc:
            manifest["session_close_error"] = f"{type(exc).__name__}"
        os.chdir(original_cwd)
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    # 原始快照行：result 可能是 list[dict]；逐字段原样保存（含原始整数 pre_close / orig_time）。
    (out_dir / "galaxy_raw_snapshot_rows.json").write_text(
        json.dumps(galaxy_rows, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    (out_dir / "galaxy_day_klines.json").write_text(
        json.dumps(day_klines, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")

    # ---- Phase 3: 汇总分析（原始值 + 缩放 + 判定算术）----
    PRICE_SCALE = 1_000_000
    analysis = []
    for code, date, role, kind in sample_days:
        rows = [g for g in galaxy_rows if g["code"] == code and g["date"] == date]
        if not rows:
            analysis.append({"code": code, "date": date, "role": role, "error": "no snapshot rows"})
            continue
        first = rows[0]["raw"]
        pre_close_raw = first.get("pre_close_price")
        pre_close_scaled = pre_close_raw / PRICE_SCALE if isinstance(pre_close_raw, int) else None
        prev = prev_day(date)
        pkl = (day_klines.get(f"{code}|{prev}") or {}).get("rows") or []
        prev_close_raw = pkl[-1]["close_price"] if pkl else None
        prev_close_scaled = prev_close_raw / PRICE_SCALE if isinstance(prev_close_raw, int) else None
        ts = ts_evidence.get(code, {})
        factors = ts.get("adj_factor") or {}
        f_prev = factors.get(str(prev))
        f_cur = factors.get(str(date))
        daily = {str(r["trade_date"]): r for r in ts.get("daily") or []}
        ts_cur = daily.get(str(date)) or {}
        ts_prev = daily.get(str(prev)) or {}
        # 除权参考价两种独立算式
        cash = next((cash for c, d, k, cash, stk in EX_SAMPLES if c == code and d == date), 0.0)
        stk = next((stk for c, d, k, cash, stk in EX_SAMPLES if c == code and d == date), 0.0)
        ref_by_dividend = (prev_close_scaled - cash) / (1 + stk) if prev_close_scaled is not None else None
        ref_by_factor = prev_close_scaled * f_prev / f_cur if (prev_close_scaled is not None and f_prev and f_cur) else None
        analysis.append({
            "code": code, "date": date, "role": role, "kind": kind,
            "snapshot_first_row_time_raw": first.get("orig_time"),
            "snapshot_pre_close_raw": pre_close_raw,
            "snapshot_pre_close_scaled": pre_close_scaled,
            "galaxy_prev_day_close_raw": prev_close_raw,
            "galaxy_prev_day_close_scaled": prev_close_scaled,
            "tushare_prev_close": ts_prev.get("close"),
            "tushare_cur_pre_close": ts_cur.get("pre_close"),
            "adj_factor_prev": f_prev, "adj_factor_cur": f_cur,
            "ref_by_dividend": ref_by_dividend,
            "ref_by_factor": ref_by_factor,
            "preclose_minus_prevclose": (pre_close_scaled - prev_close_scaled) if (pre_close_scaled is not None and prev_close_scaled is not None) else None,
            "preclose_minus_ref_by_dividend": (pre_close_scaled - ref_by_dividend) if ref_by_dividend is not None and pre_close_scaled is not None else None,
            "preclose_minus_ref_by_factor": (pre_close_scaled - ref_by_factor) if ref_by_factor is not None and pre_close_scaled is not None else None,
        })
    manifest["analysis"] = analysis
    manifest["finished_at"] = timestamp()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print("wrote manifest", manifest_path, flush=True)
    for a in analysis:
        print(json.dumps(a, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
