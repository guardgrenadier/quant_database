"""银河 AmazingData 与 Tushare 官方接口的日线（股票/指数）质量对照探针。

可丢弃研究代码：只读两个供应商接口，证据写入 data/probes，不写正式数据库。
对照维度：历史覆盖深度、逐日覆盖集合、OHLC/量额逐值一致性（银河原始整数级）、
停牌/无效代码语义、字段丰富度。凭据只从 .env 读取并脱敏。
"""

from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import ssl
import threading
import time
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
TUSHARE_ENDPOINT = "https://api.tushare.pro"
PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)
SECRETS = set()
OUTPUT = None

STOCKS = ["000001.SZ", "600519.SH", "002168.SZ", "300819.SZ", "300104.SZ", "688981.SH", "920000.BJ"]
INDICES = ["000001.SH", "000300.SH", "000905.SH", "399001.SZ", "399006.SZ", "801010.SI"]
# 逐值整数级对照的交易日：常规日、权威对照日、最近交易日
EXACT_DATES = [20240417, 20260903, 20261016]
# 语义用例：全天停牌、银河声明起点、覆盖前
SPECIAL_CASES = [
    ("300104.SZ", 20170417),  # 乐视网全天停牌
    ("000001.SZ", 20130104),  # 银河手册声明的 K 线起点附近
    ("000001.SZ", 20121231),  # 银河覆盖前（分钟已实测 kDataEmpty）
    ("000001.SZ", 20000104),  # 深度历史，仅 Tushare 预期有
]
RANGE_BEGIN, RANGE_END = 19900101, 20261021
TUSHARE_SLICES = [(19900101, 19991231), (20000101, 20091231), (20100101, 20191231), (20200101, 20261231)]
SW_SLICES = [(20100101, 20191231), (20200101, 20261231)]

ad = None
tgw = None


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def json_safe(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (ValueError, TypeError):
            pass
    return str(value)


def redact(text):
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def write_json(path, payload, compact=False):
    if compact:
        text = json.dumps(json_safe(payload), ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    else:
        text = json.dumps(json_safe(payload), ensure_ascii=False, indent=2, allow_nan=False)
    path.write_text(redact(text) + "\n", encoding="utf-8")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


# ---------------------------------------------------------------- 银河侧

def galaxy_login():
    global ad, tgw
    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required")
    SECRETS.update((username, password))
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as amazing_data
    import tgw as tgw_module
    from AmazingData.login.tgw_login import set_cfg

    ad = amazing_data
    tgw = tgw_module
    cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=False)
    started = time.monotonic()
    if not tgw.Login(cfg, api_mode):
        raise RuntimeError("AmazingData login failed")
    return {
        "login_elapsed_seconds": round(time.monotonic() - started, 3),
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
    }


def raw_kline_payload(result, status, accepted, immediate_error, elapsed):
    error_message = None
    if isinstance(status, int):
        error_message = tgw.GetErrorMsg(status)
    elif status is not None:
        error_message = str(status)
    records = result if isinstance(result, list) else []
    return {
        "accepted": accepted,
        "immediate_error": immediate_error,
        "callback_status": status,
        "error_message": error_message,
        "elapsed_seconds": round(elapsed, 3),
        "rows": len(records),
        "records": json_safe(records),
    }


def query_raw_kline(code, trade_date, period, timeout=60):
    """底层单日 K 线，保留原始整数与错误码（kDataEmpty 不改写为空表）。"""
    market_by_suffix = {"SH": tgw.MarketType.kSSE, "SZ": tgw.MarketType.kSZSE, "BJ": tgw.MarketType.kNEEQ}
    suffix = code.rsplit(".", 1)[-1]
    req = tgw.ReqKline()
    req.security_code = code.split(".", 1)[0]
    req.market_type = market_by_suffix[suffix]
    req.cq_flag = 0
    req.cq_date = 0
    req.qj_flag = 0
    req.cyc_type = period
    req.cyc_def = 0
    req.auto_complete = 1
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

    start = time.monotonic()
    accepted, immediate_error = tgw.QueryKline(req, callback, return_df_format=False)
    if accepted is not True:
        return raw_kline_payload(None, None, accepted, immediate_error, time.monotonic() - start)
    if not finished.wait(timeout):
        raise TimeoutError(f"raw kline callback exceeded {timeout}s for {code} {trade_date}")
    return raw_kline_payload(state.get("result"), state.get("status"), accepted, immediate_error, time.monotonic() - start)


def galaxy_frame_records(df):
    """高层日线 DataFrame → 紧凑记录列表；kline_time 转 YYYYMMDD 整数日期。"""
    records = []
    for _, row in df.iterrows():
        kt = row["kline_time"]
        if hasattr(kt, "strftime"):
            day = int(kt.strftime("%Y%m%d"))
        else:
            day = int(str(kt)[:10].replace("-", ""))
        records.append({
            "date": day,
            "open": json_safe(row["open"]), "high": json_safe(row["high"]),
            "low": json_safe(row["low"]), "close": json_safe(row["close"]),
            "volume": json_safe(row["volume"]), "amount": json_safe(row["amount"]),
        })
    return records


def galaxy_phase(out_dir):
    """银河：每代码一次全历史日线范围查询（覆盖/深度）+ 抽样单日底层整数（逐值对照）。"""
    login_info = galaxy_login()
    write_json(out_dir / "galaxy_login.json", login_info)
    base = ad.BaseData()
    calendar = [int(x) for x in base.get_calendar(data_type="str", market="SH", date=20261022)]
    market_data = ad.MarketData(calendar)
    summaries = {"range": {}, "raw": {}}

    for code in STOCKS + INDICES:
        case = {"code": code, "begin_date": RANGE_BEGIN, "end_date": RANGE_END, "period": "day"}
        try:
            result = market_data.query_kline(
                [code], begin_date=RANGE_BEGIN, end_date=RANGE_END, period=ad.constant.Period.day.value,
            )
            frame = result.get(code) if isinstance(result, dict) else None
            if frame is None:
                payload = {"request": case, "outcome": "NONE_FRAME", "rows": 0, "records": []}
                summaries["range"][code] = {"outcome": "NONE_FRAME", "rows": 0}
            else:
                records = galaxy_frame_records(frame)
                payload = {"request": case, "outcome": "RETURNED", "rows": len(records),
                           "columns": [str(c) for c in frame.columns], "records": records}
                summaries["range"][code] = {
                    "outcome": "RETURNED", "rows": len(records),
                    "first": records[0]["date"] if records else None,
                    "last": records[-1]["date"] if records else None,
                }
        except Exception as exc:
            payload = {"request": case, "outcome": "ERROR", "error": f"{type(exc).__name__}: {redact(str(exc))}"}
            summaries["range"][code] = {"outcome": "ERROR", "error": payload["error"]}
        write_json(out_dir / f"galaxy_range_{code.replace('.', '')}.json", payload, compact=True)
        print("galaxy range", code, summaries["range"][code], flush=True)
        time.sleep(0.2)

    raw_cases = [(code, day) for code in STOCKS for day in EXACT_DATES] + SPECIAL_CASES
    raw_cases += [(code, day) for code in INDICES if not code.endswith(".SI") for day in EXACT_DATES]
    for code, day in raw_cases:
        name = f"galaxy_raw_{code.replace('.', '')}_{day}"
        try:
            payload = query_raw_kline(code, day, ad.constant.Period.day.value)
            payload["request"] = {"code": code, "date": day, "period": "day"}
            summaries["raw"][f"{code}:{day}"] = {
                "rows": payload["rows"], "callback_status": payload["callback_status"],
                "error_message": payload["error_message"],
            }
        except Exception as exc:
            payload = {"request": {"code": code, "date": day}, "outcome": "ERROR",
                       "error": f"{type(exc).__name__}: {redact(str(exc))}"}
            summaries["raw"][f"{code}:{day}"] = {"outcome": "ERROR", "error": payload["error"]}
        write_json(out_dir / f"{name}.json", payload, compact=True)
        print("galaxy raw", code, day, summaries["raw"][f"{code}:{day}"], flush=True)
        time.sleep(0.2)

    tgw.Close()
    return summaries


# ---------------------------------------------------------------- Tushare 侧

def tushare_request(api_name, params, fields, token, interval_state):
    """串行 POST；瞬时传输错误有限重试（指数退避，上限 3 次），业务错误不重试。"""
    elapsed = time.monotonic() - interval_state["last"]
    if elapsed < 1.3:
        time.sleep(1.3 - elapsed)
    body = json.dumps({"api_name": api_name, "token": token, "params": params, "fields": fields}).encode()
    request = Request(TUSHARE_ENDPOINT, data=body, headers={"Content-Type": "application/json"})
    last_error = None
    for attempt in range(3):
        interval_state["last"] = time.monotonic()
        try:
            with urlopen(request, timeout=40) as response:
                payload = json.loads(response.read().decode())
            if payload.get("code") != 0:
                return {"outcome": "API_ERROR", "code": payload.get("code"), "msg": payload.get("msg")}
            data = payload.get("data") or {}
            return {"outcome": "RETURNED", "fields": data.get("fields"), "items": data.get("items"),
                    "has_more": data.get("has_more")}
        except (OSError, ssl.SSLError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            time.sleep(2 ** attempt)
    return {"outcome": "TRANSPORT_ERROR", "error": last_error}


def tushare_phase(out_dir):
    values = dotenv_values(ROOT / ".env")
    token = (values.get("TUSHARE_TOKEN") or "").strip()
    if not token:
        raise RuntimeError("TUSHARE_TOKEN is required")
    SECRETS.add(token)
    interval_state = {"last": 0.0}
    summaries = {}
    daily_fields = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"

    def fetch(api_name, code, slices, fields=daily_fields):
        rows = []
        meta = []
        for begin, end in slices:
            result = tushare_request(api_name, {"ts_code": code, "start_date": str(begin), "end_date": str(end)},
                                     fields, token, interval_state)
            meta.append({k: v for k, v in result.items() if k not in ("items", "fields")})
            if result["outcome"] != "RETURNED":
                return {"outcome": result["outcome"], "meta": meta, "rows": 0, "records": []}, meta
            if result.get("has_more"):
                meta[-1]["truncation_risk"] = True
            cols = result["fields"]
            for item in result["items"]:
                rows.append(dict(zip(cols, item)))
        return {"outcome": "RETURNED", "rows": len(rows), "records": rows}, meta

    for code in STOCKS:
        payload, meta = fetch("daily", code, TUSHARE_SLICES)
        payload["request"] = {"api_name": "daily", "ts_code": code, "slices": TUSHARE_SLICES, "slice_meta": meta}
        write_json(out_dir / f"tushare_daily_{code.replace('.', '')}.json", payload, compact=True)
        summaries[f"daily:{code}"] = {"outcome": payload["outcome"], "rows": payload.get("rows")}
        print("tushare daily", code, payload.get("rows"), payload["outcome"], flush=True)

    for code in INDICES:
        payload, meta = fetch("index_daily", code, TUSHARE_SLICES)
        payload["request"] = {"api_name": "index_daily", "ts_code": code, "slices": TUSHARE_SLICES, "slice_meta": meta}
        write_json(out_dir / f"tushare_index_daily_{code.replace('.', '')}.json", payload, compact=True)
        summaries[f"index_daily:{code}"] = {"outcome": payload["outcome"], "rows": payload.get("rows")}
        print("tushare index_daily", code, payload.get("rows"), payload["outcome"], flush=True)

    # 申万行业指数行情在 Tushare 侧走 sw_daily（index_daily 不含申万，已有探查结论）
    payload, meta = fetch("sw_daily", "801010.SI", SW_SLICES)
    payload["request"] = {"api_name": "sw_daily", "ts_code": "801010.SI", "slices": SW_SLICES, "slice_meta": meta}
    write_json(out_dir / "tushare_sw_daily_801010SI.json", payload, compact=True)
    summaries["sw_daily:801010.SI"] = {"outcome": payload["outcome"], "rows": payload.get("rows")}
    print("tushare sw_daily 801010.SI", payload.get("rows"), payload["outcome"], flush=True)
    return summaries


# ---------------------------------------------------------------- 对照

def to_number(value):
    return float(value) if value is not None else None


def compare_values(galaxy_rows, tushare_rows, is_index):
    """逐日逐值对照。单位换算：Tushare 股票 vol=手→股×100、amount=千元→元×1000。

    价格按 1e-6 元整数对照；量额先按换算假设计差值分布，不预设成立。
    """
    galaxy_by_date = {r["date"]: r for r in galaxy_rows}
    tushare_by_date = {int(r["trade_date"]): r for r in tushare_rows}
    common = sorted(set(galaxy_by_date) & set(tushare_by_date))
    price_fields = ("open", "high", "low", "close")
    stats = {
        "common_days": len(common),
        "price_exact": {f: 0 for f in price_fields},
        "price_mismatch": {f: 0 for f in price_fields},
        "volume_match_x100": 0, "volume_mismatch": 0,
        "amount_match_x1000_tol0.01": 0, "amount_mismatch": 0,
        "max_abs_amount_diff": 0.0,
        "mismatch_samples": [],
    }
    for day in common:
        g, t = galaxy_by_date[day], tushare_by_date[day]
        sample = {"date": day}
        mismatch = False
        for field in price_fields:
            gv, tv = to_number(g[field]), to_number(t[field])
            if gv is None or tv is None:
                stats["price_mismatch"][field] += 1
                sample[f"{field}_g"] = g[field]
                sample[f"{field}_t"] = t[field]
                mismatch = True
                continue
            # 双方浮点各自 ×1e6 取整，等价于比较原始 1e-6 元整数
            if round(gv * 1_000_000) == round(tv * 1_000_000):
                stats["price_exact"][field] += 1
            else:
                stats["price_mismatch"][field] += 1
                sample[f"{field}_g"] = g[field]
                sample[f"{field}_t"] = t[field]
                mismatch = True
        gv, tv = to_number(g["volume"]), to_number(t["vol"])
        if gv is not None and tv is not None and abs(gv - tv * 100) < 0.5:
            stats["volume_match_x100"] += 1
        else:
            stats["volume_mismatch"] += 1
            sample["volume_g"] = g["volume"]
            sample["vol_t"] = t["vol"]
            mismatch = True
        ga, ta = to_number(g["amount"]), to_number(t["amount"])
        if ga is not None and ta is not None:
            diff = abs(ga - ta * 1000)
            stats["max_abs_amount_diff"] = max(stats["max_abs_amount_diff"], diff)
            if diff <= 0.01:
                stats["amount_match_x1000_tol0.01"] += 1
            else:
                stats["amount_mismatch"] += 1
                sample["amount_g"] = g["amount"]
                sample["amount_t"] = t["amount"]
                mismatch = True
        else:
            stats["amount_mismatch"] += 1
            mismatch = True
        if mismatch and len(stats["mismatch_samples"]) < 5:
            stats["mismatch_samples"].append(sample)
    return stats, galaxy_by_date, tushare_by_date


def compare_phase(out_dir):
    comparison = {"stocks": {}, "indices": {}, "raw_exact": {}}

    def load(name):
        return json.loads((out_dir / name).read_text(encoding="utf-8"))

    for code in STOCKS + INDICES:
        g_payload = load(f"galaxy_range_{code.replace('.', '')}.json")
        if code.endswith(".SI"):
            t_name = "tushare_sw_daily_801010SI.json"
        elif code in INDICES:
            t_name = f"tushare_index_daily_{code.replace('.', '')}.json"
        else:
            t_name = f"tushare_daily_{code.replace('.', '')}.json"
        t_payload = load(t_name)
        g_rows = g_payload.get("records") or []
        t_rows = t_payload.get("records") or []
        entry = {
            "galaxy_rows": len(g_rows), "tushare_rows": len(t_rows),
            # 两侧原始顺序不作假设（银河升序、Tushare 降序），统一用 min/max
            "galaxy_first": min(r["date"] for r in g_rows) if g_rows else None,
            "galaxy_last": max(r["date"] for r in g_rows) if g_rows else None,
            "tushare_first": min(int(r["trade_date"]) for r in t_rows) if t_rows else None,
            "tushare_last": max(int(r["trade_date"]) for r in t_rows) if t_rows else None,
        }
        if g_rows and t_rows:
            stats, g_by, t_by = compare_values(g_rows, t_rows, code in INDICES)
            only_t = sorted(set(t_by) - set(g_by))
            only_g = sorted(set(g_by) - set(t_by))
            entry.update(stats)
            entry["only_tushare_days"] = len(only_t)
            entry["only_tushare_ge2013"] = sum(1 for d in only_t if d >= 20130101)
            entry["only_tushare_sample_ge2013"] = [d for d in only_t if d >= 20130101][:10]
            entry["only_galaxy_days"] = len(only_g)
            entry["only_galaxy_sample"] = only_g[:10]
        comparison["stocks" if code in STOCKS else "indices"][code] = entry

    # 单日底层整数级对照：银河 raw（价格 1e-6 元、量 100 股、额 1e-5 元）vs Tushare 换算整数
    for code in [c for c in STOCKS] + [c for c in INDICES if not c.endswith(".SI")]:
        t_name = ("tushare_daily_" if code in STOCKS else "tushare_index_daily_") + code.replace(".", "") + ".json"
        t_rows = load(t_name).get("records") or []
        t_by = {int(r["trade_date"]): r for r in t_rows}
        for day in EXACT_DATES + [d for c, d in SPECIAL_CASES if c == code]:
            raw_name = f"galaxy_raw_{code.replace('.', '')}_{day}.json"
            if not (out_dir / raw_name).exists():
                continue
            raw = load(raw_name)
            key = f"{code}:{day}"
            g_records = raw.get("records") or []
            t = t_by.get(day)
            result = {
                "galaxy_rows": len(g_records), "galaxy_status": raw.get("callback_status"),
                "tushare_row": bool(t),
            }
            if len(g_records) == 1 and t:
                g = g_records[0]
                checks = {}
                for gf, tf in (("open_price", "open"), ("high_price", "high"),
                               ("low_price", "low"), ("close_price", "close")):
                    checks[gf] = (int(g[gf]) == round(float(t[tf]) * 1_000_000))
                checks["volume"] = (int(g["volume_trade"]) == round(float(t["vol"]) * 100 * 100))
                checks["amount"] = (int(g["value_trade"]) == round(float(t["amount"]) * 1000 * 100_000))
                result["exact_int_match"] = checks
                result["all_match"] = all(checks.values())
                if not result["all_match"]:
                    result["galaxy_raw"] = {k: g[k] for k in ("open_price", "high_price", "low_price",
                                                              "close_price", "volume_trade", "value_trade")}
                    result["tushare"] = {k: t[k] for k in ("open", "high", "low", "close", "vol", "amount")}
            comparison["raw_exact"][key] = result

    write_json(out_dir / "comparison.json", comparison)
    return comparison


def main():
    global OUTPUT
    OUTPUT = (ROOT / "data/probes" / datetime.now().strftime("daily-compare-%Y%m%d-%H%M%S")).resolve()
    OUTPUT.mkdir(parents=True, exist_ok=False)
    manifest = {
        "started_at": timestamp(),
        "purpose": "银河 vs Tushare 股票/指数日线质量对照；只读，不写正式库",
        "stocks": STOCKS, "indices": INDICES, "exact_dates": EXACT_DATES,
        "special_cases": SPECIAL_CASES, "range": [RANGE_BEGIN, RANGE_END],
        "database_written": False,
    }
    write_json(OUTPUT / "run_manifest.json", manifest)
    galaxy_dir = OUTPUT
    try:
        galaxy_summaries = galaxy_phase(galaxy_dir)
        manifest["galaxy"] = "COMPLETED"
    except Exception as exc:
        manifest["galaxy"] = f"FATAL: {type(exc).__name__}: {redact(str(exc))}"
        galaxy_summaries = None
        try:
            if tgw is not None:
                tgw.Close()
        except Exception:
            pass
    try:
        tushare_summaries = tushare_phase(OUTPUT)
        manifest["tushare"] = "COMPLETED"
    except Exception as exc:
        manifest["tushare"] = f"FATAL: {type(exc).__name__}: {redact(str(exc))}"
        tushare_summaries = None
    if galaxy_summaries is not None and tushare_summaries is not None:
        compare_phase(OUTPUT)
        manifest["comparison"] = "COMPLETED"
    manifest["finished_at"] = timestamp()
    write_json(OUTPUT / "run_manifest.json", manifest)
    print("evidence dir:", OUTPUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
