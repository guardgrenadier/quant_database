"""最小可复现实验：stk_mins 请求切分、分页与终止条件（实验报告见 docs/research/minute-api-request-experiments.md）。

用法：
    set -a; source .env; set +a
    uv run python research/minute-api/experiment.py p0     # capabilities、日历、证券主数据
    uv run python research/minute-api/experiment.py p1     # 参数契约
    ... p2 过滤行为 / p3 分页 / p4 时间边界 / p5 空结果与对照 / analyze 汇总检查
    uv run python research/minute-api/experiment.py p6     # 多代码 2/5/10/20 批量矩阵
    uv run python research/minute-api/experiment.py p7     # 同一 20 代码逐代码对照
    uv run python research/minute-api/experiment.py p8     # 最近交易日 20 代码批次与逐代码交叉验证
    uv run python research/minute-api/experiment.py p9     # 已完成单代码的有界并发基准
    uv run python research/minute-api/experiment.py p10    # stock_basic 全状态单请求
    uv run python research/minute-api/experiment.py p11a   # 上退市主数据与日线边界对照
    uv run python research/minute-api/experiment.py p11b   # 上退市分钟边界单轮请求

每次 HTTP 请求追加一行脱敏摘要到 tmp/requests.jsonl；code=0 的响应行保存到 tmp/<step>.json
供 analyze 做集合对比。密钥只出现在请求 Header，不写入任何文件。
脚本不做自动重试（max_retries=0），遇到 429/503 原样记录，由人工决定是否补测。
"""

from __future__ import annotations

import json
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from quant_db.tushare import TushareClient, TushareError

BASE = "https://pcd.mobcvb.cn/tushare/pro"
CAPS = "https://pcd.mobcvb.cn/tushare/capabilities"
ROOT = Path(__file__).resolve().parent
TMP = ROOT / "tmp"
TMP.mkdir(exist_ok=True)
REQUEST_LOG = TMP / "requests.jsonl"
BENCHMARK_LOG = TMP / "batch_benchmark.jsonl"
PHASE_BUDGET = 15  # 单次运行最多发起的 HTTP 请求数

S, S2 = "000001.SZ", "600000.SH"
P6_CODES = [
    "000001.SZ", "600000.SH", "000002.SZ", "600004.SH", "920000.BJ",
    "000006.SZ", "600006.SH", "000007.SZ", "600007.SH", "920001.BJ",
    "000008.SZ", "600008.SH", "000009.SZ", "600009.SH", "920002.BJ",
    "000010.SZ", "600010.SH", "000011.SZ", "600011.SH", "920003.BJ",
]
DAY = "2026-09-18"  # 已结束交易日（周五），先用 trade_cal 核实
P8_DAY = "2026-09-22"  # 专项复测时最近的已结束交易日
PREV_DAY = "2026-09-17"
T0, T1 = f"{DAY} 09:30:00", f"{DAY} 15:00:00"

client = TushareClient(timeout=30, max_retries=0)
_request_count = 0


def http_get(url: str, params: dict | None = None) -> tuple[int, dict | None, dict, float]:
    global _request_count
    _request_count += 1
    if _request_count > PHASE_BUDGET:
        raise SystemExit(f"request budget {PHASE_BUDGET} exceeded")
    started = time.monotonic()
    resp = client.session.get(
        url,
        params=params,
        headers={"X-API-Key": client.api_key},
        timeout=client.timeout,
        verify=client.verify,
    )
    elapsed = time.monotonic() - started
    try:
        payload = resp.json()
    except ValueError:
        payload = None
    headers = {
        "x_request_id": resp.headers.get("X-Request-ID"),
        "x_cache": resp.headers.get("X-Cache"),
        "rl_remaining": resp.headers.get("X-RateLimit-Remaining"),
        "rl_ip_remaining": resp.headers.get("X-RateLimit-IP-Remaining"),
        "retry_after": resp.headers.get("Retry-After"),
    }
    return resp.status_code, payload, headers, elapsed


def rows_of(payload: dict) -> list[dict]:
    data = payload.get("data") or {}
    fields, items = data.get("fields") or [], data.get("items") or []
    return [dict(zip(fields, item)) for item in items]


def call(step: str, api: str, **params) -> list[dict]:
    """一次正式业务请求，不重试；记录摘要并保存响应行。"""
    params = {k: v for k, v in params.items() if v is not None}
    status, payload, headers, elapsed = http_get(f"{BASE}/{api}", params)
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "step": step,
        "api": api,
        "params": params,
        "http": status,
        "error": payload.get("error") if isinstance(payload, dict) else None,
        "code": payload.get("code") if isinstance(payload, dict) else None,
        "count": payload.get("count") if isinstance(payload, dict) else None,
        "elapsed_ms": round(elapsed * 1000),
        **headers,
    }
    rows: list[dict] = []
    if status == 200 and isinstance(payload, dict) and str(payload.get("code")) == "0":
        rows = rows_of(payload)
        (TMP / f"{step}.json").write_text(json.dumps(rows, ensure_ascii=False))
    record["rows"] = len(rows)
    with REQUEST_LOG.open("a") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    print(json.dumps(record, ensure_ascii=False))
    return rows


def capabilities(step: str, api: str) -> None:
    status, payload, headers, elapsed = http_get(f"{CAPS}/{api}")
    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "step": step,
        "api": f"capabilities/{api}",
        "http": status,
        "elapsed_ms": round(elapsed * 1000),
        **headers,
    }
    with REQUEST_LOG.open("a") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    if status == 200 and payload:
        keep = {
            k: payload.get(k)
            for k in ("name", "provider", "enabled", "methods", "required", "required_any", "probe_example", "notes")
            if k in payload
        }
        (TMP / f"{step}.json").write_text(json.dumps(keep, ensure_ascii=False, indent=1))
        print(step, json.dumps(keep, ensure_ascii=False))
    else:
        print(step, "HTTP", status, payload)


def load(step: str) -> list[dict]:
    path = TMP / f"{step}.json"
    return json.loads(path.read_text()) if path.exists() else []


def keys(rows: list[dict]) -> set[tuple]:
    return {(r.get("ts_code"), r.get("trade_time")) for r in rows}


def day_keys(rows: list[dict]) -> set[tuple]:
    """跨日窗口使用 (代码, 时间) 全量主键，与 keys 相同；保留函数名便于阅读。"""
    return keys(rows)


def stock_basic_codes() -> list[dict]:
    return load("stock_basic_l_0") + load("stock_basic_l_5000")


def p0() -> None:
    capabilities("caps_stk_mins", "stk_mins")
    capabilities("caps_a_share_mins", "a_share_mins")
    call(
        "trade_cal_202609",
        "trade_cal",
        exchange="SSE",
        start_date="20260901",
        end_date="20260930",
        fields="exchange,cal_date,is_open,pretrade_date",
        limit=500,
    )
    common = dict(
        list_status="L",
        fields="ts_code,symbol,name,exchange,market,list_date",
        limit=5000,
    )
    call("stock_basic_l_0", "stock_basic", offset=0, **common)
    call("stock_basic_l_5000", "stock_basic", offset=5000, **common)


def p1() -> None:
    base = dict(ts_code=S, freq="1min", start_date=T0, end_date=T1)
    call("p1_probe_limit5", "stk_mins", limit=5, **base)
    call("p1_full_day", "stk_mins", limit=500, **base)
    call("p1_freq_upper", "stk_mins", limit=5, **{**base, "freq": "1MIN"})
    call(
        "p1_freq_omitted",
        "stk_mins",
        ts_code=S,
        start_date=T0,
        end_date=T1,
        limit=5,
    )
    call(
        "p1_date_only",
        "stk_mins",
        ts_code=S,
        freq="1min",
        start_date=DAY.replace("-", ""),
        end_date=DAY.replace("-", ""),
        limit=500,
    )


def p2() -> None:
    base = dict(freq="1min", start_date=T0, end_date=T1)
    call("p2_multi2_full", "stk_mins", ts_code=f"{S},{S2}", limit=1000, **base)
    call("p2_multi2_limit300", "stk_mins", ts_code=f"{S},{S2}", limit=300, **base)
    call(
        "p2_fields_subset",
        "stk_mins",
        ts_code=S,
        fields="ts_code,trade_time,open,close,vol",
        limit=500,
        **base,
    )
    call(
        "p2_two_days",
        "stk_mins",
        ts_code=S,
        freq="1min",
        start_date=f"{PREV_DAY} 09:30:00",
        end_date=T1,
        limit=1000,
    )


def p3() -> None:
    base = dict(ts_code=S, freq="1min", start_date=T0, end_date=T1)
    for offset in (0, 100, 200, 300):
        call(f"p3_page100_{offset}", "stk_mins", limit=100, offset=offset, **base)
    call("p3_page241_0", "stk_mins", limit=241, offset=0, **base)
    call("p3_page241_241", "stk_mins", limit=241, offset=241, **base)
    call("p3_repeat_full", "stk_mins", limit=500, **base)
    call("p3_repeat_page100_100", "stk_mins", limit=100, offset=100, **base)
    multi = dict(ts_code=f"{S},{S2}", freq="1min", start_date=T0, end_date=T1, limit=300)
    call("p3_multipage_0", "stk_mins", offset=0, **multi)
    call("p3_multipage_300", "stk_mins", offset=300, **multi)


def p4() -> None:
    base = dict(ts_code=S, freq="1min", limit=500)
    call("p4_win_am", "stk_mins", start_date=T0, end_date=f"{DAY} 11:30:00", **base)
    call("p4_win_pm", "stk_mins", start_date=f"{DAY} 11:30:00", end_date=T1, **base)
    call("p4_win_to1300", "stk_mins", start_date=T0, end_date=f"{DAY} 13:00:00", **base)
    call("p4_win_from1300", "stk_mins", start_date=f"{DAY} 13:00:00", end_date=T1, **base)
    call("p4_win_wide", "stk_mins", start_date=f"{DAY} 00:00:00", end_date=f"{DAY} 23:59:59", **base)


def p5() -> None:
    base = dict(freq="1min", limit=500)
    call(
        "p5_sunday",
        "stk_mins",
        ts_code=S,
        start_date="2026-09-20 09:30:00",
        end_date="2026-09-20 15:00:00",
        **base,
    )
    listed = stock_basic_codes()
    newest = max((r for r in listed if r.get("list_date")), key=lambda r: r["list_date"])
    print("newest listing:", newest["ts_code"], newest["list_date"])
    call(
        "p5_prelisting",
        "stk_mins",
        ts_code=newest["ts_code"],
        start_date="2026-01-05 09:30:00",
        end_date="2026-01-05 15:00:00",
        **base,
    )
    susp = call(
        "p5_suspend_d",
        "suspend_d",
        trade_date=DAY.replace("-", ""),
        suspend_type="S",
        fields="ts_code,trade_date,suspend_type",
        limit=500,
    )
    if susp:
        code = susp[0]["ts_code"]
        print("suspended sample:", code)
        call("p5_suspended_mins", "stk_mins", ts_code=code, start_date=T0, end_date=T1, **base)
    bse = sorted(r["ts_code"] for r in listed if r["ts_code"].endswith(".BJ"))
    print("bse listed count:", len(bse), "sample:", bse[:3])
    if bse:
        call("p5_bse_mins", "stk_mins", ts_code=bse[len(bse) // 2], start_date=T0, end_date=T1, **base)
    call(
        "p5_ashare_1min",
        "a_share_mins",
        ts_code=S,
        freq="1min",
        start_date=T0,
        end_date=T1,
        limit=500,
    )


def p6() -> None:
    """固定字段与同日窗口，对 2/5/10/20 代码批次做可重放探测。"""

    base = dict(
        freq="1min",
        start_date=T0,
        end_date=T1,
        fields="ts_code,trade_time,freq,open,high,low,close,vol,amount",
        limit=5000,
        offset=0,
    )
    for size in (2, 5, 10, 20):
        call(f"p6_multi{size}_page0", "stk_mins", ts_code=",".join(P6_CODES[:size]), **base)


def p7() -> None:
    """与 p6 相同的 20 代码逐一请求，作为批量结果的非空与耗时对照。"""

    global PHASE_BUDGET
    PHASE_BUDGET = len(P6_CODES)
    base = dict(
        freq="1min",
        start_date=T0,
        end_date=T1,
        fields="ts_code,trade_time,freq,open,high,low,close,vol,amount",
        limit=500,
        offset=0,
    )
    for code in P6_CODES:
        call(f"p7_single_{code.replace('.', '_')}", "stk_mins", ts_code=code, **base)


def p8() -> None:
    """最近交易日的 20 代码批次与同组逐代码结果交叉验证。"""

    global PHASE_BUDGET
    PHASE_BUDGET = len(P6_CODES) + 2
    compact = P8_DAY.replace("-", "")
    start, end = f"{P8_DAY} 09:30:00", f"{P8_DAY} 15:00:00"
    fields = "ts_code,trade_time,freq,open,high,low,close,vol,amount"
    call(
        "p8_trade_cal_20260922", "trade_cal", exchange="SSE",
        start_date=compact, end_date=compact,
        fields="exchange,cal_date,is_open,pretrade_date", limit=5,
    )
    common = dict(freq="1min", start_date=start, end_date=end, fields=fields, offset=0)
    call("p8_multi20_page0", "stk_mins", ts_code=",".join(P6_CODES), limit=5000, **common)
    for code in P6_CODES:
        call(f"p8_single_{code.replace('.', '_')}", "stk_mins", ts_code=code, limit=500, **common)


def p9() -> None:
    """仅对 p7 已完成切片测 1/2/4/8 并发的热缓存传输耗时。"""

    ready = [code for code in P6_CODES if load(f"p7_single_{code.replace('.', '_')}")]
    if not ready:
        raise SystemExit("p7 has no completed single-code references")
    params = dict(
        freq="1min", start_date=T0, end_date=T1,
        fields="ts_code,trade_time,freq,open,high,low,close,vol,amount",
        limit=500, offset=0,
    )

    def fetch(code: str) -> dict:
        started = time.monotonic()
        try:
            result = TushareClient(timeout=30, max_retries=0).query("stk_mins", ts_code=code, **params)
            request = result.requests[-1]
            return {
                "code": code, "status": result.status, "rows": len(result.rows),
                "http": request.http_status, "elapsed_ms": round((time.monotonic() - started) * 1000),
            }
        except TushareError as exc:
            request = exc.requests[-1] if exc.requests else None
            return {
                "code": code, "status": "failed", "rows": 0,
                "http": request.http_status if request else None,
                "elapsed_ms": round((time.monotonic() - started) * 1000),
            }

    for workers in (1, 2, 4, 8):
        started = time.monotonic()
        with ThreadPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(fetch, ready))
        summary = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "step": "p9_hot_single_concurrency", "workers": workers,
            "codes": len(ready), "wall_ms": round((time.monotonic() - started) * 1000),
            "states": dict(Counter(r["status"] for r in results)),
            "rows": sum(r["rows"] for r in results),
            "request_elapsed_ms": [r["elapsed_ms"] for r in results],
        }
        with BENCHMARK_LOG.open("a") as fh:
            fh.write(json.dumps(summary, ensure_ascii=False) + "\n")
        print(json.dumps(summary, ensure_ascii=False))


def p10() -> None:
    """验证 stock_basic 五种 list_status 能否在一个请求中完整返回。"""

    base = dict(
        list_status="L,D,P,G,UN",
        fields="ts_code,symbol,name,exchange,market,list_status,list_date,delist_date",
        limit=10000,
        offset=0,
    )
    call("p10_all_statuses", "stock_basic", **base)
    call("p10_all_statuses_repeat", "stock_basic", **base)


def p11a() -> None:
    """验证退市主数据分交易所覆盖，并用日线定位样本的最后交易日。"""

    global PHASE_BUDGET
    PHASE_BUDGET = 9
    fields = "ts_code,symbol,name,exchange,market,list_status,list_date,delist_date"
    for exchange in ("SSE", "SZSE", "BSE"):
        call(
            f"p11_a1_d_{exchange.lower()}", "stock_basic", list_status="D", exchange=exchange,
            fields=fields, limit=10000, offset=0,
        )
    for code, start, end in (
        ("600193.SH", "20260501", "20260715"),
        ("002898.SZ", "20260515", "20260725"),
        ("920680.BJ", "20251101", "20260115"),
    ):
        call(
            f"p11_daily_{code.replace('.', '_')}", "daily", ts_code=code,
            start_date=start, end_date=end, fields="ts_code,trade_date,close,vol", limit=500, offset=0,
        )
    for code, day in (("301686.SZ", "20260922"), ("920229.BJ", "20260922"), ("920025.BJ", "20260923")):
        call(
            f"p11_c1_daily_{code.replace('.', '_')}", "daily", ts_code=code,
            trade_date=day, fields="ts_code,trade_date,open,close,vol", limit=5, offset=0,
        )


def p11b() -> None:
    """请求上市首日，以及退市样本的最后交易日、摘牌日和摘牌后日期。"""

    tasks = {
        "p11_b1_301686_list": ("301686.SZ", "2026-09-22"),
        "p11_b1_920229_list": ("920229.BJ", "2026-09-22"),
        "p11_c1_920025_list": ("920025.BJ", "2026-09-23"),
        "p11_b2_600193_last": ("600193.SH", "2026-06-29"),
        "p11_b2_600193_delist": ("600193.SH", "2026-07-06"),
        "p11_b2_600193_post": ("600193.SH", "2026-07-07"),
        "p11_b2_002898_last": ("002898.SZ", "2026-07-16"),
        "p11_b2_002898_delist": ("002898.SZ", "2026-07-17"),
        "p11_b2_002898_post": ("002898.SZ", "2026-07-20"),
        "p11_b2_920680_last": ("920680.BJ", "2025-12-31"),
        "p11_b2_920680_delist": ("920680.BJ", "2026-01-05"),
        "p11_b2_920680_post": ("920680.BJ", "2026-01-06"),
    }
    global PHASE_BUDGET
    PHASE_BUDGET = len(tasks)
    for step, (code, day) in tasks.items():
        call(
            step, "stk_mins", ts_code=code, freq="1min",
            start_date=f"{day} 09:30:00", end_date=f"{day} 15:00:00",
            fields="ts_code,trade_time,freq,open,high,low,close,vol,amount", limit=500, offset=0,
        )


def report_rows(name: str, rows: list[dict]) -> None:
    if not rows:
        print(f"{name}: 0 rows")
        return
    times = sorted(r["trade_time"] for r in rows)
    codes = sorted({r["ts_code"] for r in rows})
    ks = keys(rows)
    print(
        f"{name}: rows={len(rows)} codes={len(codes)} unique_keys={len(ks)} "
        f"min={times[0]} max={times[-1]} dup={len(rows) - len(ks)}"
    )


def compare(name: str, a: set, b: set) -> None:
    print(f"{name}: only_a={len(a - b)} only_b={len(b - a)} overlap={len(a & b)} equal={a == b}")


def analyze() -> None:
    full = load("p1_full_day")
    report_rows("p1_full_day", full)
    if full:
        times = [r["trade_time"] for r in full]
        seconds = sorted({t[-2:] for t in times})
        as_listed = list(full[0].keys())
        in_order = all(times[i] >= times[i + 1] for i in range(len(times) - 1))
        types = {k: type(full[0][k]).__name__ for k in full[0]}
        print("fields:", as_listed)
        print("value types (first row):", types)
        print("seconds suffixes:", seconds, "| descending order:", in_order)
        print("has 09:30:", any(t.endswith("09:30:00") for t in times), "| has 15:00:", any(t.endswith("15:00:00") for t in times))
        print("first row:", json.dumps(full[0], ensure_ascii=False))
        print("last row:", json.dumps(full[-1], ensure_ascii=False))
    compare("date_only vs full", keys(load("p1_date_only")), keys(full))
    compare("freq_upper rows vs limit5", keys(load("p1_freq_upper")), keys(load("p1_probe_limit5")))

    multi = load("p2_multi2_full")
    report_rows("p2_multi2_full", multi)
    per_code: dict[str, int] = {}
    for r in multi:
        per_code[r["ts_code"]] = per_code.get(r["ts_code"], 0) + 1
    print("multi2 per-code rows:", per_code)
    lim300 = load("p2_multi2_limit300")
    report_rows("p2_multi2_limit300", lim300)
    per_code300: dict[str, int] = {}
    for r in lim300:
        per_code300[r["ts_code"]] = per_code300.get(r["ts_code"], 0) + 1
    print("limit300 per-code rows:", per_code300, "-> limit 作用于整个响应" if len(lim300) == 300 else "")
    fs = load("p2_fields_subset")
    print("fields_subset fields:", list(fs[0].keys()) if fs else None, "rows:", len(fs))
    two = load("p2_two_days")
    report_rows("p2_two_days", two)
    if two:
        days = sorted({r["trade_time"][:10] for r in two})
        per_day = {d: sum(1 for r in two if r["trade_time"].startswith(d)) for d in days}
        print("two_days per-day rows:", per_day)

    ref = keys(full)
    pages = [load(f"p3_page100_{o}") for o in (0, 100, 200, 300)]
    union: set = set()
    overlaps = []
    for o, page in zip((0, 100, 200, 300), pages):
        pk = keys(page)
        overlaps.append((o, len(pk), len(union & pk)))
        union |= pk
    print("page100 sizes/overlap-with-prior:", overlaps)
    compare("page100 union vs full", union, ref)
    p241 = keys(load("p3_page241_0"))
    compare("page241_0 vs full", p241, ref)
    print("page100_300 rows:", len(pages[3]), "| page241_241 rows:", len(load("p3_page241_241")))
    print("repeat_full identical:", load("p3_repeat_full") == full)
    print("repeat_page100_100 identical:", load("p3_repeat_page100_100") == pages[1])
    mp0, mp300 = keys(load("p3_multipage_0")), keys(load("p3_multipage_300"))
    print("multipage overlap:", len(mp0 & mp300))
    compare("multipage union vs multi2_full", mp0 | mp300, keys(multi))

    am, pm = keys(load("p4_win_am")), keys(load("p4_win_pm"))
    compare("win_am + win_pm vs full", am | pm, ref)
    print("win overlap at 11:30:", len(am & pm))
    to13, from13 = keys(load("p4_win_to1300")), keys(load("p4_win_from1300"))
    compare("win_to1300 + win_from1300 vs full", to13 | from13, ref)
    print("win overlap at 13:00:", len(to13 & from13))
    compare("win_wide vs full", keys(load("p4_win_wide")), ref)

    report_rows("p5_sunday", load("p5_sunday"))
    report_rows("p5_prelisting", load("p5_prelisting"))
    report_rows("p5_suspended_mins", load("p5_suspended_mins"))
    report_rows("p5_bse_mins", load("p5_bse_mins"))
    ashare = load("p5_ashare_1min")
    report_rows("p5_ashare_1min", ashare)
    if ashare:
        print("ashare fields:", list(ashare[0].keys()))
        compare("ashare vs stk_mins keys", keys(ashare), ref)

    for size in (2, 5, 10, 20):
        name = f"p6_multi{size}_page0"
        rows = load(name)
        report_rows(name, rows)
        if rows:
            requested = set(P6_CODES[:size])
            returned = {r.get("ts_code") for r in rows}
            counts = {code: sum(r.get("ts_code") == code for r in rows) for code in sorted(returned)}
            print(
                f"{name}: missing_codes={sorted(requested - returned)} "
                f"unexpected_codes={sorted(returned - requested)} per_code={counts}"
            )

    if REQUEST_LOG.exists():
        history = [json.loads(line) for line in REQUEST_LOG.read_text().splitlines()]
        known_nonempty = {
            S: len(full),
            S2: len(load("p5_retry_600000_single")),
        }
        for size in (2, 5, 10, 20):
            name = f"p6_multi{size}_page0"
            events = [r for r in history if r.get("step") == name]
            states = Counter((r.get("http"), r.get("error"), r.get("rows")) for r in events)
            print(f"{name} request states: {dict(states)}")
            if any(http == 200 and rows == 0 for (http, _, rows) in states):
                witnesses = {code: count for code, count in known_nonempty.items() if code in P6_CODES[:size] and count}
                print(f"{name} empty response contradicted by single-code references: {witnesses}")

        single_rows = {code: load(f"p7_single_{code.replace('.', '_')}") for code in P6_CODES}
        print(
            "p7 single-code terminal coverage:",
            f"{sum(bool(rows) for rows in single_rows.values())}/{len(P6_CODES)}",
            {code: len(rows) for code, rows in single_rows.items() if rows},
        )
        recent_rows = {code: load(f"p8_single_{code.replace('.', '_')}") for code in P6_CODES}
        print(
            "p8 recent-day single-code terminal coverage:",
            f"{sum(bool(rows) for rows in recent_rows.values())}/{len(P6_CODES)}",
            {code: len(rows) for code, rows in recent_rows.items() if rows},
        )
        recent_events = [r for r in history if r.get("step") == "p8_multi20_page0"]
        recent_states = Counter((r.get("http"), r.get("error"), r.get("rows")) for r in recent_events)
        print("p8_multi20_page0 request states:", dict(recent_states))
        if any(http == 200 and rows == 0 for (http, _, rows) in recent_states):
            print(
                "p8_multi20_page0 empty response contradicted by single-code references:",
                {code: len(rows) for code, rows in recent_rows.items() if rows},
            )

    all_statuses = load("p10_all_statuses")
    repeated_statuses = load("p10_all_statuses_repeat")
    if all_statuses:
        codes = [row.get("ts_code") for row in all_statuses]
        print(
            "p10 stock_basic all statuses:",
            f"rows={len(all_statuses)} unique_codes={len(set(codes))}",
            f"statuses={dict(Counter(row.get('list_status') for row in all_statuses))}",
            f"repeat_identical={all_statuses == repeated_statuses}",
        )


PHASES = {
    "p0": p0, "p1": p1, "p2": p2, "p3": p3, "p4": p4,
    "p5": p5, "p6": p6, "p7": p7, "p8": p8, "p9": p9, "p10": p10,
    "p11a": p11a, "p11b": p11b, "analyze": analyze,
}

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in PHASES:
        raise SystemExit(f"usage: {sys.argv[0]} {{{','.join(PHASES)}}}")
    PHASES[sys.argv[1]]()
