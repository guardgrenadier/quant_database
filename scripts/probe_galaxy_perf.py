"""银河分钟接口小规模性能探针：串行 vs 低并发（2/4 在途）对照。

目的：为单股单日下载策略选择提供实测依据。复用 download_galaxy_min1.py 的
直连登录与底层 QueryKline 流程，对同一组“股票×交易日”Unit 分别按
串行（冷）→ 并发2 → 并发4 → 串行（热）执行，记录单请求延迟、总吞吐、失败，
并逐 Unit 校验并发返回内容与串行完全一致。

串行（热）对照用于区分并发收益与服务端缓存效应：如果第二轮串行远快于第一轮，
说明绝对延迟受服务端缓存影响，并发结论只按同缓存状态下的相对比较解读。

探针只写 data/probes，不写 warehouse / Canonical / Coverage。
"""

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]

# 测速样本：8 只沪深代表证券（沿用 download_galaxy_min1.py 已验证列表的子集）× 2 个已验证交易日。
CODES = (
    "000001.SZ", "000010.SZ", "300308.SZ", "300555.SZ",
    "300819.SZ", "600365.SH", "600519.SH", "688981.SH",
)
DATES = (20260903, 20261016)

# 出现这些服务端限流/配额错误码时立即停止后续加压，不得继续试探额度。
STOP_ERROR_CODES = (-82, -78, -91)  # kFlowOverLimit / kOverMaxQueryLimit / kOverLoad
# 单轮失败率超过该值也停止：说明并发已造成明显恶化。
STOP_FAILURE_RATIO = 0.25


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def run_batch(tgw, label, units, period, auto_complete, max_inflight, query_timeout):
    """执行一轮测速。max_inflight=1 为串行；>1 用线程池维持在途请求数。"""
    from download_galaxy_min1 import query_raw_kline

    results = {}

    def fetch(unit):
        code, trade_date = unit
        started = time.monotonic()
        try:
            rows = query_raw_kline(tgw, code, trade_date, period, auto_complete, query_timeout)
            return unit, {"rows": rows, "elapsed_seconds": time.monotonic() - started}
        except Exception as exc:
            return unit, {
                "error": f"{type(exc).__name__}: {exc}",
                "elapsed_seconds": time.monotonic() - started,
            }

    batch_started = time.monotonic()
    if max_inflight <= 1:
        for unit in units:
            unit, outcome = fetch(unit)
            results[unit] = outcome
    else:
        # 每个请求自带独立 Event/回调（见 query_raw_kline），线程池只控制在途数量。
        with ThreadPoolExecutor(max_workers=max_inflight) as pool:
            for unit, outcome in pool.map(fetch, units):
                results[unit] = outcome
    total = time.monotonic() - batch_started

    latencies = [o["elapsed_seconds"] for o in results.values()]
    failures = {f"{c}.{d}": o["error"] for (c, d), o in results.items() if "error" in o}
    summary = {
        "label": label,
        "max_inflight": max_inflight,
        "units": len(units),
        "total_seconds": round(total, 3),
        "ok": len(units) - len(failures),
        "failures": failures,
        "latency_seconds": {
            "min": round(min(latencies), 3),
            "median": round(sorted(latencies)[len(latencies) // 2], 3),
            "max": round(max(latencies), 3),
        },
        "per_unit": [
            {"code": c, "date": d, "rows": len(o["rows"]) if "rows" in o else None,
             "elapsed_seconds": round(o["elapsed_seconds"], 3), "error": o.get("error")}
            for (c, d), o in sorted(results.items())
        ],
    }
    ok_lat = [o["elapsed_seconds"] for o in results.values() if "rows" in o]
    if ok_lat:
        summary["throughput_units_per_second"] = round(len(ok_lat) / total, 2)
    print(
        f"[{label}] inflight={max_inflight} units={len(units)} ok={summary['ok']} "
        f"total={total:.2f}s median={summary['latency_seconds']['median']}s "
        f"max={summary['latency_seconds']['max']}s failures={len(failures)}",
        flush=True,
    )
    return summary, results


def rows_equal(a, b):
    """逐 Unit 校验并发与串行返回内容一致：行数、字段、逐行逐值完全相等。"""
    if len(a) != len(b):
        return False
    return all(ra == rb for ra, rb in zip(a, b))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True, help="必须是不存在的新目录")
    parser.add_argument("--query-timeout", type=int, default=30)
    parser.add_argument("--auto-complete", type=int, choices=(0, 1), default=1)
    args = parser.parse_args()

    out_dir = args.output.resolve()
    if out_dir.exists():
        raise SystemExit(f"refuse to use existing output dir: {out_dir}")
    out_dir.mkdir(parents=True)

    import sys
    sys.path.insert(0, str(ROOT / "scripts"))
    from download_galaxy_min1 import PROXY_NAMES, credentials

    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    report = {
        "started_at": timestamp(),
        "purpose": "galaxy min1 download: serial vs concurrency 2/4 throughput probe",
        "codes": list(CODES),
        "dates": list(DATES),
        "auto_complete": args.auto_complete,
        "query_timeout_seconds": args.query_timeout,
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
    }

    login_started = time.monotonic()
    cfg, api_mode, _log_spi = set_cfg(username, password, host, port, force_logout=False)
    if not tgw.Login(cfg, api_mode):
        # 登录失败也可能留下半初始化的会话状态，尽力关闭后再退出。
        try:
            tgw.Close()
        except Exception:
            pass
        raise SystemExit("login failed; aborting probe")
    report["login_seconds"] = round(time.monotonic() - login_started, 3)

    runs = []
    consistency = {}
    stopped_early = None
    try:
        # 与 download_galaxy_min1.py 相同：日历 + MarketData 预热，否则底层回调不触发。
        # 注意该高层调用无超时，实测可能挂起约 150s 后返回 None；生产必须用进程级硬超时包裹。
        warmup_started = time.monotonic()
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=DATES[-1])]
        if not calendar:
            raise SystemExit("empty trading calendar; aborting probe")
        ad.MarketData(calendar)
        report["warmup_seconds"] = round(time.monotonic() - warmup_started, 3)
        report["dates_in_calendar"] = {str(d): d in set(calendar) for d in DATES}

        period = ad.constant.Period.min1.value
        units = [(code, d) for d in DATES for code in CODES]

        all_results = {}
        # 顺序固定为 串行(冷) → 并发2 → 并发4 → 串行(热)。热串行是缓存对照；
        # 顺序对后期并发型有利，解读并发收益时以热串行为基准并在报告中注明。
        for label, inflight in (
            ("serial-cold", 1), ("concurrent-2", 2), ("concurrent-4", 4), ("serial-warm", 1),
        ):
            summary, results = run_batch(
                tgw, label, units, period, args.auto_complete, inflight, args.query_timeout,
            )
            runs.append(summary)
            all_results[label] = results
            # 限流/配额/服务端溢出错误码或高失败率：立即停止加压，如实记录。
            hit_limit = any(
                any(str(code) in err for code in STOP_ERROR_CODES) for err in summary["failures"].values()
            )
            if hit_limit or (len(summary["failures"]) / len(units)) > STOP_FAILURE_RATIO:
                stopped_early = f"stopped after {label}: limit error or failure ratio exceeded"
                break

        # 内容一致性：各并发轮与首轮串行逐 Unit 对照原始行。
        baseline = all_results.get("serial-cold", {})
        for label, results in all_results.items():
            if label == "serial-cold":
                continue
            mismatches = []
            for unit, base in baseline.items():
                if "rows" not in base:
                    continue
                other = results.get(unit, {})
                if "rows" not in other or not rows_equal(base["rows"], other["rows"]):
                    mismatches.append(f"{unit[0]}.{unit[1]}")
            consistency[f"{label}_vs_serial-cold"] = {
                "compared_units": sum(1 for b in baseline.values() if "rows" in b),
                "mismatched_units": mismatches,
            }
            # 内容一致性不因缓存改变，任何不一致都说明并发破坏语义，必须报告。
            if mismatches:
                print(f"[WARN] {label} content mismatch: {mismatches}", flush=True)
    finally:
        # 任何退出路径（含预热失败 SystemExit）都必须显式关闭会话并落盘已有结果：
        # 残留服务端会话会占用单账号连接额度，且疑似导致后续登录的预热挂起（本轮实测）。
        try:
            tgw.Close()
            report["session_closed"] = True
        except Exception as exc:
            report["session_close_error"] = f"{type(exc).__name__}: {exc}"
        report["runs"] = runs
        report["consistency"] = consistency
        report["stopped_early"] = stopped_early
        report["finished_at"] = timestamp()
        (out_dir / "perf-results.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8",
        )
        print("wrote", out_dir / "perf-results.json", flush=True)


if __name__ == "__main__":
    main()
