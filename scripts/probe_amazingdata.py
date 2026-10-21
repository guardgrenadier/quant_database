"""只读探查银河 AmazingData/tgw；保存脱敏证据，不写正式数据或账号配置。"""

import argparse
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
GROUPS = ("minimal", "base", "kline", "status", "factors", "index", "recovery", "extended", "authority")
PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)
SECRETS = set()
OUTPUT = None
SUMMARIES = []
ABORT = False
ad = None
tgw = None


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def json_safe(value):
    """把 SDK/pandas 返回值转为 JSON，同时把非有限浮点显式保留为 null。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        # NaN/Inf 不能悄悄序列化成非标准 JSON 常量；空值统计另在字段摘要中体现。
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except (TypeError, ValueError):
            pass
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (ValueError, TypeError):
            pass
    return str(value)


def redact(value):
    text = str(value)
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def write_json(path, payload):
    text = json.dumps(json_safe(payload), ensure_ascii=False, indent=2, allow_nan=False)
    path.write_text(redact(text) + "\n", encoding="utf-8")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def frame_records(df, full=True):
    """保留有意义的索引；K 线的 RangeIndex 不冒充供应商字段。"""
    import pandas as pd

    include_index = not isinstance(df.index, pd.RangeIndex)
    selected = df if full else pd.concat((df.head(5), df.tail(5))).drop_duplicates()
    records = []
    for index, row in selected.iterrows():
        record = {str(k): json_safe(v) for k, v in row.to_dict().items()}
        if include_index:
            record["__index"] = json_safe(index)
        records.append(record)
    return records


def frame_payload(df, full=True):
    if df is None:
        return None
    records = frame_records(df, full=full)
    return {
        "shape": list(df.shape),
        "columns": [str(c) for c in df.columns],
        "dtypes": {str(c): str(t) for c, t in df.dtypes.items()},
        "nulls": {str(c): int(df[c].isna().sum()) for c in df.columns},
        "index": {"name": json_safe(df.index.name), "type": type(df.index).__name__},
        "evidence_records_truncated": not full and len(df) > 10,
        "records": records,
    }


def dict_frames_payload(value, full=True):
    if not isinstance(value, dict):
        return {"return_type": type(value).__name__, "value": json_safe(value)}
    return {
        "keys": [str(k) for k in value],
        "frames": {str(code): frame_payload(frame, full=full) for code, frame in value.items()},
    }


def raw_kline_payload(result, status, accepted, immediate_error, elapsed):
    """原样保留底层数据与错误；kDataEmpty 不改写成正常空表。"""
    error_message = None
    if isinstance(status, int):
        error_message = tgw.GetErrorMsg(status)
    elif status is not None:
        error_message = str(status)
    if isinstance(immediate_error, int) and not error_message:
        error_message = tgw.GetErrorMsg(immediate_error)
    records = result if isinstance(result, list) else []
    fields = sorted({str(k) for row in records if isinstance(row, dict) for k in row})
    return {
        "accepted": accepted,
        "immediate_error": immediate_error,
        "callback_status": status,
        "error_message": error_message,
        "elapsed_seconds": round(elapsed, 3),
        "return_type": type(result).__name__,
        "rows": len(records),
        "fields": fields,
        "records": json_safe(result),
    }


def query_raw_kline(code, trade_date, period, auto_complete=1, begin_time=900, end_time=1700, timeout=60):
    """用异步回调加显式超时，绕开 tgw 同步接口的无期限 Event.wait()。"""
    market_by_suffix = {"SH": tgw.MarketType.kSSE, "SZ": tgw.MarketType.kSZSE, "BJ": tgw.MarketType.kNEEQ}
    suffix = code.rsplit(".", 1)[-1]
    if suffix not in market_by_suffix:
        raise ValueError(f"unsupported probe code: {code}")
    req = tgw.ReqKline()
    # SWIG 新建对象的字段可能带未初始化值；每个请求字段都显式赋值，避免垃圾值进入服务端。
    req.security_code = code.split(".", 1)[0]
    req.market_type = market_by_suffix[suffix]
    req.cq_flag = 0
    req.cq_date = 0
    req.qj_flag = 0
    req.cyc_type = period
    req.cyc_def = 0
    req.auto_complete = auto_complete
    req.begin_date = trade_date
    req.end_date = trade_date
    req.begin_time = begin_time
    req.end_time = end_time
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
    return raw_kline_payload(
        state.get("result"), state.get("status"), accepted, immediate_error, time.monotonic() - start,
    )


def query_async_with_timeout(invoke, timeout, label):
    """给复权和第三方资讯底层查询保留原始 JSON/错误码，并限制回调等待时间。"""
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"] = result
        state["status"] = status
        finished.set()

    start = time.monotonic()
    accepted, immediate_error = invoke(callback)
    if accepted is not True:
        return raw_kline_payload(None, None, accepted, immediate_error, time.monotonic() - start)
    if not finished.wait(timeout):
        raise TimeoutError(f"callback exceeded {timeout}s for {label}")
    return raw_kline_payload(
        state.get("result"), state.get("status"), accepted, immediate_error, time.monotonic() - start,
    )


def record_case(name, request, function, *, fatal_on_exception=False):
    """每个实验立即落盘；RETURNED 仅表示有返回，不表示数据完整或验收通过。"""
    global ABORT
    if ABORT:
        return None
    started = time.monotonic()
    evidence = {"case": name, "request": json_safe(request), "requested_at": timestamp()}
    try:
        response = function()
        evidence["outcome"] = "RETURNED"
        evidence["response"] = response
    except Exception as exc:
        # 失败实验同样保留；异常文本只记录脱敏后的类型与消息，不保存配置或凭据。
        evidence["outcome"] = "ERROR"
        evidence["error"] = f"{type(exc).__name__}: {redact(exc)}"
        response = None
        if fatal_on_exception:
            ABORT = True
    evidence["elapsed_seconds"] = round(time.monotonic() - started, 3)
    path = OUTPUT / f"{name}.json"
    write_json(path, evidence)
    summary = {
        "case": name,
        "outcome": evidence["outcome"],
        "elapsed_seconds": evidence["elapsed_seconds"],
        "file": path.name,
        "sha256": sha256(path),
    }
    if "error" in evidence:
        summary["error"] = evidence["error"]
    if isinstance(response, dict):
        for key in ("accepted", "immediate_error", "callback_status", "error_message", "rows"):
            if key in response:
                summary[key] = json_safe(response[key])
    SUMMARIES.append(summary)
    write_json(OUTPUT / "summary.json", SUMMARIES)
    print(name, evidence["outcome"], summary.get("rows", ""), summary.get("error_message", ""), flush=True)
    time.sleep(0.2)  # 实验调用间隔只是保守探查节奏，不是已确认的供应商限流值。
    return response


def credentials():
    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or os.environ.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or os.environ.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or os.environ.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required")
    SECRETS.update((username, password))
    return username, password, host, port


def main():
    global OUTPUT, ad, tgw
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--group", action="append", choices=GROUPS, help="默认执行全部；minimal 为最小调用")
    parser.add_argument("--output", type=Path, help="必须是不存在的新证据目录")
    parser.add_argument("--query-timeout", type=int, default=60, help="底层单次 K 线回调等待秒数")
    args = parser.parse_args()
    groups = set(args.group or GROUPS)
    OUTPUT = (args.output or ROOT / "data/probes" / datetime.now().strftime("amazingdata-%Y%m%d-%H%M%S")).resolve()
    OUTPUT.mkdir(parents=True, exist_ok=False)
    work = OUTPUT / "work"
    work.mkdir()
    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]
    manifest = {
        "started_at": timestamp(),
        "groups": sorted(groups),
        "purpose": "read-only interface research; no Canonical/Coverage writes",
        "host": host,
        "port": port,
        "username": "<redacted>",
        "password": "<redacted>",
        "force_logout": False,
        "proxy_environment_removed": removed_proxy,
        "database_written": False,
        "official_manual": "https://gitee.com/cgs2026/xysz/blob/master/xysz/xysz_tools/AmazingData开发手册.md",
        "official_manual_sha256_at_probe": "198427d2d107ba292776320ab26942099f50d085d2036b9a95f8e49b74d9f115",
    }
    write_json(OUTPUT / "run_manifest.json", manifest)
    original_cwd = Path.cwd()
    os.chdir(work)
    session_open = False
    try:
        import AmazingData as amazing_data
        import tgw as tgw_module
        from AmazingData.login.tgw_login import set_cfg

        ad = amazing_data
        tgw = tgw_module
        manifest["sdk"] = {
            "AmazingData": ad.__version__,
            "tgw_runtime": tgw.GetVersion(),
            "python": os.sys.version.split()[0],
        }
        write_json(OUTPUT / "run_manifest.json", manifest)
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=False)
        started = time.monotonic()
        session_open = bool(tgw.Login(cfg, api_mode))
        login = {
            "success": session_open,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "max_limitation_observed": bool(log_spi.max_limitation),
            "force_logout": bool(cfg.force_logout),
        }
        write_json(OUTPUT / "login.json", login)
        if not session_open:
            manifest["status"] = "LOGIN_FAILED"
            return 1

        base = ad.BaseData()
        calendars = {}

        def calendar(market):
            if market not in calendars:
                value = None

                def call():
                    nonlocal value
                    value = base.get_calendar(data_type="str", market=market, date=20261019)
                    dates = [int(x) for x in value]
                    return {
                        "rows": len(dates), "unique_rows": len(set(dates)),
                        "first": min(dates) if dates else None, "last": max(dates) if dates else None,
                        "sample_head": dates[:5], "sample_tail": dates[-5:],
                        "contains_20240417": 20240417 in dates,
                        "contains_20261017": 20261017 in dates,
                    }

                record_case(f"calendar_{market}", {"market": market, "date": 20261019}, call)
                calendars[market] = [int(x) for x in value]
            return calendars[market]

        # 所有分钟查询都依赖显式交易日列表；空 date_list 会造成高层接口根本不发请求。
        calendar("SH")
        market_data = ad.MarketData(calendar("SH"))

        if "minimal" in groups:
            record_case(
                "min1_recent_000001_20261016",
                {"codes": ["000001.SZ"], "begin_date": 20261016, "end_date": 20261016, "period": "min1"},
                lambda: dict_frames_payload(market_data.query_kline(
                    ["000001.SZ"], begin_date=20261016, end_date=20261016,
                    period=ad.constant.Period.min1.value,
                )),
                fatal_on_exception=True,
            )

        if "base" in groups:
            calendar("SZ")
            calendar("BJ")
            record_case(
                "calendar_overlap",
                {"markets": ["SH", "SZ", "BJ"]},
                lambda: {
                    "sh_sz_only_in_sh": sorted(set(calendars["SH"]) - set(calendars["SZ"]))[-20:],
                    "sh_sz_only_in_sz": sorted(set(calendars["SZ"]) - set(calendars["SH"]))[-20:],
                    "sh_sz_equal": set(calendars["SH"]) == set(calendars["SZ"]),
                    "bj_starts": min(calendars["BJ"]) if calendars["BJ"] else None,
                    "bj_not_in_sh": sorted(set(calendars["BJ"]) - set(calendars["SH"]))[-20:],
                },
            )
            record_case(
                "code_list_current",
                {"security_type": "EXTRA_STOCK_A_SH_SZ"},
                lambda: code_list_payload(base.get_code_list(security_type="EXTRA_STOCK_A_SH_SZ")),
            )
            record_case(
                "code_info_current",
                {"security_type": "EXTRA_STOCK_A_SH_SZ"},
                lambda: code_info_payload(base.get_code_info(security_type="EXTRA_STOCK_A_SH_SZ")),
            )
            record_case(
                "stock_basic_selected",
                {"codes": ["000001.SZ", "600519.SH", "300104.SZ", "300819.SZ"]},
                lambda: frame_payload(ad.InfoData().get_stock_basic(
                    ["000001.SZ", "600519.SH", "300104.SZ", "300819.SZ"],
                )),
            )
            record_case(
                "hist_code_list_20240417",
                {"security_type": "EXTRA_STOCK_A_SH_SZ", "start_date": 20240417, "end_date": 20240417},
                lambda: code_list_payload(base.get_hist_code_list(
                    security_type="EXTRA_STOCK_A_SH_SZ", start_date=20240417, end_date=20240417,
                    local_path=str(work / "hist_codes") + "/",
                )),
            )

        if "kline" in groups:
            raw = lambda code, day, auto=1: query_raw_kline(
                code, day, ad.constant.Period.min1.value, auto_complete=auto, timeout=args.query_timeout,
            )
            record_case("min1_raw_000001_20240417_auto1", {"code": "000001.SZ", "date": 20240417, "auto_complete": 1}, lambda: raw("000001.SZ", 20240417), fatal_on_exception=True)
            record_case("min1_raw_000001_20240417_auto0", {"code": "000001.SZ", "date": 20240417, "auto_complete": 0}, lambda: raw("000001.SZ", 20240417, auto=0), fatal_on_exception=True)
            high = lambda codes, begin, end, period, **kwargs: dict_frames_payload(market_data.query_kline(
                codes, begin_date=begin, end_date=end, period=period, **kwargs,
            ))
            record_case("min1_batch_20240417", {"codes": ["000001.SZ", "600519.SH"], "date": 20240417}, lambda: high(["000001.SZ", "600519.SH"], 20240417, 20240417, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("min1_split_000001_20240417", {"codes": ["000001.SZ"], "date": 20240417}, lambda: high(["000001.SZ"], 20240417, 20240417, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("min1_split_600519_20240417", {"codes": ["600519.SH"], "date": 20240417}, lambda: high(["600519.SH"], 20240417, 20240417, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("min1_multiday_000001_20240416_17", {"codes": ["000001.SZ"], "begin_date": 20240416, "end_date": 20240417}, lambda: high(["000001.SZ"], 20240416, 20240417, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("min1_slice_000001_20240417_930_1000", {"codes": ["000001.SZ"], "date": 20240417, "begin_time": 930, "end_time": 1000}, lambda: high(["000001.SZ"], 20240417, 20240417, ad.constant.Period.min1.value, begin_time=930, end_time=1000), fatal_on_exception=True)
            record_case("min1_after_close_000001_20240417", {"codes": ["000001.SZ"], "date": 20240417, "begin_time": 1500, "end_time": 1700}, lambda: high(["000001.SZ"], 20240417, 20240417, ad.constant.Period.min1.value, begin_time=1500, end_time=1700), fatal_on_exception=True)
            record_case("day_batch_20240417", {"codes": ["000001.SZ", "600519.SH"], "date": 20240417, "period": "day"}, lambda: high(["000001.SZ", "600519.SH"], 20240417, 20240417, ad.constant.Period.day.value), fatal_on_exception=True)
            if "minimal" not in groups:
                record_case("min1_recent_000001_20261016", {"codes": ["000001.SZ"], "date": 20261016}, lambda: high(["000001.SZ"], 20261016, 20261016, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("day_recent_000001_20261016", {"codes": ["000001.SZ"], "date": 20261016, "period": "day"}, lambda: high(["000001.SZ"], 20261016, 20261016, ad.constant.Period.day.value), fatal_on_exception=True)
            record_case("min1_nontrade_000001_20261017", {"codes": ["000001.SZ"], "date": 20261017}, lambda: high(["000001.SZ"], 20261017, 20261017, ad.constant.Period.min1.value), fatal_on_exception=True)
            record_case("min1_early_000001_20130104", {"code": "000001.SZ", "date": 20130104}, lambda: raw("000001.SZ", 20130104), fatal_on_exception=True)
            record_case("min1_pre_coverage_000001_20121231", {"code": "000001.SZ", "date": 20121231}, lambda: raw("000001.SZ", 20121231), fatal_on_exception=True)
            record_case("min1_invalid_999999SH_20240417", {"code": "999999.SH", "date": 20240417}, lambda: raw("999999.SH", 20240417), fatal_on_exception=True)
            record_case("min1_full_suspend_300104_20170417_auto1", {"code": "300104.SZ", "date": 20170417, "auto_complete": 1}, lambda: raw("300104.SZ", 20170417), fatal_on_exception=True)
            record_case("min1_full_suspend_300104_20170417_auto0", {"code": "300104.SZ", "date": 20170417, "auto_complete": 0}, lambda: raw("300104.SZ", 20170417, auto=0), fatal_on_exception=True)
            record_case("min1_intraday_suspend_300819_20200312_auto1", {"code": "300819.SZ", "date": 20200312, "auto_complete": 1}, lambda: raw("300819.SZ", 20200312), fatal_on_exception=True)
            record_case("min1_intraday_suspend_300819_20200312_auto0", {"code": "300819.SZ", "date": 20200312, "auto_complete": 0}, lambda: raw("300819.SZ", 20200312, auto=0), fatal_on_exception=True)

        if "status" in groups:
            info = ad.InfoData()
            record_case(
                "history_status_20200311_13",
                {"codes": ["000001.SZ", "300104.SZ", "300819.SZ"], "begin_date": 20200311, "end_date": 20200313},
                lambda: dict_frames_payload(info.get_history_stock_status(
                    ["000001.SZ", "300104.SZ", "300819.SZ"], local_path=str(work / "status") + "/",
                    begin_date=20200311, end_date=20200313,
                )),
            )
            record_case(
                "history_status_20170417",
                {"codes": ["000001.SZ", "300104.SZ"], "begin_date": 20170417, "end_date": 20170417},
                lambda: dict_frames_payload(info.get_history_stock_status(
                    ["000001.SZ", "300104.SZ"], local_path=str(work / "status") + "/",
                    begin_date=20170417, end_date=20170417,
                )),
            )

        if "factors" in groups:
            record_case(
                "adj_factor_600519",
                {"codes": ["600519.SH"], "is_local": False},
                lambda: frame_payload(base.get_adj_factor(
                    ["600519.SH"], local_path=str(work / "adj") + "/", is_local=False,
                )),
            )
            record_case(
                "backward_factor_600519",
                {"codes": ["600519.SH"], "is_local": False},
                lambda: frame_payload(base.get_backward_factor(
                    ["600519.SH"], local_path=str(work / "backward") + "/", is_local=False,
                )),
            )

        if "index" in groups:
            info = ad.InfoData()
            record_case(
                "index_constituent_000300",
                {"codes": ["000300.SH"], "is_local": False},
                lambda: dict_frames_payload(info.get_index_constituent(
                    ["000300.SH"], local_path=str(work / "constituent") + "/", is_local=False,
                )),
            )
            record_case(
                "index_weight_000300_202609",
                {"codes": ["000300.SH"], "begin_date": 20260901, "end_date": 20260930},
                lambda: dict_frames_payload(info.get_index_weight(
                    ["000300.SH"], local_path=str(work / "weight") + "/",
                    begin_date=20260901, end_date=20260930,
                )),
            )

        if "recovery" in groups:
            record_case(
                "ex_factor_raw_600519",
                {"security_code": "600519", "interface": "tgw.QueryExFactorTable"},
                lambda: query_async_with_timeout(
                    lambda callback: tgw.QueryExFactorTable(
                        "600519", query_spi=callback, return_df_format=False,
                    ), args.query_timeout, "ex factor 600519",
                ),
                fatal_on_exception=True,
            )

            def raw_index_constituent():
                task_id = tgw.GetTaskID()
                # 功能号和参数名来自已安装 SDK 的 download_index_constituent 实现。
                param_status = [
                    tgw.SetThirdInfoParam(task_id, "function_id", "A010200002"),
                    tgw.SetThirdInfoParam(task_id, "index_code", "000300.SH"),
                ]
                payload = query_async_with_timeout(
                    lambda callback: tgw.QueryThirdInfo(
                        task_id, query_spi=callback, return_df_format=False,
                    ), args.query_timeout, "index constituent 000300.SH",
                )
                payload["task_id"] = task_id
                payload["set_param_status"] = param_status
                return payload

            record_case(
                "index_constituent_raw_000300",
                {"function_id": "A010200002", "index_code": "000300.SH"},
                raw_index_constituent,
                fatal_on_exception=True,
            )

            def weight(begin_date, end_date):
                # 日期参数组按官方文档直连服务器；每次新建 InfoData，避免实例旧结果污染对照。
                return dict_frames_payload(ad.InfoData().get_index_weight(
                    ["000300.SH"], local_path=str(work / "weight_recovery") + "/",
                    begin_date=begin_date, end_date=end_date,
                ))

            record_case(
                "index_weight_000300_20260907_single",
                {"codes": ["000300.SH"], "begin_date": 20260907, "end_date": 20260907},
                lambda: weight(20260907, 20260907),
            )
            record_case(
                "index_weight_000300_202609_repeat",
                {"codes": ["000300.SH"], "begin_date": 20260901, "end_date": 20260930},
                lambda: weight(20260901, 20260930),
            )
            record_case(
                "index_weight_000300_20260911_30",
                {"codes": ["000300.SH"], "begin_date": 20260911, "end_date": 20260930},
                lambda: weight(20260911, 20260930),
            )
            record_case(
                "index_weight_000300_20261012_16",
                {"codes": ["000300.SH"], "begin_date": 20261012, "end_date": 20261016},
                lambda: weight(20261012, 20261016),
            )

            def raw_weight(begin_date, end_date):
                task_id = tgw.GetTaskID()
                param_status = [
                    tgw.SetThirdInfoParam(task_id, "function_id", "A010200005"),
                    tgw.SetThirdInfoParam(task_id, "index_code", "000300.SH"),
                    tgw.SetThirdInfoParam(task_id, "start_date", begin_date),
                    tgw.SetThirdInfoParam(task_id, "end_date", end_date),
                ]
                payload = query_async_with_timeout(
                    lambda callback: tgw.QueryThirdInfo(
                        task_id, query_spi=callback, return_df_format=False,
                    ), args.query_timeout, f"index weight {begin_date}-{end_date}",
                )
                payload["task_id"] = task_id
                payload["set_param_status"] = param_status
                return payload

            for begin_date, end_date, label in (
                ("20260907", "20260907", "single"),
                ("20260901", "20260930", "range"),
                ("20260911", "20260930", "later_empty"),
                ("20261012", "20261016", "october_empty"),
            ):
                record_case(
                    f"index_weight_raw_000300_202609_{label}" if begin_date.startswith("202609")
                    else f"index_weight_raw_000300_202610_{label}",
                    {"function_id": "A010200005", "index_code": "000300.SH",
                     "start_date": begin_date, "end_date": end_date},
                    lambda begin_date=begin_date, end_date=end_date: raw_weight(begin_date, end_date),
                    fatal_on_exception=True,
                )

        if "extended" in groups:
            record_case(
                "min1_mixed_valid_invalid_20240417",
                {"codes": ["000001.SZ", "999999.SH"], "date": 20240417},
                lambda: dict_frames_payload(market_data.query_kline(
                    ["000001.SZ", "999999.SH"], begin_date=20240417, end_date=20240417,
                    period=ad.constant.Period.min1.value,
                )),
                fatal_on_exception=True,
            )
            record_case(
                "hist_code_list_20170417",
                {"security_type": "EXTRA_STOCK_A_SH_SZ", "start_date": 20170417, "end_date": 20170417},
                lambda: code_list_payload(ad.BaseData().get_hist_code_list(
                    security_type="EXTRA_STOCK_A_SH_SZ", start_date=20170417, end_date=20170417,
                    local_path=str(work / "hist_codes_2017") + "/",
                )),
            )
            bj_state = {"code": None}

            def bj_codes():
                codes = ad.BaseData().get_code_list(security_type="BJ_A")
                if not codes:
                    raise RuntimeError("BJ_A returned no codes")
                bj_state["code"] = sorted(set(codes))[0]
                return {**code_list_payload(codes), "selected_for_minute": bj_state["code"]}

            record_case("bj_code_list_current", {"security_type": "BJ_A"}, bj_codes)
            for period_name, period in (("min1", ad.constant.Period.min1.value), ("day", ad.constant.Period.day.value)):
                record_case(
                    f"bj_{period_name}_recent_20261016",
                    {"code_dynamic_from": "bj_code_list_current", "date": 20261016, "period": period_name},
                    lambda period=period: dict_frames_payload(market_data.query_kline(
                        [bj_state["code"]], begin_date=20261016, end_date=20261016, period=period,
                    )),
                    fatal_on_exception=True,
                )
            for auto_complete in (1, 0):
                record_case(
                    f"bj_min1_raw_recent_20261016_auto{auto_complete}",
                    {"code_dynamic_from": "bj_code_list_current", "date": 20261016,
                     "auto_complete": auto_complete},
                    lambda auto_complete=auto_complete: query_raw_kline(
                        bj_state["code"], 20261016, ad.constant.Period.min1.value,
                        auto_complete=auto_complete, timeout=args.query_timeout,
                    ),
                    fatal_on_exception=True,
                )
            record_case(
                "day_600519_ex_factor_20240618_19",
                {"codes": ["600519.SH"], "begin_date": 20240618, "end_date": 20240619, "period": "day"},
                lambda: dict_frames_payload(market_data.query_kline(
                    ["600519.SH"], begin_date=20240618, end_date=20240619,
                    period=ad.constant.Period.day.value,
                )),
                fatal_on_exception=True,
            )
            record_case(
                "history_status_600519_20240618_19",
                {"codes": ["600519.SH"], "begin_date": 20240618, "end_date": 20240619},
                lambda: dict_frames_payload(ad.InfoData().get_history_stock_status(
                    ["600519.SH"], local_path=str(work / "factor_status") + "/",
                    begin_date=20240618, end_date=20240619,
                )),
            )

        if "authority" in groups:
            authority_codes = (
                "000001.SZ", "600519.SH", "300819.SZ", "688981.SH",
                "300308.SZ", "688825.SH", "688808.SH", "000010.SZ",
                "600365.SH", "300555.SZ", "688184.SH", "002168.SZ",
            )
            for code in authority_codes:
                for auto_complete in (0, 1):
                    record_case(
                        f"authority_min1_{code.replace('.', '')}_auto{auto_complete}",
                        {"code": code, "date": 20260903, "auto_complete": auto_complete,
                         "reference": "20260903-1min.parquet"},
                        lambda code=code, auto_complete=auto_complete: query_raw_kline(
                            code, 20260903, ad.constant.Period.min1.value,
                            auto_complete=auto_complete, timeout=args.query_timeout,
                        ),
                        fatal_on_exception=True,
                    )
            record_case(
                "authority_day_batch_20260903",
                {"codes": list(authority_codes), "date": 20260903, "period": "day"},
                lambda: dict_frames_payload(market_data.query_kline(
                    list(authority_codes), begin_date=20260903, end_date=20260903,
                    period=ad.constant.Period.day.value,
                )),
                fatal_on_exception=True,
            )

        manifest["status"] = "ABORTED_AFTER_CASE_ERROR" if ABORT else "COMPLETED"
        return int(ABORT)
    except Exception as exc:
        manifest["status"] = "FATAL_ERROR"
        manifest["fatal_error"] = f"{type(exc).__name__}: {redact(exc)}"
        return 1
    finally:
        try:
            if ad is not None:
                tgw.Close()
                manifest["session_closed"] = True
        except Exception as exc:
            manifest["session_close_error"] = f"{type(exc).__name__}: {redact(exc)}"
        manifest["finished_at"] = timestamp()
        manifest["cases_recorded"] = len(SUMMARIES)
        write_json(OUTPUT / "run_manifest.json", manifest)
        os.chdir(original_cwd)
        # 工作目录只含本次隔离缓存；返回证据已转为脱敏 JSON，避免缓存被后续误当新响应。
        shutil.rmtree(work, ignore_errors=True)


def code_list_payload(codes):
    unique = set(codes)
    selected = ["000001.SZ", "600519.SH", "300104.SZ", "300819.SZ", "999999.SH"]
    return {
        "rows": len(codes), "unique_rows": len(unique),
        "membership": {code: code in unique for code in selected},
        "sorted_sample_head": sorted(unique)[:10], "sorted_sample_tail": sorted(unique)[-10:],
    }


def code_info_payload(df):
    selected = ["000001.SZ", "600519.SH", "300104.SZ", "300819.SZ"]
    present = [code for code in selected if code in df.index]
    return {
        "full_summary": {k: v for k, v in frame_payload(df, full=False).items() if k != "records"},
        "selected": frame_records(df.loc[present], full=True),
    }


if __name__ == "__main__":
    raise SystemExit(main())
