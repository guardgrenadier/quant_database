"""银河 worker 内的 SDK 调用；此模块只在独立进程中登录。"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone
import math
import os
from pathlib import Path
import re
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

from quant_data.core.models import RequestAttempt
from quant_data.sources.galaxy.client import GalaxyResponse, _api_name, _request_params, _response_data


PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)
SUFFIX_MARKET = {"SH": "kSSE", "SZ": "kSZSE", "BJ": "kNEEQ"}
CIRCUIT_CODES = {-82, -78, -91}


class _SessionFailure(RuntimeError):
    pass


def _load_sdk():
    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    return ad, tgw, set_cfg


def _credentials():
    values = dotenv_values(Path.cwd() / ".env")
    username = (os.environ.get("AD_USERNAME") or values.get("AD_USERNAME") or "").strip()
    password = (os.environ.get("AD_PASSWORD") or values.get("AD_PASSWORD") or "").strip()
    host = (os.environ.get("AD_HOST") or values.get("AD_HOST") or "101.230.159.235").strip()
    port = int(os.environ.get("AD_PORT") or values.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required")
    return username, password, host, port


def run_worker(requests, artifacts, options, sdk_factory):
    """每个进程至多登录一次；所有异常均让父进程按会话故障处理。"""
    for name in PROXY_NAMES:
        # 代理/TUN 下银河登录不可靠；仅清理 worker 自身环境，不修改父进程。
        os.environ.pop(name, None)
    try:
        ad, tgw, set_cfg = (sdk_factory or _load_sdk)()
        username, password, host, port = _credentials()
        cfg, api_mode, _ = set_cfg(username, password, host, port, force_logout=False)
        if not tgw.Login(cfg, api_mode):
            raise _SessionFailure("Galaxy login failed")
        calendar_date = max(request.end_date for request in requests)
        calendar = [int(value) for value in ad.BaseData().get_calendar(
            data_type="str", market="SH", date=calendar_date,
        )]
        if not calendar:
            raise _SessionFailure("Galaxy calendar prewarm returned no dates")
        market_data = ad.MarketData(calendar)
        try:
            _execute_requests(
                requests, artifacts, options, ad, tgw, market_data, (username, password),
            )
        finally:
            tgw.Close()
    except BaseException as exc:
        if isinstance(exc, (KeyboardInterrupt, SystemExit)) and not isinstance(exc, _SessionFailure):
            raise
        # 不在控制输出中打印 SDK 异常，避免鉴权细节泄漏；具体请求错误已脱敏留证。
        raise SystemExit(2) from None


def _execute_requests(requests, artifacts, options, ad, tgw, market_data, secrets):
    """在同一已预热会话内分批并发，遇到熔断或会话故障立即停止。

    已进入 SDK 的请求先形成证据；未调度请求在熔断后也写 INCOMPLETE。快照
    超时意味着会话锁可能未释放，必须交给父进程销毁并更换整个 worker。
    """
    pending = list(enumerate(requests))
    while pending:
        batch, pending = pending[:options["parallel"]], pending[options["parallel"]:]
        responses = {}
        fatal = False
        with ThreadPoolExecutor(max_workers=options["parallel"]) as executor:
            futures = {
                executor.submit(
                    _execute_one, request, options["attempts"][request.request_id], artifacts,
                    ad, tgw, market_data, options["query_timeout"], secrets,
                ): index for index, request in batch
            }
            for future in as_completed(futures):
                response, request_fatal = future.result()
                responses[futures[future]] = response
                fatal = fatal or request_fatal
        circuit = next((response.provider_code for response in responses.values()
                        if response.provider_code in CIRCUIT_CODES), None)
        if circuit is not None:
            # 熔断后尚未进入 SDK 的请求也明确留证，不能静默消失或留待下个 worker 再试探。
            for _, request in pending:
                _record_unscheduled(request, options["attempts"][request.request_id], artifacts, circuit)
            return
        if fatal:
            raise _SessionFailure("Galaxy session must be replaced after snapshot timeout")


def _execute_one(request, attempt_number, artifacts, ad, tgw, market_data, timeout, secrets):
    """执行一个逻辑请求并先落证据，再向调度器交付响应。

    记录请求时间、耗时、源状态码和原始行。快照仅在明确 -76 时形成历史不可得
    的 COMPLETE 降级；其他失败保留脱敏错误，绝不把空响应当作合法零成交。
    """
    requested_at = datetime.now(timezone.utc)
    started = time.monotonic()
    fatal = False
    try:
        if request.kind == "kline":
            rows, provider_code = _query_kline(ad, tgw, request, timeout)
        else:
            rows, provider_code = _query_snapshot(market_data, request, timeout)
        response = GalaxyResponse(request, "COMPLETE", rows, None, provider_code)
    except Exception as exc:
        fatal = isinstance(exc, _SessionFailure)
        provider_code = _exception_provider_code(exc)
        if request.kind == "snapshot" and provider_code == -76:
            # 仅底层明确的历史不可得允许分钟 Adapter 降级；普通失败不能伪装成该状态。
            response = GalaxyResponse(request, "COMPLETE", (), None, provider_code)
        else:
            state = "INCOMPLETE" if isinstance(exc, _IncompleteSnapshot) else "FAILED"
            rows = exc.rows if isinstance(exc, _IncompleteSnapshot) else ()
            response = GalaxyResponse(request, state, rows, _safe_error(exc, secrets), provider_code)
    elapsed = round(time.monotonic() - started, 3)
    returned_fields = tuple(sorted({key for row in response.rows for key in row}))
    attempt = RequestAttempt(
        request.request_id, attempt_number, _api_name(request.kind), _request_params(request), (),
        requested_at, elapsed, None, response.provider_code, returned_fields,
        len(response.rows), None, response.error,
    )
    # 只有证据原子写完才向调度器交付响应；写盘异常直接升级为 worker 故障。
    artifacts.record_attempt(attempt, _response_data(response))
    return response, fatal


def _query_kline(ad, tgw, request, timeout):
    """以底层异步回调获取单股 min1 原始整数行。

    固定 auto_complete=0、cq_flag=0 并显式填写所有 SWIG 字段，防止未初始化值。
    成功回调的 None 状态按 0 处理；-76 返回明确空交付，其他非零码原样失败。
    """
    code = request.codes[0]
    suffix = code.rsplit(".", 1)[1]
    if suffix not in SUFFIX_MARKET:
        raise ValueError(f"Unsupported Galaxy code suffix: {suffix}")
    sdk_request = tgw.ReqKline()
    # SWIG 新对象字段可能含未初始化值；请求字段逐项显式赋值。
    sdk_request.security_code = code.split(".", 1)[0]
    sdk_request.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[suffix])
    sdk_request.cq_flag = 0
    sdk_request.cq_date = 0
    sdk_request.qj_flag = 0
    sdk_request.cyc_type = ad.constant.Period.min1.value
    sdk_request.cyc_def = 0
    sdk_request.auto_complete = 0
    sdk_request.begin_date = request.start_date
    sdk_request.end_date = request.end_date
    sdk_request.begin_time = request.begin_time
    sdk_request.end_time = request.end_time
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"], state["status"] = result, status
        finished.set()

    accepted, immediate_error = tgw.QueryKline(sdk_request, callback, return_df_format=False)
    if accepted is not True:
        raise _ProviderError("Galaxy kline request was not accepted", _provider_code(immediate_error))
    if not finished.wait(timeout):
        raise TimeoutError(f"Galaxy kline callback exceeded {timeout}s")
    status = state.get("status")
    # 真实 SDK 成功回调可返回 None；既有探针同样按成功码 0 解释。
    if status is None:
        status = 0
    if type(status) is not int:
        raise ValueError("Galaxy kline callback returned no valid provider status")
    raw_rows = state.get("result") or []
    if not isinstance(raw_rows, (list, tuple)) or any(not isinstance(row, dict) for row in raw_rows):
        raise ValueError("Galaxy kline response is not a sequence of raw objects")
    rows = tuple(_raw_row(row, code) for row in raw_rows)
    # kDataEmpty 是可由上层结合日线判别的明确空交付；其他非零状态才是请求失败。
    if status == -76:
        if rows:
            raise ValueError("Galaxy kDataEmpty response unexpectedly contained rows")
        return (), status
    if status != 0:
        raise _ProviderError(f"Galaxy kline provider status {status}", status)
    return rows, status


def _query_snapshot(market_data, request, timeout):
    """批量查询窄窗快照，并为无回调的同步接口增加显式超时。

    超时后不能继续复用可能持锁的会话，因此抛出会话级故障；普通异常保留
    给 _execute_one 按供应商码分类，最终行集合由 _snapshot_rows 校验完整性。
    """
    result, failure = {}, []

    def call():
        try:
            result["value"] = market_data.query_snapshot(
                list(request.codes), request.start_date, request.end_date,
                begin_time=request.begin_time, end_time=request.end_time,
            )
        except BaseException as exc:
            failure.append(exc)

    # 高层快照接口没有回调超时；超时后该会话不可继续使用，daemon 调用随 worker 销毁。
    thread = threading.Thread(target=call, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise _SessionFailure(f"Galaxy snapshot query exceeded {timeout}s")
    if failure:
        raise failure[0]
    rows = _snapshot_rows(result.get("value"), request)
    return rows, 0


def _snapshot_rows(result, request):
    """展开 date→code→表结构响应，并验证请求覆盖完整性。

    返回代码必须属于请求集合，日期不得越界。代码已交付但表为空是合法零竞价；
    整个 code/date 缺席则保留部分 Raw 并标为 INCOMPLETE，不能混淆两者。
    """
    if not isinstance(result, dict):
        raise ValueError("Galaxy snapshot response is not a date mapping")
    expected_dates = {request.start_date} if request.start_date == request.end_date else set()
    seen_dates, seen_codes, rows = set(), set(), []
    for date_key, code_map in result.items():
        date_number = _date_number(date_key)
        if not request.start_date <= date_number <= request.end_date:
            raise ValueError("Galaxy snapshot returned a date outside the request")
        seen_dates.add(date_number)
        if not isinstance(code_map, dict):
            raise ValueError("Galaxy snapshot date entry is not a code mapping")
        for code, frame in code_map.items():
            if code not in request.codes:
                raise ValueError(f"Galaxy snapshot returned unexpected ts_code: {code}")
            seen_codes.add(code)
            for row in _frame_records(frame):
                rows.append(_raw_row(row, code))
    missing_codes = sorted(set(request.codes) - seen_codes)
    missing_dates = sorted(expected_dates - seen_dates)
    if missing_codes or missing_dates:
        # 空 DataFrame 是合法零成交；整个 code/date 缺席则不是完整批量交付。
        raise _IncompleteSnapshot(tuple(rows), missing_codes, missing_dates)
    return tuple(rows)


class _IncompleteSnapshot(RuntimeError):
    def __init__(self, rows, codes, dates):
        super().__init__(f"Galaxy snapshot incomplete; missing codes={codes}, dates={dates}")
        self.rows = rows


def _frame_records(frame):
    """将 DataFrame 或记录序列严格转为 dict 行，拒绝 None 和非对象行。"""
    if frame is None:
        raise ValueError("Galaxy snapshot code entry is None")
    if isinstance(frame, (list, tuple)):
        records = list(frame)
    elif hasattr(frame, "to_dict"):
        try:
            records = frame.to_dict(orient="records")
        except TypeError:
            records = frame.to_dict()
    else:
        raise ValueError("Galaxy snapshot code entry is not tabular")
    if not isinstance(records, list) or any(not isinstance(row, dict) for row in records):
        raise ValueError("Galaxy snapshot rows are not objects")
    return records


def _date_number(value):
    if type(value) is int:
        return value
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d"))
    if isinstance(value, date):
        return int(value.strftime("%Y%m%d"))
    if isinstance(value, str):
        digits = value.replace("-", "")
        if len(digits) == 8 and digits.isdigit():
            return int(digits)
    raise ValueError(f"Invalid Galaxy snapshot date key: {value!r}")


def _raw_row(row, code):
    """保留原始字段并补充完整时代 ts_code，确保跨市场 Unit 归属唯一。"""
    if "ts_code" in row and row["ts_code"] != code:
        raise ValueError("Galaxy row ts_code does not match its request code")
    converted = {str(key): _json_value(value) for key, value in row.items()}
    # 裸 security_code 无法跨市场唯一归属，统一补充完整时代 ts_code，不改其他原始字段。
    converted["ts_code"] = code
    return converted


def _json_value(value):
    """业务日期用 ISO 日期；时间戳统一按市场本地解释并输出 UTC ISO-8601。"""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("Galaxy row contains a non-finite number")
        return value
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    item = getattr(value, "item", None)
    if callable(item):
        converted = item()
        if converted is not value:
            return _json_value(converted)
    raise TypeError(f"Unsupported Galaxy row value: {type(value).__name__}")


class _ProviderError(RuntimeError):
    def __init__(self, message, provider_code):
        super().__init__(message)
        self.provider_code = provider_code


def _provider_code(value):
    return value if type(value) is int else None


def _exception_provider_code(exc):
    direct = _provider_code(getattr(exc, "provider_code", None))
    if direct is not None:
        return direct
    for name in ("status", "code"):
        value = _provider_code(getattr(exc, name, None))
        if value is not None:
            return value
    match = re.search(r"(?<!\d)(-82|-78|-91|-76)(?!\d)", str(exc))
    return int(match.group(1)) if match else None


def _safe_error(exc, secrets):
    text = f"{type(exc).__name__}: {exc}"
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def _record_unscheduled(request, attempt_number, artifacts, circuit):
    """给熔断后未进入 SDK 的请求补写 INCOMPLETE 证据，保持影响范围可追踪。"""
    error = f"Not scheduled after Galaxy circuit breaker {circuit}"
    response = GalaxyResponse(request, "INCOMPLETE", (), error, None)
    attempt = RequestAttempt(
        request.request_id, attempt_number, _api_name(request.kind), _request_params(request), (),
        datetime.now(timezone.utc), 0.0, None, None, (), 0, None, error,
    )
    artifacts.record_attempt(attempt, _response_data(response))
