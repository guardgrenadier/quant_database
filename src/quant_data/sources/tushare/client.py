"""官方协议 Client：处理通信、重试与脱敏，不理解 Dataset 或 Unit。"""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import json
import math
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from quant_data.core.models import RequestAttempt

ENDPOINT = "https://api.tushare.pro"


def validate_envelope(body):
    """验证通用信封与行列宽度，保留完整交付标记。"""
    # 协议检查：code 必须为整数且为 0；HTTP 成功不等于供应商交付成功。
    # type(... ) is int 同时排除 Python 中属于 int 子类的 bool。
    if not isinstance(body, dict) or type(body.get("code")) is not int:
        raise ValueError("Invalid Tushare response envelope")
    if body["code"] != 0:
        raise RuntimeError(f"Tushare code={body['code']}: {body.get('msg')}")
    data = body.get("data")
    # 协议检查：成功信封必须含 data 对象，不能把缺失数据当作完整空交付。
    if not isinstance(data, dict):
        raise ValueError("Missing Tushare data object")
    fields, items = data.get("fields"), data.get("items")
    # 协议检查：字段名唯一、每行宽度一致、单元格非嵌套对象；具体字段类型由 Adapter 验证。
    if (not isinstance(fields, list) or not isinstance(items, list)
            or any(not isinstance(f, str) for f in fields)
            or len(fields) != len(set(fields))
            or any(not isinstance(row, list) or len(row) != len(fields) for row in items)
            or any(isinstance(cell, (list, dict)) for row in items for cell in row)):
        raise ValueError("Invalid Tushare fields/items shape")
    # 协议检查：提供 has_more 时只能是布尔值；是否必须提供以及如何判截断交给 Adapter。
    if "has_more" in data and type(data["has_more"]) is not bool:
        raise ValueError("Invalid Tushare has_more marker")


def retry_delay(header, attempt):
    """尊重 Retry-After，并限制每次等待在 60 秒内；非法值明确失败。"""
    # 无供应商等待指示时按尝试序号退避；有指示时兼容秒数和 HTTP 日期两种协议形式。
    if header is None:
        return float(attempt)
    try:
        delay = float(header)
    except ValueError:
        delay = (parsedate_to_datetime(header) - datetime.now(timezone.utc)).total_seconds()
    # 通信约束：拒绝非有限等待和超长等待；过去的 HTTP 日期按零等待处理。
    if not math.isfinite(delay) or delay > 60:
        raise RuntimeError("Retry-After exceeds the bounded 60-second retry wait or is invalid")
    return max(0.0, delay)


class TushareClient:
    """持有私有凭据和请求配置，每次尝试通过回调交付脱敏控制信息及原始证据。"""

    def __init__(self, token, *, timeout=40, attempts=3):
        """初始化有限请求配置，实例表示不包含 token。"""
        # 配置边界：必须有凭据、正超时和至少一次尝试，避免配置错误伪装成空结果。
        if not token or attempts < 1 or timeout <= 0:
            raise ValueError("Token, positive timeout and attempt count are required")
        self._token = token
        self.timeout = timeout
        self.attempts = attempts

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        """返回完整供应商信封；失败记录尝试后抛出脱敏异常，只有指定通信错误重试。"""
        for attempt in range(1, self.attempts + 1):
            # 每次实际 HTTP 尝试独立计时；逻辑 request_id 不变，attempt 区分重试。
            requested_at = datetime.now(timezone.utc)
            started = time.monotonic()
            retry, header, error, body, raw, status = False, None, None, None, None, None
            request = Request(ENDPOINT, data=json.dumps({
                "api_name": api_name, "token": self._token, "params": params, "fields": ",".join(fields)
            }).encode(), headers={"Content-Type": "application/json"}, method="POST")
            try:
                with urlopen(request, timeout=self.timeout) as response:
                    status = response.status
                    raw = response.read().decode("utf-8")
                body = json.loads(raw)
                validate_envelope(body)
            except HTTPError as exc:
                status = exc.code
                raw = exc.read().decode("utf-8", errors="replace")
                error = f"HTTP {exc.code} from official Tushare"
                # 仅限流和服务端错误可重试；其他 HTTP 错误按失败返回，避免掩盖错误请求。
                retry = exc.code == 429 or 500 <= exc.code < 600
                header = exc.headers.get("Retry-After") if exc.headers else None
            except (TimeoutError, URLError) as exc:
                error = f"{type(exc).__name__}: {exc}"
                # 网络错误只重试超时；其他 URL/连接错误不进行无差别重试。
                retry = isinstance(exc, TimeoutError) or isinstance(getattr(exc, "reason", None), TimeoutError)
            except (ValueError, RuntimeError, OSError) as exc:
                # JSON、信封或供应商状态错误直接失败，不重试成另一份看似成功的交付。
                error = f"{type(exc).__name__}: {exc}"
            # 将有效的协议控制字段提取到尝试审计；无效信封允许部分字段缺失，保留错误证据。
            error = error.replace(self._token, "<redacted>") if error else None
            data = body.get("data") if isinstance(body, dict) else None
            valid_data = data if isinstance(data, dict) else {}
            returned = valid_data.get("fields")
            returned = tuple(returned) if isinstance(returned, list) and all(isinstance(f, str) for f in returned) else ()
            record = RequestAttempt(
                request_id, attempt, api_name.replace(self._token, "<redacted>"),
                json.loads(json.dumps(params, ensure_ascii=False).replace(self._token, "<redacted>")),
                tuple(f.replace(self._token, "<redacted>") for f in fields), requested_at,
                round(time.monotonic() - started, 3), status,
                body.get("code") if isinstance(body, dict) and type(body.get("code")) is int else None,
                tuple(f.replace(self._token, "<redacted>") for f in returned),
                len(valid_data["items"]) if isinstance(valid_data.get("items"), list) else None,
                valid_data.get("has_more") if type(valid_data.get("has_more")) is bool else None, error,
            )
            # 不将鉴权请求体交给回调；供应商错误消息或响应回显的凭据同样去除。
            safe_payload = json.loads(json.dumps(body if body is not None else raw, ensure_ascii=False).replace(self._token, "<redacted>"))
            on_attempt(record, safe_payload)
            # 证据回调完成后才返回/重试；每次失败尝试都有独立证据，回调异常不静默吞掉。
            if error is None:
                return body
            if not retry or attempt == self.attempts:
                raise RuntimeError(error) from None
            try:
                delay = retry_delay(header, attempt)
            except (ValueError, TypeError, OverflowError, RuntimeError) as exc:
                raise RuntimeError(f"Cannot respect Retry-After: {str(exc).replace(self._token, '<redacted>')}") from None
            time.sleep(delay)
