#!/usr/bin/env python3
"""ProMax 网关单请求探针（stock_basic / suspend_d 实测用）。

用法: python3 probe.py <证据名> <endpoint> [k=v ...]
- TUSHARE_API_KEY 仅从环境变量读取, 只用于 X-API-Key Header, 不打印不落盘。
- 原始响应体存 tmp/<证据名>.json (重试中的中间失败响应存 <证据名>_failN.json)。
- 每次 HTTP 尝试追加一行到 tmp/requests.jsonl (时间/端点/参数/状态/行数/耗时, 无 key)。
- 通过 tmp/.last_req_ts 强制相邻请求间隔 >=0.35s。
- 429: 读 Retry-After 退避重试 (最多3次); 503: 退避 1/2/4s 重试 (最多3次)。
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone

BASE = "https://pcd.mobcvb.cn/tushare/pro"
HERE = os.path.dirname(os.path.abspath(__file__))
TMP = os.path.join(HERE, "tmp")
STATE = os.path.join(TMP, ".last_req_ts")
LOG = os.path.join(TMP, "requests.jsonl")
MIN_GAP = 0.35


def now_iso():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="milliseconds")


def enforce_gap():
    try:
        with open(STATE) as f:
            last = float(f.read().strip())
    except Exception:
        last = 0.0
    wait = MIN_GAP - (time.time() - last)
    if wait > 0:
        time.sleep(wait)


def mark_req():
    with open(STATE, "w") as f:
        f.write(str(time.time()))


def row_count(status, body):
    try:
        j = json.loads(body)
    except Exception:
        return None, None
    code = j.get("code")
    data = j.get("data")
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        return code, len(data["items"])
    if isinstance(j.get("count"), int):
        return code, j["count"]
    return code, None


def one_attempt(url, key):
    t0 = time.time()
    status, headers, body = None, {}, b""
    try:
        req = urllib.request.Request(url, headers={"X-API-Key": key})
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            headers = dict(resp.headers)
            body = resp.read()
    except urllib.error.HTTPError as e:
        status = e.code
        headers = dict(e.headers or {})
        body = e.read() or b""
    except Exception as e:
        body = ("CLIENT_ERROR: " + repr(e)).encode("utf-8")
    elapsed_ms = round((time.time() - t0) * 1000, 1)
    return status, headers, body, elapsed_ms


def main():
    if len(sys.argv) < 3:
        sys.exit("usage: probe.py <name> <endpoint> [k=v ...]")
    name, endpoint = sys.argv[1], sys.argv[2]
    params = dict(item.split("=", 1) for item in sys.argv[3:])
    key = os.environ.get("TUSHARE_API_KEY")
    if not key:
        sys.exit("TUSHARE_API_KEY not set in environment")
    os.makedirs(TMP, exist_ok=True)
    url = f"{BASE}/{endpoint}?{urllib.parse.urlencode(params)}"

    retries_left = {429: 3, 503: 3}
    fail_n = 0
    while True:
        enforce_gap()
        status, headers, body, elapsed_ms = one_attempt(url, key)
        mark_req()
        code, rows = row_count(status, body)
        with open(LOG, "a") as f:
            f.write(json.dumps({
                "ts": now_iso(), "name": name, "endpoint": endpoint,
                "params": params, "http": status, "code": code, "rows": rows,
                "elapsed_ms": elapsed_ms,
                "x_cache": headers.get("X-Cache"),
                "retry_after": headers.get("Retry-After"),
                "rl_remain": headers.get("X-RateLimit-Remaining"),
                "rl_ip_remain": headers.get("X-RateLimit-IP-Remaining"),
            }, ensure_ascii=False) + "\n")

        if status == 429 and retries_left[429] > 0:
            retries_left[429] -= 1
            fail_n += 1
            with open(os.path.join(TMP, f"{name}_fail{fail_n}.json"), "wb") as f:
                f.write(body)
            wait = float(headers.get("Retry-After") or 5)
            time.sleep(max(wait, 1.0))
            continue
        if status == 503 and retries_left[503] > 0:
            retries_left[503] -= 1
            fail_n += 1
            with open(os.path.join(TMP, f"{name}_fail{fail_n}.json"), "wb") as f:
                f.write(body)
            time.sleep([1, 2, 4][2 - retries_left[503]])
            continue
        break

    with open(os.path.join(TMP, f"{name}.json"), "wb") as f:
        f.write(body)
    print(json.dumps({"name": name, "http": status, "code": code,
                      "rows": rows, "elapsed_ms": elapsed_ms,
                      "retries": fail_n}, ensure_ascii=False))


if __name__ == "__main__":
    main()
