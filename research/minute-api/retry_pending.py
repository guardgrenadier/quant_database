"""有限重试 minute_data_pending 的请求：每 60 秒一次，最多 12 轮；仅当 HTTP 200 或出现其他终态错误才停止。"""

import importlib.util
import json
import time
from datetime import datetime, timezone

spec = importlib.util.spec_from_file_location("exp", "research/minute-api/experiment.py")
exp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(exp)
exp.PHASE_BUDGET = 40  # 本脚本就是为重试而生，放宽 experiment 的单次运行预算

PENDING = {
    "p2_multi2_full": ("stk_mins", dict(
        ts_code="000001.SZ,600000.SH", freq="1min",
        start_date=exp.T0, end_date=exp.T1, limit=1000,
    )),
    "p5_sunday": ("stk_mins", dict(
        ts_code="000001.SZ", freq="1min",
        start_date="2026-09-20 09:30:00", end_date="2026-09-20 15:00:00", limit=500,
    )),
    "p5_prelisting": ("stk_mins", dict(
        ts_code="920176.BJ", freq="1min",
        start_date="2026-01-05 09:30:00", end_date="2026-01-05 15:00:00", limit=500,
    )),
}

for attempt in range(10):
    for step in list(PENDING):
        api, params = PENDING[step]
        status, payload, headers, elapsed = exp.http_get(f"{exp.BASE}/{api}", params)
        error = payload.get("error") if isinstance(payload, dict) else None
        rows = exp.rows_of(payload) if status == 200 and isinstance(payload, dict) and str(payload.get("code")) == "0" else []
        record = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "step": step, "api": api, "params": params, "http": status,
            "error": error,
            "code": payload.get("code") if isinstance(payload, dict) else None,
            "count": payload.get("count") if isinstance(payload, dict) else None,
            "elapsed_ms": round(elapsed * 1000), "rows": len(rows), **headers,
        }
        with exp.REQUEST_LOG.open("a") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        print(json.dumps(record, ensure_ascii=False))
        if status == 200:
            (exp.TMP / f"{step}.json").write_text(json.dumps(rows, ensure_ascii=False))
            del PENDING[step]
        elif status == 503 and error is None:
            print(step, "bare 503 (gateway outage), keep waiting")
        elif error != "minute_data_pending":
            print(step, "terminal non-pending state, stop retrying")
            del PENDING[step]
    if not PENDING:
        print("all resolved")
        break
    print(f"attempt {attempt} done, still pending: {list(PENDING)}")
    time.sleep(120)
else:
    print("max attempts reached, still pending:", list(PENDING))
