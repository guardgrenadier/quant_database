"""第二轮只读补查：沿用 scripts/probe_tushare_daily_index.py 的探针机制。"""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

from probe_tushare_daily_index import (DAILY_BASIC_FIELDS, ENDPOINT, INDEX_BASIC_FIELDS, ROOT,
                                       summarize)

HALT_RANGE = {"ts_code": "300104.SZ", "start_date": "20170417", "end_date": "20170428"}


def cases():
    """第二轮用例：重试瞬时失败、停牌语义、历史起点、截断分页、index_member_all 正确参数。"""
    return [
        ("retry_db_day_20261014", "daily_basic", {"trade_date": "20261014"}, DAILY_BASIC_FIELDS),
        ("retry_ib_market_SW", "index_basic", {"market": "SW"}, INDEX_BASIC_FIELDS),
        ("retry_id_future", "index_daily", {"trade_date": "20261020"}, ""),

        ("daily_halt_300104", "daily", HALT_RANGE, ""),
        ("adj_halt_300104", "adj_factor", HALT_RANGE, ""),
        ("db_halt_300104", "daily_basic", HALT_RANGE, ""),
        ("sl_halt_300104", "stk_limit", HALT_RANGE, ""),

        ("daily_ah", "daily", {"trade_date": "20261013"}, "ts_code,trade_date,close,ah_vol,ah_amount"),
        ("daily_multi_code", "daily", {"ts_code": "000001.SZ,000002.SZ", "trade_date": "20261013"}, ""),

        ("sl_hist_20190102", "stk_limit", {"trade_date": "20190102"}, ""),
        ("sl_hist_20150601", "stk_limit", {"trade_date": "20150601"}, ""),
        ("sl_hist_20100104", "stk_limit", {"trade_date": "20100104"}, ""),
        ("sl_extra_fields", "stk_limit", {"trade_date": "20261013"},
         "ts_code,trade_date,pre_close,up_limit,down_limit,asset_type,exchange"),
        ("db_limit_status", "daily_basic", {"trade_date": "20261013"}, "ts_code,trade_date,close,limit_status"),

        ("iw_sep_000300", "index_weight", {"index_code": "000300.SH", "start_date": "20260901", "end_date": "20260930"}, ""),
        ("iw_aug_000300", "index_weight", {"index_code": "000300.SH", "start_date": "20260801", "end_date": "20260831"}, ""),
        ("iw_launch_000300", "index_weight", {"index_code": "000300.SH", "start_date": "20050401", "end_date": "20050430"}, ""),
        ("iw_may2005_000300", "index_weight", {"index_code": "000300.SH", "start_date": "20050501", "end_date": "20050531"}, ""),
        ("iw_day_page0", "index_weight", {"trade_date": "20260930", "limit": 3000, "offset": 0}, ""),
        ("iw_day_page1", "index_weight", {"trade_date": "20260930", "limit": 3000, "offset": 3000}, ""),
        ("iw_day_page2", "index_weight", {"trade_date": "20260930", "limit": 3000, "offset": 6000}, ""),
        ("iw_day_page3", "index_weight", {"trade_date": "20260930", "limit": 3000, "offset": 9000}, ""),

        ("id_day_page0", "index_daily", {"trade_date": "20261013", "limit": 8000, "offset": 0}, ""),
        ("id_day_page1", "index_daily", {"trade_date": "20261013", "limit": 8000, "offset": 8000}, ""),
        ("id_day_page2", "index_daily", {"trade_date": "20261013", "limit": 8000, "offset": 16000}, ""),

        ("ima_noargs", "index_member_all", {}, ""),
        ("ima_l1", "index_member_all", {"l1_code": "801010.SI"}, ""),
        ("ima_ts", "index_member_all", {"ts_code": "000001.SZ"}, ""),
        ("ima_l3", "index_member_all", {"l3_code": "850531.SI"}, ""),
        ("ima_hist_600519", "index_member_all", {"ts_code": "600519.SH", "is_new": "N"}, ""),
        ("ima_all_not_new", "index_member_all", {"is_new": "N"}, ""),
        ("ima_page", "index_member_all", {"l1_code": "801010.SI", "limit": 5, "offset": 0}, ""),
        ("ima_page_next", "index_member_all", {"l1_code": "801010.SI", "limit": 5, "offset": 5}, ""),

        ("sw_day_20261014", "sw_daily", {"trade_date": "20261014"}, ""),
        ("sw_early", "sw_daily", {"ts_code": "801010.SI", "start_date": "20000101", "end_date": "20120831"}, ""),
    ]


def main():
    """与第一轮相同的串行探查入口，只替换用例来源。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append", help="Run selected case names; omit for all")
    parser.add_argument("--output", type=Path, help="New local evidence directory")
    args = parser.parse_args()
    selected = [c for c in cases() if not args.case or c[0] in args.case]
    if args.case and set(args.case) - {c[0] for c in selected}:
        parser.error("unknown case name")
    token = (dotenv_values(ROOT / ".env").get("TUSHARE_TOKEN")
             or os.environ.get("TUSHARE_TOKEN", "")).strip()
    if not token:
        parser.error("TUSHARE_TOKEN is required in .env or environment")
    output = args.output or ROOT / "data/probes" / datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%f")
    output.mkdir(parents=True, exist_ok=False)
    summaries = []
    for index, (label, api, params, fields) in enumerate(selected):
        if index:
            time.sleep(1.3)
        evidence = {"case": label, "endpoint": ENDPOINT, "api_name": api, "params": params,
                    "requested_fields": fields, "requested_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()}
        request = Request(ENDPOINT, data=json.dumps({"api_name": api, "token": token,
                          "params": params, "fields": fields}).encode(),
                          headers={"Content-Type": "application/json"}, method="POST")
        start = time.monotonic()
        try:
            with urlopen(request, timeout=40) as response:
                evidence["http_status"] = response.status
                evidence["response"] = json.loads(response.read())
            summary = summarize(evidence["response"])
        except (HTTPError, URLError, TimeoutError, ValueError, OSError) as exc:
            evidence["error"] = f"{type(exc).__name__}: {exc}".replace(token, "<redacted>")
            summary = {"error": evidence["error"]}
        evidence["elapsed_seconds"] = round(time.monotonic() - start, 3)
        serialized = json.dumps(evidence, ensure_ascii=False, indent=2).replace(token, "<redacted>")
        (output / f"{label}.json").write_text(serialized + "\n")
        summary = {**{k: v for k, v in evidence.items() if k != "response"}, **summary}
        summaries.append(summary)
        (output / "summary.json").write_text(json.dumps(summaries, ensure_ascii=False, indent=2).replace(token, "<redacted>") + "\n")
        print(label, json.dumps({k: summary[k] for k in ("rows", "code", "msg", "error", "protocol_error") if k in summary}, ensure_ascii=False), flush=True)
    print(f"Evidence: {output}")
    return int(any(s.get("code") != 0 or "protocol_error" in s for s in summaries))


if __name__ == "__main__":
    raise SystemExit(main())
