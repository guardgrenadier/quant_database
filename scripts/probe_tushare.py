"""只读探查官方接口；证据保存在本地，用于研究，不执行权威数据发布。"""

import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "https://api.tushare.pro"
STOCK_FIELDS = (
    "ts_code,symbol,name,area,industry,fullname,enname,cnspell,market,exchange,"
    "curr_type,list_status,list_date,delist_date,is_hs,act_name,act_ent_type"
)
SUSPEND_FIELDS = "ts_code,trade_date,suspend_timing,suspend_type"


def cases():
    """返回本轮接口探查用例：名称、接口、参数和显式字段列表。

    固定日期与尾页 offset 属于 2026-10-13 的研究场景，不作为正式获取规则。
    """
    result = [("stock_default", "stock_basic", {}, "")]
    for status in ("L", "D", "P", "G", "UN"):
        result.append((f"stock_{status}", "stock_basic", {"list_status": status}, STOCK_FIELDS))
    for exchange in ("SSE", "SZSE", "BSE"):
        result.append((f"stock_L_{exchange}", "stock_basic", {"list_status": "L", "exchange": exchange}, STOCK_FIELDS))
        for status in ("D", "P", "G", "UN"):
            result.append((f"stock_{status}_{exchange}", "stock_basic", {"list_status": status, "exchange": exchange}, STOCK_FIELDS))
    result += [
        ("stock_empty_status", "stock_basic", {"list_status": ""}, STOCK_FIELDS),
        ("stock_L_page", "stock_basic", {"list_status": "L", "limit": 2, "offset": 0}, STOCK_FIELDS),
        ("stock_L_page_next", "stock_basic", {"list_status": "L", "limit": 2, "offset": 2}, STOCK_FIELDS),
        ("stock_missing", "stock_basic", {"ts_code": "999999.SH"}, STOCK_FIELDS),
        ("stock_L_last_page", "stock_basic", {"list_status": "L", "limit": 2, "offset": 5572}, STOCK_FIELDS),
        ("stock_L_after_end", "stock_basic", {"list_status": "L", "limit": 2, "offset": 5574}, STOCK_FIELDS),
    ]
    for date in ("20000103", "20100104", "20150708", "20200312", "20260903", "20261012", "20261013", "20261011"):
        result.append((f"suspend_{date}", "suspend_d", {"trade_date": date}, SUSPEND_FIELDS))
    result += [
        ("suspend_20200312_S", "suspend_d", {"trade_date": "20200312", "suspend_type": "S"}, SUSPEND_FIELDS),
        ("suspend_20200312_R", "suspend_d", {"trade_date": "20200312", "suspend_type": "R"}, SUSPEND_FIELDS),
        ("suspend_range", "suspend_d", {"start_date": "20200309", "end_date": "20200313"}, SUSPEND_FIELDS),
        ("suspend_intraday", "suspend_d", {"ts_code": "300819.SZ", "start_date": "20200301", "end_date": "20200331"}, SUSPEND_FIELDS),
        ("suspend_multi_code", "suspend_d", {"ts_code": "300819.SZ,300821.SZ", "trade_date": "20200312"}, SUSPEND_FIELDS),
        ("suspend_long_halt", "suspend_d", {"ts_code": "300104.SZ", "start_date": "20200309", "end_date": "20200313"}, SUSPEND_FIELDS),
        ("suspend_cap", "suspend_d", {"start_date": "20150101", "end_date": "20151231"}, SUSPEND_FIELDS),
        ("suspend_2020", "suspend_d", {"start_date": "20200101", "end_date": "20201231"}, SUSPEND_FIELDS),
        ("suspend_page", "suspend_d", {"start_date": "20150101", "end_date": "20151231", "limit": 2, "offset": 0}, SUSPEND_FIELDS),
        ("suspend_page_next", "suspend_d", {"start_date": "20150101", "end_date": "20151231", "limit": 2, "offset": 2}, SUSPEND_FIELDS),
        ("suspend_missing", "suspend_d", {"ts_code": "999999.SH", "trade_date": "20200312"}, SUSPEND_FIELDS),
        ("suspend_repeated", "suspend_d", {"ts_code": "000029.SZ", "start_date": "20200309", "end_date": "20200313"}, SUSPEND_FIELDS),
        ("suspend_2017_long_halt", "suspend_d", {"ts_code": "300104.SZ", "start_date": "20170401", "end_date": "20170430"}, SUSPEND_FIELDS),
        ("suspend_day_before", "suspend_d", {"trade_date": "20200311"}, SUSPEND_FIELDS),
        ("suspend_day_after", "suspend_d", {"trade_date": "20200313"}, SUSPEND_FIELDS),
        ("suspend_day_09", "suspend_d", {"trade_date": "20200309"}, SUSPEND_FIELDS),
        ("suspend_day_10", "suspend_d", {"trade_date": "20200310"}, SUSPEND_FIELDS),
        ("suspend_small_last", "suspend_d", {"trade_date": "20200312", "limit": 2, "offset": 22}, SUSPEND_FIELDS),
        ("suspend_small_end", "suspend_d", {"trade_date": "20200312", "limit": 2, "offset": 23}, SUSPEND_FIELDS),
    ]
    return result


def summarize(response):
    """将原始响应汇总为结构、类型、空值、范围和业务键重复情况。

    供应商错误与合法空结果分别表达；摘要保留 has_more/count 供交付研究。
    这些统计不构成正式 Canonical 发布验收。
    """
    # 先检查信封与行列形态，避免把权限错误或坏协议解释成正常零行。
    if not isinstance(response, dict):
        return {"protocol_error": "response must be an object"}
    data = response.get("data")
    if response.get("code") != 0 or not isinstance(data, dict):
        return {"code": response.get("code"), "msg": response.get("msg")}
    fields, items = data.get("fields"), data.get("items")
    if not isinstance(fields, list) or not isinstance(items, list):
        return {"protocol_error": "fields/items must be lists"}
    if (any(not isinstance(field, str) for field in fields)
            or len(set(fields)) != len(fields)
            or any(not isinstance(row, list) or len(row) != len(fields) for row in items)):
        return {"protocol_error": "duplicate fields or row width mismatch"}
    if any(isinstance(value, (list, dict)) for row in items for value in row):
        return {"protocol_error": "cells must be scalar values"}
    # 研究样本按列统计；空值合并 JSON null 与空字符串，保留实际类型信息。
    columns = {field: [row[i] for row in items] for i, field in enumerate(fields)}
    result = {"code": 0, "rows": len(items), "fields": fields, "data_keys": list(data),
              "has_more": data.get("has_more"), "count": data.get("count"),
              "nulls": {k: sum(v is None or v == "" for v in values) for k, values in columns.items()},
              "types": {k: sorted({type(v).__name__ for v in values}) for k, values in columns.items()},
              "sample": [dict(zip(fields, row, strict=True)) for row in items[:3]]}
    for field in ("list_status", "exchange", "market", "curr_type", "is_hs", "suspend_type", "suspend_timing"):
        if field in columns:
            result[field] = dict(Counter(str(v) for v in columns[field]).most_common(30))
    for field in ("trade_date", "list_date", "delist_date"):
        values = [v for v in columns.get(field, []) if v]
        result[f"{field}_range"] = [min(values), max(values)] if values else None
    keys = [("ts_code",)] if "list_status" in fields else [
        ("ts_code", "trade_date"), ("ts_code", "trade_date", "suspend_type"),
        ("ts_code", "trade_date", "suspend_type", "suspend_timing")]
    # 比较停复牌的不同候选键，为确定 Canonical 粒度提供实际证据。
    result["duplicate_keys"] = {}
    for key in keys:
        if all(k in columns for k in key):
            count = Counter(tuple(row[fields.index(k)] for k in key) for row in items)
            result["duplicate_keys"][",".join(key)] = sum(n - 1 for n in count.values())
    return result


def main():
    """读取凭据和用例选择，串行发起探查，保存原始证据及汇总结果。

    每次使用新目录，失败也保存已取得的信息；任一错误返回非零退出码。
    此入口不复用生产 Runner，也不修改 Canonical 或 Coverage。
    """
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
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    # 禁止复用已有输出目录，避免新探查覆盖此前响应证据。
    output = args.output or ROOT / "data/probes" / now.strftime("%Y%m%dT%H%M%S%f")
    output.mkdir(parents=True, exist_ok=False)
    summaries = []
    for index, (label, api, params, fields) in enumerate(selected):
        if index:
            time.sleep(1.3)  # 请求间隔使串行探查低于文档中的每分钟 50 次限制。
        # 证据只记录业务请求参数；带 token 的鉴权请求体单独构造。
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
        # 每个用例结束立即保存原始记录和累计摘要，便于排查部分执行的结果。
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
