"""只读探查官方行情/指数接口；证据保存在本地，用于研究，不执行权威数据发布。"""

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

ROOT = Path(__file__).resolve().parents[1]
ENDPOINT = "https://api.tushare.pro"

DAILY_FIELDS = "ts_code,trade_date,open,high,low,close,pre_close,change,pct_chg,vol,amount"
ADJ_FIELDS = "ts_code,trade_date,adj_factor"
DAILY_BASIC_FIELDS = ("ts_code,trade_date,close,turnover_rate,turnover_rate_f,volume_ratio,pe,pe_ttm,pb,"
                      "ps,ps_ttm,dv_ratio,dv_ttm,total_share,float_share,free_share,total_mv,circ_mv")
STK_LIMIT_FIELDS = "ts_code,trade_date,up_limit,down_limit"
INDEX_BASIC_FIELDS = ("ts_code,name,fullname,market,publisher,index_type,category,base_date,base_point,"
                      "list_date,weight_rule,desc,exp_date")
INDEX_DAILY_FIELDS = "ts_code,trade_date,close,open,high,low,pre_close,change,pct_chg,vol,amount"
INDEX_WEIGHT_FIELDS = "index_code,con_code,trade_date,weight"
INDEX_MEMBER_ALL_FIELDS = ("l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,con_code,con_name,"
                           "in_date,out_date,is_new")
SW_DAILY_FIELDS = "ts_code,trade_date,name,open,high,low,close,change,pct_change,vol,amount,pe,pb,float_mv,total_mv"


def cases():
    """返回本轮接口探查用例：名称、接口、参数和显式字段列表。

    固定日期属于 2026-10-14 的研究场景（近期交易日 20261012-20261014），
    不作为正式获取规则。
    """
    result = [
        ("daily_default", "daily", {"ts_code": "000001.SZ", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("daily_day_20261013", "daily", {"trade_date": "20261013"}, DAILY_FIELDS),
        ("daily_day_20261014", "daily", {"trade_date": "20261014"}, DAILY_FIELDS),
        ("daily_long_range", "daily", {"ts_code": "000001.SZ", "start_date": "19910403", "end_date": "20261014"}, DAILY_FIELDS),
        ("daily_range_2015", "daily", {"ts_code": "000001.SZ", "start_date": "20150101", "end_date": "20151231"}, DAILY_FIELDS),
        ("daily_suspended", "daily", {"ts_code": "300819.SZ", "start_date": "20200311", "end_date": "20200313"}, DAILY_FIELDS),
        ("daily_future", "daily", {"trade_date": "20261020"}, DAILY_FIELDS),
        ("daily_nontrade", "daily", {"trade_date": "20261011"}, DAILY_FIELDS),
        ("daily_missing", "daily", {"ts_code": "999999.SH", "trade_date": "20261013"}, DAILY_FIELDS),
        ("daily_old", "daily", {"trade_date": "20000104"}, DAILY_FIELDS),
        ("daily_page", "daily", {"trade_date": "20261013", "limit": 10, "offset": 0}, DAILY_FIELDS),
        ("daily_page_next", "daily", {"trade_date": "20261013", "limit": 10, "offset": 10}, DAILY_FIELDS),

        ("adj_default", "adj_factor", {"ts_code": "000001.SZ", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("adj_day_20261013", "adj_factor", {"trade_date": "20261013"}, ADJ_FIELDS),
        ("adj_day_20261014", "adj_factor", {"trade_date": "20261014"}, ADJ_FIELDS),
        ("adj_long_range", "adj_factor", {"ts_code": "600519.SH", "start_date": "20010101", "end_date": "20261014"}, ADJ_FIELDS),
        ("adj_dividend_2024", "adj_factor", {"ts_code": "600519.SH", "start_date": "20240101", "end_date": "20241231"}, ADJ_FIELDS),
        ("adj_future", "adj_factor", {"trade_date": "20261020"}, ADJ_FIELDS),
        ("adj_missing", "adj_factor", {"ts_code": "999999.SH", "trade_date": "20261013"}, ADJ_FIELDS),
        ("adj_old", "adj_factor", {"trade_date": "20000104"}, ADJ_FIELDS),

        ("db_default", "daily_basic", {"ts_code": "000001.SZ", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("db_day_20261013", "daily_basic", {"trade_date": "20261013"}, DAILY_BASIC_FIELDS),
        ("db_day_20261014", "daily_basic", {"trade_date": "20261014"}, DAILY_BASIC_FIELDS),
        ("db_range_2015", "daily_basic", {"ts_code": "000001.SZ", "start_date": "20150101", "end_date": "20151231"}, DAILY_BASIC_FIELDS),
        ("db_suspended", "daily_basic", {"ts_code": "300819.SZ", "start_date": "20200311", "end_date": "20200313"}, DAILY_BASIC_FIELDS),
        ("db_future", "daily_basic", {"trade_date": "20261020"}, DAILY_BASIC_FIELDS),
        ("db_old", "daily_basic", {"trade_date": "20000104"}, DAILY_BASIC_FIELDS),

        ("sl_default", "stk_limit", {"ts_code": "000001.SZ", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("sl_day_20261013", "stk_limit", {"trade_date": "20261013"}, STK_LIMIT_FIELDS),
        ("sl_day_20261014", "stk_limit", {"trade_date": "20261014"}, STK_LIMIT_FIELDS),
        ("sl_suspended", "stk_limit", {"ts_code": "300819.SZ", "start_date": "20200311", "end_date": "20200313"}, STK_LIMIT_FIELDS),
        ("sl_old", "stk_limit", {"trade_date": "20000104"}, STK_LIMIT_FIELDS),
        ("sl_future", "stk_limit", {"trade_date": "20261020"}, STK_LIMIT_FIELDS),
        ("sl_missing", "stk_limit", {"ts_code": "999999.SH", "trade_date": "20261013"}, STK_LIMIT_FIELDS),

        ("ib_noargs", "index_basic", {}, ""),
        ("ib_ts", "index_basic", {"ts_code": "000300.SH"}, INDEX_BASIC_FIELDS),
        ("ib_market_CSI", "index_basic", {"market": "CSI"}, INDEX_BASIC_FIELDS),
        ("ib_market_SSE", "index_basic", {"market": "SSE"}, INDEX_BASIC_FIELDS),
        ("ib_market_SZSE", "index_basic", {"market": "SZSE"}, INDEX_BASIC_FIELDS),
        ("ib_market_SW", "index_basic", {"market": "SW"}, INDEX_BASIC_FIELDS),
        ("ib_page", "index_basic", {"market": "CSI", "limit": 10, "offset": 0}, INDEX_BASIC_FIELDS),
        ("ib_page_next", "index_basic", {"market": "CSI", "limit": 10, "offset": 10}, INDEX_BASIC_FIELDS),
        ("ib_missing", "index_basic", {"ts_code": "999999.XX"}, INDEX_BASIC_FIELDS),

        ("id_default", "index_daily", {"ts_code": "000300.SH", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("id_day_20261013", "index_daily", {"trade_date": "20261013"}, INDEX_DAILY_FIELDS),
        ("id_long", "index_daily", {"ts_code": "000300.SH", "start_date": "20020101", "end_date": "20261014"}, INDEX_DAILY_FIELDS),
        ("id_sw_index", "index_daily", {"ts_code": "801010.SI", "start_date": "20261001", "end_date": "20261014"}, INDEX_DAILY_FIELDS),
        ("id_future", "index_daily", {"trade_date": "20261020"}, INDEX_DAILY_FIELDS),
        ("id_missing", "index_daily", {"ts_code": "999999.XX", "trade_date": "20261013"}, INDEX_DAILY_FIELDS),
        ("id_page", "index_daily", {"trade_date": "20261013", "limit": 10, "offset": 0}, INDEX_DAILY_FIELDS),
        ("id_page_next", "index_daily", {"trade_date": "20261013", "limit": 10, "offset": 10}, INDEX_DAILY_FIELDS),

        ("iw_month_202610", "index_weight", {"index_code": "000300.SH", "start_date": "20261001", "end_date": "20261014"}, INDEX_WEIGHT_FIELDS),
        ("iw_day_20260930", "index_weight", {"trade_date": "20260930"}, INDEX_WEIGHT_FIELDS),
        ("iw_index_day", "index_weight", {"index_code": "000300.SH", "trade_date": "20260930"}, INDEX_WEIGHT_FIELDS),
        ("iw_large_000852", "index_weight", {"index_code": "000852.SH", "start_date": "20261001", "end_date": "20261014"}, INDEX_WEIGHT_FIELDS),
        ("iw_hist_200501", "index_weight", {"index_code": "000300.SH", "start_date": "20050101", "end_date": "20050131"}, INDEX_WEIGHT_FIELDS),
        ("iw_missing", "index_weight", {"index_code": "999999.SH", "trade_date": "20260930"}, INDEX_WEIGHT_FIELDS),
        ("iw_con_code", "index_weight", {"con_code": "600519.SH", "start_date": "20260101", "end_date": "20261014"}, INDEX_WEIGHT_FIELDS),

        ("ima_default", "index_member_all", {"index_code": "000300.SH"}, ""),
        ("ima_fields", "index_member_all", {"index_code": "000300.SH"}, INDEX_MEMBER_ALL_FIELDS),
        ("ima_large_000852", "index_member_all", {"index_code": "000852.SH"}, ""),
        ("ima_con_code", "index_member_all", {"con_code": "600519.SH"}, ""),
        ("ima_missing", "index_member_all", {"index_code": "999999.SH"}, ""),

        ("sw_default", "sw_daily", {"ts_code": "801010.SI", "start_date": "20261001", "end_date": "20261014"}, ""),
        ("sw_fields", "sw_daily", {"ts_code": "801010.SI", "start_date": "20261001", "end_date": "20261014"}, SW_DAILY_FIELDS),
        ("sw_day_20261013", "sw_daily", {"trade_date": "20261013"}, SW_DAILY_FIELDS),
        ("sw_long", "sw_daily", {"ts_code": "801010.SI", "start_date": "20050101", "end_date": "20261014"}, SW_DAILY_FIELDS),
        ("sw_l2", "sw_daily", {"ts_code": "801880.SI", "start_date": "20261001", "end_date": "20261014"}, SW_DAILY_FIELDS),
        ("sw_future", "sw_daily", {"trade_date": "20261020"}, SW_DAILY_FIELDS),
        ("sw_missing", "sw_daily", {"ts_code": "999999.SI", "trade_date": "20261013"}, SW_DAILY_FIELDS),
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
    for field in ("ts_code", "con_code"):
        if field in columns:
            result[f"distinct_{field}"] = len(set(columns[field]))
    for field in ("market", "publisher", "index_type", "category", "is_new"):
        if field in columns:
            result[field] = dict(Counter(str(v) for v in columns[field]).most_common(30))
    for field in ("trade_date", "list_date", "base_date", "exp_date", "in_date", "out_date"):
        values = [v for v in columns.get(field, []) if v]
        result[f"{field}_range"] = [min(values), max(values)] if values else None
    # 比较各接口的候选业务键，为确定 Canonical 粒度提供实际证据。
    keys = [("ts_code",), ("ts_code", "trade_date"), ("index_code", "con_code", "trade_date"),
            ("index_code", "con_code"), ("con_code", "in_date"), ("l1_code", "con_code", "in_date")]
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
    from dotenv import dotenv_values
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
    # 禁止复用已有输出目录，避免新探查覆盖此前响应证据。
    output = args.output or ROOT / "data/probes" / datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%f")
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
