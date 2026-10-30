"""五接口只读研究探针；固定用例不是正式获取契约，不发布 Canonical。"""

import argparse
from collections import Counter
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import time
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

from quant_data.core.artifacts import Artifacts
from quant_data.core.models import control_data
from quant_data.sources.tushare.client import TushareClient
if __package__:
    from .probe_tushare_daily_index import INDEX_BASIC_FIELDS, INDEX_DAILY_FIELDS, summarize
else:
    from probe_tushare_daily_index import INDEX_BASIC_FIELDS, INDEX_DAILY_FIELDS, summarize

ROOT = Path(__file__).resolve().parents[1]
FIELDS = {
    "stock_st": "ts_code,name,trade_date,type,type_name",
    "index_basic": INDEX_BASIC_FIELDS,
    "index_daily": INDEX_DAILY_FIELDS,
    "index_weight": "index_code,con_code,trade_date,weight",
    "index_member_all": "l1_code,l1_name,l2_code,l2_name,l3_code,l3_name,ts_code,name,in_date,out_date,is_new",
}
KEYS = {
    "stock_st": [("ts_code", "trade_date"), ("ts_code", "trade_date", "type")],
    "index_basic": [("ts_code",)],
    "index_daily": [("ts_code", "trade_date")],
    "index_weight": [("index_code", "con_code", "trade_date")],
    "index_member_all": [("ts_code", "in_date"), ("l3_code", "ts_code", "in_date")],
}


def cases():
    """实验日期覆盖最近完整交易日、休市日、未来日和若干历史边界。"""
    result = []
    for day in ("20261019", "20261016", "20261018", "20261030", "20250813",
                "20200312", "20150708", "20100104", "20000104", "19991230"):
        result.append((f"st_{day}", "stock_st", {"trade_date": day}, False))
    result += [
        ("st_default", "stock_st", {}, False),
        ("st_missing", "stock_st", {"ts_code": "999999.SH", "trade_date": "20261019"}, False),
        ("st_single", "stock_st", {"ts_code": "000002.SZ", "start_date": "20260901", "end_date": "20261019"}, False),
        ("st_range", "stock_st", {"start_date": "20260901", "end_date": "20260930"}, False),
        ("st_default_fields", "stock_st", {"trade_date": "20261019"}, False),
        ("ib_default", "index_basic", {}, False),
        ("ib_single", "index_basic", {"ts_code": "000300.SH"}, False),
        ("ib_missing", "index_basic", {"ts_code": "999999.XX"}, False),
        ("ib_symbols", "index_basic", {"symbol": "000300,000001"}, False),
        ("id_day", "index_daily", {"trade_date": "20261019"}, False),
        ("id_recent", "index_daily", {"ts_code": "000300.SH", "start_date": "20261001", "end_date": "20261019"}, False),
        ("id_long", "index_daily", {"ts_code": "000300.SH", "start_date": "19900101", "end_date": "20261019"}, False),
        ("id_SW", "index_daily", {"ts_code": "801010.SI", "trade_date": "20261019"}, False),
        ("id_closed", "index_daily", {"trade_date": "20261018"}, False),
        ("id_future", "index_daily", {"trade_date": "20261030"}, False),
        ("id_missing", "index_daily", {"ts_code": "999999.XX", "trade_date": "20261019"}, False),
        ("id_multi", "index_daily", {"ts_code": "000300.SH,000001.SH", "trade_date": "20261019"}, False),
        ("iw_day", "index_weight", {"trade_date": "20260930"}, False),
        ("iw_september", "index_weight", {"index_code": "000300.SH", "start_date": "20260901", "end_date": "20260930"}, False),
        ("iw_october", "index_weight", {"index_code": "000300.SH", "start_date": "20261001", "end_date": "20261031"}, False),
        ("iw_august", "index_weight", {"index_code": "000300.SH", "start_date": "20260801", "end_date": "20260831"}, False),
        ("iw_launch", "index_weight", {"index_code": "000300.SH", "start_date": "20050401", "end_date": "20050430"}, False),
        ("iw_doc_example", "index_weight", {"index_code": "399300.SZ", "start_date": "20180901", "end_date": "20180930"}, False),
        ("iw_missing", "index_weight", {"index_code": "999999.XX", "start_date": "20260901", "end_date": "20260930"}, False),
        ("iw_invalid_param", "index_weight", {"con_code": "600519.SH", "trade_date": "20260930"}, False),
        ("ima_default", "index_member_all", {}, False),
        ("ima_Y", "index_member_all", {"is_new": "Y"}, False),
        ("ima_N", "index_member_all", {"is_new": "N"}, False),
        ("ima_empty_flag", "index_member_all", {"is_new": ""}, False),
        ("ima_l1", "index_member_all", {"l1_code": "801010.SI"}, False),
        ("ima_l2", "index_member_all", {"l2_code": "801053.SI"}, False),
        ("ima_l3", "index_member_all", {"l3_code": "850531.SI"}, False),
        ("ima_stock", "index_member_all", {"ts_code": "000001.SZ"}, False),
        ("ima_stock_history", "index_member_all", {"ts_code": "000001.SZ", "is_new": "N"}, False),
        ("ima_missing", "index_member_all", {"ts_code": "999999.SH"}, False),
        ("ima_invalid_param", "index_member_all", {"index_code": "999999.XX"}, False),
    ]
    for market in ("SSE", "SZSE", "CSI", "SW", "CNI", "BSE", "MSCI", "CICC", "OTH"):
        result.append((f"ib_{market}", "index_basic", {"market": market}, False))
    # 分页不假定文档上限就是实际上限；小页取尽后再取空页验证终止和 offset。
    result += [("ib_pages", "index_basic", {}, True),
               ("id_pages", "index_daily", {"trade_date": "20261019"}, True),
               ("iw_pages", "index_weight", {"trade_date": "20260930"}, True),
               ("ima_Y_pages", "index_member_all", {"is_new": "Y"}, True),
               ("ima_N_pages", "index_member_all", {"is_new": "N"}, True),
               ("st_pages", "stock_st", {"start_date": "20260901", "end_date": "20260930"}, True)]
    # 补证用例来自首轮观察：验证真实命中的股票、较早历史和日期/分类过滤，仍仅为实验。
    result += [(f"st_history_{day}", "stock_st", {"trade_date": day}, False)
               for day in ("20000103", "20000105", "20010102", "20050104", "20070104", "20090105")]
    result += [
        ("st_today", "stock_st", {"trade_date": "20261020"}, False),
        ("st_known", "stock_st", {"ts_code": "000010.SZ", "trade_date": "20261019"}, False),
        ("st_known_range", "stock_st", {"ts_code": "000010.SZ", "start_date": "20261016", "end_date": "20261019"}, False),
        ("st_old_known", "stock_st", {"ts_code": "000004.SZ", "trade_date": "20100104"}, False),
        ("st_small_page0", "stock_st", {"trade_date": "20261019", "limit": 2, "offset": 0}, False),
        ("st_small_page1", "stock_st", {"trade_date": "20261019", "limit": 2, "offset": 2}, False),
        ("st_day_range", "stock_st", {"start_date": "20261019", "end_date": "20261019"}, False),
        ("ib_CSI_pages", "index_basic", {"market": "CSI"}, True),
        ("ib_NH", "index_basic", {"market": "NH"}, False),
        ("ib_CI", "index_basic", {"market": "CI"}, False),
        ("id_old_day", "index_daily", {"trade_date": "20261013"}, False),
        ("id_single_day", "index_daily", {"ts_code": "000300.SH", "trade_date": "20261019"}, False),
        ("id_volume_only", "index_daily", {"ts_code": "395001.SZ", "trade_date": "20261019"}, False),
        ("id_null_close", "index_daily", {"ts_code": "H11100.CSI", "start_date": "20261012", "end_date": "20261019"}, False),
        ("id_CI", "index_daily", {"ts_code": "CI005001.CI", "trade_date": "20261019"}, False),
        ("id_NH", "index_daily", {"ts_code": "A.NH", "trade_date": "20261019"}, False),
        ("id_cross_calendar", "index_daily", {"ts_code": "H11100.CSI", "trade_date": "20261005"}, False),
        ("id_cross_calendar_all", "index_daily", {"trade_date": "20261005"}, False),
        ("iw_single_day", "index_weight", {"index_code": "000300.SH", "trade_date": "20260930"}, False),
        ("iw_nonreport_day", "index_weight", {"index_code": "000300.SH", "trade_date": "20260929"}, False),
        ("iw_1000", "index_weight", {"index_code": "000852.SH", "start_date": "20260901", "end_date": "20260930"}, False),
        ("iw_invalid_missing", "index_weight", {"con_code": "999999.XX", "trade_date": "20260930"}, False),
        ("iw_offset_100000", "index_weight", {"trade_date": "20260930", "limit": 1000, "offset": 100000}, False),
        ("iw_offset_100001", "index_weight", {"trade_date": "20260930", "limit": 1000, "offset": 100001}, False),
        ("iw_300_pages", "index_weight", {"index_code": "000300.SH", "start_date": "20260901", "end_date": "20260930"}, True),
        ("ima_invalid_fields", "index_member_all", {"l1_code": "801010.SI"}, False),
        ("ima_missing_l3", "index_member_all", {"l3_code": "999999.SI"}, False),
        ("ima_conflict", "index_member_all", {"l3_code": "850531.SI", "ts_code": "000001.SZ"}, False),
    ]
    return result


def statistics(api, body):
    """复用已有信封统计，另保留 NULL/空串区别及本轮候选键计数。"""
    result = summarize(body)
    if result.get("code") != 0 or "protocol_error" in result:
        return result
    fields, items = body["data"]["fields"], body["data"]["items"]
    columns = {f: [r[i] for r in items] for i, f in enumerate(fields)}
    # 业务证据：缺列与可空字段不能合并；原始值不清理，键重复也不去重。
    result["missing_fields"] = sorted(set(FIELDS[api].split(",")) - set(fields))
    result["json_nulls"] = {f: sum(v is None for v in values) for f, values in columns.items()}
    result["empty_strings"] = {f: sum(v == "" for v in values) for f, values in columns.items()}
    result["duplicate_keys"] = {}
    for key in KEYS[api]:
        if all(f in columns for f in key):
            counts = Counter(tuple(r[fields.index(f)] for f in key) for r in items)
            result["duplicate_keys"][",".join(key)] = sum(n - 1 for n in counts.values())
    for field in ("type", "type_name", "market", "is_new"):
        if field in columns:
            result[field] = dict(Counter(str(v) for v in columns[field]))
    for field in ("trade_date", "in_date", "out_date", "list_date", "exp_date"):
        values = [v for v in columns.get(field, []) if v is not None and v != ""]
        result[field + "_range"] = [min(values), max(values)] if values else None
    # 无序多重集合指纹保留重复记录，允许比较过滤/默认字段与不同分页分块的交付内容。
    rows = [json.dumps(dict(zip(fields, row, strict=True)), ensure_ascii=False, sort_keys=True) for row in items]
    result["rows_sha256"] = hashlib.sha256("\n".join(sorted(rows)).encode()).hexdigest()
    return result


def main():
    """串行研究获取，所有尝试留证；期望中的参数错误也保留为失败，不伪装成功。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case", action="append")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--page-size", type=int, default=1000)
    parser.add_argument("--max-pages", type=int, default=100)
    args = parser.parse_args()
    selected = [c for c in cases() if not args.case or c[0] in args.case]
    if args.case and set(args.case) - {c[0] for c in selected}:
        parser.error("unknown case")
    if args.page_size < 1 or args.max_pages < 2:
        parser.error("positive page size and at least two pages required")
    token = (dotenv_values(ROOT / ".env").get("TUSHARE_TOKEN") or os.environ.get("TUSHARE_TOKEN", "")).strip()
    if not token:
        parser.error("TUSHARE_TOKEN is required")
    output = args.output or ROOT / "data/probes" / ("st-index-" + datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%f"))
    artifacts = Artifacts(output, False)
    client = TushareClient(token)
    requests, groups, last_request = [], {}, None
    for label, api, base_params, paged in selected:
        merged, expected_fields, page_hashes = [], None, set()
        offset, saw_terminal, terminated = 0, False, False
        group = {"api_name": api, "params": base_params, "paged": paged, "request_ids": []}
        groups[label] = group
        for page in range(args.max_pages if paged else 1):
            params = {**base_params, "limit": args.page_size, "offset": offset} if paged else base_params
            request_id = f"{label}-p{page:03d}"
            fields = () if label in ("st_default_fields", "ib_default", "ima_default") else tuple(FIELDS[api].split(","))
            if label == "ima_invalid_fields":
                # 业务证据：旧探针曾使用 con_code/con_name，需直接验证这些非文档字段是否被忽略。
                fields = ("l3_code", "con_code", "con_name", "in_date", "out_date", "is_new")
            record = {"request_id": request_id, "api_name": api, "params": params, "requested_fields": fields}
            group["request_ids"].append(request_id)

            def on_attempt(attempt, payload):
                saved = artifacts.record_attempt(attempt, payload)
                record.setdefault("attempts", []).append({**control_data(saved),
                    "evidence_sha256": hashlib.sha256(Path(saved.evidence_path).read_bytes()).hexdigest()})

            if last_request is not None:
                time.sleep(max(0, 1.3 - (time.monotonic() - last_request)))
            last_request = time.monotonic()
            try:
                body = client.query(api, params, fields, request_id=request_id, on_attempt=on_attempt)
                record.update(statistics(api, body))
                data = body["data"]
                # 交付研究：页间列序须一致；重复整页表明 offset 可能被忽略，明确中止且不报完成。
                if expected_fields is None:
                    expected_fields = data["fields"]
                if data["fields"] != expected_fields:
                    raise ValueError("fields changed across pages")
                if data["items"] and record["rows_sha256"] in page_hashes:
                    raise ValueError("repeated nonempty page")
                page_hashes.add(record["rows_sha256"])
                merged.extend(data["items"])
                if paged:
                    # 终止是实测结论：必须显式 false 且不足一页；再验证后续空页，绝不从 count 推断。
                    if saw_terminal:
                        if data["items"] or data.get("has_more") is not False:
                            raise ValueError("nonempty or ambiguous page after terminal")
                        terminated = True
                    elif data.get("has_more") is False and len(data["items"]) < args.page_size:
                        saw_terminal = True
                    offset += args.page_size
            except (RuntimeError, ValueError, OSError, TypeError) as exc:
                record["error"] = f"{type(exc).__name__}: {exc}".replace(token, "<redacted>")
                group["error"] = record["error"]
            requests.append(record)
            print(request_id, json.dumps({k: record[k] for k in ("rows", "has_more", "error") if k in record}), flush=True)
            # 逐请求即时保存控制摘要；中断时已有原始信封与失败证据仍可核查。
            (output / "summary.json").write_text(json.dumps({"requests": requests, "groups": groups}, ensure_ascii=False, indent=2) + "\n")
            if "error" in record or terminated:
                break
        group["pagination_terminated"] = terminated if paged else None
        if paged and not terminated and "error" not in group:
            group["error"] = "page budget exhausted; completeness unresolved"
        if expected_fields is not None:
            group["statistics"] = statistics(api, {"code": 0, "data": {"fields": expected_fields, "items": merged}})
        (output / "summary.json").write_text(json.dumps({"requests": requests, "groups": groups}, ensure_ascii=False, indent=2) + "\n")
    print(f"Evidence: {output}", flush=True)
    return int(any("error" in g for g in groups.values()))


if __name__ == "__main__":
    raise SystemExit(main())
