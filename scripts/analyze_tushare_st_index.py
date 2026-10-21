"""离线复核五接口探查的原始证据，生成可入 Git 的摘要；不连接供应商或权威库。"""

import argparse
from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from scripts.probe_tushare_st_index import statistics


def multiset(rows, fields=None):
    """按字段名比较完整多重集合，不能用 set 消除重复后宣称两次交付相同。"""
    return Counter(json.dumps({k: r[k] for k in fields} if fields else r,
                              ensure_ascii=False, sort_keys=True) for r in rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--documents", type=Path, help="本地官方文档快照的 manifest.json")
    args = parser.parse_args()
    groups, requests, sources = {}, [], []
    for root in args.runs:
        summary_path = root / "summary.json"
        summary = json.loads(summary_path.read_text())
        sources.append({"path": str(summary_path), "sha256": hashlib.sha256(summary_path.read_bytes()).hexdigest()})
        if (root / "run-note.json").exists():
            sources[-1]["note"] = json.loads((root / "run-note.json").read_text())
        by_id = {r["request_id"]: r for r in summary["requests"]}
        for request in summary["requests"]:
            # 证据检查：核对每次实际尝试的原始文件，不把摘要本身当成响应证据。
            for attempt in request.get("attempts", []):
                raw = Path(attempt["evidence_path"]).read_bytes()
                if hashlib.sha256(raw).hexdigest() != attempt["evidence_sha256"]:
                    raise ValueError(f"Evidence digest mismatch: {attempt['evidence_path']}")
            requests.append({"run": str(root), **request})
        for label, group in summary["groups"].items():
            records, missing = [], []
            for request_id in group["request_ids"]:
                request = by_id.get(request_id)
                if request is None:
                    # 中断请求没有响应，显式记录缺失；不能把此前若干页解释成完整集合。
                    missing.append(request_id)
                    continue
                if "error" in request:
                    continue
                payload = json.loads(Path(request["attempts"][-1]["evidence_path"]).read_text())["response"]
                data = payload["data"]
                records.extend(dict(zip(data["fields"], row, strict=True)) for row in data["items"])
            groups[label] = {"run": str(root), **group, "rows": records, "missing_response_ids": missing}

    def rows(label):
        return groups[label]["rows"]

    def equal(left, right, fields=None):
        return {"left": left, "right": right, "equal_multiset": multiset(rows(left), fields) == multiset(rows(right), fields)}

    findings = {}
    comparisons = [("st_20261019", "st_default_fields"), ("id_day", "id_pages"),
                   ("iw_day", "iw_invalid_param"), ("ima_default", "ima_Y"),
                   ("ima_Y", "ima_empty_flag"), ("ima_Y", "ima_invalid_param"),
                   ("ima_Y", "ima_Y_pages"), ("ima_N", "ima_N_pages"),
                   ("st_20261019", "st_day_range"), ("iw_day", "iw_invalid_missing"),
                   ("iw_september", "iw_single_day")]
    findings["comparisons"] = [equal(a, b) for a, b in comparisons if a in groups and b in groups]
    basic = rows("ib_pages")
    by_code = {r["ts_code"]: r for r in basic}
    findings["basic"] = {"markets": dict(Counter(r["market"] for r in basic)),
                         "terminated_records": sum(r["exp_date"] is not None for r in basic),
                         "future_list_dates": sum(r["list_date"] is not None and r["list_date"] > "20261020" for r in basic)}
    if "ib_CSI_pages" in groups:
        findings["basic"]["CSI_partition_equal"] = multiset([r for r in basic if r["market"] == "CSI"]) == multiset(rows("ib_CSI_pages"))
    daily = rows("id_pages")
    no_close = [r for r in daily if r["close"] is None]
    bad_ohlc = [r for r in daily if all(r[f] is not None for f in ("open", "high", "low", "close"))
                and (r["high"] < max(r["open"], r["close"], r["low"]) or r["low"] > min(r["open"], r["close"]))]
    numeric = ("close", "open", "high", "low", "pre_close", "change", "pct_chg", "vol", "amount")
    findings["daily"] = {
        "markets": dict(Counter(by_code.get(r["ts_code"], {}).get("market", "MISSING_BASIC") for r in daily)),
        "missing_basic_codes": sorted({r["ts_code"] for r in daily if r["ts_code"] not in by_code}),
        "close_nulls": len(no_close),
        "null_close_markets": dict(Counter(by_code[r["ts_code"]]["market"] for r in no_close)),
        "all_numeric_nulls": sum(all(r[f] is None for f in numeric) for r in daily),
        "null_close_examples": [{"daily": r, "basic": by_code[r["ts_code"]]} for r in no_close[:12]],
        "OHLC_order_violations": len(bad_ohlc), "OHLC_examples": bad_ohlc[:5],
        "negative_volume": sum(r["vol"] is not None and r["vol"] < 0 for r in daily),
        "negative_amount": sum(r["amount"] is not None and r["amount"] < 0 for r in daily),
    }
    if "id_cross_calendar_all" in groups:
        # 业务证据：SSE 休市日仍有供应商指数事实，不可把 SSE 开市集合当成全指数 Scope。
        findings["daily"]["SSE_closed_date_sample"] = {"date": "20261005", "rows": len(rows("id_cross_calendar_all")),
            "H11100_rows": rows("id_cross_calendar")}
    weight = rows("iw_pages")
    # 业务证据：全市场分页被 offset 上限阻断，不能用不完整分组的权重和判断供应商质量。
    # 权重和仅检查独立完整取得的单指数样本；全市场部分样本另报规模和交付状态。
    full_samples = [r for label in ("iw_september", "iw_1000", "iw_doc_example") if label in groups for r in rows(label)]
    grouped = defaultdict(list)
    for r in full_samples:
        grouped[(r["index_code"], r["trade_date"])].append(r)
    sums = [(key, sum(r["weight"] for r in values)) for key, values in grouped.items()]
    findings["weight"] = {
        "complete_sample_indices": len(grouped), "complete_sample_rows": len(full_samples),
        "partial_global_indices": len({r["index_code"] for r in weight}), "partial_global_rows": len(weight),
        "global_pagination_terminated": groups["iw_pages"].get("pagination_terminated", False),
        "zero_weights": sum(r["weight"] == 0 for r in weight),
        "negative_weights": sum(r["weight"] < 0 for r in weight),
        "weight_range": [min(r["weight"] for r in weight), max(r["weight"] for r in weight)] if weight else None,
        "sum_outside_99_101": sum(not 99 <= total <= 101 for _, total in sums),
        "sum_examples": [{"key": key, "total": total} for key, total in sums if not 99 <= total <= 101][:8],
        "constituent_suffixes": dict(Counter(r["con_code"].rsplit(".", 1)[-1] for r in weight if "." in r["con_code"])),
        "constituents_without_dot": sum("." not in r["con_code"] for r in weight),
        "bare_constituent_examples": sorted({r["con_code"] for r in weight if "." not in r["con_code"]})[:15],
    }
    if "iw_september" in groups:
        findings["weight"]["CSI300_rows_in_partial_global"] = sum(r["index_code"] == "000300.SH" for r in weight)
    if "iw_300_pages" in groups:
        findings["comparisons"].append(equal("iw_september", "iw_300_pages"))
    members = rows("ima_Y_pages") + rows("ima_N_pages")
    fields = list(members[0])
    findings["members"] = statistics("index_member_all", {"code": 0, "data": {"fields": fields, "items": [[r[f] for f in fields] for r in members]}})
    findings["members"]["date_order_violations"] = sum(r["out_date"] is not None and r["out_date"] < r["in_date"] for r in members)
    findings["members"]["pre_1990_records"] = sum(r["in_date"] < "19900101" for r in members)
    for label, field in (("ima_l1", "l1_code"), ("ima_l2", "l2_code"), ("ima_l3", "l3_code"), ("ima_stock", "ts_code")):
        value = groups[label]["params"][field]
        findings["comparisons"].append({"left": label, "right": "ima_Y_pages filtered", "equal_multiset": multiset(rows(label)) == multiset([r for r in rows("ima_Y_pages") if r[field] == value])})
    result = {"generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
              "sources": sources, "request_count": len(requests), "attempt_count": sum(len(r.get("attempts", [])) for r in requests),
              "requests": requests, "groups": {label: {k: v for k, v in g.items() if k != "rows"} for label, g in groups.items()},
              "findings": findings}
    if args.documents is not None:
        result["documents"] = json.loads(args.documents.read_text())
        result["document_manifest_sha256"] = hashlib.sha256(args.documents.read_bytes()).hexdigest()
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({"requests": len(requests), "attempts": result["attempt_count"], "output": str(args.output)}, ensure_ascii=False))


if __name__ == "__main__":
    main()
