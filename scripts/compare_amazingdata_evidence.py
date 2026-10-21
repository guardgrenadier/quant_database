"""离线复核 AmazingData 探针证据；不访问网络，不修改正式数据。"""

import argparse
from collections import Counter
from datetime import datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path

import duckdb
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
PRICE_FIELDS = ("open", "high", "low", "close")
BAR_FIELDS = (*PRICE_FIELDS, "volume", "amount")


def load_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def response(evidence_dir, case):
    return load_json(evidence_dir / f"{case}.json")["response"]


def frame(evidence_dir, case, code):
    payload = response(evidence_dir, case)
    frame_payload = payload["frames"][code]
    if frame_payload is None:
        return None
    return frame_payload["records"]


def record_index(records, keys):
    counts = Counter(tuple(record.get(key) for key in keys) for record in records)
    indexed = {}
    for record in records:
        key = tuple(record.get(key) for key in keys)
        indexed.setdefault(key, record)
    return indexed, counts


def compare_exact(left, right, keys, fields):
    """批量/拆分一致性要求逐字段相等，不用行数或聚合值代替。"""
    left_map, left_counts = record_index(left, keys)
    right_map, right_counts = record_index(right, keys)
    missing_right = sorted(set(left_map) - set(right_map))
    missing_left = sorted(set(right_map) - set(left_map))
    mismatches = []
    for key in sorted(set(left_map) & set(right_map)):
        differences = {
            field: {"left": left_map[key].get(field), "right": right_map[key].get(field)}
            for field in fields if left_map[key].get(field) != right_map[key].get(field)
        }
        if differences:
            mismatches.append({"key": key, "differences": differences})
    return {
        "left_rows": len(left), "right_rows": len(right),
        "left_duplicate_keys": sum(n - 1 for n in left_counts.values()),
        "right_duplicate_keys": sum(n - 1 for n in right_counts.values()),
        "missing_right": len(missing_right), "missing_left": len(missing_left),
        "mismatched_rows": len(mismatches),
        "equal": not (missing_right or missing_left or mismatches),
        "missing_right_sample": missing_right[:10], "missing_left_sample": missing_left[:10],
        "mismatch_sample": mismatches[:10],
    }


def raw_to_high(raw_records, code):
    """独立复算 SDK convert_history_kline 的缩放和时间解析。"""
    converted = []
    for raw in raw_records:
        parsed = datetime.strptime(str(raw["kline_time"]), "%Y%m%d%H%M")
        converted.append({
            "code": code,
            "kline_time": parsed.isoformat(),
            "open": raw["open_price"] / 1_000_000,
            "high": raw["high_price"] / 1_000_000,
            "low": raw["low_price"] / 1_000_000,
            "close": raw["close_price"] / 1_000_000,
            # 高层 SDK 对每根分别除以 100 后 astype(int)，不是先汇总再换算。
            "volume": int(raw["volume_trade"] / 100),
            "amount": raw["value_trade"] / 100_000,
        })
    return converted


def minute_structural(records):
    """检查常见 A 股连续竞价时段模板；通过也不构成供应商完整交付证明。"""
    if not records:
        return {"rows": 0, "structural_checks_passed": False}
    times = sorted(datetime.fromisoformat(record["kline_time"]) for record in records)
    day = times[0].date()
    expected = []
    for hour, minute, count in ((9, 30, 120), (13, 0, 120)):
        start = datetime.combine(day, datetime.min.time()).replace(hour=hour, minute=minute)
        expected.extend(start + timedelta(minutes=offset) for offset in range(count))
    actual_counts = Counter(times)
    actual = set(times)
    zero_volume = [record["kline_time"] for record in records if record.get("volume") == 0]
    ohlc_errors = []
    for record in records:
        values = [record.get(field) for field in PRICE_FIELDS]
        if any(value is None for value in values):
            continue
        if max(values) != record["high"] or min(values) != record["low"]:
            ohlc_errors.append(record["kline_time"])
    negative = [record["kline_time"] for record in records
                if (record.get("volume") is not None and record["volume"] < 0)
                or (record.get("amount") is not None and record["amount"] < 0)]
    missing = sorted(set(expected) - actual)
    unexpected = sorted(actual - set(expected))
    passed = not (missing or unexpected or ohlc_errors or negative
                  or any(n != 1 for n in actual_counts.values()))
    return {
        "rows": len(records), "first": times[0].isoformat(), "last": times[-1].isoformat(),
        "unique_timestamps": len(actual), "duplicate_timestamps": sum(n - 1 for n in actual_counts.values()),
        "missing_template_times": len(missing), "unexpected_times": len(unexpected),
        "missing_sample": [x.isoformat() for x in missing[:10]],
        "unexpected_sample": [x.isoformat() for x in unexpected[:10]],
        "zero_volume_rows": len(zero_volume), "zero_volume_sample": zero_volume[:10],
        "ohlc_errors": len(ohlc_errors), "negative_volume_or_amount_rows": len(negative),
        "structural_checks_passed": passed,
        "delivery_complete_provable": False,
        "completeness_limit": "响应没有显式总数、has_more 或日终终态标志；240 行及模板检查不能单独证明完整交付",
    }


def aggregate_bars(records):
    ordered = sorted(records, key=lambda record: record["kline_time"])
    return {
        "open": ordered[0]["open"], "high": max(x["high"] for x in ordered),
        "low": min(x["low"] for x in ordered), "close": ordered[-1]["close"],
        "volume": sum(x["volume"] for x in ordered), "amount": sum(x["amount"] for x in ordered),
    }


def compare_daily(minute_records, daily_records):
    if len(daily_records) != 1:
        return {"daily_rows": len(daily_records), "comparable": False}
    minute = aggregate_bars(minute_records)
    daily = daily_records[0]
    differences = {field: minute[field] - daily[field] for field in BAR_FIELDS}
    # 同源 OHLC 和股数应精确一致；金额仅容忍 JSON 浮点求和的最后小数位误差。
    matches = {field: differences[field] == 0 for field in (*PRICE_FIELDS, "volume")}
    matches["amount"] = math.isclose(minute["amount"], daily["amount"], rel_tol=0, abs_tol=0.01)
    return {
        "comparable": True, "minute_aggregate": minute,
        "daily": {field: daily[field] for field in BAR_FIELDS},
        "differences": differences, "matches": matches, "all_match": all(matches.values()),
    }


def auto_complete_summary(auto1_records, auto0_records):
    # 底层 kDataEmpty 时 records 为 null；这与空 JSON 列表都按零记录统计，错误码另保留。
    auto1_records = auto1_records or []
    auto0_records = auto0_records or []

    def times(records):
        return {str(record["kline_time"]) for record in records}
    auto1_times, auto0_times = times(auto1_records), times(auto0_records)
    zero1 = sum(int(record["volume_trade"] / 100) == 0 for record in auto1_records)
    zero0 = sum(int(record["volume_trade"] / 100) == 0 for record in auto0_records)
    return {
        "auto1_rows": len(auto1_records), "auto0_rows": len(auto0_records),
        "auto1_zero_volume_rows": zero1, "auto0_zero_volume_rows": zero0,
        "auto1_only_rows": len(auto1_times - auto0_times),
        "auto0_only_rows": len(auto0_times - auto1_times),
        "auto1_only_sample": sorted(auto1_times - auto0_times)[:20],
        "auto0_only_sample": sorted(auto0_times - auto1_times)[:20],
    }


def normalize_local_parquet(evidence_dir, parquet_path, code, minute_records, alignment):
    """按显式时间假设对齐本地和银河，再逐字段比较，不隐藏未吻合记录。"""
    if not parquet_path.exists():
        return {"available": False, "path": str(parquet_path.relative_to(ROOT))}
    query = """
        SELECT trade_time, open, high, low, close, vol, amount
        FROM read_parquet(?) WHERE code = ? ORDER BY trade_time
    """
    rows = duckdb.connect().execute(query, [str(parquet_path), code]).fetchall()
    local = []
    for trade_time, open_, high, low, close, volume, amount in rows:
        local.append({
            "kline_time": datetime.strptime(trade_time, "%Y-%m-%d %H:%M:%S").isoformat(),
            "open": open_, "high": high, "low": low, "close": close,
            "volume": volume, "amount": amount,
        })
    day = minute_records[0]["kline_time"][:10]
    first_time = f"{day}T09:30:00"
    last = f"{day}T14:59:00"
    close_auction = f"{day}T15:00:00"
    local_1500_rows = sum(record["kline_time"] == close_auction for record in local)
    local_map = {}
    if alignment == "end_label_to_start":
        opening_auction = None
        for record in local:
            timestamp = datetime.fromisoformat(record["kline_time"])
            if record["kline_time"] == first_time:
                opening_auction = record
                continue
            shifted = dict(record)
            # 本地样本是结束分钟标签；银河是开始分钟标签，所以先减一分钟。
            shifted["kline_time"] = (timestamp - timedelta(minutes=1)).isoformat()
            local_map[shifted["kline_time"]] = shifted
        if opening_auction is not None and first_time in local_map:
            first = local_map[first_time]
            # 本地首行是独立开盘竞价；银河把竞价与首个连续竞价分钟合并。
            local_map[first_time] = {
                "kline_time": first_time, "open": opening_auction["open"],
                "high": max(opening_auction["high"], first["high"]),
                "low": min(opening_auction["low"], first["low"]), "close": first["close"],
                "volume": opening_auction["volume"] + first["volume"],
                "amount": opening_auction["amount"] + first["amount"],
            }
    elif alignment == "close_auction_only":
        local_map = {record["kline_time"]: record for record in local}
        if last in local_map and close_auction in local_map:
            combined, auction = local_map[last], local_map[close_auction]
            local_map[last] = {
                "kline_time": last, "open": combined["open"],
                "high": max(combined["high"], auction["high"]),
                "low": min(combined["low"], auction["low"]), "close": auction["close"],
                "volume": combined["volume"] + auction["volume"],
                "amount": combined["amount"] + auction["amount"],
            }
        local_map.pop(close_auction, None)
    else:
        raise ValueError(f"unknown alignment: {alignment}")

    galaxy_map = {record["kline_time"]: record for record in minute_records}
    missing_galaxy = sorted(set(local_map) - set(galaxy_map))
    missing_local = sorted(set(galaxy_map) - set(local_map))
    mismatches = {field: [] for field in BAR_FIELDS}
    max_absolute = {field: 0.0 for field in BAR_FIELDS}
    for key in sorted(set(local_map) & set(galaxy_map)):
        source, target = local_map[key], galaxy_map[key]
        for field in BAR_FIELDS:
            source_value, target_value = source[field], target[field]
            difference = source_value - target_value
            max_absolute[field] = max(max_absolute[field], abs(difference))
            if field in PRICE_FIELDS:
                # 原文件 OHLC 为 float32；股票价格先回到分位再识别真实差异。
                matched = round(source_value, 2) == round(target_value, 2)
            elif field == "volume":
                tolerance = max(0.51, abs(source_value) * 2 ** -24)
                matched = abs(difference) <= tolerance
            else:
                tolerance = max(0.02, abs(target_value) * 1e-6)
                matched = abs(difference) <= tolerance
            if not matched:
                mismatches[field].append({
                    "time": key, "local": source_value, "galaxy": target_value, "difference": difference,
                })
    local_daily = aggregate_bars(list(local_map.values()))
    galaxy_daily = aggregate_bars(minute_records)
    return {
        "available": True, "path": str(parquet_path.relative_to(ROOT)), "alignment": alignment,
        "local_original_rows": len(local), "local_normalized_rows": len(local_map),
        "galaxy_rows": len(galaxy_map), "local_1500_rows": local_1500_rows,
        "missing_galaxy": len(missing_galaxy), "missing_local": len(missing_local),
        "missing_galaxy_sample": missing_galaxy[:10], "missing_local_sample": missing_local[:10],
        "key_sets_equal": not (missing_galaxy or missing_local),
        "mismatch_counts": {field: len(values) for field, values in mismatches.items()},
        "mismatch_samples": {field: values[:5] for field, values in mismatches.items()},
        "max_absolute_difference": max_absolute,
        "daily_aggregate_difference": {field: local_daily[field] - galaxy_daily[field] for field in BAR_FIELDS},
        "daily_volume_total_equal": local_daily["volume"] == galaxy_daily["volume"],
        "all_aligned_within_source_precision": not (
            missing_galaxy or missing_local or any(mismatches.values())
        ),
    }


def validate_evidence(evidence_dir):
    summary = load_json(evidence_dir / "summary.json")
    failures = []
    for item in summary:
        path = evidence_dir / item["file"]
        if not path.exists():
            failures.append({"case": item["case"], "error": "missing file"})
            continue
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if digest != item["sha256"]:
            failures.append({"case": item["case"], "error": "sha256 mismatch"})
        try:
            load_json(path)
        except (ValueError, UnicodeError) as exc:
            failures.append({"case": item["case"], "error": str(exc)})
    values = dotenv_values(ROOT / ".env")
    secrets = {values.get("AD_USERNAME"), values.get("AD_PASSWORD")} - {None, ""}
    sensitive_hits = 0
    for path in evidence_dir.glob("*.json"):
        text = path.read_text(encoding="utf-8")
        sensitive_hits += sum(secret in text for secret in secrets)
    return {
        "cases_in_index": len(summary), "validation_failures": failures,
        "all_indexed_hashes_valid": not failures,
        "credential_string_hits": sensitive_hits,
        "desensitization_check_passed": sensitive_hits == 0,
        "temporary_work_directory_exists": (evidence_dir / "work").exists(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--parquet-root", type=Path, default=ROOT / "data/raw/stock_1min")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    evidence_dir = args.evidence.resolve()
    parquet_root = args.parquet_root.resolve()
    output = (args.output or evidence_dir / "comparisons.json").resolve()
    result = {"evidence_directory": str(evidence_dir), "checks": {}}
    checks = result["checks"]

    # 批量与拆分：证券代码批量、跨日合并和日内时间切片分别独立对照。
    checks["batch_vs_split"] = {}
    for code in ("000001.SZ", "600519.SH"):
        checks["batch_vs_split"][code] = compare_exact(
            frame(evidence_dir, "min1_batch_20240417", code),
            frame(evidence_dir, f"min1_split_{code.split('.')[0]}_20240417", code),
            ("code", "kline_time"), ("code", "kline_time", *BAR_FIELDS),
        )
    multiday = frame(evidence_dir, "min1_multiday_000001_20240416_17", "000001.SZ")
    checks["multiday_vs_single"] = compare_exact(
        [x for x in multiday if x["kline_time"].startswith("2024-04-17")],
        frame(evidence_dir, "min1_split_000001_20240417", "000001.SZ"),
        ("code", "kline_time"), ("code", "kline_time", *BAR_FIELDS),
    )
    full = frame(evidence_dir, "min1_split_000001_20240417", "000001.SZ")
    expected_slice = [x for x in full if 930 <= int(x["kline_time"][11:16].replace(":", "")) <= 1000]
    checks["time_slice_vs_full"] = compare_exact(
        frame(evidence_dir, "min1_slice_000001_20240417_930_1000", "000001.SZ"),
        expected_slice, ("code", "kline_time"), ("code", "kline_time", *BAR_FIELDS),
    )

    raw = response(evidence_dir, "min1_raw_000001_20240417_auto1")
    checks["raw_vs_high_level"] = compare_exact(
        raw_to_high(raw["records"], "000001.SZ"), full,
        ("code", "kline_time"), ("code", "kline_time", *BAR_FIELDS),
    )
    checks["raw_unit_examples"] = [
        {"raw": raw["records"][i], "converted": raw_to_high([raw["records"][i]], "000001.SZ")[0]}
        for i in sorted({0, len(raw["records"]) - 1})
    ]

    checks["minute_structure"] = {
        "000001.SZ_20240417": minute_structural(full),
        "600519.SH_20240417": minute_structural(frame(evidence_dir, "min1_split_600519_20240417", "600519.SH")),
        "000001.SZ_20261016": minute_structural(frame(evidence_dir, "min1_recent_000001_20261016", "000001.SZ")),
    }
    checks["same_source_daily"] = {
        "000001.SZ_20240417": compare_daily(full, frame(evidence_dir, "day_batch_20240417", "000001.SZ")),
        "600519.SH_20240417": compare_daily(
            frame(evidence_dir, "min1_split_600519_20240417", "600519.SH"),
            frame(evidence_dir, "day_batch_20240417", "600519.SH"),
        ),
        "000001.SZ_20261016": compare_daily(
            frame(evidence_dir, "min1_recent_000001_20261016", "000001.SZ"),
            frame(evidence_dir, "day_recent_000001_20261016", "000001.SZ"),
        ),
    }

    checks["auto_complete"] = {
        "normal_000001_20240417": auto_complete_summary(
            response(evidence_dir, "min1_raw_000001_20240417_auto1")["records"],
            response(evidence_dir, "min1_raw_000001_20240417_auto0")["records"],
        ),
        "full_suspend_300104_20170417": auto_complete_summary(
            response(evidence_dir, "min1_full_suspend_300104_20170417_auto1")["records"],
            response(evidence_dir, "min1_full_suspend_300104_20170417_auto0")["records"],
        ),
        "intraday_suspend_300819_20200312": auto_complete_summary(
            response(evidence_dir, "min1_intraday_suspend_300819_20200312_auto1")["records"],
            response(evidence_dir, "min1_intraday_suspend_300819_20200312_auto0")["records"],
        ),
    }

    checks["historical_parquet"] = {}
    checks["historical_parquet_close_only_failed_hypothesis"] = {}
    for code in ("000001.SZ", "600519.SH"):
        minute_records = frame(evidence_dir, f"min1_split_{code.split('.')[0]}_20240417", code)
        parquet_path = parquet_root / "year=2024/20240417.parquet"
        checks["historical_parquet"][f"{code}_20240417"] = normalize_local_parquet(
            evidence_dir, parquet_path, code, minute_records, "end_label_to_start",
        )
        # 保留最初只合并 15:00 的错误/不足假设，证明时间对齐规则不是事后忽略差异。
        checks["historical_parquet_close_only_failed_hypothesis"][f"{code}_20240417"] = normalize_local_parquet(
            evidence_dir, parquet_path, code, minute_records, "close_auction_only",
        )
    intraday_records = raw_to_high(
        response(evidence_dir, "min1_intraday_suspend_300819_20200312_auto1")["records"], "300819.SZ",
    )
    checks["historical_parquet"]["300819.SZ_20200312"] = normalize_local_parquet(
        evidence_dir, parquet_root / "year=2020/20200312.parquet", "300819.SZ",
        intraday_records, "end_label_to_start",
    )
    suspended_path = parquet_root / "year=2017/20170417.parquet"
    if suspended_path.exists():
        row = duckdb.connect().execute(
            "SELECT count(*), sum(CASE WHEN vol=0 THEN 1 ELSE 0 END), sum(vol), sum(amount), "
            "min(trade_time), max(trade_time) FROM read_parquet(?) WHERE code='300104.SZ'",
            [str(suspended_path)],
        ).fetchone()
        checks["historical_parquet"]["300104.SZ_20170417_local_only"] = {
            "available": True, "path": str(suspended_path.relative_to(ROOT)),
            "local_rows": row[0], "local_zero_volume_rows": row[1],
            "local_volume_sum": row[2], "local_amount_sum": row[3],
            "local_first": row[4], "local_last": row[5],
            "galaxy_rows": 0, "galaxy_callback_status": -76,
            "interpretation": "本地文件补出全天零量行，银河返回 kDataEmpty；二者零行语义不同",
        }
    else:
        checks["historical_parquet"]["300104.SZ_20170417_local_only"] = {"available": False}

    checks["empty_and_boundary_responses"] = {}
    for case in (
        "min1_pre_coverage_000001_20121231", "min1_early_000001_20130104",
        "min1_invalid_999999SH_20240417", "min1_full_suspend_300104_20170417_auto0",
        "min1_full_suspend_300104_20170417_auto1",
    ):
        payload = response(evidence_dir, case)
        checks["empty_and_boundary_responses"][case] = {
            key: payload.get(key) for key in (
                "accepted", "immediate_error", "callback_status", "error_message", "return_type", "rows",
            )
        }
    checks["empty_and_boundary_responses"]["high_level_nontrade"] = response(
        evidence_dir, "min1_nontrade_000001_20261017",
    )
    checks["empty_and_boundary_responses"]["high_level_after_close"] = response(
        evidence_dir, "min1_after_close_000001_20240417",
    )

    checks["trading_status"] = {}
    for case in ("history_status_20200311_13", "history_status_20170417"):
        payload = response(evidence_dir, case)
        checks["trading_status"][case] = {
            code: None if frame_payload is None else frame_payload["records"]
            for code, frame_payload in payload["frames"].items()
        }

    result["evidence_validation"] = validate_evidence(evidence_dir)
    write_json(output, result)
    print(json.dumps({
        "output": str(output),
        "batch_equal": {k: v["equal"] for k, v in checks["batch_vs_split"].items()},
        "raw_high_equal": checks["raw_vs_high_level"]["equal"],
        "daily_match": {k: v.get("all_match") for k, v in checks["same_source_daily"].items()},
        "historical_match": {k: v.get("all_aligned_within_source_precision") for k, v in checks["historical_parquet"].items()},
        "evidence_valid": result["evidence_validation"]["all_indexed_hashes_valid"],
        "desensitized": result["evidence_validation"]["desensitization_check_passed"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
