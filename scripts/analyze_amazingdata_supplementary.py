"""分析银河补充探针：范围缺数、复权、北交所和混合批量；只读本地证据。"""

import argparse
from collections import Counter
from datetime import datetime, timedelta
import json
from pathlib import Path

from compare_amazingdata_evidence import (
    auto_complete_summary, compare_daily, compare_exact, frame, load_json,
    raw_to_high, response, validate_evidence,
)


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def template_check(records, sessions):
    times = [datetime.fromisoformat(record["kline_time"]) for record in records]
    expected = []
    for hour, minute, count in sessions:
        start = times[0].replace(hour=hour, minute=minute, second=0, microsecond=0)
        expected.extend(start + timedelta(minutes=offset) for offset in range(count))
    actual = set(times)
    missing = sorted(set(expected) - actual)
    unexpected = sorted(actual - set(expected))
    return {
        "rows": len(records), "unique_timestamps": len(actual),
        "duplicate_timestamps": len(records) - len(actual),
        "first": min(times).isoformat(), "last": max(times).isoformat(),
        "zero_volume_rows": sum(record["volume"] == 0 for record in records),
        "missing_template_times": [x.isoformat() for x in missing],
        "unexpected_times": [x.isoformat() for x in unexpected],
        "template_checks_passed": not (missing or unexpected or len(times) != len(actual)),
        "delivery_complete_provable": False,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full", type=Path, required=True)
    parser.add_argument("--recovery", type=Path, required=True)
    parser.add_argument("--extended", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    full, recovery, extended = args.full.resolve(), args.recovery.resolve(), args.extended.resolve()
    result = {"checks": {}}
    checks = result["checks"]

    range07 = [
        record for record in response(recovery, "index_weight_raw_000300_202609_range")["records"]
        if record["TRADE_DATE"] == "20260907"
    ]
    single07 = response(recovery, "index_weight_raw_000300_202609_single")["records"]
    raw_fields = ("TOTAL_SHR", "FREE_SHR_RATIO", "CALC_SHR", "WEIGHT_FACTOR", "WEIGHT", "CLOSE")
    keys = ("INDEX_CODE", "CON_CODE", "TRADE_DATE")
    checks["index_weight_raw_range_vs_single_20260907"] = compare_exact(
        range07, single07, keys, (*keys, *raw_fields),
    )
    range_key_counts = Counter(tuple(record[key] for key in keys) for record in range07)
    single_keys = {tuple(record[key] for key in keys) for record in single07}
    duplicate_keys = [key for key, count in range_key_counts.items() if count > 1]
    checks["index_weight_raw_range_vs_single_20260907"].update({
        "duplicate_key_examples": duplicate_keys[:10],
        "missing_codes_sample": sorted({key[1] for key in single_keys - set(range_key_counts)})[:20],
        "explanation": "范围原始响应有 99 个完全重复键且缺另外 99 个成分；单日响应为 300 个唯一成分",
    })

    high_month = frame(full, "index_weight_000300_202609", "000300.SH")
    high_single = frame(recovery, "index_weight_000300_20260907_single", "000300.SH")
    high_repeat = frame(recovery, "index_weight_000300_202609_repeat", "000300.SH")
    high_fields = ("TOTAL_SHARE", "FREE_SHARE_RATIO", "CALC_SHARE", "WEIGHT_FACTOR", "WEIGHT", "CLOSE")
    checks["index_weight_high_range_vs_single_20260907"] = compare_exact(
        [record for record in high_month if record["TRADE_DATE"] == "20260907"],
        high_single, keys, (*keys, *high_fields),
    )
    checks["index_weight_repeat_vs_original"] = compare_exact(
        high_repeat, high_month, keys, (*keys, *high_fields),
    )
    checks["index_weight_empty_ranges"] = {}
    for case in ("index_weight_000300_20260911_30", "index_weight_000300_20261012_16"):
        checks["index_weight_empty_ranges"][case] = {
            key: load_json(recovery / f"{case}.json").get(key) for key in ("outcome", "error")
        }
    for case in ("index_weight_raw_000300_202609_later_empty", "index_weight_raw_000300_202610_october_empty"):
        payload = response(recovery, case)
        checks["index_weight_empty_ranges"][case] = {
            key: payload.get(key) for key in ("callback_status", "error_message", "return_type", "rows")
        }

    factors = response(recovery, "ex_factor_raw_600519")["records"]
    product_errors = []
    for previous, current in zip(factors, factors[1:]):
        expected = previous["cum_factor"] * current["ex_factor"]
        if abs(expected - current["cum_factor"]) > 5.1e-7:
            product_errors.append({"ex_date": current["ex_date"], "expected": expected, "actual": current["cum_factor"]})
    checks["ex_factor"] = {
        "rows": len(factors), "unique_dates": len({record["ex_date"] for record in factors}),
        "first_date": min(record["ex_date"] for record in factors),
        "last_date": max(record["ex_date"] for record in factors),
        "cum_product_errors_beyond_rounding": product_errors,
    }

    constituent = response(recovery, "index_constituent_raw_000300")["records"]
    rn = {record["RN"] for record in constituent}
    checks["index_constituent"] = {
        "rows": len(constituent), "current_rows": sum(record["CURRENT_SIGN"] == 1 for record in constituent),
        "historical_rows": sum(record["CURRENT_SIGN"] == 0 for record in constituent),
        "null_outdate_rows": sum(record["CON_OUTDATE"] is None for record in constituent),
        "current_with_outdate": sum(record["CURRENT_SIGN"] == 1 and record["CON_OUTDATE"] is not None for record in constituent),
        "duplicate_business_keys": len(constituent) - len({
            (record["INDEX_CODE"], record["CON_CODE"], record["CON_INDATE"]) for record in constituent
        }),
        "rn_min": min(rn), "rn_max": max(rn), "rn_contiguous": rn == set(range(1, len(constituent) + 1)),
    }

    daily = frame(extended, "day_600519_ex_factor_20240618_19", "600519.SH")
    status = frame(extended, "history_status_600519_20240618_19", "600519.SH")
    factor = next(record for record in factors if record["ex_date"] == 20240619)
    previous_close = next(record["close"] for record in daily if record["kline_time"].startswith("2024-06-18"))
    actual_preclose = next(record["PRECLOSE"] for record in status if record["TRADE_DATE"] == "20240619")
    expected_preclose = previous_close / factor["ex_factor"]
    checks["factor_preclose_relation"] = {
        "previous_unadjusted_close": previous_close, "ex_factor": factor["ex_factor"],
        "computed_preclose": expected_preclose, "vendor_preclose": actual_preclose,
        "difference": expected_preclose - actual_preclose,
        "matches_at_price_tick": round(expected_preclose, 2) == round(actual_preclose, 2),
        "status_record": next(record for record in status if record["TRADE_DATE"] == "20240619"),
        "note": "仅验证单次因子与除息日前收的关系，不据此假定所有前/后复权公式",
    }

    bj_code = response(extended, "bj_code_list_current")["selected_for_minute"]
    bj_minute = frame(extended, "bj_min1_recent_20261016", bj_code)
    bj_daily = frame(extended, "bj_day_recent_20261016", bj_code)
    bj_raw1 = response(extended, "bj_min1_raw_recent_20261016_auto1")["records"]
    bj_raw0 = response(extended, "bj_min1_raw_recent_20261016_auto0")["records"]
    auto1_only = [record for record in bj_raw1 if record["kline_time"] not in {x["kline_time"] for x in bj_raw0}]
    checks["bj_minute"] = {
        "code": bj_code,
        "template_09_30_11_30_and_13_00_15_00": template_check(bj_minute, ((9, 30, 121), (13, 0, 121))),
        "raw_vs_high": compare_exact(
            raw_to_high(bj_raw1, bj_code), bj_minute,
            ("code", "kline_time"), ("code", "kline_time", "open", "high", "low", "close", "volume", "amount"),
        ),
        "auto_complete": auto_complete_summary(bj_raw1, bj_raw0),
        "auto1_only_all_zero": all(record["volume_trade"] == 0 and record["value_trade"] == 0 for record in auto1_only),
        "auto0_has_1130": any(record["kline_time"] == 202610161130 for record in bj_raw0),
        "auto0_1500_records": [record for record in bj_raw0 if record["kline_time"] == 202610161500],
        "same_source_daily": compare_daily(bj_minute, bj_daily),
    }

    mixed = response(extended, "min1_mixed_valid_invalid_20240417")
    checks["mixed_valid_invalid"] = {
        "valid_rows": mixed["frames"]["000001.SZ"]["shape"][0],
        "invalid_is_null": mixed["frames"]["999999.SH"] is None,
        "outcome_has_no_error_field": "error" not in mixed,
        "interpretation": "有效代码数据和无效代码 null 同时返回；无效、停牌及其他空响应不能由高层返回形状区分",
    }
    checks["historical_universe_20170417"] = response(extended, "hist_code_list_20170417")
    checks["evidence_validation"] = {
        "full": validate_evidence(full), "recovery": validate_evidence(recovery),
        "extended": validate_evidence(extended),
    }
    write_json(args.output.resolve(), result)
    print(json.dumps({
        "output": str(args.output.resolve()),
        "raw_range_single_equal": checks["index_weight_raw_range_vs_single_20260907"]["equal"],
        "repeat_equal": checks["index_weight_repeat_vs_original"]["equal"],
        "factor_preclose_match": checks["factor_preclose_relation"]["matches_at_price_tick"],
        "bj_template_match": checks["bj_minute"]["template_09_30_11_30_and_13_00_15_00"]["template_checks_passed"],
        "bj_daily_match": checks["bj_minute"]["same_source_daily"]["all_match"],
        "evidence_valid": {name: value["all_indexed_hashes_valid"] for name, value in checks["evidence_validation"].items()},
        "desensitized": {name: value["desensitization_check_passed"] for name, value in checks["evidence_validation"].items()},
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
