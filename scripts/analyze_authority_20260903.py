"""以 20260903-1min.parquet 为权威参考，复核银河 2026-09-03 分钟数据质量。"""

import argparse
from collections import Counter
from decimal import Decimal
import json
from pathlib import Path

import duckdb
from dotenv import dotenv_values

from compare_amazingdata_evidence import load_json, response, validate_evidence

ROOT = Path(__file__).resolve().parents[1]
CODES = (
    "000001.SZ", "600519.SH", "300819.SZ", "688981.SH",
    "300308.SZ", "688825.SH", "688808.SH", "000010.SZ",
    "600365.SH", "300555.SZ", "688184.SH", "002168.SZ",
)
REFERENCE_MAX_TIME = 202609031456
RAW_PRICE_FIELDS = ("open_price", "high_price", "low_price", "close_price")
RAW_FIELDS = (*RAW_PRICE_FIELDS, "volume_trade", "value_trade")


def write_json(path, value):
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")


def scaled_integer(value, multiplier):
    """权威 Decimal 值必须能精确转为银河原始整数，避免浮点误差伪装成数据差异。"""
    scaled = value * multiplier
    integral = scaled.to_integral_value()
    if scaled != integral:
        raise ValueError(f"reference value cannot scale exactly: {value} * {multiplier}")
    return int(integral)


def authority_records(parquet_path, code):
    bare_code = code.split(".", 1)[0]
    rows = duckdb.connect().execute(
        """
        SELECT start_time, open_price, high_price, low_price, close_price,
               turnover_volume, turnover_value, turnover_count
        FROM read_parquet(?) WHERE security_code = ? ORDER BY start_time
        """,
        [str(parquet_path), bare_code],
    ).fetchall()
    records = []
    for start_time, open_, high, low, close, volume, value, trade_count in rows:
        kline_time = 20260903 * 10000 + start_time
        if volume == 0:
            # 权威文件用 OHLC=NULL 表示零成交槽位；量额仍显式为零。
            price_values = (None, None, None, None)
        else:
            price_values = tuple(scaled_integer(value, Decimal(1_000_000)) for value in (open_, high, low, close))
        records.append({
            "kline_time": kline_time,
            "open_price": price_values[0], "high_price": price_values[1],
            "low_price": price_values[2], "close_price": price_values[3],
            "volume_trade": volume * 100,
            "value_trade": scaled_integer(value, Decimal(100_000)),
            "turnover_count": trade_count,
        })
    return records


def compare_records(reference, galaxy, fields):
    ref_counts = Counter(record["kline_time"] for record in reference)
    galaxy_counts = Counter(record["kline_time"] for record in galaxy)
    ref_map = {record["kline_time"]: record for record in reference}
    galaxy_map = {record["kline_time"]: record for record in galaxy}
    missing_galaxy = sorted(set(ref_map) - set(galaxy_map))
    extra_galaxy = sorted(set(galaxy_map) - set(ref_map))
    mismatch_counts = {field: 0 for field in fields}
    mismatch_samples = []
    mismatched_rows = 0
    for key in sorted(set(ref_map) & set(galaxy_map)):
        differences = {}
        for field in fields:
            if ref_map[key][field] != galaxy_map[key][field]:
                mismatch_counts[field] += 1
                differences[field] = {
                    "reference": ref_map[key][field], "galaxy": galaxy_map[key][field],
                    "difference": None if ref_map[key][field] is None or galaxy_map[key][field] is None
                    else galaxy_map[key][field] - ref_map[key][field],
                }
        if differences:
            mismatched_rows += 1
            if len(mismatch_samples) < 8:
                mismatch_samples.append({"kline_time": key, "differences": differences})
    common = len(set(ref_map) & set(galaxy_map))
    return {
        "reference_rows": len(reference), "galaxy_rows": len(galaxy), "common_rows": common,
        "reference_duplicate_keys": sum(n - 1 for n in ref_counts.values()),
        "galaxy_duplicate_keys": sum(n - 1 for n in galaxy_counts.values()),
        "missing_galaxy": len(missing_galaxy), "extra_galaxy": len(extra_galaxy),
        "missing_galaxy_sample": missing_galaxy[:10], "extra_galaxy_sample": extra_galaxy[:10],
        "mismatched_rows": mismatched_rows, "exact_rows": common - mismatched_rows,
        "mismatch_counts": mismatch_counts, "mismatch_samples": mismatch_samples,
        "all_exact": not (missing_galaxy or extra_galaxy or mismatched_rows),
    }


def raw_totals(records):
    return {
        "volume_trade": sum(record["volume_trade"] for record in records),
        "value_trade": sum(record["value_trade"] for record in records),
    }


def reference_daily(records):
    nonzero = [record for record in records if record["volume_trade"] > 0]
    return {
        "open_price": nonzero[0]["open_price"],
        "high_price": max(record["high_price"] for record in nonzero),
        "low_price": min(record["low_price"] for record in nonzero),
        "close_price": nonzero[-1]["close_price"],
    }


def bucket_name(kline_time):
    hhmm = kline_time % 10000
    if hhmm == 930:
        return "0930_opening_bar"
    if hhmm < 1300:
        return "0931_1129_morning"
    if hhmm < 1456:
        return "1300_1455_afternoon"
    return "1456_last_reference_bar"


def bucket_summary(reference, galaxy, fields):
    ref_map = {record["kline_time"]: record for record in reference}
    galaxy_map = {record["kline_time"]: record for record in galaxy}
    result = {}
    for key in sorted(set(ref_map) | set(galaxy_map)):
        bucket = result.setdefault(bucket_name(key), Counter())
        source, target = ref_map.get(key), galaxy_map.get(key)
        if source is None:
            bucket["extra_galaxy"] += 1
            bucket["volume_difference"] += target["volume_trade"]
            bucket["value_difference"] += target["value_trade"]
            continue
        if target is None:
            bucket["missing_galaxy"] += 1
            bucket["volume_difference"] -= source["volume_trade"]
            bucket["value_difference"] -= source["value_trade"]
            continue
        bucket["common_rows"] += 1
        if all(source[field] == target[field] for field in fields):
            bucket["exact_rows"] += 1
        if all(source[field] == target[field] for field in RAW_PRICE_FIELDS):
            bucket["price_exact_rows"] += 1
        if source["volume_trade"] == target["volume_trade"]:
            bucket["volume_exact_rows"] += 1
        if source["value_trade"] == target["value_trade"]:
            bucket["value_exact_rows"] += 1
        bucket["volume_difference"] += target["volume_trade"] - source["volume_trade"]
        bucket["value_difference"] += target["value_trade"] - source["value_trade"]
    return {name: dict(values) for name, values in result.items()}


def shifted_minute(kline_time, offset):
    total = (kline_time % 10000 // 100) * 60 + kline_time % 100 + offset
    return kline_time // 10000 * 10000 + total // 60 * 100 + total % 60


def shifted_exact_counts(reference, galaxy, offset):
    galaxy_map = {record["kline_time"]: record for record in galaxy}
    common = exact = 0
    for record in reference:
        target = galaxy_map.get(shifted_minute(record["kline_time"], offset))
        if target is None:
            continue
        common += 1
        if all(record[field] == target[field] for field in RAW_FIELDS):
            exact += 1
    return {"common_rows": common, "exact_rows": exact}


def profile_authority(parquet_path):
    con = duckdb.connect()
    overall = con.execute(
        "SELECT count(*), count(distinct security_code), min(start_time), max(start_time), "
        "sum(turnover_volume), sum(turnover_value) FROM read_parquet(?)", [str(parquet_path)],
    ).fetchone()
    shape = con.execute(
        "SELECT count(*), count(distinct security_code), count(distinct start_time), "
        "sum(CASE WHEN turnover_volume=0 THEN 1 ELSE 0 END), "
        "sum(open_price IS NULL), sum(high_price IS NULL), sum(low_price IS NULL), sum(close_price IS NULL) "
        "FROM read_parquet(?)", [str(parquet_path)],
    ).fetchone()
    duplicates = con.execute(
        "SELECT count(*) FROM (SELECT security_code,start_time,count(*) n FROM read_parquet(?) "
        "GROUP BY 1,2 HAVING n>1)", [str(parquet_path)],
    ).fetchone()[0]
    consistency = con.execute(
        "SELECT count(*) FROM read_parquet(?) WHERE open_price IS NOT NULL AND "
        "(high_price<low_price OR open_price<low_price OR open_price>high_price OR "
        "close_price<low_price OR close_price>high_price)", [str(parquet_path)],
    ).fetchone()[0]
    prefixes = con.execute(
        "SELECT CASE WHEN security_code LIKE '6%' THEN 'SH' WHEN security_code LIKE '00%' THEN 'SZ00' "
        "WHEN security_code LIKE '30%' THEN 'SZ30' ELSE 'OTHER' END grp, count(distinct security_code) "
        "FROM read_parquet(?) GROUP BY 1 ORDER BY 1", [str(parquet_path)],
    ).fetchall()
    return {
        "declared_by_user": "根据逐笔成交数据聚合的权威分钟行情",
        "rows": overall[0], "securities": overall[1], "first_time": overall[2], "last_time": overall[3],
        "volume_total": overall[4], "amount_total": str(overall[5]),
        "time_slots": shape[2], "zero_volume_rows": shape[3],
        "null_counts": {"open": shape[4], "high": shape[5], "low": shape[6], "close": shape[7]},
        "duplicate_keys": duplicates, "ohlc_consistency_errors": consistency,
        "market_prefixes": dict(prefixes),
        "coverage_note": "仅含沪深样本；零成交分钟 OHLC=NULL；不含 14:57-15:00 收盘竞价时段",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--reference", type=Path, default=ROOT / "20260903-1min.parquet")
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    evidence = args.evidence.resolve()
    reference_path = args.reference.resolve()
    output = (args.output or evidence / "authority-comparisons.json").resolve()
    result = {
        "reference": str(reference_path.relative_to(ROOT)),
        "authority_profile": profile_authority(reference_path),
        "codes": {},
        "aggregate": {},
    }
    aggregate = Counter()
    mismatch_totals = Counter()
    daily_formula_failures = []
    bucket_totals = {}
    opening_totals = Counter()
    continuous_totals = Counter()
    daily_price_match_totals = Counter()
    shift_totals = {offset: Counter() for offset in (-1, 0, 1)}
    difference_rows = []

    for code in CODES:
        reference = authority_records(reference_path, code)
        reference_nonzero = [record for record in reference if record["volume_trade"] > 0]
        reference_zero = [record for record in reference if record["volume_trade"] == 0]
        galaxy0 = response(evidence, f"authority_min1_{code.replace('.', '')}_auto0")["records"]
        galaxy1 = response(evidence, f"authority_min1_{code.replace('.', '')}_auto1")["records"]
        galaxy0_overlap = [record for record in galaxy0 if record["kline_time"] <= REFERENCE_MAX_TIME]
        galaxy0_closing = [record for record in galaxy0 if record["kline_time"] > REFERENCE_MAX_TIME]
        galaxy1_overlap = [record for record in galaxy1 if record["kline_time"] <= REFERENCE_MAX_TIME]
        galaxy1_closing = [record for record in galaxy1 if record["kline_time"] > REFERENCE_MAX_TIME]

        auto0 = compare_records(reference_nonzero, galaxy0_overlap, RAW_FIELDS)
        reference_by_time = {record["kline_time"]: record for record in reference_nonzero}
        galaxy0_by_time = {record["kline_time"]: record for record in galaxy0_overlap}
        for key in sorted(set(reference_by_time) | set(galaxy0_by_time)):
            reference_row = reference_by_time.get(key)
            galaxy_row = galaxy0_by_time.get(key)
            reference_volume = 0 if reference_row is None else reference_row["volume_trade"]
            galaxy_volume = 0 if galaxy_row is None else galaxy_row["volume_trade"]
            if reference_volume != galaxy_volume:
                difference_rows.append({
                    "code": code, "kline_time": key,
                    "reference_volume_shares": reference_volume // 100,
                    "galaxy_volume_shares": galaxy_volume // 100,
                    "galaxy_minus_reference_shares": (galaxy_volume - reference_volume) // 100,
                    "missing_galaxy": reference_row is not None and galaxy_row is None,
                    "extra_galaxy": reference_row is None and galaxy_row is not None,
                })
        reference_nonzero_times = {record["kline_time"] for record in reference_nonzero}
        galaxy1_reference_nonzero = [
            record for record in galaxy1_overlap if record["kline_time"] in reference_nonzero_times
        ]
        # auto=1 的零成交行会补 OHLC，因此值比较仍只针对权威非零分钟。
        auto1 = compare_records(reference_nonzero, galaxy1_reference_nonzero, RAW_FIELDS)
        opening_reference = [record for record in reference_nonzero if record["kline_time"] % 10000 == 930]
        opening_galaxy = [record for record in galaxy0_overlap if record["kline_time"] % 10000 == 930]
        continuous_reference = [record for record in reference_nonzero if record["kline_time"] % 10000 != 930]
        continuous_galaxy = [record for record in galaxy0_overlap if record["kline_time"] % 10000 != 930]
        opening = compare_records(opening_reference, opening_galaxy, RAW_FIELDS)
        continuous = compare_records(continuous_reference, continuous_galaxy, RAW_FIELDS)
        buckets = bucket_summary(reference_nonzero, galaxy0_overlap, RAW_FIELDS)
        for name, values in buckets.items():
            bucket_totals.setdefault(name, Counter()).update(values)
        opening_totals.update({
            "reference_rows": opening["reference_rows"], "galaxy_rows": opening["galaxy_rows"],
            "exact_rows": opening["exact_rows"], "missing_galaxy": opening["missing_galaxy"],
            "extra_galaxy": opening["extra_galaxy"], "mismatched_rows": opening["mismatched_rows"],
        })
        opening_totals["volume_difference"] += (
            raw_totals(opening_galaxy)["volume_trade"] - raw_totals(opening_reference)["volume_trade"]
        )
        opening_totals["value_difference"] += (
            raw_totals(opening_galaxy)["value_trade"] - raw_totals(opening_reference)["value_trade"]
        )
        continuous_totals.update({
            "reference_rows": continuous["reference_rows"], "galaxy_rows": continuous["galaxy_rows"],
            "exact_rows": continuous["exact_rows"], "missing_galaxy": continuous["missing_galaxy"],
            "extra_galaxy": continuous["extra_galaxy"], "mismatched_rows": continuous["mismatched_rows"],
        })
        continuous_totals["volume_difference"] += (
            raw_totals(continuous_galaxy)["volume_trade"] - raw_totals(continuous_reference)["volume_trade"]
        )
        continuous_totals["value_difference"] += (
            raw_totals(continuous_galaxy)["value_trade"] - raw_totals(continuous_reference)["value_trade"]
        )
        for offset in shift_totals:
            counts = shifted_exact_counts(reference_nonzero, galaxy0_overlap, offset)
            shift_totals[offset].update(counts)
        galaxy1_by_time = {record["kline_time"]: record for record in galaxy1_overlap}
        zero_semantics = []
        for record in reference_zero:
            galaxy = galaxy1_by_time.get(record["kline_time"])
            if galaxy is not None:
                zero_semantics.append({
                    "volume_zero": galaxy["volume_trade"] == 0,
                    "amount_zero": galaxy["value_trade"] == 0,
                    "filled_ohlc": any(galaxy[field] is not None for field in RAW_PRICE_FIELDS),
                })
        daily = response(evidence, "authority_day_batch_20260903")["frames"][code]["records"][0]
        ref_total = raw_totals(reference_nonzero)
        overlap_total = raw_totals(galaxy0_overlap)
        closing_total = raw_totals(galaxy0_closing)
        daily_formula = {
            "reference_volume": ref_total["volume_trade"] // 100,
            "reference_amount": ref_total["value_trade"] / 100_000,
            "galaxy_overlap_volume": overlap_total["volume_trade"] // 100,
            "galaxy_overlap_amount": overlap_total["value_trade"] / 100_000,
            "galaxy_closing_rows": len(galaxy0_closing),
            "galaxy_closing_volume": closing_total["volume_trade"] // 100,
            "galaxy_closing_amount": closing_total["value_trade"] / 100_000,
            "galaxy_daily_volume": daily["volume"], "galaxy_daily_amount": daily["amount"],
        }
        volume_formula_ok = (
            daily_formula["galaxy_overlap_volume"] + daily_formula["galaxy_closing_volume"]
            == daily_formula["galaxy_daily_volume"]
        )
        amount_formula_ok = abs(
            daily_formula["galaxy_overlap_amount"] + daily_formula["galaxy_closing_amount"]
            - daily_formula["galaxy_daily_amount"]
        ) <= 0.01
        daily_formula.update({
            "daily_equals_overlap_plus_closing": volume_formula_ok and amount_formula_ok,
            "daily_minus_reference_volume": daily_formula["galaxy_daily_volume"] - daily_formula["reference_volume"],
            "daily_minus_reference_amount": daily_formula["galaxy_daily_amount"] - daily_formula["reference_amount"],
        })
        if not daily_formula["daily_equals_overlap_plus_closing"]:
            daily_formula_failures.append(code)
        reference_day = reference_daily(reference)
        daily_price_matches = {
            field: abs(daily[field.removesuffix("_price")] - reference_day[field] / 1_000_000) < 1e-9
            for field in RAW_PRICE_FIELDS
        }
        daily_price_match_totals.update({field: int(match) for field, match in daily_price_matches.items()})

        result["codes"][code] = {
            "authority": {
                "rows": len(reference), "nonzero_rows": len(reference_nonzero),
                "zero_rows": len(reference_zero), "first": reference[0]["kline_time"],
                "last": reference[-1]["kline_time"],
            },
            "galaxy_auto0_vs_reference_nonzero": auto0,
            "opening_bar_comparison": opening,
            "continuous_comparison_excluding_opening_bar": continuous,
            "time_bucket_summary": buckets,
            "galaxy_auto1_vs_reference_nonzero": auto1,
            "galaxy_auto1_zero_slot_semantics": {
                "reference_zero_rows": len(reference_zero), "checked_rows": len(zero_semantics),
                "volume_zero_rows": sum(item["volume_zero"] for item in zero_semantics),
                "amount_zero_rows": sum(item["amount_zero"] for item in zero_semantics),
                "filled_ohlc_rows": sum(item["filled_ohlc"] for item in zero_semantics),
            },
            "galaxy_closing_not_covered_by_reference": {
                "auto0_rows": len(galaxy0_closing), "auto1_rows": len(galaxy1_closing),
                "times": [record["kline_time"] for record in galaxy0_closing],
            },
            "daily_formula": daily_formula,
            "daily_price_matches": daily_price_matches,
        }
        aggregate.update({
            "authority_nonzero_rows": len(reference_nonzero), "authority_zero_rows": len(reference_zero),
            "auto0_overlap_rows": auto0["galaxy_rows"], "auto0_missing": auto0["missing_galaxy"],
            "auto0_extra": auto0["extra_galaxy"], "auto0_mismatched_rows": auto0["mismatched_rows"],
            "auto0_exact_rows": auto0["exact_rows"],
            "auto1_mismatched_rows": auto1["mismatched_rows"], "auto1_exact_rows": auto1["exact_rows"],
            "reference_volume_trade": ref_total["volume_trade"],
            "reference_value_trade": ref_total["value_trade"],
            "galaxy_overlap_volume_trade": overlap_total["volume_trade"],
            "galaxy_overlap_value_trade": overlap_total["value_trade"],
            "closing_rows": len(galaxy0_closing), "closing_volume": daily_formula["galaxy_closing_volume"],
        })
        mismatch_totals.update(auto0["mismatch_counts"])

    compared = aggregate["authority_nonzero_rows"]
    exact = aggregate["auto0_exact_rows"]
    overlap_volume_difference = aggregate["galaxy_overlap_volume_trade"] - aggregate["reference_volume_trade"]
    overlap_value_difference = aggregate["galaxy_overlap_value_trade"] - aggregate["reference_value_trade"]
    opening_common = opening_totals["exact_rows"] + opening_totals["mismatched_rows"]
    continuous_common = continuous_totals["exact_rows"] + continuous_totals["mismatched_rows"]
    opening_summary = {
        **{key: value for key, value in opening_totals.items()
           if key not in ("volume_difference", "value_difference")},
        "common_rows": opening_common,
        "exact_rate_of_reference_rows": opening_totals["exact_rows"] / opening_totals["reference_rows"],
        "volume_difference_shares": opening_totals["volume_difference"] / 100,
        "amount_difference_yuan": opening_totals["value_difference"] / 100_000,
        "share_of_overlap_volume_difference": (
            opening_totals["volume_difference"] / overlap_volume_difference
            if overlap_volume_difference else None
        ),
    }
    continuous_summary = {
        **{key: value for key, value in continuous_totals.items()
           if key not in ("volume_difference", "value_difference")},
        "common_rows": continuous_common,
        "exact_rate_of_reference_rows": (
            continuous_totals["exact_rows"] / continuous_totals["reference_rows"]
        ),
        "volume_difference_shares": continuous_totals["volume_difference"] / 100,
        "amount_difference_yuan": continuous_totals["value_difference"] / 100_000,
    }
    bucket_output = {}
    for name, values in bucket_totals.items():
        bucket_output[name] = {
            **{key: value for key, value in values.items()
               if key not in ("volume_difference", "value_difference")},
            "volume_difference_shares": values.get("volume_difference", 0) / 100,
            "amount_difference_yuan": values.get("value_difference", 0) / 100_000,
        }
    shift_output = {
        str(offset): {
            **dict(counts),
            "exact_rate_of_authority_nonzero": counts["exact_rows"] / compared if compared else None,
        }
        for offset, counts in shift_totals.items()
    }
    result["aggregate"] = {
        **dict(aggregate),
        "mismatch_field_totals": dict(mismatch_totals),
        "auto0_exact_rate_of_authority_nonzero": exact / compared if compared else None,
        "reference_volume_shares": aggregate["reference_volume_trade"] / 100,
        "reference_amount_yuan": aggregate["reference_value_trade"] / 100_000,
        "galaxy_overlap_volume_shares": aggregate["galaxy_overlap_volume_trade"] / 100,
        "galaxy_overlap_amount_yuan": aggregate["galaxy_overlap_value_trade"] / 100_000,
        "overlap_volume_difference_shares": overlap_volume_difference / 100,
        "overlap_amount_difference_yuan": overlap_value_difference / 100_000,
        "overlap_volume_relative_difference": (
            overlap_volume_difference / aggregate["reference_volume_trade"]
        ),
        "opening_bar_summary": opening_summary,
        "continuous_summary_excluding_opening_bar": continuous_summary,
        "time_bucket_summary": bucket_output,
        "shift_check": shift_output,
        "daily_price_matches_out_of_12": dict(daily_price_match_totals),
        "largest_absolute_volume_differences": sorted(
            difference_rows, key=lambda row: abs(row["galaxy_minus_reference_shares"]), reverse=True,
        )[:15],
        "daily_formula_failures": daily_formula_failures,
        "quality_note": "比较只覆盖权威文件存在的 09:30-14:56 非零分钟；银河 14:57-14:59 另列，不计入权威缺失或银河错误",
    }
    result["evidence_validation"] = validate_evidence(evidence)
    values = dotenv_values(ROOT / ".env")
    secrets = {values.get("AD_USERNAME"), values.get("AD_PASSWORD")} - {None, ""}
    result["reference_desensitization"] = {
        "credential_string_hits": sum(
            secret in reference_path.read_bytes().decode("utf-8", errors="ignore") for secret in secrets
        ),
    }
    write_json(output, result)
    print(json.dumps({
        "output": str(output),
        "authority_nonzero_rows": compared,
        "auto0_exact_rows": exact,
        "auto0_exact_rate": result["aggregate"]["auto0_exact_rate_of_authority_nonzero"],
        "auto0_missing": aggregate["auto0_missing"], "auto0_extra": aggregate["auto0_extra"],
        "mismatch_field_totals": dict(mismatch_totals),
        "daily_formula_failures": daily_formula_failures,
        "evidence_valid": result["evidence_validation"]["all_indexed_hashes_valid"],
        "desensitized": result["evidence_validation"]["desensitization_check_passed"],
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
