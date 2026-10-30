"""仅用本地文件检查 2026-09-03 银河分钟行情；不导入或调用券商 SDK。"""

import argparse
from collections import Counter
import csv
from datetime import date, datetime
from decimal import Decimal
import hashlib
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

ROOT = Path(__file__).resolve().parents[1]
FIELDS = ("open", "high", "low", "close", "volume", "amount")
SCALES = (1_000_000, 1_000_000, 1_000_000, 1_000_000, 100, 100_000)
DAY = date(2026, 9, 3)


def exact_integer(value, scale):
    """权威 Decimal 转银河整数单位，拒绝舍入，不用浮点容差掩盖差异。"""
    if value is None:
        return None
    result = Decimal(value) * scale
    if result != result.to_integral_value():
        raise ValueError(f"Cannot scale exactly: {value}, {scale}")
    return int(result)


def merge_bars(bars):
    """按发生顺序合并竞价与分钟；无成交的 NULL 价格不参与 OHLC。"""
    active = [bar for bar in bars if bar[4] > 0]
    prices = (active[0][0], max(b[1] for b in active), min(b[2] for b in active), active[-1][3]) if active else (None,) * 4
    return (*prices, sum(b[4] for b in bars), sum(b[5] for b in bars))


def compare(reference, galaxy):
    """零成交权威行只比较量额；补价不同单独记录，不作为成交价错误。"""
    return [name for i, name in enumerate(FIELDS) if (i >= 4 or reference[4] > 0) and reference[i] != galaxy[i]]


def human(value, index):
    return None if value is None else str(Decimal(value) / SCALES[index])


def write_csv(path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def digest(path):
    with path.open("rb") as handle:
        return hashlib.file_digest(handle, "sha256").hexdigest()


def self_check():
    """最小离线检查：合并顺序、合法空价、逐字段差异与严格缩放。"""
    auction = (10, 10, 10, 10, 100, 1000)
    minute = (11, 13, 9, 12, 200, 2300)
    assert merge_bars([auction, minute]) == (10, 13, 9, 12, 300, 3300)
    assert merge_bars([auction, (None, None, None, None, 0, 0)]) == auction
    assert compare((None, None, None, None, 0, 0), (10, 10, 10, 10, 0, 0)) == []
    assert compare(minute, (11, 13, 9, 12, 201, 2300)) == ["volume"]
    assert exact_integer(Decimal("0.0001"), 1_000_000) == 100
    try:
        exact_integer(Decimal("0.00001"), 100)
    except ValueError:
        pass
    else:
        raise AssertionError("Lossy scaling accepted")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    self_check()
    paths = {
        "galaxy": ROOT / "data/downloads/galaxy/20260903_min1_auto1.parquet",
        "manifest": ROOT / "data/downloads/galaxy/20260903_min1_auto1.manifest.json",
        "minute": ROOT / "20260903-1min.parquet",
        "auction": ROOT / "20260903-callauction.parquet",
    }
    hashes = {name: digest(path) for name, path in paths.items()}
    manifest = json.loads(paths["manifest"].read_text())
    codes = sorted(manifest["codes"])
    # 业务范围来自下载清单，保留后缀对应关系，不从全市场权威文件扩大样本。
    if manifest["trade_date"] != 20260903 or manifest["auto_complete"] != 1 or len(codes) != len(set(codes)):
        raise ValueError("Unexpected download scope")
    code_map = {code.split(".")[0]: code for code in codes}
    if len(code_map) != len(codes):
        raise ValueError("Ambiguous bare security codes")
    output = args.output or ROOT / "data/probes" / datetime.now(ZoneInfo("Asia/Shanghai")).strftime("galaxy-local-quality-%Y%m%dT%H%M%S%f")
    output.mkdir(parents=True, exist_ok=False)
    with duckdb.connect() as con:
        galaxy_rows = con.execute(
            "SELECT security_code,kline_time,open_price_raw,high_price_raw,low_price_raw,close_price_raw,"
            "volume_trade_raw,value_trade_raw FROM read_parquet(?) ORDER BY 1,2", [str(paths["galaxy"])]
        ).fetchall()
        sources = {}
        # 只读取十只股票；权威 Decimal 与整数保持精确类型，避免全市场转 Python 对象。
        for name in ("minute", "auction"):
            sources[name] = con.execute(
                "SELECT security_code,trading_day,start_time,open_price,high_price,low_price,close_price,"
                "turnover_volume,turnover_value FROM read_parquet(?) "
                "WHERE security_code IN (SELECT unnest(?)) ORDER BY 1,3",
                [str(paths[name]), list(code_map)],
            ).fetchall()
        nulls = con.execute(
            "SELECT " + ",".join(f'count(*) FILTER (WHERE "{column}" IS NULL)' for column in
                ("security_code", "kline_time", "open_price_raw", "high_price_raw", "low_price_raw", "close_price_raw", "volume_trade_raw", "value_trade_raw"))
            + " FROM read_parquet(?)", [str(paths["galaxy"])]
        ).fetchone()
        # 下载转换质量独立于供应商值质量；原始列副本与便读缩放列逐行核对。
        conversion_errors = {}
        raw_names = ("open_price", "high_price", "low_price", "close_price", "volume_trade", "value_trade")
        for name, raw, scale in zip(FIELDS, raw_names, SCALES):
            conversion_errors[name] = con.execute(
                f'SELECT count(*) FROM read_parquet(?) WHERE "{raw}" IS DISTINCT FROM "{raw}_raw" '
                f'OR "{name}" IS DISTINCT FROM "{raw}_raw" / {scale}', [str(paths["galaxy"])]
            ).fetchone()[0]

    # 键重复必须明确失败，禁止 dict 覆盖重复记录而产生虚假一致。
    keys = [(code, stamp.hour * 100 + stamp.minute) for code, stamp, *_ in galaxy_rows]
    if len(keys) != len(set(keys)) or set(code for code, _ in keys) != set(codes):
        raise ValueError("Duplicate galaxy keys or unexpected securities")
    if any(stamp.date() != DAY or stamp.second or stamp.microsecond for _, stamp, *_ in galaxy_rows) or any(nulls):
        raise ValueError("Invalid galaxy identity or NULL values")
    galaxy = {key: tuple(row[2:]) for key, row in zip(keys, galaxy_rows)}
    reference = {}
    source_checks = {}
    for name, rows in sources.items():
        counts = Counter((code, time) for code, _, time, *_ in rows)
        if any(n != 1 for n in counts.values()) or any(day != DAY for _, day, *_ in rows):
            raise ValueError(f"Invalid authority keys or date: {name}")
        source_checks[name] = {"selected_rows": len(rows), "duplicate_keys": 0, "dates": [str(DAY)]}
        reference[name] = {
            (code_map[code], time): tuple(exact_integer(value, scale) for value, scale in zip(values, SCALES))
            for code, _, time, *values in rows
        }
        bars = list(reference[name].values())
        source_checks[name].update({
            "nonzero_rows": sum(b[4] > 0 for b in bars),
            "ohlc_violations": sum(b[4] > 0 and not (b[2] <= b[0] <= b[1] and b[2] <= b[3] <= b[1]) for b in bars),
            "zero_volume_nonzero_amount": sum(b[4] == 0 and b[5] != 0 for b in bars),
            "vwap_outside_low_high": sum(b[4] > 0 and not (b[2] * b[4] <= b[5] * 1000 <= b[1] * b[4]) for b in bars),
        })

    minute, auctions = reference["minute"], reference["auction"]
    normalized = dict(minute)
    for code in codes:
        # 研究对齐：09:25 竞价在首分钟之前，15:00 竞价映射到银河末分钟。
        # 保留原文件；不假造 14:57/14:58 的权威 OHLC 或零行记录。
        normalized[code, 930] = merge_bars([auctions[code, 925], minute[code, 930]])
        normalized[code, 1459] = auctions[code, 1500]
    sessions = [hour * 100 + m for hour in (9, 10, 11, 13, 14) for m in range(60)
                if 930 <= hour * 100 + m <= 1129 or 1300 <= hour * 100 + m <= 1459]
    expected_keys = {(code, time) for code in codes for time in sessions}
    structural = {
        "rows": len(galaxy), "securities": len(codes), "duplicate_keys": 0,
        "missing_session_keys": sorted(expected_keys - set(galaxy)),
        "extra_session_keys": sorted(set(galaxy) - expected_keys),
        "null_cells_in_required_fields": sum(nulls), "conversion_errors": conversion_errors,
        "ohlc_violations": sum(not (b[2] <= b[0] <= b[1] and b[2] <= b[3] <= b[1]) for b in galaxy.values()),
        "close_above_high": sum(b[3] > b[1] for b in galaxy.values()),
        "close_below_low": sum(b[3] < b[2] for b in galaxy.values()),
        "open_above_high": sum(b[0] > b[1] for b in galaxy.values()),
        "open_below_low": sum(b[0] < b[2] for b in galaxy.values()),
        "high_below_low": sum(b[1] < b[2] for b in galaxy.values()),
        "nonpositive_prices": sum(any(p <= 0 for p in b[:4]) for b in galaxy.values()),
        "negative_volume_or_amount": sum(b[4] < 0 or b[5] < 0 for b in galaxy.values()),
        "fractional_share_rows": sum(b[4] % 100 != 0 for b in galaxy.values()),
        "zero_volume_nonzero_amount": sum(b[4] == 0 and b[5] != 0 for b in galaxy.values()),
        "positive_volume_nonpositive_amount": sum(b[4] > 0 and b[5] <= 0 for b in galaxy.values()),
        "zero_volume_nonflat_prices": sum(b[4] == 0 and len(set(b[:4])) != 1 for b in galaxy.values()),
        "vwap_outside_low_high": sum(b[4] > 0 and not (b[2] * b[4] <= b[5] * 1000 <= b[1] * b[4]) for b in galaxy.values()),
    }
    vwap_anomalies = []
    for (code, time), bar in sorted(galaxy.items()):
        # 诊断而非直接判错：仅当 OHLC 与量额来自同一组成交时，VWAP 才必在高低价内。
        # 快照采样价格与累计量额差分可能不对应同一集合；生成机制尚未证实。
        if bar[4] > 0 and not (bar[2] * bar[4] <= bar[5] * 1000 <= bar[1] * bar[4]):
            volume = Decimal(bar[4]) / 100
            amount = Decimal(bar[5]) / 100_000
            low, high = Decimal(bar[2]) / 1_000_000, Decimal(bar[1]) / 1_000_000
            vwap = amount / volume
            gap = low * volume - amount if vwap < low else amount - high * volume
            vwap_anomalies.append({"code": code, "time": f"{time // 100:02}:{time % 100:02}",
                "low": str(low), "high": str(high), "volume": str(volume), "amount": str(amount),
                "vwap": str(vwap), "outside_price_gap": str(max(low - vwap, vwap - high)),
                "amount_outside_feasible_range": str(gap)})

    comparisons = []
    cumulative = []
    per_code = []
    auction_checks = []
    for code in codes:
        selected = sorted((time, bar) for (c, time), bar in normalized.items() if c == code)
        running = [0, 0]
        last_close = None
        zero_not_previous = 0
        # auto_complete 补价只核对是否沿用银河前一 close，不把补价当作真实成交。
        for (c, _), bar in sorted(galaxy.items()):
            if c != code:
                continue
            if bar[4] == 0 and last_close is not None and bar[:4] != (last_close,) * 4:
                zero_not_previous += 1
            last_close = bar[3]
        for time, ref in selected:
            g = galaxy.get((code, time))
            if g is None:
                raise ValueError(f"Missing comparable galaxy row: {code} {time}")
            differences = compare(ref, g)
            row = {"code": code, "time": f"{time // 100:02}:{time % 100:02}", "reference_nonzero": ref[4] > 0,
                   "galaxy_nonzero": g[4] > 0, "different_fields": ",".join(differences), "comparable_equal": not differences}
            for i, field in enumerate(FIELDS):
                row[f"reference_{field}"] = human(ref[i], i)
                row[f"galaxy_{field}"] = human(g[i], i)
                row[f"delta_{field}"] = human(g[i] - ref[i], i) if ref[i] is not None else None
            comparisons.append(row)
            for i in range(2):
                running[i] += g[i + 4] - ref[i + 4]
            cumulative.append({"code": code, "time": row["time"], "cumulative_volume_delta": human(running[0], 4),
                               "cumulative_amount_delta": human(running[1], 5)})
        rows = [r for r in comparisons if r["code"] == code]
        positive = [r for r in rows if r["reference_nonzero"]]
        zero = [r for r in rows if not r["reference_nonzero"]]
        actual_day = merge_bars([b for (c, _), b in sorted(galaxy.items()) if c == code])
        ref_day = merge_bars([auctions[code, 925], *(b for (c, _), b in sorted(minute.items()) if c == code), auctions[code, 1500]])
        totals = {field: human(actual_day[i] - ref_day[i], i) for i, field in enumerate(FIELDS)}
        counts = {field: sum(field in r["different_fields"].split(",") for r in rows) for field in FIELDS}
        per_code.append({"code": code, "galaxy_rows": sum(c == code for c, _ in galaxy),
            "reference_nonzero_rows": len(positive), "exact_nonzero_rows": sum(r["comparable_equal"] for r in positive),
            "reference_zero_rows": len(zero), "reference_positive_galaxy_zero": sum(not r["galaxy_nonzero"] for r in positive),
            "reference_zero_galaxy_positive": sum(r["galaxy_nonzero"] for r in zero), "mismatch_counts": counts,
            "day_delta": totals, "day_reference": {field: human(ref_day[i], i) for i, field in enumerate(FIELDS)},
            "day_galaxy": {field: human(actual_day[i], i) for i, field in enumerate(FIELDS)},
            "zero_fill_not_previous_close": zero_not_previous,
            "vwap_anomalies": sum(r["code"] == code for r in vwap_anomalies)})
        morning_galaxy = [b for (c, time), b in galaxy.items() if c == code and time < 1200]
        morning_minute = [b for (c, time), b in minute.items() if c == code and time < 1200]
        # 开盘竞价混在首根中，不能用首根减逐笔首分钟假装精确拆出竞价量额。
        # 上午总量减权威连续竞价只证明净量额吻合，不能证明独立竞价分量已精确交付。
        auction_checks.append({"code": code, "close_auction_exact": galaxy[code, 1459] == auctions[code, 1500],
            "open_auction_price_exact": galaxy[code, 930][0] == auctions[code, 925][0],
            "open_auction_reference_price": human(auctions[code, 925][0], 0),
            "open_auction_reference_volume": human(auctions[code, 925][4], 4),
            "open_auction_reference_amount": human(auctions[code, 925][5], 5),
            "open_auction_net_volume_residual": human(sum(b[4] for b in morning_galaxy) - sum(b[4] for b in morning_minute) - auctions[code, 925][4], 4),
            "open_auction_net_amount_residual": human(sum(b[5] for b in morning_galaxy) - sum(b[5] for b in morning_minute) - auctions[code, 925][5], 5),
            "open_auction_separate_volume_amount_verifiable": False,
            "open_merged_different_fields": compare(normalized[code, 930], galaxy[code, 930]),
            "open_merged_volume_delta": human(galaxy[code, 930][4] - normalized[code, 930][4], 4),
            "open_merged_amount_delta": human(galaxy[code, 930][5] - normalized[code, 930][5], 5)})

    # 分段和跨分钟抵消只是定位证据，不据此宣称底层逐笔时间戳或延迟原因已被证明。
    segment_results = []
    for label, begin, end in (("open", 930, 930), ("morning_after_open", 931, 1129),
                               ("afternoon_before_last", 1300, 1455), ("1456", 1456, 1456), ("close", 1459, 1459)):
        keys_in = [key for key in normalized if begin <= key[1] <= end]
        segment_results.append({"segment": label, "rows": len(keys_in),
            "exact_nonzero_rows": sum(normalized[k][4] > 0 and not compare(normalized[k], galaxy[k]) for k in keys_in),
            "reference_nonzero_rows": sum(normalized[k][4] > 0 for k in keys_in),
            "volume_delta": human(sum(galaxy[k][4] - normalized[k][4] for k in keys_in), 4),
            "amount_delta": human(sum(galaxy[k][5] - normalized[k][5] for k in keys_in), 5)})
    bucket_results = []
    for width in (1, 5, 15, 30, 60):
        pairs = {}
        for (code, time), ref in sorted(normalized.items()):
            # 分时聚合不跨午休；竞价已合并到首尾，不采用整体时间平移。
            minutes = (time // 100 * 60 + time % 100) - (570 if time < 1200 else 780)
            key = (code, "am" if time < 1200 else "pm", minutes // width)
            pair = pairs.setdefault(key, ([], []))
            pair[0].append(ref)
            pair[1].append(galaxy[code, time])
        bucket_results.append({"minutes": width, "buckets": len(pairs),
            "exact_ohlcva_buckets": sum(not compare(merge_bars(a), merge_bars(b)) for a, b in pairs.values()),
            "exact_volume_amount_buckets": sum(merge_bars(a)[4:] == merge_bars(b)[4:] for a, b in pairs.values())})
    shift_results = []
    for shift in (-1, 0, 1):
        count = exact = 0
        for (code, time), ref in minute.items():
            # 只用普通连续竞价分钟检验整体错位，排除首分钟集合竞价影响。
            if time == 930 or ref[4] == 0:
                continue
            m = time // 100 * 60 + time % 100 + shift
            target = m // 60 * 100 + m % 60
            if (code, target) not in minute or target == 930:
                continue
            count += 1
            exact += not compare(ref, galaxy[code, target])
        shift_results.append({"galaxy_label_shift_minutes": shift, "paired_nonzero_rows": count, "exact_rows": exact})

    uncovered = [{"code": code, "time": time, "galaxy_volume": human(galaxy[code, time][4], 4),
                  "galaxy_amount": human(galaxy[code, time][5], 5)} for code, time in sorted(set(galaxy) - set(normalized))]
    result = {"date": str(DAY), "codes": codes, "inputs": {name: {"path": str(path.relative_to(ROOT)), "sha256": hashes[name]} for name, path in paths.items()},
        "assessment_scope": "Business sanity and auction accuracy; continuous-minute equality and implied-VWAP bounds are diagnostics, not automatic rejection criteria; 3-second snapshot generation unconfirmed",
        "method": "Exact raw integers; opening auction merged before 09:30, closing auction mapped to 14:59; zero-reference OHLC excluded",
        "structural": structural, "authority_sources": source_checks,
        "comparable_rows": len(comparisons), "reference_nonzero_rows": sum(x["reference_nonzero_rows"] for x in per_code),
        "exact_nonzero_rows": sum(x["exact_nonzero_rows"] for x in per_code),
        "mismatch_counts": {field: sum(x["mismatch_counts"][field] for x in per_code) for field in FIELDS},
        "nonzero_mismatch_counts": {field: sum(r["reference_nonzero"] and field in r["different_fields"].split(",") for r in comparisons) for field in FIELDS},
        "per_code": per_code, "auctions": auction_checks, "segments": segment_results,
        "aggregation": bucket_results, "shift_test": shift_results,
        "vwap_anomalies": vwap_anomalies,
        "authority_uncovered_rows": uncovered,
        "source_hashes_unchanged": all(digest(path) == hashes[name] for name, path in paths.items()), "self_checks_passed": True}
    write_csv(output / "all-comparisons.csv", comparisons)
    write_csv(output / "differences.csv", [r for r in comparisons if not r["comparable_equal"]])
    write_csv(output / "cumulative-deltas.csv", cumulative)
    write_csv(output / "vwap-anomalies.csv", vwap_anomalies)
    (output / "summary.json").write_text(json.dumps(result, ensure_ascii=False, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), **{k: result[k] for k in ("comparable_rows", "reference_nonzero_rows", "exact_nonzero_rows", "mismatch_counts", "segments", "aggregation", "shift_test", "source_hashes_unchanged")}}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
