"""日线对照的分歧归因分析：读取 daily-compare 证据目录，产出第三方仲裁与口径证据。

可丢弃研究代码。仲裁来源：本地历史分钟文件（第三方，逐分钟 last-trade）、
搜狐历史行情 API（官方发布收盘价的独立转载）。结果写入证据目录 arbitration.json。
"""

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import time
from urllib.request import Request, urlopen

import duckdb

ROOT = Path(__file__).resolve().parents[1]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(out_dir, name):
    return json.loads((out_dir / name).read_text(encoding="utf-8"))


def divergence_by_year(g_rows, t_rows, field="close"):
    """按年统计逐日分歧（1e-6 精度不等即计）。"""
    g = {r["date"]: r for r in g_rows}
    t = {int(r["trade_date"]): r for r in t_rows}
    by_year = defaultdict(list)
    for day in sorted(set(g) & set(t)):
        gv, tv = g[day][field], t[day][field]
        if gv is None or tv is None:
            continue
        if round(gv * 1_000_000) != round(tv * 1_000_000):
            by_year[day // 10000].append({"date": day, "diff": gv - tv})
    return {
        str(year): {
            "count": len(items),
            "max_abs_diff": max(abs(i["diff"]) for i in items),
            "min_abs_diff": min(abs(i["diff"]) for i in items),
            "first_date": items[0]["date"], "last_date": items[-1]["date"],
        }
        for year, items in sorted(by_year.items())
    }


def minute_file_arbitration(g_rows, t_rows, samples_per_year=4):
    """用本地历史分钟文件的当日最后一根 close（最后成交价口径）仲裁分歧日。"""
    g = {r["date"]: r for r in g_rows}
    t = {int(r["trade_date"]): r for r in t_rows}
    div_by_year = defaultdict(list)
    for day in sorted(set(g) & set(t)):
        gv, tv = g[day]["close"], t[day]["close"]
        if gv is not None and tv is not None and round(gv * 100) != round(tv * 100):
            div_by_year[day // 10000].append(day)
    picked = []
    for year in sorted(div_by_year):
        days = div_by_year[year]
        step = max(1, len(days) // samples_per_year)
        picked += days[::step][:samples_per_year]

    con = duckdb.connect()
    results = []
    for day in picked:
        path = ROOT / f"data/raw/stock_1min/year={day // 10000}/{day}.parquet"
        if not path.exists():
            results.append({"date": day, "minute_file": "MISSING"})
            continue
        # 分钟文件为结束分钟标签；当日最大 trade_time 的 close 即最后成交价
        row = con.execute(
            f"SELECT any_value(close), any_value(vol), any_value(amount) FROM read_parquet('{path}') "
            f"WHERE code='600519.SH' AND trade_time=("
            f"SELECT max(trade_time) FROM read_parquet('{path}') WHERE code='600519.SH')"
        ).fetchone()
        minute_close = round(float(row[0]), 2) if row and row[0] is not None else None
        gc, tc = round(g[day]["close"], 2), round(t[day]["close"], 2)
        match = "both" if minute_close == gc == tc else (
            "galaxy" if minute_close == gc else ("tushare" if minute_close == tc else "neither"))
        results.append({"date": day, "galaxy": gc, "tushare": tc,
                        "minute_last_close": minute_close, "minute_matches": match,
                        "minute_file": str(path.relative_to(ROOT)),
                        "minute_file_sha256": sha256(path)})
        time.sleep(0.05)
    tally = defaultdict(int)
    for r in results:
        tally[r.get("minute_matches", "missing")] += 1
    return {"samples": results, "tally": dict(tally)}


def last_minute_vwap_check(days):
    """检验 Tushare close 是否等于旧上交所收盘价规则（最后一分钟成交量加权平均价）。"""
    con = duckdb.connect()
    results = []
    for day in days:
        path = ROOT / f"data/raw/stock_1min/year={day // 10000}/{day}.parquet"
        row = con.execute(
            f"SELECT any_value(close), any_value(vol), any_value(amount) FROM read_parquet('{path}') "
            f"WHERE code='600519.SH' AND trade_time=("
            f"SELECT max(trade_time) FROM read_parquet('{path}') WHERE code='600519.SH')"
        ).fetchone()
        vol, amount = float(row[1]), float(row[2])
        results.append({
            "date": day,
            "minute_last_close": round(float(row[0]), 2),
            "last_minute_vwap": round(amount / vol, 4) if vol else None,
            "minute_file_sha256": sha256(path),
        })
    return results


def sohu_arbitration(cases):
    """搜狐历史行情作为官方收盘价的独立转载；原始响应落盘。"""
    out = {}
    for label, code, day in cases:
        url = f"https://q.stock.sohu.com/hisHq?code={code}&start={day}&end={day}"
        raw = None
        for attempt in range(4):
            try:
                req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
                with urlopen(req, timeout=20) as resp:
                    raw = resp.read().decode()
                if raw.lstrip().startswith("["):
                    break
            except OSError:
                raw = None
            time.sleep(3 + attempt * 2)
        entry = {"url": url, "raw_response": raw}
        try:
            hq = json.loads(raw)[0]["hq"]
            entry["parsed_row"] = hq[0] if hq else None
        except (TypeError, ValueError, IndexError, KeyError):
            entry["parsed_row"] = None
        out[label] = entry
        time.sleep(3)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", type=Path, required=True, help="daily-compare 证据目录")
    args = parser.parse_args()
    out_dir = args.evidence.resolve()

    g5 = load(out_dir, "galaxy_range_600519SH.json")["records"]
    t5 = load(out_dir, "tushare_daily_600519SH.json")["records"]

    arbitration = {"divergence_by_year": {}, "minute_file": {}, "vwap_check": [], "sohu": {}}
    for code, gname, tname in (
        ("600519.SH", "galaxy_range_600519SH.json", "tushare_daily_600519SH.json"),
        ("000300.SH", "galaxy_range_000300SH.json", "tushare_index_daily_000300SH.json"),
        ("000001.SH", "galaxy_range_000001SH.json", "tushare_index_daily_000001SH.json"),
        ("399001.SZ", "galaxy_range_399001SZ.json", "tushare_index_daily_399001SZ.json"),
    ):
        arbitration["divergence_by_year"][code] = divergence_by_year(
            load(out_dir, gname)["records"], load(out_dir, tname)["records"])

    # 600519 分歧日与上交所 2018-08-20 收盘集合竞价改革的边界关系
    div = [d for y, ys in arbitration["divergence_by_year"]["600519.SH"].items() for d in [ys["last_date"]]]
    arbitration["close_rule_boundary"] = {
        "sse_closing_auction_effective": 20180820,
        "600519_last_divergent_date": max(div),
        "600519_divergent_after_20180820": 0 if max(div) < 20180820 else "CHECK",
    }

    arbitration["minute_file"] = minute_file_arbitration(g5, t5)
    arbitration["vwap_check"] = last_minute_vwap_check([20171229, 20180621, 20130104])
    arbitration["sohu"] = sohu_arbitration([
        ("000300.SH:20130104", "zs_000300", "20130104"),
        ("000001.SH:20130104", "zs_000001", "20130104"),
        ("600519.SH:20130104", "cn_600519", "20130104"),
        ("600519.SH:20171229", "cn_600519", "20171229"),
    ])

    path = out_dir / "arbitration.json"
    path.write_text(json.dumps(arbitration, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("written:", path)
    print(json.dumps({k: (v if k != "sohu" else {kk: vv.get("parsed_row") for kk, vv in v.items()})
                      for k, v in arbitration.items()}, ensure_ascii=False, indent=2)[:3000])


if __name__ == "__main__":
    raise SystemExit(main())
