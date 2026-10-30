"""探查脚本（可丢弃实验代码）：银河接口北交所（BSE）分钟行情与快照能力验证。

对应 docs/design/stock_1min_ingestion_proposal.md 第六节未验证项：
1. 分钟时间模板（242 槽位 = 09:30–11:30 + 13:00–15:00）多样本多日验证；越界标签（15:01/15:02/15:30）排查；
2. 历史覆盖与 920 代码迁移（920000.BJ vs 旧代码 832000.BJ 跨 2025-10-09 切换日前后）；
3. 全天停牌行为（suspend_daily 中的 BJ 停牌样本，是否返回 kDataEmpty）；
4. 集合竞价口径（快照窄窗锚点规则是否适用 BJ；930 守恒；收盘竞价并入 15:00 末根的形态）；
6. 日历假设（get_calendar market=BJ vs SH/SZ 全量对比）；
7. 零成交形态（auto_complete=0 下非零分钟占比）。

只读取与本地落盘到 data/probes/，不写入 warehouse / Canonical / Coverage，不修改任何 ingestion 架构。
"""

import argparse
import json
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)

# 模板验证：12 只在市 920 代码（stock_basic 顺序前 12）。
TEMPLATE_CODES = [
    "920000.BJ", "920001.BJ", "920002.BJ", "920003.BJ", "920005.BJ", "920006.BJ",
    "920007.BJ", "920008.BJ", "920009.BJ", "920010.BJ", "920011.BJ", "920012.BJ",
]
TEMPLATE_DATES = [20261023, 20261022, 20261021, 20260903]
# 宽窗口越界标签排查：子集即可（银河对沪深已证实 900–1700 外无 bar，BJ 需独立验证）。
WIDE_CODES = ["920000.BJ", "920001.BJ", "920002.BJ"]
WIDE_DATES = [20261023, 20260903]
# 历史覆盖与 920 迁移：920000.BJ（安徽凤凰，旧代码 832000）跨迁移日 2025-10-09 前后 + 深历史。
HISTORY_PROBES = [
    ("920000.BJ", 20130104), ("920000.BJ", 20201223), ("920000.BJ", 20211115),
    ("920000.BJ", 20211116), ("920000.BJ", 20250930), ("920000.BJ", 20251008),
    ("920000.BJ", 20251009), ("920000.BJ", 20251010),
    ("832000.BJ", 20201223), ("832000.BJ", 20211115), ("832000.BJ", 20250930),
    ("832000.BJ", 20251008), ("832000.BJ", 20251009), ("832000.BJ", 20261023),
]
# 全天停牌样本（suspend_daily：2020-03-09/10 全天 S）+ 对照正常交易日 2020-03-06。
SUSPEND_PROBES = [
    ("832651.BJ", 20200309), ("833394.BJ", 20200309),
    ("834770.BJ", 20200309), ("835857.BJ", 20200309),
    ("832651.BJ", 20200310), ("833394.BJ", 20200306),
]
# 竞价快照：窄窗含 930 连续竞价首分钟（供守恒核对），日期取最近交易日与 20260903。
SNAPSHOT_DATES = [20261023, 20260903]
FULLDAY_SNAPSHOT_CODES = ["920000.BJ", "920001.BJ"]

SUFFIX_MARKET = {"SH": "kSSE", "SZ": "kSZSE", "BJ": "kNEEQ"}


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def credentials():
    """凭据只从本地 .env / 环境变量读取，绝不写盘或打印。"""
    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or os.environ.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or os.environ.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or os.environ.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or os.environ.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required in .env")
    return username, password, host, port


def query_raw_kline(tgw, code, trade_date, period, begin_time, end_time, timeout=60):
    """单股单日原始 K 线（auto_complete=0）；非零状态码照常返回，不抛异常（kDataEmpty 是探查对象）。"""
    suffix = code.rsplit(".", 1)[-1]
    req = tgw.ReqKline()
    req.security_code = code.split(".", 1)[0]
    req.market_type = getattr(tgw.MarketType, SUFFIX_MARKET[suffix])
    req.cq_flag = 0
    req.cq_date = 0
    req.qj_flag = 0
    req.cyc_type = period
    req.cyc_def = 0
    req.auto_complete = 0
    req.begin_date = trade_date
    req.end_date = trade_date
    req.begin_time = begin_time
    req.end_time = end_time
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"] = result
        state["status"] = status
        finished.set()

    accepted, immediate_error = tgw.QueryKline(req, callback, return_df_format=False)
    if accepted is not True:
        return {"code": code, "date": trade_date, "accepted": False,
                "immediate_error": str(immediate_error)}
    if not finished.wait(timeout):
        return {"code": code, "date": trade_date, "accepted": True, "timeout": True}
    status = state.get("status")
    rec = {"code": code, "date": trade_date, "accepted": True,
           "status": status if isinstance(status, int) else None,
           "status_msg": tgw.GetErrorMsg(status) if isinstance(status, int) and status != 0 else None}
    rows = state.get("result") or []
    rec["rows"] = rows
    return rec


def kline_stats(rec):
    """单 (code, date) K 线响应的结构化摘要（BJ 模板：09:30–11:30 + 13:00–15:00 共 242 槽）。"""
    rows = rec.get("rows") or []
    out = {k: v for k, v in rec.items() if k != "rows"}
    out["n_rows"] = len(rows)
    if not rows:
        return out
    labels = sorted(r["kline_time"] % 10000 for r in rows)
    out["first_label"] = labels[0]
    out["last_label"] = labels[-1]
    label_set = set(labels)
    template = set(range(930, 1131)) | set(range(1300, 1501))
    out["has_1130"] = 1130 in label_set
    out["has_1500"] = 1500 in label_set
    out["out_of_template_labels"] = sorted(label_set - template)
    out["zero_volume_rows"] = sum(1 for r in rows if r["volume_trade"] == 0)
    vol = {lab: 0 for lab in (1500, 1501, 1502, 1530)}
    for r in rows:
        lab = r["kline_time"] % 10000
        if lab in vol:
            vol[lab] += r["volume_trade"]
    out["closing_slot_volume_raw"] = vol
    out["total_volume_raw"] = sum(r["volume_trade"] for r in rows)
    return out


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="输出目录，默认 data/probes/galaxy-bj-min1-<当天>/")
    parser.add_argument("--force-login", action="store_true")
    args = parser.parse_args()

    today = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")
    out_dir = (args.output or ROOT / "data/probes" / f"galaxy-bj-min1-{today}").resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"
    if manifest_path.exists():
        raise SystemExit(f"refuse to overwrite existing probe dir: {out_dir}")
    raw_dir = out_dir / "raw"
    raw_dir.mkdir(exist_ok=True)

    username, password, host, port = credentials()
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    import AmazingData as ad
    import tgw
    from AmazingData.login.tgw_login import set_cfg

    manifest = {
        "started_at": timestamp(),
        "source": "galaxy-amazingdata-tgw",
        "probe_goal": "BJ (BSE) minute kline + auction snapshot verification for stock_1min proposal section 6",
        "host": host, "port": port,
        "proxy_environment_removed": removed_proxy,
        "sdk": {"AmazingData": ad.__version__, "tgw_runtime": tgw.GetVersion()},
    }
    work = out_dir / f".work-{int(time.time())}"
    work.mkdir()
    original_cwd = Path.cwd()
    os.chdir(work)

    import pandas as pd

    template_results, wide_results, history_results, suspend_results = [], [], [], []
    snapshot_auction = {}
    snapshot_fullday = {}
    calendar_cmp = {}
    try:
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=args.force_login)
        if not tgw.Login(cfg, api_mode):
            raise RuntimeError("login failed")
        manifest["login"] = True
        calendar = [int(x) for x in ad.BaseData().get_calendar(data_type="str", market="SH", date=20261023)]
        md = ad.MarketData(calendar)
        period = ad.constant.Period.min1.value

        # 第 6 项：BJ vs SH vs SZ 日历全量对比
        base = ad.BaseData()
        cals = {}
        for mkt in ("SH", "SZ", "BJ"):
            try:
                cals[mkt] = sorted(int(x) for x in base.get_calendar(data_type="str", market=mkt, date=20261023))
            except Exception as exc:
                cals[mkt] = f"{type(exc).__name__}: {exc}"
        if all(isinstance(cals[m], list) for m in cals):
            calendar_cmp = {
                "lengths": {m: len(c) for m, c in cals.items()},
                "first": {m: c[0] for m, c in cals.items()},
                "last": {m: c[-1] for m, c in cals.items()},
                "bj_minus_sh": sorted(set(cals["BJ"]) - set(cals["SH"])),
                "sh_minus_bj": sorted(set(cals["SH"]) - set(cals["BJ"])),
                "sz_minus_sh": sorted(set(cals["SZ"]) - set(cals["SH"])),
            }
        else:
            calendar_cmp = {"raw": {m: (c if isinstance(c, str) else f"list[{len(c)}]") for m, c in cals.items()}}
        print("calendar:", json.dumps(calendar_cmp, ensure_ascii=False)[:300], flush=True)

        def run_kline(code, date, bt, et):
            rec = query_raw_kline(tgw, code, date, period, bt, et)
            rows = rec.pop("rows", None)
            stats = kline_stats({**rec, "rows": rows})
            if rows:
                df = pd.DataFrame(rows)
                # 原始行自带裸码 security_code，请求代码另设列避免冲突。
                df.insert(0, "req_code", code)
                df.to_parquet(raw_dir / f"kline_{code.replace('.', '_')}_{date}_bt{bt}.parquet", index=False)
            time.sleep(0.2)
            return stats

        # 第 1/7 项：模板矩阵（12 代码 × 4 日期，09:00–17:00）
        for date in TEMPLATE_DATES:
            for code in TEMPLATE_CODES:
                stats = run_kline(code, date, 900, 1700)
                template_results.append(stats)
                print("template", code, date, "rows:", stats.get("n_rows"), "status:", stats.get("status"), flush=True)

        # 越界标签排查：宽窗口 00:00–23:59
        for date in WIDE_DATES:
            for code in WIDE_CODES:
                stats = run_kline(code, date, 0, 2359)
                wide_results.append(stats)
                print("wide", code, date, "rows:", stats.get("n_rows"), "last:", stats.get("last_label"), flush=True)

        # 第 2 项：历史覆盖与 920 迁移
        for code, date in HISTORY_PROBES:
            stats = run_kline(code, date, 900, 1700)
            history_results.append(stats)
            print("history", code, date, "rows:", stats.get("n_rows"), "status:", stats.get("status"), stats.get("status_msg"), flush=True)

        # 第 3 项：全天停牌样本
        for code, date in SUSPEND_PROBES:
            stats = run_kline(code, date, 900, 1700)
            suspend_results.append(stats)
            print("suspend", code, date, "rows:", stats.get("n_rows"), "status:", stats.get("status"), stats.get("status_msg"), flush=True)

        # 第 4 项：竞价窄窗（含 930 首分钟，供守恒核对）
        for date in SNAPSHOT_DATES:
            try:
                res = md.query_snapshot(list(TEMPLATE_CODES), date, date, begin_time=92457000, end_time=93100000)
            except Exception as exc:
                snapshot_auction[date] = {"error": f"{type(exc).__name__}: {exc}"}
                print("snapshot auction", date, "ERROR", exc, flush=True)
                continue
            per_code = {}
            for dkey, code_map in (res or {}).items():
                for code, df in (code_map or {}).items():
                    if df is None:
                        per_code[code] = None
                        continue
                    df.to_parquet(raw_dir / f"snap_auction_{code.replace('.', '_')}_{date}.parquet", index=False)
                    pre = df[df["trade_time"].dt.strftime("%H%M%S") < "093000"].sort_values("trade_time")
                    anc = pre[pre["volume"] > 0]
                    last930 = df[df["trade_time"] <= df["trade_time"].dt.normalize() + pd.Timedelta(hours=9, minutes=30, seconds=57)]
                    per_code[code] = {
                        "rows": int(len(df)),
                        "anchor_time": str(anc["trade_time"].iloc[0]) if not anc.empty else None,
                        "anchor_volume": float(anc["volume"].iloc[0]) if not anc.empty else None,
                        "anchor_amount": float(anc["amount"].iloc[0]) if not anc.empty else None,
                        "anchor_phase": str(anc["trading_phase_code"].iloc[0]) if not anc.empty else None,
                        "phase_codes": sorted(df["trading_phase_code"].astype(str).unique().tolist()),
                        "cum_at_last_le_093057": float(last930["volume"].iloc[-1]) if not last930.empty else None,
                    }
            snapshot_auction[date] = per_code
            print("snapshot auction", date, "codes:", len(per_code), flush=True)

        # 第 4 项：收盘形态——两只 BJ 全天快照
        for code in FULLDAY_SNAPSHOT_CODES:
            date = 20261023
            try:
                res = md.query_snapshot([code], date, date, begin_time=145500000, end_time=150100000)
            except Exception as exc:
                snapshot_fullday[code] = {"error": f"{type(exc).__name__}: {exc}"}
                continue
            for dkey, code_map in (res or {}).items():
                for c, df in (code_map or {}).items():
                    if df is None:
                        snapshot_fullday[c] = None
                        continue
                    df = df.sort_values("trade_time")
                    df.to_parquet(raw_dir / f"snap_close_{c.replace('.', '_')}_{date}.parquet", index=False)
                    tail = df[["trade_time", "last", "volume", "amount", "num_trades", "trading_phase_code"]].tail(25)
                    snapshot_fullday[c] = {
                        "rows": int(len(df)),
                        "tail": tail.assign(trade_time=tail["trade_time"].astype(str)).to_dict("records"),
                    }
            print("snapshot close", code, "done", flush=True)
    finally:
        try:
            tgw.Close()
            manifest["session_closed"] = True
        except Exception as exc:
            manifest["session_close_error"] = f"{type(exc).__name__}"
        os.chdir(original_cwd)
        import shutil
        shutil.rmtree(work, ignore_errors=True)

    # 930 守恒核对：min1[0930] 量 == 快照 09:30:57 累计量；竞价量 == 锚点累计量
    conservation = []
    kline_by_key = {(r["code"], r["date"]): r for r in template_results}
    for date, per_code in snapshot_auction.items():
        if not isinstance(per_code, dict) or "error" in per_code:
            continue
        for code, snap in per_code.items():
            if not snap:
                continue
            kl = kline_by_key.get((code, date))
            if not kl:
                continue
            raw_path = raw_dir / f"kline_{code.replace('.', '_')}_{date}_bt900.parquet"
            if not raw_path.exists():
                continue
            df = pd.read_parquet(raw_path)
            bar930 = df[df["kline_time"] % 10000 == 930]
            if bar930.empty:
                continue
            bar930_vol = int(bar930["volume_trade"].iloc[0])
            conservation.append({
                "code": code, "date": date,
                "bar930_volume_raw": bar930_vol,
                "snap_cum_093057_shares": snap.get("cum_at_last_le_093057"),
                "auction_volume_shares": snap.get("anchor_volume"),
                "diff_bar930_raw_vs_snap_cum_x100": bar930_vol - (snap["cum_at_last_le_093057"] * 100 if snap.get("cum_at_last_le_093057") is not None else None),
            })

    manifest["calendar_compare"] = calendar_cmp
    manifest["template_results"] = template_results
    manifest["wide_results"] = wide_results
    manifest["history_results"] = history_results
    manifest["suspend_results"] = suspend_results
    manifest["snapshot_auction"] = {str(k): v for k, v in snapshot_auction.items()}
    manifest["snapshot_close_tail"] = snapshot_fullday
    manifest["conservation_930"] = conservation
    manifest["finished_at"] = timestamp()
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    print("wrote manifest", manifest_path, flush=True)


if __name__ == "__main__":
    main()
