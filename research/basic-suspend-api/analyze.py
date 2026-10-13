#!/usr/bin/env python3
"""汇总 tmp/ 下证据文件: 行数/字段/状态分布/交易所分布/空值率/唯一代码/重复代码。

用法: python3 analyze.py [文件名前缀过滤, 可多个]
"""
import glob
import json
import os
import sys
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))
TMP = os.path.join(HERE, "tmp")


def load(path):
    with open(path, encoding="utf-8", errors="replace") as f:
        txt = f.read()
    try:
        j = json.loads(txt)
    except Exception:
        return None, txt[:120]
    return j, None


def summarize(name):
    path = os.path.join(TMP, name + ".json")
    j, err = load(path)
    if err is not None:
        print(f"{name}: NON-JSON {err!r}")
        return None
    data = j.get("data") or {}
    fields = data.get("fields") or []
    items = data.get("items") or []
    print(f"{name}: http-body keys={sorted(j.keys())} code={j.get('code')} "
          f"count={j.get('count')} rows={len(items)} fields={len(fields)}")
    if not items:
        return None
    idx = {f: i for i, f in enumerate(fields)}
    out = {"name": name, "fields": fields, "n": len(items)}
    if "ts_code" in idx:
        codes = [r[idx["ts_code"]] for r in items]
        c = Counter(codes)
        dups = {k: v for k, v in c.items() if v > 1}
        out["codes"] = set(codes)
        print(f"  unique ts_code={len(c)} dup_codes={len(dups)}")
        if dups:
            print(f"  dup sample: {dict(list(dups.items())[:5])}")
    for col in ("list_status", "exchange", "market", "suspend_type", "suspend_timing"):
        if col in idx:
            print(f"  {col}: {dict(Counter(r[idx[col]] for r in items))}")
    for col in ("list_date", "delist_date", "trade_date"):
        if col in idx:
            vals = [r[idx[col]] for r in items]
            null = sum(1 for v in vals if v in (None, "", "None"))
            print(f"  {col}: null={null}/{len(vals)} ({null/len(vals)*100:.2f}%)")
    return out


def main():
    pats = sys.argv[1:] or ["*"]
    results = {}
    for p in pats:
        for path in sorted(glob.glob(os.path.join(TMP, p + ".json"))):
            name = os.path.basename(path)[:-5]
            if "_fail" in name:
                continue
            r = summarize(name)
            if r:
                results[name] = r
    # 常用对账
    def codes(n):
        r = results.get(n)
        return r.get("codes", set()) if r else set()
    pairs = [("sb_L_p1", "sb_L_p2")]
    for a, b in pairs:
        if a in results and b in results:
            inter = codes(a) & codes(b)
            print(f"overlap {a} & {b}: {len(inter)}")
    if "sb_L_p1" in results and "sb_L_p2" in results and "sb_all_LDPGUN" in results:
        l_pages = codes("sb_L_p1") | codes("sb_L_p2")
        allc = codes("sb_all_LDPGUN")
        print(f"L pages total={len(l_pages)}; all-status total={len(allc)}; "
              f"L⊆all: {len(l_pages - allc)} missing, extra in all: {len(allc - l_pages)}")


if __name__ == "__main__":
    main()
