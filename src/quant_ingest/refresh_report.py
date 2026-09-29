"""汇总观察期 T+1 重抓的修订发生率。"""

from __future__ import annotations

from datetime import date

import duckdb

from .ledger import Ledger


def summarize_refresh(ledger: Ledger, since: date) -> dict:
    """只统计已完成重抓发布的日子，并按日汇总最大修订规模。"""

    with duckdb.connect(str(ledger.database)) as conn:
        rows = conn.execute(
            """select p.trade_date, max(q.finding_count) as changes
               from plans p join quality_findings q
                 on p.dataset = q.dataset and p.trade_date = q.trade_date
               where p.dataset = 'stk_mins' and p.status = 'refreshed'
                 and q.check_name = 'revision_diff' and p.trade_date >= ?
               group by p.trade_date order by p.trade_date""", [since]
        ).fetchall()
    changed = sum(count > 0 for _, count in rows)
    return {
        "since": since.isoformat(), "observed_days": len(rows),
        "changed_days": changed,
        "changed_day_rate": changed / len(rows) if rows else None,
        "changed_rows_or_keys": sum(count for _, count in rows),
        "days": [{"trade_date": day.isoformat(), "changes": count} for day, count in rows],
    }
