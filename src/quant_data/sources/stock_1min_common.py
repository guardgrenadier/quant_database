"""银河与本地分钟源共用的停牌区间和完整研究网格构造。"""

from datetime import time
import re

import polars as pl

from quant_data.core.models import Check

TIMING = re.compile(
    r"^([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?"
    r"-([01]?\d|2[0-3]):([0-5]\d)(?::([0-5]\d))?$"
)


def suspension_intervals(rows, full_suspended, known_units):
    """把 S 的多段 H:MM[:SS] 文本规范为完整分钟的 [start,end) 区间并集。"""
    intervals = []
    checks = []
    for code, day, kind, timing in rows:
        if kind != "S":
            continue
        unit = f"{code}:{day.isoformat()}"
        # 非当前 membership 的停牌正文不能升级为全局错误。
        if unit not in known_units or timing is None or unit in full_suspended:
            continue
        ranges = []
        error_rule = None
        for segment in (timing or "").split(","):
            match = TIMING.fullmatch(segment)
            if not match:
                error_rule = "suspend.timing_format"
                break
            start_seconds = int(match[1]) * 3600 + int(match[2]) * 60 + int(match[3] or 0)
            end_seconds = int(match[4]) * 3600 + int(match[5]) * 60 + int(match[6] or 0)
            # 原始跨日、空区间和逆序区间均不能猜测为可交易状态。
            if start_seconds >= end_seconds:
                error_rule = "suspend.timing_range"
                break
            # 秒级边界只保留完全被停牌覆盖的分钟：开始向上、结束向下取整。
            start_minute = (start_seconds + 59) // 60
            end_minute = end_seconds // 60
            if start_minute < end_minute:
                ranges.append((start_minute, end_minute))
        if error_rule:
            checks.append(Check("ERROR", error_rule, "prepare", (unit,), {"timing": timing}))
            continue
        merged = []
        for start, end in sorted(ranges):
            # 多段停牌按时间并集解释；相邻和重叠区间不改变最终槽位集合。
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        for start, end in merged:
            intervals.append({
                "unit_key": unit,
                "start_time": time(start // 60, start % 60),
                "end_time": time(end // 60, end % 60),
            })
    frame = pl.DataFrame(intervals, schema={"unit_key": pl.String, "start_time": pl.Time, "end_time": pl.Time})
    return frame, checks


def build_candidate(con, full_suspended):
    """从标准化成交、停牌区间和固定日线生成完整网格。

    调用方必须先建立 _minute_traded、_minute_intervals 和 _scope_units 临时表。
    _minute_traded 使用最终 Canonical 代码、日期、开始分钟和量额单位；本函数不再
    解释来源、转换时间或执行开盘竞价扣减。
    """
    # 真实成交优先，其后才是盘中和全天停牌；925 仅由实际成交行触发。
    return con.execute("""
        WITH local_units AS (SELECT unnest(?::VARCHAR[]) AS unit_key),
        u AS (
            SELECT unit_key,split_part(unit_key,':',1) AS code,
                   split_part(unit_key,':',2)::DATE AS trade_date
            FROM _scope_units
        ), slots AS (
            SELECT u.unit_key,u.code,u.trade_date,
                CASE WHEN u.code LIKE '%.BJ' THEN
                    CASE WHEN i<=120 THEN TIME '09:30:00'+INTERVAL (i) MINUTE
                         ELSE TIME '13:00:00'+INTERVAL (i-121) MINUTE END
                ELSE
                    CASE WHEN i<120 THEN TIME '09:30:00'+INTERVAL (i) MINUTE
                         ELSE TIME '13:00:00'+INTERVAL (i-120) MINUTE END
                END::TIME_NS AS bar_time
            FROM u JOIN LATERAL (SELECT * FROM range(CASE WHEN u.code LIKE '%.BJ' THEN 242 ELSE 240 END)) r(i) ON true
            UNION ALL
            SELECT u.unit_key,u.code,u.trade_date,TIME '09:25:00'::TIME_NS
            FROM u JOIN (SELECT DISTINCT unit_key FROM _minute_traded WHERE bar_time=TIME '09:25:00') t USING(unit_key)
        ), statuses AS (
            SELECT s.unit_key,s.code,s.trade_date,s.bar_time,
                t.open,t.high,t.low,t.close,t.volume,t.amount,
                (t.unit_key IS NOT NULL) AS traded_row,
                CASE WHEN t.unit_key IS NOT NULL THEN 'TRADED'
                     WHEN i.unit_key IS NOT NULL OR l.unit_key IS NOT NULL THEN 'SUSPENDED'
                     ELSE 'NO_TRADE' END AS status
            FROM slots s
            LEFT JOIN _minute_traded t ON t.unit_key=s.unit_key AND t.trade_date=s.trade_date AND t.bar_time=s.bar_time
            LEFT JOIN _minute_intervals i ON i.unit_key=s.unit_key AND s.bar_time>=i.start_time AND s.bar_time<i.end_time
            LEFT JOIN local_units l ON l.unit_key=s.unit_key
        ), priced AS (
            SELECT s.*,COALESCE(last_value(CASE WHEN traded_row THEN s.close END IGNORE NULLS) OVER (
                    PARTITION BY s.unit_key ORDER BY bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ),d.pre_close) AS reference_price
            FROM statuses s LEFT JOIN canonical.stock_daily d ON d.ts_code=s.code AND d.trade_date=s.trade_date
        )
        -- 内部 JOIN 统一用 TIME_NS；Candidate 输出和 Canonical 存储显式使用 TIME。
        SELECT code AS ts_code,trade_date,bar_time::TIME AS bar_time,status,
            CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                 WHEN traded_row THEN open ELSE reference_price END AS open,
            CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                 WHEN traded_row THEN high ELSE reference_price END AS high,
            CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                 WHEN traded_row THEN low ELSE reference_price END AS low,
            CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                 WHEN traded_row THEN close ELSE reference_price END AS close,
            -- Candidate 不重复保存派生参考价；它仍用于构造 NO_TRADE 的 OHLC。
            COALESCE(volume,0)::BIGINT AS volume,COALESCE(amount,0)::DOUBLE AS amount
        FROM priced ORDER BY ts_code,trade_date,bar_time
    """, [sorted(full_suspended)]).pl()
