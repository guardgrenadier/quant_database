"""沪深北股票完整一分钟研究网格及发布契约。"""

from datetime import date
from pathlib import Path

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation

MIGRATION_DATE = date(2025, 10, 9)
BSE_START = date(2021, 11, 15)


class Stock1MinDataset:
    """完整分钟研究网格的业务契约，而非银河稀疏行副本。

    每个 Unit 是一只时代代码证券的一个交易日。Canonical 由真实成交行和明确
    的派生规则共同形成，状态与参考价必须逐行可重算；其他 Dataset 不共享这
    一补全许可。
    """

    name, table = "stock_1min", "canonical.stock_1min"
    record_key = ("ts_code", "trade_date", "bar_time")
    schema = pl.Schema({
        "ts_code": pl.String, "trade_date": pl.Date, "bar_time": pl.Time, "status": pl.String,
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
        "volume": pl.Int64, "amount": pl.Float64, "reference_price": pl.Float64,
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        """声明目标区间所需的全部固定上游版本。

        日历依赖覆盖每个自然日；停复牌和日线依赖只覆盖已发布日历确认的开市日，
        避免把没有 Coverage 的休市日误判为缺依赖。该读取与版本固定处于同一事务。
        """
        # 业务边界：分钟只接受显式闭区间；parallel 仅作执行审计，不改变业务范围。
        if not {"start_date", "end_date"} <= set(params) <= {"start_date", "end_date", "parallel"}:
            raise ValueError("stock_1min requires start_date/end_date and optional parallel")
        if "parallel" in params and params["parallel"] not in (2, 4):
            raise ValueError("stock_1min parallel must be 2 or 4")
        refs = list(calendar_dependencies({k: params[k] for k in ("start_date", "end_date")}))
        if params["start_date"] < "20130101":
            raise ValueError("stock_1min history starts at 2013-01-01")
        start, end = params["start_date"], params["end_date"]
        # 依赖派生：只为已发布日历确认的开市日声明稀疏日集合，休市日不要求其 Coverage。
        days = [r[0].strftime("%Y-%m-%d") for r in con.execute("""
            SELECT c.cal_date FROM canonical.trade_calendar c
            JOIN meta.coverage v ON v.dataset='trade_calendar'
              AND v.unit_key='SSE:'||strftime(c.cal_date,'%Y-%m-%d') AND v.status='PUBLISHED'
            WHERE c.exchange='SSE' AND c.is_open AND c.cal_date BETWEEN ?::DATE AND ?::DATE
            ORDER BY c.cal_date
        """, [date.fromisoformat(start), date.fromisoformat(end)]).fetchall()]
        refs.extend(DependencyRef(dataset, day) for dataset in ("suspend_daily", "stock_daily") for day in days)
        refs.extend((DependencyRef("stock_basic", "SNAPSHOT"), DependencyRef("bse_mapping", "SNAPSHOT")))
        return tuple(refs)

    def resolve_scope(self, params, pins, con):
        """同时确定发布 Scope 和银河请求子集。

        stock_basic 决定在市日期，bse_mapping 只决定 BJ 时代代码。无冲突的
        全天停牌 Unit 仍进入 units 以生成本地网格，但不进入 request_units；
        全天停牌与日线正成交量冲突则留 UNKNOWN，不能发布成全天停牌。
        """
        calendar = calendar_scope(pins, con)
        start, end = date.fromisoformat(params["start_date"]), date.fromisoformat(params["end_date"])
        basics = con.execute("""
            SELECT ts_code,exchange,list_date,delist_date FROM canonical.stock_basic
            WHERE exchange IN ('SSE','SZSE','BSE') AND list_date<=?::DATE
              AND (delist_date IS NULL OR delist_date>=?::DATE)
        """, [end, start]).fetchall()
        mappings = dict(con.execute("SELECT n_code,o_code FROM canonical.bse_mapping").fetchall())
        # 业务转换：同一证券按日期使用时代代码；membership 仍来自 stock_basic 的原始在市日期。
        memberships = {}
        for code, exchange, listed, delisted in basics:
            periods = [(code, listed, delisted or date.max)]
            if exchange == "BSE" and code in mappings:
                periods = [(mappings[code], listed, min(delisted or date.max, date(2025, 10, 8))),
                           (code, max(listed, MIGRATION_DATE), delisted or date.max)]
            for era_code, era_start, era_end in periods:
                if era_end < era_start:
                    continue
                memberships.setdefault(era_code, (max(era_start, start), min(era_end, end)))
        days = [date.fromisoformat(x) for x in calendar.units]
        suspended = {(r[0], r[1]) for r in con.execute("""
            SELECT ts_code,trade_date FROM canonical.suspend_daily
            WHERE trade_date BETWEEN ?::DATE AND ?::DATE
              AND suspend_type='S' AND suspend_timing IS NULL
        """, [start, end]).fetchall()}
        positive = {(r[0], r[1]) for r in con.execute("""
            SELECT ts_code,trade_date FROM canonical.stock_daily
            WHERE trade_date BETWEEN ?::DATE AND ?::DATE AND vol>0
        """, [start, end]).fetchall()}
        units, requests, unknown = [], [], []
        for code, (first, last) in memberships.items():
            for day in days:
                if not first <= day <= last:
                    continue
                if code.endswith(".BJ") and day < BSE_START:
                    continue
                unit = f"{code}:{day.isoformat()}"
                if (code, day) in suspended and (code, day) in positive:
                    unknown.append(ScopeReason(unit, "suspend_conflict: full-day suspension conflicts with positive stock_daily.vol"))
                    continue
                units.append(unit)
                if (code, day) not in suspended:
                    requests.append(unit)
        # 休市和日历未知只针对对应 membership 日期展开，不影响其他开市 Unit。
        era_members = tuple(memberships.items())
        excluded = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                         for code, (first, last) in era_members for r in calendar.out_of_scope
                         if first <= date.fromisoformat(r.unit_key) <= last)
        calendar_unknown = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                                 for code, (first, last) in era_members for r in calendar.unknown
                                 if first <= date.fromisoformat(r.unit_key) <= last)
        return Scope(tuple(sorted(units)), excluded, calendar_unknown + tuple(unknown),
                     request_units=tuple(sorted(requests)))

    def unit_key_sql(self, alias):
        return f"{alias}.ts_code||':'||strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """按 Unit 集合验收键、完整槽位、状态字段和参考价序列。

        连续槽位必须与 240/242 模板精确一致，925 只能作为可选 TRADED 行附加。
        参考价由固定日线 pre_close 与 TRADED close 独立重算，不能只凭数值
        与某个历史或未来价格相同就推断来源正确。
        """
        table = relation(candidate_relation)
        key = self.unit_key_sql("c")
        checks = []
        duplicates = con.execute(f"""
            SELECT DISTINCT ({key}) FROM {table} c
            GROUP BY c.ts_code,c.trade_date,c.bar_time HAVING count(*)>1
        """).fetchall()
        if duplicates:
            checks.append(Check("ERROR", "stock_1min.key_unique", "validation", tuple(r[0] for r in duplicates)))
        # 业务网格：只比较连续交易槽位；925 是可选附加成交事实，不能用来填补缺失槽位。
        expected = """
            WITH u AS (
                SELECT unit_key,split_part(unit_key,':',1) AS code
                FROM _scope_units
            ), e AS (
                SELECT u.unit_key,
                    CASE WHEN u.code LIKE '%.BJ' THEN
                        CASE WHEN i<=120 THEN TIME '09:30:00'+INTERVAL (i) MINUTE
                             ELSE TIME '13:00:00'+INTERVAL (i-121) MINUTE END
                    ELSE
                        CASE WHEN i<120 THEN TIME '09:30:00'+INTERVAL (i) MINUTE
                             ELSE TIME '13:00:00'+INTERVAL (i-120) MINUTE END
                    END::TIME_NS AS bar_time
                FROM u JOIN LATERAL (
                    SELECT * FROM range(CASE WHEN u.code LIKE '%.BJ' THEN 242 ELSE 240 END)
                ) r(i) ON true
            )
        """
        grid_units = con.execute(f"""
            {expected}, problems AS (
                SELECT e.unit_key FROM e LEFT JOIN {table} c
                  ON c.ts_code||':'||strftime(c.trade_date,'%Y-%m-%d')=e.unit_key AND c.bar_time=e.bar_time
                WHERE c.ts_code IS NULL
                UNION ALL
                SELECT ({key}) FROM {table} c LEFT JOIN e
                  ON e.unit_key=({key}) AND e.bar_time=c.bar_time
                WHERE c.bar_time<>'09:25:00' AND e.unit_key IS NULL
            ) SELECT DISTINCT unit_key FROM problems
        """).fetchall()
        if grid_units:
            checks.append(Check("ERROR", "stock_1min.grid", "validation", tuple(r[0] for r in grid_units)))
        invalid = con.execute(f"""
            SELECT DISTINCT ({key}) FROM {table} c WHERE
              (bar_time='09:25:00' AND status<>'TRADED') OR
              (status='TRADED' AND (open IS NULL OR volume<=0 OR amount<=0 OR reference_price<>close)) OR
              (status='NO_TRADE' AND (volume<>0 OR amount<>0 OR
                 NOT ((reference_price IS NULL AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
                   OR (reference_price IS NOT NULL AND open=reference_price AND high=reference_price
                       AND low=reference_price AND close=reference_price)))) OR
              (status='SUSPENDED' AND (open IS NOT NULL OR high IS NOT NULL OR low IS NOT NULL OR close IS NOT NULL
                                      OR volume<>0 OR amount<>0)) OR
              (open IS NOT NULL AND (NOT isfinite(open) OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close)
                                    OR open<=0 OR high<=0 OR low<=0 OR close<=0 OR high<low
                                    OR open NOT BETWEEN low AND high OR close NOT BETWEEN low AND high)) OR
              (reference_price IS NOT NULL AND (NOT isfinite(reference_price) OR reference_price<=0))
        """).fetchall()
        if invalid:
            checks.append(Check("ERROR", "stock_1min.status_values", "validation", tuple(r[0] for r in invalid)))
        # 参考价独立重算：只让 TRADED close 参与日内传递，初始值来自固定日线 pre_close。
        wrong_reference = con.execute(f"""
            WITH ordered AS (
                SELECT c.*,({key}) AS unit_key,
                    last_value(CASE WHEN status='TRADED' THEN close END IGNORE NULLS) OVER (
                        PARTITION BY ({key}) ORDER BY bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS traded_reference
                FROM {table} c
            ), expected AS (
                SELECT o.unit_key,o.bar_time,COALESCE(o.traded_reference,d.pre_close) AS expected_reference
                FROM ordered o LEFT JOIN canonical.stock_daily d
                  ON d.ts_code=o.ts_code AND d.trade_date=o.trade_date
            )
            SELECT DISTINCT e.unit_key FROM {table} c
            JOIN expected e ON e.unit_key=({key}) AND e.bar_time=c.bar_time
            WHERE c.reference_price IS DISTINCT FROM e.expected_reference
        """).fetchall()
        if wrong_reference:
            checks.append(Check("ERROR", "stock_1min.reference_price", "validation", tuple(r[0] for r in wrong_reference)))
        return tuple(checks)
