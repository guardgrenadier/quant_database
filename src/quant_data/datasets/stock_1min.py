"""沪深北股票完整一分钟研究网格及发布契约。"""

from datetime import date
from pathlib import Path

import polars as pl

from quant_data.core.models import Check, DependencyRef, Scope, ScopeReason
from quant_data.datasets.daily import calendar_dependencies, calendar_scope
from quant_data.db import relation

BSE_START = date(2021, 11, 15)


class Stock1MinDataset:
    """完整分钟研究网格的业务契约，而非银河稀疏行副本。

    每个 Unit 是一只统一代码证券的一个交易日。Canonical 由真实成交行和明确
    的派生规则共同形成，状态与参考价必须逐行可重算；其他 Dataset 不共享这
    一补全许可。
    """

    name, table = "stock_1min", "canonical.stock_1min"
    record_key = ("ts_code", "trade_date", "bar_time")
    schema = pl.Schema({
        "ts_code": pl.String, "trade_date": pl.Date, "bar_time": pl.Time, "status": pl.String,
        "open": pl.Float64, "high": pl.Float64, "low": pl.Float64, "close": pl.Float64,
        "volume": pl.Int64, "amount": pl.Float64,
    })
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        """声明目标区间所需的全部固定上游版本。

        日历依赖覆盖每个自然日；停复牌和日线依赖只覆盖已发布日历确认的开市日，
        避免把没有 Coverage 的休市日误判为缺依赖。该读取与版本固定处于同一事务。
        """
        # source 只记录本地输入身份；parallel 仅用于银河执行，ts_codes 只允许银河显式补数。
        if not {"start_date", "end_date"} <= set(params) <= {"start_date", "end_date", "parallel", "source", "ts_codes"}:
            raise ValueError("stock_1min requires start_date/end_date and optional parallel/source/ts_codes")
        if "parallel" in params and params["parallel"] not in (2, 4):
            raise ValueError("stock_1min parallel must be 2 or 4")
        if "ts_codes" in params:
            codes = params["ts_codes"]
            if (not isinstance(codes, (list, tuple)) or not codes
                    or any(not isinstance(c, str) or not c.strip() for c in codes) or len(set(codes)) != len(codes)):
                raise ValueError("ts_codes must be a nonempty sequence of unique codes")
        if "source" in params:
            if "parallel" in params or "ts_codes" in params or params["start_date"] != params["end_date"]:
                raise ValueError("Local Parquet stock_1min requires one day and does not accept parallel/ts_codes")
            if params["source"].get("type") != "LOCAL_PARQUET":
                raise ValueError("Unsupported stock_1min source")
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

        stock_basic 决定在市日期和统一证券代码；北交所全历史使用当前新代码。
        bse_mapping 只供来源发现旧代码时归一化，不再反向生成历史旧代码 Unit。
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
        # 业务边界：stock_basic 的当前新代码就是 Canonical 全历史代码，不按日期拆分。
        memberships = {}
        for code, exchange, listed, delisted in basics:
            first, last = max(listed, start), min(delisted or date.max, end)
            if last >= first:
                memberships[code] = (first, last)
        days = [date.fromisoformat(x) for x in calendar.units]
        suspended = {(r[0], r[1]) for r in con.execute("""
            SELECT ts_code,trade_date FROM canonical.suspend_daily
            WHERE trade_date BETWEEN ?::DATE AND ?::DATE
              AND suspend_type='S' AND suspend_timing IS NULL
        """, [start, end]).fetchall()}
        resumed = {(r[0], r[1]) for r in con.execute("""
            SELECT ts_code,trade_date FROM canonical.suspend_daily
            WHERE trade_date BETWEEN ?::DATE AND ?::DATE AND suspend_type='R'
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
                # 业务例外：只有 R 同日与正成交日线共同证明可交易时才解除全天停牌；
                # 无日线的 S+R 维持原状，没有 R 的正成交冲突仍为 UNKNOWN。
                resumed_positive = (code, day) in resumed and (code, day) in positive
                if (code, day) in suspended and (code, day) in positive and not resumed_positive:
                    unknown.append(ScopeReason(unit, "suspend_conflict: full-day suspension conflicts with positive stock_daily.vol"))
                    continue
                units.append(unit)
                if (code, day) not in suspended or resumed_positive:
                    requests.append(unit)
        # 休市和日历未知只针对对应 membership 日期展开，不影响其他开市 Unit。
        era_members = tuple(memberships.items())
        excluded = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                         for code, (first, last) in era_members for r in calendar.out_of_scope
                         if first <= date.fromisoformat(r.unit_key) <= last)
        calendar_unknown = tuple(ScopeReason(f"{code}:{r.unit_key}", r.reason)
                                 for code, (first, last) in era_members for r in calendar.unknown
                                 if first <= date.fromisoformat(r.unit_key) <= last)
        resolved = Scope(tuple(sorted(units)), excluded, calendar_unknown + tuple(unknown),
                         request_units=tuple(sorted(requests)))
        if "ts_codes" not in params:
            return resolved
        # 业务补数：先按固定上游解析完整 Scope，再筛选显式代码；未知和未在市代码不得被静默丢弃。
        selected = set(params["ts_codes"])
        outside = sorted(selected - set(memberships))
        if outside:
            raise ValueError(f"Stock codes outside resolved membership: {outside}")
        return Scope(
            tuple(u for u in resolved.units if u.rsplit(":", 1)[0] in selected),
            tuple(r for r in resolved.out_of_scope if r.unit_key.rsplit(":", 1)[0] in selected),
            tuple(r for r in resolved.unknown if r.unit_key.rsplit(":", 1)[0] in selected),
            request_units=tuple(u for u in resolved.request_units if u.rsplit(":", 1)[0] in selected),
        )

    def unit_key_sql(self, alias):
        return f"{alias}.ts_code||':'||strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        """按 Unit 集合验收键、完整槽位、状态字段和参考价派生。

        连续槽位必须与 240/242 模板精确一致，925 只能作为可选 TRADED 行附加。
        Candidate 不保存参考价，但仍从固定日线和 TRADED close 独立重算，
        以验证 NO_TRADE 的 OHLC 构造，不能用相同数值代替来源证明。
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
              (status='TRADED' AND (open IS NULL OR volume<=0 OR amount<=0)) OR
              (status='NO_TRADE' AND (volume<>0 OR amount<>0 OR
                 NOT ((open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
                   OR (open=high AND high=low AND low=close)))) OR
              (status='SUSPENDED' AND (open IS NOT NULL OR high IS NOT NULL OR low IS NOT NULL OR close IS NOT NULL
                                      OR volume<>0 OR amount<>0)) OR
              (open IS NOT NULL AND (NOT isfinite(open) OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close)
                                    OR open<=0 OR high<=0 OR low<=0 OR close<=0 OR high<low
                                    OR open NOT BETWEEN low AND high OR close NOT BETWEEN low AND high))
        """).fetchall()
        if invalid:
            checks.append(Check("ERROR", "stock_1min.status_values", "validation", tuple(r[0] for r in invalid)))
        # 参考价只用于验收 NO_TRADE 的 OHLC；初始值来自固定日线，日内只继承 TRADED close。
        wrong_reference = con.execute(f"""
            WITH ordered AS (
                SELECT c.*,({key}) AS unit_key,
                    last_value(CASE WHEN status='TRADED' THEN close END IGNORE NULLS) OVER (
                        PARTITION BY ({key}) ORDER BY bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS traded_reference
                FROM {table} c
            ), expected AS (
                SELECT o.*,COALESCE(o.traded_reference,d.pre_close) AS expected_reference
                FROM ordered o LEFT JOIN canonical.stock_daily d
                  ON d.ts_code=o.ts_code AND d.trade_date=o.trade_date
            )
            SELECT DISTINCT unit_key FROM expected e
            WHERE status='NO_TRADE' AND (
                (expected_reference IS NULL AND (open IS NOT NULL OR high IS NOT NULL OR low IS NOT NULL OR close IS NOT NULL))
                OR (expected_reference IS NOT NULL AND
                    (open IS DISTINCT FROM expected_reference OR high IS DISTINCT FROM expected_reference
                     OR low IS DISTINCT FROM expected_reference OR close IS DISTINCT FROM expected_reference))
            )
        """).fetchall()
        if wrong_reference:
            checks.append(Check("ERROR", "stock_1min.reference_price", "validation", tuple(r[0] for r in wrong_reference)))
        return tuple(checks)
