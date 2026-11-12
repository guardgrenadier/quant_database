"""网盘日 Parquet 到 stock_1min 完整研究网格的本地适配器。"""

from datetime import date
import hashlib
import io
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from quant_data.core import deps
from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.core.validation import blocked_units
from quant_data.db import registered, transaction
from quant_data.sources.stock_1min_common import build_candidate, suspension_intervals

MAPPING_PROFILE = "NETDISK_END_MINUTE_V1"
REQUIRED_COLUMNS = (
    "code", "trade_time", "date", "open", "high", "low", "close",
    "vol", "amount", "pre_close", "change", "pct_chg",
)
STRING_COLUMNS = ("code", "trade_time", "date")
FLOAT_COLUMNS = tuple(c for c in REQUIRED_COLUMNS if c not in STRING_COLUMNS)


class LocalParquetStock1MinAdapter:
    """读取一日一文件，执行源专属转换与两组守恒检查。"""

    def __init__(self, source):
        self.source = dict(source)
        self.path = Path(self.source["path"])
        if self.source.get("type") != "LOCAL_PARQUET" or self.source.get("mapping_profile") != MAPPING_PROFILE:
            raise ValueError("Unsupported local stock_1min source or mapping profile")

    @staticmethod
    def inspect(path):
        """在登记 Run 前固定文件身份；fetch 会用实际读取字节再次核对。"""
        path = Path(path)
        if not path.is_file():
            raise ValueError(f"Local Parquet does not exist: {path}")
        with path.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        try:
            rows = pq.read_metadata(str(path)).num_rows
        except Exception as exc:
            raise ValueError(f"Cannot read Parquet metadata: {path}: {exc}") from exc
        return {
            "type": "LOCAL_PARQUET", "path": str(path.resolve()), "sha256": digest,
            "size_bytes": path.stat().st_size, "row_count": rows, "mapping_profile": MAPPING_PROFILE,
        }

    def fetch(self, scope, artifacts):
        """从同一份字节计算哈希并解码 Raw，不产生 HTTP Attempt。"""
        try:
            payload = self.path.read_bytes()
            if len(payload) != self.source["size_bytes"]:
                raise ValueError("Local Parquet size changed after inspection")
            if hashlib.sha256(payload).hexdigest() != self.source["sha256"]:
                raise ValueError("Local Parquet SHA-256 changed after inspection")
            raw = pl.read_parquet(io.BytesIO(payload))
            if raw.height != self.source["row_count"]:
                raise ValueError("Local Parquet row count differs from metadata recorded at inspection")
        except (OSError, ValueError, pl.exceptions.PolarsError) as exc:
            # 文件级故障对所有发布 Unit 一致阻断；request_ids 始终为空。
            check = Check("ERROR", "local.input", "fetch", None, {"path": str(self.path), "error": str(exc)})
            return Delivery(None, tuple(UnitDelivery(u, "FAILED") for u in scope.units), (), (check,))
        # 行号只用于源行去向审计，不进入 Canonical。
        raw = raw.with_row_index("_raw_row_index")
        return Delivery(raw, tuple(UnitDelivery(u, "COMPLETE") for u in scope.units))

    def prepare(self, delivery, scope, con, pins):
        """集中完成代码、时间、竞价转换，以及源行和正量记录守恒检查。"""
        if delivery.frame is None:
            return None, ()
        with transaction(con):
            # 日线、停牌和北交所映射必须来自 Scope 固定的成功版本。
            deps.recheck(con, pins)
            daily = con.execute("""
                SELECT ts_code,trade_date,pre_close,vol FROM canonical.stock_daily
                WHERE trade_date=?::DATE
            """, [self._day(scope)]).fetchall()
            suspensions = con.execute("""
                SELECT ts_code,trade_date,suspend_type,suspend_timing FROM canonical.suspend_daily
                WHERE trade_date=?::DATE
            """, [self._day(scope)]).fetchall()
            mappings = con.execute("SELECT o_code,n_code FROM canonical.bse_mapping").fetchall()
        requested = scope.units if scope.request_units is None else scope.request_units
        full_suspended = set(scope.units) - set(requested)
        intervals, checks = suspension_intervals(suspensions, full_suspended, set(scope.units))
        checks = list(checks)
        raw = delivery.frame
        schema_error = self._schema_error(raw)
        if schema_error:
            checks.append(Check("ERROR", "local.schema", "prepare", None, schema_error))
            return None, tuple(checks)
        if raw.height == 0:
            checks.append(Check("ERROR", "local.empty_file", "prepare", None, {"path": str(self.path)}))
            return None, tuple(checks)

        mapping = pl.DataFrame(mappings, schema={"o_code": pl.String, "n_code": pl.String}, orient="row")
        with registered(con, "_local_raw_input", raw) as raw_relation, \
             registered(con, "_local_bse_input", mapping) as mapping_relation, \
             registered(con, "_minute_interval_input", intervals) as interval_relation:
            con.execute(f"""
                CREATE OR REPLACE TEMP TABLE _local_raw AS
                SELECT _raw_row_index,code,trade_time,date,open,high,low,close,vol,amount
                FROM {raw_relation}
            """)
            con.execute(f"CREATE OR REPLACE TEMP TABLE _local_bse_map AS SELECT * FROM {mapping_relation}")
            con.execute(f"CREATE OR REPLACE TEMP TABLE _minute_intervals AS SELECT * FROM {interval_relation}")

        target_day = date.fromisoformat(self._day(scope))
        self._classify(con, scope, full_suspended, target_day)
        checks.extend(self._classification_checks(con, scope))
        checks.extend(self._daily_and_boundary_checks(con, scope, requested, daily, checks))
        self._create_traded(con)
        candidate = build_candidate(con, full_suspended)
        checks.extend(self._equivalence_check(con, scope, candidate, checks))
        return candidate, tuple(checks)

    @staticmethod
    def _day(scope):
        days = {u.rsplit(":", 1)[1] for u in scope.units}
        if len(days) != 1:
            raise ValueError("Local Parquet stock_1min requires exactly one trade_date per Run")
        return next(iter(days))

    @staticmethod
    def _schema_error(raw):
        columns = set(raw.columns) - {"_raw_row_index", "__index_level_0__"}
        missing = sorted(set(REQUIRED_COLUMNS) - columns)
        extra = sorted(columns - set(REQUIRED_COLUMNS))
        if missing or extra:
            return {"missing": missing, "extra": extra}
        wrong = {c: str(raw.schema[c]) for c in STRING_COLUMNS if raw.schema[c] != pl.String}
        wrong.update({c: str(raw.schema[c]) for c in FLOAT_COLUMNS if raw.schema[c] != pl.Float32})
        return {"wrong_types": wrong} if wrong else None

    def _classify(self, con, scope, full_suspended, target_day):
        """为每条 Raw 行分配唯一去向，并保留可定位的错误原因。"""
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _local_classified AS
            WITH parsed AS (
                SELECT r.*,
                    CASE WHEN regexp_matches(date,'^[0-9]{8}$')
                         THEN try_strptime(date,'%Y%m%d')::DATE END AS trade_date,
                    CASE
                        WHEN regexp_matches(trade_time,'^[0-9]{2}:[0-9]{2}$')
                            THEN try_strptime(trade_time,'%H:%M')::TIME
                        WHEN regexp_matches(trade_time,'^[0-9]{2}:[0-9]{2}:[0-9]{2}$')
                            THEN try_strptime(trade_time,'%H:%M:%S')::TIME
                        WHEN regexp_matches(trade_time,'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
                            THEN try_strptime(trade_time,'%Y-%m-%d %H:%M:%S')::TIME
                    END AS source_time,
                    CASE WHEN regexp_matches(trade_time,'^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
                         THEN try_strptime(trade_time,'%Y-%m-%d %H:%M:%S')::DATE END AS source_time_date,
                    open::DOUBLE AS open,high::DOUBLE AS high,low::DOUBLE AS low,close::DOUBLE AS close,
                    vol::DOUBLE AS vol,amount::DOUBLE AS amount
                FROM _local_raw r
            ), mapped AS (
                SELECT p.*,mn.o_code AS new_o,mn.n_code AS new_n,mo.o_code AS old_o,mo.n_code AS old_n
                FROM parsed p
                LEFT JOIN _local_bse_map mn ON p.code=mn.n_code AND p.code LIKE '%.BJ'
                LEFT JOIN _local_bse_map mo ON p.code=mo.o_code AND p.code LIKE '%.BJ'
            ), identified AS (
                SELECT *,
                    CASE
                        WHEN code NOT LIKE '%.BJ' THEN code
                        WHEN new_n IS NOT NULL THEN new_n
                        WHEN old_o IS NOT NULL THEN old_n
                        ELSE code
                    END AS ts_code,
                    COALESCE(new_n,old_n,code) AS security_code
                FROM mapped
            ), known AS (
                SELECT i.*,b.ts_code IS NOT NULL AS known_security
                FROM identified i LEFT JOIN canonical.stock_basic b ON b.ts_code=i.security_code
            ), attributed AS (
                SELECT *,
                    ts_code||':'||strftime(trade_date,'%Y-%m-%d') AS unit_key,
                    substr(source_time::VARCHAR,1,5) AS label,
                    vol>0 AND amount>0 AS positive,
                    vol=0 AND amount=0 AS zero,
                    code LIKE '%.BJ' AS is_bj
                FROM known
            ), validated AS (
                SELECT *,
                    CASE
                        WHEN trade_date IS NULL OR trade_date<>?::DATE THEN 'local.date'
                        WHEN source_time_date IS NOT NULL AND source_time_date<>trade_date THEN 'local.date'
                        WHEN code IS NULL OR code<>trim(code) OR NOT regexp_matches(code,'^[0-9]{6}\\.(SH|SZ|BJ)$') THEN 'local.code'
                        WHEN source_time IS NULL OR substr(source_time::VARCHAR,7,2)<>'00' THEN 'local.time_format'
                        WHEN vol IS NULL OR NOT isfinite(vol) OR vol<0 OR vol<>trunc(vol) THEN 'local.volume'
                        WHEN amount IS NULL OR NOT isfinite(amount) OR amount<0 THEN 'local.amount'
                        WHEN NOT positive AND NOT zero THEN 'local.volume_amount'
                        WHEN positive AND (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL
                            OR NOT isfinite(open) OR NOT isfinite(high) OR NOT isfinite(low) OR NOT isfinite(close)
                            OR open<=0 OR high<=0 OR low<=0 OR close<=0 OR high<low
                            OR open NOT BETWEEN low AND high OR close NOT BETWEEN low AND high) THEN 'local.ohlc'
                        WHEN ts_code IS NULL THEN 'local.code_era'
                        WHEN unit_key NOT IN (SELECT unit_key FROM _scope_units)
                             AND (positive OR NOT known_security) THEN 'local.scope_conflict'
                        ELSE NULL
                    END AS base_error
                FROM attributed
            ), classified AS (
                SELECT *,
                    CASE
                        WHEN base_error IS NOT NULL THEN 'REJECTED'
                        WHEN is_bj AND NOT (
                            label='09:30' OR source_time BETWEEN TIME '09:31' AND TIME '11:30'
                            OR source_time BETWEEN TIME '13:01' AND TIME '15:30') THEN 'REJECTED'
                        WHEN NOT is_bj AND NOT (
                            label='09:30' OR source_time BETWEEN TIME '09:31' AND TIME '11:30'
                            OR source_time BETWEEN TIME '13:01' AND TIME '15:00') THEN 'REJECTED'
                        WHEN is_bj AND label='15:30' THEN 'EXPLICITLY_EXCLUDED'
                        WHEN is_bj AND source_time BETWEEN TIME '15:03' AND TIME '15:29' AND zero THEN 'EXPLICITLY_EXCLUDED'
                        WHEN is_bj AND source_time BETWEEN TIME '15:03' AND TIME '15:29' THEN 'REJECTED'
                        WHEN unit_key NOT IN (SELECT unit_key FROM _scope_units) AND zero THEN 'EXPLICITLY_EXCLUDED'
                        WHEN unit_key IN (SELECT unnest(?::VARCHAR[])) AND positive THEN 'REJECTED'
                        WHEN unit_key IN (SELECT unnest(?::VARCHAR[])) THEN 'FULL_SUSPENSION_PLACEHOLDER'
                        WHEN positive AND label='09:30' THEN 'OPENING_AUCTION'
                        WHEN positive AND is_bj AND label IN ('15:00','15:01','15:02') THEN 'BSE_CLOSING_TRADED'
                        WHEN positive THEN 'REGULAR_TRADED'
                        ELSE 'ZERO_PLACEHOLDER'
                    END AS disposition,
                    CASE
                        WHEN base_error IS NOT NULL THEN base_error
                        WHEN is_bj AND NOT (
                            label='09:30' OR source_time BETWEEN TIME '09:31' AND TIME '11:30'
                            OR source_time BETWEEN TIME '13:01' AND TIME '15:30') THEN 'local.time_slot'
                        WHEN NOT is_bj AND NOT (
                            label='09:30' OR source_time BETWEEN TIME '09:31' AND TIME '11:30'
                            OR source_time BETWEEN TIME '13:01' AND TIME '15:00') THEN 'local.time_slot'
                        WHEN is_bj AND source_time BETWEEN TIME '15:03' AND TIME '15:29' AND positive THEN 'local.post_close_positive'
                        WHEN unit_key IN (SELECT unnest(?::VARCHAR[])) AND positive THEN 'local.full_suspension_conflict'
                        ELSE NULL
                    END AS error_rule,
                    CASE
                        WHEN base_error IS NULL AND is_bj AND label='15:30' THEN 'BSE_POST_CLOSE_1530'
                        WHEN base_error IS NULL AND is_bj AND source_time BETWEEN TIME '15:03' AND TIME '15:29' AND zero
                            THEN 'BSE_POST_CLOSE_ZERO'
                        WHEN base_error IS NULL AND unit_key NOT IN (SELECT unit_key FROM _scope_units) AND zero
                            THEN 'KNOWN_ZERO_OUT_OF_SCOPE'
                        ELSE NULL
                    END AS exclusion_reason
                FROM validated
            ), closing_counts AS (
                SELECT unit_key,count(*) AS n FROM classified
                WHERE disposition='BSE_CLOSING_TRADED' GROUP BY unit_key
            )
            SELECT c.*,
                CASE WHEN n>1 THEN 'REJECTED' ELSE c.disposition END AS final_disposition,
                CASE WHEN n>1 THEN 'local.close_conflict' ELSE c.error_rule END AS final_error_rule
            FROM classified c LEFT JOIN closing_counts x USING(unit_key)
        """, [target_day, sorted(full_suspended), sorted(full_suspended), sorted(full_suspended)])
        # 后续使用统一字段名，保留原始 disposition 供排查收盘冲突。
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _local_classified AS
            SELECT * REPLACE (final_disposition AS disposition,final_error_rule AS error_rule)
            FROM _local_classified
        """)

    def _classification_checks(self, con, scope):
        """检查源行一对一分类，并把拒绝、重复和显式排除归因到 Unit。"""
        checks = []
        accounting = con.execute("""
            WITH classified_counts AS (
                SELECT _raw_row_index,count(*) AS n FROM _local_classified GROUP BY _raw_row_index
            )
            SELECT count(*) AS raw_rows,
                count(*) FILTER (WHERE c.n IS NULL) AS missing_rows,
                COALESCE(sum(c.n-1) FILTER (WHERE c.n>1),0) AS duplicate_classifications,
                (SELECT count(*) FROM _local_classified
                 WHERE disposition NOT IN ('OPENING_AUCTION','REGULAR_TRADED','BSE_CLOSING_TRADED',
                     'ZERO_PLACEHOLDER','FULL_SUSPENSION_PLACEHOLDER','EXPLICITLY_EXCLUDED','REJECTED')
                    OR disposition='REJECTED' AND error_rule IS NULL
                    OR disposition='EXPLICITLY_EXCLUDED' AND exclusion_reason IS NULL) AS invalid_dispositions
            FROM _local_raw r LEFT JOIN classified_counts c USING(_raw_row_index)
        """).fetchone()
        if any(accounting[1:]):
            checks.append(Check("ERROR", "local.row_accounting", "prepare", None, {
                "raw_rows": accounting[0], "unclassified_rows": accounting[1],
                "duplicate_classifications": accounting[2], "invalid_disposition_rows": accounting[3],
            }))
        rejected = con.execute("""
            SELECT error_rule,unit_key,count(*) FROM _local_classified
            WHERE disposition='REJECTED' GROUP BY error_rule,unit_key
            ORDER BY error_rule,unit_key
        """).fetchall()
        checks.extend(self._grouped_unit_checks("ERROR", rejected, scope))
        duplicates = con.execute("""
            SELECT unit_key,count(*) AS rows FROM _local_classified
            WHERE base_error IS NULL
            GROUP BY unit_key,ts_code,trade_date,source_time HAVING count(*)>1
            ORDER BY unit_key
        """).fetchall()
        if duplicates:
            checks.append(self._unit_check("ERROR", "local.source_key_unique", duplicates, scope))
        era_conflicts = con.execute("""
            SELECT unit_key,count(*) AS rows FROM _local_classified
            WHERE base_error IS NULL AND is_bj
            GROUP BY unit_key HAVING count(DISTINCT code)>1
        """).fetchall()
        if era_conflicts:
            checks.append(self._unit_check("ERROR", "local.code_era_conflict", era_conflicts, scope))
        excluded = con.execute("""
            SELECT unit_key,count(*),count(*) FILTER (WHERE positive),
                   sum(vol),sum(amount) FROM _local_classified
            WHERE exclusion_reason='BSE_POST_CLOSE_1530' GROUP BY unit_key ORDER BY unit_key
        """).fetchall()
        if excluded:
            rows = sum(r[1] for r in excluded)
            check = self._unit_check("WARNING", "local.post_close_excluded", excluded, scope, {
                "rows": rows, "positive_rows": sum(r[2] for r in excluded),
                "volume": sum(r[3] for r in excluded), "amount": sum(r[4] for r in excluded),
            })
            checks.append(check)
        out_of_scope_zero = con.execute("""
            SELECT count(*),count(DISTINCT unit_key) FROM _local_classified
            WHERE exclusion_reason='KNOWN_ZERO_OUT_OF_SCOPE'
        """).fetchone()
        if out_of_scope_zero[0]:
            # 只有固定基础表能解释且量额均为零的未在市记录允许排除；其他越界仍 ERROR。
            checks.append(Check("WARNING", "local.out_of_scope_zero", "prepare", None, {
                "rows": out_of_scope_zero[0], "unit_count": out_of_scope_zero[1],
            }))
        return checks

    @staticmethod
    def _grouped_unit_checks(level, rows, scope):
        grouped = {}
        for rule, unit, count in rows:
            grouped.setdefault(rule, []).append((unit, count))
        return [LocalParquetStock1MinAdapter._unit_check(level, rule, values, scope)
                for rule, values in sorted(grouped.items())]

    @staticmethod
    def _unit_check(level, rule, rows, scope, detail=None):
        units = tuple(sorted({r[0] for r in rows if r[0] is not None}))
        unresolved = sorted({r[0] for r in rows if r[0] is not None and r[0] not in set(scope.units)})
        if any(r[0] is None for r in rows) or unresolved:
            units = None
        data = {"rows": sum(r[-1] for r in rows)} if detail is None else detail
        if unresolved:
            data = {**data, "unresolved_units": unresolved}
        return Check(level, rule, "prepare", units, data)

    def _daily_and_boundary_checks(self, con, scope, requested, daily, existing_checks):
        """只在源记录可解析时判断全日成交及开盘/收盘边界。"""
        daily_map = {(c, d): (p, v) for c, d, p, v in daily}
        ready = set(requested) - blocked_units(scope.units, existing_checks)
        positive = {r[0] for r in con.execute(
            "SELECT DISTINCT unit_key FROM _local_classified WHERE disposition IN "
            "('OPENING_AUCTION','REGULAR_TRADED','BSE_CLOSING_TRADED')"
        ).fetchall()}
        checks = []
        for unit in sorted(ready):
            code, day_text = unit.rsplit(":", 1)
            row = daily_map.get((code, date.fromisoformat(day_text)))
            daily_positive = row is not None and row[1] > 0
            has_positive = unit in positive
            if (daily_positive and not has_positive) or (has_positive and not daily_positive):
                checks.append(Check("ERROR", "local.daily_conflict", "prepare", (unit,)))
            elif not daily_positive and not has_positive:
                checks.append(Check("ERROR", "local.empty_ambiguous", "prepare", (unit,)))
        ready -= blocked_units(scope.units, checks)
        if not ready:
            return checks
        rows = con.execute("""
            SELECT unit_key,label,disposition FROM _local_classified
            WHERE disposition<>'REJECTED' AND (label='09:30' OR label IN ('15:00','15:01','15:02'))
            ORDER BY unit_key,label
        """).fetchall()
        boundaries = {}
        for unit, label, disposition in rows:
            boundaries.setdefault(unit, []).append((label, disposition))
        for unit in sorted(ready):
            records = boundaries.get(unit, [])
            opening = [d for label, d in records if label == "09:30"]
            if "OPENING_AUCTION" not in opening:
                if opening:
                    checks.append(Check("WARNING", "local.zero_auction", "prepare", (unit,)))
                else:
                    checks.append(Check("ERROR", "local.auction_not_reported", "prepare", (unit,)))
            if not unit.split(":", 1)[0].endswith(".BJ"):
                continue
            closing = [d for label, d in records if label in ("15:00", "15:01", "15:02")]
            if "BSE_CLOSING_TRADED" not in closing:
                if closing:
                    checks.append(Check("WARNING", "local.zero_close_auction", "prepare", (unit,)))
                else:
                    checks.append(Check("ERROR", "local.close_not_reported", "prepare", (unit,)))
        return checks

    def _create_traded(self, con):
        """生成统一新代码和开始分钟；本地竞价不参与 930 扣减。"""
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _minute_traded AS
            WITH opening AS (
                SELECT unit_key,trade_date,open,high,low,close,vol,amount
                FROM _local_classified WHERE disposition='OPENING_AUCTION'
            ), regular AS (
                SELECT c.*,
                    CASE WHEN is_bj AND label IN ('15:00','15:01','15:02')
                         THEN TIME '15:00:00'::TIME_NS
                         ELSE (source_time-INTERVAL '1 minute')::TIME_NS END AS bar_time
                FROM _local_classified c
                WHERE disposition IN ('REGULAR_TRADED','BSE_CLOSING_TRADED')
            )
            SELECT r.unit_key,r.trade_date,r.bar_time,
                CASE WHEN r.label='09:31' AND o.unit_key IS NOT NULL THEN o.open ELSE r.open END AS open,
                CASE WHEN r.label='09:31' AND o.unit_key IS NOT NULL THEN greatest(o.high,r.high) ELSE r.high END AS high,
                CASE WHEN r.label='09:31' AND o.unit_key IS NOT NULL THEN least(o.low,r.low) ELSE r.low END AS low,
                r.close,r.vol::BIGINT AS volume,r.amount
            FROM regular r LEFT JOIN opening o ON o.unit_key=r.unit_key AND r.label='09:31'
            UNION ALL
            SELECT unit_key,trade_date,TIME '09:25:00'::TIME_NS,open,high,low,close,vol::BIGINT,amount
            FROM opening
        """)

    def _equivalence_check(self, con, scope, candidate, existing_checks):
        """正量源记录与 Candidate TRADED 九字段双向多重集合对账。"""
        ready = sorted(set(scope.units) - blocked_units(scope.units, existing_checks))
        if not ready:
            return []
        with registered(con, "_local_candidate_input", candidate) as candidate_relation:
            con.execute(f"CREATE OR REPLACE TEMP TABLE _local_candidate AS SELECT * FROM {candidate_relation}")
        fields = "ts_code,trade_date,bar_time,open,high,low,close,volume,amount"
        differences = con.execute(f"""
            WITH expected AS (
                SELECT split_part(unit_key,':',1) AS ts_code,trade_date,bar_time,
                       open,high,low,close,volume,amount
                FROM _minute_traded WHERE unit_key IN (SELECT unnest(?::VARCHAR[]))
            ), actual AS (
                SELECT {fields} FROM _local_candidate
                WHERE status='TRADED'
                  AND ts_code||':'||strftime(trade_date,'%Y-%m-%d') IN (SELECT unnest(?::VARCHAR[]))
            ), expected_only AS (
                SELECT {fields} FROM expected EXCEPT ALL SELECT {fields} FROM actual
            ), actual_only AS (
                SELECT {fields} FROM actual EXCEPT ALL SELECT {fields} FROM expected
            ), differences AS (
                SELECT 'EXPECTED_ONLY' AS side,* FROM expected_only
                UNION ALL
                SELECT 'CANDIDATE_ONLY' AS side,* FROM actual_only
            )
            SELECT side,ts_code||':'||strftime(trade_date,'%Y-%m-%d') AS unit_key,count(*)
            FROM differences GROUP BY side,unit_key ORDER BY unit_key,side
        """, [ready, ready]).fetchall()
        if not differences:
            return []
        units = tuple(sorted({r[1] for r in differences}))
        return [Check("ERROR", "local.traded_equivalence", "prepare", units, {
            "expected_only_rows": sum(r[2] for r in differences if r[0] == "EXPECTED_ONLY"),
            "candidate_only_rows": sum(r[2] for r in differences if r[0] == "CANDIDATE_ONLY"),
        })]
