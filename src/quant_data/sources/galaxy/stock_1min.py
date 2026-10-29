"""银河分钟与快照交付，以及单一完整研究网格的集中构造。"""

from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo
import re

import polars as pl

from quant_data.core import deps
from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.db import registered, transaction
from quant_data.sources.galaxy.client import GalaxyRequest

PRICE_SCALE = 1_000_000
VOLUME_SCALE = 100
AMOUNT_SCALE = 100_000
TIMING = re.compile(r"^([01][0-9]|2[0-3]):([0-5][0-9])-([01][0-9]|2[0-3]):([0-5][0-9])$")


def _unit(code, day):
    return f"{code}:{day}"


def _raw_integer(value, scale, field):
    """把快照常规单位还原为供应商整数单位，供零容差守恒校验。

    使用十进制文本而非二进制浮点运算；若缩放后不是整数，说明源值不能按
    已确认量纲精确表达，应报错而不是以容差或四舍五入掩盖。
    """
    # 快照高层值已是常规单位；十进制文本还原源整数，避免二进制浮点参与守恒。
    try:
        scaled = Decimal(str(value)) * scale
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid snapshot {field}: {value!r}") from exc
    if scaled != scaled.to_integral_value():
        raise ValueError(f"snapshot {field} cannot be represented in source integer units")
    return int(scaled)


def _timestamp(value):
    """解析快照时间并转换为不带时区的上海本地时间。

    worker 的证据时间戳使用 UTC ISO；竞价锚点的 trade_date 和窄窗判断必须
    先转回 Asia/Shanghai，否则可能把本地 09:25 错归到另一个业务日或时段。
    """
    # 执行层时间戳为 UTC ISO；网格归属前必须转换回市场本地 Asia/Shanghai。
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, int):
        parsed = datetime.strptime(str(value).zfill(17), "%Y%m%d%H%M%S%f")
    else:
        text = str(value)
        parsed = (datetime.strptime(text.zfill(17), "%Y%m%d%H%M%S%f") if text.isdigit()
                  else datetime.fromisoformat(text.replace("Z", "+00:00")))
    if parsed.tzinfo is not None:
        return parsed.astimezone(ZoneInfo("Asia/Shanghai")).replace(tzinfo=None)
    return parsed


class GalaxyStock1MinAdapter:
    """组织银河请求并将原始交付转换为完整分钟 Candidate。

    获取阶段保留 Raw、请求身份和 Unit 影响关系；准备阶段才结合固定上游事实
    生成派生行。一次请求失败只影响其覆盖 Unit，全天停牌 Unit 则无请求本地交付。
    """

    def __init__(self, client):
        self.client = client
        self.dataset = Stock1MinDataset()
        self._responses = {}

    def fetch(self, scope, artifacts):
        """按股票十日分片 K 线、按交易日批量快照，并返回完整 Unit Delivery。

        request_units 决定外部请求，owners 保留请求到 code×date 的影响映射。
        scope.units 中每个 Unit 都有交付状态：无请求的全天停牌为 COMPLETE 且
        request_ids 为空，不能用有行或无行反推获取状态。
        """
        requested = scope.units if scope.request_units is None else scope.request_units
        by_code, by_day = {}, {}
        for unit in requested:
            code, day = unit.rsplit(":", 1)
            by_code.setdefault(code, []).append(day)
            by_day.setdefault(day, []).append(code)
        requests, owners = [], {}
        for code, days in sorted(by_code.items()):
            days = sorted(days)
            for offset in range(0, len(days), 10):
                part = days[offset:offset + 10]
                request_id = f"minute-kline-{code}-{part[0]}-{part[-1]}"
                request = GalaxyRequest(request_id, "kline", (code,), int(part[0].replace("-", "")),
                                        int(part[-1].replace("-", "")), 900, 1700)
                requests.append(request)
                owners[request_id] = tuple(f"{code}:{day}" for day in part)
        for day, codes in sorted(by_day.items()):
            codes = sorted(codes)
            for offset in range(0, len(codes), 200):
                part = tuple(codes[offset:offset + 200])
                request_id = f"minute-snapshot-{day}-{offset // 200 + 1}"
                request = GalaxyRequest(request_id, "snapshot", part, int(day.replace("-", "")),
                                        int(day.replace("-", "")), 92457000, 92530000)
                requests.append(request)
                owners[request_id] = tuple(f"{code}:{day}" for code in part)
        responses, attempts = self.client.fetch(requests, artifacts) if requests else ((), ())
        self._responses = {r.request.request_id: r for r in responses}
        self._owners = owners
        checks = []
        states = {unit: "COMPLETE" for unit in scope.units}
        request_ids = {unit: [] for unit in scope.units}
        for response in responses:
            request_id = response.request.request_id
            affected = owners[request_id]
            for unit in affected:
                request_ids[unit].append(request_id)
                if response.state != "COMPLETE":
                    states[unit] = response.state
            if response.state != "COMPLETE":
                checks.append(Check("ERROR", "source.request", "fetch", affected,
                                    {"request_id": request_id, "error": response.error,
                                     "provider_code": response.provider_code}))
        # 本地全天停牌没有外部请求，但其固定事实交付计划仍为 COMPLETE。
        deliveries = tuple(UnitDelivery(unit, states[unit], tuple(request_ids[unit])) for unit in scope.units)
        frames = []
        for response in responses:
            if not response.rows:
                continue
            kind = response.request.kind
            start, end = response.request.start_date, response.request.end_date
            for row in response.rows:
                item = dict(row)
                item.update(_kind=kind, _request_id=response.request.request_id,
                            _code=response.request.codes[0] if kind == "kline" else None,
                            _start=start, _end=end)
                frames.append(item)
        raw = pl.DataFrame(frames, infer_schema_length=None) if frames else None
        return Delivery(raw, deliveries, attempts, tuple(checks))

    def prepare(self, delivery, scope, con, pins):
        """复核固定上游并集中构造网格、状态和参考价。

        先在与上游读取相同的事务中复核 pins，再解析停牌时段、日线 pre_close
        与 vol、K 线原始整数和竞价锚点。后续 SQL 完成 930 扣减及网格生成；
        任何冲突或未知状态都以 Check 交给统一 Validator，不直接决定发布。
        """
        with transaction(con):
            # 固定版本复核与实际上游读取处于同一事务，不读取前一分钟 Unit。
            deps.recheck(con, pins)
            daily = con.execute("""
                SELECT ts_code,trade_date,pre_close,vol FROM canonical.stock_daily
                WHERE trade_date BETWEEN ?::DATE AND ?::DATE
            """, self._range(scope)).fetchall()
            suspensions = con.execute("""
                SELECT ts_code,trade_date,suspend_type,suspend_timing FROM canonical.suspend_daily
                WHERE trade_date BETWEEN ?::DATE AND ?::DATE
            """, self._range(scope)).fetchall()
        checks = []
        daily_map = {(c, d): (p, v) for c, d, p, v in daily}
        requested_units = scope.units if scope.request_units is None else scope.request_units
        full_suspended = set(scope.units) - set(requested_units)
        intervals, interval_errors = self._intervals(suspensions, full_suspended, set(scope.units))
        checks.extend(interval_errors)
        kline, auction, raw_checks = self._raw_frames(delivery, scope)
        checks.extend(raw_checks)
        requested = scope.units if scope.request_units is None else scope.request_units
        # 快照覆盖按股票日判断；历史不可得与当前批次缺席不能混用。
        snapshot_presence = self._snapshot_presence()
        auction_keys = set(auction["unit_key"].to_list()) if auction is not None else set()
        for unit in requested:
            history_missing = any(r.request.kind == "snapshot" and unit in self._owners[r.request.request_id]
                                  and r.provider_code == -76 for r in self._responses.values())
            if unit not in snapshot_presence and not history_missing:
                checks.append(Check("ERROR", "snapshot.missing", "prepare", (unit,)))
            elif unit not in snapshot_presence:
                checks.append(Check("WARNING", "snapshot.history_unavailable", "prepare", (unit,)))
            elif unit not in auction_keys:
                checks.append(Check("WARNING", "snapshot.zero_auction", "prepare", (unit,)))
        # 在集合关系中完成原始整数核对、930 扣减、完整网格与参考价。
        with registered(con, "_minute_kline_input", kline) as kline_relation, \
             registered(con, "_minute_auction_input", auction) as auction_relation, \
             registered(con, "_minute_interval_input", intervals) as interval_relation:
            con.execute(f"CREATE OR REPLACE TEMP TABLE _minute_kline AS SELECT * FROM {kline_relation}")
            con.execute(f"CREATE OR REPLACE TEMP TABLE _minute_auction AS SELECT * FROM {auction_relation}")
            con.execute(f"CREATE OR REPLACE TEMP TABLE _minute_intervals AS SELECT * FROM {interval_relation}")
        checks.extend(self._source_checks(con, scope, daily_map))
        candidate = self._candidate(con, full_suspended)
        return candidate, tuple(checks)

    def _range(self, scope):
        days = [u.rsplit(":", 1)[1] for u in scope.units]
        return [min(days), max(days)] if days else [date.min, date.max]

    def _intervals(self, rows, full_suspended, known_units):
        """把 S 的单段 HH:MM-HH:MM 文本转为 [start,end) 分钟区间。

        只处理本次发布 Unit；全天停牌由 Scope 另行表达，R 不独立改变区间。
        非当前 membership、未知格式、跨日或逆序时段不得被猜测为可交易状态。
        """
        intervals = []
        checks = []
        for code, day, kind, timing in rows:
            if kind != "S":
                continue
            unit = _unit(code, day.isoformat())
            # 非本次 membership 的停牌正文不能升级为全局错误。
            if unit not in known_units or timing is None or unit in full_suspended:
                continue
            match = TIMING.fullmatch(timing or "")
            if not match:
                checks.append(Check("ERROR", "suspend.timing_format", "prepare", (unit,), {"timing": timing}))
                continue
            start = time(int(match[1]), int(match[2]))
            end = time(int(match[3]), int(match[4]))
            if start >= end:
                checks.append(Check("ERROR", "suspend.timing_range", "prepare", (unit,), {"timing": timing}))
                continue
            intervals.append({"unit_key": unit, "start_time": start, "end_time": end})
        frame = pl.DataFrame(intervals, schema={"unit_key": pl.String, "start_time": pl.Time, "end_time": pl.Time})
        return frame, checks

    def _raw_frames(self, delivery, scope):
        """将 Raw 拆成严格整数 K 线和少量竞价锚点关系。

        K 线保留请求区间以便后续越界检查；快照逐行核对代码、业务日和窄窗，
        取每 Unit 第一根 volume>0 行。错时标不能降格成零竞价，类型错误按
        可定位的请求或 Unit 归因，避免污染整个批次。
        """
        empty_kline = pl.DataFrame(schema={
            "unit_key": pl.String, "kline_time": pl.Int64, "open_price": pl.Int64,
            "high_price": pl.Int64, "low_price": pl.Int64, "close_price": pl.Int64,
            "volume_trade": pl.Int64, "value_trade": pl.Int64,
            "_request_id": pl.String, "_start": pl.Int64, "_end": pl.Int64,
        })
        empty_auction = pl.DataFrame(schema={
            "unit_key": pl.String, "open_price": pl.Int64, "high_price": pl.Int64,
            "low_price": pl.Int64, "close_price": pl.Int64, "volume_trade": pl.Int64, "value_trade": pl.Int64,
        })
        if delivery.frame is None:
            return empty_kline, empty_auction, ()
        checks = []
        kline = delivery.frame.filter(pl.col("_kind") == "kline")
        if kline.height:
            required = {"kline_time", "open_price", "high_price", "low_price", "close_price", "volume_trade", "value_trade"}
            missing = sorted(required - set(kline.columns))
            if missing:
                return empty_kline, empty_auction, (Check("ERROR", "source.fields", "prepare", None, {"missing": missing}),)
            kline = kline.with_columns(
                (pl.col("_code") + ":" + pl.col("kline_time").cast(pl.String).str.slice(0, 4) + "-" +
                 pl.col("kline_time").cast(pl.String).str.slice(4, 2) + "-" +
                 pl.col("kline_time").cast(pl.String).str.slice(6, 2)).alias("unit_key")
            ).select("unit_key", "kline_time", "open_price", "high_price", "low_price", "close_price",
                     "volume_trade", "value_trade", "_request_id", "_start", "_end")
            kline = kline.cast({c: pl.Int64 for c in kline.columns if c not in ("unit_key", "_request_id")})
        else:
            kline = empty_kline
        snapshots = delivery.frame.filter(pl.col("_kind") == "snapshot")
        anchors = []
        if snapshots.height:
            required = {"ts_code", "trade_time", "open", "high", "low", "last", "volume", "amount"}
            missing = sorted(required - set(snapshots.columns))
            if missing:
                return kline, empty_auction, (Check("ERROR", "snapshot.fields", "prepare", None, {"missing": missing}),)
            by_unit = {}
            requested_units = scope.units if scope.request_units is None else scope.request_units
            for row in snapshots.iter_rows(named=True):
                request = self._responses[row["_request_id"]].request
                affected = self._owners[row["_request_id"]]
                try:
                    stamp = _timestamp(row["trade_time"])
                    unit = _unit(row["ts_code"], stamp.date().isoformat())
                    day_number = int(stamp.strftime("%Y%m%d"))
                    time_number = (stamp.hour * 10000000 + stamp.minute * 100000
                                   + stamp.second * 1000 + stamp.microsecond // 1000)
                    if (row["ts_code"] not in request.codes or day_number != request.start_date
                            or day_number != request.end_date or not request.begin_time <= time_number <= request.end_time
                            or unit not in requested_units):
                        checks.append(Check("ERROR", "snapshot.range", "prepare", affected,
                                            {"request_id": request.request_id, "actual_unit": unit}))
                        continue
                    volume = _raw_integer(row["volume"], VOLUME_SCALE, "volume")
                    amount = _raw_integer(row["amount"], AMOUNT_SCALE, "amount")
                    values = tuple(_raw_integer(row[c], PRICE_SCALE, c) for c in ("open", "high", "low", "last"))
                    by_unit.setdefault(unit, []).append((stamp, volume, amount, values))
                except (ValueError, TypeError, InvalidOperation) as exc:
                    checks.append(Check("ERROR", "snapshot.types", "prepare", affected,
                                        {"request_id": request.request_id, "error": str(exc)}))
            for unit, rows in by_unit.items():
                rows.sort(key=lambda x: x[0])
                positive = next((r for r in rows if r[1] > 0), None)
                if positive is not None:
                    _, volume, amount, values = positive
                    anchors.append({"unit_key": unit, "open_price": values[0], "high_price": values[1],
                                    "low_price": values[2], "close_price": values[3],
                                    "volume_trade": volume, "value_trade": amount})
        auction = pl.DataFrame(anchors, schema=empty_auction.schema) if anchors else empty_auction
        return kline, auction, tuple(checks)

    def _snapshot_presence(self):
        # COMPLETE 表示批次中所有 code/date 均已交付；空 DataFrame 是合法零竞价，不等于缺席。
        present = set()
        for response in self._responses.values():
            if response.request.kind == "snapshot" and response.state == "COMPLETE" and response.provider_code != -76:
                present.update(self._owners[response.request.request_id])
        return present

    def _source_checks(self, con, scope, daily_map):
        """在网格构造前拒绝无法归属、非法槽位及跨源冲突的交付。

        除请求闭区间外，还检查实际 code×date 和市场分钟模板，防止错行在 join
        中静默消失。930 先验证竞价守恒和双零/双正边界；全日分钟与日线必须
        同时支持“有成交”，空响应或单边成交不能解释为合法全天 NO_TRADE。
        """
        checks = []
        # 原始 K 线日期和 Unit 必须可由可信请求区间核对。
        bad_range = con.execute("""
            SELECT DISTINCT _request_id FROM _minute_kline
            WHERE kline_time<100000000000 OR kline_time>999999999999
               OR (kline_time/10000)::BIGINT<_start OR (kline_time/10000)::BIGINT>_end
        """).fetchall()
        for request_id, in bad_range:
            checks.append(Check("ERROR", "source.range", "prepare", self._owners[request_id],
                                {"request_id": request_id}))
        requested = scope.units if scope.request_units is None else scope.request_units
        # 请求区间内的错日/休市日及越界分钟也必须报错，不能在网格 join 中静默消失。
        bad_units = con.execute("""
            SELECT DISTINCT _request_id FROM _minute_kline
            WHERE unit_key NOT IN (SELECT unnest(?::VARCHAR[]))
        """, [list(requested)]).fetchall()
        for request_id, in bad_units:
            checks.append(Check("ERROR", "source.unit_range", "prepare", self._owners[request_id],
                                {"request_id": request_id}))
        bad_slots = con.execute("""
            SELECT DISTINCT _request_id FROM _minute_kline
            WHERE (unit_key LIKE '%.BJ:%' AND NOT ((kline_time%10000 BETWEEN 930 AND 1130)
                                               OR (kline_time%10000 BETWEEN 1300 AND 1500)))
               OR (unit_key NOT LIKE '%.BJ:%' AND NOT ((kline_time%10000 BETWEEN 930 AND 1129)
                                                   OR (kline_time%10000 BETWEEN 1300 AND 1459)))
        """).fetchall()
        for request_id, in bad_slots:
            checks.append(Check("ERROR", "source.time_slot", "prepare", self._owners[request_id],
                                {"request_id": request_id}))
        bad_scale = con.execute("""
            SELECT DISTINCT unit_key FROM _minute_kline
            WHERE volume_trade<=0 OR value_trade<=0 OR volume_trade%100<>0
        """).fetchall()
        if bad_scale:
            checks.append(Check("ERROR", "source.volume_scale", "prepare", tuple(r[0] for r in bad_scale)))
        auction_conservation = con.execute("""
            SELECT a.unit_key FROM _minute_auction a
            LEFT JOIN _minute_kline k ON k.unit_key=a.unit_key AND k.kline_time%10000=930
            WHERE k.unit_key IS NULL OR a.volume_trade>k.volume_trade OR a.value_trade>k.value_trade
               OR a.close_price<k.low_price OR a.close_price>k.high_price
        """).fetchall()
        if auction_conservation:
            checks.append(Check("ERROR", "auction.conservation", "prepare", tuple(r[0] for r in auction_conservation)))
        remainder = con.execute("""
            SELECT k.unit_key FROM _minute_kline k JOIN _minute_auction a USING(unit_key)
            WHERE k.kline_time%10000=930 AND ((k.volume_trade-a.volume_trade=0)<>(k.value_trade-a.value_trade=0))
        """).fetchall()
        if remainder:
            checks.append(Check("ERROR", "auction.remainder", "prepare", tuple(r[0] for r in remainder)))
        delivered = {r[0] for r in con.execute("SELECT DISTINCT unit_key FROM _minute_kline").fetchall()}
        requested = scope.units if scope.request_units is None else scope.request_units
        for unit in requested:
            code, day_text = unit.rsplit(":", 1)
            daily = daily_map.get((code, date.fromisoformat(day_text)))
            positive = daily is not None and daily[1] > 0
            has_minutes = unit in delivered
            if (positive and not has_minutes) or (has_minutes and not positive):
                checks.append(Check("ERROR", "source.daily_conflict", "prepare", (unit,)))
            elif not positive and not has_minutes:
                checks.append(Check("ERROR", "source.empty_ambiguous", "prepare", (unit,)))
        return checks

    def _candidate(self, con, full_suspended):
        """以集合 SQL 生成最终完整网格，不逐股逐行拼 Python 对象。

        真实成交与 925 行优先，停牌区间次之，其余槽位为 NO_TRADE。930 只在
        该行扣减；双零时由网格重新生成 NO_TRADE。参考价仅由 pre_close 和
        TRADED close 顺序传递，全天停牌直接取 Scope 决定的本地 Unit。
        """
        # 本地全天停牌直接由 Scope 决定；真实成交优先，其后才是盘中和全天停牌。
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
                FROM u JOIN _minute_auction a USING(unit_key)
            ), adjusted AS (
                SELECT k.unit_key,k.kline_time,
                    strptime((k.kline_time/10000)::BIGINT::VARCHAR,'%Y%m%d')::DATE AS trade_date,
                    k.open_price/1000000.0 AS open,k.high_price/1000000.0 AS high,
                    k.low_price/1000000.0 AS low,k.close_price/1000000.0 AS close,
                    (k.volume_trade-CASE WHEN k.kline_time%10000=930 THEN COALESCE(a.volume_trade,0) ELSE 0 END)/100 AS volume,
                    (k.value_trade-CASE WHEN k.kline_time%10000=930 THEN COALESCE(a.value_trade,0) ELSE 0 END)/100000.0 AS amount,
                    k.kline_time%10000=930 AND a.unit_key IS NOT NULL
                      AND k.volume_trade=a.volume_trade AND k.value_trade=a.value_trade AS drop_930
                FROM _minute_kline k LEFT JOIN _minute_auction a USING(unit_key)
            ), traded AS (
                SELECT unit_key,trade_date,
                    strptime((kline_time/10000)::BIGINT::VARCHAR||lpad((kline_time%10000)::VARCHAR,4,'0'),'%Y%m%d%H%M')::TIME::TIME_NS AS bar_time,
                    open,high,low,close,volume,amount
                FROM adjusted WHERE NOT drop_930 AND volume>0 AND amount>0
                UNION ALL
                SELECT unit_key,split_part(unit_key,':',2)::DATE,TIME '09:25:00'::TIME_NS,
                    open_price/1000000.0,high_price/1000000.0,low_price/1000000.0,close_price/1000000.0,
                    volume_trade/100,value_trade/100000.0
                FROM _minute_auction
            ), statuses AS (
                SELECT s.unit_key,s.code,s.trade_date,s.bar_time,
                    t.open,t.high,t.low,t.close,t.volume,t.amount,
                    (t.unit_key IS NOT NULL) AS traded_row,
                    CASE WHEN t.unit_key IS NOT NULL THEN 'TRADED'
                         WHEN i.unit_key IS NOT NULL OR l.unit_key IS NOT NULL THEN 'SUSPENDED'
                         ELSE 'NO_TRADE' END AS status
                FROM slots s
                LEFT JOIN traded t ON t.unit_key=s.unit_key AND t.trade_date=s.trade_date AND t.bar_time=s.bar_time
                LEFT JOIN _minute_intervals i ON i.unit_key=s.unit_key AND s.bar_time>=i.start_time AND s.bar_time<i.end_time
                LEFT JOIN local_units l ON l.unit_key=s.unit_key
            ), priced AS (
                SELECT s.*,COALESCE(last_value(CASE WHEN traded_row THEN s.close END IGNORE NULLS) OVER (
                        PARTITION BY s.unit_key ORDER BY bar_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ),d.pre_close) AS reference_price
                FROM statuses s LEFT JOIN canonical.stock_daily d ON d.ts_code=s.code AND d.trade_date=s.trade_date
            )
            SELECT code AS ts_code,trade_date,bar_time,status,
                CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                     WHEN traded_row THEN open ELSE reference_price END AS open,
                CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                     WHEN traded_row THEN high ELSE reference_price END AS high,
                CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                     WHEN traded_row THEN low ELSE reference_price END AS low,
                CASE WHEN status='SUSPENDED' OR reference_price IS NULL AND NOT traded_row THEN NULL
                     WHEN traded_row THEN close ELSE reference_price END AS close,
                COALESCE(volume,0)::BIGINT AS volume,COALESCE(amount,0)::DOUBLE AS amount,reference_price
            FROM priced ORDER BY ts_code,trade_date,bar_time
        """, [sorted(full_suspended)]).pl()
