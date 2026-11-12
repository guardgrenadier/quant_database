"""银河分钟与快照交付，以及单一完整研究网格的集中构造。"""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import polars as pl

from quant_data.core import deps
from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.core.validation import blocked_units
from quant_data.datasets.stock_1min import Stock1MinDataset
from quant_data.db import registered, transaction
from quant_data.sources.galaxy.client import GalaxyRequest
from quant_data.sources.stock_1min_common import build_candidate, suspension_intervals

PRICE_SCALE = 1_000_000
VOLUME_SCALE = 100
AMOUNT_SCALE = 100_000
BSE_MIGRATION = "2025-10-09"


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
    生成派生行。批量失败经一层单股单日补取后只阻断最终失败 Unit，全天停牌
    Unit 则无请求本地交付；熔断和会话级故障仍按原影响范围处理。
    """

    def __init__(self, client, bse_mapping=None):
        self.client = client
        self.dataset = Stock1MinDataset()
        # Canonical 始终使用 n_code；仅银河改代码前的请求路由使用 o_code。
        self._bse_mapping = dict(bse_mapping or {})
        self._bse_current = {old: new for new, old in self._bse_mapping.items()}
        self._responses = {}

    def fetch(self, scope, artifacts):
        """批量优先获取，失败时以一层单股单日补取确定最终 Unit 交付。

        父、子请求都保留审计，但 prepare 只消费最终响应；被替换父请求的部分行
        不与补取行重复进入 Raw。熔断和会话级故障不启动额外补取会话。
        """
        requested = scope.units if scope.request_units is None else scope.request_units
        requests, owners = self._request_plan(requested)
        primary, primary_attempts = self.client.fetch(requests, artifacts) if requests else ((), ())
        retry_requests, retry_owners, retry_parents = self._retry_plan(primary, owners)
        retries, retry_attempts = self.client.fetch(
            retry_requests, artifacts, allow_unrelated_evidence=True,
        ) if retry_requests else ((), ())

        # 最终响应只包括未被替换的父响应和补取子响应，准备阶段不会误用父批次 partial Raw。
        retried_parents = set(retry_parents.values())
        responses = tuple(r for r in primary if r.request.request_id not in retried_parents) + retries
        attempts = tuple(primary_attempts) + tuple(retry_attempts)
        self._responses = {r.request.request_id: r for r in responses}
        self._owners = {r.request.request_id: owners[r.request.request_id]
                        for r in primary if r.request.request_id not in retried_parents}
        self._owners.update(retry_owners)

        states = {unit: "COMPLETE" for unit in scope.units}
        request_ids = {unit: [] for unit in scope.units}
        for response in primary:
            for unit in owners[response.request.request_id]:
                request_ids[unit].append(response.request.request_id)
        for response in retries:
            for unit in retry_owners[response.request.request_id]:
                request_ids[unit].append(response.request.request_id)

        checks = []
        primary_by_id = {r.request.request_id: r for r in primary}
        for parent_id in sorted(retried_parents):
            parent = primary_by_id[parent_id]
            retry_count = sum(value == parent_id for value in retry_parents.values())
            # 父请求失败只作补取审计；最终失败由对应子请求单独阻断，成功补取不残留父级 ERROR。
            checks.append(Check("WARNING", "source.request_retry", "fetch", owners[parent_id], {
                "request_id": parent_id, "error": parent.error,
                "provider_code": parent.provider_code, "retry_requests": retry_count,
            }))
        for response in responses:
            request_id = response.request.request_id
            affected = self._owners[request_id]
            if response.state == "COMPLETE":
                continue
            for unit in affected:
                states[unit] = response.state
            detail = {"request_id": request_id, "error": response.error,
                      "provider_code": response.provider_code}
            if request_id in retry_parents:
                detail["retry_of"] = retry_parents[request_id]
            checks.append(Check("ERROR", "source.request", "fetch", affected, detail))

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

    def _request_code(self, code, day):
        """Canonical 新代码在银河改代码前历史请求中路由为旧代码。"""
        if code.endswith(".BJ") and day < BSE_MIGRATION:
            return self._bse_mapping.get(code, code)
        return code

    def _request_plan(self, requested):
        """保持 K 线一股最多十日、快照一日最多二百股；owners 始终用新代码。"""
        by_code, by_day = {}, {}
        for unit in requested:
            code, day = unit.rsplit(":", 1)
            by_code.setdefault(code, []).append(day)
            by_day.setdefault(day, []).append(code)
        requests, owners = [], {}
        for code, days in sorted(by_code.items()):
            # 跨迁移日的同一 Canonical 代码必须按实际请求代码分段，不能混在一个 K 线请求。
            segments = {}
            for day in sorted(days):
                segments.setdefault(self._request_code(code, day), []).append(day)
            for request_code, era_days in sorted(segments.items()):
                for offset in range(0, len(era_days), 10):
                    part = era_days[offset:offset + 10]
                    request_id = f"minute-kline-{request_code}-{part[0]}-{part[-1]}"
                    request = GalaxyRequest(request_id, "kline", (request_code,), int(part[0].replace("-", "")),
                                            int(part[-1].replace("-", "")), 900, 1700)
                    requests.append(request)
                    owners[request_id] = tuple(f"{code}:{day}" for day in part)
        for day, codes in sorted(by_day.items()):
            codes = sorted(codes)
            for offset in range(0, len(codes), 200):
                part = tuple(codes[offset:offset + 200])
                request_codes = tuple(self._request_code(code, day) for code in part)
                request_id = f"minute-snapshot-{day}-{offset // 200 + 1}"
                request = GalaxyRequest(request_id, "snapshot", request_codes, int(day.replace("-", "")),
                                        int(day.replace("-", "")), 92457000, 92530000)
                requests.append(request)
                owners[request_id] = tuple(f"{code}:{day}" for code in part)
        return requests, owners

    @staticmethod
    def _retry_plan(responses, owners):
        """仅在主波次没有全局故障时，为可补取失败构造一层股票日请求。"""
        # 不可补取响应表示熔断、会话或 worker 保护已生效，不得用新会话绕过。
        if any(r.state != "COMPLETE" and not r.unit_retryable for r in responses):
            return (), {}, {}
        requests, retry_owners, retry_parents = [], {}, {}
        for response in responses:
            if response.state == "COMPLETE":
                continue
            parent = response.request
            for position, unit in enumerate(owners[parent.request_id]):
                code, day = unit.rsplit(":", 1)
                day_number = int(day.replace("-", ""))
                suffix = day if parent.kind == "kline" else code
                request_id = f"{parent.request_id}-retry-{suffix}"
                # 补取沿用父请求的实际时代代码，但 Unit 归属始终保持 Canonical 新代码。
                request_code = parent.codes[0] if parent.kind == "kline" else parent.codes[position]
                request = GalaxyRequest(request_id, parent.kind, (request_code,), day_number, day_number,
                                        parent.begin_time, parent.end_time)
                requests.append(request)
                retry_owners[request_id] = (unit,)
                retry_parents[request_id] = parent.request_id
        return requests, retry_owners, retry_parents

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
        intervals, interval_errors = suspension_intervals(suspensions, full_suspended, set(scope.units))
        checks.extend(interval_errors)
        kline, auction, raw_checks = self._raw_frames(delivery, scope)
        checks.extend(raw_checks)
        # 两类输入分别确认前提：一类获取失败不能停止另一类已返回记录的独立检查。
        ready = {"kline": set(), "snapshot": set()}
        history_unavailable = set()
        for response in self._responses.values():
            if response.state == "COMPLETE":
                affected = self._owners[response.request.request_id]
                ready[response.request.kind].update(affected)
                if response.request.kind == "snapshot" and response.provider_code == -76:
                    history_unavailable.update(affected)
        for kind, prefix in (("kline", "source."), ("snapshot", "snapshot.")):
            # Raw 解析仅产生 source.* 或 snapshot.* 诊断；排除对应输入的解析失败范围。
            errors = tuple(c for c in raw_checks if c.rule.startswith(prefix))
            ready[kind].difference_update(blocked_units(scope.units, errors))
        # 快照缺失由获取阶段的 source.request 表达；完整输入才解释零竞价或历史不可得。
        auction_keys = set(auction["unit_key"].to_list())
        for unit in sorted(ready["snapshot"]):
            if unit in history_unavailable:
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
        checks.extend(self._source_checks(con, scope, daily_map, ready))
        candidate = self._candidate(con, full_suspended)
        return candidate, tuple(checks)

    def _range(self, scope):
        days = [u.rsplit(":", 1)[1] for u in scope.units]
        return [min(days), max(days)] if days else [date.min, date.max]

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
                checks.append(Check("ERROR", "source.fields", "prepare", None, {"missing": missing}))
                kline = empty_kline
            else:
                # 转换失败保留明确根因，继续解析独立的快照输入，不用空 K 线制造跨源冲突。
                try:
                    # 银河旧代码只用于请求和 Raw 证据；Unit 归属统一映射回当前新代码。
                    kline = kline.with_columns(
                        (pl.col("_code").replace(self._bse_current) + ":" + pl.col("kline_time").cast(pl.String).str.slice(0, 4) + "-" +
                         pl.col("kline_time").cast(pl.String).str.slice(4, 2) + "-" +
                         pl.col("kline_time").cast(pl.String).str.slice(6, 2)).alias("unit_key")
                    ).select("unit_key", "kline_time", "open_price", "high_price", "low_price", "close_price",
                             "volume_trade", "value_trade", "_request_id", "_start", "_end")
                    kline = kline.cast({c: pl.Int64 for c in kline.columns if c not in ("unit_key", "_request_id")})
                except pl.exceptions.PolarsError as exc:
                    checks.append(Check("ERROR", "source.types", "prepare", None, {"error": str(exc)}))
                    kline = empty_kline
        else:
            kline = empty_kline
        snapshots = delivery.frame.filter(pl.col("_kind") == "snapshot")
        anchors = []
        if snapshots.height:
            required = {"ts_code", "trade_time", "open", "high", "low", "last", "volume", "amount"}
            missing = sorted(required - set(snapshots.columns))
            if missing:
                checks.append(Check("ERROR", "snapshot.fields", "prepare", None, {"missing": missing}))
                return kline, empty_auction, tuple(checks)
            by_unit = {}
            requested_units = scope.units if scope.request_units is None else scope.request_units
            for row in snapshots.iter_rows(named=True):
                request = self._responses[row["_request_id"]].request
                affected = self._owners[row["_request_id"]]
                try:
                    stamp = _timestamp(row["trade_time"])
                    canonical_code = self._bse_current.get(row["ts_code"], row["ts_code"])
                    unit = _unit(canonical_code, stamp.date().isoformat())
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

    def _source_checks(self, con, scope, daily_map, ready):
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
        # 独立源检查仍完整执行；只有有效、完整的两类输入才参与竞价跨源比较。
        kline_ready = ready["kline"] - blocked_units(scope.units, checks)
        auction_ready = sorted(kline_ready & ready["snapshot"])
        auction_conservation = con.execute("""
            SELECT a.unit_key FROM _minute_auction a
            LEFT JOIN _minute_kline k ON k.unit_key=a.unit_key AND k.kline_time%10000=930
            WHERE a.unit_key IN (SELECT unnest(?::VARCHAR[]))
              AND (k.unit_key IS NULL OR a.volume_trade>k.volume_trade OR a.value_trade>k.value_trade
                   OR a.close_price<k.low_price OR a.close_price>k.high_price)
        """, [auction_ready]).fetchall()
        if auction_conservation:
            checks.append(Check("ERROR", "auction.conservation", "prepare", tuple(r[0] for r in auction_conservation)))
        remainder = con.execute("""
            SELECT k.unit_key FROM _minute_kline k JOIN _minute_auction a USING(unit_key)
            WHERE k.unit_key IN (SELECT unnest(?::VARCHAR[]))
              AND k.kline_time%10000=930 AND ((k.volume_trade-a.volume_trade=0)<>(k.value_trade-a.value_trade=0))
        """, [auction_ready]).fetchall()
        if remainder:
            checks.append(Check("ERROR", "auction.remainder", "prepare", tuple(r[0] for r in remainder)))
        delivered = {r[0] for r in con.execute("SELECT DISTINCT unit_key FROM _minute_kline").fetchall()}
        # 完整空交付仍参与业务冲突判断；获取/解析失败造成的空数据不参与。
        for unit in sorted(kline_ready):
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
        """先执行银河专属 930 扣减，再交给共用完整网格构造。"""
        # 银河 930 原始量额包含 925；双零时不保留成交行，由共用网格生成 NO_TRADE。
        con.execute("""
            CREATE OR REPLACE TEMP TABLE _minute_traded AS
            WITH adjusted AS (
                SELECT k.unit_key,k.kline_time,
                    strptime((k.kline_time/10000)::BIGINT::VARCHAR,'%Y%m%d')::DATE AS trade_date,
                    k.open_price/1000000.0 AS open,k.high_price/1000000.0 AS high,
                    k.low_price/1000000.0 AS low,k.close_price/1000000.0 AS close,
                    (k.volume_trade-CASE WHEN k.kline_time%10000=930 THEN COALESCE(a.volume_trade,0) ELSE 0 END)/100 AS volume,
                    (k.value_trade-CASE WHEN k.kline_time%10000=930 THEN COALESCE(a.value_trade,0) ELSE 0 END)/100000.0 AS amount,
                    k.kline_time%10000=930 AND a.unit_key IS NOT NULL
                      AND k.volume_trade=a.volume_trade AND k.value_trade=a.value_trade AS drop_930
                FROM _minute_kline k LEFT JOIN _minute_auction a USING(unit_key)
            )
            SELECT unit_key,trade_date,
                strptime((kline_time/10000)::BIGINT::VARCHAR||lpad((kline_time%10000)::VARCHAR,4,'0'),'%Y%m%d%H%M')::TIME::TIME_NS AS bar_time,
                open,high,low,close,volume::BIGINT AS volume,amount
            FROM adjusted WHERE NOT drop_930 AND volume>0 AND amount>0
            UNION ALL
            SELECT unit_key,split_part(unit_key,':',2)::DATE,TIME '09:25:00'::TIME_NS,
                open_price/1000000.0,high_price/1000000.0,low_price/1000000.0,close_price/1000000.0,
                (volume_trade/100)::BIGINT,value_trade/100000.0
            FROM _minute_auction
        """)
        return build_candidate(con, full_suspended)
