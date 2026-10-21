"""Tushare 指数基础快照：宽基逐代码请求，申万按类别和发布方请求。"""

import time

import polars as pl
import pyarrow as pa

from quant_data.core.models import Check, Delivery, UnitDelivery
from quant_data.datasets.index_basic import BROAD_CODES, SW_CATEGORY, SW_PUBLISHER, IndexBasicDataset

ROW_LIMIT = 8000
REQUEST_INTERVAL = 1.3
DATES = ('base_date', 'list_date', 'exp_date')


class TushareIndexBasicAdapter:
    """固定请求计划交付一个 SNAPSHOT；无全量获取、分类辅助请求或分页框架。"""

    def __init__(self, client):
        self.client = client
        self.fields = tuple(IndexBasicDataset.schema.names())

    def _frame(self, data, code):
        """严格解码 13 个源字段，并保存可信请求代码；NULL 标签表示申万过滤请求。"""
        arrays = []
        for field in self.fields:
            # 业务映射：使用字段名定位列，不能假定源列序；代码和日期保持字符串。
            values = [row[data['fields'].index(field)] for row in data['items']]
            if field == 'base_point':
                # 业务检查：只接受 JSON 数值或 NULL；拒绝布尔和字符串的隐式数值转换。
                if any(v is not None and type(v) not in (int, float) for v in values):
                    raise ValueError('base_point must be a JSON number or NULL, excluding bool')
                arrays.append(pa.array(values, type=pa.float64()))
            else:
                arrays.append(pa.array(values, type=pa.string()))
        return pl.from_arrow(pa.Table.from_arrays(arrays, names=self.fields)).with_columns(
            pl.lit(code, dtype=pl.String).alias('_requested_code'))

    def fetch(self, scope, artifacts):
        """每个白名单代码一次请求，申万一次过滤请求，所有分片共同决定完整快照。"""
        if scope.units != (IndexBasicDataset.unit,):
            raise ValueError('index_basic adapter requires the complete snapshot scope')
        frames, attempts, checks, request_ids = [], [], [], []
        state, last_request = 'COMPLETE', None

        def record(attempt, payload):
            """每次尝试即时留证，证据写失败不能继续发布。"""
            attempts.append(attempt)
            attempts[-1] = artifacts.record_attempt(attempt, payload)

        # 业务范围：供应商端直接筛选，无 index_classify、本地集合关联或未过滤全量请求。
        plans = [(code, {'ts_code': code}) for code in BROAD_CODES]
        plans.append((None, {'category': SW_CATEGORY, 'publisher': SW_PUBLISHER}))
        for code, params in plans:
            if last_request is not None:
                time.sleep(max(0, REQUEST_INTERVAL - (time.monotonic() - last_request)))
            last_request = time.monotonic()
            request_id = 'index_basic-' + (code or 'sw-industry')
            request_ids.append(request_id)
            detail = {'request_id': request_id}
            try:
                body = self.client.query('index_basic', params, self.fields, request_id=request_id, on_attempt=record)
            except (RuntimeError, ValueError, OSError) as exc:
                checks.append(Check('ERROR', 'source.request', 'fetch', scope.units, {**detail, 'error': str(exc)}))
                state = 'FAILED'
                break
            data = body['data']
            # 业务交付：仅明确 false 且低于单次 8,000 行上限才可确认完成；触顶不自动分页。
            if data.get('has_more') is not False or len(data['items']) >= ROW_LIMIT:
                checks.append(Check('ERROR', 'source.truncated', 'fetch', scope.units,
                                    {**detail, 'rows': len(data['items']), 'has_more': data.get('has_more')}))
                state = 'INCOMPLETE'
                break
            # 准备诊断：缺列不能补 NULL，类型错误不改写已完成的获取；仍完成剩余请求。
            missing = sorted(set(self.fields) - set(data['fields']))
            if missing:
                checks.append(Check('ERROR', 'source.fields', 'prepare', scope.units, {**detail, 'missing': missing}))
                continue
            try:
                frames.append(self._frame(data, code))
            except (pa.ArrowException, ValueError, TypeError, OverflowError) as exc:
                checks.append(Check('ERROR', 'source.types', 'prepare', scope.units, {**detail, 'error': str(exc)}))
        return Delivery(pl.concat(frames) if frames else None,
                        (UnitDelivery(IndexBasicDataset.unit, state, tuple(request_ids)),), tuple(attempts), tuple(checks))

    def prepare(self, delivery, scope):
        """一次清理、核对请求身份及日期解析；只删除可信请求标签，不二次筛选或去重。"""
        if delivery.frame is None:
            return None, ()
        # 业务转换：空白转 NULL，保留 Raw；源返回必须符合对应代码或申万过滤范围。
        clean = delivery.frame.with_columns(pl.col(c).str.strip_chars().replace('', None)
                                             for c in self.fields if c != 'base_point')
        valid = pl.when(pl.col('_requested_code').is_not_null()).then(pl.col('ts_code') == pl.col('_requested_code')).otherwise(
            (pl.col('category') == SW_CATEGORY) & (pl.col('publisher') == SW_PUBLISHER))
        if clean.filter(~valid.fill_null(False)).height:
            return None, (Check('ERROR', 'source.range', 'prepare', scope.units),)
        # 业务转换：每列日期只解析一次，格式和历法错误不能作为合法 NULL 放行。
        parsed = clean.with_columns(
            [pl.col(c).str.strptime(pl.Date, '%Y%m%d', strict=False).alias(f'_{c}') for c in DATES]
            + [pl.col(c).str.contains(r'^[0-9]{8}$').alias(f'_{c}_format') for c in DATES])
        checks = []
        for field in DATES:
            for rule, mask in (('source.date_format', pl.col(field).is_not_null() & ~pl.col(f'_{field}_format')),
                               ('source.date_parse', pl.col(f'_{field}_format') & pl.col(f'_{field}').is_null())):
                count = parsed.filter(mask).height
                if count:
                    checks.append(Check('ERROR', rule, 'prepare', scope.units, {'field': field, 'rows': count}))
        if checks:
            return None, tuple(checks)
        return parsed.select(pl.col(f'_{c}').alias(c) if c in DATES else pl.col(c) for c in self.fields), ()
