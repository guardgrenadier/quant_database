"""stk_limit 正式链路：分页终止、整日失败隔离、源价格语义和版本保护。"""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.__main__ import main
from quant_data.core import runner
from quant_data.core.models import RequestAttempt, RunOptions
from quant_data.datasets.stk_limit import StkLimitDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.stk_limit import TushareStkLimitAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter
from tests.test_suspend_daily import ProviderClient as CalendarClient

SAMPLES = Path('data/probes/stk-limit-onboarding-20261029T213351571183/requests')


def row(code='000001.SZ', day='20200312', **changes):
    return dict(dict(ts_code=code, trade_date=day, pre_close=10.0, up_limit=11.0,
                     down_limit=9.0, asset_type='STK', exchange='SZSE'), **changes)


class ProviderClient:
    """只替换 HTTP，按请求 offset 返回字段重排的分页信封。"""
    def __init__(self, records=None, transform=None):
        self.records = ([row(), row('900901.SH', exchange='SSE'), row('920001.BJ', exchange='BSE'),
                         row(day='20200313')] if records is None else records)
        self.transform, self.calls = transform, []

    def query(self, api, params, fields, *, request_id, on_attempt):
        self.calls.append((api, dict(params), tuple(fields)))
        selected = [r for r in self.records if r['trade_date'] == params['trade_date']]
        start, size = params['offset'], params['limit']
        returned = list(reversed(fields))
        data = {'fields': returned, 'items': [[r[f] for f in returned] for r in selected[start:start+size]],
                'has_more': start+size < len(selected), 'count': 0}
        if self.transform:
            self.transform(params, data)
        body = {'code': 0, 'data': data}
        on_attempt(RequestAttempt(request_id, 1, api, params, tuple(fields), datetime.now(timezone.utc), 0.0,
                                  200, 0, tuple(data['fields']), len(data['items']), data.get('has_more')), body)
        return body


class StkLimitTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'warehouse.duckdb'
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.params = {'start_date': '20200312', 'end_date': '20200315'}
        for module in ('stk_limit', 'trade_calendar'):
            setting = patch(f'quant_data.sources.tushare.{module}.REQUEST_INTERVAL', 0)
            setting.start()
            self.addCleanup(setting.stop)
        # 小页用例真实经过分页控制，不制造另一套生产获取实现。
        setting = patch('quant_data.sources.tushare.stk_limit.PAGE_SIZE', 2)
        setting.start()
        self.addCleanup(setting.stop)
        calendar = TradeCalendarDataset()
        db.initialize(self.con, calendar)
        result = runner.run(self.con, calendar, TushareTradeCalendarAdapter(CalendarClient()),
                            self.params, RunOptions(self.root/'calendar'))
        self.assertEqual(result.status, 'SUCCESS', result)
        self.dataset = StkLimitDataset()
        db.initialize(self.con, self.dataset)

    def ingest(self, records=None, transform=None, params=None, keep=False, con=None, client=None):
        self.client = client if client is not None else ProviderClient(records, transform)
        return runner.run(con or self.con, self.dataset, TushareStkLimitAdapter(self.client),
                          self.params if params is None else params, RunOptions(self.root/'landing', keep))

    def current(self, day='2020-03-12'):
        return [self.con.execute(sql, [day]).fetchall() for sql in (
            'SELECT * FROM canonical.stk_limit WHERE trade_date=? ORDER BY ts_code',
            "SELECT * FROM meta.coverage WHERE dataset='stk_limit' AND unit_key=?",
            "SELECT * FROM meta.publication WHERE dataset='stk_limit' AND unit_key=? ORDER BY publication_id")]

    def checks(self, result, level='ERROR'):
        return self.con.execute('SELECT rule,stage,units,detail FROM meta.validation_check WHERE run_id=? AND level=?',
                                [result.run_id, level]).fetchall()

    def test_paged_scope_replacement_and_artifacts(self):
        result = self.ingest(keep=True)
        self.assertEqual(result.status, 'SUCCESS', result)
        self.assertTrue(result.artifacts_complete, result)
        self.assertEqual([(p['trade_date'], p['offset']) for _, p, _ in self.client.calls],
                         [('20200312', 0), ('20200312', 2), ('20200313', 0)])
        self.assertTrue(all(api == 'stk_limit' and fields == tuple(self.dataset.schema.names())
                            for api, _, fields in self.client.calls))
        self.assertEqual({p.unit_key:p.row_count for p in result.publications}, {'2020-03-12':3, '2020-03-13':1})
        self.assertEqual(self.con.execute('SELECT count(*) FROM meta.run_dependency WHERE run_id=?',
                                         [result.run_id]).fetchone()[0], 4)
        self.assertEqual([u.status for u in result.units].count('OUT_OF_SCOPE'), 2)
        raw = pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')
        self.assertEqual(raw.schema['trade_date'], pl.String)
        self.assertEqual(raw['_offset'].to_list(), [0, 0, 2, 0])
        self.assertEqual(pl.read_parquet(Path(result.artifact_dir)/'candidate.parquet').schema, self.dataset.schema)
        before_id = self.current()[1][0][3]
        repeat = self.ingest(records=[row(up_limit=12), row(day='20200313')])
        self.assertEqual(repeat.status, 'SUCCESS', repeat)
        self.assertEqual(len(self.current()[0]), 1)
        self.assertGreater(self.current()[1][0][3], before_id)

    def test_full_false_tail_and_short_true_progress(self):
        # 满页 false 不足以终止，必须获取空尾页；短页 true 则按实际行数前移。
        for records, transform, offsets in (
            ([row(), row('000002.SZ')], None, [0, 2]),
            ([row(), row('000002.SZ'), row('000003.SZ')],
             lambda p,d: (d.update(items=d['items'][:1], has_more=True) if p['offset']==0 else None), [0, 1, 3]),
        ):
            with self.subTest(offsets=offsets):
                result = self.ingest(records, transform, params={'start_date':'20200312','end_date':'20200312'})
                self.assertEqual(result.status, 'SUCCESS', result)
                self.assertEqual([p['offset'] for _, p, _ in self.client.calls], offsets)
                self.assertEqual(result.publications[0].row_count, len(records))

    def test_later_page_failures_and_repeated_page_preserve_day(self):
        self.ingest()
        before = self.current()
        def failure(p, d):
            raise RuntimeError('offline injected page failure')
        first_page = [[r[f] for f in reversed(self.dataset.schema.names())] for r in ProviderClient().records[:2]]
        faults = (
            failure,
            lambda p,d: d.pop('has_more'),
            lambda p,d: d.update(items=[], has_more=True),
            lambda p,d: d.update(items=d['items']*3),
            lambda p,d: d.update(items=list(reversed(first_page)), has_more=True),
        )
        for fault in faults:
            def transform(p, d):
                if p['trade_date']=='20200312' and p['offset']==2:
                    fault(p, d)
            with self.subTest(fault=fault):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status, 'PARTIAL', result)
                self.assertEqual(self.current(), before)
                self.assertEqual([p.unit_key for p in result.publications], ['2020-03-13'])
                self.assertTrue(all(json.loads(c[2])==['2020-03-12'] for c in self.checks(result)))
                self.assertEqual(len(self.client.calls), 3)

    def test_preparation_failures_stay_complete_and_do_not_cross_days(self):
        self.ingest()
        before = self.current()
        def missing(p, d):
            i = d['fields'].index('pre_close')
            d['fields'].pop(i)
            d['items'] = [r[:i]+r[i+1:] for r in d['items']]
        cases = [(None, None, 'source.fields')]
        cases += [(f,v,'source.types') for f,v in [('ts_code',12),('trade_date',20200312),
                                                   ('pre_close',True),('up_limit','11'),('asset_type',False)]]
        cases += [('trade_date',v,'source.range') for v in ('20200313','20200230',None)]
        for field, value, rule in cases:
            def transform(p, d):
                if p['trade_date']=='20200312' and p['offset']==0:
                    if field is None:
                        missing(p, d)
                    else:
                        d['items'][0][d['fields'].index(field)] = value
            with self.subTest(field=field, value=value):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status, 'PARTIAL', result)
                self.assertEqual(self.current(), before)
                self.assertIn(rule, [c[0] for c in self.checks(result)])
                self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=? "
                                                 "AND unit_key='2020-03-12'", [result.run_id]).fetchone()[0], 'COMPLETE')

    def test_source_values_null_zero_open_categories_and_extremes(self):
        records = [row(' special.code ', pre_close=None, up_limit=100000, down_limit=.01,
                       asset_type=' ', exchange=None), row('900901.SH', pre_close=0, asset_type=' NEW ', exchange=' OTHER '),
                   row('920285.BJ', pre_close=20.3, up_limit=99999.99, down_limit=0, exchange='BSE'),
                   row(day='20200313', pre_close=1000)]
        result = self.ingest(records, keep=True)
        self.assertEqual(result.status, 'SUCCESS', result)
        self.assertEqual(self.checks(result), [])
        actual = self.con.execute("SELECT ts_code,pre_close,up_limit,down_limit,asset_type,exchange "
                                  "FROM canonical.stk_limit WHERE trade_date='2020-03-12' ORDER BY ts_code").fetchall()
        self.assertEqual(actual, [('900901.SH',0,11,9,'NEW','OTHER'),
                                  ('920285.BJ',20.3,99999.99,0,'STK','BSE'),
                                  ('special.code',None,100000,.01,None,None)])
        self.assertEqual(len(self.checks(result, 'WARNING')), 5)
        warning = next(c for c in self.checks(result, 'WARNING') if c[0]=='stk_limit.down_limit_zero')
        self.assertEqual(json.loads(warning[2]), ['2020-03-12'])
        self.assertEqual(json.loads(warning[3]), {'rows_by_unit':{'2020-03-12':1}})
        self.assertIn(' special.code ', pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')['ts_code'].to_list())

    def test_business_errors_and_empty_protect_old_publication(self):
        self.ingest()
        before = self.current()
        params = {'start_date':'20200312','end_date':'20200312'}
        cases = [([], 'nonempty'), ([row(ts_code=' ')], 'required'), ([row(up_limit=None)], 'required'),
                 ([row(pre_close=-1)], 'sign'), ([row(down_limit=-.01)], 'sign'),
                 ([row(up_limit=0, down_limit=0)], 'sign'), ([row(down_limit=None)], 'required'),
                 ([row(up_limit=8)], 'order'), ([row(pre_close=float('nan'))], 'finite'),
                 ([row(up_limit=float('inf'))], 'finite'),
                 ([row(), row('000002.SZ'), row(up_limit=12)], 'key_unique')]
        for records, rule in cases:
            with self.subTest(rule=rule):
                result = self.ingest(records, params=params, keep=not records)
                self.assertEqual(result.status, 'FAILED', result)
                self.assertEqual(self.current(), before)
                self.assertIn('stk_limit.'+rule, [c[0] for c in self.checks(result)])
                if not records:
                    empty = pl.read_parquet(Path(result.artifact_dir)/'candidate.parquet')
                    self.assertEqual(empty.schema, self.dataset.schema)
                    self.assertEqual(empty.height, 0)
        # 数据库最终约束也拒绝非法价格；NULL 和零的合法入库已由上一测试验证。
        with self.assertRaises(duckdb.ConstraintException):
            self.con.execute("INSERT INTO canonical.stk_limit VALUES ('bad','2020-03-12',NULL,1,2,NULL,NULL)")

    def test_dependencies_inputs_and_excluded_version_change(self):
        for params in ({'start_date':'20200311','end_date':'20200312'},
                       {**self.params,'ts_codes':['000001.SZ']},
                       {'start_date':'20990101','end_date':'20990101'}):
            result = self.ingest(params=params)
            self.assertEqual(result.status, 'FAILED', result)
            self.assertEqual(self.client.calls, [])
        self.ingest()
        before = self.current()
        # 即使修订的是被排除休市日，也必须在提交前阻断使用旧版本的整个批次。
        def revise(p, d):
            if p['trade_date']=='20200312' and p['offset']==0:
                self.con.execute("UPDATE meta.coverage SET publication_id=publication_id+1000 "
                                 "WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'")
        result = self.ingest(transform=revise)
        self.assertEqual(result.status, 'FAILED', result)
        self.assertIn('Dependency changed', result.error)
        self.assertEqual(self.current(), before)

    def test_rollback_evidence_failure_and_cli(self):
        self.ingest()
        before = self.current()
        proxy = Mock(wraps=self.con)
        def execute(sql, *args):
            if ' '.join(sql.split()).startswith('INSERT INTO "canonical"."stk_limit"'):
                raise RuntimeError('offline injected insert failure')
            return self.con.execute(sql, *args)
        proxy.execute.side_effect = execute
        self.assertEqual(self.ingest(con=proxy).status, 'FAILED')
        self.assertEqual(self.current(), before)
        with patch('quant_data.core.artifacts.Artifacts.record_attempt', side_effect=OSError('evidence disk failure')):
            self.assertEqual(self.ingest().status, 'FAILED')
        self.assertEqual(self.current(), before)
        # CLI 仅替换供应商客户端，真实装配、连接、Runner 和退出状态均执行。
        args = ['quant_data','ingest','stk_limit','--start-date','20200312','--end-date','20200315',
                '--db',str(self.path),'--landing-dir',str(self.root/'cli')]
        for client, expected, status in ((ProviderClient(),0,'SUCCESS'),
                                         (ProviderClient([row()]),1,'PARTIAL'), (ProviderClient([]),1,'FAILED')):
            output = io.StringIO()
            with patch('sys.argv',args), patch('quant_data.__main__.dotenv_values',return_value={'TUSHARE_TOKEN':'test'}), \
                 patch('quant_data.__main__.TushareClient',return_value=client), redirect_stdout(output):
                self.assertEqual(main(), expected)
            self.assertEqual(json.loads(output.getvalue())['status'], status)
        with patch('sys.argv',args+['--ts-code','000001.SZ']), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                main()

    @unittest.skipUnless(SAMPLES.exists(), '本地真实 stk_limit 信封不随 Git 分发')
    def test_saved_real_pages_and_historical_values(self):
        # 上游日历为明确的测试输入，价格页全部来自真实已保存信封，二者不混称实时获取。
        bodies = {name:json.loads((SAMPLES/f'{name}-attempt-01.json').read_text())['response']
                  for name in ('recent','page0','page1','historic_2010')}
        class ReplayClient:
            def query(self, api, params, fields, *, request_id, on_attempt):
                if api == 'trade_cal':
                    day = params['start_date']
                    values = dict(exchange='SSE', cal_date=day, is_open=1,
                                  pretrade_date=(date.fromisoformat(day)-timedelta(days=1)).strftime('%Y%m%d'))
                    body = {'code':0,'data':{'fields':list(fields),'items':[[values[f] for f in fields]],'has_more':False}}
                else:
                    name = 'historic_2010' if params['trade_date']=='20100104' else ('page0' if params['offset']==0 else 'page1')
                    body = deepcopy(bodies[name])
                data = body['data']
                on_attempt(RequestAttempt(request_id,1,api,params,tuple(fields),datetime.now(timezone.utc),0.0,
                                          200,0,tuple(data['fields']),len(data['items']),data['has_more']),body)
                return body
        with patch('quant_data.sources.tushare.stk_limit.PAGE_SIZE',3000):
            for day, source in (('20261028','recent'),('20100104','historic_2010')):
                params = {'start_date':day,'end_date':day}
                calendar = TradeCalendarDataset()
                result = runner.run(self.con,calendar,TushareTradeCalendarAdapter(ReplayClient()),
                                    params,RunOptions(self.root/'calendar'))
                self.assertEqual(result.status,'SUCCESS',result)
                result = self.ingest(params=params,client=ReplayClient(),keep=True)
                self.assertEqual(result.status,'SUCCESS',result)
                data = bodies[source]['data']
                # 独立逐字段生成期望值，不调用正式 prepare 构造预期。
                expected = []
                for values in data['items']:
                    record = dict(zip(data['fields'],values))
                    record['trade_date'] = date.fromisoformat(record['trade_date'])
                    expected.append(tuple(record[f] for f in self.dataset.schema.names()))
                actual = self.con.execute('SELECT * FROM canonical.stk_limit WHERE trade_date=? ORDER BY ts_code',
                                          [date.fromisoformat(day)]).fetchall()
                self.assertEqual(actual,sorted(expected))
                candidate = pl.read_parquet(Path(result.artifact_dir)/'candidate.parquet')
                self.assertEqual(sorted(candidate.rows()),sorted(expected))
                raw = pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')
                self.assertEqual(sorted(raw.select(data['fields']).rows()),sorted(map(tuple,data['items'])))


if __name__ == '__main__':
    unittest.main()
