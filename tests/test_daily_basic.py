"""每日指标正式链路：分页、合法 NULL/负估值、关系告警与按日失败保护。"""

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
from quant_data.datasets.daily_basic import DailyBasicDataset, NUMERIC
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.daily_basic import TushareDailyBasicAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter
from tests.test_suspend_daily import ProviderClient as CalendarClient

SAMPLES = Path('data/probes/daily-basic-onboarding-20261030T081207214252/requests')


def row(code='000001.SZ', day='20200312', **changes):
    return dict(dict(ts_code=code, trade_date=day, close=10.5, turnover_rate=2.5, turnover_rate_f=3.5,
                     volume_ratio=1.2, pe=10.0, pe_ttm=12.0, pb=1.5, ps=2.0, ps_ttm=2.2,
                     dv_ratio=0.0, dv_ttm=None, total_share=100.25, float_share=80.5, free_share=60.5,
                     total_mv=1234.5, circ_mv=876.5), **changes)


class ProviderClient:
    """只替换 HTTP；业务列逆序返回，按请求日及 offset 分页。"""
    def __init__(self, records=None, transform=None):
        self.records = ([row(), row('600000.SH'), row('920001.BJ'), row(day='20200313')]
                        if records is None else records)
        self.transform, self.calls = transform, []

    def query(self, api, params, fields, *, request_id, on_attempt):
        self.calls.append((api, dict(params), tuple(fields)))
        selected = [r for r in self.records if r['trade_date']==params['trade_date']]
        offset, size = params['offset'], params['limit']
        returned = list(reversed(fields))
        data = {'fields':returned, 'items':[[r[f] for f in returned] for r in selected[offset:offset+size]],
                'has_more':offset+size<len(selected), 'count':0}
        if self.transform:
            self.transform(params, data)
        body = {'code':0, 'data':data}
        on_attempt(RequestAttempt(request_id,1,api,params,tuple(fields),datetime.now(timezone.utc),0.0,
                                  200,0,tuple(data['fields']),len(data['items']),data.get('has_more')),body)
        return body


class DailyBasicTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'warehouse.duckdb'
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.params = {'start_date':'20200312','end_date':'20200315'}
        for module in ('daily_basic','trade_calendar'):
            setting = patch(f'quant_data.sources.tushare.{module}.REQUEST_INTERVAL',0)
            setting.start()
            self.addCleanup(setting.stop)
        setting = patch('quant_data.sources.tushare.daily_basic.PAGE_SIZE',2)
        setting.start()
        self.addCleanup(setting.stop)
        # 上游经正式 Runner 发布；不建立股票、停牌或行情表作为隐式依赖。
        calendar = TradeCalendarDataset()
        db.initialize(self.con,calendar)
        result = runner.run(self.con,calendar,TushareTradeCalendarAdapter(CalendarClient()),
                            self.params,RunOptions(self.root/'calendar'))
        self.assertEqual(result.status,'SUCCESS',result)
        self.dataset = DailyBasicDataset()
        db.initialize(self.con,self.dataset)

    def ingest(self, records=None, transform=None, params=None, keep=False, con=None, client=None):
        self.client = client if client is not None else ProviderClient(records,transform)
        return runner.run(con or self.con,self.dataset,TushareDailyBasicAdapter(self.client),
                          self.params if params is None else params,RunOptions(self.root/'landing',keep))

    def current(self, day='2020-03-12'):
        return [self.con.execute(sql,[day]).fetchall() for sql in (
            'SELECT * FROM canonical.daily_basic WHERE trade_date=? ORDER BY ts_code',
            "SELECT * FROM meta.coverage WHERE dataset='daily_basic' AND unit_key=?",
            "SELECT * FROM meta.publication WHERE dataset='daily_basic' AND unit_key=? ORDER BY publication_id")]

    def checks(self, result, level='ERROR'):
        return self.con.execute('SELECT rule,stage,units,detail FROM meta.validation_check WHERE run_id=? AND level=?',
                                [result.run_id,level]).fetchall()

    def test_paged_scope_schema_replacement_and_artifacts(self):
        first = self.ingest(keep=True)
        self.assertEqual(first.status,'SUCCESS',first)
        self.assertTrue(first.artifacts_complete,first)
        self.assertEqual([(p['trade_date'],p['offset']) for _,p,_ in self.client.calls],
                         [('20200312',0),('20200312',2),('20200313',0)])
        self.assertTrue(all(api=='daily_basic' and fields==tuple(self.dataset.schema.names())
                            for api,_,fields in self.client.calls))
        self.assertEqual(len(self.dataset.schema),18)
        self.assertNotIn('limit_status',self.dataset.schema)
        self.assertEqual({p.unit_key:p.row_count for p in first.publications},{'2020-03-12':3,'2020-03-13':1})
        self.assertEqual([u.status for u in first.units].count('OUT_OF_SCOPE'),2)
        self.assertEqual(self.con.execute('SELECT count(*) FROM meta.run_dependency WHERE run_id=?',
                                         [first.run_id]).fetchone()[0],4)
        raw = pl.read_parquet(Path(first.artifact_dir)/'landing.parquet')
        self.assertEqual(raw.schema['trade_date'],pl.String)
        self.assertEqual(raw['_offset'].to_list(),[0,0,2,0])
        self.assertEqual(pl.read_parquet(Path(first.artifact_dir)/'candidate.parquet').schema,self.dataset.schema)
        before_id = self.current()[1][0][3]
        repeat = self.ingest([row(close=11),row(day='20200313')])
        self.assertEqual(repeat.status,'SUCCESS',repeat)
        self.assertEqual(len(self.current()[0]),1)
        self.assertGreater(self.current()[1][0][3],before_id)

    def test_full_false_tail_and_short_true_progress(self):
        for records,transform,offsets in (
            ([row(),row('000002.SZ')],None,[0,2]),
            ([row(),row('000002.SZ'),row('000003.SZ')],
             lambda p,d: d.update(items=d['items'][:1],has_more=True) if p['offset']==0 else None,[0,1,3]),
        ):
            with self.subTest(offsets=offsets):
                result = self.ingest(records,transform,params={'start_date':'20200312','end_date':'20200312'})
                self.assertEqual(result.status,'SUCCESS',result)
                self.assertEqual([p['offset'] for _,p,_ in self.client.calls],offsets)
                self.assertEqual(result.publications[0].row_count,len(records))

    def test_later_page_faults_preserve_whole_day(self):
        self.ingest()
        before = self.current()
        def failure(p,d):
            raise RuntimeError('offline injected page failure')
        first_page = [[r[f] for f in reversed(self.dataset.schema.names())] for r in ProviderClient().records[:2]]
        faults = (failure,lambda p,d:d.pop('has_more'),lambda p,d:d.update(items=[],has_more=True),
                  lambda p,d:d.update(items=d['items']*3),
                  lambda p,d:d.update(items=list(reversed(first_page)),has_more=True))
        for fault in faults:
            def transform(p,d):
                if p['trade_date']=='20200312' and p['offset']==2:
                    fault(p,d)
            with self.subTest(fault=fault):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status,'PARTIAL',result)
                self.assertEqual(self.current(),before)
                self.assertEqual([p.unit_key for p in result.publications],['2020-03-13'])
                self.assertTrue(all(json.loads(c[2])==['2020-03-12'] for c in self.checks(result)))
                self.assertEqual(len(self.client.calls),3)

    def test_missing_columns_types_and_wrong_dates_stay_isolated(self):
        self.ingest()
        before = self.current()
        cases = [(None,None,'source.fields')]
        cases += [(f,v,'source.types') for f,v in (('ts_code',12),('trade_date',20200312),
                                                   ('pe',True),('free_share','100'))]
        cases += [('trade_date',v,'source.range') for v in ('20200313','20200230',None)]
        for field,value,rule in cases:
            def transform(p,d):
                if p['trade_date']=='20200312' and p['offset']==0:
                    if field is None:
                        index = d['fields'].index('pe')
                        d['fields'].pop(index)
                        d['items'] = [r[:index]+r[index+1:] for r in d['items']]
                    else:
                        d['items'][0][d['fields'].index(field)] = value
            with self.subTest(field=field,value=value):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status,'PARTIAL',result)
                self.assertEqual(self.current(),before)
                self.assertIn(rule,[c[0] for c in self.checks(result)])
                self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=? "
                                                 "AND unit_key='2020-03-12'",[result.run_id]).fetchone()[0],'COMPLETE')

    def test_nulls_signed_valuations_units_and_relationship_warnings(self):
        record = row(' special.code ',turnover_rate=150,turnover_rate_f=200,pe=-1,pe_ttm=-2,pb=-3,ps=-4,ps_ttm=-5,
                     total_share=100.25,float_share=120.5,free_share=150.5,total_mv=1234.5,circ_mv=1500.25)
        sparse = row('000002.SZ',**{f:None for f in NUMERIC if f!='close'})
        result = self.ingest([record,sparse,row(day='20200313')],keep=True)
        self.assertEqual(result.status,'SUCCESS',result)
        self.assertEqual(self.checks(result),[])
        actual = self.con.execute("SELECT * FROM canonical.daily_basic WHERE ts_code='special.code'").fetchone()
        expected = {**record,'ts_code':'special.code','trade_date':date(2020,3,12)}
        self.assertEqual(actual,tuple(expected[f] for f in self.dataset.schema.names()))
        self.assertEqual(self.con.execute("SELECT pe,dv_ttm,free_share FROM canonical.daily_basic "
                                         "WHERE ts_code='000002.SZ'").fetchone(),(None,None,None))
        warnings = self.checks(result,'WARNING')
        self.assertEqual({c[0] for c in warnings},{'daily_basic.free_share_above_float_share',
                                                 'daily_basic.float_share_above_total_share',
                                                 'daily_basic.circ_mv_above_total_mv'})
        self.assertTrue(all(json.loads(c[3])=={'rows_by_unit':{'2020-03-12':1}} for c in warnings))
        self.assertIn(' special.code ',pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')['ts_code'].to_list())

    def test_business_errors_and_empty_protect_old_versions(self):
        self.ingest()
        before = self.current()
        cases = [([],'nonempty'),([row(ts_code=' ')],'required'),([row(close=None)],'required'),
                 ([row(close=0)],'close_positive'),([row(pe=float('nan'))],'finite'),
                 ([row(total_mv=float('inf'))],'finite'),
                 ([row(),row('000002.SZ'),row(close=11)],'key_unique')]
        cases += [([row(**{field:-1})],'nonnegative') for field in
                  ('turnover_rate','volume_ratio','dv_ratio','free_share','circ_mv')]
        for records,rule in cases:
            with self.subTest(rule=rule,records=records):
                result = self.ingest(records,params={'start_date':'20200312','end_date':'20200312'},keep=not records)
                self.assertEqual(result.status,'FAILED',result)
                self.assertEqual(self.current(),before)
                self.assertIn('daily_basic.'+rule,[c[0] for c in self.checks(result)])
                if not records:
                    empty = pl.read_parquet(Path(result.artifact_dir)/'candidate.parquet')
                    self.assertEqual(empty.schema,self.dataset.schema)
                    self.assertEqual(empty.height,0)
        with self.assertRaises(duckdb.ConstraintException):
            self.con.execute("UPDATE canonical.daily_basic SET free_share=-1 WHERE ts_code='000001.SZ'")

    def test_dependencies_unknown_inputs_and_excluded_version_change(self):
        for params in ({'start_date':'20200311','end_date':'20200312'},
                       {**self.params,'ts_codes':['000001.SZ']},
                       {'start_date':'20990101','end_date':'20990101'}):
            result = self.ingest(params=params)
            self.assertEqual(result.status,'FAILED',result)
            self.assertEqual(self.client.calls,[])
        # 已发布 Coverage 但实际日历事实缺失，不能从行情空响应反推休市。
        closed = self.con.execute("SELECT * FROM canonical.trade_calendar WHERE cal_date='2020-03-14'").fetchone()
        self.con.execute("DELETE FROM canonical.trade_calendar WHERE cal_date='2020-03-14'")
        unknown = self.ingest()
        self.assertEqual(unknown.status,'PARTIAL',unknown)
        self.assertEqual(next(u.status for u in unknown.units if u.unit_key=='2020-03-14'),'UNKNOWN')
        self.con.execute('INSERT INTO canonical.trade_calendar VALUES (?,?,?,?)',closed)
        before = self.current()
        def revise(p,d):
            if p['trade_date']=='20200312' and p['offset']==0:
                self.con.execute("UPDATE meta.coverage SET publication_id=publication_id+1000 "
                                 "WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'")
        changed = self.ingest(transform=revise)
        self.assertEqual(changed.status,'FAILED',changed)
        self.assertIn('Dependency changed',changed.error)
        self.assertEqual(self.current(),before)

    def test_rollback_evidence_failure_and_cli(self):
        self.ingest()
        before = self.current()
        proxy = Mock(wraps=self.con)
        def execute(sql,*args):
            if ' '.join(sql.split()).startswith('INSERT INTO "canonical"."daily_basic"'):
                raise RuntimeError('offline injected insert failure')
            return self.con.execute(sql,*args)
        proxy.execute.side_effect = execute
        self.assertEqual(self.ingest(con=proxy).status,'FAILED')
        self.assertEqual(self.current(),before)
        with patch('quant_data.core.artifacts.Artifacts.record_attempt',side_effect=OSError('evidence disk failure')):
            self.assertEqual(self.ingest().status,'FAILED')
        self.assertEqual(self.current(),before)
        args = ['quant_data','ingest','daily_basic','--start-date','20200312','--end-date','20200315',
                '--db',str(self.path),'--landing-dir',str(self.root/'cli')]
        for client,expected,status in ((ProviderClient(),0,'SUCCESS'),
                                       (ProviderClient([row()]),1,'PARTIAL'),(ProviderClient([]),1,'FAILED')):
            output = io.StringIO()
            with patch('sys.argv',args),patch('quant_data.__main__.dotenv_values',return_value={'TUSHARE_TOKEN':'test'}), \
                 patch('quant_data.__main__.TushareClient',return_value=client),redirect_stdout(output):
                self.assertEqual(main(),expected)
            self.assertEqual(json.loads(output.getvalue())['status'],status)
        for invalid in (args+['--ts-code','000001.SZ'],['quant_data','ingest','daily_basic']):
            with patch('sys.argv',invalid),redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    main()

    @unittest.skipUnless(SAMPLES.exists(),'本地真实 daily_basic 信封不随 Git 分发')
    def test_saved_real_pages_history_and_ipo(self):
        # 指标响应来自真实信封；日历为明确的测试夹具，不混称实时获取。
        bodies = {name:json.loads((SAMPLES/f'{name}-attempt-01.json').read_text())['response']
                  for name in ('recent','page0','page1','historic_2000','ipo_star')}
        class ReplayClient:
            def query(self,api,params,fields,*,request_id,on_attempt):
                if api=='trade_cal':
                    day = params['start_date']
                    values = dict(exchange='SSE',cal_date=day,is_open=1,
                                  pretrade_date=(date.fromisoformat(day)-timedelta(days=1)).strftime('%Y%m%d'))
                    body = {'code':0,'data':{'fields':list(fields),'items':[[values[f] for f in fields]],'has_more':False}}
                elif params['trade_date']=='20261029':
                    body = deepcopy(bodies['page0' if params['offset']==0 else 'page1'])
                else:
                    body = deepcopy(bodies['historic_2000' if params['trade_date']=='20000104' else 'ipo_star'])
                    data = body['data']
                    i = data['fields'].index('trade_date')
                    data['items'] = [r for r in data['items'] if r[i]==params['trade_date']]
                data = body['data']
                on_attempt(RequestAttempt(request_id,1,api,params,tuple(fields),datetime.now(timezone.utc),0.0,
                                          200,0,tuple(data['fields']),len(data['items']),data['has_more']),body)
                return body
        with patch('quant_data.sources.tushare.daily_basic.PAGE_SIZE',3000):
            for day,source in (('20261029','recent'),('20000104','historic_2000'),('20200716','ipo_star')):
                params = {'start_date':day,'end_date':day}
                result = runner.run(self.con,TradeCalendarDataset(),TushareTradeCalendarAdapter(ReplayClient()),
                                    params,RunOptions(self.root/'calendar'))
                self.assertEqual(result.status,'SUCCESS',result)
                result = self.ingest(params=params,client=ReplayClient(),keep=True)
                self.assertEqual(result.status,'SUCCESS',result)
                data = bodies[source]['data']
                expected,raw_expected = [],[]
                for values in data['items']:
                    record = dict(zip(data['fields'],values))
                    if record['trade_date']!=day:
                        continue
                    raw_expected.append(tuple(record[f] for f in self.dataset.schema.names()))
                    record['trade_date'] = date.fromisoformat(day)
                    expected.append(tuple(record[f] for f in self.dataset.schema.names()))
                actual = self.con.execute('SELECT * FROM canonical.daily_basic WHERE trade_date=? ORDER BY ts_code',
                                          [date.fromisoformat(day)]).fetchall()
                self.assertEqual(actual,sorted(expected))
                folder = Path(result.artifact_dir)
                candidate = pl.read_parquet(folder/'candidate.parquet')
                self.assertEqual(candidate.schema,self.dataset.schema)
                self.assertEqual(sorted(candidate.rows()),sorted(expected))
                raw = pl.read_parquet(folder/'landing.parquet')
                self.assertEqual(sorted(raw.select(self.dataset.schema.names()).rows()),sorted(raw_expected))
                if source=='recent':
                    warnings = self.checks(result,'WARNING')
                    self.assertEqual(len(warnings),1)
                    self.assertEqual(json.loads(warnings[0][3]),{'rows_by_unit':{'2026-10-29':3}})
                else:
                    self.assertEqual(self.checks(result,'WARNING'),[])


if __name__=='__main__':
    unittest.main()
