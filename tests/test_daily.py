"""股票/指数日线正式链路：固定上游、源路由、零行及单位、失败隔离。"""

from contextlib import redirect_stderr, redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
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
from quant_data.datasets.index_basic import BROAD_CODES, SW_CATEGORY, SW_PUBLISHER, IndexBasicDataset
from quant_data.datasets.index_daily import IndexDailyDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.sources.tushare.index_basic import TushareIndexBasicAdapter
from quant_data.sources.tushare.index_daily import TushareIndexDailyAdapter
from quant_data.sources.tushare.stock_daily import TushareStockDailyAdapter
from quant_data.sources.tushare.trade_calendar import TushareTradeCalendarAdapter
from tests.test_index_basic import ProviderClient as BasicClient, row as basic_row
from tests.test_suspend_daily import ProviderClient as CalendarClient

SAMPLE = Path('data/probes/daily-onboarding-20261022T012318968173/requests')
CODES = ('000300.SH', '801010.SI', '801011.SI')


def quote(code, day, **changes):
    return dict(dict(ts_code=code, trade_date=day, open=10.0, high=12.0, low=9.0, close=11.0,
                     pre_close=10.0, change=1.0, pct_chg=10.0, vol=2.5, amount=3.0), **changes)


class QuoteClient:
    """只替换 HTTP，三类行情保留各自真实字段和参数，正式内核不替换。"""
    def __init__(self, stock=None, index=None, sw=None, transform=None):
        self.stock = [quote('000001.SZ', d) for d in ('20200312', '20200313')] if stock is None else stock
        self.index = [quote(CODES[0], d) for d in ('20200312', '20200313')] if index is None else index
        self.sw = [dict({k:v for k,v in quote(CODES[1], d).items() if k not in ('pre_close','pct_chg')}, pct_change=10.0)
                   for d in ('20200312', '20200313')] if sw is None else sw
        self.transform, self.calls = transform, []

    def query(self, api, params, fields, *, request_id, on_attempt):
        self.calls.append((api, dict(params), tuple(fields)))
        source = self.stock if api == 'daily' else self.sw if api == 'sw_daily' else self.index
        # 文档外字段被忽略的供应商行为：正式适配器必须查缺列，不能靠测试替身补字段。
        allowed = set(quote('', '').keys())
        if api == 'sw_daily':
            allowed = allowed - {'pre_close','pct_chg'} | {'pct_change'}
        returned = [f for f in reversed(fields) if f in allowed]
        selected = [r for r in source if ('trade_date' in params and r['trade_date'] == params['trade_date']) or
                    ('ts_code' in params and r['ts_code'] == params['ts_code'] and params['start_date'] <= r['trade_date'] <= params['end_date'])]
        data = {'fields':returned, 'items':[[r[f] for f in returned] for r in reversed(selected)], 'has_more':False, 'count':0}
        if self.transform:
            self.transform(api, params, data)
        body = {'code':0, 'data':data}
        on_attempt(RequestAttempt(request_id,1,api,params,tuple(fields),datetime.now(timezone.utc),0.0,
                                  200,0,tuple(data['fields']),len(data['items']),data.get('has_more')),body)
        return body


class DailyTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.path = self.root/'warehouse.duckdb'
        self.con = duckdb.connect(str(self.path)); self.addCleanup(self.con.close)
        for module in ('trade_calendar','index_basic','stock_daily','index_daily'):
            setting = patch(f'quant_data.sources.tushare.{module}.REQUEST_INTERVAL',0)
            setting.start(); self.addCleanup(setting.stop)
        self.params = {'start_date':'20200312','end_date':'20200315'}
        calendar = TradeCalendarDataset(); db.initialize(self.con,calendar)
        r = runner.run(self.con,calendar,TushareTradeCalendarAdapter(CalendarClient()),
                       {**self.params,'end_date':'20200316'},RunOptions(self.root/'calendar'))
        self.assertEqual(r.status,'SUCCESS',r)
        basic = IndexBasicDataset(); db.initialize(self.con,basic)
        # 上游也是正式发布，future list_date/过去 exp_date 不得裁掉行情历史请求。
        records = [basic_row(ts_code=c,market='BSE' if c.endswith('.BJ') else 'CSI',list_date='20990101') for c in BROAD_CODES]
        records += [basic_row(ts_code=c,market='SW',category=SW_CATEGORY,publisher=SW_PUBLISHER,
                             list_date='20990101',exp_date='19990101') for c in CODES[1:]]
        r = runner.run(self.con,basic,TushareIndexBasicAdapter(BasicClient(records)),{},RunOptions(self.root/'basic'))
        self.assertEqual(r.status,'SUCCESS',r)
        self.datasets = {'stock_daily':StockDailyDataset(), 'index_daily':IndexDailyDataset()}
        for d in self.datasets.values(): db.initialize(self.con,d)

    def ingest(self, name='stock_daily', client=None, params=None, keep=False, con=None):
        self.client = client or QuoteClient()
        if params is None:
            params = {**self.params, **({'ts_codes':CODES} if name == 'index_daily' else {})}
        adapter = TushareStockDailyAdapter(self.client) if name == 'stock_daily' else TushareIndexDailyAdapter(self.client)
        return runner.run(con or self.con,self.datasets[name],adapter,params,RunOptions(self.root/'landing',keep))

    def current(self,name):
        return [self.con.execute(f'SELECT * FROM canonical.{name} ORDER BY ts_code,trade_date').fetchall(),
                self.con.execute('SELECT * FROM meta.coverage WHERE dataset=? ORDER BY unit_key',[name]).fetchall(),
                self.con.execute('SELECT * FROM meta.publication WHERE dataset=? ORDER BY publication_id',[name]).fetchall()]

    def errors(self,result):
        return [(rule,stage,json.loads(units) if units is not None else None) for rule,stage,units in self.con.execute(
            "SELECT rule,stage,units FROM meta.validation_check WHERE run_id=? AND level='ERROR'",[result.run_id]).fetchall()]

    def test_stock_day_scope_sparse_rows_rerun_and_zero_block(self):
        """没有股票/停牌表也可获取全市场；只请求开市日，缺个股不补造。"""
        first = self.ingest()
        self.assertEqual(first.status,'SUCCESS',first)
        self.assertEqual(self.client.calls[0][1],{'trade_date':'20200312'})
        self.assertEqual(len(self.client.calls),2)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.run_dependency WHERE run_id=?",[first.run_id]).fetchone()[0],4)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.coverage WHERE dataset='stock_daily'").fetchone()[0],2)
        repeat = self.ingest()
        self.assertGreater(repeat.publications[0].publication_id,first.publications[0].publication_id)
        self.assertEqual(self.con.execute('SELECT count(*) FROM canonical.stock_daily').fetchone()[0],2)
        before = self.current('stock_daily')
        result = self.ingest(client=QuoteClient(stock=[]),keep=True)
        self.assertEqual(result.status,'FAILED',result)
        self.assertEqual(self.current('stock_daily'),before)
        self.assertIn('stock_daily.nonempty',{r[0] for r in self.errors(result)})
        self.assertEqual(pl.read_parquet(Path(result.artifact_dir)/'candidate.parquet').schema,StockDailyDataset.schema)

    def test_index_routes_units_mapping_empty_success_and_default_universe(self):
        """全部及子集都在固定 universe 内；SW 字段/单位在准备时映射，代码日空集合成功。"""
        result = self.ingest('index_daily',keep=True)
        self.assertEqual(result.status,'SUCCESS',result)
        self.assertEqual(len(result.publications),6)
        self.assertEqual([p.row_count for p in result.publications].count(0),2)
        self.assertEqual([p['ts_code'] for api,p,_ in self.client.calls if api=='index_daily'],['000300.SH'])
        self.assertEqual([p for api,p,_ in self.client.calls if api=='sw_daily'],[{'trade_date':'20200312'},{'trade_date':'20200313'}])
        for api,params,fields in self.client.calls:
            if api == 'index_daily':
                self.assertEqual((params['start_date'],params['end_date']),('20200312','20200313'))
            self.assertEqual(('pct_change' in fields,'pre_close' in fields),(True,False) if api == 'sw_daily' else (False,True))
        value = self.con.execute("SELECT pre_close,pct_chg,vol,amount FROM canonical.index_daily WHERE ts_code='801010.SI' LIMIT 1").fetchone()
        self.assertEqual(value,(None,10,250,30))
        raw = pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')
        self.assertEqual(raw.filter(pl.col('_api') == 'sw_daily')['vol'].to_list(),[2.5,2.5])
        # 不传代码时选中全部已发布代码；没有行情的代码仍有明确零行 Publication。
        all_result = self.ingest('index_daily',params=self.params)
        self.assertEqual(all_result.status,'SUCCESS',all_result)
        self.assertEqual(len(self.client.calls),len(BROAD_CODES)+2)
        self.assertEqual(len(all_result.publications),2*(len(BROAD_CODES)+2))
        # 仅 SW 时也具有完整 Canonical Schema，不依赖宽基批次提供 pre_close 列。
        empty = self.ingest('index_daily',client=QuoteClient(sw=[]),params={**self.params,'ts_codes':['801010.SI']},keep=True)
        self.assertEqual(empty.status,'SUCCESS',empty)
        self.assertEqual(sum(p.row_count for p in empty.publications),0)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_daily WHERE ts_code='801010.SI'").fetchone()[0],0)
        self.assertEqual(pl.read_parquet(Path(empty.artifact_dir)/'candidate.parquet').schema,IndexDailyDataset.schema)

    def test_stock_wrong_day_and_index_wrong_identity_are_isolated(self):
        """坏行不进入另一目标 Unit；股票按请求日、指数按可信代码年片阻断。"""
        self.ingest(); self.ingest('index_daily')
        def bad_stock(api,params,data):
            if api == 'daily' and params['trade_date']=='20200312':
                data['items'][0][data['fields'].index('trade_date')]='20200313'
        result = self.ingest(client=QuoteClient(transform=bad_stock))
        self.assertEqual(result.status,'PARTIAL',result)
        self.assertEqual(self.errors(result),[('source.range','prepare',['2020-03-12'])])
        for field,value in (('ts_code','801010.SI'),('trade_date','20200230'),('trade_date','20200314')):
            def bad_index(api,params,data):
                if api == 'index_daily' and data['items']: data['items'][0][data['fields'].index(field)]=value
            result = self.ingest('index_daily',client=QuoteClient(transform=bad_index))
            self.assertEqual(result.status,'PARTIAL',result)
            self.assertEqual(set(self.errors(result)[0][2]),{'000300.SH:2020-03-12','000300.SH:2020-03-13'})
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_daily WHERE ts_code='000300.SH'").fetchone()[0],2)

    def test_sw_daily_batch_keeps_raw_superset_and_respects_selected_scope(self):
        """全 universe 单日为24宽基＋1申万；合法超集留 Raw 和排除审计，不发布范围外代码。"""
        base=QuoteClient().sw[0]
        sw=[*QuoteClient().sw,{**base,'ts_code':CODES[2]},{**base,'ts_code':'801999.SI'}]
        params={'start_date':'20200312','end_date':'20200312'}
        result=self.ingest('index_daily',client=QuoteClient(sw=sw),params=params,keep=True)
        self.assertEqual(result.status,'SUCCESS',result)
        self.assertEqual(len(self.client.calls),len(BROAD_CODES)+1)
        self.assertEqual(sum(api=='sw_daily' for api,_,_ in self.client.calls),1)
        raw=pl.read_parquet(Path(result.artifact_dir)/'landing.parquet')
        self.assertEqual(raw.filter(pl.col('_api')=='sw_daily').height,3)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_daily WHERE ts_code='801999.SI'").fetchone()[0],0)
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.coverage WHERE dataset='index_daily' AND unit_key LIKE '801999.SI:%'").fetchone()[0],0)
        detail=json.loads(self.con.execute("SELECT detail FROM meta.validation_check WHERE run_id=? AND rule='source.scope_excluded'",[result.run_id]).fetchone()[0])
        self.assertEqual(detail['rows'],1)
        # 子集请求也获取同一申万日表，已经在 universe 但不在本次子集的代码同样不发布。
        subset=self.ingest('index_daily',client=QuoteClient(sw=sw),params={**params,'ts_codes':[CODES[1]]},keep=True)
        self.assertEqual([p.unit_key for p in subset.publications],['801010.SI:2020-03-12'])
        candidate=pl.read_parquet(Path(subset.artifact_dir)/'candidate.parquet')
        self.assertEqual(candidate['ts_code'].to_list(),[CODES[1]])
        detail=json.loads(self.con.execute("SELECT detail FROM meta.validation_check WHERE run_id=? AND rule='source.scope_excluded'",[subset.run_id]).fetchone()[0])
        self.assertEqual(detail['rows'],2)

    def test_sw_batch_faults_block_all_members_of_that_day_only(self):
        """申万请求/日期/截断失败影响整个目标申万日；范围外坏日期不能被筛选掩盖。"""
        self.ingest('index_daily')
        def current_sw_day():
            return [self.con.execute("SELECT * FROM canonical.index_daily WHERE ts_code LIKE '%.SI' AND trade_date='2020-03-12' ORDER BY ts_code").fetchall(),
                    self.con.execute("SELECT * FROM meta.coverage WHERE dataset='index_daily' AND unit_key LIKE '%.SI:2020-03-12' ORDER BY unit_key").fetchall()]
        before=current_sw_day()
        for mode,rule in (('request','source.request'),('range','source.range'),('truncated','source.truncated')):
            def mutate(api,params,data):
                if api=='sw_daily' and params['trade_date']=='20200312':
                    if mode=='request':raise RuntimeError('injected batch failure')
                    if mode=='range':
                        self.assertEqual(data['items'][0][data['fields'].index('ts_code')],'801999.SI')
                        data['items'][0][data['fields'].index('trade_date')]='20200313'
                    else:data['items']=data['items'][:1]*4000
            sw=[*QuoteClient().sw,{**QuoteClient().sw[0],'ts_code':'801999.SI'}]
            result=self.ingest('index_daily',client=QuoteClient(sw=sw,transform=mutate))
            self.assertEqual(result.status,'PARTIAL',result)
            self.assertEqual(self.errors(result)[0][0],rule)
            self.assertEqual(set(self.errors(result)[0][2]),{'801010.SI:2020-03-12','801011.SI:2020-03-12'})
            self.assertEqual(current_sw_day(),before)
            self.assertEqual(len(result.publications),4)

    def test_source_fields_types_requests_and_markers_do_not_publish_empty(self):
        """获取/准备失败与合法空不同；类型/缺列错误仍可保留 COMPLETE，失败只影响请求片段。"""
        self.ingest(); self.ingest('index_daily')
        def change(field,value):
            def mutate(api,params,data):
                if data['items']: data['items'][0][data['fields'].index(field)]=value
            return mutate
        def missing(api,params,data):
            i=data['fields'].index('vol');data['fields'].pop(i)
            data['items']=[r[:i]+r[i+1:] for r in data['items']]
        def marker(api,params,data): data.pop('has_more')
        def failure(api,params,data): raise RuntimeError('injected source failure')
        for name in self.datasets:
            before=self.current(name)
            # 一个确定的股票日或宽基日作为失败目标，便于直接核对旧成功状态完全不变。
            params={'start_date':'20200312','end_date':'20200312',**({'ts_codes':['000300.SH']} if name=='index_daily' else {})}
            for transform,rule in ((change('ts_code',123),'source.types'),(change('vol',True),'source.types'),
                                   (missing,'source.fields'),(marker,'source.truncated'),(failure,'source.request')):
                result=self.ingest(name,client=QuoteClient(transform=transform),params=params)
                self.assertEqual(result.status,'FAILED',result)
                self.assertEqual(self.current(name),before)
                self.assertIn(rule,{r[0] for r in self.errors(result)})
                if rule in ('source.types','source.fields'):
                    self.assertEqual(self.con.execute('SELECT delivery_state FROM meta.run_unit WHERE run_id=?',[result.run_id]).fetchone()[0],'COMPLETE')
        with patch('quant_data.sources.tushare.stock_daily.ROW_LIMIT',1):
            self.assertEqual(self.ingest().status,'FAILED')
        def missing_sw(api,params,data):
            i=data['fields'].index('pct_change');data['fields'].pop(i)
            data['items']=[r[:i]+r[i+1:] for r in data['items']]
        result=self.ingest('index_daily',client=QuoteClient(transform=missing_sw),
                           params={'start_date':'20200312','end_date':'20200312','ts_codes':['801010.SI']})
        self.assertEqual(result.status,'FAILED')
        self.assertEqual(self.errors(result)[0][:2],('source.fields','prepare'))

    def test_business_rules_nulls_duplicates_and_sql_final_constraints(self):
        """股票必填/正价、指数可空/非全空、四价关系及重复键分别验收。"""
        stock_cases=[{'open':None},{'close':0},{'high':9.5},{'vol':-1},{'amount':float('inf')},{'pre_close':-1}]
        for changes in stock_cases:
            result=self.ingest(client=QuoteClient(stock=[quote('x','20200312',**changes),quote('x','20200313')]))
            self.assertEqual(result.status,'PARTIAL',result)
        for changes in ({'open':None,'high':None,'low':None,'close':None},dict.fromkeys(tuple(IndexDailyDataset.schema.names())[2:])):
            client=QuoteClient(index=[quote(CODES[0],'20200312',**changes)])
            result=self.ingest('index_daily',client=client,params={'start_date':'20200312','end_date':'20200312','ts_codes':[CODES[0]]})
            self.assertEqual(result.status,'FAILED' if changes.get('vol','present') is None else 'SUCCESS',result)
        client=QuoteClient(stock=[quote('x','20200312'),quote('x','20200312'),quote('x','20200313')])
        self.assertEqual(self.ingest(client=client).status,'PARTIAL')
        with self.assertRaises(duckdb.ConstraintException):
            self.con.execute("INSERT INTO canonical.stock_daily VALUES ('bad','2020-03-12',0,1,1,1,1,0,0,1,1)")

    def test_dependencies_outside_universe_future_inputs_and_unknown(self):
        """依赖及代码选择在请求前检查，缺事实保留 UNKNOWN，不使用股票上市日期截历史。"""
        for name,params in (('stock_daily',{'start_date':'２０２００３１２','end_date':'20200313'}),
                            ('stock_daily',{'start_date':'20200313','end_date':'20200312'}),
                            ('stock_daily',{'start_date':'29990101','end_date':'29990101'}),
                            ('index_daily',{**self.params,'ts_codes':['not-in-universe']}),
                            ('index_daily',{**self.params,'ts_codes':[]})):
            result=self.ingest(name,params=params)
            self.assertEqual(result.status,'FAILED');self.assertEqual(self.client.calls,[])
        self.con.execute("UPDATE canonical.index_basic SET market='NH' WHERE ts_code='000300.SH'")
        result=self.ingest('index_daily',params={**self.params,'ts_codes':['000300.SH']})
        self.assertTrue(any(u.status=='UNKNOWN' for u in result.units));self.assertEqual(self.client.calls,[])
        self.con.execute("UPDATE canonical.index_basic SET market='CSI' WHERE ts_code='000300.SH'")
        self.con.execute("DELETE FROM meta.coverage WHERE dataset='trade_calendar' AND unit_key='SSE:2020-03-14'")
        result=self.ingest('index_daily')
        self.assertEqual(result.status,'FAILED');self.assertEqual(self.client.calls,[])
        # 另用仍有依赖的单日，删除实际日历事实，验证不能从零行行情判定闭市。
        self.con.execute("DELETE FROM canonical.trade_calendar WHERE cal_date='2020-03-12'")
        result=self.ingest(params={'start_date':'20200312','end_date':'20200312'})
        self.assertEqual(result.units[0].status,'UNKNOWN');self.assertEqual(self.client.calls,[])

    def test_snapshot_and_excluded_calendar_revision_protect_old_data(self):
        """版本复核包括 universe 及排除日；历史数据/成功指针保持旧版。"""
        self.ingest('index_daily');before=self.current('index_daily')
        for target in ('index_basic','trade_calendar'):
            changed=[]
            def mutate(api,params,data):
                if not changed:
                    self.con.execute("UPDATE meta.coverage SET publication_id=publication_id+1000 WHERE dataset=? AND unit_key=?",
                                     [target,'SNAPSHOT' if target=='index_basic' else 'SSE:2020-03-14'])
                    changed.append(True)
            result=self.ingest('index_daily',client=QuoteClient(transform=mutate))
            self.assertEqual(result.status,'FAILED',result)
            self.assertIn('Dependency changed',result.error)
            self.assertEqual(self.current('index_daily'),before)

    def test_cross_year_requests_and_failure_only_blocks_its_year(self):
        """同一代码按年拆请求，某年失败不污染另一年合格 Unit。"""
        records=[{'exchange':'SSE','cal_date':d,'is_open':opened,'pretrade_date':prev} for d,opened,prev in
                 [('20201231',1,'20201230'),('20210101',0,'20201231'),('20210102',0,'20201231'),
                  ('20210103',0,'20201231'),('20210104',1,'20201231')]]
        class Calendar:
            def query(self,api,params,fields,*,request_id,on_attempt):
                selected=[r for r in records if params['start_date']<=r['cal_date']<=params['end_date']]
                body={'code':0,'data':{'fields':list(fields),'items':[[r[f] for f in fields] for r in selected],'has_more':False}}
                on_attempt(RequestAttempt(request_id,1,api,params,fields,datetime.now(timezone.utc),0,200,0,fields,len(selected),False),body)
                return body
        dates={'start_date':'20201231','end_date':'20210104'}
        result=runner.run(self.con,TradeCalendarDataset(),TushareTradeCalendarAdapter(Calendar()),dates,RunOptions(self.root/'year-calendar'))
        self.assertEqual(result.status,'SUCCESS',result)
        def failure(api,params,data):
            if params['start_date']=='20201231': raise RuntimeError('injected year failure')
        result=self.ingest('index_daily',client=QuoteClient(index=[quote(CODES[0],'20210104')],transform=failure),params={**dates,'ts_codes':[CODES[0]]})
        self.assertEqual(result.status,'PARTIAL',result)
        self.assertEqual(len(self.client.calls),2)
        self.assertEqual([p.unit_key for p in result.publications],['000300.SH:2021-01-04'])

    def test_artifacts_rollback_and_cli(self):
        """完整产物可核验，发布回滚保护原数据，CLI 明确限制代码参数。"""
        first=self.ingest('index_daily',keep=True)
        self.assertTrue(first.artifacts_complete,first)
        root=Path(first.artifact_dir)
        for item in json.loads((root/'manifest.json').read_text())['files']:
            self.assertEqual(hashlib.sha256((root/item['path']).read_bytes()).hexdigest(),item['sha256'])
        before=self.current('index_daily');proxy=Mock(wraps=self.con)
        def execute(sql,*args):
            if ' '.join(sql.split()).startswith('INSERT INTO "canonical"."index_daily"'):
                raise RuntimeError('injected publication failure')
            return self.con.execute(sql,*args)
        proxy.execute.side_effect=execute
        result=self.ingest('index_daily',con=proxy)
        self.assertEqual(result.status,'FAILED');self.assertEqual(self.current('index_daily'),before)
        argv=['quant_data','ingest','index_daily','--db',str(self.path),'--landing-dir',str(self.root/'cli'),
              '--start-date','20200312','--end-date','20200313','--ts-code','000300.SH']
        output=io.StringIO()
        with patch('sys.argv',argv),patch('quant_data.__main__.dotenv_values',return_value={'TUSHARE_TOKEN':'test'}),\
             patch('quant_data.__main__.TushareClient',return_value=QuoteClient()),redirect_stdout(output):
            self.assertEqual(main(),0)
        self.assertEqual(json.loads(output.getvalue())['status'],'SUCCESS')
        with patch('sys.argv',['quant_data','ingest','stock_daily','--start-date','20200312','--end-date','20200313','--ts-code','x']),redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):main()

    @unittest.skipUnless((SAMPLE/'stock_day-attempt-01.json').exists(),'本地真实响应不随 Git 分发')
    def test_saved_real_stock_and_index_signals(self):
        """新完整信封重放；独立解释日期/单位，与真实规范化结果逐字段对照。"""
        class Replay:
            def query(self,api,params,fields,*,request_id,on_attempt):
                label={'daily':'stock_day','index_daily':'index_day','sw_daily':'sw_day'}[api]
                body=json.loads((SAMPLE/(label+'-attempt-01.json')).read_text())['response'];d=body['data']
                on_attempt(RequestAttempt(request_id,1,api,params,fields,datetime.now(timezone.utc),0,200,0,tuple(d['fields']),len(d['items']),d['has_more']),body)
                return deepcopy(body)
        # 仅为离线重放建立显式日历夹具；该测试不作为真实上游或实时接口验收。
        self.con.execute("INSERT INTO canonical.trade_calendar VALUES ('SSE','2026-10-21',true,'2026-10-20')")
        self.con.execute("INSERT INTO meta.coverage VALUES ('trade_calendar','SSE:2026-10-21','PUBLISHED',999,current_timestamp,NULL)")
        params={'start_date':'20261021','end_date':'20261021'}
        for name in self.datasets:
            result=self.ingest(name,client=Replay(),params={**params,**({'ts_codes':CODES[:2]} if name=='index_daily' else {})})
            self.assertEqual(result.status,'SUCCESS',result)
            labels=['stock_day'] if name=='stock_daily' else ['index_day','sw_day']
            expected=[]
            for label in labels:
                d=json.loads((SAMPLE/(label+'-attempt-01.json')).read_text())['response']['data']
                for values in d['items']:
                    r=dict(zip(d['fields'],values));r['trade_date']=datetime.strptime(r['trade_date'],'%Y%m%d').date()
                    if label=='sw_day':r.update(pre_close=None,pct_chg=r.pop('pct_change'),vol=r['vol']*100,amount=r['amount']*10)
                    expected.append(r)
            actual=self.con.execute(f'SELECT * FROM canonical.{name} ORDER BY ts_code,trade_date').pl()
            self.assertTrue(actual.equals(pl.DataFrame(expected,schema=self.datasets[name].schema).sort('ts_code','trade_date')))
