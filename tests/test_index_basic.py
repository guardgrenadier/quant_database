"""指数目标快照：显式宽基请求、申万过滤、原子替换及故障保护。"""

from contextlib import redirect_stdout
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
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import RequestAttempt, RunOptions
from quant_data.datasets.index_basic import BROAD_CODES, SW_CATEGORY, SW_PUBLISHER, IndexBasicDataset
from quant_data.sources.tushare.index_basic import TushareIndexBasicAdapter

REFERENCE = Path('data/probes/st-index-20261021T220900218316/requests')


def row(**changes):
    """完整 13 源字段；可覆盖以注入业务/类型/转换故障。"""
    return dict(dict(ts_code='000300.SH', name=' 沪深300 ', fullname='沪深300指数', market='SSE',
        publisher='中证指数有限公司', index_type=None, category='规模指数', base_date='20041231',
        base_point=1000.0, list_date='20050408', weight_rule='市值加权', desc='源描述', exp_date=None), **changes)


def rows():
    return [row(), row(ts_code='801010.SI', market='SW', category=SW_CATEGORY, publisher=SW_PUBLISHER),
            row(ts_code='801011.SI', market='SW', category=SW_CATEGORY, publisher=SW_PUBLISHER, exp_date='20200101')]


class ProviderClient:
    """只替换通信；响应字段逆序，其他环节全部执行正式代码。"""
    def __init__(self, records=(), transform=None):
        self.records, self.transform, self.calls = records, transform, []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        self.calls.append((api_name, dict(params), tuple(fields)))
        selected = [r for r in self.records if all(r[k] == v for k, v in params.items())]
        returned = list(reversed(fields))
        data = {'fields':returned, 'items':[[r[f] for f in returned] for r in selected], 'has_more':False, 'count':0}
        if self.transform:
            self.transform(params, data)
        body = {'code':0, 'data':data}
        on_attempt(RequestAttempt(request_id,1,api_name,params,tuple(fields),datetime.now(timezone.utc),0.0,
                                  200,0,tuple(data['fields']),len(data['items']),data.get('has_more')),body)
        return body


class IndexBasicTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root/'warehouse.duckdb'
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.dataset = IndexBasicDataset()
        db.initialize(self.con,self.dataset)
        # 单个白名单代码用于边界测试；重放恢复正式名单，测试不会改变生产默认配置。
        for name,value in (('quant_data.sources.tushare.index_basic.REQUEST_INTERVAL',0),
                           ('quant_data.sources.tushare.index_basic.BROAD_CODES',('000300.SH',)),
                           ('quant_data.datasets.index_basic.BROAD_CODES',('000300.SH',))):
            setting=patch(name,value);setting.start();self.addCleanup(setting.stop)

    def ingest(self,records=None,transform=None,keep=False,con=None,params=None):
        self.client=ProviderClient(rows() if records is None else records,transform)
        return runner.run(con or self.con,self.dataset,TushareIndexBasicAdapter(self.client),params or {},RunOptions(self.root/'landing',keep))

    def current(self):
        return [self.con.execute(sql).fetchall() for sql in (
            'SELECT * FROM canonical.index_basic ORDER BY ts_code','SELECT * FROM meta.coverage',
            'SELECT * FROM meta.publication ORDER BY publication_id')]

    def failed_preserving(self,action):
        before=self.current();result=action()
        self.assertEqual(result.status,'FAILED',result)
        self.assertEqual(self.current(),before)
        self.assertTrue(Path(result.artifact_dir).is_dir())
        return result

    def errors(self,result):
        return self.con.execute("SELECT rule,stage FROM meta.validation_check WHERE run_id=? AND level='ERROR'",[result.run_id]).fetchall()

    def test_request_plan_rerun_and_replacement(self):
        """仅请求白名单及申万过滤，终止记录保留；重跑替换并删除非目标旧数据。"""
        first=self.ingest()
        self.assertEqual(first.status,'SUCCESS',first)
        self.assertEqual([(api,params) for api,params,_ in self.client.calls],
            [('index_basic',{'ts_code':'000300.SH'}),('index_basic',{'category':SW_CATEGORY,'publisher':SW_PUBLISHER})])
        self.assertTrue(all(fields==tuple(self.dataset.schema.names()) for _,_,fields in self.client.calls))
        self.assertEqual(first.publications[0].row_count,3)
        self.assertEqual(self.con.execute('SELECT count(*) FROM meta.run_dependency').fetchone()[0],0)
        repeat=self.ingest()
        self.assertGreater(repeat.publications[0].publication_id,first.publications[0].publication_id)
        self.con.execute("INSERT INTO canonical.index_basic(ts_code,name,market,publisher,category) VALUES ('old','旧非目标','CNI','P','风格指数')")
        last=self.ingest(rows()[:2])
        self.assertEqual(last.status,'SUCCESS')
        self.assertEqual(self.con.execute('SELECT count(*) FROM canonical.index_basic').fetchone()[0],2)
        self.assertEqual(self.con.execute('SELECT status,publication_id FROM meta.coverage').fetchone(),('PUBLISHED',last.publications[0].publication_id))

    def test_missing_whitelist_and_empty_sw_preserve_old(self):
        """宽基缺失和申万零行分别阻断；完整空响应保持 COMPLETE，空表仍有正式 Schema。"""
        empty=self.ingest([],keep=True)
        self.assertEqual(empty.status,'FAILED')
        self.assertEqual(pl.read_parquet(Path(empty.artifact_dir)/'candidate.parquet').schema,self.dataset.schema)
        self.assertEqual(self.con.execute('SELECT delivery_state FROM meta.run_unit').fetchone()[0],'COMPLETE')
        self.assertEqual(self.con.execute('SELECT status,publication_id FROM meta.coverage').fetchone(),('FAILED',None))
        self.ingest()
        for records,rule in ((rows()[1:],'index.broad_missing'),(rows()[:1],'index.sw_nonempty')):
            result=self.failed_preserving(lambda:self.ingest(records))
            self.assertIn((rule,'validation'),self.errors(result))

    def test_source_failures_fields_types_truncation_and_range(self):
        """两类请求失败、错误筛选结果、缺列、严格类型和截断均保护整个旧快照。"""
        self.ingest()
        def missing(params,data):
            i=data['fields'].index('exp_date');data['fields'].pop(i)
            data['items']=[r[:i]+r[i+1:] for r in data['items']]
        def numeric(params,data):
            if data['items']:data['items'][0][data['fields'].index('ts_code')]=123
        def wrong(params,data):
            field='ts_code' if 'ts_code' in params else 'publisher'
            if data['items']:data['items'][0][data['fields'].index(field)]='wrong'
        def more(params,data):data['has_more']=True
        def no_marker(params,data):data.pop('has_more')
        def failure(params,data):raise RuntimeError('injected request failure')
        for transform,rule,stage in ((missing,'source.fields','prepare'),(numeric,'source.types','prepare'),
            (wrong,'source.range','prepare'),(more,'source.truncated','fetch'),(no_marker,'source.truncated','fetch'),
            (failure,'source.request','fetch')):
            for target in ('broad','sw'):
                def change(params,data):
                    if ('ts_code' in params)==(target=='broad'):transform(params,data)
                result=self.failed_preserving(lambda:self.ingest(transform=change))
                self.assertIn((rule,stage),self.errors(result))
        with patch('quant_data.sources.tushare.index_basic.ROW_LIMIT',2):
            self.assertIn(('source.truncated','fetch'),self.errors(self.failed_preserving(lambda:self.ingest())))

    def test_business_and_conversion_errors(self):
        """基础约束保持；不静默去重，非法日期和基点类型不转为合法 NULL。"""
        self.ingest()
        for field,value,rule,stage in (('name',' ','index.required','validation'),
            ('base_point',float('inf'),'index.base_point_finite','validation'),
            ('base_point',True,'source.types','prepare'),('base_point','1000','source.types','prepare'),
            ('base_date','20260230','source.date_parse','prepare'),('list_date','2026101','source.date_format','prepare')):
            result=self.failed_preserving(lambda:self.ingest([row(**{field:value}),*rows()[1:]]))
            self.assertIn((rule,stage),self.errors(result))
        result=self.failed_preserving(lambda:self.ingest([*rows(),rows()[1]]))
        self.assertIn(('index.key_unique','validation'),self.errors(result))
        result=self.ingest([row(base_point=0,base_date='20120427'),*rows()[1:]])
        self.assertEqual(result.status,'SUCCESS',result)

    def test_raw_manifest_and_evidence_failure(self):
        """Raw 只需业务字段加请求标签；原始空白保留，证据写失败阻断。"""
        result=self.ingest(keep=True);self.assertTrue(result.artifacts_complete,result)
        root=Path(result.artifact_dir);raw=pl.read_parquet(root/'landing.parquet')
        self.assertEqual(raw.width,14)
        self.assertEqual(raw['name'].to_list(),[' 沪深300 ']*3)
        self.assertEqual(pl.read_parquet(root/'candidate.parquet')['name'].to_list(),['沪深300']*3)
        for item in json.loads((root/'manifest.json').read_text())['files']:
            self.assertEqual(hashlib.sha256((root/item['path']).read_bytes()).hexdigest(),item['sha256'])
        with patch.object(Artifacts,'record_attempt',side_effect=OSError('injected evidence failure')):
            self.failed_preserving(lambda:self.ingest())

    def test_rollback_and_cli(self):
        """发布删除后失败回滚；CLI 保持固定目标入口和退出状态，不增加兼容模式。"""
        self.ingest();proxy=Mock(wraps=self.con)
        def execute(sql,*args):
            if ' '.join(sql.split()).startswith('INSERT INTO "canonical"."index_basic"'):
                raise RuntimeError('injected publication failure')
            return self.con.execute(sql,*args)
        proxy.execute.side_effect=execute
        self.failed_preserving(lambda:self.ingest(con=proxy))
        self.assertEqual(self.ingest(params={'category':'行业指数'}).status,'FAILED')
        self.assertEqual(self.client.calls,[])
        argv=['quant_data','ingest','index_basic','--db',str(self.path),'--landing-dir',str(self.root/'cli')]
        for records,expected in ((rows(),0),([],1)):
            output=io.StringIO()
            with patch('sys.argv',argv),patch('quant_data.__main__.dotenv_values',return_value={'TUSHARE_TOKEN':'test-secret'}), \
                patch('quant_data.__main__.TushareClient',return_value=ProviderClient(records)),redirect_stdout(output):
                self.assertEqual(main(),expected)

    @unittest.skipUnless(REFERENCE.exists(),'真实基础响应不随 Git 分发')
    def test_saved_real_filtered_replay(self):
        """用真实源记录生成明确过滤响应，独立比较全部目标，不再需要分类接口样本。"""
        source=[]
        for p in sorted(REFERENCE.glob('*.json')):
            d=json.loads(p.read_text())['response']['data']
            source.extend(dict(zip(d['fields'],r)) for r in d['items'])
        expected=[]
        for r in source:
            if r['ts_code'] in BROAD_CODES or (r['category']==SW_CATEGORY and r['publisher']==SW_PUBLISHER):
                record={k:(v.strip() or None) if isinstance(v,str) else v for k,v in r.items()}
                for field in ('base_date','list_date','exp_date'):
                    if record[field] is not None:record[field]=datetime.strptime(record[field],'%Y%m%d').date()
                expected.append(record)
        with patch('quant_data.sources.tushare.index_basic.BROAD_CODES',BROAD_CODES),patch('quant_data.datasets.index_basic.BROAD_CODES',BROAD_CODES):
            result=runner.run(self.con,self.dataset,TushareIndexBasicAdapter(ProviderClient(source)),{},RunOptions(self.root/'replay'))
        self.assertEqual(result.status,'SUCCESS',result)
        actual=self.con.execute('SELECT * FROM canonical.index_basic ORDER BY ts_code').pl()
        self.assertTrue(actual.equals(pl.DataFrame(expected,schema=self.dataset.schema).sort('ts_code')))
        self.assertEqual(actual.height,len(BROAD_CODES)+769)
