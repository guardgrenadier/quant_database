"""真实 dbt 子进程验证 Serving 契约；所有输入均为明确的临时库夹具。"""

import json
import os
from pathlib import Path
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest

import duckdb

from quant_data import db
from quant_data.datasets.index_member_all import IndexMemberAllDataset
from quant_data.datasets.stock_daily import StockDailyDataset
from quant_data.datasets.stock_1min import Stock1MinDataset

ROOT = Path(__file__).resolve().parents[1]


class DbtServingTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / 'warehouse.duckdb'
        with duckdb.connect(str(self.path)) as con:
            for dataset in (StockDailyDataset(), IndexMemberAllDataset(), Stock1MinDataset()):
                db.initialize(con, dataset)
            # 明确测试夹具：覆盖旧区间结束日、新区间开始日、无归属股票和历史断档。
            con.execute("""
                insert into canonical.stock_daily
                select code, day::date, 10, 12, 9, 11, 10, 1, 10, 2.5, 3
                from (values ('000001.SZ','2020-03-12'),('000001.SZ','2020-03-13'),
                             ('000002.SZ','2020-03-13'),('000003.SZ','2020-03-13')) as v(code,day)
            """)
            con.execute("""
                insert into canonical.index_member_all values
                ('L1-old',NULL,'L2-old',NULL,'L3-old',NULL,'000001.SZ',NULL,'2010-01-01','2020-03-12','N'),
                ('L1-new',NULL,'L2-new',NULL,'L3-new',NULL,'000001.SZ',NULL,'2020-03-13',NULL,'Y'),
                ('L1-gap',NULL,'L2-gap',NULL,'L3-gap',NULL,'000003.SZ',NULL,'2010-01-01','2020-03-12','N'),
                ('L1-gap',NULL,'L2-gap',NULL,'L3-gap',NULL,'000003.SZ',NULL,'2020-03-16',NULL,'Y')
            """)
            # 分钟夹具覆盖日线初始化、TRADED 前向继承及停牌期间的连续参考价。
            con.execute("""
                insert into canonical.stock_1min values
                ('000001.SZ','2020-03-12','09:30','TRADED',11,12,10,11,100,1100),
                ('000001.SZ','2020-03-12','09:31','NO_TRADE',11,11,11,11,0,0),
                ('000001.SZ','2020-03-12','09:32','SUSPENDED',NULL,NULL,NULL,NULL,0,0),
                ('000002.SZ','2020-03-13','09:30','NO_TRADE',10,10,10,10,0,0),
                ('000002.SZ','2020-03-13','09:31','TRADED',10,11,10,10.5,100,1050),
                ('000002.SZ','2020-03-13','09:32','NO_TRADE',10.5,10.5,10.5,10.5,0,0)
            """)
            # 成功元数据为测试夹具，不将手工插入称作真实 ingestion 或供应商发布。
            con.execute("""
                insert into meta.run(run_id,dataset,params,status,artifact_dir)
                values ('fixture','fixture','{}','SUCCESS','test-fixture');
                insert into meta.publication(dataset,unit_key,row_count,run_id) values
                    ('stock_daily','2020-03-12',1,'fixture'),('stock_daily','2020-03-13',3,'fixture'),
                    ('index_member_all','SNAPSHOT',4,'fixture');
                insert into meta.coverage
                select dataset,unit_key,'PUBLISHED',publication_id,current_timestamp,NULL from meta.publication
            """)

    def build(self):
        # 复用已安装的 dbt，不调用安装器或供应商；产物及日志写入测试临时目录。
        env = {**os.environ, 'QUANT_DATA_DB': str(self.path), 'DBT_TARGET_PATH': str(self.root/'target'),
               'DBT_LOG_PATH': str(self.root/'logs'), 'DBT_SEND_ANONYMOUS_USAGE_STATS': 'false'}
        result = subprocess.run([str(Path(sys.executable).with_name('dbt')), 'build', '--project-dir', str(ROOT/'dbt'),
                                 '--profiles-dir', str(ROOT/'dbt'), '--no-use-colors'],
                                env=env, cwd=ROOT, capture_output=True, text=True, timeout=90)
        return result

    def snapshot(self):
        with duckdb.connect(str(self.path), read_only=True) as con:
            return [con.execute(sql).fetchall() for sql in (
                'select * from canonical.stock_daily order by ts_code,trade_date',
                'select * from canonical.index_member_all order by ts_code,in_date',
                'select * from canonical.stock_1min order by ts_code,trade_date,bar_time',
                'select * from meta.coverage order by dataset,unit_key',
                'select * from meta.publication order by publication_id')]

    def test_inclusive_match_nulls_grain_and_live_view_revision(self):
        before = self.snapshot()
        result = self.build()
        self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
        self.assertEqual(self.snapshot(), before)
        results = json.loads((self.root/'target/run_results.json').read_text())['results']
        unmatched = next(r for r in results if r['unique_id'].endswith('.stock_daily_industry_unmatched'))
        self.assertEqual((unmatched['status'], unmatched['failures']), ('warn', 2))
        with duckdb.connect(str(self.path)) as con:
            self.assertEqual(con.execute("select table_type from information_schema.tables where table_schema='serving' "
                                         "and table_name='stock_daily_with_sw_industry'").fetchone()[0], 'VIEW')
            self.assertEqual(con.execute('select ts_code,trade_date::varchar,sw_l1_code,sw_l2_code,sw_l3_code '
                                         'from serving.stock_daily_with_sw_industry order by ts_code,trade_date').fetchall(),
                             [('000001.SZ','2020-03-12','L1-old','L2-old','L3-old'),
                              ('000001.SZ','2020-03-13','L1-new','L2-new','L3-new'),
                              ('000002.SZ','2020-03-13',None,None,None),
                              ('000003.SZ','2020-03-13',None,None,None)])
            # View 应保留原始日线所有字段，且只追加约定的三列。
            columns = [r[0] for r in con.execute('describe serving.stock_daily_with_sw_industry').fetchall()]
            self.assertEqual(columns, StockDailyDataset.schema.names()+['sw_l1_code','sw_l2_code','sw_l3_code'])
            # 分钟 View 只增加派生参考价，不重写 Canonical 的十列事实。
            minute_columns = [r[0] for r in con.execute('describe serving.stock_1min_with_reference').fetchall()]
            self.assertEqual(minute_columns, Stock1MinDataset.schema.names()+['reference_price'])
            references = con.execute("""
                select ts_code,bar_time::varchar,reference_price
                from serving.stock_1min_with_reference order by ts_code,trade_date,bar_time
            """).fetchall()
            self.assertEqual(references, [
                ('000001.SZ','09:30:00',11.0),('000001.SZ','09:31:00',11.0),
                ('000001.SZ','09:32:00',11.0),('000002.SZ','09:30:00',10.0),
                ('000002.SZ','09:31:00',10.5),('000002.SZ','09:32:00',10.5)])
            con.execute("update canonical.index_member_all set l3_code='L3-revised' where l3_code='L3-new'")
            self.assertEqual(con.execute("select sw_l3_code from serving.stock_daily_with_sw_industry "
                                         "where ts_code='000001.SZ' and trade_date='2020-03-13'").fetchone()[0], 'L3-revised')
            # 普通 View 使用当前日线；上游修订不冒充固定历史版本。
            con.execute("update canonical.stock_daily set pre_close=9.75 "
                        "where ts_code='000002.SZ' and trade_date='2020-03-13'")
            self.assertEqual(con.execute("select reference_price from serving.stock_1min_with_reference "
                                         "where ts_code='000002.SZ' and bar_time=time '09:30'").fetchone()[0], 9.75)

    def test_unpublished_snapshot_or_daily_unit_blocks_before_view(self):
        for dataset, unit in (('index_member_all','SNAPSHOT'), ('stock_daily','2020-03-13')):
            with duckdb.connect(str(self.path)) as con:
                con.execute("update meta.coverage set status='FAILED',publication_id=NULL where dataset=? and unit_key=?",
                            [dataset,unit])
            result = self.build()
            self.assertNotEqual(result.returncode, 0, result.stdout+result.stderr)
            self.assertIn('Upstream data not published', result.stdout+result.stderr)
            with duckdb.connect(str(self.path)) as con:
                self.assertEqual(con.execute("select count(*) from information_schema.tables where table_schema='serving' "
                                             "and table_name='stock_daily_with_sw_industry'").fetchone()[0], 0)
                con.execute("update meta.coverage set status='PUBLISHED',publication_id=p.publication_id "
                            "from meta.publication p where meta.coverage.dataset=p.dataset "
                            "and meta.coverage.unit_key=p.unit_key")

    def test_multiple_matches_fail_grain_without_deduplicating(self):
        with duckdb.connect(str(self.path)) as con:
            # 绕过正式 ingestion 的显式坏夹具，验证 Serving 不以任选一条隐藏多重匹配。
            con.execute("insert into canonical.index_member_all values "
                        "('L1-conflict',NULL,'L2-conflict',NULL,'L3-conflict',NULL,'000001.SZ',NULL,'2019-01-01',NULL,'Y')")
        result = self.build()
        self.assertNotEqual(result.returncode, 0, result.stdout+result.stderr)
        results = json.loads((self.root/'target/run_results.json').read_text())['results']
        grain = next(r for r in results if r['unique_id'].endswith('.stock_daily_industry_grain'))
        self.assertEqual((grain['status'], grain['failures']), ('fail', 2))


if __name__ == '__main__':
    unittest.main()
