"""仅用离线命令替身验证交接脚本的范围、日志和失败停止，不调用供应商。"""

import json
import csv
import os
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest


SCRIPTS = Path(__file__).resolve().parents[1] / 'scripts'
PROXIES = ('HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
           'http_proxy', 'https_proxy', 'all_proxy', 'no_proxy')


class GalaxyHandoffTest(unittest.TestCase):
    def test_fixed_scope_logs_and_stop_on_failure(self):
        for name, day, failed in (
                (name, day, failed)
                for name, day in (('run_galaxy_validation_20261030.sh', '20261030'),
                                  ('run_galaxy_pilot_20130104.sh', '20130104'))
                for failed in ('', 'stock_1min')):
            with self.subTest(script=name, failed=failed), TemporaryDirectory() as temp:
                root = Path(temp) / 'workspace with spaces'
                (root / 'scripts').mkdir(parents=True)
                script = root / 'scripts' / name
                shutil.copyfile(SCRIPTS / name, script)
                (root / 'data').mkdir()
                database = root / 'data/warehouse.duckdb'
                database.touch()
                python = root / '.venv/bin/python'
                python.parent.mkdir(parents=True)
                # 显式离线替身：只记录 argv/环境并返回退出码，不导入正式 CLI。
                python.write_text(f'#!{sys.executable}\n' + '''import json, os, sys
from pathlib import Path
with Path('calls.jsonl').open('a') as stream:
    checked_env = {key: value for key, value in os.environ.items()
                   if key.lower() in ('http_proxy', 'https_proxy', 'all_proxy', 'no_proxy', 'pythondontwritebytecode')}
    stream.write(json.dumps({'args': sys.argv[1:], 'env': checked_env}) + '\\n')
failed = sys.argv[4] == os.environ['HANDOFF_TEST_FAILED']
print(json.dumps({'status': 'PARTIAL' if failed else 'SUCCESS'}))
print('offline handoff fixture', file=sys.stderr)
raise SystemExit(1 if failed else 0)
''')
                python.chmod(0o700)
                env = {**os.environ, **dict.fromkeys(PROXIES, 'offline-test-proxy'),
                       'HANDOFF_TEST_FAILED': failed}
                # 无参数约束与已有库门禁：不能因误传参数或缺库而启动获取。
                rejected = subprocess.run(['bash', str(script), '20261102'], env=env, capture_output=True)
                self.assertEqual(rejected.returncode, 2)
                database.unlink()
                rejected = subprocess.run(['bash', str(script)], env=env, capture_output=True)
                self.assertNotEqual(rejected.returncode, 0)
                self.assertFalse((root / 'calls.jsonl').exists())
                self.assertFalse(database.exists())
                database.touch()
                result = subprocess.run(['bash', str(script)], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 1 if failed else 0, result.stderr)
                calls = [json.loads(line) for line in (root / 'calls.jsonl').read_text().splitlines()]
                # 本次补跑只允许启动分钟命令，不得重新请求已确认供应商滞后的权重。
                expected = ['stock_1min']
                self.assertEqual([call['args'][3] for call in calls], expected)
                folder, = (root / 'data/landing').iterdir()
                for dataset, call in zip(expected, calls, strict=True):
                    args = ['-m', 'quant_data', 'ingest', dataset, '--start-date', day,
                            '--end-date', day, '--parallel', '2', '--db', str(database),
                            '--landing-dir', str(root / 'data/landing')]
                    self.assertEqual(call['args'], args)
                    self.assertTrue(all(name not in call['env'] for name in PROXIES))
                    self.assertEqual(call['env']['PYTHONDONTWRITEBYTECODE'], '1')
                    self.assertEqual((folder / f'{dataset}.exit-code').read_text(), '1\n' if dataset == failed else '0\n')
                    self.assertEqual(json.loads((folder / f'{dataset}.json').read_text())['status'],
                                     'PARTIAL' if dataset == failed else 'SUCCESS')
                    self.assertIn('offline handoff fixture', (folder / f'{dataset}.stderr.log').read_text())
                if day == '20130104':
                    self.assertEqual((folder / 'script.exit-code').read_text(), '1\n' if failed else '0\n')
                    with (folder / 'resources.csv').open() as stream:
                        samples = list(csv.DictReader(stream))
                    self.assertTrue(samples)
                    self.assertTrue(all(int(row['elapsed_seconds']) >= 0 and
                                        int(row['process_tree_rss_bytes']) >= 0 and
                                        int(row['database_bytes']) == 0 for row in samples))


if __name__ == '__main__':
    unittest.main()
