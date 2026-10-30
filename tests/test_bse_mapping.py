"""北交所代码映射正式链路：完整快照、严格转换、整表替换及失败保护。"""

from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.core import runner
from quant_data.core.models import RequestAttempt, RunOptions
from quant_data.datasets.bse_mapping import BseMappingDataset
from quant_data.sources.tushare.bse_mapping import TushareBseMappingAdapter


def row(**changes):
    """完整四字段样本，可覆盖单项以注入源或业务错误。"""
    return dict(dict(name=" 方大新材 ", o_code="838163.BJ", n_code="920163.BJ", list_date="20200727"), **changes)


def rows():
    return [row(), row(name="示例股份", o_code="838164.BJ", n_code="920164.BJ", list_date="20200810")]


class ProviderClient:
    """只替换供应商通信；响应字段逆序，正式 Adapter 和 Runner 均实际执行。"""

    def __init__(self, records=(), transform=None):
        self.records, self.transform, self.calls = records, transform, []

    def query(self, api_name, params, fields, *, request_id, on_attempt):
        self.calls.append((api_name, dict(params), tuple(fields)))
        returned = tuple(reversed(fields))
        data = {"fields": list(returned), "items": [[r[f] for f in returned] for r in self.records],
                "has_more": False, "count": 0}
        if self.transform:
            self.transform(data)
        body = {"code": 0, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api_name, params, tuple(fields), datetime.now(timezone.utc), 0.0,
                                  200, 0, tuple(data["fields"]), len(data["items"]), data.get("has_more")), body)
        return body


class BseMappingTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.dataset = BseMappingDataset()
        db.initialize(self.con, self.dataset)

    def ingest(self, records=None, transform=None, keep=False, con=None, params=None):
        self.client = ProviderClient(rows() if records is None else records, transform)
        return runner.run(con or self.con, self.dataset, TushareBseMappingAdapter(self.client),
                          params or {}, RunOptions(self.root / "landing", keep))

    def current(self):
        return [self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.bse_mapping ORDER BY o_code",
            "SELECT * FROM meta.coverage WHERE dataset='bse_mapping'",
            "SELECT * FROM meta.publication WHERE dataset='bse_mapping' ORDER BY publication_id")]

    def failed_preserving(self, action):
        before = self.current()
        result = action()
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(result.publications, ())
        self.assertEqual(self.current(), before)
        self.assertTrue(Path(result.artifact_dir).is_dir())
        return result

    def errors(self, result):
        return self.con.execute(
            "SELECT rule,stage FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id]
        ).fetchall()

    def test_single_request_rerun_and_snapshot_replacement(self):
        """单次无过滤请求显式四字段；重跑生成新版本，整表替换不累加。"""
        first = self.ingest(keep=True)
        self.assertEqual(first.status, "SUCCESS", first)
        self.assertEqual(self.client.calls, [("bse_mapping", {}, ("name", "o_code", "n_code", "list_date"))])
        self.assertEqual(first.publications[0].row_count, 2)
        self.assertEqual(self.con.execute(
            "SELECT count(*) FROM meta.run_dependency WHERE run_id=?", [first.run_id]).fetchone()[0], 0)
        raw = pl.read_parquet(Path(first.artifact_dir) / "landing.parquet")
        self.assertEqual(raw.schema, pl.Schema({c: pl.String for c in self.dataset.schema.names()}))
        self.assertEqual(raw["name"].to_list(), [" 方大新材 ", "示例股份"])
        candidate = pl.read_parquet(Path(first.artifact_dir) / "candidate.parquet")
        self.assertEqual(candidate.schema, self.dataset.schema)
        self.assertEqual(candidate.sort("o_code")["name"].to_list(), ["方大新材", "示例股份"])
        self.assertEqual(candidate.sort("o_code")["list_date"].to_list(), [date(2020, 7, 27), date(2020, 8, 10)])

        repeat = self.ingest()
        self.assertEqual(repeat.status, "SUCCESS", repeat)
        self.assertGreater(repeat.publications[0].publication_id, first.publications[0].publication_id)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.bse_mapping").fetchone()[0], 2)
        self.con.execute("INSERT INTO canonical.bse_mapping VALUES ('旧记录','830001.BJ','920001.BJ','2020-01-01')")
        last = self.ingest([row(name="更新名称")])
        self.assertEqual(last.status, "SUCCESS", last)
        self.assertEqual(self.con.execute("SELECT name,o_code,n_code,list_date FROM canonical.bse_mapping").fetchall(),
                         [("更新名称", "838163.BJ", "920163.BJ", date(2020, 7, 27))])
        self.assertEqual(self.con.execute(
            "SELECT status,publication_id FROM meta.coverage WHERE dataset='bse_mapping'").fetchone(),
            ("PUBLISHED", last.publications[0].publication_id))

    def test_complete_empty_is_blocked_and_preserves_old_snapshot(self):
        """完整空响应产生空 Candidate，但 nonempty 阻断且不清除旧版本。"""
        self.ingest()
        result = self.failed_preserving(lambda: self.ingest([], keep=True))
        self.assertIn(("bse.nonempty", "validation"), self.errors(result))
        self.assertEqual(self.con.execute(
            "SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchone(), ("COMPLETE",))
        candidate = pl.read_parquet(Path(result.artifact_dir) / "candidate.parquet")
        self.assertEqual(candidate.height, 0)
        self.assertEqual(candidate.schema, self.dataset.schema)

    def test_missing_fields_bad_types_and_request_failure(self):
        """缺列、四字段坏类型和请求失败均阻断，旧 Canonical 与版本保持不变。"""
        self.ingest()

        def missing(data):
            index = data["fields"].index("name")
            data["fields"].pop(index)
            data["items"] = [r[:index] + r[index + 1:] for r in data["items"]]

        result = self.failed_preserving(lambda: self.ingest(transform=missing))
        self.assertIn(("source.fields", "prepare"), self.errors(result))
        for field, value in (("name", False), ("o_code", 838163), ("n_code", 920163), ("list_date", 20200727)):
            def bad_type(data, field=field, value=value):
                data["items"][0][data["fields"].index(field)] = value
            with self.subTest(field=field):
                result = self.failed_preserving(lambda: self.ingest(transform=bad_type))
                self.assertIn(("source.types", "prepare"), self.errors(result))

        def failure(data):
            raise RuntimeError("injected request failure")

        result = self.failed_preserving(lambda: self.ingest(transform=failure))
        self.assertIn(("source.request", "fetch"), self.errors(result))
        self.assertEqual(self.con.execute(
            "SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchone(), ("FAILED",))

    def test_truncation_marker_and_row_limit(self):
        """has_more 缺失/true 和触及单次上限均 INCOMPLETE，不分页发布。"""
        self.ingest()
        for present in (True, False):
            def more(data, present=present):
                if present:
                    data["has_more"] = True
                else:
                    del data["has_more"]
            with self.subTest(has_more=present):
                result = self.failed_preserving(lambda: self.ingest(transform=more))
                self.assertIn(("source.truncated", "fetch"), self.errors(result))
                self.assertEqual(self.con.execute(
                    "SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchone(),
                    ("INCOMPLETE",))
                self.assertEqual(len(self.client.calls), 1)
        with patch("quant_data.sources.tushare.bse_mapping.ROW_LIMIT", 2):
            result = self.failed_preserving(lambda: self.ingest())
        self.assertIn(("source.truncated", "fetch"), self.errors(result))

    def test_bad_dates_and_business_invariants(self):
        """格式和历法错误不转 NULL；必填、BJ 后缀和新旧不同均阻断。"""
        self.ingest()
        for value, rule in (("2020727", "source.date_format"), ("2026-07-27", "source.date_format"),
                            ("20260230", "source.date_parse")):
            with self.subTest(value=value):
                result = self.failed_preserving(lambda: self.ingest([row(list_date=value), rows()[1]]))
                self.assertIn((rule, "prepare"), self.errors(result))
        for field, value, rule in (
            ("name", " ", "bse.required"), ("o_code", None, "bse.required"),
            ("n_code", "", "bse.required"), ("list_date", None, "bse.required"),
            ("o_code", "838163.SZ", "bse.code_suffix"), ("n_code", "920163", "bse.code_suffix"),
            ("n_code", "838163.BJ", "bse.code_changed"),
        ):
            with self.subTest(field=field, value=value):
                result = self.failed_preserving(lambda: self.ingest([row(**{field: value}), rows()[1]]))
                self.assertIn((rule, "validation"), self.errors(result))

    def test_old_and_new_code_duplicates(self):
        """o_code 主键与 n_code 全快照唯一分别验收，不静默去重。"""
        self.ingest()
        cases = (([row(), row(name="另一名称", n_code="920999.BJ")], "bse.o_code_unique"),
                 ([row(), row(name="另一名称", o_code="838999.BJ")], "bse.n_code_unique"))
        for records, rule in cases:
            with self.subTest(rule=rule):
                result = self.failed_preserving(lambda: self.ingest(records))
                self.assertIn((rule, "validation"), self.errors(result))

    def test_publication_failure_rolls_back_and_subsets_are_rejected(self):
        """删除旧快照后若插入失败，事务回滚；任何子集参数在请求前拒绝。"""
        self.ingest()
        proxy = Mock(wraps=self.con)

        def execute(sql, *args):
            if " ".join(sql.split()).startswith('INSERT INTO "canonical"."bse_mapping"'):
                raise RuntimeError("injected publication failure")
            return self.con.execute(sql, *args)

        proxy.execute.side_effect = execute
        result = self.failed_preserving(lambda: self.ingest([row(name="不会发布")], con=proxy))
        self.assertIn("injected publication failure", result.error)
        result = self.ingest(params={"o_code": "838163.BJ"})
        self.assertEqual(result.status, "FAILED")
        self.assertEqual(self.client.calls, [])


if __name__ == "__main__":
    unittest.main()
