"""仅验证研究证据的统计语义，不改变正式 ingestion 契约。"""

import unittest

from scripts.probe_tushare_st_index import FIELDS, cases, statistics
from scripts.analyze_tushare_st_index import multiset


class StIndexProbeTest(unittest.TestCase):
    def test_comparisons_preserve_multiplicity(self):
        """分页核对不依赖行列顺序，但重复记录必须影响比较结果。"""
        self.assertEqual(multiset([{"a": 1, "b": 2}, {"a": 3}]), multiset([{"a": 3}, {"b": 2, "a": 1}]))
        self.assertNotEqual(multiset([{"a": 1}]), multiset([{"a": 1}, {"a": 1}]))

    def test_evidence_preserves_duplicates_and_null_distinctions(self):
        """顺序无关指纹保留重复，NULL、空串和缺列分开统计。"""
        fields = FIELDS["stock_st"].split(",")
        row = ["000001.SZ", None, "20261019", "ST", ""]
        body = {"code": 0, "data": {"fields": fields, "items": [row, row]}}
        result = statistics("stock_st", body)
        self.assertEqual(result["duplicate_keys"]["ts_code,trade_date"], 1)
        self.assertEqual(result["json_nulls"]["name"], 2)
        self.assertEqual(result["empty_strings"]["type_name"], 2)
        single = statistics("stock_st", {"code": 0, "data": {"fields": fields, "items": [row]}})
        self.assertNotEqual(single["rows_sha256"], result["rows_sha256"])
        missing = statistics("stock_st", {"code": 0, "data": {"fields": ["ts_code"], "items": []}})
        self.assertIn("trade_date", missing["missing_fields"])

    def test_empty_failure_and_case_identity(self):
        """零行交付与失败不可互换，用例不能覆盖彼此证据。"""
        empty = statistics("stock_st", {"code": 0, "data": {"fields": FIELDS["stock_st"].split(","), "items": []}})
        self.assertEqual(empty["rows"], 0)
        self.assertEqual(empty["missing_fields"], [])
        error = statistics("stock_st", {"code": 2002, "data": None})
        self.assertEqual(error["code"], 2002)
        self.assertNotIn("rows", error)
        names = [c[0] for c in cases()]
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()
