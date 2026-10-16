import unittest

from scripts.probe_tushare import cases, summarize


class ProbeSummaryTest(unittest.TestCase):
    """验证探针摘要保留空交付、协议错误和记录粒度的区别。"""

    def test_empty_delivery_and_failure_remain_distinct(self):
        """合法空响应有明确零行数，权限错误不能被转换为零行成功。"""
        empty = summarize({"code": 0, "data": {"fields": ["ts_code", "trade_date"], "items": [], "has_more": False, "count": 0}})
        self.assertEqual(empty["rows"], 0)
        self.assertFalse(empty["has_more"])
        error = summarize({"code": 2002, "msg": "permission denied", "data": None})
        self.assertEqual(error["code"], 2002)
        self.assertNotIn("rows", error)

    def test_bad_shape_and_duplicate_business_keys(self):
        """坏响应形态明确报错，三字段业务键冲突不被不同 timing 掩盖。"""
        self.assertIn("protocol_error", summarize(None))
        self.assertIn("protocol_error", summarize({"code": 0, "data": {"fields": ["ts_code"], "items": [None]}}))
        self.assertIn("protocol_error", summarize({"code": 0, "data": {"fields": ["ts_code"], "items": [["x", "y"]]}}))
        fields = ["ts_code", "trade_date", "suspend_type", "suspend_timing"]
        summary = summarize({"code": 0, "data": {"fields": fields, "items": [
            ["x", "20200312", "S", None], ["x", "20200312", "S", "09:30-10:00"],
            ["x", "20200312", "R", None]], "has_more": True}})
        self.assertEqual(summary["duplicate_keys"]["ts_code,trade_date,suspend_type"], 1)
        self.assertTrue(summary["has_more"])

    def test_case_names_are_unique(self):
        """用例名称唯一，避免按名称筛选和写文件时发生证据混淆。"""
        names = [c[0] for c in cases()]
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()
