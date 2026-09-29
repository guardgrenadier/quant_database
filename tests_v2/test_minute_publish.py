"""验证分钟 dbt 告警、原子发布和默认消费视图。"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quant_ingest.minute_publish import MinutePublisher
from quant_ingest import config
from tests_v2.test_minute_candidate import CODE, DAY, _setup


def _publish_empty_suspend(ledger, root: Path) -> None:
    """登记同日已发布、但没有停牌证券的 Parquet。"""

    suspend = root / "warehouse" / "canonical" / "suspend_d" / "date=20260918.parquet"
    suspend.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist([], schema=pa.schema([("ts_code", pa.string())])), suspend)
    with duckdb.connect(str(ledger.database)) as conn:
        conn.execute(
            """insert into publications (dataset, trade_date, stage, canonical_path, canonical_sha256)
               values ('suspend_d', ?, 'published', ?, ?)""",
            [DAY, str(suspend), hashlib.sha256(suspend.read_bytes()).hexdigest()],
        )


class MinutePublishTest(unittest.TestCase):
    """用离线 raw 跑通 L2、L3 和长期质量账。"""

    def test_semantic_anomalies_warn_and_publish_full_canonical(self):
        """负量额与填充 bar 保留在 canonical，只在质量账告警。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [
                [CODE, "2026-09-18 09:30:00", "1MIN", 10, 10, 10, 10, 100, 1000],
                [CODE, "2026-09-18 09:31:00", "1MIN", -1, -1, -1, -1, -1, -0.001],
                [CODE, "2026-09-18 12:00:00", "1MIN", 10, 10, 10, 10, 0, 0],
            ]
            ledger, raw = _setup(root, rows)
            publisher = MinutePublisher(ledger, raw, root / "warehouse")
            target = publisher.publish(DAY)
            self.assertEqual(ledger.publication("stk_mins", DAY)[0], "published")
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                canonical_rows = conn.execute("select count(*) from read_parquet(?)", [str(target)]).fetchone()[0]
                findings = conn.execute(
                    "select check_name from quality_findings where dataset = 'stk_mins'"
                ).fetchall()
                ledger_tables = {row[0] for row in conn.execute("show tables").fetchall()}
            with duckdb.connect(str(config.dbt_db_for(ledger.database)), read_only=True) as conn:
                view_rows = conn.execute("select count(*) from minute_bars where trade_date = ?", [DAY]).fetchone()[0]
                work_tables = {row[0] for row in conn.execute("show tables").fetchall()}
            self.assertEqual(canonical_rows, 3)
            self.assertEqual(view_rows, 1)
            self.assertNotIn("minute_bars", ledger_tables)
            self.assertNotIn("plans", work_tables)
            self.assertIn(("negative_value",), findings)
            self.assertIn(("suspend_d_missing",), findings)

    def test_incomplete_plan_blocks_before_dbt(self):
        """存在未决切片时不生成候选，更不能登记 published。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, raw = _setup(root, [])
            with duckdb.connect(str(ledger.database)) as conn:
                conn.execute("update slices set status = 'pending' where dataset = 'stk_mins'")
            publisher = MinutePublisher(ledger, raw, root / "warehouse")
            with self.assertRaisesRegex(ValueError, "incomplete"):
                publisher.publish(DAY)
            self.assertIsNone(ledger.publication("stk_mins", DAY))

    def test_rebuild_recovers_after_replace_before_ledger_update(self):
        """已发布日可从 raw 整日替换；替换后坠机按文件哈希补登记。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 100, 1000]
            ledger, raw = _setup(root, [row])
            publisher = MinutePublisher(ledger, raw, root / "warehouse")
            target = publisher.publish(DAY)
            with patch.object(ledger, "finish_publish", side_effect=RuntimeError("simulated crash")):
                with self.assertRaisesRegex(RuntimeError, "simulated crash"):
                    publisher.publish(DAY, rebuild=True)
            self.assertEqual(ledger.publication("stk_mins", DAY)[0], "prepared")
            self.assertTrue(target.is_file())
            self.assertEqual(publisher.publish(DAY), target)
            self.assertEqual(ledger.publication("stk_mins", DAY)[0], "published")
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                diff = conn.execute(
                    "select finding_count from quality_findings where check_name = 'revision_diff'"
                ).fetchone()
            self.assertEqual(diff, (0,))

    def test_clean_day_has_no_quality_findings(self):
        """同日停牌文件已发布且行情正常时，不把外部零行哨兵记为告警。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 100, 1000]
            ledger, raw = _setup(root, [row])
            _publish_empty_suspend(ledger, root)
            MinutePublisher(ledger, raw, root / "warehouse").publish(DAY)
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                count = conn.execute(
                    "select count(*) from quality_findings where dataset = 'stk_mins'"
                ).fetchone()[0]
            self.assertEqual(count, 0)

    def test_zero_volume_without_suspension_warns(self):
        """有零量骨架但停牌记录为空时发布照常，并保留交叉告警。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 0, 0]
            ledger, raw = _setup(root, [row])
            _publish_empty_suspend(ledger, root)
            MinutePublisher(ledger, raw, root / "warehouse").publish(DAY)
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                count = conn.execute(
                    """select finding_count from quality_findings
                       where dataset = 'stk_mins' and check_name = 'suspension_mismatch'"""
                ).fetchone()
            self.assertEqual(count, (1,))

    def test_revision_diff_counts_keys_and_value_changes(self):
        """整日替换只留差异规模与样例，能发现主键不变的值修订。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger, raw = _setup(root, [])
            publisher = MinutePublisher(ledger, raw, root / "warehouse")
            stamp = datetime(2026, 9, 18, 9, 31)
            def row(code, close):
                """生成一条供修订差异检查使用的分钟记录。"""

                return {"ts_code": code, "trade_time": stamp, "open": 10.0, "high": 10.0,
                        "low": 10.0, "close": close, "vol": 100, "amount": 1000}
            old = root / "old.parquet"
            new = root / "new.parquet"
            pq.write_table(pa.Table.from_pylist([row("000001.SZ", 10.0), row("000002.SZ", 10.0)]), old)
            pq.write_table(pa.Table.from_pylist([row("000001.SZ", 11.0), row("000003.SZ", 10.0)]), new)
            publisher._record_diff(DAY, "diff", old, new)
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                count, samples = conn.execute(
                    "select finding_count, samples_json from quality_findings where check_name = 'revision_diff'"
                ).fetchone()
            self.assertEqual(count, 3)
            summary = json.loads(samples)[0]
            self.assertEqual((summary["added_keys"], summary["removed_keys"], summary["changed_values"]), (1, 1, 1))
            self.assertEqual(len(summary["changed_samples"]), 1)

    def test_quarantine_self_heal_is_written_during_publication(self):
        """已自愈的隔离切片在日级发布时留下长期 warn。"""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            row = [CODE, "2026-09-18 09:31:00", "1MIN", 10, 10, 10, 10, 100, 1000]
            ledger, raw = _setup(root, [row])
            evidence = raw.quarantine("earlier-000001_SZ", "stk_mins", DAY, b'{"code":1}',
                                      slice_key=CODE, reason="minute key has conflicting values")
            target = MinutePublisher(ledger, raw, root / "warehouse").publish(DAY)
            self.assertTrue(target.is_file())
            with duckdb.connect(str(ledger.database), read_only=True) as conn:
                finding = conn.execute(
                    """select severity, finding_count, samples_json from quality_findings
                       where check_name = 'quarantine_self_healed'"""
                ).fetchone()
            self.assertEqual(finding[:2], ("warn", 1))
            self.assertEqual(json.loads(finding[2])[0]["evidence_path"], str(evidence))


if __name__ == "__main__":
    unittest.main()
