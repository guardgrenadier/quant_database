"""验证 dbt 子进程输出在成功和失败时都进入 run 级日志。"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch

from quant_ingest import config
from quant_ingest.dbt_runner import ROOT, run_dbt
from quant_ingest.raw import RawStore


class DbtRunnerTest(unittest.TestCase):
    """覆盖输出保留、返回码阻断和密钥脱敏。"""

    def test_success_and_failure_are_logged_before_return(self):
        """成功保留完整输出，失败先落日志再抛异常。"""

        with tempfile.TemporaryDirectory() as directory:
            raw = RawStore(Path(directory) / "raw")
            day = date(2026, 9, 18)
            project = ROOT / "dbt_v2"
            kwargs = dict(project=project, ledger_database=Path(directory) / "state.duckdb",
                          raw=raw, dataset="stock_basic", day=day, run_id="offline")
            with patch.dict(os.environ, {"TUSHARE_API_KEY": "private-key"}):
                with patch("quant_ingest.dbt_runner.subprocess.run", return_value=subprocess.CompletedProcess(
                    [], 0, stdout="success private-key", stderr="",
                )) as command:
                    run_dbt("run", "stock_basic", {}, **kwargs)
                    self.assertNotIn("TUSHARE_API_KEY", command.call_args.kwargs["env"])
                    self.assertEqual(command.call_args.kwargs["env"]["QUANT_V2_DBT_PATH"],
                                     str(config.dbt_db_for(kwargs["ledger_database"])))
                with patch("quant_ingest.dbt_runner.subprocess.run", return_value=subprocess.CompletedProcess(
                    [], 1, stdout="partial", stderr="failed private-key",
                )):
                    with self.assertRaisesRegex(RuntimeError, "failed"):
                        run_dbt("test", "basic_contract", {}, **kwargs)

            log = raw.root / "stock_basic" / "trade_date=20260918" / "runs" / "offline.jsonl"
            records = [json.loads(line) for line in log.read_text().splitlines()]
            self.assertEqual([item["returncode"] for item in records], [0, 1])
            self.assertEqual(records[0]["stdout"], "success [redacted]")
            self.assertEqual(records[1]["stderr"], "failed [redacted]")
            self.assertNotIn("private-key", log.read_text())


if __name__ == "__main__":
    unittest.main()
