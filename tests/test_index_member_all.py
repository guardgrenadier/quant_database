"""申万归属链路：Y/N 完整快照、闭区间、源修订和失败旧版保护。"""

from contextlib import redirect_stderr, redirect_stdout
from datetime import date, datetime, timezone
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
from quant_data.datasets.index_member_all import IndexMemberAllDataset
from quant_data.sources.tushare.index_member_all import TushareIndexMemberAllAdapter

# 真实响应作为仓库固定夹具保存，清理探查目录不影响离线回放。
SAMPLES = Path(__file__).with_name("fixtures") / "index_member_all"


def row(code="000001.SZ", **changes):
    return dict(dict(l1_code="801010.SI", l1_name="一级", l2_code="801011.SI", l2_name="二级",
                     l3_code="850111.SI", l3_name="三级", ts_code=code, name=" 股票名称 ",
                     in_date="20200102", out_date=None, is_new="Y"), **changes)


def rows():
    return [row(), row("000002.SZ"), row("000003.SZ"),
            row(l3_code="850112.SI", in_date="20100104", out_date="20200101", is_new="N"),
            row("000004.SZ", in_date="20150105", out_date="20191231", is_new="N")]


class ProviderClient:
    """仅替换 HTTP；字段重排与真实 offset 分页交给正式 Adapter 处理。"""
    def __init__(self, records=None, transform=None):
        self.records = rows() if records is None else records
        self.transform, self.calls = transform, []

    def query(self, api, params, fields, *, request_id, on_attempt):
        self.calls.append((api, dict(params), tuple(fields)))
        selected = [r for r in self.records if r["is_new"] == params["is_new"]]
        offset, size = params["offset"], params["limit"]
        returned = list(reversed(fields))
        data = {"fields": returned, "items": [[r[f] for f in returned] for r in selected[offset:offset+size]],
                "has_more": offset+size<len(selected), "count": 0}
        if self.transform:
            self.transform(params, data)
        body = {"code": 0, "data": data}
        on_attempt(RequestAttempt(request_id, 1, api, params, tuple(fields), datetime.now(timezone.utc), 0.0,
                                  200, 0, tuple(data["fields"]), len(data["items"]), data.get("has_more")), body)
        return body


class IndexMemberAllTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "warehouse.duckdb"
        self.con = duckdb.connect(str(self.path))
        self.addCleanup(self.con.close)
        self.dataset = IndexMemberAllDataset()
        db.initialize(self.con, self.dataset)
        for name, value in (("REQUEST_INTERVAL", 0), ("PAGE_SIZE", 2)):
            setting = patch(f"quant_data.sources.tushare.index_member_all.{name}", value)
            setting.start()
            self.addCleanup(setting.stop)

    def ingest(self, records=None, transform=None, *, keep=False, params=None, con=None, client=None):
        self.client = client if client is not None else ProviderClient(records, transform)
        return runner.run(con or self.con, self.dataset, TushareIndexMemberAllAdapter(self.client),
                          {} if params is None else params, RunOptions(self.root / "landing", keep))

    def current(self):
        return [self.con.execute(sql).fetchall() for sql in (
            "SELECT * FROM canonical.index_member_all ORDER BY l3_code,ts_code,in_date",
            "SELECT * FROM meta.coverage WHERE dataset='index_member_all'",
            "SELECT * FROM meta.publication WHERE dataset='index_member_all' ORDER BY publication_id")]

    def checks(self, result, level="ERROR"):
        return self.con.execute("SELECT rule,stage,units,detail FROM meta.validation_check WHERE run_id=? AND level=?",
                                [result.run_id, level]).fetchall()

    def failed_preserving(self, **kwargs):
        before = self.current()
        result = self.ingest(**kwargs)
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(result.publications, ())
        self.assertEqual(self.current(), before)
        return result

    def test_complete_snapshot_pagination_artifacts_and_rerun(self):
        result = self.ingest(keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual([(p["is_new"], p["offset"]) for _, p, _ in self.client.calls],
                         [("Y", 0), ("Y", 2), ("N", 0), ("N", 2)])
        self.assertTrue(all(set(p)=={"is_new", "limit", "offset"} for _, p, _ in self.client.calls))
        self.assertEqual([(p.unit_key, p.row_count) for p in result.publications], [("SNAPSHOT", 5)])
        self.assertEqual(self.con.execute("SELECT count(*) FROM meta.run_dependency WHERE run_id=?",
                                         [result.run_id]).fetchone()[0], 0)
        folder = Path(result.artifact_dir)
        raw, candidate = pl.read_parquet(folder / "landing.parquet"), pl.read_parquet(folder / "candidate.parquet")
        self.assertEqual(candidate.schema, self.dataset.schema)
        self.assertEqual(raw["name"][0], " 股票名称 ")
        self.assertEqual(candidate["name"][0], "股票名称")
        self.assertEqual(set(raw["_requested_is_new"].to_list()), {"Y", "N"})
        self.assertTrue(result.artifacts_complete)
        before = self.current()[0]
        repeat = self.ingest()
        self.assertEqual(repeat.status, "SUCCESS", repeat)
        self.assertEqual(self.current()[0], before)
        self.assertGreater(repeat.publications[0].publication_id, result.publications[0].publication_id)

    def test_pagination_full_false_and_short_true(self):
        for short in (False, True):
            def transform(params, data):
                if len(data["items"]) == 2:
                    data["has_more"] = False
                if short and params["is_new"] == "Y" and params["offset"] == 0:
                    data["items"] = data["items"][:1]
                    data["has_more"] = True
            with self.subTest(short=short):
                result = self.ingest(transform=transform)
                self.assertEqual(result.status, "SUCCESS", result)
                offsets = [p["offset"] for _, p, _ in self.client.calls if p["is_new"] == "Y"]
                self.assertEqual(offsets, [0, 1, 3] if short else [0, 2])
                self.assertEqual(result.publications[0].row_count, 5)

    def test_either_state_page_fault_blocks_whole_snapshot(self):
        self.ingest()
        first_pages = {}
        def request_failure(data):
            raise RuntimeError("offline injected later-page failure")
        faults = (request_failure, lambda d: d.pop("has_more"), lambda d: d.update(items=[], has_more=True),
                  lambda d: d.update(items=d["items"]*3 if d["items"] else first_pages["N"]*2),
                  lambda d: d.update(items=list(reversed(first_pages["active"])), has_more=True))
        for status in ("Y", "N"):
            for fault in faults:
                def transform(params, data):
                    if params["offset"] == 0:
                        first_pages[params["is_new"]] = list(data["items"])
                    if params["is_new"] == status and params["offset"] == 2:
                        first_pages["active"] = first_pages[status]
                        fault(data)
                with self.subTest(status=status, fault=fault):
                    result = self.failed_preserving(transform=transform)
                    self.assertTrue(self.checks(result))
                    self.assertTrue(all(json.loads(c[2]) == ["SNAPSHOT"] for c in self.checks(result)))
                    state = self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=?", [result.run_id]).fetchone()[0]
                    self.assertEqual(state, "FAILED" if fault is request_failure else "INCOMPLETE")

    def test_preparation_errors_do_not_redefine_complete_delivery(self):
        self.ingest()
        cases = [(None, None, "source.fields"), ("ts_code", 1, "source.types"),
                 ("name", False, "source.types"), ("in_date", 20200102, "source.types"),
                 ("is_new", "N", "source.range"), ("in_date", "2020-01-02", "source.date_format"),
                 ("out_date", "20200230", "source.date_parse")]
        for field, value, rule in cases:
            def transform(params, data):
                if params["is_new"] == "Y" and params["offset"] == 0:
                    index = data["fields"].index(field or "name")
                    if field is None:
                        data["fields"].pop(index)
                        data["items"] = [r[:index]+r[index+1:] for r in data["items"]]
                    else:
                        data["items"][0][index] = value
            with self.subTest(field=field, value=value):
                result = self.failed_preserving(transform=transform)
                self.assertIn(rule, [c[0] for c in self.checks(result)])
                self.assertEqual(self.con.execute("SELECT delivery_state FROM meta.run_unit WHERE run_id=?",
                                                 [result.run_id]).fetchone()[0], "COMPLETE")

    def test_required_status_dates_keys_and_empty_protect_old_snapshot(self):
        self.ingest()
        cases = []
        for field in ("l1_code", "l2_code", "l3_code", "ts_code", "in_date"):
            records = rows()
            records[0][field] = None
            cases.append((records, "required"))
        records = rows()
        records[0]["out_date"] = "20200103"
        cases.append((records, "status"))
        records = rows()
        records[3]["out_date"] = None
        cases.append((records, "status"))
        records = rows()
        records[3]["out_date"] = "20100101"
        cases.append((records, "date_order"))
        cases.extend(((rows()+[row()], "key_unique"),
                      (rows()+[row(is_new="N", out_date="20200105")], "key_unique"),
                      ([], "nonempty"),
                      ([r for r in rows() if r["is_new"]=="Y"], "nonempty"),
                      ([r for r in rows() if r["is_new"]=="N"], "nonempty")))
        for records, rule in cases:
            with self.subTest(rule=rule):
                result = self.failed_preserving(records=records, keep=not records)
                self.assertIn("index_member_all."+rule, [c[0] for c in self.checks(result)])
                if not records:
                    empty = pl.read_parquet(Path(result.artifact_dir) / "candidate.parquet")
                    self.assertEqual(empty.schema, self.dataset.schema)
                    self.assertEqual(empty.height, 0)
        with self.assertRaises(duckdb.ConstraintException):
            self.con.execute("UPDATE canonical.index_member_all SET out_date='2021-01-01' WHERE is_new='Y'")

    def test_inclusive_end_overlap_gaps_and_warnings(self):
        result = self.ingest()
        self.assertEqual(result.status, "SUCCESS", result)
        # 边界证据：旧区间最后一天仍命中旧行业，次日命中新行业，不依靠 is_new 筛历史。
        for day, expected in (("2020-01-01", "850112.SI"), ("2020-01-02", "850111.SI")):
            self.assertEqual(self.con.execute("SELECT l3_code FROM canonical.index_member_all WHERE ts_code='000001.SZ' "
                                             "AND in_date<=?::DATE AND (out_date IS NULL OR ?::DATE<=out_date)",
                                             [day, day]).fetchall(), [(expected,)])
        overlap = rows()
        overlap[3]["out_date"] = "20200102"
        result = self.failed_preserving(records=overlap)
        self.assertIn("index_member_all.interval_overlap", [c[0] for c in self.checks(result)])
        # 合法单日区间和未报告断档不补齐；异常早日期、缺名称保留并告警。
        records = rows()
        records[3].update(in_date="19840509", out_date="19840509", name=None)
        result = self.ingest(records)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual({c[0] for c in self.checks(result, "WARNING")},
                         {"index_member_all.early_date", "index_member_all.name_missing"})
        self.assertEqual(self.con.execute("SELECT in_date,out_date,name FROM canonical.index_member_all "
                                         "WHERE l3_code='850112.SI'").fetchone(), (date(1984,5,9), date(1984,5,9), None))

    def test_classification_migration_correction_and_deletion_replace_together(self):
        first = self.ingest()
        records = rows()
        records[0].update(is_new="N", out_date="20260630")
        records.append(row(l3_code="850113.SI", in_date="20260701"))
        records[3].update(in_date="20100105", name="历史名称纠正")
        records = [r for r in records if r["ts_code"] != "000003.SZ"]
        result = self.ingest(records)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertGreater(result.publications[0].publication_id, first.publications[0].publication_id)
        actual = self.con.execute("SELECT l3_code,in_date,out_date,is_new FROM canonical.index_member_all "
                                  "WHERE ts_code='000001.SZ' ORDER BY in_date").fetchall()
        self.assertEqual(actual, [("850112.SI", date(2010,1,5), date(2020,1,1), "N"),
                                  ("850111.SI", date(2020,1,2), date(2026,6,30), "N"),
                                  ("850113.SI", date(2026,7,1), None, "Y")])
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_member_all WHERE ts_code='000003.SZ'").fetchone()[0], 0)

    def test_rollback_evidence_failure_subset_rejection_and_cli(self):
        self.ingest()
        proxy = Mock(wraps=self.con)
        def execute(sql, *args):
            if " ".join(sql.split()).startswith('INSERT INTO "canonical"."index_member_all"'):
                raise RuntimeError("offline injected publication failure")
            return self.con.execute(sql, *args)
        proxy.execute.side_effect = execute
        self.failed_preserving(con=proxy)
        with patch("quant_data.core.artifacts.Artifacts.record_attempt", side_effect=OSError("evidence disk failure")):
            self.failed_preserving()
        for params in ({"start_date": "20200101"}, {"ts_code": "000001.SZ"}, {"is_new": "Y"}):
            result = self.failed_preserving(params=params)
            self.assertEqual(self.client.calls, [])
        args = ["quant_data", "ingest", "index_member_all", "--db", str(self.path), "--landing-dir", str(self.root / "cli")]
        for records, expected in ((rows(), 0), ([], 1)):
            output = io.StringIO()
            with patch("sys.argv", args), patch("quant_data.__main__.dotenv_values", return_value={"TUSHARE_TOKEN": "test"}), \
                 patch("quant_data.__main__.TushareClient", return_value=ProviderClient(records)), redirect_stdout(output):
                self.assertEqual(main(), expected)
            self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS" if expected==0 else "FAILED")
        for invalid in (args+["--start-date", "20200101", "--end-date", "20200102"], args+["--ts-code", "000001.SZ"]):
            with patch("sys.argv", invalid), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    main()

    def test_saved_real_y_and_n_replay_matches_all_stages(self):
        bodies, records = {}, []
        for path in sorted(SAMPLES.glob("*.json")):
            envelope = json.loads(path.read_text())
            params, data = envelope["attempt"]["params"], envelope["response"]["data"]
            bodies[(params["is_new"], params["offset"])] = envelope["response"]
            records.extend(dict(zip(data["fields"], r, strict=True)) for r in data["items"])
        class ReplayClient:
            def query(self, api, params, fields, *, request_id, on_attempt):
                body = bodies[(params["is_new"], params["offset"])]
                data = body["data"]
                on_attempt(RequestAttempt(request_id, 1, api, params, tuple(fields), datetime.now(timezone.utc), 0.0,
                                          200, 0, tuple(data["fields"]), len(data["items"]), data["has_more"]), body)
                return body
        with patch("quant_data.sources.tushare.index_member_all.PAGE_SIZE", 1000):
            result = self.ingest(client=ReplayClient(), keep=True)
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(result.publications[0].row_count, len(records))
        self.assertEqual(len(records), 7928)
        self.assertEqual(self.checks(result), [])
        warning = self.checks(result, "WARNING")
        self.assertEqual([(c[0], json.loads(c[3])) for c in warning], [("index_member_all.early_date", {"rows": 9})])
        expected = []
        for record in records:
            expected.append(tuple(date.fromisoformat(record[f]) if record[f] is not None and f in ("in_date", "out_date")
                                  else record[f] for f in self.dataset.schema.names()))
        folder = Path(result.artifact_dir)
        self.assertEqual(sorted(pl.read_parquet(folder / "candidate.parquet").rows()), sorted(expected))
        self.assertEqual(sorted(self.current()[0]), sorted(expected))
        raw = pl.read_parquet(folder / "landing.parquet").select(self.dataset.schema.names())
        self.assertEqual(sorted(raw.rows()), sorted(tuple(r[f] for f in self.dataset.schema.names()) for r in records))


if __name__ == "__main__":
    unittest.main()
