"""银河指数权重 Dataset 与正式 ingestion 链路。"""

from contextlib import redirect_stdout
from datetime import date, datetime
import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

import duckdb
import polars as pl

from quant_data import db
from quant_data.core import runner
from quant_data.core.models import DependencyPin, RunOptions, Scope
from quant_data.datasets.index_basic import IndexBasicDataset
from quant_data.datasets.index_weight import NUMERIC, WEIGHT_CODES, IndexWeightDataset
from quant_data.datasets.trade_calendar import TradeCalendarDataset
from quant_data.db import registered
from quant_data.sources.galaxy.client import GalaxyResponse
from quant_data.sources.galaxy.index_weight import FIELD_MAP, GalaxyIndexWeightAdapter

REFERENCE = Path("data/probes/amazingdata-recovery-20261019-114800/index_weight_raw_000300_202609_single.json")


def row(con_code="000001.SZ", **changes):
    value = dict(index_code="000300.SH", con_code=con_code, trade_date=date(2026, 9, 7),
                 total_share=1000.0, free_share_ratio=None, calc_share=500.0,
                 weight_factor=1.0, weight=60.0, close=10.0)
    value.update(changes)
    return value


class IndexWeightDatasetTest(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.addCleanup(self.con.close)
        self.dataset = IndexWeightDataset()
        for dataset in (TradeCalendarDataset(), IndexBasicDataset(), self.dataset):
            db.initialize(self.con, dataset)
        self.con.executemany(
            "INSERT INTO canonical.trade_calendar VALUES ('SSE',?,?,NULL)",
            [(date(2026, 9, 5), False), (date(2026, 9, 7), True)],
        )
        self.con.executemany(
            "INSERT INTO canonical.index_basic(ts_code,name,market,publisher,category) VALUES (?,?,?,?,?)",
            [(code, code, "SSE", "中证指数有限公司", "规模指数") for code in WEIGHT_CODES],
        )

    def resolve(self, params):
        refs = self.dataset.dependencies(params, self.con)
        pins = tuple(DependencyPin(ref, i + 1) for i, ref in enumerate(refs))
        return self.dataset.resolve_scope(params, pins, self.con)

    def checks(self, records, units=("000300.SH:2026-09-07",)):
        self.con.execute("CREATE OR REPLACE TEMP TABLE _scope_units(unit_key VARCHAR)")
        self.con.execute("DELETE FROM _scope_units")
        self.con.executemany("INSERT INTO _scope_units VALUES (?)", [(unit,) for unit in units])
        frame = pl.DataFrame(records, schema=self.dataset.schema)
        with registered(self.con, "candidate", frame):
            return self.dataset.validate_candidate(self.con, "candidate", Scope(tuple(units)))

    def test_dependencies_and_scope_expand_only_open_code_days(self):
        params = {"start_date": "20260905", "end_date": "20260907",
                  "index_codes": ["000300.SH", "000905.SH"]}
        refs = self.dataset.dependencies(params, self.con)
        self.assertEqual([ref.dataset for ref in refs], ["index_basic", *["trade_calendar"] * 3])
        scope = self.resolve(params)
        self.assertEqual(scope.units, ("000300.SH:2026-09-07", "000905.SH:2026-09-07"))
        self.assertEqual([r.unit_key for r in scope.out_of_scope],
                         ["000300.SH:2026-09-05", "000905.SH:2026-09-05"])
        self.assertEqual([r.unit_key for r in scope.unknown],
                         ["000300.SH:2026-09-06", "000905.SH:2026-09-06"])
        default = self.resolve({"start_date": "20260907", "end_date": "20260907"})
        self.assertEqual(len(default.units), len(WEIGHT_CODES))

    def test_invalid_inputs_and_unpublished_index_are_rejected(self):
        valid = {"start_date": "20260907", "end_date": "20260907"}
        for params in (
            valid | {"index_codes": []},
            valid | {"index_codes": ["000300.SH", "000300.SH"]},
            valid | {"index_codes": ["932000.CSI"]},
            valid | {"unexpected": True},
            {"start_date": "20260907", "end_date": "20260905"},
            {"start_date": "20260907", "end_date": "29990101"},
        ):
            with self.subTest(params=params), self.assertRaises(ValueError):
                self.dataset.dependencies(params, self.con)
        self.con.execute("DELETE FROM canonical.index_basic WHERE ts_code='000300.SH'")
        with self.assertRaisesRegex(ValueError, "outside published universe"):
            self.resolve(valid | {"index_codes": ["000300.SH"]})

    def test_valid_candidate_preserves_null_and_has_no_errors(self):
        checks = self.checks([row(), row("600000.SH", weight=40.0)])
        self.assertFalse([c for c in checks if c.level == "ERROR"])
        warning = next(c for c in checks if c.rule == "index_weight.free_share_ratio_null")
        self.assertEqual(warning.detail, {"rows_by_unit": {"000300.SH:2026-09-07": 2}})

    def test_duplicate_empty_and_weight_sum_block_affected_units(self):
        other = "000905.SH:2026-09-07"
        checks = self.checks([row(), row(), row("600000.SH", weight=40.0)],
                             ("000300.SH:2026-09-07", other))
        rules = {c.rule for c in checks if c.level == "ERROR"}
        self.assertIn("index_weight.key_unique", rules)
        self.assertIn("index_weight.weight_sum", rules)
        empty = next(c for c in checks if c.rule == "index_weight.nonempty")
        self.assertEqual(empty.units, (other,))

    def test_numeric_rules_boundaries_and_warnings(self):
        cases = (
            ({"con_code": " "}, "index_weight.required"),
            ({"weight": None}, "index_weight.required"),
            ({"close": float("inf")}, "index_weight.finite"),
            ({"weight": -1.0}, "index_weight.sign"),
            ({"free_share_ratio": 101.0}, "index_weight.free_share_ratio"),
            ({"weight": 59.8}, "index_weight.weight_sum"),
        )
        for changes, expected in cases:
            with self.subTest(changes=changes):
                checks = self.checks([row(**changes), row("600000.SH", weight=40.0)])
                self.assertIn(expected, {c.rule for c in checks if c.level == "ERROR"})
        checks = self.checks([row(weight=0.0, calc_share=1001.0), row("600000.SH", weight=100.0)])
        self.assertFalse([c for c in checks if c.level == "ERROR"])
        self.assertIn("index_weight.zero_weight", {c.rule for c in checks})
        self.assertIn("index_weight.calc_share_above_total", {c.rule for c in checks})
        checks = self.checks([row(weight=59.9), row("600000.SH", weight=40.0)])
        self.assertNotIn("index_weight.weight_sum", {c.rule for c in checks})

    def test_ddl_schema_and_final_constraints(self):
        columns = self.con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='canonical' AND table_name='index_weight' ORDER BY ordinal_position"
        ).fetchall()
        self.assertEqual([r[0] for r in columns], list(self.dataset.schema.names()))
        self.assertEqual(set(NUMERIC), set(self.dataset.schema.names()) - {"index_code", "con_code", "trade_date"})
        with self.assertRaises(duckdb.ConstraintException):
            self.con.execute("INSERT INTO canonical.index_weight VALUES "
                             "('000300.SH','000001.SZ','2026-09-07',1000,NULL,500,1,-1,10)")


def source_row(index_code="000300.SH", con_code="000001.SZ", day="20260907", weight=60.0):
    return {"INDEX_CODE": index_code, "CON_CODE": con_code, "TRADE_DATE": day,
            "TOTAL_SHR": 1000, "FREE_SHR_RATIO": None, "CALC_SHR": 500,
            "WEIGHT_FACTOR": 1, "WEIGHT": weight, "CLOSE": 10.0}


def source_records():
    return [source_row(code, con_code, weight=weight)
            for code in WEIGHT_CODES for con_code, weight in (("000001.SZ", 60.0), ("600000.SH", 40.0))]


class WeightClient:
    """只替换银河通信，正式 Adapter、Runner 和数据库约束保持执行。"""

    def __init__(self, records=None, transform=None):
        self.records = source_records() if records is None else records
        self.transform = transform
        self.requests = []

    def fetch(self, requests, artifacts):
        responses = []
        for request in requests:
            self.requests.append(request)
            rows = tuple(dict(r) for r in self.records
                         if r["INDEX_CODE"] == request.codes[0]
                         and r["TRADE_DATE"] == str(request.start_date))
            response = GalaxyResponse(request, "COMPLETE", rows, None, 0)
            if self.transform:
                response = self.transform(request, response)
            responses.append(response)
        return tuple(responses), ()


class IndexWeightIngestionTest(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.con = duckdb.connect(str(self.root / "warehouse.duckdb"))
        self.addCleanup(self.con.close)
        self.dataset = IndexWeightDataset()
        for dataset in (TradeCalendarDataset(), IndexBasicDataset(), self.dataset):
            db.initialize(self.con, dataset)
        calendar = [(date(2026, 9, day), day in (7, 8)) for day in range(5, 10)]
        self.con.executemany("INSERT INTO canonical.trade_calendar VALUES ('SSE',?,?,NULL)", calendar)
        self.con.executemany(
            "INSERT INTO canonical.index_basic(ts_code,name,market,publisher,category) VALUES (?,?,?,?,?)",
            [(code, code, "SSE", "中证指数有限公司", "规模指数") for code in WEIGHT_CODES],
        )
        coverage = [("index_basic", "SNAPSHOT", 1)] + [
            ("trade_calendar", f"SSE:2026-09-{day:02d}", day - 3) for day in range(5, 10)
        ]
        self.con.executemany(
            "INSERT INTO meta.coverage(dataset,unit_key,status,publication_id) VALUES (?,?,'PUBLISHED',?)",
            coverage,
        )
        self.params = {"start_date": "20260907", "end_date": "20260907"}

    def ingest(self, client=None, params=None, keep=False, con=None):
        self.client = client or WeightClient()
        return runner.run(con or self.con, self.dataset, GalaxyIndexWeightAdapter(self.client),
                          params or self.params, RunOptions(self.root / "landing", keep))

    def current(self):
        return [self.con.execute("SELECT * FROM canonical.index_weight ORDER BY index_code,con_code,trade_date").fetchall(),
                self.con.execute("SELECT dataset,unit_key,status,publication_id,note FROM meta.coverage "
                                 "WHERE dataset='index_weight' ORDER BY unit_key").fetchall(),
                self.con.execute("SELECT publication_id,dataset,unit_key,row_count,run_id FROM meta.publication "
                                 "WHERE dataset='index_weight' ORDER BY publication_id").fetchall()]

    def errors(self, result):
        return {r[0] for r in self.con.execute(
            "SELECT rule FROM meta.validation_check WHERE run_id=? AND level='ERROR'", [result.run_id],
        ).fetchall()}

    def test_requests_artifacts_rerun_and_unit_replacement(self):
        first = self.ingest(keep=True)
        self.assertEqual(first.status, "SUCCESS", first)
        self.assertEqual(len(first.publications), len(WEIGHT_CODES))
        self.assertEqual(sum(p.row_count for p in first.publications), 2 * len(WEIGHT_CODES))
        self.assertEqual({r.kind for r in self.client.requests}, {"index_weight"})
        self.assertTrue(all(r.start_date == r.end_date and r.begin_time == r.end_time == 0
                            for r in self.client.requests))
        root = Path(first.artifact_dir)
        raw = pl.read_parquet(root / "landing.parquet")
        candidate = pl.read_parquet(root / "candidate.parquet")
        self.assertIn("TOTAL_SHR", raw.columns)
        self.assertEqual(candidate.schema, self.dataset.schema)
        self.assertTrue(first.artifacts_complete)
        second = self.ingest()
        self.assertEqual(second.status, "SUCCESS", second)
        self.assertEqual(self.con.execute("SELECT count(*) FROM canonical.index_weight").fetchone()[0],
                         2 * len(WEIGHT_CODES))
        self.assertGreater(min(p.publication_id for p in second.publications),
                           max(p.publication_id for p in first.publications))

    def test_empty_duplicates_and_request_failures_protect_old_unit(self):
        self.ingest()
        before = self.con.execute("SELECT * FROM canonical.index_weight WHERE index_code='000300.SH' ORDER BY con_code").fetchall()

        def duplicate(request, response):
            return GalaxyResponse(request, "COMPLETE", response.rows + response.rows[:1], None, 0) \
                if request.codes[0] == "000300.SH" else response

        def empty(request, response):
            return GalaxyResponse(request, "COMPLETE", (), None, 0) \
                if request.codes[0] == "000300.SH" else response

        def provider_empty(request, response):
            return GalaxyResponse(request, "COMPLETE", (), None, -76) \
                if request.codes[0] == "000300.SH" else response

        def failed(request, response):
            return GalaxyResponse(request, "FAILED", (), "injected failure", -1) \
                if request.codes[0] == "000300.SH" else response

        for transform, rule in ((duplicate, "index_weight.key_unique"), (empty, "index_weight.nonempty"),
                                (provider_empty, "source.empty"), (failed, "source.request")):
            with self.subTest(rule=rule):
                result = self.ingest(WeightClient(transform=transform))
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual(len(result.publications), len(WEIGHT_CODES) - 1)
                self.assertIn(rule, self.errors(result))
                actual = self.con.execute(
                    "SELECT * FROM canonical.index_weight WHERE index_code='000300.SH' ORDER BY con_code"
                ).fetchall()
                self.assertEqual(actual, before)

    def test_prepare_and_business_errors_are_isolated(self):
        self.ingest()

        def change(mode):
            def transform(request, response):
                if request.codes[0] != "000300.SH" or not response.rows:
                    return response
                rows = [dict(r) for r in response.rows]
                if mode == "missing":
                    rows[0].pop("FREE_SHR_RATIO")
                elif mode == "type":
                    rows[0]["WEIGHT"] = "60"
                elif mode == "range":
                    rows[0]["TRADE_DATE"] = "20260908"
                else:
                    rows[0]["WEIGHT"] = 1.0
                return GalaxyResponse(request, "COMPLETE", tuple(rows), None, 0)
            return transform

        for mode, rule in (("missing", "source.fields"), ("type", "source.types"),
                           ("range", "source.range"), ("sum", "index_weight.weight_sum")):
            with self.subTest(mode=mode):
                result = self.ingest(WeightClient(transform=change(mode)))
                self.assertEqual(result.status, "PARTIAL", result)
                self.assertEqual(len(result.publications), len(WEIGHT_CODES) - 1)
                self.assertIn(rule, self.errors(result))

    def test_dependency_drift_and_publication_failure_roll_back(self):
        self.ingest()
        before = self.current()

        def drift(request, response):
            if request.codes[0] == "000300.SH":
                self.con.execute("UPDATE meta.coverage SET publication_id=publication_id+1000 "
                                 "WHERE dataset='index_basic' AND unit_key='SNAPSHOT'")
            return response

        result = self.ingest(WeightClient(transform=drift))
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)
        proxy = Mock(wraps=self.con)

        def execute(sql, *args):
            if " ".join(sql.split()).startswith('INSERT INTO "canonical"."index_weight"'):
                raise RuntimeError("injected publication failure")
            return self.con.execute(sql, *args)

        proxy.execute.side_effect = execute
        result = self.ingest(con=proxy)
        self.assertEqual(result.status, "FAILED", result)
        self.assertEqual(self.current(), before)

    def test_cli_assembles_serial_galaxy_and_index_subset(self):
        from quant_data.__main__ import main

        argv = ["quant_data", "ingest", "index_weight", "--db", str(self.root / "warehouse.duckdb"),
                "--landing-dir", str(self.root / "cli"), "--start-date", "20260907",
                "--end-date", "20260907", "--index-code", "000300.SH"]
        client = WeightClient()
        output = io.StringIO()
        with patch("sys.argv", argv), \
             patch("quant_data.__main__.dotenv_values", return_value={"AD_USERNAME": "user", "AD_PASSWORD": "secret"}), \
             patch("quant_data.__main__.GalaxyClient", return_value=client) as factory, redirect_stdout(output):
            self.assertEqual(main(), 0)
        factory.assert_called_once_with(parallel=1)
        self.assertEqual(len(client.requests), 1)
        self.assertEqual(json.loads(output.getvalue())["status"], "SUCCESS")

    @unittest.skipUnless(REFERENCE.exists(), "真实银河权重样本不随 Git 分发")
    def test_saved_real_single_day_replay(self):
        source = json.loads(REFERENCE.read_text())["response"]["records"]
        result = self.ingest(WeightClient(source), self.params | {"index_codes": ["000300.SH"]})
        self.assertEqual(result.status, "SUCCESS", result)
        self.assertEqual(result.publications[0].row_count, 300)
        expected = []
        for source_row in source:
            record = {target: source_row[field] for field, target in FIELD_MAP.items()}
            record["trade_date"] = datetime.strptime(source_row["TRADE_DATE"], "%Y%m%d").date()
            expected.append(record)
        actual = self.con.execute("SELECT * FROM canonical.index_weight ORDER BY con_code").pl()
        self.assertTrue(actual.equals(pl.DataFrame(expected, schema=self.dataset.schema).sort("con_code")))


if __name__ == "__main__":
    unittest.main()
