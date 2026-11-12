"""串行调用正式本地分钟 CLI，执行连续年度回填、Meta 验收和 D 盘备份轮换。"""

import argparse
from datetime import datetime
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from zoneinfo import ZoneInfo

import duckdb
import pyarrow.parquet as pq

from quant_data.core import deps
from quant_data.db import transaction
from quant_data.datasets.stock_1min import Stock1MinDataset

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLI_PREFIX = ["uv", "run", "--frozen", "python", "-m", "quant_data"]
TIME_COMMAND = "/usr/bin/time"
HOST_MOUNTS = (Path("/mnt/c"), Path("/mnt/d"))
REQUIRED_BACKUP_DIR = Path("/mnt/d/quant_data_backups")
KNOWN_ERRORS = frozenset({
    "local.daily_conflict", "local.auction_not_reported", "local.empty_ambiguous",
    "local.volume_amount", "suspend.timing_format", "suspend.timing_range",
})
KNOWN_WARNINGS = frozenset({
    "local.zero_auction", "local.zero_close_auction",
    "local.post_close_excluded", "local.out_of_scope_zero",
})
CONSERVATION_RULES = frozenset({"local.row_accounting", "local.traded_equivalence"})


def file_sha256(path):
    """流式读取文件；源 Parquet 的日常第三次哈希不调用此函数。"""
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def write_json(path, value):
    """原子更新小型执行状态，避免中断后留下半个 JSON。"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with temporary.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, default=str)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def append_jsonl(path, records):
    """执行台账只追加；完整记录留在文件，不进入 Agent 输出。"""
    if not records:
        return
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        for record in records:
            stream.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def load_json(path, default):
    path = Path(path)
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else default


def save_state(work_dir, state):
    write_json(Path(work_dir) / "state.json", state)


def save_stop(work_dir, year, day, reason):
    """停止原因持久化；恢复必须显式传 --resume，且不会重跑已登记日期。"""
    write_json(Path(work_dir) / "STOPPED.json", {
        "year": year, "date": day, "reason": reason,
        "stopped_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    })


def assert_no_lock(db_path):
    """锁检查只针对权威库路径；其他程序的无关 SQLite 锁不影响回填。"""
    result = subprocess.run(
        ["lslocks", "--notruncate", "--noheadings", "--output", "PATH"],
        check=True, capture_output=True, text=True,
    )
    target = str(Path(db_path).resolve())
    if any(line.strip() == target for line in result.stdout.splitlines()):
        raise RuntimeError(f"Database lock is held: {target}")


def assert_resources(args):
    """只使用宿主真实余量；WSL 内部或未分配空间不抵扣此门槛。"""
    for mount in HOST_MOUNTS:
        free_gib = shutil.disk_usage(mount).free / 1024 ** 3
        if free_gib < args.min_free_gib:
            raise RuntimeError(f"Insufficient host space at {mount}: {free_gib:.2f} GiB")


def database_snapshot(con):
    """仅用于备份恢复核验，不作为日常历史事实对账。"""
    tables = con.execute("""
        SELECT table_schema,table_name FROM information_schema.tables
        WHERE table_schema IN ('canonical','meta') AND table_type='BASE TABLE'
        ORDER BY 1,2
    """).fetchall()
    counts = {
        f"{schema}.{table}": con.execute(f'SELECT count(*) FROM "{schema}"."{table}"').fetchone()[0]
        for schema, table in tables
    }
    latest = con.execute("""
        SELECT run_id,status,started_at,finished_at FROM meta.run
        WHERE dataset='stock_1min' ORDER BY started_at DESC LIMIT 1
    """).fetchone()
    running = con.execute("SELECT count(*) FROM meta.run WHERE status='RUNNING'").fetchone()[0]
    return {
        "table_counts": counts,
        "latest_minute_run": tuple(map(str, latest)) if latest else None,
        "running_runs": running,
    }


def one_backup(backup_dir):
    """单恢复点是外部执行边界；多份文件需要人工辨认，不能自动选择删除。"""
    backups = sorted(Path(backup_dir).glob("warehouse-*.duckdb"))
    if len(backups) != 1:
        raise RuntimeError(f"Expected exactly one recovery point in {backup_dir}, found {len(backups)}")
    return backups[0]


def establish_backup_state(args, state):
    """新任务要求主库与恢复点一致；恢复任务信任既有台账但仍验证恢复点本身。"""
    backup = one_backup(args.backup_dir)
    recorded = state.get("backup")
    with duckdb.connect(str(args.db), read_only=True) as con:
        if database_snapshot(con)["running_runs"]:
            raise RuntimeError("Cannot start with RUNNING runs")
    if recorded and Path(recorded["path"]) == backup and backup.is_file():
        digest = file_sha256(backup)
        if digest != recorded["sha256"]:
            raise RuntimeError("Recorded recovery point SHA-256 changed")
        return recorded
    # 上次进程若在“新备份验证完成、状态尚未更新”之间退出，只能以唯一文件和主库哈希重新确认。
    main_hash = file_sha256(args.db)
    backup_hash = file_sha256(backup)
    if main_hash != backup_hash:
        raise RuntimeError("Main database differs from the sole recovery point; establish a new baseline explicitly")
    return {
        "path": str(backup), "bytes": backup.stat().st_size, "sha256": backup_hash,
    }


def existing_minute_days(con, year):
    """任何既有日期都不自动重跑；包括本地、银河、PARTIAL 和 FAILED。"""
    prefix = f"{year:04d}-"
    days = set()
    for run_id, params_text in con.execute(
        "SELECT run_id,params FROM meta.run WHERE dataset='stock_1min'"
    ).fetchall():
        params = json.loads(params_text)
        start, end = params.get("start_date"), params.get("end_date")
        if start and end and start[:4] == f"{year:04d}":
            start_day = datetime.strptime(start, "%Y%m%d").date()
            end_day = datetime.strptime(end, "%Y%m%d").date()
            if start_day == end_day:
                days.add(start_day.isoformat())
            else:
                rows = con.execute("""
                    SELECT DISTINCT unit_key FROM meta.run_unit
                    WHERE run_id=? AND dataset='stock_1min'
                """, [run_id]).fetchall()
                days.update(r[0].rsplit(":", 1)[1] for r in rows if r[0].rsplit(":", 1)[1].startswith(prefix))
    rows = con.execute("""
        SELECT DISTINCT unit_key FROM meta.run_unit
        WHERE dataset='stock_1min' AND unit_key LIKE ?
    """, [f"%:{prefix}%"]).fetchall()
    days.update(r[0].rsplit(":", 1)[1] for r in rows)
    return days


def missing_daily_dependencies(con, day):
    """单日缺日频依赖可跳过；共享快照和日历在年度预检中单独阻断。"""
    return con.execute("""
        WITH required(dataset) AS (VALUES ('suspend_daily'),('stock_daily'))
        SELECT r.dataset FROM required r
        LEFT JOIN meta.coverage c ON c.dataset=r.dataset AND c.unit_key=? AND c.status='PUBLISHED'
        WHERE c.dataset IS NULL ORDER BY r.dataset
    """, [day]).fetchall()


def build_year_plan(con, year, source_dir):
    """从正式日历、依赖和 Dataset 生成日清单，不从文件名推断交易日。"""
    rows = con.execute("""
        SELECT c.cal_date,v.status FROM canonical.trade_calendar c
        LEFT JOIN meta.coverage v ON v.dataset='trade_calendar'
          AND v.unit_key='SSE:'||strftime(c.cal_date,'%Y-%m-%d')
        WHERE c.exchange='SSE' AND c.is_open AND c.cal_date BETWEEN ?::DATE AND ?::DATE
        ORDER BY c.cal_date
    """, [f"{year:04d}-01-01", f"{year:04d}-12-31"]).fetchall()
    unpublished = [str(day) for day, status in rows if status != "PUBLISHED"]
    if unpublished:
        raise RuntimeError(f"Published calendar coverage missing for {year}: {unpublished[:3]}")
    snapshots = dict(con.execute("""
        SELECT dataset,status FROM meta.coverage
        WHERE dataset IN ('stock_basic','bse_mapping') AND unit_key='SNAPSHOT'
    """).fetchall())
    if snapshots.get("stock_basic") != "PUBLISHED" or snapshots.get("bse_mapping") != "PUBLISHED":
        raise RuntimeError("Shared stock_basic/bse_mapping snapshots are not PUBLISHED")
    existing = existing_minute_days(con, year)
    dataset = Stock1MinDataset()
    plan = []
    for day_date, _ in rows:
        iso = day_date.isoformat()
        number = day_date.strftime("%Y%m%d")
        source = (source_dir / f"{number}.parquet").resolve()
        entry = {
            "date": iso, "day": number, "source": str(source), "skip_reason": None,
            "scope_units": None, "request_units": None, "unknown_units": 0, "pins": {},
        }
        if iso in existing:
            # 既有日期明确不重跑，也不要求旧源文件仍存在；恢复时优先保护这个边界。
            entry["skip_reason"] = "EXISTING_RUN"
            plan.append(entry)
            continue
        if not source.is_file() or not os.access(source, os.R_OK):
            entry["skip_reason"] = "MISSING_SOURCE"
        else:
            # 预检只读 Parquet 元数据；完整 SHA-256 由正式 CLI 的 inspect/fetch 执行。
            try:
                entry["source_rows"] = pq.read_metadata(str(source)).num_rows
            except Exception as exc:
                raise RuntimeError(f"Cannot read Parquet metadata for {source}: {exc}") from exc
        missing = missing_daily_dependencies(con, iso)
        if missing:
            entry["skip_reason"] = "MISSING_DEPENDENCY"
            entry["missing_dependencies"] = [r[0] for r in missing]
        if entry["skip_reason"] != "MISSING_DEPENDENCY":
            params = {"start_date": number, "end_date": number}
            with transaction(con):
                pins = deps.check(con, dataset.dependencies(params, con))
                scope = dataset.resolve_scope(params, pins, con)
            entry.update({
                "scope_units": len(scope.units), "request_units": len(scope.request_units or ()),
                "unknown_units": len(scope.unknown),
                "pins": {f"{p.ref.dataset}:{p.ref.unit_key}": p.publication_id for p in pins},
            })
        plan.append(entry)
    return plan


def inspect_run(con, run_id, planned):
    """常规验收仅读取 Meta；不重新计算源哈希，也不扫描 Canonical。"""
    row = con.execute("""
        SELECT status,summary,error,params FROM meta.run WHERE run_id=?
    """, [run_id]).fetchone()
    if row is None:
        raise RuntimeError(f"CLI returned unknown run_id: {run_id}")
    status, summary_text, run_error, params_text = row
    summary = json.loads(summary_text)
    params = json.loads(params_text)
    units = con.execute("""
        SELECT unit_key,status,scope_decision,row_count,publication_id,reason
        FROM meta.run_unit WHERE run_id=?
    """, [run_id]).fetchall()
    publication_count, publication_rows = con.execute("""
        SELECT count(*),COALESCE(sum(row_count),0) FROM meta.publication WHERE run_id=?
    """, [run_id]).fetchone()
    checks = con.execute("""
        SELECT level,rule,units FROM meta.validation_check WHERE run_id=? ORDER BY check_id
    """, [run_id]).fetchall()
    pins = dict(con.execute("""
        SELECT upstream_dataset||':'||upstream_unit,publication_id
        FROM meta.run_dependency WHERE run_id=?
    """, [run_id]).fetchall())
    failed = {u for u, status, *_ in units if status == "FAILED"}
    unknown = {u for u, status, decision, *_ in units if status == "UNKNOWN" or decision == "UNKNOWN"}
    published = {u for u, status, *_ in units if status == "PUBLISHED"}
    pending = {u for u, status, *_ in units if status == "PENDING"}
    error_units = set()
    global_errors = []
    error_rules = {}
    warning_rules = {}
    issues = []
    for level, rule, units_text in checks:
        affected = json.loads(units_text) if units_text is not None else None
        counts = error_rules if level == "ERROR" else warning_rules
        counts[rule] = len(affected) if affected is not None else 1
        if level == "ERROR":
            if affected is None:
                global_errors.append(rule)
            else:
                error_units.update(affected)
                issues.extend({"unit_key": unit, "rule": rule} for unit in affected)
    for unit, _, decision, _, _, reason in units:
        if unit in unknown:
            issues.append({"unit_key": unit, "rule": "scope.unknown", "reason": reason or decision})
    problems = []
    if run_error:
        problems.append(f"Run.error: {run_error}")
    if pending:
        problems.append(f"PENDING units remain: {len(pending)}")
    if global_errors:
        problems.append(f"Global ERROR rules: {sorted(set(global_errors))}")
    if error_units != failed:
        problems.append("ERROR Unit set differs from FAILED Unit set")
    if len(published) != publication_count:
        problems.append("PUBLISHED Unit count differs from Publication count")
    unit_rows = sum(row_count or 0 for _, status, _, row_count, _, _ in units if status == "PUBLISHED")
    if unit_rows != publication_rows:
        problems.append("Run Unit row_count differs from Publication row_count")
    coverage_mismatch = con.execute("""
        SELECT count(*) FROM meta.run_unit u LEFT JOIN meta.coverage c
          ON c.dataset=u.dataset AND c.unit_key=u.unit_key
        WHERE u.run_id=? AND u.status='PUBLISHED'
          AND c.publication_id IS DISTINCT FROM u.publication_id
    """, [run_id]).fetchone()[0]
    if coverage_mismatch:
        problems.append(f"Successful Coverage pointers differ: {coverage_mismatch}")
    failed_publications = con.execute("""
        SELECT count(*) FROM meta.run_unit u JOIN meta.publication p
          ON p.run_id=u.run_id AND p.unit_key=u.unit_key
        WHERE u.run_id=? AND u.status<>'PUBLISHED'
    """, [run_id]).fetchone()[0]
    if failed_publications:
        problems.append(f"Failed/unknown Units have new Publications: {failed_publications}")
    blocked = len(failed) + len(unknown)
    expected_status = "PARTIAL" if published and blocked else "FAILED" if blocked else "SUCCESS"
    if status != expected_status:
        problems.append(f"Run status {status} does not match Unit outcomes {expected_status}")
    if summary.get("request_attempts") != 0:
        problems.append("Local Parquet Run has HTTP request attempts")
    source = params.get("source", {})
    if (params.get("start_date") != planned["day"] or params.get("end_date") != planned["day"]
            or source.get("type") != "LOCAL_PARQUET"
            or source.get("mapping_profile") != "NETDISK_END_MINUTE_V1"
            or Path(source.get("path", "")).resolve() != Path(planned["source"]).resolve()):
        problems.append("Run source identity or date differs from the approved day")
    if pins != planned["pins"]:
        problems.append("Run dependencies differ from preflight pins")
    scope_units = sum(decision == "IN_SCOPE" for _, _, decision, *_ in units)
    if scope_units != planned["scope_units"] or len(unknown) != planned["unknown_units"]:
        problems.append("Run Scope or UNKNOWN count differs from preflight")
    return {
        "run_id": run_id, "status": status, "scope": scope_units,
        "published": len(published), "failed": len(failed), "skipped": len(unknown),
        "publication_rows": publication_rows, "error_rules": error_rules,
        "warning_rules": warning_rules, "issues": issues, "problems": problems,
        "new_error_rules": sorted(set(error_rules) - KNOWN_ERRORS),
        "new_warning_rules": sorted(set(warning_rules) - KNOWN_WARNINGS),
        "conservation_rules": sorted(set(error_rules) & CONSERVATION_RULES),
    }


def rule_policy(day, result, state):
    """新 Unit ERROR 允许首日登记；跨日重复、守恒失败和新 WARNING 熔断。"""
    if result["problems"]:
        return result["problems"]
    if result["new_warning_rules"]:
        return [f"New WARNING rules: {result['new_warning_rules']}"]
    if result["conservation_rules"]:
        return [f"Conservation checks failed: {result['conservation_rules']}"]
    repeated = sorted(rule for rule in result["new_error_rules"]
                      if rule in state["new_error_rules"] and state["new_error_rules"][rule] != day)
    if repeated:
        return [f"New Unit ERROR rule repeated on another day: {repeated}"]
    return []


def update_skip_policy(state, reason):
    """日频依赖缺口只登记；只有连续缺源文件和连续零发布保留熔断。"""
    state["consecutive_missing_dependency"] = 0
    if reason != "MISSING_SOURCE":
        state["consecutive_missing_source"] = 0
        return None
    state["consecutive_missing_source"] = state.get("consecutive_missing_source", 0) + 1
    if state["consecutive_missing_source"] >= 2:
        return "Consecutive MISSING_SOURCE on two trading days"
    return None


def update_execution_policy(state, published):
    """正常执行清除缺源计数；连续两个执行日零发布则停止空跑。"""
    state["consecutive_missing_source"] = 0
    state["consecutive_missing_dependency"] = 0
    state["consecutive_zero_published"] = state.get("consecutive_zero_published", 0) + 1 if published == 0 else 0
    if state["consecutive_zero_published"] >= 2:
        return "Two consecutive executed days have zero PUBLISHED Units"
    return None


def summarize_records(records):
    """月度/年度输出只包含聚合值，完整规则和逐 Unit 明细留在 JSONL。"""
    executed = [r for r in records if r.get("status") != "SKIPPED"]
    error_rules, warning_rules, skip_reasons = {}, {}, {}
    for record in records:
        for target, key in ((error_rules, "error_rules"), (warning_rules, "warning_rules")):
            for rule, count in record.get(key, {}).items():
                target[rule] = target.get(rule, 0) + count
        if record.get("status") == "SKIPPED":
            reason = record.get("reason")
            skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
    rss = [r["max_rss_kib"] for r in executed if r.get("max_rss_kib") is not None]
    return {
        "days": len(records), "executed_days": len(executed),
        "skipped_days": len(records) - len(executed),
        "published_units": sum(r.get("published", 0) for r in executed),
        "failed_units": sum(r.get("failed", 0) for r in executed),
        "unknown_units": sum(r.get("skipped", 0) for r in executed),
        "publication_rows": sum(r.get("publication_rows", 0) for r in executed),
        "elapsed_seconds": round(sum(r.get("elapsed_seconds", 0) for r in executed), 2),
        "max_rss_kib": max(rss) if rss else None,
        "error_rules": error_rules, "warning_rules": warning_rules, "skip_reasons": skip_reasons,
    }


def daily_records(work_dir):
    path = Path(work_dir) / "daily.jsonl"
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def replay_policy_state(work_dir, state):
    """从已追加日志恢复规则历史和连续计数，避免状态文件与日志之间的小中断窗口。"""
    records = daily_records(work_dir)
    if not records:
        return set()
    state["new_error_rules"] = {}
    for key in ("consecutive_missing_source", "consecutive_missing_dependency", "consecutive_zero_published"):
        state[key] = 0
    for record in records:
        if record.get("status") == "SKIPPED":
            update_skip_policy(state, record.get("reason"))
        else:
            for rule in record.get("new_error_rules", []):
                state["new_error_rules"].setdefault(rule, record["date"])
            update_execution_policy(state, record.get("published", 0))
    return {record["date"] for record in records}


def year_summary(work_dir, year):
    records = daily_records(work_dir)
    return summarize_records([r for r in records if r.get("date", "").startswith(f"{year:04d}-")])


def run_cli(planned, args):
    """正式业务写入只发生在这个子进程；编排器没有直接写 Canonical 的路径。"""
    paths = {name: args.logs_dir / f"{planned['day']}.{suffix}" for name, suffix in {
        "stdout": "stdout.json", "stderr": "stderr.log", "time": "time.log",
    }.items()}
    for path in paths.values():
        if path.exists() or paths["stdout"].with_suffix(".json.gz").exists():
            raise RuntimeError(f"Refusing to overwrite existing day log: {planned['day']}")
    command = [
        TIME_COMMAND, "-v", "-o", str(paths["time"]), *CLI_PREFIX,
        "ingest", "stock_1min", "--start-date", planned["day"], "--end-date", planned["day"],
        "--source-parquet", planned["source"], "--db", str(args.db),
        "--landing-dir", str(args.landing_dir),
    ]
    started = time.perf_counter()
    with paths["stdout"].open("wb") as stdout, paths["stderr"].open("wb") as stderr:
        process = subprocess.Popen(command, cwd=PROJECT_ROOT, stdout=stdout, stderr=stderr, start_new_session=True)
        try:
            returncode = process.wait(timeout=args.day_timeout_seconds)
        except BaseException:
            # 超时或键盘中断只终止本次新建的进程组；随后停止，不自动 reconcile 或重试。
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=30)
                except subprocess.TimeoutExpired:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            raise
    elapsed = time.perf_counter() - started
    time_text = paths["time"].read_text(encoding="utf-8") if paths["time"].exists() else ""
    rss_match = re.search(r"Maximum resident set size \(kbytes\): (\d+)", time_text)
    max_rss = int(rss_match[1]) if rss_match else None
    try:
        output = json.loads(paths["stdout"].read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"CLI stdout is not valid JSON: {paths['stdout']}") from exc
    if returncode not in (0, 1) or not output.get("run_id"):
        raise RuntimeError(f"CLI execution failed with exit {returncode}; see {paths['stderr']}")
    if (returncode == 0) != (output.get("status") == "SUCCESS"):
        raise RuntimeError(f"CLI exit {returncode} and status {output.get('status')} disagree")
    return output, elapsed, max_rss, paths


def compress_stdout(paths):
    """保留完整原始 JSON，但避免 Agent 读取和在线未压缩日志膨胀。"""
    raw = paths["stdout"]
    compressed = raw.with_suffix(".json.gz")
    with raw.open("rb") as source, gzip.open(compressed, "wb") as target:
        shutil.copyfileobj(source, target)
    raw.unlink()


def rotate_backup(args, state, year):
    """年度结束先在 D 盘验证新副本，再删除旧副本；不执行历史 EXCEPT ALL。"""
    assert_no_lock(args.db)
    assert_resources(args)
    old = Path(state["backup"]["path"])
    if one_backup(args.backup_dir) != old or not old.is_file():
        raise RuntimeError("Backup directory changed before rotation; refusing to delete an unexpected file")
    if Path(args.db).with_suffix(".duckdb.wal").exists():
        raise RuntimeError("WAL exists before backup rotation")
    free = shutil.disk_usage(args.backup_dir).free
    required = args.db.stat().st_size + args.min_free_gib * 1024 ** 3
    if free < required:
        raise RuntimeError(f"Insufficient D space for safe rotation: need {required} bytes, have {free}")
    stamp = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%z")
    final = args.backup_dir / f"warehouse-{stamp}-after-local-minute-{year}.duckdb"
    temporary = args.backup_dir / f".{final.name}.{os.getpid()}.tmp"
    initial = args.db.stat()
    source_con = duckdb.connect(str(args.db), read_only=True)
    try:
        before = database_snapshot(source_con)
        if before["running_runs"]:
            raise RuntimeError("RUNNING Run before backup rotation")
        digest = hashlib.sha256()
        with args.db.open("rb") as reader, temporary.open("xb") as writer:
            while chunk := reader.read(16 * 1024 * 1024):
                digest.update(chunk)
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
        current = args.db.stat()
        if current.st_size != initial.st_size or current.st_mtime_ns != initial.st_mtime_ns:
            raise RuntimeError("Source database changed during backup copy")
        temporary.rename(final)
        directory_fd = os.open(args.backup_dir, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        with duckdb.connect(str(final), read_only=True) as backup_con:
            after = database_snapshot(backup_con)
        if before != after or before != database_snapshot(source_con):
            raise RuntimeError("Backup table counts or latest Run differ")
        backup_hash = file_sha256(final)
        if backup_hash != digest.hexdigest():
            raise RuntimeError("Backup SHA-256 differs from source")
        if old != final:
            old.unlink()
        return {"path": str(final), "bytes": final.stat().st_size, "sha256": backup_hash}
    finally:
        source_con.close()
        if temporary.exists():
            temporary.unlink()


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    parser.add_argument("--db", type=Path, default=PROJECT_ROOT / "data/warehouse.duckdb")
    parser.add_argument("--source-dir", type=Path, default=Path("/mnt/d/BaiduNetdiskDownload/stock_1min"))
    parser.add_argument("--backup-dir", type=Path, default=REQUIRED_BACKUP_DIR)
    parser.add_argument("--landing-dir", type=Path, default=PROJECT_ROOT / "data/landing")
    parser.add_argument("--work-dir", type=Path, default=PROJECT_ROOT / "data/probes/local_parquet_2023_2013_overnight")
    parser.add_argument("--min-free-gib", type=float, default=20)
    parser.add_argument("--day-timeout-seconds", type=int, default=1800)
    parser.add_argument("--resume", action="store_true", help="人工复核停止原因后恢复；不重跑已有日期")
    parser.add_argument("--dry-run", action="store_true", help="只生成只读预检和计划，不运行 CLI 或轮换备份")
    args = parser.parse_args(argv)
    args.db = args.db.resolve()
    args.source_dir = args.source_dir.resolve()
    args.backup_dir = args.backup_dir.resolve()
    args.landing_dir = args.landing_dir.resolve()
    args.work_dir = args.work_dir.resolve()
    args.logs_dir = args.work_dir / "logs"
    # 运维边界由代码强制：临时备份也只能位于 D 盘固定目录，不能写回 WSL。
    if args.backup_dir != REQUIRED_BACKUP_DIR:
        parser.error(f"--backup-dir must be {REQUIRED_BACKUP_DIR}; backups outside this D drive directory are forbidden")
    if args.start_year < args.end_year or args.end_year < 2013:
        parser.error("Years must descend and end no earlier than 2013")
    if args.min_free_gib < 0 or args.day_timeout_seconds <= 0:
        parser.error("Invalid resource thresholds")
    return args


def main(argv=None):
    args = parse_args(argv)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    args.logs_dir.mkdir(parents=True, exist_ok=True)
    state = load_json(args.work_dir / "state.json", {"new_error_rules": {}, "completed_years": []})
    for key in ("consecutive_missing_source", "consecutive_missing_dependency", "consecutive_zero_published"):
        state.setdefault(key, 0)
    state.setdefault("year_start_bytes", {})
    recorded_days = replay_policy_state(args.work_dir, state)
    stopped = load_json(args.work_dir / "STOPPED.json", None)
    if stopped and not args.resume and not args.dry_run:
        print(f"STOPPED: previous stop requires review: {stopped['reason']}", file=sys.stderr)
        return 1
    if args.resume and stopped and not args.dry_run:
        (args.work_dir / "STOPPED.json").unlink()
    year, day = None, None
    try:
        assert_no_lock(args.db)
        assert_resources(args)
        if not args.dry_run:
            state["backup"] = establish_backup_state(args, state)
            save_state(args.work_dir, state)
        for current_year in range(args.start_year, args.end_year - 1, -1):
            year = current_year
            if current_year in state["completed_years"]:
                continue
            with duckdb.connect(str(args.db), read_only=True) as con:
                plan = build_year_plan(con, current_year, args.source_dir)
            write_json(args.work_dir / f"plan-{current_year}.json", plan)
            print(json.dumps({
                "event": "YEAR_PLAN", "year": current_year, "days": len(plan),
                "execute": sum(p["skip_reason"] is None for p in plan),
                "skipped": sum(p["skip_reason"] is not None for p in plan),
            }), flush=True)
            if args.dry_run:
                continue
            state["year_start_bytes"].setdefault(str(current_year), args.db.stat().st_size)
            save_state(args.work_dir, state)
            month, month_records = None, []
            for planned in plan:
                day = planned["day"]
                if planned["date"] in recorded_days:
                    continue
                current_month = planned["date"][:7]
                if month is not None and current_month != month:
                    print(json.dumps({"event": "MONTH_DONE", "month": month,
                                      **summarize_records(month_records)}), flush=True)
                    month_records = []
                month = current_month
                reason = planned["skip_reason"]
                if reason:
                    stored = {"date": planned["date"], "status": "SKIPPED", "reason": reason}
                    append_jsonl(args.work_dir / "issues.jsonl", [{
                        "date": planned["date"], "run_id": None, "rule": reason,
                        "missing_dependencies": planned.get("missing_dependencies", []),
                    }])
                    append_jsonl(args.work_dir / "daily.jsonl", [stored])
                    month_records.append(stored)
                    stop_reason = update_skip_policy(state, reason)
                    save_state(args.work_dir, state)
                    print(f"{planned['date']} SKIPPED {reason}", flush=True)
                    if stop_reason:
                        raise RuntimeError(stop_reason)
                    continue
                assert_no_lock(args.db)
                assert_resources(args)
                output, elapsed, max_rss, paths = run_cli(planned, args)
                with duckdb.connect(str(args.db), read_only=True) as con:
                    result = inspect_run(con, output["run_id"], planned)
                if result["status"] != output["status"]:
                    raise RuntimeError("CLI and Meta Run status differ")
                result.update({"date": planned["date"], "elapsed_seconds": round(elapsed, 2), "max_rss_kib": max_rss})
                stop_reasons = rule_policy(planned["date"], result, state)
                zero_stop = update_execution_policy(state, result["published"])
                if zero_stop:
                    stop_reasons.append(zero_stop)
                for rule in result["new_error_rules"]:
                    state["new_error_rules"].setdefault(rule, planned["date"])
                append_jsonl(args.work_dir / "issues.jsonl", [{
                    "date": planned["date"], "run_id": result["run_id"], **issue,
                } for issue in result["issues"]])
                stored = {k: v for k, v in result.items() if k != "issues"}
                append_jsonl(args.work_dir / "daily.jsonl", [stored])
                month_records.append(stored)
                save_state(args.work_dir, state)
                compress_stdout(paths)
                print(json.dumps(stored, ensure_ascii=False), flush=True)
                if stop_reasons:
                    raise RuntimeError("; ".join(stop_reasons))
            if month_records:
                print(json.dumps({"event": "MONTH_DONE", "month": month,
                                  **summarize_records(month_records)}), flush=True)
            summary = year_summary(args.work_dir, current_year)
            state["backup"] = rotate_backup(args, state, current_year)
            state["completed_years"].append(current_year)
            summary.update({
                "backup": state["backup"], "independent_historical_audit": "NOT_TRIGGERED",
                "database_bytes": args.db.stat().st_size,
                "database_growth_bytes": args.db.stat().st_size - state["year_start_bytes"][str(current_year)],
                "host_free_gib": {str(mount): round(shutil.disk_usage(mount).free / 1024 ** 3, 2)
                                  for mount in HOST_MOUNTS},
            })
            write_json(args.work_dir / f"year-{current_year}-summary.json", summary)
            save_state(args.work_dir, state)
            print(json.dumps({"event": "YEAR_DONE", "year": current_year, **summary}), flush=True)
        if not args.dry_run:
            campaign = summarize_records(daily_records(args.work_dir))
            campaign.update({
                "completed_years": state["completed_years"], "backup": state["backup"],
                "independent_historical_audit": "NOT_TRIGGERED", "database_bytes": args.db.stat().st_size,
            })
            write_json(args.work_dir / "campaign-summary.json", campaign)
        print(json.dumps({"event": "DRY_RUN_DONE" if args.dry_run else "CAMPAIGN_DONE"}), flush=True)
        return 0
    except BaseException as exc:
        save_stop(args.work_dir, year, day, f"{type(exc).__name__}: {exc}")
        (args.work_dir / "stop.traceback.log").write_text(traceback.format_exc(), encoding="utf-8")
        print(f"STOPPED: {type(exc).__name__}: {exc}; see {args.work_dir / 'STOPPED.json'}", file=sys.stderr)
        return 1 if not isinstance(exc, KeyboardInterrupt) else 130


if __name__ == "__main__":
    raise SystemExit(main())
