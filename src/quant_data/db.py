"""连接、事务和短生命周期的列式关系，统一由调用方持有连接。"""

from contextlib import contextmanager
from pathlib import Path

import duckdb
import polars as pl

META_COLUMNS = {
    "run": "run_id dataset params started_at finished_at status summary error failed_stage artifact_dir artifact_status artifacts_complete export_errors",
    "run_unit": "run_id dataset unit_key scope_decision reason status stage delivery_state request_ids check_ids publication_id row_count error",
    "run_dependency": "run_id upstream_dataset upstream_unit publication_id",
    "request_attempt": "run_id request_id attempt api_name params fields requested_at elapsed_seconds http_status provider_code returned_fields row_count has_more error evidence_path",
    "validation_check": "run_id check_id level rule stage units detail",
    "publication": "publication_id dataset unit_key row_count run_id published_at",
    "coverage": "dataset unit_key status publication_id updated_at note",
}


@contextmanager
def transaction(con):
    """事务块成功才提交，异常（包括中断）回滚并保留最初错误。"""
    # 发布方用同一事务包住数据替换与成功元数据，保证外部看不到只完成一半的发布。
    con.execute("BEGIN TRANSACTION")
    try:
        yield
        con.execute("COMMIT")
    except BaseException:
        try:
            con.execute("ROLLBACK")
        except duckdb.Error:
            pass
        raise


def identifier(name):
    """引用单个标识符；数据值另使用绑定参数。"""
    return '"' + name.replace('"', '""') + '"'


def relation(name):
    """分别引用 Schema 和表名。"""
    return ".".join(identifier(part) for part in name.split("."))


@contextmanager
def registered(con, name, frame):
    """注册 Arrow 视图供一次批量操作使用，退出后解绑；不复制业务行为 Python 对象。"""
    con.register(name, frame.to_arrow())
    try:
        yield relation(name)
    finally:
        con.unregister(name)


def strings_frame(rows, names):
    """将小规模控制记录组装为明确的字符串列；SQL 中转换 ID、计数和时间类型。"""
    return pl.DataFrame(rows, schema={name: pl.String for name in names}, orient="row")


def initialize(con, dataset):
    """初始化新版 Meta 和业务表；旧 Meta 明确报错，事务内不迁移或删除旧数据。"""
    # 兼容性检查先于建表：已有 Meta 的表/字段布局必须完全匹配本版，不改写旧库。
    actual_meta = {}
    for table, column in con.execute(
        "SELECT table_name,column_name FROM information_schema.columns "
        "WHERE table_schema='meta' ORDER BY table_name,ordinal_position"
    ).fetchall():
        actual_meta.setdefault(table, []).append(column)
    if actual_meta and actual_meta != {name: value.split() for name, value in META_COLUMNS.items()}:
        raise ValueError("Meta schema is incompatible with V3.2; use a new --db path (no automatic migration)")
    # 业务时间：审计时间展示和 SQL current_date 使用上海时区，未来日期告警也以此为基准。
    con.execute("SET TimeZone = 'Asia/Shanghai'")
    with transaction(con):
        con.execute(Path(__file__).with_name("core").joinpath("meta.sql").read_text())
        con.execute(dataset.ddl)
        # 用空的明确 Schema 关系验证 SQL 类型，避免维护第二套 Polars→DuckDB 类型映射。
        with registered(con, "_schema_check", pl.DataFrame(schema=dataset.schema)) as source:
            expected = [(r[0], r[1]) for r in con.execute(f"DESCRIBE SELECT * FROM {source}").fetchall()]
        actual = [(r[0], r[1]) for r in con.execute(f"DESCRIBE {relation(dataset.table)}").fetchall()]
        # 业务检查：现有 Canonical 的列顺序、名称和 SQL 类型须符合 Dataset；不自动迁移或强转。
        if actual != expected:
            raise ValueError(f"Existing {dataset.table} schema differs from the dataset contract; no automatic migration")
