"""在临时文件中烟测 DuckDB、Arrow/Parquet 与 dbt View 的协作。"""

from importlib.metadata import version
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]


def main():
    """验证工具链和关键 Schema 约束，临时目录退出后自动清理。

    不调用供应商接口，不写权威库；失败通过断言或子进程异常报告。
    """
    with TemporaryDirectory(prefix="quant-data-smoke-") as directory:
        temp = Path(directory)
        table = pa.table({"ts_code": ["T600018.SH", "920157.BJ"], "value": [1, 2]})
        # Arrow/Parquet 往返与 DuckDB 文件扫描共同验证列式数据交换能力。
        pq.write_table(table, temp / "sample.parquet")
        assert pq.read_table(temp / "sample.parquet").equals(table)
        with duckdb.connect(str(temp / "smoke.duckdb")) as con:
            # 业务结构：股票、停复牌都复用正式 DDL，不维护第二份约束定义。
            con.execute((ROOT / "src/quant_data/datasets/stock_basic.sql").read_text())
            con.execute((ROOT / "src/quant_data/datasets/suspend_daily.sql").read_text())
            assert con.execute("SELECT sum(value) FROM read_parquet(?)", [str(temp / "sample.parquet")]).fetchone()[0] == 3
            con.execute("INSERT INTO canonical.stock_basic (ts_code,symbol,name,exchange,list_status,list_date,delist_date) VALUES ('601198.SH','601198','example','SSE','L','2015-02-26','2026-10-19'), ('874396.BJ','920238','example','BSE','UN',NULL,NULL)")
            con.execute("INSERT INTO canonical.suspend_daily VALUES ('300819.SZ','2020-03-12','S','09:30-10:00'), ('300070.SZ','2020-03-12','R',NULL)")
            # 业务键相同而时间段不同仍应冲突，避免由描述字段改变记录粒度。
            try:
                con.execute("INSERT INTO canonical.suspend_daily VALUES ('300819.SZ','2020-03-12','S',NULL)")
            except duckdb.ConstraintException:
                pass
            else:
                raise AssertionError("duplicate business key was accepted")
        # 关闭写连接后运行独立 dbt 进程，遵守同一 DuckDB 文件的串行访问约束。
        (temp / "models").mkdir()
        (temp / "dbt_project.yml").write_text("name: quant_data_smoke\nversion: '1.0'\nconfig-version: 2\nprofile: quant_data_smoke\nmodel-paths: ['models']\nmodels:\n  quant_data_smoke:\n    +materialized: view\n")
        (temp / "profiles.yml").write_text(f"quant_data_smoke:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: '{temp / 'smoke.duckdb'}'\n      schema: serving\n      threads: 1\n")
        (temp / "models/smoke.sql").write_text("SELECT count(*) AS n FROM canonical.stock_basic\n")
        result = subprocess.run([str(ROOT / ".venv/bin/dbt"), "run", "--project-dir", str(temp), "--profiles-dir", str(temp), "--no-use-colors"], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(result.stdout + result.stderr)
        # 重新打开只读连接，确认 dbt 实际创建的 View 可查询。
        with duckdb.connect(str(temp / "smoke.duckdb"), read_only=True) as con:
            assert con.execute("SELECT n FROM serving.smoke").fetchone()[0] == 2
    print("PASS: canonical DDL, business-key rejection, Arrow/Parquet roundtrip, DuckDB scan, dbt serving view")
    print({name: version(name) for name in ("duckdb", "pyarrow", "dbt-core", "dbt-duckdb", "python-dotenv")})


if __name__ == "__main__":
    main()
