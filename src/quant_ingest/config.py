"""V2 本地存储路径；状态账与 dbt 工作库使用不同文件。"""

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WAREHOUSE_ROOT = PROJECT_ROOT / "data" / "warehouse" / "v2"
LEDGER_DB = WAREHOUSE_ROOT / "state.duckdb"
DBT_DB = WAREHOUSE_ROOT / "dbt.duckdb"
DBT_PROJECT = PROJECT_ROOT / "dbt_v2"
RAW_ROOT = PROJECT_ROOT / "data" / "raw" / "v2"
HISTORY_ROOT = PROJECT_ROOT / "data" / "raw" / "stock_1min"


def dbt_db_for(ledger_db: Path) -> Path:
    """测试与独立部署均在账本同目录选用另一个 dbt 工作库文件。"""

    work_db = ledger_db.parent / DBT_DB.name
    if work_db.resolve() == ledger_db.resolve():
        raise ValueError("dbt work database must differ from ledger database")
    return work_db
