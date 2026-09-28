"""项目路径与仅从环境变量读取的本地凭证。"""

import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 三类数据目录严格分离：raw 可恢复、candidate 不可被消费、canonical 长期保存。
DUCKDB_PATH = PROJECT_ROOT / "data" / "warehouse" / "quant.duckdb"
RAW_TUSHARE_DIR = PROJECT_ROOT / "data" / "raw" / "tushare"
RAW_STK_MINS_DIR = PROJECT_ROOT / "data" / "raw" / "stk_mins"
CANONICAL_STOCK_1MIN_DIR = PROJECT_ROOT / "data" / "warehouse" / "canonical" / "stock_1min"
CANDIDATE_STOCK_1MIN_DIR = PROJECT_ROOT / "data" / "warehouse" / "candidates" / "stock_1min"
TUSHARE_TOKEN = os.getenv("TUSHARE_TOKEN")
