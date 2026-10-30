"""通过公开 CLI 留存完整新 Run，旧库与旧 walkthrough 不参与写入。"""

import argparse
from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys
from zoneinfo import ZoneInfo


def main():
    """创建独立观察库，开启正式保留模式；业务或诊断不完整时返回非零。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="新的独立观察目录")
    args = parser.parse_args()
    # 每次创建独立观察目录和新库，避免覆盖历史数据；默认目录名使用上海时间。
    root = (args.output or Path("data/probes") / (
        "walkthrough-v3_2-" + datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%dT%H%M%S%f"))).resolve()
    root.mkdir(parents=True, exist_ok=False)
    # 通过正式入口执行同一条业务流程；保留模式显式开启，不旁路 Runner 拼装诊断文件。
    completed = subprocess.run([
        sys.executable, "-m", "quant_data", "ingest", "stock_basic",
        "--db", str(root / "warehouse.duckdb"), "--landing-dir", str(root / "artifacts"), "--keep-artifacts",
    ], capture_output=True, text=True)
    print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, file=sys.stderr, end="")
    if completed.returncode:
        # 业务失败时沿用正式 CLI 的非零退出码，不能仅因有产物就视为 walkthrough 成功。
        return completed.returncode
    result = json.loads(completed.stdout)
    print(f"Walkthrough: {result['artifact_dir']}")
    # walkthrough 还要求诊断产物完整；导出不完整时即使业务发布成功也以非零退出提示。
    return 0 if result["artifacts_complete"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
