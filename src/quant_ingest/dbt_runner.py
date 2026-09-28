"""运行 V2 dbt 命令，并把完整结果留在 run 级过程日志。"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from datetime import date
from pathlib import Path

from .raw import RawStore


ROOT = Path(__file__).resolve().parents[2]


def run_dbt(
    command: str, selector: str, variables: dict, *,
    project: Path, database: Path, raw: RawStore, dataset: str, day: date, run_id: str,
) -> None:
    """执行一条 dbt 命令；先记录输出和返回码，再决定是否阻断发布。"""

    with tempfile.TemporaryDirectory() as profile_dir:
        profile = Path(profile_dir) / "profiles.yml"
        profile.write_text((project / "profiles.yml.example").read_text())
        # dbt 不需要供应商密钥；从子进程环境移除，日志再做兜底脱敏。
        env = {key: value for key, value in os.environ.items() if key != "TUSHARE_API_KEY"}
        env["QUANT_V2_DB_PATH"] = str(database)
        cmd = [
            "dbt", command, "--select", selector, "--project-dir", str(project),
            "--profiles-dir", profile_dir, "--vars", json.dumps(variables),
        ]
        try:
            result = subprocess.run(cmd, cwd=ROOT, env=env, text=True, capture_output=True)
        except OSError as exc:
            raw.log(run_id, dataset, day, "dbt_command", command=command, selector=selector,
                    returncode=None, error=type(exc).__name__)
            raise RuntimeError(f"dbt {command} could not start: {type(exc).__name__}") from exc

    secret = os.getenv("TUSHARE_API_KEY")
    stdout = result.stdout or ""
    stderr = result.stderr or ""
    if secret:
        stdout = stdout.replace(secret, "[redacted]")
        stderr = stderr.replace(secret, "[redacted]")
    raw.log(
        run_id, dataset, day, "dbt_command", command=command, selector=selector,
        returncode=result.returncode, stdout=stdout, stderr=stderr,
    )
    if result.returncode:
        raise RuntimeError(f"dbt {command} failed:\n{(stdout + stderr)[-5000:]}")
