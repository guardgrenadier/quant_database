#!/usr/bin/env bash
# 单次分钟补跑交接脚本：用户确认 B 阶段并关闭系统代理/TUN 后手动执行。
set -euo pipefail
umask 077

# 固定批次不接受参数，避免其他日期或子集参数被静默忽略。
if (( $# != 0 )); then
    printf 'This fixed validation batch accepts no arguments.\n' >&2
    exit 2
fi

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python="$repo_root/.venv/bin/python"
database="$repo_root/data/warehouse.duckdb"
cd "$repo_root"

# 生产入口边界：必须使用已有权威库和环境，路径错误不能创建另一个空库。
test -x "$python"
test -f "$database"

# 只清除本脚本及子进程的代理变量；系统代理/TUN 需由执行者事先关闭。
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy
export PYTHONDONTWRITEBYTECODE=1

run_dir="$repo_root/data/landing/galaxy-minute-validation-20261030-$(TZ=Asia/Shanghai date +%Y%m%dT%H%M%S%z)-$$"
mkdir -p "$run_dir"
printf 'Execution logs: %s\n' "$run_dir"

# 固定业务范围：仅补跑全市场股票分钟；权重供应商滞后不作为分钟的前置门禁。
# 生产分钟禁止添加 --keep-artifacts：成功留痕使用库内 Meta，不能为诊断导出整张 Canonical。
dataset=stock_1min
printf 'START %s\n' "$dataset"
if "$python" -m quant_data ingest "$dataset" \
    --start-date 20261030 --end-date 20261030 --parallel 2 \
    --db "$database" --landing-dir "$repo_root/data/landing" \
    >"$run_dir/$dataset.json" 2>"$run_dir/$dataset.stderr.log"; then
    status=0
else
    status=$?
fi
printf '%s\n' "$status" >"$run_dir/$dataset.exit-code"
# PARTIAL 与 FAILED 均返回非零；保留证据并停止，不自行重跑。
if (( status != 0 )); then
    printf 'STOP %s: exit %s; inspect %s\n' "$dataset" "$status" "$run_dir" >&2
    exit "$status"
fi

printf 'Minute CLI succeeded. Return the log directory for C-stage verification: %s\n' "$run_dir"
