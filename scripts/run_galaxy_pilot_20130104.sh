#!/usr/bin/env bash
# 历史单日 Pilot：须先确认交接文档中的 19 个证据缺口及本次 B 阶段执行范围。
set -euo pipefail
umask 077

# 日期与范围固定，禁止把额外参数静默忽略。
if (( $# != 0 )); then
    printf 'This fixed pilot accepts no arguments.\n' >&2
    exit 2
fi
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
python="$repo_root/.venv/bin/python"
database="$repo_root/data/warehouse.duckdb"
cd "$repo_root"
test -x "$python"
test -f "$database"
for program in setsid ps awk du stat; do
    command -v "$program" >/dev/null
done

# 仅清理脚本及子进程的代理变量；执行者需另行关闭系统代理/TUN。
unset HTTP_PROXY HTTPS_PROXY ALL_PROXY NO_PROXY http_proxy https_proxy all_proxy no_proxy
export PYTHONDONTWRITEBYTECODE=1
run_dir="$repo_root/data/landing/galaxy-pilot-20130104-$(TZ=Asia/Shanghai date +%Y%m%dT%H%M%S%z)-$$"
mkdir -p "$run_dir"
printf 'Execution logs: %s\n' "$run_dir"
printf 'elapsed_seconds,process_tree_rss_bytes,system_available_bytes,database_bytes,landing_allocated_bytes\n' >"$run_dir/resources.csv"

# 中断或监测命令失败时收尾本脚本创建的进程组，不留下脱离监测的写库进程。
cli_pid=''
cleanup() {
    local script_status=$?
    trap - EXIT INT TERM
    if [[ -n "$cli_pid" ]]; then
        kill -TERM -- "-$cli_pid" 2>/dev/null || true
        wait "$cli_pid" 2>/dev/null || true
    fi
    printf '%s\n' "$script_status" >"$run_dir/script.exit-code"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# 固定业务范围为全市场单日分钟；不请求权重、不修改既有验收与有界重试机制。
# 生产分钟禁止添加 --keep-artifacts：它会导出整张 Canonical 和全阶段快照，显著增加内存、I/O 与磁盘风险。
started=$SECONDS
setsid "$python" -m quant_data ingest stock_1min \
    --start-date 20130104 --end-date 20130104 --parallel 2 \
    --db "$database" --landing-dir "$repo_root/data/landing" \
    >"$run_dir/stock_1min.json" 2>"$run_dir/stock_1min.stderr.log" &
cli_pid=$!

while :; do
    # 对 CLI 父进程及全部后代求 RSS 和；共享页可能重复计入，值为采样估计。
    rss=$(ps -eo pid=,ppid=,rss= | awk -v root="$cli_pid" '
        { parent[$1]=$2; resident[$1]=$3 }
        END {
            total=0
            for (pid in resident) {
                cursor=pid
                while (cursor>1 && cursor!=root) cursor=parent[cursor]+0
                if (cursor==root) total+=resident[pid]
            }
            printf "%.0f",total*1024
        }')
    available=$(awk '/^MemAvailable:/ {printf "%.0f",$2*1024; found=1} END {if (!found) printf "-1"}' /proc/meminfo)
    database_bytes=$(stat -c %s "$database")
    # Landing 正在创建或清理，du 可能遇到并发消失文件；-1 显式表示该次采样不完整。
    if landing=$(du -s -B1 "$repo_root/data/landing" 2>/dev/null); then
        landing_bytes=${landing%%$'\t'*}
    else
        landing_bytes=-1
    fi
    printf '%s,%s,%s,%s,%s\n' "$((SECONDS-started))" "$rss" "$available" "$database_bytes" "$landing_bytes" >>"$run_dir/resources.csv"
    if ! kill -0 "$cli_pid" 2>/dev/null; then
        break
    fi
    sleep 2
done

if wait "$cli_pid"; then
    status=0
else
    status=$?
fi
cli_pid=''
printf '%s\n' "$status" >"$run_dir/stock_1min.exit-code"
printf 'Minute CLI exit %s; return logs for verification: %s\n' "$status" "$run_dir"
# PARTIAL/FAILED 原样交回，资源采样不放宽规则、不转换退出码，也不自动重跑。
exit "$status"
