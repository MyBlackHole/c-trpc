#!/usr/bin/env bash
# 由 Echo 冒烟步骤加载；PID 必须仍是当前 shell 的直接子进程。
echo_smoke_shutdown() {
    local server_pid=$1
    local server_log=$2
    local shutdown_timeout=${3:-10s}
    local status=0

    # 正常路径必须主动停止服务端，不能让 EXIT trap 代替退出状态断言。
    if ! kill -TERM "$server_pid"; then
        cat "$server_log"
        return 1
    fi
    # Ubuntu 的 GNU tail 只负责有界等待进程消失，真实退出状态仍由 wait 取得。
    if timeout "$shutdown_timeout" tail --pid="$server_pid" --sleep-interval=0.1 -f /dev/null; then
        wait "$server_pid" || status=$?
    else
        status=$?
    fi
    if [ "$status" -ne 0 ]; then
        printf 'echo_shutdown_status=%s\n' "$status" >&2
        cat "$server_log"
    fi
    return "$status"
}
