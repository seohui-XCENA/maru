#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi

echo "Warning: LMCache MP connector support for vLLM v1 is experimental and subject to change."

# Load common environment variables
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

PIDS=()

# Switch to the directory of the current script
cd "$(dirname "${BASH_SOURCE[0]}")"

check_num_gpus() {
    num_gpus=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    if [ "$num_gpus" -lt 2 ]; then
        echo "You need at least 2 GPUs to run KV cache sharing."
        exit 1
    else
        echo "Found $num_gpus GPUs."
    fi
}

ensure_python_library_installed() {
    echo "Checking if $1 is installed..."
    python3 -c "import $1" > /dev/null 2>&1
    if [ $? -ne 0 ]; then
        echo "$1 is not installed. Please install it via pip install $1."
        exit 1
    else
        echo "$1 is installed."
    fi
}

kill_tree() {
    # Recursively kill a process and all its descendants
    local pid=$1 sig=${2:-TERM}
    for child in $(pgrep -P "$pid" 2>/dev/null); do
        kill_tree "$child" "$sig"
    done
    kill -"$sig" "$pid" 2>/dev/null
}

cleanup() {
    echo "Stopping everything..."
    trap - INT TERM USR1 EXIT   # prevent re-entrancy

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Killing process tree of $pid"
            kill_tree "$pid" TERM
        fi
    done

    sleep 2

    for pid in "${PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            echo "Force killing process tree of $pid"
            kill_tree "$pid" 9
        fi
    done

    echo "All processes stopped."
    exit 0
}

wait_for_tcp_port() {
    local name=$1 port=$2
    local timeout_seconds=60
    local start_time=$(date +%s)

    echo "[$(date +%T)] Waiting for $name on port $port (timeout: ${timeout_seconds}s)..."
    while true; do
        if timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
            echo "[$(date +%T)] $name is ready on port $port"
            return 0
        fi
        if (( $(date +%s) - start_time >= timeout_seconds )); then
            echo "[$(date +%T)] Timeout waiting for $name on port $port (${timeout_seconds}s)"
            return 1
        fi
        sleep 1
    done
}

wait_for_vllm() {
    local port=$1 log_file=$2
    local timeout_seconds=1200
    local start_time=$(date +%s)
    local last_report=$start_time

    echo "[$(date +%T)] Waiting for vLLM on port $port (timeout: ${timeout_seconds}s)..."

    while true; do
        if curl -s "localhost:${port}/v1/completions" > /dev/null; then
            echo "[$(date +%T)] vLLM on port $port is ready"
            return 0
        fi

        local now=$(date +%s)
        if (( now - last_report >= 30 )); then
            local elapsed=$(( now - start_time ))
            echo "[$(date +%T)] Still waiting for port $port... (${elapsed}s elapsed)"
            if [ -f "$log_file" ]; then
                echo "  [last log] $(tail -1 "$log_file" 2>/dev/null)"
            fi
            last_report=$now
        fi

        if (( now - start_time >= timeout_seconds )); then
            echo "[$(date +%T)] Timeout waiting for vLLM on port $port (${timeout_seconds}s)"
            if [ -f "$log_file" ]; then
                echo "--- log (last 20 lines) ---"
                tail -20 "$log_file" 2>/dev/null
            fi
            return 1
        fi

        sleep 1
    done
}

launch_mp_server() {
    local name=$1 port=$2 http_port=$3 prom_port=$4 log_file=$5

    echo "[$(date +%T)] Launching LMCache MP server '$name' on port $port (http: $http_port, prometheus: $prom_port)..."
    PYTHONHASHSEED=123 \
    PYTHONUNBUFFERED=1 \
    LMCACHE_DISABLE_BANNER=1 \
        lmcache server \
        --port "$port" \
        --chunk-size "$CHUNK_SIZE" \
        --maru-server-url "maru://localhost:${MARU_SERVER_PORT}" \
        --maru-pool-size-gb "$MARU_POOL_SIZE_GB" \
        --maru-instance-id "$name" \
        --l1-size-gb 0 \
        --supported-transfer-mode lmcache_driven \
        --eviction-policy LRU \
        --http-port "$http_port" \
        --prometheus-port "$prom_port" \
        > >(tee "$log_file") 2>&1 &
    local pid=$!
    PIDS+=($pid)
    echo "[$(date +%T)] LMCache MP server '$name' PID: $pid (log: $log_file)"
    sleep 2
    if ! kill -0 $pid 2>/dev/null; then
        echo "[$(date +%T)] ERROR: LMCache MP server '$name' died! Log:"
        cat "$log_file" 2>/dev/null || true
        return 1
    fi
    wait_for_tcp_port "LMCache MP server '$name'" "$port"
}

main() {
    echo "Using Maru storage backend with LMCache MP connector (mode: $MODE)..."

    check_num_gpus
    ensure_python_library_installed lmcache
    ensure_python_library_installed vllm

    trap cleanup INT TERM USR1 EXIT

    # Launch MaruServer
    if timeout 1 bash -c "echo >/dev/tcp/localhost/$MARU_SERVER_PORT" 2>/dev/null; then
        echo "[$(date +%T)] MaruServer already running on port $MARU_SERVER_PORT, skipping launch..."
    else
        echo "[$(date +%T)] Launching MaruServer on port $MARU_SERVER_PORT..."
        echo "[$(date +%T)] maru package: $(python3 -c 'import maru; print(maru.__file__)' 2>&1)"
        PYTHONUNBUFFERED=1 python3 -m maru_server --port $MARU_SERVER_PORT \
            --rm-address "${MARU_RM_ADDRESS:-127.0.0.1:9850}" \
            --log-level "${_LOG_LEVEL:-INFO}" \
            "${DAX_ARGS[@]}" \
            > >(tee "${LOG_DIR:-.}/maru_server.log") 2>&1 &
        maru_server_pid=$!
        PIDS+=($maru_server_pid)
        echo "[$(date +%T)] MaruServer PID: $maru_server_pid (log: ${LOG_DIR:-.}/maru_server.log)"
        sleep 2
        if ! kill -0 $maru_server_pid 2>/dev/null; then
            echo "[$(date +%T)] ERROR: MaruServer process died! Log:"
            cat "${LOG_DIR:-.}/maru_server.log" 2>/dev/null || true
            return 1
        fi
        wait_for_tcp_port "MaruServer" $MARU_SERVER_PORT
        echo "[$(date +%T)] MaruServer ready."
    fi

    # Log file names (injectable via env vars)
    LOG_MP1="${LOG_MP1:-mp1.log}"
    LOG_MP2="${LOG_MP2:-mp2.log}"
    LOG_INST1="${LOG_INST1:-inst1.log}"
    LOG_INST2="${LOG_INST2:-inst2.log}"

    # Launch LMCache MP server(s)
    # shared:   inst1 + inst2 -> mp1 -> Maru   (one MP server serving both engines)
    # separate: inst1 -> mp1 -> Maru <- mp2 <- inst2   (sharing happens through Maru only,
    #           emulating multiple MP servers on different nodes sharing a CXL pool)
    if [[ "$MODE" == "shared" ]]; then
        launch_mp_server "mp1" "$LMCACHE_MP1_PORT" "$LMCACHE_MP1_HTTP_PORT" "$LMCACHE_MP1_PROM_PORT" "$LOG_MP1" || return 1
        INST1_MP_PORT=$LMCACHE_MP1_PORT
        INST2_MP_PORT=$LMCACHE_MP1_PORT
    else
        launch_mp_server "mp1" "$LMCACHE_MP1_PORT" "$LMCACHE_MP1_HTTP_PORT" "$LMCACHE_MP1_PROM_PORT" "$LOG_MP1" || return 1
        launch_mp_server "mp2" "$LMCACHE_MP2_PORT" "$LMCACHE_MP2_HTTP_PORT" "$LMCACHE_MP2_PROM_PORT" "$LOG_MP2" || return 1
        INST1_MP_PORT=$LMCACHE_MP1_PORT
        INST2_MP_PORT=$LMCACHE_MP2_PORT
    fi

    echo "[$(date +%T)] Launching vLLM instances (MODEL=${_MODEL:-Qwen/Qwen2.5-0.5B}, GPU_MEM_UTIL=$GPU_MEM_UTIL)..."
    echo "Please check $LOG_INST1 and $LOG_INST2 for logs."

    # Launch Instance 1 (GPU 0) and wait for it to be ready
    bash mp_vllm_launcher.sh inst1 "$INST1_MP_PORT" ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_INST1") 2>&1 &
    inst1_pid=$!
    PIDS+=($inst1_pid)
    wait_for_vllm $LMCACHE_INST1_PORT "$LOG_INST1"

    # Launch Instance 2 (GPU 1) after Instance 1 is ready
    bash mp_vllm_launcher.sh inst2 "$INST2_MP_PORT" ${_MODEL:+"$_MODEL"} \
        > >(tee "$LOG_INST2") 2>&1 &
    inst2_pid=$!
    PIDS+=($inst2_pid)
    wait_for_vllm $LMCACHE_INST2_PORT "$LOG_INST2"

    echo "==================================================="
    echo "All servers are up (mode: $MODE)."
    echo "  inst1: localhost:$LMCACHE_INST1_PORT -> MP server :$INST1_MP_PORT"
    echo "  inst2: localhost:$LMCACHE_INST2_PORT -> MP server :$INST2_MP_PORT"
    echo "Run the correctness test in another terminal:"
    echo "  bash run_simple_query.sh"
    echo "Press Ctrl-C to terminate all instances."
    echo "==================================================="

    while true; do
        sleep 1
    done
}

# --- Help ---
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Launch two vLLM instances sharing KV cache through Maru via LMCache MP server(s)."
    echo "Servers stay running until Ctrl-C."
    echo ""
    echo "Options:"
    echo "  --mode MODE            Topology: 'shared' or 'separate' (default: shared)"
    echo "                           shared:   inst1 + inst2 -> one MP server -> Maru"
    echo "                           separate: each instance gets its own MP server;"
    echo "                                     sharing happens only through Maru (CXL),"
    echo "                                     emulating multi-node MP servers"
    echo "  --model MODEL          HuggingFace model name (default: Qwen/Qwen2.5-0.5B)"
    echo "  --dax-path PATH        DAX device path for MaruServer (repeatable)"
    echo "  --log-level LEVEL      Log level: DEBUG, INFO, WARNING, ERROR"
    echo "  -h, --help             Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                                  # shared MP server"
    echo "  $0 --mode separate                  # one MP server per instance"
    echo "  $0 --mode separate --dax-path /dev/dax9.0"
    echo "  $0 --model Qwen/Qwen3-8B"
    echo ""
    echo "Environment variables (from env.sh):"
    echo "  LMCACHE_PORT_BASE      Port base (default: 12000 + UID)"
    echo "  LMCACHE_INST1_PORT     Instance 1 port (default: PORT_BASE + 30)"
    echo "  LMCACHE_INST2_PORT     Instance 2 port (default: PORT_BASE + 31)"
    echo "  LMCACHE_MP1_PORT       MP server 1 port (default: PORT_BASE + 40)"
    echo "  LMCACHE_MP2_PORT       MP server 2 port (default: PORT_BASE + 41)"
    echo "  MARU_SERVER_PORT       MaruServer port (default: 10000 + UID)"
    echo "  MARU_POOL_SIZE_GB      CXL pool size per MP server in GB (default: 4)"
    echo "  CHUNK_SIZE             LMCache chunk size (default: 256)"
    echo "  GPU_MEM_UTIL           GPU memory utilization (default: 0.1)"
    exit 0
}

# --- Argument parsing ---
MODE="shared"
_LOG_LEVEL=""
_MODEL=""
DAX_ARGS=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)   usage ;;
        --mode)      MODE="$2"; shift 2 ;;
        --log-level) _LOG_LEVEL="$2"; shift 2 ;;
        --model)     _MODEL="$2"; shift 2 ;;
        --dax-path)  DAX_ARGS+=(--dax-path "$2"); shift 2 ;;
        *) echo "Unknown option: $1"; usage ;;
    esac
done

if [[ "$MODE" != "shared" && "$MODE" != "separate" ]]; then
    echo "Invalid mode: $MODE (should be 'shared' or 'separate')"
    exit 1
fi

# Apply log level (override env.sh defaults if specified)
if [[ -n "$_LOG_LEVEL" ]]; then
    export VLLM_LOG_LEVEL="$_LOG_LEVEL"
    export LMCACHE_LOG_LEVEL="$_LOG_LEVEL"
fi

main
