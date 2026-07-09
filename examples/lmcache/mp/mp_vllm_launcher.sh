#!/bin/bash

if [ -z "${VIRTUAL_ENV:-}" ]; then
    echo "Warning: No virtual environment detected. Consider activating a venv first."
fi
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"

# NOTE: For correct KV cache sharing, ensure all processes use the same PYTHONHASHSEED to keep the hash of the KV cache consistent across processes.

GPU_MEM_UTIL="${GPU_MEM_UTIL:-0.1}"

if [[ $# -lt 2 ]]; then
    echo "Usage: $0 <inst1 | inst2> <mp_server_port> [model]"
    exit 1
fi

ROLE=$1
MP_PORT=$2

if [[ $# -ge 3 ]]; then
    MODEL=$3
else
    MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
fi
echo "Using model: ${MODEL}"

if [[ $ROLE == "inst1" ]]; then
    DEVICE=$INST1_DEVICE
    PORT=$LMCACHE_INST1_PORT
elif [[ $ROLE == "inst2" ]]; then
    DEVICE=$INST2_DEVICE
    PORT=$LMCACHE_INST2_PORT
else
    echo "Invalid role: $ROLE"
    echo "Should be either inst1, inst2"
    exit 1
fi

echo "Connecting to LMCache MP server on port ${MP_PORT}"

# --no-enable-prefix-caching: without this, vLLM's own prefix cache serves
# repeated prompts from GPU memory and the LMCache/Maru path is never exercised.
PYTHONHASHSEED=123 \
    CUDA_VISIBLE_DEVICES=$DEVICE \
    vllm serve $MODEL \
    --trust-remote-code \
    --gpu-memory-utilization $GPU_MEM_UTIL \
    --port $PORT \
    --no-enable-prefix-caching \
    --kv-transfer-config \
    "{\"kv_connector\":\"LMCacheMPConnector\", \"kv_role\":\"kv_both\", \"kv_connector_extra_config\":{\"lmcache.mp.port\":${MP_PORT}}}"
