#!/bin/bash

export VLLM_LOG_LEVEL=${VLLM_LOG_LEVEL:-DEBUG}
export LMCACHE_LOG_LEVEL=${LMCACHE_LOG_LEVEL:-INFO}
export GPU_MEM_UTIL=${GPU_MEM_UTIL:-0.1}

# Port base configuration
# Uses user ID to avoid port conflicts between users on shared machines
export LMCACHE_PORT_BASE=${LMCACHE_PORT_BASE:-$((12000 + $(id -u)))}

# vLLM instance ports
export LMCACHE_INST1_PORT=${LMCACHE_INST1_PORT:-$((LMCACHE_PORT_BASE + 30))}
export LMCACHE_INST2_PORT=${LMCACHE_INST2_PORT:-$((LMCACHE_PORT_BASE + 31))}

# LMCache MP server ports
# shared mode uses MP1 only; separate mode uses MP1 (inst1) and MP2 (inst2)
export LMCACHE_MP1_PORT=${LMCACHE_MP1_PORT:-$((LMCACHE_PORT_BASE + 40))}
export LMCACHE_MP2_PORT=${LMCACHE_MP2_PORT:-$((LMCACHE_PORT_BASE + 41))}

# Per-MP-server observability ports (defaults 8080/9090 collide when two
# MP servers run on the same machine, so always assign them explicitly)
export LMCACHE_MP1_HTTP_PORT=${LMCACHE_MP1_HTTP_PORT:-$((LMCACHE_PORT_BASE + 50))}
export LMCACHE_MP2_HTTP_PORT=${LMCACHE_MP2_HTTP_PORT:-$((LMCACHE_PORT_BASE + 51))}
export LMCACHE_MP1_PROM_PORT=${LMCACHE_MP1_PROM_PORT:-$((LMCACHE_PORT_BASE + 60))}
export LMCACHE_MP2_PROM_PORT=${LMCACHE_MP2_PROM_PORT:-$((LMCACHE_PORT_BASE + 61))}

# Maru Server port
export MARU_SERVER_PORT=${MARU_SERVER_PORT:-$((10000 + $(id -u)))}

# LMCache MP server settings
export CHUNK_SIZE=${CHUNK_SIZE:-256}
export MARU_POOL_SIZE_GB=${MARU_POOL_SIZE_GB:-4}

# GPU assignment
export INST1_DEVICE=${INST1_DEVICE:-0}
export INST2_DEVICE=${INST2_DEVICE:-1}
