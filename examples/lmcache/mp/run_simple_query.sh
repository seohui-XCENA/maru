#!/bin/bash
# Correctness test for MP-mode KV cache sharing through Maru
# Default flow: inst1 stores KV -> inst2 retrieves via MP server(s) + Maru
# --single:     store and re-query on inst1 only (quick smoke test)
#
# PASS criteria:
#   1. Outputs of the two queries are identical (greedy decoding)
#   2. Retrieve evidence appears in the logs after the second query
#      (a miss silently falls back to recompute, so output match alone
#       is NOT sufficient to prove the cache was used)

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
PORT1="${LMCACHE_INST1_PORT:-12030}"
PORT2="${LMCACHE_INST2_PORT:-12031}"

SINGLE=0
if [[ "$1" == "--single" ]]; then
    SINGLE=1
    PORT2=$PORT1
fi

# Logs scanned for retrieve evidence (whichever exist)
EVIDENCE_LOGS=(inst1.log inst2.log mp1.log mp2.log)

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches.\n\nSummarize the key benefits of CXL technology:"

send_query() {
    local port="$1"
    curl -sS "http://localhost:${port}/v1/completions" \
      -H "Content-Type: application/json" \
      -d "{\"model\": \"${MODEL}\", \"prompt\": \"$PROMPT\", \"max_tokens\": 200, \"temperature\": 0.0, \"ignore_eos\": true}" 2>&1 \
      | python3 -c "
import sys, json
output = []
for line in sys.stdin:
    line = line.strip()
    if not line or line == 'data: [DONE]':
        continue
    if line.startswith('data: '):
        line = line[6:]
    try:
        data = json.loads(line)
        output.append(data['choices'][0]['text'])
    except (json.JSONDecodeError, KeyError, IndexError):
        pass
print(''.join(output))
"
}

count_retrieve_evidence() {
    local total=0 c
    for f in "${EVIDENCE_LOGS[@]}"; do
        if [ -f "$f" ]; then
            c=$(grep -ci "retriev" "$f" 2>/dev/null) || c=0
            total=$((total + c))
        fi
    done
    echo "$total"
}

show_retrieve_evidence() {
    for f in "${EVIDENCE_LOGS[@]}"; do
        if [ -f "$f" ] && grep -qi "retriev" "$f" 2>/dev/null; then
            echo "--- $f ---"
            grep -i "retriev" "$f" | tail -3
        fi
    done
}

echo "=== Prompt ==="
echo "$PROMPT"
echo ""

# Step 1: Query the first endpoint to store KV cache
if [ "$SINGLE" -eq 1 ]; then
    echo "=== Query 1 - Store KV (inst1, port ${PORT1}) ==="
else
    echo "=== inst1 - Store KV (port ${PORT1}) ==="
fi
t0=$(date +%s%N)
OUT1=$(send_query "$PORT1")
t1=$(date +%s%N)
LAT1=$(( (t1 - t0) / 1000000 ))
echo "$OUT1"
echo "(latency: ${LAT1} ms)"
echo ""

# Give the async store path time to flush KV into Maru
sleep 3

BEFORE=$(count_retrieve_evidence)

# Step 2: Query the second endpoint; must hit the KV stored in Maru
if [ "$SINGLE" -eq 1 ]; then
    echo "=== Query 2 - Re-query same prompt (inst1, port ${PORT2}) ==="
else
    echo "=== inst2 - Retrieve via Maru (port ${PORT2}) ==="
fi
t0=$(date +%s%N)
OUT2=$(send_query "$PORT2")
t1=$(date +%s%N)
LAT2=$(( (t1 - t0) / 1000000 ))
echo "$OUT2"
echo "(latency: ${LAT2} ms)"
echo ""

AFTER=$(count_retrieve_evidence)

# --- Verdict ---
echo "==================== RESULT ===================="
PASS=1

if [ -z "$OUT1" ] || [ -z "$OUT2" ]; then
    echo "[FAIL] Empty output (query 1: ${#OUT1} chars, query 2: ${#OUT2} chars)"
    PASS=0
elif [ "$OUT1" == "$OUT2" ]; then
    echo "[OK]   Outputs match exactly (greedy decoding)"
else
    echo "[FAIL] Outputs differ - KV cache may be corrupted"
    echo "--- output 1 ---"
    echo "$OUT1"
    echo "--- output 2 ---"
    echo "$OUT2"
    PASS=0
fi

if [ "$AFTER" -gt "$BEFORE" ]; then
    echo "[OK]   Retrieve evidence found in logs (+$((AFTER - BEFORE)) lines)"
    show_retrieve_evidence
else
    echo "[FAIL] No new retrieve evidence in logs - second query likely"
    echo "       recomputed the prefill instead of hitting the cache"
    PASS=0
fi

echo "latency: query1=${LAT1} ms, query2=${LAT2} ms"

if [ "$PASS" -eq 1 ]; then
    echo "================ PASS ================"
    exit 0
else
    echo "================ FAIL ================"
    exit 1
fi
