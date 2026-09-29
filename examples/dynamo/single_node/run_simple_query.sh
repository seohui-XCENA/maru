#!/bin/bash
# Cross-instance KV sharing test through the Dynamo frontend
#
# Flow: pick the two registered workers from the frontend's /health, then
#   1. send a long prompt direct-routed to worker A  -> cold prefill, KV
#      chunks stored to the Maru CXL pool
#   2. send the SAME prompt direct-routed to worker B -> B has never seen
#      it, but retrieves A's KV from the shared pool instead of recomputing
#
# The test fails unless both answers match and worker B logs a nonzero
# external prefix cache hit rate (prefix caching is disabled on the
# workers, so Maru is the only cache). With a larger model worker B's
# latency also drops well below worker A's.
# Direct routing ({"nvext": {"backend_instance_id": N}}) is what makes the
# store/retrieve split deterministic — a kv or round-robin router could
# send both requests to the same worker.
#
# Prerequisites: the full stack from single_node_example.sh (or the manual
# steps in README.md) is up and serving, with the worker logs in w0.log
# and w1.log next to this script.
#
# Usage:
#   ./run_simple_query.sh

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
FRONTEND="http://localhost:${DYN_HTTP_PORT}"

PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches.

Summarize the key benefits of CXL technology:"

# Discover the two workers' instance_ids from the frontend registry.
IDS=$(curl -sS "$FRONTEND/health" | python3 -c "
import json, sys
d = json.load(sys.stdin)
ids = sorted(x['instance_id'] for x in d.get('instances', []) if x.get('endpoint') == 'generate')
print(' '.join(str(i) for i in ids))
")
read -r ID_A ID_B <<< "$IDS" || true
if [[ -z "${ID_A:-}" || -z "${ID_B:-}" ]]; then
    echo "ERROR: expected 2 generate workers in $FRONTEND/health, got: '$IDS'"
    exit 1
fi
echo "Workers: A=$ID_A B=$ID_B (direct routing via nvext.backend_instance_id)"
echo ""

# Send one direct-routed completion; print the generated text and elapsed
# time, and keep the text in ANSWER. An HTTP error or a response without a
# completion fails the test.
send_query() {
    local instance_id="$1" label="$2"
    local t0 t1 body
    t0=$(python3 -c "import time; print(time.time())")
    if ! body=$(curl -sS --fail-with-body "$FRONTEND/v1/completions" \
        -H "Content-Type: application/json" \
        -d "{\"model\": \"${MODEL}\", \"prompt\": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$PROMPT"), \"max_tokens\": 32, \"temperature\": 0.0, \"nvext\": {\"backend_instance_id\": ${instance_id}}}"); then
        echo "FAIL: request to worker $instance_id failed: ${body:0:300}" >&2
        return 1
    fi
    t1=$(python3 -c "import time; print(time.time())")
    ANSWER=$(printf '%s' "$body" | python3 -c "
import json, sys
raw = sys.stdin.read()
try:
    print(json.loads(raw)['choices'][0]['text'].strip())
except Exception:
    print('FAIL: unexpected response: ' + raw[:300], file=sys.stderr)
    sys.exit(1)
")
    echo "$ANSWER"
    python3 -c "print(f'  elapsed: {$t1 - $t0:.3f}s  ($label)')"
}

echo "=== Worker A ($ID_A) — cold prefill, KV stored to Maru ==="
send_query "$ID_A" "cold: full prefill + store"
ANSWER_A=$ANSWER
echo ""

# Give worker A's asynchronous KV stores a moment to land.
sleep 2

echo "=== Worker B ($ID_B) — first sight of this prompt, retrieves A's KV ==="
send_query "$ID_B" "warm: cross-instance retrieve from the CXL pool"
ANSWER_B=$ANSWER
echo ""

if [[ "$ANSWER_A" != "$ANSWER_B" ]]; then
    echo "FAIL: workers A and B answered differently (temperature 0, same prompt)."
    exit 1
fi
echo "PASS: both workers returned the same answer."

# With the tiny demo model the prefill takes a few ms, so latency is not a
# reliable signal. The pass condition is worker B's external prefix cache
# counter: nonzero means its prefill tokens were served from Maru rather
# than computed. vLLM logs the counter every ~10s.
B_LOGS=$(grep -l -a -F "$ID_B" w0.log w1.log 2>/dev/null || true)
if [[ -z "$B_LOGS" || "$B_LOGS" == *$'\n'* ]]; then
    echo "FAIL: cannot identify worker B's log (w0.log/w1.log naming instance $ID_B)."
    echo "  Run the workers with their output in w0.log and w1.log (see README.md)."
    exit 1
fi
echo "Waiting for worker B ($B_LOGS) to log its external prefix cache hit rate..."
rate=""
for i in $(seq 1 30); do
    rate=$(sed -n 's/.*External prefix cache hit rate: \([0-9.]*\)%.*/\1/p' "$B_LOGS" | tail -1)
    if [[ -n "$rate" ]] && python3 -c "import sys; sys.exit(0 if float(sys.argv[1]) > 0 else 1)" "$rate"; then
        echo "PASS: worker B external prefix cache hit rate ${rate}% — its prefill was served from the Maru pool."
        exit 0
    fi
    sleep 1
done
last="${rate:+${rate}%}"
echo "FAIL: worker B logged no nonzero external prefix cache hit rate within 30s (last: ${last:-none}) — its prefill was recomputed, not retrieved from Maru."
exit 1
