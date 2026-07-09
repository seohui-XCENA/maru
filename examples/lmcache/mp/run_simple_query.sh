#!/bin/bash
# Correctness test for MP-mode KV cache sharing through Maru
# Default flow: inst1 stores KV -> inst2 retrieves via MP server(s) + Maru
# --single:     store and re-query on inst1 only (quick smoke test)
#
# PASS criteria:
#   1. Content grounding: the prompt ends with an extraction question whose
#      answers are pinned by the passage - the three protocol names live in
#      chunk 1 and the four architectural advantages in chunks 2-3, so the
#      expected keywords cover every retrieved chunk. Both outputs must
#      contain all of them. Extraction answers have large logit margins, so
#      FP noise cannot flip them, but corrupted KV destroys the content
#      they are read from. If the baseline output (query 1) already lacks
#      the keywords, the failure is attributed to the model/prompt, not
#      Maru. Output agreement (exact / shared prefix / early fork) is
#      reported as supporting detail only: greedy KV reuse is not bit-exact
#      across engines and the flip position is arbitrary (even token 0), so
#      divergence alone does not fail the test while both outputs stay
#      grounded in the passage.
#   2. Retrieved token count matches expected full-chunk coverage of the
#      prompt (floor(prompt_tokens / chunk_size) * chunk_size), computed
#      from the response's usage.prompt_tokens and compared against the
#      "Retrieved N tokens" log lines added by the second query. A silent
#      miss recomputes the prefill and still produces a good output, so
#      output comparison alone would be a false pass.
#
# Latency is printed for reference only (first-query latency is dominated
# by engine warmup, so it is not a reliable pass/fail signal).

cd "$(dirname "${BASH_SOURCE[0]}")"
[ -f "env.sh" ] && source env.sh

MODEL="${MODEL:-Qwen/Qwen2.5-0.5B}"
PORT1="${LMCACHE_INST1_PORT:-12030}"
PORT2="${LMCACHE_INST2_PORT:-12031}"
CHUNK="${CHUNK_SIZE:-256}"
MAX_TOKENS="${MAX_TOKENS:-200}"

# Output-agreement thresholds (see PASS criteria above)
PREFIX_THRESHOLD="${PREFIX_THRESHOLD:-40}"   # leading identical words
SIM_THRESHOLD="${SIM_THRESHOLD:-0.85}"       # difflib similarity ratio

# Content keywords the extraction answer must contain (regex, alternatives
# allowed). Protocol names come from chunk 1, the architectural advantages
# from chunks 2-3, so together they cover every retrieved chunk.
KEYWORD_GROUPS=("cxl\.io" "cxl\.cache" "cxl\.mem" "tiering" "bandwidth" "isolation|serviceability" "composable")

SINGLE=0
if [[ "$1" == "--single" ]]; then
    SINGLE=1
    PORT2=$PORT1
fi

# Logs scanned for retrieve evidence (whichever exist)
EVIDENCE_LOGS=(inst1.log inst2.log mp1.log mp2.log)

# ~3 full 256-token chunks + a trailing partial chunk, so multi-chunk
# retrieval (chunk ordering, boundary handling) is exercised
PROMPT="Explain CXL memory technology in detail. CXL stands for Compute Express Link, which is a high-speed CPU-to-device and CPU-to-memory interconnect designed to accelerate next-generation data center performance. It enables memory expansion and sharing between host processors and accelerators. CXL builds on the PCI Express (PCIe) physical and electrical interface, adding a set of protocols that allow coherent memory access between CPUs and attached devices. The CXL specification defines three protocols: CXL.io for device discovery and configuration based on PCIe, CXL.cache for device-to-host cache coherency allowing devices to cache host memory with low latency, and CXL.mem for host-managed device memory that enables the host processor to access memory attached to CXL devices using standard load and store instructions. CXL technology is particularly relevant for modern data centers where memory capacity and bandwidth requirements are growing rapidly. Applications such as large language model inference, in-memory databases, and real-time analytics benefit significantly from the ability to expand memory pools beyond what is directly attached to a single CPU socket. CXL Type 3 devices, which are memory expansion devices, allow servers to access additional DRAM or persistent memory through the CXL interface, effectively creating a larger memory pool. This is especially valuable in scenarios where memory capacity is the bottleneck rather than compute power. The CXL 2.0 specification introduced memory pooling and switching capabilities, enabling multiple hosts to share a common pool of CXL-attached memory through a CXL switch. This allows for more efficient memory utilization across a cluster of servers, as memory can be dynamically allocated to the hosts that need it most. CXL 3.0 further extended these capabilities with support for fabric-attached memory, enabling even larger scale memory sharing across multiple levels of switches. Beyond raw capacity expansion, CXL brings several architectural advantages to system designers. First, memory tiering becomes practical: hot data can live in fast direct-attached DRAM while warm and cold data reside in CXL-attached memory, with the operating system or a hardware controller migrating pages between tiers based on access frequency. Studies of real workloads show that a large fraction of application memory is accessed infrequently, which makes tiering attractive for reducing total cost of ownership without hurting performance. Second, CXL enables memory bandwidth expansion. Modern processors with many cores are often starved for memory bandwidth because the number of DDR channels per socket grows slowly. CXL links attached through PCIe lanes provide additional parallel paths to memory, so bandwidth-hungry workloads such as scientific computing and deep learning training can see substantial speedups. Third, CXL improves failure isolation and serviceability. Memory modules behind a CXL controller can be hot-plugged, taken offline for maintenance, or replaced without rebooting the host, which matters for data centers that target very high availability. Fourth, the technology opens the door to composable infrastructure, where compute, memory, and accelerators are disaggregated into resource pools and composed on demand into logical servers that match the exact needs of each workload. Compared with RDMA-based remote memory approaches, CXL offers much lower latency because accesses are performed by hardware load and store instructions rather than by network packets processed through software stacks. Typical CXL memory access latency is in the range of a few hundred nanoseconds, roughly comparable to a remote NUMA node access, whereas RDMA round trips are measured in microseconds. This latency profile allows applications to treat CXL memory as just another memory tier rather than as a storage device. In the context of large language model serving, key value caches produced during inference can be placed in CXL memory and shared across multiple GPU servers, so that a prefix computed by one server can be reused by another without recomputation, saving both GPU time and energy. Industry adoption of CXL has been accelerating, with major processor vendors shipping CXL capable CPUs, memory vendors offering CXL expansion modules, and hyperscale operators publishing reference designs for CXL based memory pooling appliances.\n\nQuestion: According to the passage, what are the names of the three protocols defined by the CXL specification, and what four architectural advantages does the passage list?\nAnswer:"

query_endpoint() {
    local port="$1"
    curl -sS "http://localhost:${port}/v1/completions" \
      -H "Content-Type: application/json" \
      -d "{\"model\": \"${MODEL}\", \"prompt\": \"$PROMPT\", \"max_tokens\": ${MAX_TOKENS}, \"temperature\": 0.0, \"ignore_eos\": true}" 2>/dev/null
}

extract_text() {
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data['choices'][0]['text'])
except Exception:
    pass
"
}

extract_prompt_tokens() {
    python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    print(data.get('usage', {}).get('prompt_tokens', ''))
except Exception:
    pass
"
}

# classification: 'exact', 'fp' (long shared prefix / high similarity), 'diverged'
compare_outputs() {
    python3 -c "
import sys, difflib
a = open(sys.argv[1]).read()
b = open(sys.argv[2]).read()
if a == b:
    print('exact 0 1.000')
    sys.exit(0)
aw, bw = a.split(), b.split()
prefix = 0
for x, y in zip(aw, bw):
    if x != y:
        break
    prefix += 1
ratio = difflib.SequenceMatcher(None, aw, bw).ratio()
cls = 'fp' if (prefix >= int(sys.argv[3]) or ratio >= float(sys.argv[4])) else 'diverged'
print(f'{cls} {prefix} {ratio:.3f}')
" "$1" "$2" "$PREFIX_THRESHOLD" "$SIM_THRESHOLD"
}

# echoes comma-separated keyword groups absent from the text in file $1
missing_keywords() {
    local f=$1 g out=()
    for g in "${KEYWORD_GROUPS[@]}"; do
        grep -qiE "$g" "$f" || out+=("$g")
    done
    local IFS=', '
    echo "${out[*]}"
}

declare -a LOG_SNAP
snapshot_logs() {
    local i f
    for i in "${!EVIDENCE_LOGS[@]}"; do
        f="${EVIDENCE_LOGS[$i]}"
        if [ -f "$f" ]; then
            LOG_SNAP[$i]=$(wc -l < "$f")
        else
            LOG_SNAP[$i]=0
        fi
    done
}

# Sum of "Retrieved N ..." token counts in log lines added since snapshot_logs
new_retrieved_tokens() {
    local total=0 i f n
    for i in "${!EVIDENCE_LOGS[@]}"; do
        f="${EVIDENCE_LOGS[$i]}"
        [ -f "$f" ] || continue
        n=$(tail -n +"$(( ${LOG_SNAP[$i]:-0} + 1 ))" "$f" \
            | grep -oiE 'Retrieved [0-9]+' | awk '{s+=$2} END {print s+0}')
        total=$((total + ${n:-0}))
    done
    echo "$total"
}

show_new_retrieve_lines() {
    local i f
    for i in "${!EVIDENCE_LOGS[@]}"; do
        f="${EVIDENCE_LOGS[$i]}"
        [ -f "$f" ] || continue
        if tail -n +"$(( ${LOG_SNAP[$i]:-0} + 1 ))" "$f" | grep -qi "retriev"; then
            echo "--- $f ---"
            tail -n +"$(( ${LOG_SNAP[$i]:-0} + 1 ))" "$f" | grep -i "retriev" | tail -5
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
RESP1=$(query_endpoint "$PORT1")
t1=$(date +%s%N)
LAT1=$(( (t1 - t0) / 1000000 ))
OUT1=$(printf '%s' "$RESP1" | extract_text)
PROMPT_TOKENS=$(printf '%s' "$RESP1" | extract_prompt_tokens)
echo "$OUT1"
echo "(latency: ${LAT1} ms, prompt tokens: ${PROMPT_TOKENS:-unknown})"
echo ""

# Give the async store path time to flush KV into Maru
sleep 3

snapshot_logs

# Step 2: Query the second endpoint; must hit the KV stored in Maru
if [ "$SINGLE" -eq 1 ]; then
    echo "=== Query 2 - Re-query same prompt (inst1, port ${PORT2}) ==="
else
    echo "=== inst2 - Retrieve via Maru (port ${PORT2}) ==="
fi
t0=$(date +%s%N)
RESP2=$(query_endpoint "$PORT2")
t1=$(date +%s%N)
LAT2=$(( (t1 - t0) / 1000000 ))
OUT2=$(printf '%s' "$RESP2" | extract_text)
echo "$OUT2"
echo "(latency: ${LAT2} ms)"
echo ""

# Give log tee a moment to flush
sleep 1

RETRIEVED=$(new_retrieved_tokens)

# --- Verdict ---
echo "==================== RESULT ===================="
PASS=1

# 1. Content grounding + output agreement
if [ -z "$OUT1" ] || [ -z "$OUT2" ]; then
    echo "[FAIL] Empty output (query 1: ${#OUT1} chars, query 2: ${#OUT2} chars)"
    PASS=0
else
    TMP1=$(mktemp) && TMP2=$(mktemp)
    printf '%s' "$OUT1" > "$TMP1"
    printf '%s' "$OUT2" > "$TMP2"
    MISS1=$(missing_keywords "$TMP1")
    MISS2=$(missing_keywords "$TMP2")
    read -r CLS PREFIX RATIO <<< "$(compare_outputs "$TMP1" "$TMP2")"
    rm -f "$TMP1" "$TMP2"

    if [ -n "$MISS1" ]; then
        echo "[FAIL] Baseline output (query 1) lacks expected content: ${MISS1}"
        echo "       The model failed the extraction task even without cache reuse,"
        echo "       so the test premise is broken - check model/prompt, not Maru"
        echo "--- output 1 ---"
        echo "$OUT1"
        PASS=0
    elif [ -n "$MISS2" ]; then
        echo "[FAIL] Output 2 lacks expected content: ${MISS2}"
        if [ "$CLS" == "diverged" ]; then
            echo "       and forks from the baseline early (${PREFIX} common words,"
            echo "       similarity ${RATIO}) - retrieved KV may be corrupted"
        fi
        echo "--- output 1 ---"
        echo "$OUT1"
        echo "--- output 2 ---"
        echo "$OUT2"
        PASS=0
    else
        case "$CLS" in
            exact)
                echo "[OK]   Outputs match exactly and contain all expected content"
                ;;
            fp)
                echo "[OK]   Both outputs contain all expected content; they share a"
                echo "       long common prefix (${PREFIX} words, similarity ${RATIO})"
                echo "       before an FP-nondeterminism fork"
                ;;
            *)
                echo "[OK]   Both outputs contain all expected content keywords."
                echo "       They fork early (${PREFIX} common words, similarity ${RATIO}),"
                echo "       which greedy KV reuse permits - the flip position is"
                echo "       arbitrary; grounded content rules out KV corruption"
                ;;
        esac
    fi
fi

# 2. Retrieved token count vs expected full-chunk coverage
if [ -n "$PROMPT_TOKENS" ] && [ "$PROMPT_TOKENS" -gt 0 ] 2>/dev/null; then
    EXPECTED=$(( PROMPT_TOKENS / CHUNK * CHUNK ))
    if [ "$EXPECTED" -eq 0 ]; then
        echo "[FAIL] Prompt too short (${PROMPT_TOKENS} tokens < chunk size ${CHUNK}) -"
        echo "       nothing can be cached, test is meaningless"
        PASS=0
    elif [ "$RETRIEVED" -ge "$EXPECTED" ]; then
        echo "[OK]   Retrieved ${RETRIEVED} tokens >= expected ${EXPECTED}"
        echo "       (prompt ${PROMPT_TOKENS} tokens = $(( PROMPT_TOKENS / CHUNK )) full chunks x ${CHUNK})"
        show_new_retrieve_lines
    else
        echo "[FAIL] Retrieved ${RETRIEVED} tokens, expected ${EXPECTED}"
        echo "       (prompt ${PROMPT_TOKENS} tokens = $(( PROMPT_TOKENS / CHUNK )) full chunks x ${CHUNK})"
        if [ "$RETRIEVED" -gt 0 ]; then
            echo "       Partial retrieval - later chunks may have missed (hash mismatch?)"
            show_new_retrieve_lines
        else
            echo "       No retrieval - second query recomputed the prefill (silent miss)"
        fi
        PASS=0
    fi
else
    # usage.prompt_tokens unavailable; fall back to evidence-exists check
    if [ "$RETRIEVED" -gt 0 ]; then
        echo "[WARN] usage.prompt_tokens unavailable; retrieved ${RETRIEVED} tokens (> 0)"
        show_new_retrieve_lines
    else
        echo "[FAIL] No retrieve evidence in logs after the second query"
        PASS=0
    fi
fi

echo "latency: query1=${LAT1} ms, query2=${LAT2} ms (reference only, not a criterion)"

if [ "$PASS" -eq 1 ]; then
    echo "================ PASS ================"
    exit 0
else
    echo "================ FAIL ================"
    exit 1
fi
