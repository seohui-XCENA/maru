# MP-mode KV Cache Sharing with Maru

This example verifies KV cache sharing correctness between multiple vLLM instances using the **LMCache MP connector** (`LMCacheMPConnector`) with Maru (CXL shared memory) as the storage backend.

Unlike the `p2p_sharing` example (where the LMCache engine lives inside each vLLM worker), MP mode runs the LMCache engine as a **standalone `lmcache server` process** that vLLM connects to over IPC.

---

## Topologies

Two topologies are supported via `--mode`:

### `shared` (default) — one MP server serving both engines

```
inst1 (GPU0) ─┐
              ├─> lmcache server (mp1) ──> MaruServer ──> /dev/daxX.Y
inst2 (GPU1) ─┘
```

Verifies that a single MP server correctly stores/retrieves KV through Maru for multiple engines.

### `separate` — one MP server per engine, sharing only through Maru

```
inst1 (GPU0) ──> lmcache server (mp1) ──┐
                                        ├─> MaruServer ──> /dev/daxX.Y
inst2 (GPU1) ──> lmcache server (mp2) ──┘
```

Emulates the ultimate multi-node deployment on a single machine: multiple MP servers (as if on different nodes) sharing KV cache **only through the Maru CXL pool**. KV stored by `mp1` must be discovered and read back by `mp2`.

Both MP servers run with `--l1-size-gb 0`, so every store/retrieve goes through Maru — there is no local L1 that could mask a broken Maru path.

---

## Prerequisites

1. Install maru package

```bash
cd ~/maru
pip install -e .
```

2. LMCache with Maru backend support (`lmcache server --maru-server-url ...`)
3. At least 2 GPUs available

---

## Running the Test

```bash
cd examples/lmcache/mp

# Shared MP server topology
bash mp_example.sh --dax-path /dev/dax9.0

# Or: one MP server per instance (multi-node emulation)
bash mp_example.sh --mode separate --dax-path /dev/dax9.0

# In another terminal: run the correctness check
bash run_simple_query.sh

# Quick smoke test (store + re-query on inst1 only)
bash run_simple_query.sh --single
```

If you launched with a non-default model, pass it to the query script:

```bash
MODEL=Qwen/Qwen3-8B bash run_simple_query.sh
```

---

## What the Test Asserts

`run_simple_query.sh` sends a long prompt (~834 tokens = 3 full 256-token chunks + a partial chunk) to inst1, then the **same prompt** to inst2, and checks:

1. **Content grounding** — the prompt ends with an extraction question whose answers are pinned by the passage: the three protocol names (`CXL.io`, `CXL.cache`, `CXL.mem`) live in chunk 1 and the four architectural advantages (tiering, bandwidth, isolation/serviceability, composable) in chunks 2–3, so the expected keywords cover every retrieved chunk. **Both outputs must contain all keywords.** Extraction answers have large logit margins, so FP noise cannot flip them — but corrupted KV destroys the content they are read from. If the *baseline* output already lacks the keywords, the failure is attributed to the model/prompt, not Maru.
2. **Output agreement (informational)** — exact match / long shared prefix (≥ 40 words or similarity ≥ 0.85, tunable via `PREFIX_THRESHOLD`/`SIM_THRESHOLD`) / early fork. Greedy KV reuse is **not bit-exact across engines**: the recomputed suffix takes a different kernel path, slightly different logits can flip one greedy token, and the fork position is arbitrary — even token 0. Divergence alone therefore does not fail the test while both outputs stay grounded.
3. **Retrieved token count matches expected chunk coverage** — the response's `usage.prompt_tokens` gives the expected retrievable amount (`floor(prompt_tokens / 256) * 256`); the `Retrieved N tokens` log lines added by the second query must sum to at least that. This catches **partial retrieval** (e.g., only 1 of 3 chunks hitting due to a hash mismatch) and **silent misses** (vLLM recomputes the prefill and still produces a correct output), neither of which output comparison alone can detect.

Latency is printed for reference only — the first query is dominated by engine warmup, so it is not a pass/fail criterion.

Example retrieve evidence (in `mp1.log` / `mp2.log`):

```
LMCache INFO: Retrieved 768 tokens in 0.010 seconds (lmcache_driven_transfer.py:1304:...)
```

---

## Notes

- Both vLLM instances run with `--no-enable-prefix-caching`. Without it, vLLM's own prefix cache serves repeated prompts straight from GPU memory and the LMCache/Maru path is never exercised (this matters for `--single` mode and repeated runs).
- All processes are launched with `PYTHONHASHSEED=123` so KV chunk hashes stay consistent across processes — a mismatch makes inst2 miss every lookup.
- Each MP server gets an explicit `--http-port` / `--prometheus-port` since the defaults (8080/9090) would collide in `separate` mode.
- Each MP server registers with a stable `--maru-instance-id` (`mp1` / `mp2`) for ownership tracking in MaruServer stats.

---

## Port Configuration

All ports derive from `LMCACHE_PORT_BASE` (default: `12000 + UID`) in [env.sh](env.sh):

| Service | Env var | Default |
|---|---|---|
| vLLM inst1 | `LMCACHE_INST1_PORT` | BASE + 30 |
| vLLM inst2 | `LMCACHE_INST2_PORT` | BASE + 31 |
| MP server 1 | `LMCACHE_MP1_PORT` | BASE + 40 |
| MP server 2 | `LMCACHE_MP2_PORT` | BASE + 41 |
| MaruServer | `MARU_SERVER_PORT` | 10000 + UID |

If you hit port conflicts, change `LMCACHE_PORT_BASE` in [env.sh](env.sh).
