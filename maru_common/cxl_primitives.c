/* SPDX-License-Identifier: Apache-2.0
 * Copyright 2026 XCENA Inc.
 *
 * CXL memory primitives: ntstore (non-temporal store) and clflush.
 * These are x86 instructions that cannot be called directly from Python.
 *
 * ntstore: bypasses CPU cache, writes directly to CXL memory.
 *          Other nodes can see the data without explicit flush.
 * clflush: invalidates a cache line, forcing next load to fetch from CXL memory.
 *          Used by readers to avoid stale cached data.
 *
 * Build: gcc -O2 -msse2 -shared -fPIC -o libcxl_primitives.so cxl_primitives.c
 */

#include <emmintrin.h> /* _mm_stream_si128, _mm_sfence */
#include <mmintrin.h>  /* _mm_clflush (via xmmintrin) */
#include <xmmintrin.h> /* _mm_clflush */
#include <stdint.h>
#include <string.h>

#define CACHELINE_SIZE 64

/**
 * Non-temporal store: write `len` bytes from `src` to `dst`,
 * bypassing the CPU cache hierarchy.
 *
 * - 16-byte aligned chunks use _mm_stream_si128 (movntdq).
 * - Remaining bytes use movnti (4-byte) or regular store + clflush.
 * - Issues _mm_sfence() at the end to ensure all stores are globally visible.
 *
 * dst must point to a region mapped from a DAX device (e.g., /dev/dax0.0).
 */
void ntstore(void *dst, const void *src, size_t len)
{
    uint8_t *d = (uint8_t *)dst;
    const uint8_t *s = (const uint8_t *)src;
    size_t i = 0;

    /* Handle leading unaligned bytes with regular store + clflush */
    size_t head = (16 - ((uintptr_t)d & 15)) & 15;
    if (head > len)
        head = len;
    if (head > 0) {
        memcpy(d, s, head);
        /* Flush the cache lines touched by the unaligned head */
        _mm_clflush(d);
        if (head > CACHELINE_SIZE)
            _mm_clflush(d + CACHELINE_SIZE);
        i = head;
    }

    /* 16-byte aligned non-temporal stores (movntdq) */
    for (; i + 16 <= len; i += 16) {
        __m128i v;
        memcpy(&v, s + i, 16);
        _mm_stream_si128((__m128i *)(d + i), v);
    }

    /* Handle trailing bytes with movnti (4-byte) */
    for (; i + 4 <= len; i += 4) {
        uint32_t val;
        memcpy(&val, s + i, 4);
        _mm_stream_si32((int *)(d + i), (int)val);
    }

    /* Remaining 1-3 bytes: regular store + clflush */
    if (i < len) {
        memcpy(d + i, s + i, len - i);
        _mm_clflush(d + i);
    }

    /* Ensure all non-temporal stores are globally visible */
    _mm_sfence();
}

/**
 * Flush cache lines covering [addr, addr+len).
 * After clflush, the next load will fetch fresh data from CXL memory
 * instead of returning a stale cached copy.
 *
 * Uses CLFLUSH (not CLFLUSHOPT) for strongest visibility guarantee:
 * CLFLUSH is synchronous — the cache line is evicted before the
 * instruction completes. CLFLUSHOPT is asynchronous and may leave
 * stale data visible to other nodes.
 */
void clflush(const void *addr, size_t len)
{
    const uintptr_t start = (uintptr_t)addr & ~(CACHELINE_SIZE - 1);
    const uintptr_t end = (uintptr_t)addr + len;

    for (uintptr_t p = start; p < end; p += CACHELINE_SIZE) {
        _mm_clflush((const void *)p);
    }

    /* Ensure all flushes complete before subsequent loads */
    _mm_mfence();
}
