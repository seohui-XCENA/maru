#!/usr/bin/env python3
"""Cross-instance sharing in a single script (threaded).

Demonstrates the core Maru value: two independent handlers sharing
KV data through CXL shared memory with zero data copy.

  Producer: alloc → write → store (CXL write + metadata register)
  Consumer: retrieve → read (CXL read, zero copy)

Usage:
    python examples/basic/cross_instance.py
"""

import logging
import threading
import time

from maru import MaruConfig, MaruHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1MB — matches default chunk_size_bytes
SERVER_URL = "tcp://localhost:5555"
POOL_SIZE = 1024 * 1024 * 100  # 100MB
KEYS = [1001, 1002, 1003]


def run_producer(ready_event: threading.Event):
    """Allocate CXL memory, write data, register metadata."""
    config = MaruConfig(
        server_url=SERVER_URL,
        instance_id="producer",
        pool_size=POOL_SIZE,
    )

    with MaruHandler(config) as handler:
        logger.info("[Producer] Connected")

        for i, key in enumerate(KEYS):
            data = bytes([ord("A") + i]) * CHUNK_SIZE

            handle = handler.alloc(size=len(data))
            handle.buf[:] = data
            handler.store(key=key, handle=handle)

            logger.info("[Producer] Stored key=%d (%r)", key, chr(data[0]))

        logger.info("[Producer] All keys stored — signaling consumer")
        ready_event.set()

        # Keep connection alive until consumer finishes
        time.sleep(2)


def run_consumer(ready_event: threading.Event):
    """Retrieve KV data — zero copy from producer's CXL region."""
    config = MaruConfig(
        server_url=SERVER_URL,
        instance_id="consumer",
        pool_size=POOL_SIZE,
    )

    # Wait for producer to finish storing
    ready_event.wait()

    with MaruHandler(config) as handler:
        logger.info("[Consumer] Connected")

        for key in KEYS:
            result = handler.retrieve(key=key)
            assert result is not None, f"key={key} not found"

            # result.view → producer's CXL region (mapped read-only, zero copy)
            logger.info(
                "[Consumer] Retrieved key=%d — %d bytes, char=%r (zero copy)",
                key,
                len(result.view),
                chr(result.view[0]),
            )

        logger.info("[Consumer] Done — all reads were zero-copy from CXL")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(message)s",
    )

    ready = threading.Event()

    producer = threading.Thread(target=run_producer, args=(ready,))
    consumer = threading.Thread(target=run_consumer, args=(ready,))

    producer.start()
    consumer.start()

    producer.join()
    consumer.join()

    logger.info("Metadata traveled, data didn't.")


if __name__ == "__main__":
    main()
