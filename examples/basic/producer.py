#!/usr/bin/env python3
"""Producer: allocate CXL memory, write data, register metadata.

Run this in Terminal 1, then run consumer.py in Terminal 2.

Usage:
    python examples/basic/producer.py
"""

import logging

from maru import MaruConfig, MaruHandler

logger = logging.getLogger(__name__)

CHUNK_SIZE = 1024 * 1024  # 1MB — matches default chunk_size_bytes


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Producer] %(message)s",
    )

    config = MaruConfig(
        server_url="tcp://localhost:5555",
        instance_id="producer",
        pool_size=1024 * 1024 * 100,  # 100MB
    )

    with MaruHandler(config) as handler:
        logger.info("Connected")

        # Store 3 keys
        for i, key in enumerate([1001, 1002, 1003]):
            data = bytes([ord("A") + i]) * CHUNK_SIZE

            # 1. Allocate page in CXL shared memory
            handle = handler.alloc(size=len(data))

            # 2. Write directly to CXL (mmap)
            handle.buf[:] = data

            # 3. Register metadata only
            handler.store(key=key, handle=handle)

            logger.info(
                "Stored key=%d (%d bytes, char=%r) in CXL shared memory",
                key,
                len(data),
                chr(data[0]),
            )

        logger.info("All keys stored. Run consumer.py in another terminal.")
        input("Press Enter to exit (keeps connection alive until then)...")


if __name__ == "__main__":
    main()
