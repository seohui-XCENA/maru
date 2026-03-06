#!/usr/bin/env python3
"""Consumer: retrieve KV data from CXL shared memory (zero copy).

Run producer.py first in another terminal, then run this.

Usage:
    python examples/basic/consumer.py
"""

import logging

from maru import MaruConfig, MaruHandler

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [Consumer] %(message)s",
    )

    config = MaruConfig(
        server_url="tcp://localhost:5555",
        instance_id="consumer",
        pool_size=1024 * 1024 * 100,  # 100MB
    )

    with MaruHandler(config) as handler:
        logger.info("Connected")

        for key in [1001, 1002, 1003]:
            result = handler.retrieve(key=key)

            if result is None:
                logger.warning("key=%d not found (is producer running?)", key)
                continue

            # result.view points directly into Producer's CXL region (mapped read-only).
            # No data was copied — consumer reads the same physical memory.
            logger.info(
                "Retrieved key=%d — %d bytes, first char=%r (zero copy from CXL)",
                key,
                len(result.view),
                chr(result.view[0]),
            )

        logger.info("Done — all reads were zero-copy from producer's CXL region")


if __name__ == "__main__":
    main()
