# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Local pin: same-node reads without MaruServer (design 5.2 b).

Off in v1: LOCAL_PIN is answered with DISABLED. The block step before every
removal is part of the contract from the start and is a no-op while local
pins are off. The issuing side is stage S9.
"""
