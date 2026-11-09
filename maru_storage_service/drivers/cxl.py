# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CXL driver: RM regions and their DAX mapping. Not implemented yet.

Its TierDriver opens a region MaruServer granted, or one adopted after a
restart, as a MAPPED DriverPool whose descriptor carries the DaxMapping and no
fd; it creates none, since regions come from MaruServer. close() unmaps it.
"""
