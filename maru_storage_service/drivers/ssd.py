# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""SSD driver: file extents, O_DIRECT alignment and short I/O. Not implemented
yet.

Its TierDriver creates the tier's file for a new run and opens one adopted
from the run before, each as an IO_ONLY DriverPool (no descriptor, no fd).
close() closes the file.
"""
