# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""CopyTracker: an owner is released once after its copy ended, or kept
until the process exits."""

import gc
import threading
import weakref

from maru_vllm import copy_tracker
from maru_vllm.copy_tracker import CopyTracker


class Owner:
    """Something a copy uses: a page, a pin, a staging buffer."""


def test_an_owner_goes_once_and_only_after_its_copy_ended():
    tracker = CopyTracker()
    record = tracker.register(Owner(), "r1")
    tracker.seal(record, "event")
    assert not tracker.confirm(record, False)  # still copying: not an error
    assert tracker.confirm(record, True)
    assert record.owner is None and record.proof is None
    assert not tracker.confirm(record, True)  # released once
    assert not tracker.keep(record, RuntimeError("late"))
    assert not tracker.kept


def test_a_kept_owner_is_never_released():
    tracker = CopyTracker()
    owner = Owner()
    record = tracker.register(owner)
    error = RuntimeError("CUDA error")
    assert tracker.keep(record, error)
    assert not tracker.keep(record, error)  # kept and counted once
    assert not tracker.confirm(record, True)  # a later wait that succeeds
    assert tracker.kept == [record] and record.error is error
    assert sum(r is record for r in copy_tracker._PROCESS_KEPT) == 1
    assert record.owner is owner


def test_a_kept_owner_outlives_its_tracker():
    tracker = CopyTracker()
    owner = Owner()
    alive = weakref.ref(owner)
    tracker.keep(tracker.register(owner))
    del owner, tracker
    gc.collect()
    assert alive() is not None


def test_a_wait_and_a_keep_racing_settle_a_record_once():
    for _ in range(50):
        tracker = CopyTracker()
        record = tracker.register(Owner())
        start = threading.Barrier(2)
        results = {}

        def confirm(record=record, start=start, results=results, tracker=tracker):
            start.wait()
            results["confirm"] = tracker.confirm(record, True)

        def keep(record=record, start=start, results=results, tracker=tracker):
            start.wait()
            results["keep"] = tracker.keep(record)

        threads = [threading.Thread(target=f) for f in (confirm, keep)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        assert sorted(results.values()) == [False, True]
        assert (record in tracker.kept) == results["keep"]
