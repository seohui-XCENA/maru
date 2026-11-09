# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""PoolManager states, budgets and the bytes invariant (spec 6.2), including a
stateful property test over every transition."""

import dataclasses

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    Bundle,
    RuleBasedStateMachine,
    consumes,
    invariant,
    multiple,
    rule,
)

from maru_common.node_protocol import Budgets
from maru_common.storage_types import (
    GENERATION_COUNTER_MAX,
    Key,
    Location,
    PoolInfo,
    PoolKind,
    generation_epoch_seq,
    make_generation,
)
from maru_storage_service.pool_manager import (
    RECOVERY_TAG,
    InvalidTransitionError,
    PageRef,
    PageState,
    PoolManager,
    holder_tag,
)

PS = 4096
PAGES = 16


def budgets(**overrides) -> Budgets:
    values = {
        "credit_max_bytes": 4 * PS,
        "credit_total_max_bytes": 8 * PS,
        "credit_refill_low": PS,
        "credit_refill_high": 4 * PS,
        "commit_pending_max_bytes": 3 * PS,
        "transfer_reserve_bytes": {"dram": 2 * PS},
    }
    values.update(overrides)
    return Budgets(**values)


def dram(pool_id="dram:n:e", pages=PAGES) -> PoolInfo:
    return PoolInfo(
        pool_id=pool_id,
        kind=PoolKind.DRAM,
        node_id="n",
        pool_generation=1,
        capacity=pages * PS,
        page_size=PS,
    )


def manager(epoch_seq=3, **overrides) -> PoolManager:
    pm = PoolManager(budgets(**overrides), PS)
    pm.add_pool(dram())
    pm.set_epoch_seq(epoch_seq)
    return pm


def ref_of(credit: Location) -> PageRef:
    return PageRef(credit.pool_id, credit.offset // PS, credit.generation)


class TestCredit:
    def test_no_credit_before_epoch_seq(self):
        pm = PoolManager(budgets(), PS)
        pm.add_pool(dram())
        assert pm.credit("w", PS) == []
        pm.set_epoch_seq(1)
        assert len(pm.credit("w", PS)) == 1

    def test_credit_is_whole_pages_with_new_generations(self):
        pm = manager()
        credits = pm.credit("w", PS + 1)
        assert len(credits) == 2
        assert all(c.length == PS and c.offset % PS == 0 for c in credits)
        assert all(generation_epoch_seq(c.generation) == 3 for c in credits)
        assert credits[0].generation != credits[1].generation
        assert credits[0].allocation_id != credits[1].allocation_id
        assert pm.held_bytes("w") == 2 * PS
        pm.check()

    def test_per_worker_budget(self):
        pm = manager()
        assert len(pm.credit("w", 100 * PS)) == 4  # credit_max_bytes
        assert pm.credit("w", PS) == []
        assert len(pm.credit("other", PS)) == 1

    def test_node_budget(self):
        pm = manager()
        for worker in ("a", "b"):
            assert len(pm.credit(worker, 4 * PS)) == 4
        assert pm.credit("c", PS) == []  # credit_total_max_bytes is 8 pages

    def test_transfer_reserve_is_not_credit(self):
        pm = manager(credit_max_bytes=16 * PS, credit_total_max_bytes=16 * PS)
        assert len(pm.credit("w", 16 * PS)) == PAGES - 2
        # the reserve stays for transfers
        reserved = pm.reserve_transfer("dram:n:e", 5)
        assert len(reserved) == 2
        pm.check()

    def test_pending_cap_stops_credit(self):
        pm = manager()
        credits = pm.credit("w", 4 * PS)
        for i, c in enumerate(credits[:3]):
            pm.write_done("w", c, Key("ns", f"k{i}"), 10)
        assert pm.bytes_in(PageState.PENDING) == 3 * PS  # the cap
        assert pm.credit("x", PS) == []
        ref = pm.find_allocation(
            credits[0].pool_id, credits[0].allocation_id, credits[0].generation
        )
        pm.commit_added(ref)
        assert len(pm.credit("x", PS)) == 1

    def test_stopped_credit(self):
        pm = manager()
        pm.stop_credit("safe state")
        assert pm.credit("w", PS) == []
        assert pm.credit_stopped == "safe state"

    def test_generation_counter_exhaustion_stops_credit(self):
        pm = manager()
        pm._counter = GENERATION_COUNTER_MAX
        assert len(pm.credit("w", 4 * PS)) == 1  # the last generation
        assert pm.credit("w", PS) == []
        assert "restart" in pm.credit_stopped

    def test_epoch_seq_is_set_once(self):
        pm = manager(epoch_seq=3)
        pm.set_epoch_seq(3)
        with pytest.raises(InvalidTransitionError):
            pm.set_epoch_seq(4)


class TestWorkerTransitions:
    def test_write_done_moves_to_pending_with_logical_length(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        ref, location, why = pm.write_done("w", credit, Key("ns", "k"), 100)
        assert why == ""
        assert location.length == 100 and location.offset == credit.offset
        assert pm.page(ref).state == PageState.PENDING
        assert pm.held_bytes("w") == 0
        pm.check()

    @pytest.mark.parametrize(
        "mutate, reason",
        [
            (
                lambda c: c.__class__(**{**c.__dict__, "pool_id": "dram:x:y"}),
                "not a pool",
            ),
            (
                lambda c: c.__class__(
                    **{**c.__dict__, "generation": make_generation(2, 0)}
                ),
                "another StorageService run",
            ),
            (
                lambda c: c.__class__(
                    **{**c.__dict__, "generation": c.generation + 99}
                ),
                "another generation",
            ),
            (
                lambda c: c.__class__(**{**c.__dict__, "allocation_id": "x"}),
                "allocation id",
            ),
            (lambda c: c.__class__(**{**c.__dict__, "offset": c.offset + 1}), "offset"),
            (lambda c: c.__class__(**{**c.__dict__, "length": 1}), "length"),
            (lambda c: c.__class__(**{**c.__dict__, "kind": PoolKind.CXL}), "kind"),
        ],
    )
    def test_write_done_refuses_credit_that_does_not_match(self, mutate, reason):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        ref, _, why = pm.write_done("w", mutate(credit), Key("ns", "k"), 10)
        assert ref is None and reason in why
        assert pm.bytes_in(PageState.CREDITED) == PS  # nothing changed
        pm.check()

    def test_write_done_refuses_another_holder_and_bad_sizes(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert "another worker" in pm.write_done("x", credit, Key("ns", "k"), 10)[2]
        assert pm.write_done("w", credit, Key("ns", "k"), 0)[0] is None
        assert pm.write_done("w", credit, Key("ns", "k"), PS + 1)[0] is None
        assert pm.write_done("w", credit, Key("ns", "k"), PS)[0] is not None
        # the same credit twice: the page is PENDING now
        assert "PENDING" in pm.write_done("w", credit, Key("ns", "k"), 10)[2]

    def test_abort_frees_only_the_holders_page(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert pm.abort("x", credit) == (
            False,
            "the page is credited to another worker",
        )
        assert pm.abort("w", credit) == (True, "")
        assert pm.bytes_in(PageState.FREE) == PAGES * PS
        assert pm.abort("w", credit)[0] is False
        pm.check()

    def test_credit_return_is_idempotent(self):
        pm = manager()
        (credit,) = pm.credit("w", PS)
        assert pm.credit_return("w", credit) == (True, "")
        assert pm.credit_return("w", credit) == (True, "already returned")
        # the page moved on to another worker: the old credit stays a no-op
        while True:
            (again,) = pm.credit("v", PS)
            if again.offset == credit.offset:
                break
            pm.credit_return("v", again)
        assert pm.credit_return("w", credit) == (True, "already returned")
        assert pm.page(ref_of(again)).holder == "v"
        # a pending page is not returned
        pm.write_done("v", again, Key("ns", "k"), 1)
        assert pm.credit_return("v", again)[0] is False
        pm.check()


class TestServiceTransitions:
    def test_commit_results(self):
        pm = manager()
        a, b = pm.credit("w", 2 * PS)
        ra, la, _ = pm.write_done("w", a, Key("ns", "a"), 10)
        rb, _, _ = pm.write_done("w", b, Key("ns", "b"), 10)
        assert pm.commit_added(ra) == la
        pm.commit_freed(rb)
        assert pm.page(ra).state == PageState.READY
        assert pm.page(rb).state == PageState.FREE
        # never freed twice
        with pytest.raises(InvalidTransitionError):
            pm.commit_freed(rb)
        with pytest.raises(InvalidTransitionError):
            pm.commit_added(rb)
        pm.check()

    def test_retire_then_reclaim(self):
        pm = manager()
        (c,) = pm.credit("w", PS)
        ref, location, _ = pm.write_done("w", c, Key("ns", "a"), 10)
        pm.commit_added(ref)
        assert pm.find(location) == ref
        pm.retired(ref)
        assert pm.page(ref).state == PageState.RETIRING
        pm.reclaim(ref)
        assert pm.page(ref).state == PageState.FREE
        assert pm.find(location) is None
        with pytest.raises(InvalidTransitionError):
            pm.reclaim(ref)
        pm.check()

    def test_quarantine_and_release_by_holder(self):
        pm = manager()
        pm.credit("w", 3 * PS)
        pm.credit("v", PS)
        refs = pm.quarantine_holder("w")
        assert len(refs) == 3 and pm.held_bytes("w") == 0
        assert pm.bytes_in(PageState.QUARANTINED) == 3 * PS
        assert pm.release_quarantine(holder_tag("v")) == 0
        assert pm.release_quarantine("w") == 0  # tags are not bare incarnations
        assert pm.release_quarantine(holder_tag("w")) == 3
        assert pm.bytes_in(PageState.CREDITED) == PS
        pm.check()

    def test_transfer_path(self):
        pm = manager()
        (ref,) = pm.reserve_transfer("dram:n:e", 1)
        assert pm.page(ref).state == PageState.TRANSFER
        pm.transfer_done(ref, Key("ns", "t"), 5)
        assert pm.page(ref).location.length == 5
        pm.commit_added(ref)
        (other,) = pm.reserve_transfer("dram:n:e", 1)
        pm.transfer_failed(other)
        pm.check()

    def test_adopted_pool_ledger(self):
        pm = manager()
        info = PoolInfo(
            pool_id="ssd:n:/x",
            kind=PoolKind.SSD,
            node_id="n",
            pool_generation=1,
            capacity=4 * PS,
            page_size=PS,
            path="/x",
        )
        pm.add_pool(info, creditable=False, state=PageState.QUARANTINED)
        loc = Location(
            "ssd:n:/x", "a1", make_generation(2, 7), 2 * PS, 100, PoolKind.SSD
        )
        (ref,) = pm.recovered("ssd:n:/x", [(Key("ns", "r"), loc)])
        assert pm.page(ref).state == PageState.READY
        assert pm.find(loc) == ref
        assert pm.bytes_in(PageState.QUARANTINED, "ssd:n:/x") == 3 * PS
        assert pm.release_quarantine(RECOVERY_TAG, "ssd:n:/x") == 3
        bad = Location("ssd:n:/x", "b", 1, -PS, 1, PoolKind.SSD)
        pm2 = PoolManager(budgets(), PS)
        pm2.add_pool(info, creditable=False, state=PageState.QUARANTINED)
        with pytest.raises(InvalidTransitionError):
            pm2.recovered("ssd:n:/x", [(Key("ns", "x"), bad)])
        # credit never comes from a pool this stage cannot map
        pm2 = PoolManager(budgets(), PS)
        pm2.add_pool(info, creditable=False, state=PageState.QUARANTINED)
        pm2.set_epoch_seq(1)
        assert pm2.credit("w", PS) == []
        pm.check()

    def _ready(self, pm, key="a"):
        (c,) = pm.credit("w", PS)
        ref, location, _ = pm.write_done("w", c, Key("ns", key), 10)
        pm.commit_added(ref)
        return ref, location

    def test_reconcile_takes_the_occupant(self):
        pm = manager()
        ref, location = self._ready(pm)
        occupant = Location(
            location.pool_id,
            "theirs",
            location.generation + 5,
            location.offset,
            20,
            PoolKind.DRAM,
        )
        new = pm.reconcile(ref, Key("ns", "other"), occupant)
        page = pm.page(new)
        assert (page.state, page.key, page.location) == (
            PageState.READY,
            Key("ns", "other"),
            occupant,
        )
        assert (
            pm.find_allocation(occupant.pool_id, "theirs", occupant.generation) == new
        )
        assert pm.find(location) is None  # the old allocation is gone
        pm.check()

    @pytest.mark.parametrize(
        "change",
        [
            {"pool_id": "dram:x:y"},
            {"offset": PS},
            {"kind": PoolKind.CXL},
            {"length": PS + 1},
            {"length": 0},
        ],
        ids=["pool", "offset", "kind", "too-long", "empty"],
    )
    def test_reconcile_refuses_what_does_not_fit(self, change):
        pm = manager()
        ref, location = self._ready(pm)
        occupant = Location(
            **{**location.__dict__, "allocation_id": "theirs", **change}
        )
        with pytest.raises(InvalidTransitionError):
            pm.reconcile(ref, Key("ns", "other"), occupant)
        assert pm.page(ref).location == location  # unchanged
        pm.check()

    def test_reconcile_refuses_an_allocation_of_another_page(self):
        pm = manager()
        ref, location = self._ready(pm, "a")
        _, elsewhere = self._ready(pm, "b")
        occupant = dataclasses.replace(location, allocation_id=elsewhere.allocation_id)
        with pytest.raises(InvalidTransitionError, match="already"):
            pm.reconcile(ref, Key("ns", "b"), occupant)
        pm.check()

    def test_usage_reports_each_state(self):
        pm = manager()
        a, b, c = pm.credit("w", 3 * PS)
        ra, _, _ = pm.write_done("w", a, Key("ns", "a"), 1)
        pm.commit_added(ra)
        pm.write_done("w", b, Key("ns", "b"), 1)
        (usage,) = pm.usage()
        assert (usage.ready, usage.pending, usage.credited) == (PS, PS, PS)
        assert usage.free == (PAGES - 3) * PS
        assert (
            usage.free
            + usage.credited
            + usage.transfer
            + usage.pending
            + usage.ready
            + usage.retiring
            + usage.quarantined
            == usage.capacity
        )


class LedgerMachine(RuleBasedStateMachine):
    """Every transition in any order the service could ask for."""

    credits = Bundle("credits")
    pending = Bundle("pending")
    ready = Bundle("ready")
    retiring = Bundle("retiring")
    transfers = Bundle("transfers")

    def __init__(self):
        super().__init__()
        self.pm = PoolManager(budgets(), PS)
        self.pm.add_pool(dram())
        self.pm.set_epoch_seq(5)
        self.keys = 0
        self.history = []  # every credit ever given, for stale replays

    @rule(
        target=credits,
        worker=st.sampled_from(["a", "b", "c"]),
        want=st.integers(min_value=1, max_value=6 * PS),
    )
    def credit(self, worker, want):
        pending_before = self.pm.bytes_in(PageState.PENDING)
        held = self.pm.held_bytes(worker)
        got = self.pm.credit(worker, want)
        if pending_before >= self.pm.budgets.commit_pending_max_bytes:
            assert got == []
        assert held + len(got) * PS <= self.pm.budgets.credit_max_bytes
        self.history.extend((worker, c) for c in got)
        return multiple(*[(worker, c) for c in got])

    @rule(target=pending, credit=consumes(credits))
    def write_done(self, credit):
        worker, location = credit
        self.keys += 1
        ref, _, _ = self.pm.write_done(worker, location, Key("ns", f"k{self.keys}"), 7)
        # None when the worker ended and its page was quarantined meanwhile
        return multiple(ref) if ref is not None else multiple()

    @rule(credit=consumes(credits))
    def abort(self, credit):
        self.pm.abort(*credit)

    @rule(credit=consumes(credits))
    def credit_return(self, credit):
        worker, location = credit
        page = self.pm.page(ref_of(location))
        # Held, or already back (FREE at that generation, or moved past it)
        expected = (
            page is None
            or page.state == PageState.FREE
            or (page.state == PageState.CREDITED and page.holder == worker)
        )
        assert self.pm.credit_return(worker, location)[0] == expected
        if expected:
            assert self.pm.credit_return(worker, location) == (True, "already returned")

    @rule(target=ready, ref=consumes(pending))
    def added(self, ref):
        self.pm.commit_added(ref)
        return ref

    @rule(ref=consumes(pending))
    def duplicate_or_rejected(self, ref):
        self.pm.commit_freed(ref)

    @rule(target=retiring, ref=consumes(ready))
    def retired(self, ref):
        self.pm.retired(ref)
        return ref

    @rule(ref=consumes(retiring))
    def reclaimed(self, ref):
        self.pm.reclaim(ref)

    @rule(worker=st.sampled_from(["a", "b", "c"]))
    def worker_ends(self, worker):
        self.pm.quarantine_holder(worker)

    @rule(worker=st.sampled_from(["a", "b", "c"]))
    def grace_over(self, worker):
        self.pm.release_quarantine(holder_tag(worker))

    @rule(target=transfers, count=st.integers(min_value=1, max_value=3))
    def reserve(self, count):
        return multiple(*self.pm.reserve_transfer("dram:n:e", count))

    @rule(target=pending, ref=consumes(transfers))
    def transfer_done(self, ref):
        self.keys += 1
        self.pm.transfer_done(ref, Key("ns", f"t{self.keys}"), 9)
        return ref

    @rule(ref=consumes(transfers))
    def transfer_failed(self, ref):
        self.pm.transfer_failed(ref)

    @rule(
        data=st.data(),
        worker=st.sampled_from(["a", "b", "c"]),
        action=st.sampled_from(["write_done", "abort", "credit_return"]),
    )
    def stale_or_reused_credit(self, data, worker, action):
        """Any credit ever given, by any worker, at any later time."""
        if not self.history:
            return
        _, credit = data.draw(st.sampled_from(self.history))
        page = self.pm.page(ref_of(credit))
        held = (
            page is not None
            and page.state == PageState.CREDITED
            and page.holder == worker
        )
        before = self.pm.usage()
        if action == "write_done":
            self.keys += 1
            ok = self.pm.write_done(worker, credit, Key("ns", f"s{self.keys}"), 3)[0]
            assert (ok is not None) == held
        elif action == "abort":
            assert self.pm.abort(worker, credit)[0] == held
        else:
            back = page is None or page.state == PageState.FREE
            assert self.pm.credit_return(worker, credit)[0] == (held or back)
        if not held:
            assert self.pm.usage() == before  # refused: nothing changed

    @rule(
        pool=st.sampled_from(["dram:n:e", "dram:other:x"]),
        offset=st.integers(min_value=-PS, max_value=(PAGES + 1) * PS),
        generation=st.integers(min_value=0, max_value=make_generation(6, 3)),
        worker=st.sampled_from(["a", "b", "c"]),
    )
    def foreign_credit(self, pool, offset, generation, worker):
        credit = Location(pool, "nope", generation, offset, PS, PoolKind.DRAM)
        before = self.pm.usage()
        assert self.pm.write_done(worker, credit, Key("ns", "f"), 1)[0] is None
        assert self.pm.abort(worker, credit)[0] is False
        self.pm.credit_return(worker, credit)
        assert self.pm.usage() == before

    @invariant()
    def bytes_add_up(self):
        (u,) = self.pm.usage()
        # credit + pending + used + quarantine (+ transfer, retiring, free)
        assert (
            u.credited
            + u.pending
            + u.ready
            + u.quarantined
            + u.transfer
            + u.retiring
            + u.free
            == u.capacity
            == PAGES * PS
        )
        assert u.credited <= self.pm.budgets.credit_total_max_bytes
        self.pm.check()


LedgerMachine.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=60,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestLedgerProperty = LedgerMachine.TestCase
