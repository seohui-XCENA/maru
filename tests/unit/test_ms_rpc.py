# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Multi-storage requests over real ZMQ round trips, on both RPC servers."""

import socket
import struct
import threading
import time

import msgpack
import pytest
import zmq

from maru_common.protocol import (
    HEADER_FORMAT,
    PROTOCOL_MAGIC,
    PROTOCOL_VERSION,
    MessageType,
    StorageRequest,
)
from maru_common.storage_types import Status
from maru_server.rpc_async_server import RpcAsyncServer
from maru_server.rpc_server import RpcServer
from maru_server.server import MaruServer
from tests.unit.ms_fakes import FakeNode, FakeWorker, RpcWire, admin_delete, key


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(params=["async", "sync"])
def served(request):
    server = MaruServer(enable_cxl=False)
    port = free_port()
    if request.param == "async":
        rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    else:
        rpc = RpcServer(server, host="127.0.0.1", port=port)
    thread = threading.Thread(target=rpc.start, daemon=True)
    thread.start()
    time.sleep(0.2)
    wire = RpcWire(f"tcp://127.0.0.1:{port}", server.ms_service)
    yield server, wire
    wire.close()
    rpc.stop()
    thread.join(5)
    server.close()


def raw_frame(code: int, payload, seq: int = 77) -> bytes:
    body = msgpack.packb(payload, use_bin_type=True)
    header = struct.pack(
        HEADER_FORMAT,
        PROTOCOL_MAGIC,
        PROTOCOL_VERSION,
        code,
        0,
        0,
        0,
        0,
        seq,
        len(body),
    )
    return header + body


def test_store_lookup_unpin_round_trip(served):
    server, wire = served
    node = FakeNode(wire, "n1")
    assert node.open().status == Status.OK
    k = key("k")
    loc = node.loc(node.dram(), 1)
    assert node.commit((k, loc)).items[0].result == "ADDED"
    worker = FakeWorker(wire, node)
    assert worker.open().status == Status.OK
    hit = worker.lookup(k).items[0]
    assert hit.result == "HIT" and hit.location == loc
    assert worker.unpin(hit.pin_id).items[0].result == "OK"
    assert node.retire((k, loc)).items[0].result == "RETIRED"
    # the first multi-storage request started the maintenance thread
    assert server.ms_service._thread is not None


@pytest.mark.parametrize(
    "code, payload",
    [
        # wrong field types
        (
            MessageType.COMMIT,
            {"session": 1, "server_epoch": "e", "op": {}, "items": []},
        ),
        # bool where an int belongs
        (
            MessageType.UNPIN,
            {"session": "s", "server_epoch": "e", "pin_ids": [True]},
        ),
        # unknown enum value
        (MessageType.NODE_CLOSE, {"session": "s", "server_epoch": "e", "mode": "nap"}),
        # missing required field
        (MessageType.LOOKUP_AND_PIN, {"session": "s", "server_epoch": "e"}),
        # not a map
        (MessageType.WORKER_OPEN, ["not", "a", "map"]),
    ],
    ids=["types", "bool", "enum", "missing", "not-a-map"],
)
def test_malformed_request_is_rejected_with_its_header(served, code, payload):
    server, wire = served
    header, response = wire.exchange(raw_frame(code, payload, seq=4321))
    assert header.msg_type == code | 0x80
    assert header.sequence == 4321
    assert response.status == Status.REJECTED and response.error
    # the server keeps serving
    assert FakeNode(wire, "n1").open().status == Status.OK


def test_resume_and_reregister_round_trip(served):
    server, wire = served
    server.ms_service.recovery_ends = time.monotonic() + 30  # in the window
    node = FakeNode(wire, "n1")
    opened = node.open(resume=5)
    assert opened.status == Status.OK and opened.epoch_seq == 5
    assert opened.recovering  # the default window follows the start
    k = key("k")
    response = node.reregister((k, node.loc(node.dram(), 0)), last=True)
    assert response.status == Status.OK
    assert [i.result.value for i in response.items] == ["ADDED"]
    worker = FakeWorker(wire, node)
    assert worker.open().recovering
    lookup = worker.lookup(k, key("absent"))
    assert [i.result.value for i in lookup.items] == ["HIT", "RECOVERING"]


def test_legacy_requests_keep_their_path(served):
    server, wire = served
    header, response = wire.exchange(
        wire.serializer.encode(MessageType.STORAGE, StorageRequest(action="usage"))
    )
    assert header.msg_type == MessageType.STORAGE | 0x80
    assert response.success is True
    # a broken legacy payload is still answered the old way
    wire.socket.send(raw_frame(MessageType.STORAGE, ["x"]))
    reply = wire.serializer.decode(wire.socket.recv())[1]
    assert "error" in reply


def test_a_failing_handler_answers_rejected(monkeypatch):
    from maru_common.serializer import Serializer
    from maru_server.rpc_handler_mixin import RpcHandlerMixin

    server = MaruServer(enable_cxl=False)

    class Dispatch(RpcHandlerMixin):
        _server = server

    def boom(request, payload):
        raise RuntimeError("boom")

    dispatch = Dispatch()
    dispatch._get_ms_handlers()[MessageType.PROBE] = boom
    serializer = Serializer()
    frame = raw_frame(
        MessageType.PROBE, {"session": "s", "server_epoch": "e", "items": []}, seq=9
    )
    header, response = serializer.decode_response(
        dispatch._process_ms_frame(serializer, frame)
    )
    assert header.msg_type == MessageType.PROBE | 0x80 and header.sequence == 9
    assert response.status == Status.REJECTED and "boom" in response.error
    server.close()


def test_admin_delete_and_its_cli_over_zmq(served, capsys):
    from maru_server import ms_admin

    server, wire = served
    server.ms_service.recovery_ends = 0.0  # past the window: absent is known
    node = FakeNode(wire, "n1")
    node.open()
    loc = node.loc(node.dram(), 1)
    node.commit((key("k"), loc))
    address = wire.socket.getsockopt_string(zmq.LAST_ENDPOINT)
    assert (
        ms_admin.main(["delete", "--server", address, "--namespace", "ns", "k", "gone"])
        == 0
    )
    out = capsys.readouterr().out.splitlines()
    assert out == ["k\tOK", "gone\tMISS (absent)"]
    (block,) = node.heartbeat().blocks
    assert [(i.key, i.location) for i in block.items] == [(key("k"), loc)]
    # idempotent: the same request again changes nothing
    response = admin_delete(wire, key("k"))
    assert [i.result for i in response.items] == ["OK"]
    assert len(node.heartbeat().blocks) == 1


def test_admin_cli_reports_an_unreachable_server(capsys):
    from maru_server import ms_admin

    port = free_port()
    code = ms_admin.main(
        [
            "delete",
            "--server",
            f"tcp://127.0.0.1:{port}",
            "--namespace",
            "ns",
            "--timeout-ms",
            "200",
            "k",
        ]
    )
    assert code == 1 and "delete failed" in capsys.readouterr().err


def test_stop_while_multi_storage_requests_are_in_flight():
    server = MaruServer(enable_cxl=False)
    port = free_port()
    rpc = RpcAsyncServer(server, host="127.0.0.1", port=port, num_workers=2)
    serving = threading.Thread(target=rpc.start, daemon=True)
    serving.start()
    time.sleep(0.2)
    address = f"tcp://127.0.0.1:{port}"
    setup = RpcWire(address, server.ms_service)
    node = FakeNode(setup, "n1")
    node.open()
    keys = [key(f"k{i}") for i in range(64)]
    node.commit(*[(k, node.loc(node.dram(), i)) for i, k in enumerate(keys)])
    setup.close()
    stop = threading.Event()
    answered = []

    def client():
        wire = RpcWire(address, server.ms_service, timeout_ms=1000)
        worker = FakeWorker(wire, node)
        try:
            worker.open()
            while not stop.is_set():
                worker.lookup(*keys)
                answered.append(1)
        except zmq.ZMQError:
            pass  # the server went away mid-request
        finally:
            wire.close()

    clients = [threading.Thread(target=client, daemon=True) for _ in range(4)]
    for t in clients:
        t.start()
    while len(answered) < 20:
        time.sleep(0.01)
    started = time.monotonic()
    rpc.stop()
    serving.join(5)
    stop.set()
    for t in clients:
        t.join(5)
    server.close()
    assert time.monotonic() - started < 5
    assert not serving.is_alive()
    assert not any(t.is_alive() for t in clients)


class _ProxyHarness:
    """Drives _proxy_routine with mock sockets and a scripted poller."""

    def __init__(self, queue_max=2):
        from unittest.mock import MagicMock

        from maru_common.serializer import Serializer

        self.server = MaruServer(enable_cxl=False)
        self.rpc = RpcAsyncServer(
            self.server, port=5556, num_workers=1, lane_queue_max=queue_max
        )
        self.rpc._frontend = MagicMock()
        self.rpc._backend = MagicMock()
        self.rpc._ms_backend = MagicMock()
        self.serializer = Serializer()
        self.inbox = []

    def frame(self, code, request, seq):
        return [b"id", b"", self.serializer.encode(code, request, seq=seq)]

    def run(self, rounds):
        """rounds: list of (incoming frame or None, {socket: events})."""
        from unittest.mock import MagicMock, patch

        frames = [f for f, _ in rounds if f is not None]
        self.rpc._frontend.recv_multipart.side_effect = frames
        script = iter(rounds)

        def poll(_timeout):
            try:
                frame, events = next(script)
            except StopIteration:
                self.rpc._running = False
                return []
            ready = dict(events)
            if frame is not None:
                ready[self.rpc._frontend] = zmq.POLLIN
            return list(ready.items())

        poller = MagicMock()
        poller.poll.side_effect = poll
        self.rpc._running = True
        with patch.object(zmq, "Poller", return_value=poller):
            self.rpc._proxy_routine()

    def replies(self):
        return [
            self.serializer.decode_response(call.args[0][2])
            for call in self.rpc._frontend.send_multipart.call_args_list
        ]

    def close(self):
        self.server.close()


def _probe(seq):
    from maru_common.protocol import ProbeRequest

    return MessageType.PROBE, ProbeRequest(session="s", server_epoch="e", items=[]), seq


def _legacy(seq):
    return MessageType.STORAGE, StorageRequest(action="usage"), seq


def test_a_busy_lane_queues_then_answers_busy_on_overflow():
    h = _ProxyHarness(queue_max=2)
    h.rpc._ms_backend.send_multipart.side_effect = zmq.Again()
    h.run([(h.frame(*_probe(seq)), {}) for seq in (1, 2, 3)])
    # two frames wait in the queue; the third overflows and gets BUSY
    ((header, reply),) = h.replies()
    assert header.sequence == 3 and reply.status == Status.BUSY
    assert h.rpc._ms_backend.send_multipart.call_count == 1  # then queued
    h.close()


def test_a_queued_lane_flushes_in_order_when_writable():
    h = _ProxyHarness(queue_max=10)
    sent = []
    attempts = [0]

    def send(msg):
        attempts[0] += 1
        if attempts[0] == 1:
            raise zmq.Again()
        sent.append(msg)

    h.rpc._backend.send_multipart.side_effect = send
    first, second = h.frame(*_legacy(1)), h.frame(*_legacy(2))
    h.run([(first, {}), (second, {}), (None, {h.rpc._backend: zmq.POLLOUT})])
    assert sent == [first, second]  # queued, not refused, and in order
    assert h.replies() == []
    h.close()


def test_a_full_legacy_lane_does_not_hold_back_multi_storage():
    h = _ProxyHarness(queue_max=10)
    h.rpc._backend.send_multipart.side_effect = zmq.Again()
    probe = h.frame(*_probe(2))
    h.run([(h.frame(*_legacy(1)), {}), (probe, {})])
    h.rpc._ms_backend.send_multipart.assert_called_once_with(probe)
    h.close()


def test_a_request_sent_before_workers_connect_is_served():
    class SlowWorkers(RpcAsyncServer):
        def _worker_routine(self, worker_id, endpoint=None):
            time.sleep(0.5)
            if endpoint is None:
                super()._worker_routine(worker_id)
            else:
                super()._worker_routine(worker_id, endpoint)

    server = MaruServer(enable_cxl=False)
    port = free_port()
    rpc = SlowWorkers(server, host="127.0.0.1", port=port, num_workers=2)
    serving = threading.Thread(target=rpc.start, daemon=True)
    serving.start()
    wire = RpcWire(f"tcp://127.0.0.1:{port}", server.ms_service)
    try:
        header, response = wire.exchange(
            wire.serializer.encode(MessageType.STORAGE, StorageRequest(action="usage"))
        )
        assert response.success is True
        assert FakeNode(wire, "n1").open().status == Status.OK
    finally:
        wire.close()
        rpc.stop()
        serving.join(5)
        server.close()


def test_stop_leaves_a_running_proxys_sockets_to_it():
    from unittest.mock import MagicMock

    server = MaruServer(enable_cxl=False)
    rpc = RpcAsyncServer(server, port=5556, num_workers=1)
    rpc._owns_sockets = True  # as after start()
    frontend, context = MagicMock(), MagicMock()
    rpc._frontend, rpc._context = frontend, context
    rpc._proxy_thread = MagicMock()
    rpc._proxy_thread.is_alive.return_value = True  # join timed out
    rpc.stop()
    frontend.close.assert_not_called()
    context.term.assert_not_called()
    server.close()


@pytest.mark.filterwarnings("ignore::pytest.PytestUnhandledThreadExceptionWarning")
def test_start_gives_up_when_workers_never_connect():
    class DeadWorkers(RpcAsyncServer):
        def _worker_routine(self, worker_id, endpoint=None):
            raise RuntimeError("worker died before connecting")

    server = MaruServer(enable_cxl=False)
    rpc = DeadWorkers(server, port=free_port(), num_workers=1, connect_timeout_s=0.5)
    started = time.monotonic()
    with pytest.raises(RuntimeError, match="did not connect"):
        rpc.start()
    assert time.monotonic() - started < 5
    assert rpc._context is None and rpc._frontend is None and not rpc._running
    server.close()
