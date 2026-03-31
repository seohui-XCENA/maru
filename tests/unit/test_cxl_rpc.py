# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 XCENA Inc.
"""Tests for CXL-RPC transport layer."""

import mmap
import struct
import threading
import time

import pytest

from maru_common.cxl_rpc import (
    CHANNEL_SIZE,
    IDLE,
    PAYLOAD_MAX,
    REQ_READY,
    RESP_READY,
    Channel,
    ChannelHeader,
    CxlRpcClientTransport,
    CxlRpcServerTransport,
)

# Enough for 8 channels
REGION_SIZE = CHANNEL_SIZE * 8


@pytest.fixture
def shared_mm():
    """Anonymous shared mmap for testing (DRAM-backed)."""
    mm = mmap.mmap(-1, REGION_SIZE, mmap.MAP_SHARED | mmap.MAP_ANONYMOUS)
    yield mm
    mm.close()


# =============================================================================
# Channel low-level tests
# =============================================================================


class TestChannel:
    def test_request_roundtrip(self, shared_mm):
        """Write request via ntstore, read via clflush+load."""
        ch = Channel(shared_mm, 0)

        payload = b"hello from client"
        ch.req_payload.write(payload)
        ch.req_ctl.write(REQ_READY, 1, len(payload))

        status, seq, plen = ch.req_ctl.read()
        assert status == REQ_READY
        assert seq == 1
        assert plen == len(payload)
        assert ch.req_payload.read(plen) == payload

    def test_response_roundtrip(self, shared_mm):
        """Write response via ntstore, read via clflush+load."""
        ch = Channel(shared_mm, 0)

        payload = b"hello from server"
        ch.resp_payload.write(payload)
        ch.resp_ctl.write(RESP_READY, 5, len(payload))

        status, seq, plen = ch.resp_ctl.read()
        assert status == RESP_READY
        assert seq == 5
        assert ch.resp_payload.read(plen) == payload

    def test_idle_status(self, shared_mm):
        """Fresh channel has IDLE status."""
        ch = Channel(shared_mm, 0)
        assert ch.req_ctl.read_status() == IDLE
        assert ch.resp_ctl.read_status() == IDLE

    def test_multiple_channels_independent(self, shared_mm):
        """Different channels at different offsets don't interfere."""
        ch0 = Channel(shared_mm, 0)
        ch1 = Channel(shared_mm, CHANNEL_SIZE)

        ch0.req_payload.write(b"channel zero")
        ch0.req_ctl.write(REQ_READY, 1, 12)

        ch1.req_payload.write(b"channel one!")
        ch1.req_ctl.write(REQ_READY, 2, 12)

        _, seq0, plen0 = ch0.req_ctl.read()
        _, seq1, plen1 = ch1.req_ctl.read()

        assert seq0 == 1
        assert seq1 == 2
        assert ch0.req_payload.read(plen0) == b"channel zero"
        assert ch1.req_payload.read(plen1) == b"channel one!"

    def test_large_payload(self, shared_mm):
        """Payload up to PAYLOAD_MAX bytes."""
        ch = Channel(shared_mm, 0)
        data = bytes(range(256)) * 16  # 4096 bytes = PAYLOAD_MAX
        ch.req_payload.write(data)
        ch.req_ctl.write(REQ_READY, 1, len(data))

        _, _, plen = ch.req_ctl.read()
        assert ch.req_payload.read(plen) == data

    def test_payload_too_large_raises(self, shared_mm):
        """Payload exceeding PAYLOAD_MAX raises ValueError."""
        ch = Channel(shared_mm, 0)
        with pytest.raises(ValueError, match="Payload too large"):
            ch.req_payload.write(b"X" * (PAYLOAD_MAX + 1))


class TestChannelHeader:
    def test_heartbeat_write_read(self, shared_mm):
        """Heartbeat timestamp can be written and read."""
        hdr = ChannelHeader(shared_mm, 0)
        before = time.monotonic_ns()
        hdr.update_heartbeat()
        after = time.monotonic_ns()

        ts = hdr.read_heartbeat()
        assert before <= ts <= after

    def test_client_id(self, shared_mm):
        """Client ID can be written and read."""
        hdr = ChannelHeader(shared_mm, 0)
        hdr.write_client_id("metaserver-inst-a")

        _, _, _, cid = hdr.read()
        assert cid == "metaserver-inst-a"


# =============================================================================
# Client + Server integration tests
# =============================================================================


class TestClientServerRoundtrip:
    """Test CxlRpcClientTransport + CxlRpcServerTransport together."""

    def _echo_handler(self, channel_id: int, seq: int, request: bytes) -> bytes:
        """Simple echo handler: returns request as response."""
        return request

    def _upper_handler(self, channel_id: int, seq: int, request: bytes) -> bytes:
        """Returns uppercased request."""
        return request.upper()

    def test_single_roundtrip(self, shared_mm):
        """Client sends request, server echoes back."""
        region_offset = 0

        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)

        client = CxlRpcClientTransport(shared_mm, region_offset + 0 * CHANNEL_SIZE)

        # Run server in background thread (1 iteration then stop)
        def server_one_shot():
            result = server.poll_once()
            if result:
                ch_id, seq, req = result
                resp = self._echo_handler(ch_id, seq, req)
                server.send_response(ch_id, seq, resp)

        request = b"hello server"

        # Client writes request
        ch = client._channel
        client._seq += 1
        seq = client._seq
        ch.header.update_heartbeat()
        ch.req_payload.write(request)
        ch.req_ctl.write(REQ_READY, seq, len(request))

        # Server processes
        server_one_shot()

        # Client reads response
        status, resp_seq, resp_len = ch.resp_ctl.read()
        assert status == RESP_READY
        assert resp_seq == seq
        assert ch.resp_payload.read(resp_len) == request

    def test_send_request_blocking(self, shared_mm):
        """Client.send_request() blocks until server responds."""
        region_offset = 0

        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)

        client = CxlRpcClientTransport(shared_mm, region_offset + 0 * CHANNEL_SIZE)

        # Server thread: poll and respond
        def server_thread():
            for _ in range(3):  # handle 3 requests
                while True:
                    result = server.poll_once()
                    if result:
                        ch_id, seq, req = result
                        server.send_response(ch_id, seq, req.upper())
                        break
                    time.sleep(0.0001)

        t = threading.Thread(target=server_thread)
        t.start()

        # Client sends 3 requests
        assert client.send_request(b"hello") == b"HELLO"
        assert client.send_request(b"world") == b"WORLD"
        assert client.send_request(b"test!") == b"TEST!"

        t.join(timeout=5)
        assert not t.is_alive()

    def test_concurrent_channels(self, shared_mm):
        """Multiple clients on different channels, served concurrently."""
        region_offset = 0

        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)
        server.activate_channel(1)
        server.activate_channel(2)

        clients = [
            CxlRpcClientTransport(shared_mm, region_offset + i * CHANNEL_SIZE)
            for i in range(3)
        ]

        results = [None, None, None]

        def server_thread():
            handled = 0
            while handled < 3:
                result = server.poll_once()
                if result:
                    ch_id, seq, req = result
                    # Include channel_id in response so we can verify routing
                    resp = req + f"_ch{ch_id}".encode()
                    server.send_response(ch_id, seq, resp)
                    handled += 1
                time.sleep(0.0001)

        def client_thread(idx):
            results[idx] = clients[idx].send_request(f"msg{idx}".encode())

        st = threading.Thread(target=server_thread)
        st.start()

        ct = [threading.Thread(target=client_thread, args=(i,)) for i in range(3)]
        for t in ct:
            t.start()
        for t in ct:
            t.join(timeout=5)
        st.join(timeout=5)

        assert results[0] == b"msg0_ch0"
        assert results[1] == b"msg1_ch1"
        assert results[2] == b"msg2_ch2"

    def test_timeout(self, shared_mm):
        """Client times out if server doesn't respond."""
        client = CxlRpcClientTransport(shared_mm, 0)

        with pytest.raises(TimeoutError, match="CXL-RPC timeout"):
            client.send_request(b"no one listening", timeout_ms=100)

    def test_seq_num_increments(self, shared_mm):
        """Each request gets an incrementing seq_num."""
        region_offset = 0
        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)
        client = CxlRpcClientTransport(shared_mm, region_offset)

        seqs_seen = []

        def server_thread():
            for _ in range(3):
                while True:
                    result = server.poll_once()
                    if result:
                        ch_id, seq, req = result
                        seqs_seen.append(seq)
                        server.send_response(ch_id, seq, b"ok")
                        break
                    time.sleep(0.0001)

        t = threading.Thread(target=server_thread)
        t.start()

        client.send_request(b"a")
        client.send_request(b"b")
        client.send_request(b"c")

        t.join(timeout=5)

        assert seqs_seen == [1, 2, 3]

    def test_run_loop(self, shared_mm):
        """Server run_loop processes requests until stopped."""
        region_offset = 0
        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)
        client = CxlRpcClientTransport(shared_mm, region_offset)

        def handler(ch_id, seq, req):
            return b"resp:" + req

        st = threading.Thread(target=server.run_loop, args=(handler,))
        st.start()

        assert client.send_request(b"one") == b"resp:one"
        assert client.send_request(b"two") == b"resp:two"

        server.stop()
        st.join(timeout=5)
        assert not st.is_alive()

    def test_large_payload_roundtrip(self, shared_mm):
        """Roundtrip with large payload (close to PAYLOAD_MAX)."""
        region_offset = 0
        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)
        client = CxlRpcClientTransport(shared_mm, region_offset)

        large_data = bytes(range(256)) * 15  # 3840 bytes

        def handler(ch_id, seq, req):
            return req  # echo

        st = threading.Thread(target=server.run_loop, args=(handler,))
        st.start()

        result = client.send_request(large_data)
        assert result == large_data

        server.stop()
        st.join(timeout=5)

    def test_heartbeat_updated_on_request(self, shared_mm):
        """Heartbeat is updated with each send_request call."""
        region_offset = 0
        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        server.activate_channel(0)
        client = CxlRpcClientTransport(shared_mm, region_offset)

        def handler(ch_id, seq, req):
            return b"ok"

        st = threading.Thread(target=server.run_loop, args=(handler,))
        st.start()

        before = time.monotonic_ns()
        client.send_request(b"ping")
        after = time.monotonic_ns()

        ch = server.get_channel(0)
        hb = ch.header.read_heartbeat()
        assert before <= hb <= after

        server.stop()
        st.join(timeout=5)

    def test_inactive_channel_not_polled(self, shared_mm):
        """Server skips inactive channels."""
        region_offset = 0
        server = CxlRpcServerTransport(shared_mm, region_offset, max_channels=4)
        # Only activate channel 1, not 0
        server.activate_channel(1)

        # Write request to channel 0 (inactive)
        ch0 = Channel(shared_mm, region_offset)
        ch0.req_payload.write(b"should be ignored")
        ch0.req_ctl.write(REQ_READY, 1, 17)

        # poll_once should not see it
        assert server.poll_once() is None

        # Write to channel 1 (active)
        ch1 = Channel(shared_mm, region_offset + CHANNEL_SIZE)
        ch1.req_payload.write(b"active channel")
        ch1.req_ctl.write(REQ_READY, 1, 14)

        result = server.poll_once()
        assert result is not None
        ch_id, seq, payload = result
        assert ch_id == 1
        assert payload == b"active channel"
