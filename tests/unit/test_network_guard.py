"""The autouse network guard in tests/conftest.py.

A test without the network marker must not reach a remote host. Python clients
go through ``socket.connect``, which the guard replaces. Rust and C clients
(obstore, GDAL) do not, so the guard also points the proxy variables at a dead
port on the loopback interface.
"""

from __future__ import annotations

import contextlib
import os
import socket
import time
from datetime import timedelta
from typing import TYPE_CHECKING

import pytest

from tests.network_guard import NetworkBlockedError

if TYPE_CHECKING:
    from tests.network_guard import NetworkGuard

pytestmark = pytest.mark.unit

# TEST-NET-1 (RFC 5737). No host answers on it, so a broken guard waits for the
# socket timeout and does not reach a real server.
_REMOTE = ("192.0.2.1", 80)


def test_a_remote_connect_fails_at_once_and_names_the_test(network_guard: NetworkGuard) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    start = time.monotonic()
    with sock, pytest.raises(NetworkBlockedError) as excinfo:
        sock.connect(_REMOTE)

    assert time.monotonic() - start < 0.5
    assert "test_a_remote_connect_fails_at_once_and_names_the_test" in str(excinfo.value)
    assert "192.0.2.1:80" in str(excinfo.value)
    # The test expected the block. Clear it, or teardown fails the test.
    assert network_guard.attempts == ["192.0.2.1:80"]
    network_guard.attempts.clear()


def test_a_remote_hostname_is_blocked(network_guard: NetworkGuard) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with sock, pytest.raises(NetworkBlockedError):
        sock.connect_ex(("s3.amazonaws.com", 443))

    assert network_guard.attempts == ["s3.amazonaws.com:443"]
    network_guard.attempts.clear()


def test_a_swallowed_block_is_still_recorded(network_guard: NetworkGuard) -> None:
    """Code under test may catch the error. Teardown still sees the attempt."""
    with contextlib.suppress(Exception):
        socket.create_connection(_REMOTE, timeout=2)

    assert network_guard.attempts == ["192.0.2.1:80"]
    network_guard.attempts.clear()


@pytest.mark.parametrize("host", ["127.0.0.1", "localhost"])
def test_a_loopback_connect_goes_through(host: str, network_guard: NetworkGuard) -> None:
    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
        with socket.create_connection((host, port), timeout=2):
            pass

    assert network_guard.attempts == []


def test_the_proxy_variables_point_at_the_sentinel(network_guard: NetworkGuard) -> None:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        assert os.environ[key] == network_guard.sentinel.url, key
    assert os.environ["NO_PROXY"] == "localhost,127.0.0.1,::1"


def test_a_python_client_that_reads_the_proxy_variables_is_recorded(
    network_guard: NetworkGuard,
) -> None:
    import urllib.request

    with pytest.raises(OSError, match="403"):
        urllib.request.urlopen("https://example.com/", timeout=2)  # noqa: S310

    assert network_guard.collect() == ["example.com:443"]
    network_guard.attempts.clear()


def test_obstore_is_refused_and_recorded(network_guard: NetworkGuard) -> None:
    """obstore is Rust. Only the sentinel proxy sees it, not socket.connect."""
    import obstore as obs
    from obstore.store import S3Store

    store = S3Store(
        "portolan-network-guard-probe",
        region="us-east-1",
        skip_signature=True,
        retry_config={
            "max_retries": 0,
            "retry_timeout": timedelta(seconds=1),
            "backoff": {
                "init_backoff": timedelta(milliseconds=10),
                "max_backoff": timedelta(milliseconds=10),
                "base": 2,
            },
        },
    )
    start = time.monotonic()
    with pytest.raises(Exception, match="error sending request"):
        obs.delete(store, "key")

    # Unguarded, the call reaches the real S3 API and fails with AccessDenied.
    assert time.monotonic() - start < 10
    assert network_guard.collect() == ["s3.us-east-1.amazonaws.com:443"]
    network_guard.attempts.clear()


@pytest.mark.network
def test_a_network_test_is_not_guarded() -> None:
    # socket.socket inherits connect from _socket.socket. The guard sets its own.
    assert "connect" not in vars(socket.socket)
    assert not os.environ.get("HTTPS_PROXY", "").startswith("http://127.0.0.1:")


@pytest.mark.refused_hosts("example.com")
def test_a_refused_host_does_not_fail_the_test(network_guard: NetworkGuard) -> None:
    """The sentinel still refuses the request. Teardown ignores it."""
    import urllib.request

    with pytest.raises(OSError, match="403"):
        urllib.request.urlopen("https://example.com/", timeout=2)  # noqa: S310

    assert network_guard.unexpected(["example.com"]) == []
    assert network_guard.unexpected([]) == ["example.com:443"]


def test_the_host_of_a_target() -> None:
    from tests.network_guard import _host

    assert _host("example.com:443") == "example.com"
    assert _host("http://169.254.169.254/latest/api/token") == "169.254.169.254"


def test_a_remote_datagram_is_blocked(network_guard: NetworkGuard) -> None:
    """A UDP socket sends without connect, so sendto needs its own check."""
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        with pytest.raises(NetworkBlockedError):
            sock.sendto(b"x", _REMOTE)
        with pytest.raises(NetworkBlockedError):
            sock.sendto(b"x", 0, _REMOTE)

    assert network_guard.attempts == ["192.0.2.1:80", "192.0.2.1:80"]
    network_guard.attempts.clear()


def test_a_loopback_datagram_goes_through(network_guard: NetworkGuard) -> None:
    with (
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server,
        socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as client,
    ):
        server.bind(("127.0.0.1", 0))
        server.settimeout(2)
        client.sendto(b"ping", server.getsockname())
        assert server.recv(16) == b"ping"

    assert network_guard.attempts == []
