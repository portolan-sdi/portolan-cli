"""Stop a test without the network marker from reaching a remote host.

tests/conftest.py applies the guard to each test through an autouse fixture.
The guard has two layers, because Portolan reaches the network through two
kinds of client:

- Python clients call ``socket.connect``, or ``socket.sendto`` for UDP. The
  guard replaces both. A remote address raises ``NetworkBlockedError`` at
  once, with the test and the host.
- Rust and C clients (obstore, GDAL through libcurl) open their own sockets.
  The guard points the proxy variables at a sentinel proxy on the loopback
  interface. The sentinel records the host that each request asks for and
  refuses it. The client retries by its own policy and then fails. obstore
  retries 10 times, for about 2 s a call. Python clients that read the proxy
  variables (botocore, urllib, requests) also reach the sentinel.

The guard records each attempt. The fixture fails the test at teardown when a
record exists, because the code under test can catch the error. Before the
guard, an unmocked obstore delete in a unit test waited 110 s on the real S3
API and the test passed.

A connection to the loopback interface goes through, so moto servers and the
Docker e2e services keep working.

A test can mark ``@pytest.mark.refused_hosts("host", ...)``. The guard still
refuses those hosts, but the test does not fail for them. Use it only for a
request from a child process that the test cannot patch, and that the test
does not depend on.
"""

from __future__ import annotations

import ipaddress
import socket
import sys
import threading
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    import pytest

_LOOPBACK_NAMES = frozenset({"localhost", "localhost.localdomain"})
PROXY_KEYS = ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy")
NO_PROXY_KEYS = ("NO_PROXY", "no_proxy")
NO_PROXY = "localhost,127.0.0.1,::1"
_REFUSAL = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"


class NetworkBlockedError(RuntimeError):
    """A test without the network marker tried to connect to a remote host."""


class SentinelProxy:
    """A proxy on the loopback interface that records each request and refuses it."""

    def __init__(self) -> None:
        self._server = socket.create_server(("127.0.0.1", 0))
        self.address: tuple[str, int] = self._server.getsockname()[:2]
        self.url = f"http://{self.address[0]}:{self.address[1]}"
        self.targets: list[str] = []
        self._thread = threading.Thread(target=self._serve, name="sentinel-proxy", daemon=True)

    def _serve(self) -> None:
        while True:
            try:
                client, _ = self._server.accept()
            except OSError:
                return  # the session closed the server
            with client:
                client.settimeout(1)
                try:
                    request_line = client.recv(4096).split(b"\r\n", 1)[0]
                    self.targets.append(_target(request_line))
                    client.sendall(_REFUSAL)
                except OSError:
                    self.targets.append("an unknown host (the client sent no request)")

    def run(self) -> Iterator[SentinelProxy]:
        """Serve for the session, then close the server."""
        self._thread.start()
        try:
            yield self
        finally:
            self._server.close()


def _target(request_line: bytes) -> str:
    """Return the host of ``CONNECT host:443 HTTP/1.1`` or the URL of a plain request."""
    parts = request_line.decode("latin-1").split()
    return parts[1] if len(parts) >= 2 else repr(request_line)


def _host(target: str) -> str:
    """Return the host of ``host:port`` or of a URL."""
    if "://" in target:
        return urlsplit(target).hostname or target
    return target.rsplit(":", 1)[0]


def _is_loopback(host: str) -> bool:
    if host.lower() in _LOOPBACK_NAMES:
        return True
    try:
        address = ipaddress.ip_address(host.split("%", 1)[0])
    except ValueError:
        return False
    return address.is_loopback or address.is_unspecified


@dataclass
class NetworkGuard:
    """The guard state for one test."""

    test: str
    sentinel: SentinelProxy
    attempts: list[str] = field(default_factory=list)
    _seen: int = 0

    def check(self, address: Any) -> None:
        """Raise ``NetworkBlockedError`` when ``address`` is a remote host."""
        if not isinstance(address, tuple):
            return  # an AF_UNIX path
        host, port = str(address[0]), address[1]
        if _is_loopback(host):
            return  # the sentinel records a proxied request with its real host
        target = f"{host}:{port}"
        self.attempts.append(target)
        raise NetworkBlockedError(
            f"{self.test} tried to connect to {target}. Mock the call, or mark the "
            "test with @pytest.mark.network if it needs the network."
        )

    def collect(self) -> list[str]:
        """Add the sentinel requests since the last call to ``attempts``."""
        new = self.sentinel.targets[self._seen :]
        self._seen += len(new)
        self.attempts.extend(new)
        return self.attempts

    def unexpected(self, refused_hosts: Iterable[str]) -> list[str]:
        """Return the attempts whose host is not in ``refused_hosts``."""
        refused = set(refused_hosts)
        return [target for target in self.collect() if _host(target) not in refused]

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Replace ``socket.connect`` and point the proxy variables at the sentinel."""
        self._seen = len(self.sentinel.targets)
        connect = socket.socket.connect
        connect_ex = socket.socket.connect_ex
        sendto = socket.socket.sendto

        def guarded_connect(sock: socket.socket, address: Any) -> None:
            self.check(address)
            connect(sock, address)

        def guarded_connect_ex(sock: socket.socket, address: Any) -> int:
            self.check(address)
            return connect_ex(sock, address)

        # A UDP socket sends without connect. sendto takes (data, address) or
        # (data, flags, address), so the address is the last argument.
        def guarded_sendto(sock: socket.socket, *args: Any) -> int:
            self.check(args[-1])
            return sendto(sock, *args)

        monkeypatch.setattr(socket.socket, "connect", guarded_connect)
        monkeypatch.setattr(socket.socket, "connect_ex", guarded_connect_ex)
        monkeypatch.setattr(socket.socket, "sendto", guarded_sendto)
        for key in PROXY_KEYS:
            monkeypatch.setenv(key, self.sentinel.url)
        for key in NO_PROXY_KEYS:
            monkeypatch.setenv(key, NO_PROXY)


# geoparquet-io installs DuckDB extensions on first use. The network guard
# refuses that download, so a fresh CI runner could load none of them. Install
# them once, before the guard starts. An installed extension costs no download.
# Each entry is (name, repository). None is the core repository. geoparquet-io
# loads h3, geography (s2), and a5 from the community repository.
_DUCKDB_EXTENSIONS: tuple[tuple[str, str | None], ...] = (
    ("spatial", None),
    ("httpfs", None),
    ("encodings", None),
    # The Iceberg backend attaches a REST catalog through this one.
    ("iceberg", None),
    ("h3", "community"),
    ("geography", "community"),
    ("a5", "community"),
)


def install_duckdb_extensions() -> None:
    """Install the DuckDB extensions that geoparquet-io loads, before the guard starts.

    pytest calls this from tests/conftest.py. The perf scripts run outside
    pytest, so ``tests.perf.bench`` and ``tests.perf.gates`` call it too.
    """
    import duckdb

    connection = duckdb.connect()
    try:
        for name, repository in _DUCKDB_EXTENSIONS:
            try:
                connection.install_extension(name, repository=repository)
            except duckdb.Error as exc:
                # Offline, or no build for this platform. A run that needs the
                # extension then fails and names extensions.duckdb.org.
                print(f"Could not install the DuckDB {name} extension: {exc}", file=sys.stderr)
    finally:
        connection.close()
