"""Replay recorded HTTP responses on the loopback interface.

The extractors reach a service through three clients: ``httpx`` (ArcGIS),
``requests`` (WFS and CARTO discovery), and geoparquet-io, which makes its own
requests for WFS and CARTO data. A patch on one client cannot reach the
others. A server on the loopback interface reaches all of them, and
``tests/network_guard.py`` lets loopback connections through.

A scenario is a directory under ``tests/fixtures/network/<kind>/<scenario>/``.
It holds ``routes.json``, the body files, and a README that names the issue and
the source of the recording. ``routes.json`` is a list of routes. The server
answers a request with the first route that matches it:

- ``method`` and ``path``: the HTTP method, and an ``fnmatch`` pattern on the
  URL path.
- ``query_subset``: parameters that the request must carry with these values.
  Names match without case, as OGC servers read them.
- ``requires_query``: parameter names that the request must carry with any
  value. A gated service is two routes on one path: the first requires the
  credential, and the second answers a request without it.
- ``times``: the route answers this many requests and then stops matching. A
  transient error is a route with ``times: 1`` before the good route.
- ``status``, ``content_type``, ``body_file``: the response. The server
  replaces ``{{base_url}}`` in a text body with its own address, so that a
  capabilities document points the client back at the replay.
- ``pages_from``: a GeoParquet file under ``tests/fixtures``. The server
  answers with the ArcGIS GeoJSON page that ``resultOffset`` and
  ``resultRecordCount`` select. A recording of 500 features then needs no
  copy of them in JSON.

The server answers a request that no route matches with 404 and appends it to
``unmatched``. Every test asserts ``server.unmatched == []``. That proves the
recording covers each request, including the requests of geoparquet-io.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from functools import cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import parse_qs, urlparse

if TYPE_CHECKING:
    from types import TracebackType

FIXTURES = Path(__file__).resolve().parent / "fixtures"
_TEXT_TYPES = ("json", "xml", "text", "html")


@dataclass(frozen=True)
class Request:
    """One request that the replay server received."""

    method: str
    path: str
    query: dict[str, list[str]]
    status: int


@dataclass
class Route:
    """One recorded response and the requests that it answers."""

    method: str
    path: str
    status: int = 200
    content_type: str = "application/json"
    body_file: str | None = None
    query_subset: dict[str, str] = field(default_factory=dict)
    requires_query: list[str] = field(default_factory=list)
    times: int | None = None
    pages_from: str | None = None

    def matches(self, method: str, path: str, query: dict[str, list[str]]) -> bool:
        """Return True when this route answers the request."""
        if self.times is not None and self.times <= 0:
            return False
        if method != self.method or not fnmatchcase(path, self.path):
            return False
        names = {name.lower(): values for name, values in query.items()}
        for name, value in self.query_subset.items():
            if value not in names.get(name.lower(), []):
                return False
        return all(name.lower() in names for name in self.requires_query)


def load_routes(fixture_dir: Path) -> list[Route]:
    """Read and check the routes of one scenario."""
    raw = json.loads((fixture_dir / "routes.json").read_text(encoding="utf-8"))
    routes = [Route(**entry) for entry in raw]
    for route in routes:
        if route.body_file is not None and not (fixture_dir / route.body_file).is_file():
            raise FileNotFoundError(fixture_dir / route.body_file)
        if route.pages_from is not None and not (FIXTURES / route.pages_from).is_file():
            raise FileNotFoundError(FIXTURES / route.pages_from)
    return routes


@cache
def _features(source: Path) -> list[dict[str, Any]]:
    """Convert a GeoParquet file to ArcGIS GeoJSON features, once per file."""
    import pyarrow.parquet as pq
    from shapely.geometry import mapping
    from shapely.wkb import loads as load_wkb

    features: list[dict[str, Any]] = []
    for row in pq.read_table(source).to_pylist():
        geometry_wkb = row.pop("geometry")
        row.pop("bbox", None)
        geometry = None if geometry_wkb is None else mapping(load_wkb(geometry_wkb))
        features.append(
            {"type": "Feature", "id": row["objectid"], "properties": row, "geometry": geometry}
        )
    return features


def _page(source: Path, query: dict[str, list[str]]) -> bytes:
    features = _features(source)
    offset = int(query.get("resultOffset", ["0"])[0])
    count = int(query.get("resultRecordCount", [str(len(features))])[0])
    page = {"type": "FeatureCollection", "features": features[offset : offset + count]}
    return json.dumps(page, separators=(",", ":")).encode()


class _Server(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, replay: ReplayServer) -> None:
        super().__init__(("127.0.0.1", 0), _Handler)
        self.replay = replay


class _Handler(BaseHTTPRequestHandler):
    server: _Server

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - BaseHTTPRequestHandler API
        """Keep stderr quiet. ``ReplayServer.requests`` is the log."""

    def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        """Answer a GET request."""
        self._answer("GET")

    def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
        """Answer a POST request. A form body counts as query parameters."""
        self._answer("POST")

    def _answer(self, method: str) -> None:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        query = parse_qs(parsed.query, keep_blank_values=True)
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else b""
        if "form-urlencoded" in (self.headers.get("Content-Type") or ""):
            for name, values in parse_qs(body.decode(), keep_blank_values=True).items():
                query.setdefault(name, []).extend(values)

        status, content_type, payload = self.server.replay.respond(method, path, query)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


class ReplayServer:
    """Serve one recorded scenario on the loopback interface.

    Use it as a context manager. ``with ReplayServer(path) as base_url:``
    yields the base URL, such as ``http://127.0.0.1:43123``.
    """

    def __init__(self, fixture_dir: Path) -> None:
        self.fixture_dir = fixture_dir
        self.routes = load_routes(fixture_dir)
        self.requests: list[Request] = []
        self.unmatched: list[Request] = []
        self._lock = threading.Lock()
        self._server: _Server | None = None
        self._thread: threading.Thread | None = None

    @property
    def url(self) -> str:
        """Return the base URL of the running server."""
        if self._server is None:
            raise RuntimeError("ReplayServer is not running")
        return f"http://127.0.0.1:{self._server.server_address[1]}"

    def respond(
        self, method: str, path: str, query: dict[str, list[str]]
    ) -> tuple[int, str, bytes]:
        """Return the status, the content type, and the body for one request."""
        with self._lock:
            route = next((r for r in self.routes if r.matches(method, path, query)), None)
            if route is not None and route.times is not None:
                route.times -= 1
            status = 404 if route is None else route.status
            request = Request(method=method, path=path, query=query, status=status)
            self.requests.append(request)
            if route is None:
                self.unmatched.append(request)
        if route is None:
            body = {"error": {"code": 404, "message": f"No recorded route for {method} {path}"}}
            return 404, "application/json", json.dumps(body).encode()
        return route.status, route.content_type, self._body(route, query)

    def _body(self, route: Route, query: dict[str, list[str]]) -> bytes:
        if route.pages_from is not None:
            return _page(FIXTURES / route.pages_from, query)
        if route.body_file is None:
            return b""
        payload = (self.fixture_dir / route.body_file).read_bytes()
        if any(kind in route.content_type for kind in _TEXT_TYPES):
            payload = payload.replace(b"{{base_url}}", self.url.encode())
        return payload

    def __enter__(self) -> str:
        """Start the server in a daemon thread and return its base URL."""
        self._server = _Server(self)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self.url

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Stop the server and wait for its thread."""
        self.stop()

    def stop(self) -> None:
        """Stop the server. A second call does nothing."""
        if self._server is None or self._thread is None:
            return
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)
        if self._thread.is_alive():
            raise RuntimeError("ReplayServer did not stop")
        self._thread = None
