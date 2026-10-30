"""A client for the parts of the Iceberg REST catalog that DuckDB does not cover.

DuckDB writes the data and commits the data snapshots. It has no statement
for the metadata-only operations the backend needs: a table property and a
tag that record a version, a ``main`` that moves back to an older snapshot,
and snapshot expiry. Each one is one ``POST`` to the table with a list of
``requirements`` and ``updates``, as the Iceberg REST specification defines
``commitTable``. This client sends those, reads the table metadata document
(``loadTable``), and manages namespaces for the tests.

Retries follow the rule the extractors use (``retry_with_backoff`` with
``is_transient_status``): a transport error, a timeout, 408, 429, or 5xx is
retried; any other status ends at once. A commit is never retried, because
a 5xx after a commit may mean the commit landed. The caller reloads the
table and decides. A 401 drops the cached token and the request runs once
more. An error message names the method, the path, the status, and the
server's text. It never carries a header.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import httpx

from portolan_cli.api import RetryConfig, is_transient_status, retry_with_backoff

if TYPE_CHECKING:
    from portolan_cli.backends.iceberg.auth import TokenProvider

#: The ``loadTable`` header that asks the catalog for a storage credential.
VENDED_CREDENTIALS_HEADER = "X-Iceberg-Access-Delegation"


class RestCatalogError(RuntimeError):
    """The catalog answered with an error status, or did not answer."""

    def __init__(self, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class NoSuchTableError(RestCatalogError):
    """The table does not exist in the catalog."""


class CommitConflictError(RestCatalogError):
    """A requirement failed. Another writer committed first."""


@dataclass(frozen=True)
class LoadTableResult:
    """The ``loadTable`` response: where the metadata is, and what it says."""

    metadata_location: str
    metadata: dict[str, Any]
    config: dict[str, str]


class _TransientStatusError(Exception):
    """A response with a transient status, raised so the retry helper sees it."""

    def __init__(self, response: httpx.Response) -> None:
        super().__init__(str(response.status_code))
        self.response = response


def _server_text(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text[:200]
    if isinstance(body, dict):
        error = body.get("error")
        if isinstance(error, dict) and isinstance(error.get("message"), str):
            return str(error["message"])[:200]
    return response.text[:200]


class RestCatalog:
    """One REST catalog, reached with one credential.

    Args:
        base_url: The catalog root, such as ``https://catalog.lakehousebox.com``.
        auth: The credential.
        warehouse: The warehouse to name in ``GET /v1/config``, or None.
        http: The HTTP client, replaceable in tests. The default has a 30 s timeout.
        retry: The retry policy for reads. The default is three attempts.
    """

    def __init__(
        self,
        base_url: str,
        *,
        auth: TokenProvider,
        warehouse: str | None = None,
        http: httpx.Client | None = None,
        retry: RetryConfig | None = None,
    ) -> None:
        self._base = base_url.rstrip("/")
        self._auth = auth
        self._warehouse = warehouse
        self._http = http or httpx.Client(timeout=30.0)
        self._retry = retry or RetryConfig(max_attempts=3, initial_delay=1.0)
        self._config: dict[str, Any] | None = None

    # ---------------------------------------------------------------- transport

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None,
        json_body: dict[str, Any] | None,
        headers: dict[str, str] | None,
    ) -> httpx.Response:
        """Send once, with one more try after a 401 that a fresh token may fix."""
        response = self._http.request(
            method,
            self._base + path,
            params=params,
            json=json_body,
            headers={**self._auth.headers(), **(headers or {})},
        )
        if response.status_code == httpx.codes.UNAUTHORIZED:
            invalidate = getattr(self._auth, "invalidate", None)
            if callable(invalidate):
                invalidate()
            response = self._http.request(
                method,
                self._base + path,
                params=params,
                json=json_body,
                headers={**self._auth.headers(), **(headers or {})},
            )
        return response

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        retry_transient: bool = True,
    ) -> httpx.Response:
        def attempt() -> httpx.Response:
            response = self._send(method, path, params=params, json_body=json_body, headers=headers)
            if retry_transient and is_transient_status(response.status_code):
                raise _TransientStatusError(response)
            return response

        result = retry_with_backoff(
            attempt, self._retry, retry_on=(_TransientStatusError, httpx.TransportError)
        )
        if result.success:
            return result.value  # type: ignore[return-value]
        if isinstance(result.error, _TransientStatusError):
            return result.error.response
        raise RestCatalogError(f"{method} {path} failed: {result.error}") from result.error

    @staticmethod
    def _fail(method: str, path: str, response: httpx.Response) -> RestCatalogError:
        return RestCatalogError(
            f"{method} {path} returned {response.status_code}: {_server_text(response)}",
            status=response.status_code,
        )

    # ---------------------------------------------------------------- paths

    def config(self) -> dict[str, Any]:
        """Return ``GET /v1/config``, read once per client."""
        if self._config is None:
            params = {"warehouse": self._warehouse} if self._warehouse else None
            response = self._request("GET", "/v1/config", params=params)
            if response.status_code != httpx.codes.OK:
                raise self._fail("GET", "/v1/config", response)
            body = response.json()
            self._config = body if isinstance(body, dict) else {}
        return self._config

    @property
    def prefix(self) -> str:
        """The path prefix the catalog puts between ``/v1/`` and ``namespaces``."""
        config = self.config()
        for key in ("overrides", "defaults"):
            section = config.get(key)
            if isinstance(section, dict) and section.get("prefix"):
                return str(section["prefix"])
        return ""

    def _path(self, tail: str) -> str:
        prefix = self.prefix
        return f"/v1/{prefix}/{tail}" if prefix else f"/v1/{tail}"

    @staticmethod
    def _encode(part: str) -> str:
        return quote(part, safe="")

    def _namespace_path(self, namespace: str) -> str:
        return self._path(f"namespaces/{self._encode(namespace)}")

    def _table_path(self, namespace: str, name: str) -> str:
        return self._path(f"namespaces/{self._encode(namespace)}/tables/{self._encode(name)}")

    # ---------------------------------------------------------------- namespaces

    def namespace_exists(self, namespace: str) -> bool:
        """Return whether the namespace exists."""
        path = self._namespace_path(namespace)
        response = self._request("HEAD", path)
        if response.status_code in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            return True
        if response.status_code == httpx.codes.NOT_FOUND:
            return False
        raise self._fail("HEAD", path, response)

    def create_namespace(self, namespace: str) -> None:
        """Create the namespace. One that exists already is fine."""
        path = self._path("namespaces")
        response = self._request(
            "POST", path, json_body={"namespace": [namespace], "properties": {}}
        )
        if response.status_code == httpx.codes.CONFLICT:
            return
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise self._fail("POST", path, response)

    def drop_namespace(self, namespace: str) -> None:
        """Drop an empty namespace."""
        path = self._namespace_path(namespace)
        response = self._request("DELETE", path)
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise self._fail("DELETE", path, response)

    # ---------------------------------------------------------------- tables

    def table_exists(self, namespace: str, name: str) -> bool:
        """Return whether the table exists."""
        path = self._table_path(namespace, name)
        response = self._request("HEAD", path)
        if response.status_code in (httpx.codes.OK, httpx.codes.NO_CONTENT):
            return True
        if response.status_code == httpx.codes.NOT_FOUND:
            return False
        raise self._fail("HEAD", path, response)

    def list_tables(self, namespace: str) -> list[str]:
        """Return the table names of the namespace, first page."""
        path = self._path(f"namespaces/{self._encode(namespace)}/tables")
        response = self._request("GET", path)
        if response.status_code != httpx.codes.OK:
            raise self._fail("GET", path, response)
        identifiers = response.json().get("identifiers", [])
        return [str(entry["name"]) for entry in identifiers]

    def load_table(
        self, namespace: str, name: str, *, vended_credentials: bool = False
    ) -> LoadTableResult:
        """Return the table's metadata location, metadata, and config.

        Args:
            namespace: The namespace.
            name: The table name.
            vended_credentials: Ask the catalog for a storage credential
                scoped to the table. The answer, when the catalog vends one,
                is in ``config`` under the ``s3.*`` or ``gcs.*`` keys.

        Raises:
            NoSuchTableError: The table does not exist.
        """
        path = self._table_path(namespace, name)
        headers = {VENDED_CREDENTIALS_HEADER: "vended-credentials"} if vended_credentials else None
        response = self._request("GET", path, headers=headers)
        if response.status_code == httpx.codes.NOT_FOUND:
            raise NoSuchTableError(
                f"Table {namespace}.{name} does not exist in {self._base}", status=404
            )
        if response.status_code != httpx.codes.OK:
            raise self._fail("GET", path, response)
        return self._loaded(response.json())

    def commit_table(
        self,
        namespace: str,
        name: str,
        *,
        requirements: list[dict[str, Any]],
        updates: list[dict[str, Any]],
    ) -> LoadTableResult:
        """Commit metadata updates to the table, guarded by requirements.

        Sent once, never retried. A 5xx after a commit may mean the commit
        landed, so the caller reloads the table before it decides.

        Raises:
            NoSuchTableError: The table does not exist.
            CommitConflictError: A requirement failed (409).
            RestCatalogError: Any other error status.
        """
        path = self._table_path(namespace, name)
        response = self._request(
            "POST",
            path,
            json_body={"requirements": requirements, "updates": updates},
            retry_transient=False,
        )
        if response.status_code == httpx.codes.NOT_FOUND:
            raise NoSuchTableError(
                f"Table {namespace}.{name} does not exist in {self._base}", status=404
            )
        if response.status_code == httpx.codes.CONFLICT:
            raise CommitConflictError(
                f"Another writer committed to {namespace}.{name} first: {_server_text(response)}",
                status=409,
            )
        if response.status_code != httpx.codes.OK:
            raise self._fail("POST", path, response)
        return self._loaded(response.json())

    def drop_table(self, namespace: str, name: str, *, purge: bool = False) -> None:
        """Drop the table. With ``purge``, ask the catalog to delete its files too."""
        path = self._table_path(namespace, name)
        params = {"purgeRequested": "true"} if purge else None
        response = self._request("DELETE", path, params=params)
        if response.status_code >= httpx.codes.BAD_REQUEST:
            raise self._fail("DELETE", path, response)

    @staticmethod
    def _loaded(body: dict[str, Any]) -> LoadTableResult:
        config = body.get("config") or {}
        return LoadTableResult(
            metadata_location=str(body["metadata-location"]),
            metadata=dict(body["metadata"]),
            config={str(key): str(value) for key, value in config.items()},
        )
