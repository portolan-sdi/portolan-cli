# Folder-aware ArcGIS services-root extraction, Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `portolan extract arcgis` against a services root recurse into ArcGIS Server folders by default, accept folder URLs, reach token-secured folders when credentials are given (skip otherwise), map folders to nested subcatalogs, and report coverage.

**Architecture:** All changes are contained to `portolan_cli/extract/arcgis/` (discovery, url_parser, orchestrator, new auth module), one shared model addition in `extract/common/report.py`, CLI flag wiring in `cli.py`, one error class in `errors.py`, plus a new ADR. The CLI stays a thin Click layer (ADR-0007); all logic lives in the extract modules. Network is mocked in unit tests.

**Tech Stack:** Python 3.10+, `httpx` (sync client, already used in discovery), `click`, `pytest`, `mypy --strict`, `ruff`. Design doc: `context/shared/plans/2026-06-09-arcgis-folder-recursion-design.md`. Issue: [#493](https://github.com/portolan-sdi/portolan-cli/issues/493).

**Conventions to follow (from `.claude/rules/python.md`):** `from __future__ import annotations` at top of every module; modern typing (`X | None`, `list[str]`); raise typed errors from `errors.py`; user-facing messages via `portolan_cli.output`, diagnostics via `logging`; `pathlib.Path` only.

**Run tests with:** `uv run pytest <path> -v`. Lint with `uv run ruff check .`, types with `uv run mypy portolan_cli`.

---

## File map

- Create `portolan_cli/extract/arcgis/auth.py` — credentials + token resolution.
- Create `tests/unit/extract/arcgis/test_auth.py`.
- Create `context/shared/adr/0053-arcgis-folder-recursion-and-structure.md`.
- Modify `portolan_cli/errors.py` — add `ArcGISAuthError`.
- Modify `portolan_cli/extract/arcgis/discovery.py` — auth + embedded-error in `_fetch_json`, `FolderTraversal`, `discover_services_recursive`.
- Modify `portolan_cli/extract/arcgis/url_parser.py` — `SERVICES_FOLDER`, `folder` field, folder-URL parsing.
- Modify `portolan_cli/extract/common/report.py` — `FolderCoverage`, `ExtractionReport.folder_coverage`.
- Modify `portolan_cli/extract/arcgis/orchestrator.py` — recursive discovery, folder scoping, nested paths, token threading, coverage on report, `ExtractionOptions` fields.
- Modify `portolan_cli/cli.py` — `--token`/`--username`/`--password`/`--no-recurse`, credential build, `SERVICES_FOLDER` handling, coverage rendering.
- Modify `CLAUDE.md` — register ADR-0053.
- Tests touched: `tests/unit/extract/arcgis/test_discovery.py`, `test_url_parser.py`, `test_orchestrator.py`, `tests/integration/extract/arcgis/test_orchestrator_live.py`.

---

## Task 1: Add `ArcGISAuthError`

**Files:**
- Modify: `portolan_cli/errors.py`
- Test: `tests/unit/test_errors.py` (create if absent, else append)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/test_errors.py
import pytest
from portolan_cli.errors import ArcGISAuthError, PortolanError


@pytest.mark.unit
def test_arcgis_auth_error_code_and_message() -> None:
    err = ArcGISAuthError("token request failed", url="https://x/generateToken")
    assert isinstance(err, PortolanError)
    assert err.code == "PRTLN-EXT002"
    assert "token request failed" in str(err)
    assert err.to_dict()["context"]["url"] == "https://x/generateToken"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/unit/test_errors.py::test_arcgis_auth_error_code_and_message -v`
Expected: FAIL with `ImportError: cannot import name 'ArcGISAuthError'`.

- [ ] **Step 3: Implement**

Find the extract error section. `InvalidArcGISURLError` (code `PRTLN-EXT001`) currently lives in `portolan_cli/extract/arcgis/url_parser.py`. Add a sibling in `errors.py` near the bottom of the error hierarchy:

```python
# Extract Errors (PRTLN-EXT*)
class ArcGISAuthError(PortolanError):
    """Raised when ArcGIS token resolution or authentication fails.

    Error code: PRTLN-EXT002
    """

    code = "PRTLN-EXT002"
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/unit/test_errors.py::test_arcgis_auth_error_code_and_message -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/errors.py tests/unit/test_errors.py
git commit -m "feat(extract): add ArcGISAuthError (PRTLN-EXT002) (#493)"
```

---

## Task 2: `_fetch_json` accepts a token and detects embedded ArcGIS errors

ArcGIS returns HTTP 200 with a body like `{"error": {"code": 499, "message": "Token Required"}}` for secured endpoints. `_fetch_json` must raise `ArcGISDiscoveryError` for that, and append `token=` when a token is supplied.

**Files:**
- Modify: `portolan_cli/extract/arcgis/discovery.py` (`_fetch_json`, ~122-150)
- Test: `tests/unit/extract/arcgis/test_discovery.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/extract/arcgis/test_discovery.py  (append)
import httpx
import pytest
from portolan_cli.extract.arcgis.discovery import ArcGISDiscoveryError, _fetch_json


@pytest.mark.unit
def test_fetch_json_raises_on_embedded_error(monkeypatch) -> None:
    def fake_get(self, url):  # noqa: ANN001
        return httpx.Response(200, json={"error": {"code": 499, "message": "Token Required"}})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    with pytest.raises(ArcGISDiscoveryError, match="499"):
        _fetch_json("https://x/rest/services/Secret")


@pytest.mark.unit
def test_fetch_json_appends_token(monkeypatch) -> None:
    seen: dict[str, str] = {}

    def fake_get(self, url):  # noqa: ANN001
        seen["url"] = url
        return httpx.Response(200, json={"services": [], "folders": []})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    _fetch_json("https://x/rest/services", token="ABC123")
    assert "token=ABC123" in seen["url"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_discovery.py -k "embedded_error or appends_token" -v`
Expected: FAIL (`_fetch_json` has no `token` param; no embedded-error check).

- [ ] **Step 3: Implement**

Replace the `_fetch_json` signature and body in `discovery.py`:

```python
def _fetch_json(url: str, timeout: float = 60.0, token: str | None = None) -> dict[str, Any]:
    """Fetch JSON from URL with standard error handling.

    Appends f=json and, when provided, token=<token>. ArcGIS returns HTTP 200
    with an embedded {"error": {...}} body for secured or invalid endpoints;
    that case is raised as ArcGISDiscoveryError.
    """
    request_url = _ensure_json_format(url)
    if token:
        request_url = _append_query_param(request_url, "token", token)

    try:
        with httpx.Client(timeout=timeout) as client:
            response = client.get(request_url)
            response.raise_for_status()
            data = cast("dict[str, Any]", response.json())
    except httpx.HTTPStatusError as e:
        msg = f"Failed to fetch from {url}: HTTP {e.response.status_code}"
        raise ArcGISDiscoveryError(msg) from e
    except httpx.RequestError as e:
        msg = f"Failed to fetch from {url}: {e}"
        raise ArcGISDiscoveryError(msg) from e
    except ValueError as e:
        msg = f"Invalid JSON response from {url}: {e}"
        raise ArcGISDiscoveryError(msg) from e

    error = data.get("error")
    if isinstance(error, dict):
        code = error.get("code", "unknown")
        message = error.get("message", "ArcGIS error")
        raise ArcGISDiscoveryError(f"ArcGIS error from {url}: {code} {message}")

    return data
```

Add the helper near `_ensure_json_format`:

```python
def _append_query_param(url: str, key: str, value: str) -> str:
    """Append a single query parameter to a URL."""
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    query_params[key] = [value]
    new_query = urlencode(query_params, doseq=True)
    return urlunparse(parsed._replace(query=new_query))
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_discovery.py -k "embedded_error or appends_token" -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/discovery.py tests/unit/extract/arcgis/test_discovery.py
git commit -m "feat(extract): _fetch_json token support + embedded-error detection (#493)"
```

---

## Task 3: `FolderTraversal` + `discover_services_recursive`

Queue-based BFS over folders. Names returned by ArcGIS are already root-qualified, so no renaming. Default `max_depth=2` (folders are single-level per spec; the guard protects against misbehaving servers). Folder fetch failures are recorded and skipped, never raised.

**Files:**
- Modify: `portolan_cli/extract/arcgis/discovery.py`
- Test: `tests/unit/extract/arcgis/test_discovery.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/extract/arcgis/test_discovery.py  (append)
from portolan_cli.extract.arcgis.discovery import (
    FolderTraversal,
    discover_services_recursive,
)


def _mock_endpoints(monkeypatch, responses: dict[str, dict]) -> None:
    """Map base URL (without query) -> JSON body."""
    def fake_fetch(url, timeout=60.0, token=None):  # noqa: ANN001
        base = url.split("?")[0]
        if base not in responses:
            raise ArcGISDiscoveryError(f"ArcGIS error from {url}: 499 Token Required")
        return responses[base]

    monkeypatch.setattr("portolan_cli.extract.arcgis.discovery._fetch_json", fake_fetch)


@pytest.mark.unit
def test_recursive_merges_folder_services(monkeypatch) -> None:
    root = "https://x/rest/services"
    _mock_endpoints(monkeypatch, {
        root: {"services": [{"name": "Top", "type": "MapServer"}],
               "folders": ["NationalDatasets"]},
        f"{root}/NationalDatasets": {
            "services": [
                {"name": "NationalDatasets/Property", "type": "MapServer"},
                {"name": "NationalDatasets/Agriculture", "type": "MapServer"},
            ],
            "folders": [],
        },
    })
    services, traversal = discover_services_recursive(root)
    names = sorted(s.name for s in services)
    assert names == ["NationalDatasets/Agriculture", "NationalDatasets/Property", "Top"]
    assert traversal.visited == ["NationalDatasets"]
    assert traversal.skipped == []
    assert traversal.service_count == 3


@pytest.mark.unit
def test_recursive_skips_secured_folder(monkeypatch) -> None:
    root = "https://x/rest/services"
    _mock_endpoints(monkeypatch, {
        root: {"services": [], "folders": ["Open", "Locked"]},
        f"{root}/Open": {"services": [{"name": "Open/A", "type": "MapServer"}], "folders": []},
        # Locked intentionally absent -> fake_fetch raises -> skipped
    })
    services, traversal = discover_services_recursive(root)
    assert [s.name for s in services] == ["Open/A"]
    assert traversal.visited == ["Open"]
    assert [f for f, _ in traversal.skipped] == ["Locked"]


@pytest.mark.unit
def test_recursive_respects_max_depth(monkeypatch) -> None:
    root = "https://x/rest/services"
    _mock_endpoints(monkeypatch, {
        root: {"services": [], "folders": ["L1"]},
        f"{root}/L1": {"services": [{"name": "L1/A", "type": "MapServer"}], "folders": ["L2"]},
        f"{root}/L2": {"services": [{"name": "L2/B", "type": "MapServer"}], "folders": []},
    })
    services, _ = discover_services_recursive(root, max_depth=1)
    # depth 1 fetches root (depth 0) + L1 (depth 1), not L2
    assert [s.name for s in services] == ["L1/A"]


@pytest.mark.unit
def test_recursive_filters_by_service_type(monkeypatch) -> None:
    root = "https://x/rest/services"
    _mock_endpoints(monkeypatch, {
        root: {"services": [{"name": "Tool", "type": "GPServer"},
                            {"name": "Map", "type": "MapServer"}], "folders": []},
    })
    services, _ = discover_services_recursive(root, service_types=["FeatureServer", "MapServer"])
    assert [s.name for s in services] == ["Map"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_discovery.py -k recursive -v`
Expected: FAIL (`FolderTraversal`/`discover_services_recursive` not defined).

- [ ] **Step 3: Implement**

Add to `discovery.py` (after `discover_services`):

```python
@dataclass
class FolderTraversal:
    """Record of a recursive services-root traversal.

    Attributes:
        visited: Folder names successfully fetched.
        skipped: (folder_name, reason) pairs for folders that errored.
        service_count: Total services discovered (root plus all folders).
    """

    visited: list[str]
    skipped: list[tuple[str, str]]
    service_count: int


def _build_service_list(
    data: dict[str, Any],
    service_types: Sequence[str] | None,
) -> list[ServiceInfo]:
    """Build a filtered ServiceInfo list from a services-root JSON payload."""
    services: list[ServiceInfo] = []
    for service_data in data.get("services", []):
        service = ServiceInfo(name=service_data["name"], service_type=service_data["type"])
        if service_types is None or service.service_type in service_types:
            services.append(service)
    return services


def discover_services_recursive(
    url: str,
    *,
    service_types: Sequence[str] | None = None,
    token: str | None = None,
    timeout: float = 60.0,
    max_depth: int = 2,
) -> tuple[list[ServiceInfo], FolderTraversal]:
    """Discover services from a services root, recursing into folders.

    ArcGIS returns service names already qualified by folder (e.g.
    "NationalDatasets/Property"), so ServiceInfo.get_url(root) stays correct.
    Folders that error (secured, non-200, embedded error) are recorded and
    skipped, never raised. max_depth guards against pathological nesting;
    standard ArcGIS folders are single-level.

    Returns:
        (services, traversal) where traversal records visited/skipped folders.
    """
    root_base = url.split("?")[0].rstrip("/")
    root_data = _fetch_json(root_base, timeout=timeout, token=token)

    services = _build_service_list(root_data, service_types)
    visited: list[str] = []
    skipped: list[tuple[str, str]] = []

    # Queue of (folder_name, depth). Folder names are paths relative to root.
    queue: list[tuple[str, int]] = [(f, 1) for f in root_data.get("folders", [])]
    while queue:
        folder, depth = queue.pop(0)
        if depth > max_depth:
            continue
        folder_url = f"{root_base}/{folder}"
        try:
            folder_data = _fetch_json(folder_url, timeout=timeout, token=token)
        except ArcGISDiscoveryError as e:
            skipped.append((folder, str(e)))
            logger.warning("Skipping folder '%s': %s", folder, e)
            continue
        visited.append(folder)
        services.extend(_build_service_list(folder_data, service_types))
        for sub in folder_data.get("folders", []):
            # Enqueue the full folder path so nested fetch URLs stay correct
            # (<root>/<parent>/<sub>, not <root>/<sub>).
            queue.append((f"{folder}/{sub}", depth + 1))

    traversal = FolderTraversal(
        visited=visited, skipped=skipped, service_count=len(services)
    )
    return services, traversal
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_discovery.py -k recursive -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/discovery.py tests/unit/extract/arcgis/test_discovery.py
git commit -m "feat(extract): recursive folder discovery with traversal record (#493)"
```

---

## Task 4: Auth module (`auth.py`)

Token directly, or username/password minted via ArcGIS `generateToken`. Token-services URL discovered from `<server>/rest/info?f=json`.

**Files:**
- Create: `portolan_cli/extract/arcgis/auth.py`
- Test: `tests/unit/extract/arcgis/test_auth.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/extract/arcgis/test_auth.py
import httpx
import pytest
from portolan_cli.errors import ArcGISAuthError
from portolan_cli.extract.arcgis.auth import (
    ArcGISCredentials,
    apply_token,
    resolve_token,
)


@pytest.mark.unit
def test_apply_token_appends_param() -> None:
    assert "token=T" in apply_token("https://x/rest/services?f=json", "T")


@pytest.mark.unit
def test_resolve_token_returns_explicit_token() -> None:
    creds = ArcGISCredentials(token="EXPLICIT")
    assert resolve_token(creds, "https://x/rest/services") == "EXPLICIT"


@pytest.mark.unit
def test_resolve_token_none_when_no_credentials() -> None:
    assert resolve_token(ArcGISCredentials(), "https://x/rest/services") is None


@pytest.mark.unit
def test_resolve_token_mints_from_username_password(monkeypatch) -> None:
    def fake_get(self, url):  # noqa: ANN001
        # /rest/info returns the token services URL
        return httpx.Response(200, json={
            "authInfo": {"tokenServicesUrl": "https://x/portal/sharing/rest/generateToken"}
        })

    def fake_post(self, url, data=None):  # noqa: ANN001
        assert data["username"] == "u" and data["password"] == "p"
        return httpx.Response(200, json={"token": "MINTED", "expires": 1})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    creds = ArcGISCredentials(username="u", password="p")
    assert resolve_token(creds, "https://x/server/rest/services") == "MINTED"


@pytest.mark.unit
def test_resolve_token_raises_on_mint_error(monkeypatch) -> None:
    def fake_get(self, url):  # noqa: ANN001
        return httpx.Response(200, json={"authInfo": {"tokenServicesUrl": "https://x/generateToken"}})

    def fake_post(self, url, data=None):  # noqa: ANN001
        return httpx.Response(200, json={"error": {"code": 400, "message": "Invalid credentials"}})

    monkeypatch.setattr(httpx.Client, "get", fake_get)
    monkeypatch.setattr(httpx.Client, "post", fake_post)
    with pytest.raises(ArcGISAuthError, match="Invalid credentials"):
        resolve_token(ArcGISCredentials(username="u", password="p"), "https://x/server/rest/services")
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_auth.py -v`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Implement**

```python
# portolan_cli/extract/arcgis/auth.py
"""ArcGIS authentication, minimal token pass-through.

Supports a pre-generated token or username/password minted via the ArcGIS
generateToken endpoint. The token is appended to discovery requests and passed
to gpio.extract_arcgis. This is a contained subset; the full unified auth design
lives in issue #311.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import httpx

from portolan_cli.errors import ArcGISAuthError


@dataclass
class ArcGISCredentials:
    """ArcGIS credentials. token takes precedence over username/password."""

    token: str | None = None
    username: str | None = None
    password: str | None = None

    @property
    def is_empty(self) -> bool:
        """True when no usable credentials are present."""
        return not self.token and not (self.username and self.password)


def apply_token(url: str, token: str) -> str:
    """Append token=<token> to a URL."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params["token"] = [token]
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


def _token_services_url(base_url: str, timeout: float) -> str:
    """Discover the generateToken endpoint from <server>/rest/info."""
    # base_url is e.g. https://host/server/rest/services[/folder]; derive /rest/info
    root = base_url.split("/rest/")[0] + "/rest/info"
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.get(_with_json(root))
            resp.raise_for_status()
            data = cast("dict[str, Any]", resp.json())
    except (httpx.HTTPError, ValueError) as e:
        raise ArcGISAuthError(f"Failed to read token services URL from {root}: {e}", url=root) from e
    token_url = data.get("authInfo", {}).get("tokenServicesUrl")
    if not token_url:
        raise ArcGISAuthError(f"Server does not advertise a token endpoint at {root}", url=root)
    return cast("str", token_url)


def _with_json(url: str) -> str:
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    params.setdefault("f", ["json"])
    return urlunparse(parsed._replace(query=urlencode(params, doseq=True)))


def resolve_token(creds: ArcGISCredentials, base_url: str, timeout: float = 60.0) -> str | None:
    """Resolve a usable token from credentials.

    Returns the explicit token, mints one from username/password, or None when
    no credentials are present. Raises ArcGISAuthError when minting fails.
    """
    if creds.token:
        return creds.token
    if not (creds.username and creds.password):
        return None

    token_url = _token_services_url(base_url, timeout)
    referer = base_url.split("/rest/")[0]
    payload = {
        "username": creds.username,
        "password": creds.password,
        "client": "referer",
        "referer": referer,
        "expiration": "60",
        "f": "json",
    }
    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(token_url, data=payload)
            resp.raise_for_status()
            data = cast("dict[str, Any]", resp.json())
    except (httpx.HTTPError, ValueError) as e:
        raise ArcGISAuthError(f"Token request failed at {token_url}: {e}", url=token_url) from e

    error = data.get("error")
    if isinstance(error, dict):
        raise ArcGISAuthError(
            f"Token request rejected: {error.get('message', 'unknown')}", url=token_url
        )
    token = data.get("token")
    if not token:
        raise ArcGISAuthError(f"No token in response from {token_url}", url=token_url)
    return cast("str", token)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_auth.py -v`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/auth.py tests/unit/extract/arcgis/test_auth.py
git commit -m "feat(extract): ArcGIS token/username-password auth module (#493, refs #311)"
```

---

## Task 5: URL parser accepts folder URLs (`SERVICES_FOLDER`)

**Files:**
- Modify: `portolan_cli/extract/arcgis/url_parser.py`
- Test: `tests/unit/extract/arcgis/test_url_parser.py`

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/extract/arcgis/test_url_parser.py  (append)
import pytest
from portolan_cli.extract.arcgis.url_parser import (
    ArcGISURLType,
    parse_arcgis_url,
)


@pytest.mark.unit
def test_parse_folder_url() -> None:
    r = parse_arcgis_url("https://x/server/rest/services/NationalDatasets")
    assert r.url_type == ArcGISURLType.SERVICES_FOLDER
    assert r.folder == "NationalDatasets"
    assert r.base_url == "https://x/server/rest/services"
    assert r.is_single_service is False


@pytest.mark.unit
def test_parse_folder_url_unicode() -> None:
    r = parse_arcgis_url("https://x/server/rest/services/ecml")
    assert r.url_type == ArcGISURLType.SERVICES_FOLDER
    assert r.folder == "ecml"


@pytest.mark.unit
def test_service_in_folder_still_parses_as_service() -> None:
    r = parse_arcgis_url("https://x/server/rest/services/NationalDatasets/Property/MapServer")
    assert r.url_type == ArcGISURLType.MAP_SERVER
    assert r.service_name == "NationalDatasets/Property"


@pytest.mark.unit
def test_bare_services_root_still_parses() -> None:
    r = parse_arcgis_url("https://x/server/rest/services")
    assert r.url_type == ArcGISURLType.SERVICES_ROOT
    assert r.folder is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_url_parser.py -k "folder or bare_services" -v`
Expected: FAIL (`SERVICES_FOLDER`/`folder` attribute missing).

- [ ] **Step 3: Implement**

In `url_parser.py`:

1. Add the enum member:

```python
class ArcGISURLType(Enum):
    """Type of ArcGIS REST endpoint."""

    FEATURE_SERVER = "FeatureServer"
    MAP_SERVER = "MapServer"
    IMAGE_SERVER = "ImageServer"
    SERVICES_ROOT = "services"
    SERVICES_FOLDER = "services_folder"
```

2. Add the `folder` field and update `is_single_service`:

```python
@dataclass(frozen=True)
class ParsedArcGISURL:
    url_type: ArcGISURLType
    base_url: str
    service_name: str | None
    layer_id: int | None
    folder: str | None = None

    @property
    def is_single_service(self) -> bool:
        """Whether this URL targets a single service (vs a services root/folder)."""
        return self.url_type not in (
            ArcGISURLType.SERVICES_ROOT,
            ArcGISURLType.SERVICES_FOLDER,
        )
```

3. Add a folder pattern next to the others:

```python
# Match: /rest/services/<folder-path> (no server-type segment) -> folder-scoped root
_SERVICES_FOLDER_PATTERN = re.compile(
    r"/rest/services/(.+?)/?$",
    re.IGNORECASE,
)
```

4. In `parse_arcgis_url`, insert a branch AFTER the services-root check and BEFORE the final raise:

```python
    # Try to match services root
    if _SERVICES_ROOT_PATTERN.search(url_path):
        base_url = url_path.rstrip("/")
        return ParsedArcGISURL(
            url_type=ArcGISURLType.SERVICES_ROOT,
            base_url=base_url,
            service_name=None,
            layer_id=None,
        )

    # Try to match a folder-scoped services root: /rest/services/<folder>
    # (reached only when no FeatureServer/MapServer/ImageServer matched)
    folder_match = _SERVICES_FOLDER_PATTERN.search(url_path)
    if folder_match:
        folder = folder_match.group(1).strip("/")
        # base_url is the true services root (folder stripped) so qualified
        # service names from discovery resolve correctly via ServiceInfo.get_url.
        services_idx = url_path.lower().find("/rest/services") + len("/rest/services")
        base_url = url_path[:services_idx].rstrip("/")
        return ParsedArcGISURL(
            url_type=ArcGISURLType.SERVICES_FOLDER,
            base_url=base_url,
            service_name=None,
            layer_id=None,
            folder=folder,
        )

    # No match - not a recognized ArcGIS URL
    raise InvalidArcGISURLError(
        url,
        "not a recognized ArcGIS REST URL; expected FeatureServer, MapServer, ImageServer, or rest/services",
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_url_parser.py -v`
Expected: PASS (new tests plus all existing url_parser tests still green).

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/url_parser.py tests/unit/extract/arcgis/test_url_parser.py
git commit -m "feat(extract): parse folder URLs as SERVICES_FOLDER (#493)"
```

---

## Task 6: `FolderCoverage` on the extraction report

Coverage lives in the shared report model so it serializes into `extraction-report.json` and `--json`. Keep it arcgis-agnostic (no import from `extract.arcgis`).

**Files:**
- Modify: `portolan_cli/extract/common/report.py`
- Test: `tests/unit/extract/common/test_report.py` (create if absent, else append)

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/common/test_report.py  (append)
import pytest
from portolan_cli.extract.common.report import FolderCoverage


@pytest.mark.unit
def test_folder_coverage_roundtrip() -> None:
    cov = FolderCoverage(
        folders_visited=["A", "B"],
        folders_skipped=[("Locked", "499 Token Required")],
        services_found=5,
    )
    d = cov.to_dict()
    assert d["folders_visited"] == ["A", "B"]
    assert d["folders_skipped"] == [{"folder": "Locked", "reason": "499 Token Required"}]
    assert d["services_found"] == 5
    back = FolderCoverage.from_dict(d)
    assert back == cov
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/common/test_report.py -k folder_coverage -v`
Expected: FAIL (`FolderCoverage` missing).

- [ ] **Step 3: Implement**

Add to `common/report.py` (before `ExtractionReport`):

```python
@dataclass
class FolderCoverage:
    """Coverage of a recursive services-root traversal.

    Attributes:
        folders_visited: Folder names successfully traversed.
        folders_skipped: (folder, reason) pairs for folders that errored.
        services_found: Total services discovered.
    """

    folders_visited: list[str]
    folders_skipped: list[tuple[str, str]]
    services_found: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "folders_visited": self.folders_visited,
            "folders_skipped": [{"folder": f, "reason": r} for f, r in self.folders_skipped],
            "services_found": self.services_found,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FolderCoverage:
        return cls(
            folders_visited=data.get("folders_visited", []),
            folders_skipped=[
                (s["folder"], s["reason"]) for s in data.get("folders_skipped", [])
            ],
            services_found=data.get("services_found", 0),
        )
```

Then add an optional field to `ExtractionReport` and wire it through its `to_dict`/`from_dict`. Locate the `ExtractionReport` dataclass and add:

```python
    folder_coverage: FolderCoverage | None = None
```

In `ExtractionReport.to_dict`, before `return result` (or equivalent), add:

```python
        if self.folder_coverage is not None:
            result["folder_coverage"] = self.folder_coverage.to_dict()
```

In `ExtractionReport.from_dict`, add to the constructor kwargs:

```python
            folder_coverage=(
                FolderCoverage.from_dict(data["folder_coverage"])
                if data.get("folder_coverage")
                else None
            ),
```

NOTE: read the existing `ExtractionReport.to_dict`/`from_dict` first and match their exact construction style. `folder_coverage` must default to `None` so all existing `ExtractionReport(...)` call sites stay valid.

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/common/test_report.py -k folder_coverage -v`
Then full report suite: `uv run pytest tests/unit/extract/common/test_report.py -v`
Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/common/report.py tests/unit/extract/common/test_report.py
git commit -m "feat(extract): FolderCoverage on extraction report (#493)"
```

---

## Task 7: `list_services` recurses and reports coverage

**Files:**
- Modify: `portolan_cli/extract/arcgis/orchestrator.py` (`ServicesRootDiscoveryResult`, `list_services`)
- Test: `tests/unit/extract/arcgis/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/arcgis/test_orchestrator.py  (append)
import pytest
from portolan_cli.extract.arcgis.discovery import FolderTraversal, ServiceInfo
from portolan_cli.extract.arcgis.orchestrator import list_services


@pytest.mark.unit
def test_list_services_recurses_and_reports_coverage(monkeypatch) -> None:
    def fake_recursive(url, *, service_types=None, token=None, timeout=60.0, max_depth=2):  # noqa: ANN001
        services = [
            ServiceInfo("Top", "MapServer"),
            ServiceInfo("NationalDatasets/Property", "MapServer"),
        ]
        traversal = FolderTraversal(
            visited=["NationalDatasets"], skipped=[("Locked", "499")], service_count=2
        )
        return services, traversal

    monkeypatch.setattr(
        "portolan_cli.extract.arcgis.orchestrator.discover_services_recursive", fake_recursive
    )
    result = list_services("https://x/rest/services")
    names = [s.name for s in result.services]
    assert "NationalDatasets/Property" in names
    assert result.coverage is not None
    assert result.coverage.folders_visited == ["NationalDatasets"]
    assert result.coverage.folders_skipped == [("Locked", "499")]
    d = result.to_dict()
    assert d["folder_coverage"]["folders_skipped"] == [{"folder": "Locked", "reason": "499"}]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k list_services_recurses -v`
Expected: FAIL (no `coverage` attribute; `list_services` still uses old discovery).

- [ ] **Step 3: Implement**

In `orchestrator.py` imports, add `discover_services_recursive`, `FolderTraversal`, and `FolderCoverage`:

```python
from portolan_cli.extract.arcgis.discovery import (
    FolderTraversal,
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
    discover_layers,
    discover_services,
    discover_services_recursive,
)
...
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    FolderCoverage,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)
```

Add a converter helper near the top of the module:

```python
def _coverage_from_traversal(traversal: FolderTraversal) -> FolderCoverage:
    """Map a discovery FolderTraversal to a serializable FolderCoverage."""
    return FolderCoverage(
        folders_visited=traversal.visited,
        folders_skipped=traversal.skipped,
        services_found=traversal.service_count,
    )
```

Extend `ServicesRootDiscoveryResult`:

```python
@dataclass
class ServicesRootDiscoveryResult:
    services: list[ServiceInfo]
    folders: list[str]
    base_url: str
    coverage: FolderCoverage | None = None

    def to_dict(self) -> dict[str, object]:
        result: dict[str, object] = {
            "base_url": self.base_url,
            "services": [
                {"name": s.name, "type": s.service_type, "url": s.get_url(self.base_url)}
                for s in self.services
            ],
            "folders": self.folders,
            "total_services": len(self.services),
        }
        if self.coverage is not None:
            result["folder_coverage"] = self.coverage.to_dict()
        return result
```

Rewrite `list_services` to recurse (add `token` and `recurse` params):

```python
def list_services(
    url: str,
    *,
    service_types: Sequence[str] | None = None,
    service_filter: list[str] | None = None,
    token: str | None = None,
    recurse: bool = True,
    timeout: float = 60.0,
) -> ServicesRootDiscoveryResult:
    """List services from an ArcGIS services root or folder URL.

    Recurses into folders by default. Folders that error are skipped and
    recorded in the returned coverage.
    """
    from portolan_cli.extract.arcgis.filters import filter_services

    parsed = parse_arcgis_url(url)
    if parsed.url_type not in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
        msg = f"URL is not a services root or folder URL: {url}"
        raise ValueError(msg)

    if recurse:
        services, traversal = discover_services_recursive(
            url,
            service_types=list(service_types) if service_types else None,
            token=token,
            timeout=timeout,
        )
        coverage: FolderCoverage | None = _coverage_from_traversal(traversal)
        folders = traversal.visited
    else:
        services, folders = discover_services(
            url, service_types=list(service_types) if service_types else None,
            return_folders=True, timeout=timeout,
        )
        coverage = None

    if service_filter:
        service_names = [s.name for s in services]
        filtered_names = filter_services(service_names, include=service_filter, case_sensitive=False)
        services = [s for s in services if s.name in filtered_names]

    return ServicesRootDiscoveryResult(
        services=services,
        folders=folders,
        base_url=parsed.base_url,
        coverage=coverage,
    )
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k list_services -v`
Expected: PASS. Re-run existing orchestrator tests, fix any that asserted the old `list_services` discovery path (update their mocks to patch `discover_services_recursive`).

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/orchestrator.py tests/unit/extract/arcgis/test_orchestrator.py
git commit -m "feat(extract): list_services recurses folders and reports coverage (#493)"
```

---

## Task 8: `_discover_and_filter_services` recurses, supports folder scoping and token

**Files:**
- Modify: `portolan_cli/extract/arcgis/orchestrator.py` (`_discover_and_filter_services`)
- Test: `tests/unit/extract/arcgis/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/arcgis/test_orchestrator.py  (append)
from portolan_cli.extract.arcgis.orchestrator import _discover_and_filter_services


@pytest.mark.unit
def test_discover_and_filter_scopes_to_folder(monkeypatch) -> None:
    def fake_recursive(url, *, service_types=None, token=None, timeout=60.0, max_depth=2):  # noqa: ANN001
        services = [
            ServiceInfo("ecml/active_faults", "MapServer"),
            ServiceInfo("water/rivers", "MapServer"),
        ]
        return services, FolderTraversal(visited=["ecml", "water"], skipped=[], service_count=2)

    monkeypatch.setattr(
        "portolan_cli.extract.arcgis.orchestrator.discover_services_recursive", fake_recursive
    )
    services, coverage = _discover_and_filter_services(
        "https://x/rest/services", None, None, 60.0, token=None, folder="ecml"
    )
    assert [s.name for s in services] == ["ecml/active_faults"]
    assert coverage is not None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k scopes_to_folder -v`
Expected: FAIL (signature mismatch, returns a list not a tuple, no folder scoping).

- [ ] **Step 3: Implement**

Replace `_discover_and_filter_services`:

```python
def _discover_and_filter_services(
    url: str,
    service_filter: list[str] | None,
    service_exclude: list[str] | None,
    timeout: float,
    *,
    token: str | None = None,
    folder: str | None = None,
) -> tuple[list[ServiceInfo], FolderCoverage]:
    """Discover services recursively, scope to a folder, and apply filters.

    Returns (services, coverage). When folder is set (SERVICES_FOLDER URL), only
    services under that folder prefix are kept.
    """
    from portolan_cli.extract.arcgis.filters import filter_services

    services, traversal = discover_services_recursive(
        url,
        service_types=["FeatureServer", "MapServer"],
        token=token,
        timeout=timeout,
    )

    if folder:
        prefix = f"{folder.rstrip('/')}/"
        services = [s for s in services if s.name.startswith(prefix)]

    if service_filter or service_exclude:
        service_names = [s.name for s in services]
        filtered_names = filter_services(
            service_names, include=service_filter, exclude=service_exclude, case_sensitive=False
        )
        services = [s for s in services if s.name in filtered_names]

    return services, _coverage_from_traversal(traversal)
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k scopes_to_folder -v`
Expected: PASS. (Callers are updated in Task 10; the suite may show `_extract_services_root` call-site type errors until then, which is expected.)

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/orchestrator.py tests/unit/extract/arcgis/test_orchestrator.py
git commit -m "feat(extract): recursive service discovery with folder scoping (#493)"
```

---

## Task 9: Nested-by-folder output paths

**Files:**
- Modify: `portolan_cli/extract/arcgis/orchestrator.py` (`_extract_services_root`, path building ~966-986)
- Test: `tests/unit/extract/arcgis/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/arcgis/test_orchestrator.py  (append)
from portolan_cli.extract.arcgis.orchestrator import _service_output_dir


@pytest.mark.unit
def test_service_output_dir_nests_folders(tmp_path) -> None:
    # qualified single/multi-segment names -> nested slugified dirs
    assert _service_output_dir(tmp_path, "ecml/active_faults") == tmp_path / "ecml" / "active_faults"
    assert _service_output_dir(tmp_path, "Top") == tmp_path / "top"
    assert (
        _service_output_dir(tmp_path, "RDH_hazard/Flood Risk")
        == tmp_path / "rdh_hazard" / "flood_risk"
    )
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k service_output_dir -v`
Expected: FAIL (`_service_output_dir` not defined).

- [ ] **Step 3: Implement**

Add the helper near `_slugify`:

```python
def _service_output_dir(output_dir: Path, service_name: str) -> Path:
    """Map a (possibly folder-qualified) service name to a nested directory.

    "ecml/active_faults" -> output_dir/ecml/active_faults
    "Top"                -> output_dir/top
    Each path segment is slugified independently so the folder hierarchy is
    preserved as nested subcatalogs (ADR-0032, ADR-0053).
    """
    parts = [_slugify(p) for p in service_name.split("/") if p]
    result = output_dir
    for part in parts:
        result = result / part
    return result
```

Then in `_extract_services_root`, replace the slug/path block (currently):

```python
        service_slug = _slugify(service.name)
        layer_slug = _slugify(layer.name)
        ...
        is_single_layer = layer_count_per_service.get(service.name, 0) == 1
        service_dir = output_dir / service_slug

        if is_single_layer:
            output_path = service_dir / f"{service_slug}.parquet"
        else:
            collection_dir = service_dir / layer_slug
            output_path = collection_dir / f"{layer_slug}.parquet"
```

with:

```python
        layer_slug = _slugify(layer.name)
        service_dir = _service_output_dir(output_dir, service.name)
        service_leaf_slug = service_dir.name

        is_single_layer = layer_count_per_service.get(service.name, 0) == 1

        if is_single_layer:
            output_path = service_dir / f"{service_leaf_slug}.parquet"
        else:
            collection_dir = service_dir / layer_slug
            output_path = collection_dir / f"{layer_slug}.parquet"
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k service_output_dir -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/orchestrator.py tests/unit/extract/arcgis/test_orchestrator.py
git commit -m "feat(extract): nest folder-qualified services as subcatalogs (#493)"
```

---

## Task 10: Route folder URLs, thread auth/coverage through `_extract_services_root` and `extract_arcgis_catalog`

**Files:**
- Modify: `portolan_cli/extract/arcgis/orchestrator.py` (`ExtractionOptions`, `_extract_services_root`, `extract_arcgis_catalog`)
- Test: `tests/unit/extract/arcgis/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/arcgis/test_orchestrator.py  (append)
from pathlib import Path
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    extract_arcgis_catalog,
)


@pytest.mark.unit
def test_extract_routes_folder_url_and_attaches_coverage(monkeypatch, tmp_path) -> None:
    def fake_recursive(url, *, service_types=None, token=None, timeout=60.0, max_depth=2):  # noqa: ANN001
        return (
            [ServiceInfo("ecml/active_faults", "MapServer")],
            FolderTraversal(visited=["ecml"], skipped=[], service_count=1),
        )

    monkeypatch.setattr(
        "portolan_cli.extract.arcgis.orchestrator.discover_services_recursive", fake_recursive
    )
    # No layers discovered -> dry_run avoids real extraction
    monkeypatch.setattr(
        "portolan_cli.extract.arcgis.orchestrator._collect_layers_from_services",
        lambda services, base_url, timeout: ([], {}, {}, []),
    )
    report = extract_arcgis_catalog(
        url="https://x/server/rest/services/ecml",
        output_dir=tmp_path / "out",
        options=ExtractionOptions(dry_run=True),
    )
    assert report.folder_coverage is not None
    assert report.folder_coverage.folders_visited == ["ecml"]
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k routes_folder_url -v`
Expected: FAIL (SERVICES_FOLDER not routed; report has no coverage).

- [ ] **Step 3: Implement**

1. Add auth/recurse fields to `ExtractionOptions`:

```python
@dataclass
class ExtractionOptions:
    workers: int = 3
    retries: int = 3
    timeout: float = 60.0
    resume: bool = False
    raw: bool = False
    dry_run: bool = False
    sort_hilbert: bool = True
    token: str | None = None
    recurse: bool = True
```

2. In `extract_arcgis_catalog`, route folder URLs to the services-root path:

```python
    # Handle services root and folder-scoped URLs
    if parsed.url_type in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
        return _extract_services_root(
            url=url,
            parsed=parsed,
            output_dir=output_dir,
            layer_filter=layer_filter,
            layer_exclude=layer_exclude,
            service_filter=service_filter,
            service_exclude=service_exclude,
            options=options,
            on_progress=on_progress,
        )
```

3. In `_extract_services_root`, capture coverage, pass token/folder, and attach coverage to BOTH the dry-run and final reports. Update the discovery call:

```python
    # Discover and filter services (recursive, folder-scoped, auth-aware)
    services, coverage = _discover_and_filter_services(
        url,
        service_filter,
        service_exclude,
        options.timeout,
        token=options.token,
        folder=parsed.folder,
    )
```

Then where the dry-run report is built, set coverage before returning:

```python
        dry_report = _build_report(
            url=url, discovery_result=combined_discovery, layer_results=dry_run_results
        )
        dry_report.folder_coverage = coverage
        return dry_report
```

And for the final report, before `save_report`:

```python
    report = _build_report(
        url=url, discovery_result=combined_discovery, layer_results=layer_results
    )
    report.folder_coverage = coverage
    report_path = output_dir / ".portolan" / "extraction-report.json"
    save_report(report, report_path)
```

(Replace the existing un-attributed `report = _build_report(...)` block with the version above.)

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k routes_folder_url -v`
Then the full orchestrator suite: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -v`
Expected: PASS. Fix any older tests still asserting the previous (non-recursive, list-returning) `_discover_and_filter_services` shape.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/orchestrator.py tests/unit/extract/arcgis/test_orchestrator.py
git commit -m "feat(extract): route folder URLs, thread auth + coverage through extraction (#493)"
```

---

## Task 11: Pass token into `gpio.extract_arcgis` (feature-detected)

**Files:**
- Modify: `portolan_cli/extract/arcgis/orchestrator.py` (`_extract_single_layer`, ~234-264)
- Test: `tests/unit/extract/arcgis/test_orchestrator.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/unit/extract/arcgis/test_orchestrator.py  (append)
import sys
import types
from portolan_cli.extract.arcgis.discovery import LayerInfo
from portolan_cli.extract.arcgis.orchestrator import ExtractionOptions, _extract_single_layer


@pytest.mark.unit
def test_extract_single_layer_passes_token(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    def fake_extract_arcgis(url, max_workers=None, token=None):  # noqa: ANN001
        captured["token"] = token

        class _T:
            def to_parquet(self, path, **kwargs):  # noqa: ANN001, ANN002
                Path(path).parent.mkdir(parents=True, exist_ok=True)
                Path(path).write_bytes(b"PAR1")

            num_rows = 1

        return _T()

    fake_gpio = types.ModuleType("geoparquet_io")
    fake_gpio.extract_arcgis = fake_extract_arcgis  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "geoparquet_io", fake_gpio)

    layer = LayerInfo(id=0, name="L", layer_type="Feature Layer")
    _extract_single_layer(
        "https://x/rest/services/F/FeatureServer",
        layer,
        tmp_path / "out.parquet",
        ExtractionOptions(token="TKN", sort_hilbert=False),
    )
    assert captured["token"] == "TKN"
```

NOTE: align the fake's `to_parquet`/`num_rows`/return-shape with the real `_extract_single_layer` body. Read lines ~254-300 first and mirror exactly what it calls on the returned table; adjust the fake accordingly so the test exercises only the token-passing branch.

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k passes_token -v`
Expected: FAIL (token never forwarded).

- [ ] **Step 3: Implement**

In `_extract_single_layer`, extend the existing `inspect.signature` feature-detection to include `token`:

```python
    sig = inspect.signature(gpio.extract_arcgis)
    kwargs: dict[str, object] = {}
    if "max_workers" in sig.parameters:
        kwargs["max_workers"] = options.workers
    if options.token and "token" in sig.parameters:
        kwargs["token"] = options.token
    table = gpio.extract_arcgis(layer_url, **kwargs)
```

(Replace the current `if "max_workers" in sig.parameters: ... else: table = gpio.extract_arcgis(layer_url)` block with the kwargs form above, preserving the rest of the function.)

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/extract/arcgis/test_orchestrator.py -k passes_token -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/extract/arcgis/orchestrator.py tests/unit/extract/arcgis/test_orchestrator.py
git commit -m "feat(extract): forward ArcGIS token to gpio.extract_arcgis (#493)"
```

---

## Task 12: CLI flags, credentials, folder URLs, coverage rendering

**Files:**
- Modify: `portolan_cli/cli.py` (`extract arcgis` options + `extract_arcgis_cmd` + `_handle_list_services_mode`)
- Test: `tests/unit/test_cli_extract_arcgis.py` (create if absent, else the existing CLI extract test module)

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/test_cli_extract_arcgis.py  (append/create)
import pytest
from click.testing import CliRunner
from portolan_cli.cli import cli


@pytest.mark.unit
def test_extract_arcgis_has_auth_and_recurse_flags() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["extract", "arcgis", "--help"])
    assert result.exit_code == 0
    for flag in ("--token", "--username", "--password", "--no-recurse"):
        assert flag in result.output


@pytest.mark.unit
def test_list_services_accepts_folder_url(monkeypatch) -> None:
    from portolan_cli.extract.arcgis.discovery import ServiceInfo
    from portolan_cli.extract.common.report import FolderCoverage
    from portolan_cli.extract.arcgis.orchestrator import ServicesRootDiscoveryResult

    def fake_list_services(url, *, service_filter=None, token=None, recurse=True, timeout=60.0):  # noqa: ANN001
        return ServicesRootDiscoveryResult(
            services=[ServiceInfo("ecml/active_faults", "MapServer")],
            folders=["ecml"],
            base_url="https://x/server/rest/services",
            coverage=FolderCoverage(folders_visited=["ecml"], folders_skipped=[("Locked", "499")], services_found=1),
        )

    monkeypatch.setattr("portolan_cli.extract.arcgis.orchestrator.list_services", fake_list_services)
    runner = CliRunner()
    result = runner.invoke(
        cli, ["extract", "arcgis", "https://x/server/rest/services/ecml", "--list-services"]
    )
    assert result.exit_code == 0
    assert "ecml/active_faults" in result.output
    assert "skipped" in result.output.lower()
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/test_cli_extract_arcgis.py -v`
Expected: FAIL (flags absent; `--list-services` rejects folder URL).

- [ ] **Step 3: Implement**

1. Add options to the `extract arcgis` command (next to `--exclude-services`/`--list-services`):

```python
@click.option("--token", default=None, help="ArcGIS token (or set ARCGIS_TOKEN). For secured services/folders.")
@click.option("--username", default=None, help="ArcGIS username (mints a token via generateToken).")
@click.option("--password", default=None, help="ArcGIS password (used with --username).")
@click.option(
    "--no-recurse",
    "no_recurse",
    is_flag=True,
    help="Do not traverse folders for services-root URLs (default: recurse).",
)
```

2. Add the matching params to `extract_arcgis_cmd` signature: `token: str | None`, `username: str | None`, `password: str | None`, `no_recurse: bool`.

3. Near the top of the function body, build credentials (precedence: `--token` > `--username/--password` > `ARCGIS_TOKEN` env):

```python
    import os
    from portolan_cli.extract.arcgis.auth import ArcGISCredentials, resolve_token

    creds = ArcGISCredentials(
        token=token or os.environ.get("ARCGIS_TOKEN"),
        username=username,
        password=password,
    )
    try:
        resolved_token = resolve_token(creds, url, timeout=timeout) if not creds.is_empty else None
    except Exception as e:  # ArcGISAuthError and transport errors
        _output_extract_error(use_json, type(e).__name__, str(e), url)
        raise SystemExit(1) from None
```

4. Update the `--list-services` guard to allow folder URLs and pass token/recurse:

```python
    if list_services:
        if parsed.url_type not in (ArcGISURLType.SERVICES_ROOT, ArcGISURLType.SERVICES_FOLDER):
            _output_extract_error(
                use_json,
                "InvalidURLError",
                "--list-services requires a services root or folder URL",
                url,
            )
            raise SystemExit(1)
        service_filter = _parse_filter_patterns(services)
        _handle_list_services_mode(
            url, service_filter, timeout, use_json, token=resolved_token, recurse=not no_recurse
        )
        return
```

5. Update default output dir to also cover `SERVICES_FOLDER`:

```python
    if output_dir is None:
        if parsed.url_type == ArcGISURLType.SERVICES_ROOT:
            output_dir = Path("services_extract")
        elif parsed.url_type == ArcGISURLType.SERVICES_FOLDER:
            output_dir = Path((parsed.folder or "services_extract").replace("/", "_").lower())
        else:
            service_name = parsed.service_name or "arcgis_extract"
            output_dir = Path(service_name.replace("/", "_").lower())
```

6. Set token/recurse on the options object:

```python
    options = ExtractionOptions(
        workers=workers,
        retries=retries,
        timeout=timeout,
        resume=resume,
        dry_run=dry_run,
        raw=raw,
        token=resolved_token,
        recurse=not no_recurse,
    )
```

7. Update `_handle_list_services_mode` to accept and forward token/recurse, and render coverage:

```python
def _handle_list_services_mode(
    url: str,
    service_filter: list[str] | None,
    timeout: float,
    use_json: bool,
    *,
    token: str | None = None,
    recurse: bool = True,
) -> None:
    """Handle --list-services mode: list available services and exit."""
    from portolan_cli.extract.arcgis.orchestrator import list_services as list_services_func

    try:
        result = list_services_func(
            url, service_filter=service_filter, token=token, recurse=recurse, timeout=timeout
        )
    except Exception as e:
        _output_extract_error(use_json, type(e).__name__, str(e), url)
        raise SystemExit(1) from None

    if use_json:
        click.echo(json.dumps(result.to_dict(), indent=2))
        return

    click.echo(f"Services at {url}:")
    click.echo()
    for svc in result.services:
        click.echo(f"  • {svc.name} ({svc.service_type})")
    click.echo()
    click.echo(f"Total: {len(result.services)} services")
    if result.coverage is not None:
        cov = result.coverage
        click.echo(
            f"Folders traversed: {len(cov.folders_visited)}, skipped: {len(cov.folders_skipped)}"
        )
        for folder, reason in cov.folders_skipped:
            click.echo(f"  ⚠ skipped {folder}: {reason}")
    elif result.folders:
        click.echo(f"Folders: {', '.join(result.folders)}")
```

8. Render coverage in the extraction summary. In `_output_extract_result` (find it in cli.py), after the existing summary lines and only for text output, add:

```python
    coverage = getattr(report, "folder_coverage", None)
    if coverage is not None and not use_json:
        from portolan_cli.output import info, warn

        info(
            f"Folders traversed: {len(coverage.folders_visited)}, "
            f"skipped: {len(coverage.folders_skipped)}, "
            f"services found: {coverage.services_found}"
        )
        for folder, reason in coverage.folders_skipped:
            warn(f"Skipped folder {folder}: {reason}")
```

(Read `_output_extract_result`'s exact signature first; it already receives `report` and a json flag, reuse those names.)

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/test_cli_extract_arcgis.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add portolan_cli/cli.py tests/unit/test_cli_extract_arcgis.py
git commit -m "feat(cli): arcgis auth flags, folder URLs in --list-services, coverage output (#493)"
```

---

## Task 13: Filter behavior on folder-qualified names (tests + docs)

Filters already match qualified names via `fnmatch`; lock it with a test and document it.

**Files:**
- Test: `tests/unit/extract/common/test_filters.py` (append) or `tests/unit/extract/arcgis/test_filters.py`
- Modify: `docs/` ArcGIS extract reference if present (grep first), else skip the doc edit and note it in the ADR.

- [ ] **Step 1: Write the test**

```python
# tests/unit/extract/common/test_filters.py  (append)
import pytest
from portolan_cli.extract.common.filters import filter_services


@pytest.mark.unit
def test_filter_matches_folder_qualified_names() -> None:
    names = ["ecml/active_faults", "ecml/airports_v2", "water/rivers", "Top"]
    assert filter_services(names, include=["ecml/*"]) == ["ecml/active_faults", "ecml/airports_v2"]
    assert filter_services(names, exclude=["water/*"]) == [
        "ecml/active_faults", "ecml/airports_v2", "Top",
    ]
```

- [ ] **Step 2: Run to verify it passes immediately (documents existing behavior)**

Run: `uv run pytest tests/unit/extract/common/test_filters.py -k folder_qualified -v`
Expected: PASS (no code change needed). If it FAILS, fnmatch handling differs from assumption, investigate before proceeding.

- [ ] **Step 3: Docs**

Run `grep -ril "extract arcgis" docs/` to find the reference page. If found, add a short note that `--services`/`--exclude-services` patterns match folder-qualified names (e.g. `--services "ecml/*"`). If no page exists, record the behavior in ADR-0053 (Task 14) instead.

- [ ] **Step 4: Commit**

```bash
git add tests/unit/extract/common/test_filters.py docs/ 2>/dev/null
git commit -m "test(extract): filters match folder-qualified service names; document (#493)"
```

---

## Task 14: ADR-0053 and CLAUDE.md index

**Files:**
- Create: `context/shared/adr/0053-arcgis-folder-recursion-and-structure.md`
- Modify: `CLAUDE.md` (ADR Index table)

- [ ] **Step 1: Write the ADR**

```markdown
# ADR-0053: ArcGIS folder recursion and nested-folder catalog structure

## Status
Adopted

## Context
`portolan extract arcgis` against a services root only discovered top-level
services, silently skipping every service nested in an ArcGIS Server folder, and
rejected folder URLs. ArcGIS Enterprise and federated servers organize services
into folders (some put ALL services in folders), so services-root extraction was
unusable against them. See issue #493.

## Decision
1. Services-root extraction recurses into folders by default. ArcGIS returns
   folder-qualified service names (e.g. `NationalDatasets/Property`), so URL
   construction and glob filters work unchanged. `--no-recurse` opts out.
2. Folders that error or require a token are logged as warnings and skipped, the
   run never aborts. Coverage (folders traversed/skipped, services found) is
   recorded on the extraction report and printed.
3. Folder URLs (`.../rest/services/<folder>`) parse as a new `SERVICES_FOLDER`
   type, scoped to that folder, with the base URL normalized to the true
   services root.
4. Token-secured folders/services are reachable when the user supplies a token
   (`--token`/`ARCGIS_TOKEN`) or username/password (minted via `generateToken`).
   This is a contained pass-through; the full auth design remains issue #311.
5. Folders map to nested subcatalogs, each folder segment becomes a slugified
   subcatalog directory and the service becomes a collection (single layer) or
   subcatalog (multi layer), consistent with ADR-0032 (nested catalogs for
   hierarchy). `--services`/`--exclude-services` match folder-qualified names.

## Consequences
- Enterprise/federated catalogs extract completely by default.
- Catalog trees gain a folder tier; deeper nesting for multi-layer services.
- Slug collisions are possible for names differing only by stripped characters
  (e.g. `türkiye` vs `turkiye`), accepted for now, not mitigated.
- ImageServer-in-folder extraction and the full auth module are out of scope
  (issue #311).

## Related
Issues #493, #6, #492, #358, #311. Supersedes nothing. Builds on ADR-0032,
ADR-0048, ADR-0007.
```

- [ ] **Step 2: Register in CLAUDE.md**

Add this row to the ADR Index table in `CLAUDE.md`, after the ADR-0052 row:

```markdown
| [0053](context/shared/adr/0053-arcgis-folder-recursion-and-structure.md) | ArcGIS folder recursion (default-on), folder URLs, token auth pass-through, nested-folder subcatalogs |
```

- [ ] **Step 3: Validate CLAUDE.md if a validator exists**

Run: `uv run python scripts/validate_claude_md.py` (if present)
Expected: PASS. If the script does not exist, skip.

- [ ] **Step 4: Commit**

```bash
git add context/shared/adr/0053-arcgis-folder-recursion-and-structure.md CLAUDE.md
git commit -m "docs(adr): ADR-0053 ArcGIS folder recursion and structure (#493)"
```

---

## Task 15: Integration tests against real servers (network-marked)

**Files:**
- Modify: `tests/integration/extract/arcgis/test_orchestrator_live.py`

- [ ] **Step 1: Write the tests**

```python
# tests/integration/extract/arcgis/test_orchestrator_live.py  (append)
import pytest
from portolan_cli.extract.arcgis.orchestrator import list_services

SA_ROOT = "https://nspdr.dlrrd.gov.za/server/rest/services"
JRC_ROOT = "https://arcgis-maps.jrc.ec.europa.eu/federated_server/rest/services"


@pytest.mark.network
@pytest.mark.slow
def test_sa_root_traverses_folders() -> None:
    result = list_services(SA_ROOT, timeout=60.0)
    names = [s.name for s in result.services]
    # NationalDatasets services live in a folder; recursion must surface them
    assert any(n.startswith("NationalDatasets/") for n in names)
    assert result.coverage is not None
    assert "NationalDatasets" in result.coverage.folders_visited


@pytest.mark.network
@pytest.mark.slow
def test_jrc_root_has_only_folder_services() -> None:
    # JRC root has ZERO top-level services; everything is in folders.
    result = list_services(JRC_ROOT, timeout=60.0)
    assert len(result.services) > 0
    assert all("/" in s.name for s in result.services)
```

- [ ] **Step 2: Run (network)**

Run: `uv run pytest tests/integration/extract/arcgis/test_orchestrator_live.py -k "sa_root or jrc_root" -m network -v`
Expected: PASS when online. These are `network`-marked, so they are excluded from the default unit run and gated to nightly CI.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/extract/arcgis/test_orchestrator_live.py
git commit -m "test(extract): live folder-recursion tests for SA and JRC roots (#493)"
```

---

## Task 16: Full verification gate

- [ ] **Step 1: Run the arcgis unit + integration (non-network) suites**

Run: `uv run pytest tests/unit/extract/arcgis tests/unit/extract/common tests/integration/extract/arcgis -m "not network and not slow" -v`
Expected: all PASS.

- [ ] **Step 2: Lint, format, types, imports**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy portolan_cli
uv run lint-imports
```
Expected: clean. Fix any issues (the new `auth.py` and `discovery.py` changes must satisfy `mypy --strict`).

- [ ] **Step 3: Quick manual smoke (network, optional)**

Run:
```bash
uv run portolan extract arcgis "https://arcgis-maps.jrc.ec.europa.eu/federated_server/rest/services" --list-services
```
Expected: non-empty list of folder-qualified services, plus a "Folders traversed/skipped" line.

- [ ] **Step 4: Final commit if any fixups were needed**

```bash
git add -A
git commit -m "chore(extract): lint/type fixups for folder recursion (#493)"
```

---

## Self-review notes (for the implementer)

- **Spec coverage:** Task 3 = recursion; Task 4 + 11 + 12 = auth (token + username/password, threaded to discovery and gpio); Task 2 + 3 = graceful skip; Task 5 = folder URLs; Task 9 = nested structure; Task 6 + 7 + 10 + 12 = coverage reporting; Task 13 = filter on qualified names; Task 14 = ADR. All five issue points plus the auth refinement are covered.
- **Type consistency:** `FolderTraversal` (discovery) is converted to `FolderCoverage` (report) only via `_coverage_from_traversal`. `discover_services_recursive` always returns `(list[ServiceInfo], FolderTraversal)`. `_discover_and_filter_services` returns `(list[ServiceInfo], FolderCoverage)`. `ExtractionOptions.token`/`.recurse` are the single source of auth/recurse state inside the orchestrator.
- **Order dependency:** Tasks 7, 8, 10 touch overlapping orchestrator regions, run them in order. Task 8 leaves callers temporarily mismatched until Task 10, this is called out in Task 8 Step 4.
- **Existing tests:** Tasks 7 and 10 explicitly require updating older orchestrator tests that assumed the non-recursive discovery shape. Patch their mocks to `discover_services_recursive`.
```
