"""Catalog management for Portolan.

Primary API (v2):
- init_catalog(): Initialize catalog with STAC catalog.json at root level
- detect_state(): Detect catalog state (MANAGED, UNMANAGED_STAC, FRESH)
- create_catalog(): Create a CatalogModel with auto-extracted fields
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Literal, overload

from portolan_cli.agents_md import visible_stac_files
from portolan_cli.errors import CatalogAlreadyExistsError
from portolan_cli.humanize import humanize_slug
from portolan_cli.json_io import write_json_atomic, write_text_atomic
from portolan_cli.models.catalog import CatalogModel
from portolan_cli.utils import relative_href

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


# Cross-platform file locking (fcntl on Unix, msvcrt on Windows)
if sys.platform == "win32":
    import msvcrt

    def _lock_file(f: Any) -> None:
        """Lock file on Windows using msvcrt."""
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Windows using msvcrt."""
        try:
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        except OSError:
            pass  # May fail if not locked

else:
    import fcntl

    def _lock_file(f: Any) -> None:
        """Lock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


class CatalogState(Enum):
    """The state of a directory with respect to Portolan catalog management.

    States:
        MANAGED: A fully managed Portolan catalog exists. .portolan/config.yaml exists.
            This is the target state after `portolan init`.
            Per issue #290, config.yaml alone is sufficient (state.json removed).

        UNMANAGED_STAC: An existing STAC catalog (catalog.json) exists but is not
            managed by Portolan. This happens when someone has a pre-existing STAC
            catalog that wasn't created by Portolan. Use `portolan adopt` to manage it.

        FRESH: No catalog exists. This is a clean directory suitable for `portolan init`.
            Note: An empty .portolan directory is also considered FRESH.
    """

    MANAGED = "managed"
    UNMANAGED_STAC = "unmanaged_stac"
    FRESH = "fresh"


def detect_state(path: Path) -> CatalogState:
    """Detect the catalog state of a directory.

    Checks only file/directory existence - does NOT read file contents.
    This ensures fast detection without I/O overhead.

    The detection logic (per issue #290, updating ):
    1. If .portolan/config.yaml exists -> MANAGED
    2. If catalog.json exists at root (and not MANAGED) -> UNMANAGED_STAC
    3. Otherwise -> FRESH

    Args:
        path: Directory to check for catalog state.

    Returns:
        CatalogState indicating the current state of the directory.

    Examples:
        >>> detect_state(Path("/empty/dir"))
        CatalogState.FRESH

        >>> detect_state(Path("/my-catalog"))  # where .portolan/config.yaml exists
        CatalogState.MANAGED

        >>> detect_state(Path("/with/only/catalog.json"))
        CatalogState.UNMANAGED_STAC
    """
    portolan_dir = path / ".portolan"
    config_file = portolan_dir / "config.yaml"
    root_catalog = path / "catalog.json"

    # Check for managed state first (config.yaml alone is sufficient per issue #290)
    if config_file.exists():
        return CatalogState.MANAGED

    # Check for unmanaged STAC catalog (catalog.json at root, but not managed)
    if root_catalog.exists():
        return CatalogState.UNMANAGED_STAC

    # Everything else is fresh (including empty .portolan)
    return CatalogState.FRESH


def find_catalog_root(
    start_path: Path | None = None,
    *,
    require_operational: bool = True,
) -> Path | None:
    """Find the catalog root by walking up from the given path.

    Searches for a managed Portolan catalog starting from start_path (or cwd if None)
    and walking up parent directories. This provides git-style behavior
    where commands work from any subdirectory within a catalog.

    issue #290, this uses .portolan/config.yaml as the sole sentinel,
    unifying detection across all CLI commands. By default (require_operational=True),
    it also requires catalog.json to exist to avoid detecting half-initialized repos.

    Security: Limited to MAX_CATALOG_SEARCH_DEPTH levels to prevent
    traversing to filesystem root where a malicious .portolan might exist.

    Args:
        start_path: Starting directory for search (defaults to cwd).
        require_operational: If True (default), require .portolan/config.yaml
            AND catalog.json to exist. Set to False during init_catalog() when
            creating a new catalog where config.yaml is written before catalog.json.

    Returns:
        Path to catalog root if found, None otherwise.

    Examples:
        >>> find_catalog_root(Path("/my-catalog/collection/item"))
        PosixPath('/my-catalog')

        >>> find_catalog_root(Path("/no-catalog-here"))
        None

        >>> find_catalog_root()  # Uses current working directory
        PosixPath('/my-catalog')

        >>> # During init, check for config.yaml only (catalog.json not yet written)
        >>> find_catalog_root(start_path, require_operational=False)
    """
    from portolan_cli.constants import MAX_CATALOG_SEARCH_DEPTH

    def _is_catalog_root(path: Path) -> bool:
        """Check if path is a valid catalog root."""
        config_yaml = path / ".portolan" / "config.yaml"
        if not config_yaml.exists():
            return False

        if not require_operational:
            # During init, config.yaml alone is sufficient
            return True

        # Require operational file: catalog.json at root (state.json removed per issue #290)
        catalog_json = path / "catalog.json"
        return catalog_json.exists()

    # Handle non-existent paths gracefully
    if start_path is not None and not start_path.exists():
        return None

    current = (start_path or Path.cwd()).resolve()
    depth = 0

    # Walk up until we find a valid catalog root, hit filesystem root, or exceed depth
    while current != current.parent and depth < MAX_CATALOG_SEARCH_DEPTH:
        if _is_catalog_root(current):
            return current
        current = current.parent
        depth += 1

    # Check the root directory itself (only if within depth limit)
    if depth < MAX_CATALOG_SEARCH_DEPTH:
        if _is_catalog_root(current):
            return current

    return None


# Keep legacy exception for backward compatibility
class CatalogExistsError(Exception):
    """Raised when attempting to initialize a catalog that already exists.

    Legacy exception kept for backward compatibility.
    New code should use CatalogAlreadyExistsError from portolan_cli.errors.
    """

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"Catalog already exists at {path}")


def _sanitize_id(name: str) -> str:
    """Sanitize a string to be a valid STAC identifier.

    STAC IDs must match pattern ^[a-zA-Z0-9_-]+$

    Args:
        name: Raw string (e.g., directory name).

    Returns:
        Sanitized string suitable for use as STAC id.
    """
    # Replace spaces and special chars with hyphens
    sanitized = re.sub(r"[^a-zA-Z0-9_-]", "-", name)
    # Remove leading/trailing hyphens
    sanitized = sanitized.strip("-")
    # Collapse multiple hyphens
    sanitized = re.sub(r"-+", "-", sanitized)
    # If empty after sanitization, use a default
    if not sanitized:
        sanitized = "catalog"
    return sanitized


@overload
def create_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    return_warnings: Literal[False] = False,
) -> CatalogModel: ...


@overload
def create_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    return_warnings: Literal[True],
) -> tuple[CatalogModel, list[str]]: ...


def create_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    return_warnings: bool = False,
) -> CatalogModel | tuple[CatalogModel, list[str]]:
    """Create a CatalogModel with auto-extracted and optional user-provided fields.

    Auto-extracted fields:
    - id: Derived from directory name (sanitized)
    - created: Current timestamp
    - updated: Current timestamp

    User-provided fields (optional):
    - title: Human-readable title
    - description: Catalog description

    Args:
        path: Directory path for the catalog.
        title: Optional catalog title.
        description: Optional catalog description.
        return_warnings: If True, return (CatalogModel, warnings) tuple.

    Returns:
        CatalogModel instance, or (CatalogModel, warnings) if return_warnings=True.

    Raises:
        CatalogAlreadyExistsError: If .portolan directory already exists.
    """
    portolan_path = path / ".portolan"
    if portolan_path.exists():
        raise CatalogAlreadyExistsError(str(path))

    warnings: list[str] = []

    # Auto-extract id from directory name
    catalog_id = _sanitize_id(path.name)

    # Auto-generate timestamps
    now = datetime.now(timezone.utc)

    # Set description with default if not provided
    if description is None:
        description = "A Portolan-managed STAC catalog"

    # Collect warnings for missing best-practice fields
    if title is None:
        warnings.append("Missing title (recommended for discoverability)")

    catalog = CatalogModel(
        id=catalog_id,
        description=description,
        title=title,
        created=now,
        updated=now,
    )

    if return_warnings:
        return catalog, warnings
    return catalog


class CatalogInitError(Exception):
    """Raised when catalog initialization fails due to filesystem errors."""

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


def _seed_root_metadata(
    portolan_dir: Path,
    license_id: str | None,
    license_url: str | None,
) -> list[str]:
    """Write the catalog-level metadata.yaml carrying the license (issue #686).

    Every collection inherits this license through the hierarchical merge, which is
    what lets ``add`` require one without the human editing anything first.

    Args:
        portolan_dir: The catalog's ``.portolan`` directory.
        license_id: SPDX identifier, or None to leave metadata.yaml to the caller.
        license_url: URL of the license text, required alongside "other".

    Returns:
        Warnings to surface to the user.

    Raises:
        CatalogInitError: If the file cannot be written.
    """
    if license_id is None:
        return []

    metadata_file = portolan_dir / "metadata.yaml"
    if metadata_file.exists():
        # A human who wrote metadata.yaml before running init keeps it. Overwriting
        # would drop their contact, providers, and source fields.
        return [
            f"Kept existing {metadata_file}; the --license value was not written. "
            "Check that its 'license:' is set."
        ]

    from portolan_cli.metadata_yaml import generate_metadata_template

    try:
        write_text_atomic(
            metadata_file,
            generate_metadata_template(license_id=license_id, license_url=license_url or ""),
        )
    except OSError as e:
        raise CatalogInitError(f"Cannot write metadata.yaml: {e}") from e
    return []


def init_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    backend: str = "file",
    license_id: str | None,
    license_url: str | None = None,
) -> tuple[Path, list[str]]:
    """Initialize a new Portolan catalog with the v2 file structure.

    Creates (in order for partial failure recovery):
    1. .portolan/ directory
    2. versions.json at ROOT level (file backend only, consumer-visible)
    3. catalog.json at ROOT level (valid STAC catalog via pystac)
    4. Self link in catalog.json
    5. .portolan/metadata.yaml — seeded with the license
    6. .portolan/config.yaml — sentinel file, written LAST (per issue #290)

    Write order ensures failed runs stay in FRESH state (retry-safe).
    Versions.json is user-visible metadata and lives at the
    catalog root alongside STAC files; only internal tooling state goes in
    .portolan/.

    Note: state.json was removed per issue #290. config.yaml alone is now
    sufficient for MANAGED state detection.

    ``license_id`` has no default, so every caller states its intent. Passing an
    identifier seeds metadata.yaml with it, and the hierarchical merge then hands
    that license to every collection, which is what lets ``add`` require one
    (issue #686). Passing None writes no metadata.yaml, for callers that own the
    file themselves: ``extract`` seeds it from harvested service metadata, and
    ``clone`` copies collections that already carry their own licenses. Callers
    validate the value with ``licensing.license_gap`` before getting here.

    Args:
        path: Directory path for the catalog. Will be created if doesn't exist.
        title: Optional catalog title.
        description: Optional catalog description.
        backend: Versioning backend name.
        license_id: SPDX identifier, or "other" alongside license_url. None leaves
            metadata.yaml to the caller.
        license_url: URL of the license text. Required when license_id is "other".

    Returns:
        Tuple of (catalog_file_path, warnings).

    Raises:
        CatalogAlreadyExistsError: If directory is in MANAGED state.
        UnmanagedStacCatalogError: If directory is in UNMANAGED_STAC state.
        CatalogInitError: If filesystem operations fail.
    """
    import pystac

    from portolan_cli.errors import UnmanagedStacCatalogError

    # Ensure path exists
    path = Path(path)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create directory: {e}") from e

    # Check state and raise appropriate errors
    state = detect_state(path)
    if state == CatalogState.MANAGED:
        raise CatalogAlreadyExistsError(str(path))
    if state == CatalogState.UNMANAGED_STAC:
        raise UnmanagedStacCatalogError(str(path))

    # Validate non-file backends are available before creating any files
    if backend != "file":
        from portolan_cli.backends import get_backend

        try:
            get_backend(backend)
        except ValueError as e:
            raise CatalogInitError(str(e)) from e

    warnings: list[str] = []

    # Auto-extract id from directory name
    catalog_id = _sanitize_id(path.resolve().name)

    # Set defaults. Issue #502: title is mandatory and must be human-readable,
    # so derive one from the directory name instead of leaving it empty.
    if not title:
        title = humanize_slug(catalog_id)
        warnings.append(f"Derived catalog title '{title}' from directory name")

    if description is None:
        description = "A Portolan-managed STAC catalog"

    # ─────────────────────────────────────────────────────────────────────────
    # WRITE ORDER: config.yaml LAST for atomicity (per issue #290)
    # detect_state() checks for config.yaml to determine MANAGED state.
    # Writing config.yaml LAST ensures that if init fails partway through,
    # the directory stays in FRESH state and can be safely retried.
    # ─────────────────────────────────────────────────────────────────────────

    # Step 1: Create .portolan directory
    portolan_dir = path / ".portolan"
    try:
        portolan_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create .portolan directory: {e}") from e

    # Step 2: versions.json - only for file backend
    # Versions.json is consumer-visible metadata and must live at
    # the catalog root alongside STAC files, NOT inside .portolan/ (which is
    # reserved for internal tooling state only).
    # Written early so failure here leaves directory in FRESH state.
    if backend == "file":
        now = datetime.now(timezone.utc)
        versions_data = {
            "schema_version": "1.0.0",
            "catalog_id": catalog_id,
            "created": now.isoformat(),
            "collections": {},
        }
        try:
            write_json_atomic(path / "versions.json", versions_data)
        except OSError as e:
            raise CatalogInitError(f"Cannot write versions.json: {e}") from e

    # Step 3: Create STAC catalog using pystac
    catalog = pystac.Catalog(
        id=catalog_id,
        description=description,
        title=title,
    )

    catalog_file = path / "catalog.json"
    # Trailing slash required: pystac treats dotted paths (e.g., tmp.xyz) as files
    catalog.normalize_hrefs(f"{path}/")
    try:
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog.json: {e}") from e

    # No self link: a SELF_CONTAINED catalog omits it, which is also
    # what pystac emits and what rashid's PTL-LNK-005 enforces. `init` used to
    # append one by hand; `add` then stripped it, so only init-only catalogs
    # carried the violation and the conformance gate (which runs init + add)
    # never saw it.

    # Step 4b: AGENTS.md - scaffold the AI/agent guide and add its rel="agents"
    # link (rashid PTL-FIL-002). Emitting it here keeps freshly-created catalogs
    # schema-valid without a follow-up `check --fix`.
    from portolan_cli.agents_md import ensure_agents_md

    try:
        ensure_agents_md(catalog_file)
    except OSError as e:
        raise CatalogInitError(f"Cannot write AGENTS.md: {e}") from e

    # Step 4c: declare the Portolan profile schema URI and scaffold README.md
    # with its rel="describedby" link (issue #654), so a catalog conforms the
    # moment it is created, before anything is added to it.
    from portolan_cli.readme import ensure_readmes

    try:
        ensure_schema_uris(path)
        ensure_readmes(path)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog conformance files: {e}") from e

    # Step 4d: metadata.yaml - seed the license the human supplied. Every collection
    # inherits it through the hierarchical merge, so the gate in add_files passes
    # without the human editing anything first (issue #686). Skipped when the caller
    # owns the file: extract seeds it from harvested metadata, and writing here first
    # would make its O_EXCL create a no-op and silently drop everything harvested.
    warnings.extend(_seed_root_metadata(portolan_dir, license_id, license_url))

    # Step 5: config.yaml - sentinel file per issue #290 (sufficient for MANAGED state)
    # Written LAST for atomicity: if any previous step fails, directory stays FRESH
    # and init can be safely retried. Also serves as user configuration file for
    # settings like remote, aws_profile, etc.
    config_content = "# Portolan configuration\n"
    if backend != "file":
        config_content = f"# Portolan configuration\nbackend: {backend}\n"
    try:
        write_text_atomic(portolan_dir / "config.yaml", config_content)
    except OSError as e:
        raise CatalogInitError(f"Cannot write config.yaml: {e}") from e

    return catalog_file, warnings


class Catalog:
    """A Portolan catalog backed by a .portolan directory.

    The Catalog class provides the Python API for all catalog operations.
    The CLI commands are thin wrappers around these methods.

    Note: This is the legacy v1 API. New code should use init_catalog()
    which creates the v2 file structure with catalog.json at root level.

    Attributes:
        root: The root directory containing the .portolan folder.
    """

    PORTOLAN_DIR = ".portolan"
    CATALOG_FILE = "catalog.json"

    def __init__(self, root: Path) -> None:
        """Initialize a Catalog instance.

        Args:
            root: The root directory containing the .portolan folder.
        """
        self.root = root

    @property
    def portolan_path(self) -> Path:
        """Path to the .portolan directory."""
        return self.root / self.PORTOLAN_DIR

    @property
    def catalog_file(self) -> Path:
        """Path to the catalog.json file (at root, not inside .portolan)."""
        return self.root / self.CATALOG_FILE

    @classmethod
    def init(cls, root: Path, *, license_id: str | None = None) -> Self:
        """Initialize a new Portolan catalog.

        Creates the catalog using the v2 file structure via init_catalog().

        Args:
            root: The directory where the catalog should be created.
            license_id: SPDX identifier to seed into metadata.yaml. Left None, the
                catalog starts unlicensed and ``add`` will ask for a license before
                it writes a collection (issue #686).

        Returns:
            A Catalog instance for the newly created catalog.

        Raises:
            CatalogExistsError: If a .portolan directory already exists.
        """
        portolan_path = root / cls.PORTOLAN_DIR

        if portolan_path.exists():
            raise CatalogExistsError(portolan_path)

        # Use init_catalog for v2 file structure
        init_catalog(root, license_id=license_id)

        return cls(root)


def intermediate_catalog_ids(collection_id: str) -> list[str]:
    """Return the ancestor sub-catalog ids for a nested collection.

    These are the intermediate directory levels between the catalog root and the
    leaf collection, each of which holds a ``catalog.json`` (created by
    ``create_intermediate_catalogs`` during ``add``). This is the single source
    of truth for the path-segment walk, reused by both ``add`` (to create the
    files) and ``push`` (to discover them as upload targets).

    Examples:
        "climate/hittekaart" -> ["climate"]
        "env/air/quality"    -> ["env", "env/air"]
        "demographics"       -> []  (leaf holds collection.json, no intermediates)

    Args:
        collection_id: The (possibly nested) collection ID, POSIX-separated.

    Returns:
        Ancestor sub-catalog ids in root-to-leaf order (empty for single-level).
    """
    parts = collection_id.split("/")
    return ["/".join(parts[: i + 1]) for i in range(len(parts) - 1)]


def create_intermediate_catalogs(collection_id: str, catalog_root: Path) -> None:
    """Create intermediate catalog.json files for nested collection paths.

    For a nested collection ID like "climate/hittekaart", this creates:
    - climate/catalog.json (intermediate catalog)

    For deeper nesting like "env/air/quality", this creates:
    - env/catalog.json
    - env/air/catalog.json

    Single-level collection IDs (e.g., "demographics") create no intermediate catalogs
    since the directory will contain collection.json directly.

    Args:
        collection_id: The nested collection ID (e.g., "climate/hittekaart").
        catalog_root: Root directory of the catalog.
    """
    # Create catalog.json at each intermediate level (all but the last).
    # intermediate_catalog_ids is the shared source of truth for this walk,
    # also used by push discovery (keeps add/push in lockstep).
    for intermediate_path in intermediate_catalog_ids(collection_id):
        catalog_dir = catalog_root / intermediate_path
        catalog_file = catalog_dir / "catalog.json"

        # Skip if already exists
        if catalog_file.exists():
            continue

        # Create directory if needed
        catalog_dir.mkdir(parents=True, exist_ok=True)

        # Root and parent diverge below the first level: the root catalog is
        # `depth` levels up, while the containing object is always the catalog
        # one level up. Deriving both from the depth made every intermediate
        # below the first point past its own parent (issue #711).
        root_href = relative_href(catalog_dir, catalog_root / "catalog.json")
        parent_href = relative_href(catalog_dir, catalog_dir.parent / "catalog.json")

        # Titled after its own path segment, not the full id: the title is what
        # a browser shows and what ensure_link_titles copies onto the parent's
        # child link, so "Air Quality" reads better than "Env/Air Quality"
        # (issue #502, rashid PTL-TTL-001 and PTL-TTL-003).
        title = humanize_slug(catalog_dir.name)

        # Create intermediate catalog
        catalog_data = {
            "type": "Catalog",
            "id": intermediate_path,
            "stac_version": "1.1.0",
            "title": title,
            "description": f"Catalog: {intermediate_path}",
            "links": [
                {"rel": "root", "href": root_href, "type": "application/json"},
                {"rel": "parent", "href": parent_href, "type": "application/json"},
            ],
        }

        write_json_atomic(catalog_file, catalog_data)

        # Intermediate catalogs are catalogs too: scaffold AGENTS.md and add the
        # rel="agents" link so every catalog.json satisfies rashid PTL-FIL-002.
        from portolan_cli.agents_md import ensure_agents_md

        ensure_agents_md(catalog_file)


def update_catalog_links_for_nested(catalog_root: Path, collection_id: str) -> None:
    """Update catalog links for nested collection structure.

    Ensures:
    - Root catalog links to intermediate catalogs (not directly to leaf collections)
    - Intermediate catalogs link to their child catalogs/collections

    For "climate/hittekaart":
    - Root catalog links to ./climate/catalog.json
    - climate/catalog.json links to ./hittekaart/collection.json

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: The nested collection ID (e.g., "climate/hittekaart").
    """
    parts = collection_id.split("/")

    # For single-level collections, just ensure root links to collection
    if len(parts) == 1:
        _ensure_root_links_to_child(catalog_root, f"./{parts[0]}/collection.json")
        return

    # For nested collections:
    # 1. Root links to first-level catalog
    first_level = parts[0]
    _ensure_root_links_to_child(catalog_root, f"./{first_level}/catalog.json")

    # 2. Each intermediate catalog links to next level
    for i in range(len(parts) - 1):
        intermediate_path = "/".join(parts[: i + 1])
        catalog_file = catalog_root / intermediate_path / "catalog.json"

        if not catalog_file.exists():
            continue

        # Determine what the intermediate should link to
        next_part = parts[i + 1]
        is_last_intermediate = i == len(parts) - 2

        if is_last_intermediate:
            # Link to leaf collection
            child_href = f"./{next_part}/collection.json"
        else:
            # Link to next intermediate catalog
            child_href = f"./{next_part}/catalog.json"

        _ensure_catalog_links_to_child(catalog_file, child_href)


def _ensure_root_links_to_child(catalog_root: Path, child_href: str) -> None:
    """Ensure root catalog has a child link."""
    catalog_file = catalog_root / "catalog.json"
    if not catalog_file.exists():
        return

    content = json.loads(catalog_file.read_text(encoding="utf-8"))
    links = content.get("links", [])

    # Check if link already exists
    existing_hrefs = {link.get("href") for link in links if link.get("rel") == "child"}
    if child_href in existing_hrefs:
        return

    # Add the child link
    links.append({"rel": "child", "href": child_href, "type": "application/json"})
    content["links"] = links
    write_json_atomic(catalog_file, content)


def _ensure_catalog_links_to_child(catalog_file: Path, child_href: str) -> None:
    """Ensure a catalog file has a child link."""
    if not catalog_file.exists():
        return

    content = json.loads(catalog_file.read_text(encoding="utf-8"))
    links = content.get("links", [])

    # Check if link already exists
    existing_hrefs = {link.get("href") for link in links if link.get("rel") == "child"}
    if child_href in existing_hrefs:
        return

    # Add the child link
    links.append({"rel": "child", "href": child_href, "type": "application/json"})
    content["links"] = links
    write_json_atomic(catalog_file, content)


def _link_title_from_target(
    stac_file: Path, link: dict[str, object], catalog_root: Path
) -> str | None:
    """Read the human-readable title of a child/item link's target.

    Child links target a ``collection.json``/``catalog.json`` (title at the top
    level); item links target an ``item.json`` (title under ``properties``).

    Args:
        stac_file: The STAC file the link lives in (for relative resolution).
        link: The link mapping (must have an ``href``).
        catalog_root: Root the resolved target must stay within.

    Returns:
        The target's title, or None if it can't be read.
    """
    href = link.get("href")
    if not isinstance(href, str) or not href:
        return None

    target = (stac_file.parent / href).resolve()

    # input hardening: ignore ``../`` hrefs that resolve outside the
    # catalog so a crafted link can't read files elsewhere on disk.
    root = catalog_root.resolve()
    if target != root and root not in target.parents:
        return None

    if not target.exists():
        return None

    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None

    if link.get("rel") == "item":
        properties = data.get("properties", {})
        title = properties.get("title") if isinstance(properties, dict) else None
    else:
        title = data.get("title")

    return title if isinstance(title, str) and title.strip() else None


def ensure_link_titles(catalog_root: Path) -> bool:
    """Backfill ``title`` (and ``type``) on all child/item links (Issue #502).

    Walks every ``catalog.json``/``collection.json`` under ``catalog_root`` and,
    for each ``child``/``item`` link, copies the target's human-readable title
    onto the link so STAC Browser can render names without fetching every child.
    Also fills a missing ``type`` (``application/json`` for child links,
    ``application/geo+json`` for item links).

    Idempotent: only rewrites a file when a link actually changed. Reused by the
    ``check --fix`` repair path.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        True if any file was modified.
    """
    changed_any = False

    stac_files = visible_stac_files(catalog_root)

    for stac_file in stac_files:
        try:
            content = json.loads(stac_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue

        links = content.get("links")
        if not isinstance(links, list):
            continue

        file_changed = False
        for link in links:
            if not isinstance(link, dict) or link.get("rel") not in ("child", "item"):
                continue

            # Fill a missing media type (STAC best practice).
            if not link.get("type"):
                link["type"] = (
                    "application/geo+json" if link.get("rel") == "item" else "application/json"
                )
                file_changed = True

            title = _link_title_from_target(stac_file, link, catalog_root)
            if title and link.get("title") != title:
                link["title"] = title
                file_changed = True

        if file_changed:
            write_json_atomic(stac_file, content)
            changed_any = True

    return changed_any


def ensure_schema_uris(catalog_root: Path) -> bool:
    """Declare the Portolan profile schema URI across a catalog tree (issue #654).

    Walks every ``catalog.json`` and ``collection.json`` under ``catalog_root``
    and stamps the versioned profile URI into ``stac_extensions``. Items are left
    alone: the conformance claim lives on catalogs and collections.

    Idempotent — a tree that already declares the current URI is not rewritten.
    Shared by ``init``, ``add``, and the ``check --fix`` repair path so all three
    produce the same output.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        True if any file was modified.
    """
    from portolan_cli.stac import ensure_portolan_schema_uri

    changed_any = False
    stac_files = visible_stac_files(catalog_root)

    for stac_file in stac_files:
        try:
            content = json.loads(stac_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        if not isinstance(content, dict):
            continue
        if ensure_portolan_schema_uri(content):
            write_json_atomic(stac_file, content)
            changed_any = True

    return changed_any


def update_catalog_versions(
    catalog_root: Path,
    collection_id: str,
    current_version: str,
    asset_count: int,
    total_size_bytes: int,
) -> None:
    """Update catalog-level versions.json with collection state.

    The catalog-level versions.json tracks aggregate state of all collections,
    providing a quick overview without needing to read each collection's
    versions.json individually.

    This function is called after each successful collection update to keep
    the catalog-level view in sync.

    Uses file locking to prevent race conditions when multiple `add` commands
    run concurrently (e.g., CI parallelism, adding to multiple collections).

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: The collection that was updated (e.g., "demographics").
        current_version: The new current version of the collection.
        asset_count: Number of assets in the current version.
        total_size_bytes: Total size of all assets in bytes.

    Raises:
        CatalogVersionsCorruptedError: If catalog versions.json is invalid JSON
            or has invalid structure.
    """

    from portolan_cli.output import warn

    versions_path = catalog_root / "versions.json"

    if not versions_path.exists():
        # Not a file-backend catalog (e.g., Iceberg backend)
        return

    lock_path = catalog_root / ".portolan" / ".versions.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # Use file locking to prevent concurrent read-modify-write races
    with lock_path.open("w") as lock_file:
        _lock_file(lock_file)
        try:
            # Read existing catalog versions.json with error handling
            try:
                content = json.loads(versions_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError as e:
                # Clear error message for corrupted file
                msg = (
                    f"Catalog versions.json is corrupted: {e}. "
                    f"File: {versions_path}. "
                    "Fix manually or delete to reinitialize."
                )
                warn(msg)
                raise CatalogVersionsCorruptedError(msg) from e

            # Validate structure before mutating
            if not isinstance(content, dict):
                msg = (
                    f"Catalog versions.json has invalid structure: expected dict, "
                    f"got {type(content).__name__}. File: {versions_path}. "
                    "Fix manually or delete to reinitialize."
                )
                warn(msg)
                raise CatalogVersionsCorruptedError(msg)

            collections_value = content.get("collections")
            if collections_value is not None and not isinstance(collections_value, dict):
                msg = (
                    f"Catalog versions.json 'collections' has invalid type: "
                    f"expected dict or null, got {type(collections_value).__name__}. "
                    f"File: {versions_path}. Fix manually or delete to reinitialize."
                )
                warn(msg)
                raise CatalogVersionsCorruptedError(msg)

            # Update the collections entry
            now = datetime.now(timezone.utc).isoformat()
            if "collections" not in content:
                content["collections"] = {}

            content["collections"][collection_id] = {
                "current_version": current_version,
                "updated": now,
                "asset_count": asset_count,
                "total_size_bytes": total_size_bytes,
            }

            # Update catalog-level updated timestamp
            content["updated"] = now

            write_json_atomic(versions_path, content)
        finally:
            _unlock_file(lock_file)


class CatalogVersionsCorruptedError(Exception):
    """Raised when catalog-level versions.json is corrupted."""

    pass


# Re-export add_files for STAC-aligned imports (ADR terminology)
from portolan_cli.add import add_files as add_files  # noqa: E402, F401, PLC0414
