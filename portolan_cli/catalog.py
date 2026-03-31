"""Catalog management for Portolan.

Primary API (v2, per ADR-0023):
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
from typing import Literal, overload

from portolan_cli.errors import CatalogAlreadyExistsError
from portolan_cli.models.catalog import CatalogModel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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

    The detection logic (per issue #290, updating ADR-0027):
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

    Per ADR-0029 and issue #290, this uses .portolan/config.yaml as the sole sentinel,
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


def init_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
) -> tuple[Path, list[str]]:
    """Initialize a new Portolan catalog with the v2 file structure.

    Creates (in order for partial failure recovery):
    1. .portolan/ directory
    2. .portolan/config.yaml (sentinel file, per issue #290)
    3. versions.json at ROOT level (consumer-visible per ADR-0023)
    4. catalog.json at ROOT level (valid STAC catalog via pystac)

    Write order ensures failed runs stay in FRESH state (retry-safe).
    Per ADR-0023: versions.json is user-visible metadata and lives at the
    catalog root alongside STAC files; only internal tooling state goes in
    .portolan/.

    Note: state.json was removed per issue #290. config.yaml alone is now
    sufficient for MANAGED state detection.

    Args:
        path: Directory path for the catalog. Will be created if doesn't exist.
        title: Optional catalog title.
        description: Optional catalog description.

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

    warnings: list[str] = []

    # Auto-extract id from directory name
    catalog_id = _sanitize_id(path.resolve().name)

    # Set defaults
    if description is None:
        description = "A Portolan-managed STAC catalog"

    if title is None:
        warnings.append("Missing title (recommended for discoverability)")

    # ─────────────────────────────────────────────────────────────────────────
    # WRITE ORDER: config.yaml FIRST as sentinel (per issue #290)
    # detect_state() checks for config.yaml to determine MANAGED state.
    # Writing config.yaml first means init is atomic - once it exists, catalog
    # is MANAGED. However, we still write catalog.json last to ensure a valid
    # STAC catalog is in place before the directory is considered complete.
    # ─────────────────────────────────────────────────────────────────────────

    # Step 1: Create .portolan directory
    portolan_dir = path / ".portolan"
    try:
        portolan_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create .portolan directory: {e}") from e

    # Step 2: config.yaml - sentinel file per issue #290 (sufficient for MANAGED state)
    # Also serves as user configuration file for settings like remote, aws_profile, etc.
    try:
        (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    except OSError as e:
        raise CatalogInitError(f"Cannot write config.yaml: {e}") from e

    # Step 3: versions.json - minimal catalog-level versioning
    # Per ADR-0023: versions.json is consumer-visible metadata and must live at
    # the catalog root alongside STAC files, NOT inside .portolan/ (which is
    # reserved for internal tooling state only).
    now = datetime.now(timezone.utc)
    versions_data = {
        "schema_version": "1.0.0",
        "catalog_id": catalog_id,
        "created": now.isoformat(),
        "collections": {},
    }
    try:
        (path / "versions.json").write_text(json.dumps(versions_data, indent=2) + "\n")
    except OSError as e:
        raise CatalogInitError(f"Cannot write versions.json: {e}") from e

    # Step 4: Create STAC catalog using pystac
    catalog = pystac.Catalog(
        id=catalog_id,
        description=description,
        title=title,
    )

    catalog_file = path / "catalog.json"
    catalog.normalize_hrefs(str(path))
    try:
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog.json: {e}") from e

    # Step 5: Add self link (STAC best practice)
    # pystac SELF_CONTAINED doesn't add self link, so we add it manually
    try:
        data = json.loads(catalog_file.read_text())
        # Use setdefault for defensive coding (pystac should always create links)
        data.setdefault("links", []).append(
            {
                "rel": "self",
                "href": "./catalog.json",
                "type": "application/json",
            }
        )
        catalog_file.write_text(json.dumps(data, indent=2))
    except json.JSONDecodeError as e:
        raise CatalogInitError(f"Cannot parse catalog.json: {e}") from e
    except OSError as e:
        raise CatalogInitError(f"Cannot update catalog.json with self link: {e}") from e

    # Note: state.json creation removed per issue #290
    # config.yaml alone is sufficient for MANAGED state detection

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
    STAC_VERSION = "1.0.0"

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
    def init(cls, root: Path) -> Self:
        """Initialize a new Portolan catalog.

        Creates the catalog using the v2 file structure via init_catalog().

        Args:
            root: The directory where the catalog should be created.

        Returns:
            A Catalog instance for the newly created catalog.

        Raises:
            CatalogExistsError: If a .portolan directory already exists.
        """
        portolan_path = root / cls.PORTOLAN_DIR

        if portolan_path.exists():
            raise CatalogExistsError(portolan_path)

        # Use init_catalog for v2 file structure
        init_catalog(root)

        return cls(root)


def create_intermediate_catalogs(collection_id: str, catalog_root: Path) -> None:
    """Create intermediate catalog.json files for nested collection paths (ADR-0032).

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
    parts = collection_id.split("/")

    # No intermediates needed for single-level collections
    if len(parts) <= 1:
        return

    # Create catalog.json at each intermediate level (all but the last)
    for i in range(len(parts) - 1):
        intermediate_path = "/".join(parts[: i + 1])
        catalog_dir = catalog_root / intermediate_path
        catalog_file = catalog_dir / "catalog.json"

        # Skip if already exists
        if catalog_file.exists():
            continue

        # Create directory if needed
        catalog_dir.mkdir(parents=True, exist_ok=True)

        # Calculate relative path depth for links
        depth = i + 1  # How many levels deep from root
        parent_href = "../" * depth + "catalog.json"

        # Create intermediate catalog
        catalog_data = {
            "type": "Catalog",
            "id": intermediate_path,
            "stac_version": "1.1.0",
            "description": f"Catalog: {intermediate_path}",
            "links": [
                {"rel": "root", "href": parent_href, "type": "application/json"},
                {"rel": "parent", "href": parent_href, "type": "application/json"},
                {"rel": "self", "href": "./catalog.json", "type": "application/json"},
            ],
        }

        catalog_file.write_text(json.dumps(catalog_data, indent=2))


def update_catalog_links_for_nested(catalog_root: Path, collection_id: str) -> None:
    """Update catalog links for nested collection structure (ADR-0032).

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

    content = json.loads(catalog_file.read_text())
    links = content.get("links", [])

    # Check if link already exists
    existing_hrefs = {link.get("href") for link in links if link.get("rel") == "child"}
    if child_href in existing_hrefs:
        return

    # Add the child link
    links.append({"rel": "child", "href": child_href, "type": "application/json"})
    content["links"] = links
    catalog_file.write_text(json.dumps(content, indent=2))


def _ensure_catalog_links_to_child(catalog_file: Path, child_href: str) -> None:
    """Ensure a catalog file has a child link."""
    if not catalog_file.exists():
        return

    content = json.loads(catalog_file.read_text())
    links = content.get("links", [])

    # Check if link already exists
    existing_hrefs = {link.get("href") for link in links if link.get("rel") == "child"}
    if child_href in existing_hrefs:
        return

    # Add the child link
    links.append({"rel": "child", "href": child_href, "type": "application/json"})
    content["links"] = links
    catalog_file.write_text(json.dumps(content, indent=2))
