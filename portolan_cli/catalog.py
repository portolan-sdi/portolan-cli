"""Catalog management for Portolan.

Primary API (v2, per ADR-0023):
- init_catalog(): Initialize catalog with STAC catalog.json at root level
- detect_state(): Detect catalog state (MANAGED, UNMANAGED_STAC, FRESH)

Legacy API (v1, unused — candidates for removal):
- create_catalog(): Create a CatalogModel with auto-extracted fields
- write_catalog_json(): Serialize CatalogModel to .portolan/catalog.json
- read_catalog_json(): Load CatalogModel from .portolan/catalog.json
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
        MANAGED: A fully managed Portolan catalog exists. Both .portolan/config.json
            AND .portolan/state.json exist. This is the target state after `portolan init`.

        UNMANAGED_STAC: An existing STAC catalog (catalog.json) exists but is not
            managed by Portolan. This happens when someone has a pre-existing STAC
            catalog that wasn't created by Portolan. Use `portolan adopt` to manage it.

        FRESH: No catalog exists. This is a clean directory suitable for `portolan init`.
            Note: An empty .portolan directory or partial .portolan (only one of
            config.json/state.json) is also considered FRESH.
    """

    MANAGED = "managed"
    UNMANAGED_STAC = "unmanaged_stac"
    FRESH = "fresh"


def detect_state(path: Path) -> CatalogState:
    """Detect the catalog state of a directory.

    Checks only file/directory existence - does NOT read file contents.
    This ensures fast detection without I/O overhead.

    The detection logic:
    1. If .portolan/config.json AND .portolan/state.json both exist -> MANAGED
    2. If catalog.json exists at root (and not MANAGED) -> UNMANAGED_STAC
    3. Otherwise -> FRESH

    Args:
        path: Directory to check for catalog state.

    Returns:
        CatalogState indicating the current state of the directory.

    Examples:
        >>> detect_state(Path("/empty/dir"))
        CatalogState.FRESH

        >>> detect_state(Path("/my-catalog"))  # where .portolan/config.json and state.json exist
        CatalogState.MANAGED

        >>> detect_state(Path("/with/only/catalog.json"))
        CatalogState.UNMANAGED_STAC
    """
    portolan_dir = path / ".portolan"
    config_file = portolan_dir / "config.json"
    state_file = portolan_dir / "state.json"
    root_catalog = path / "catalog.json"

    # Check for fully managed state first (both config AND state must exist)
    if config_file.exists() and state_file.exists():
        return CatalogState.MANAGED

    # Check for unmanaged STAC catalog (catalog.json at root, but not managed)
    if root_catalog.exists():
        return CatalogState.UNMANAGED_STAC

    # Everything else is fresh (including empty .portolan, partial .portolan, etc.)
    return CatalogState.FRESH


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


def write_catalog_json(catalog: CatalogModel, path: Path) -> Path:
    """Write CatalogModel to .portolan/catalog.json.

    Creates the .portolan directory if it doesn't exist.

    Args:
        catalog: CatalogModel to serialize.
        path: Root directory containing .portolan.

    Returns:
        Path to the written catalog.json file.
    """
    portolan_path = path / ".portolan"
    portolan_path.mkdir(parents=True, exist_ok=True)

    catalog_file = portolan_path / "catalog.json"
    data = catalog.to_dict()
    catalog_file.write_text(json.dumps(data, indent=2))

    return catalog_file


def read_catalog_json(path: Path) -> CatalogModel:
    """Read CatalogModel from .portolan/catalog.json.

    Args:
        path: Root directory containing .portolan.

    Returns:
        CatalogModel loaded from JSON.

    Raises:
        FileNotFoundError: If catalog.json doesn't exist.
    """
    catalog_file = path / ".portolan" / "catalog.json"
    data = json.loads(catalog_file.read_text())
    return CatalogModel.from_dict(data)


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
    2. .portolan/config.json (empty {} for now)
    3. .portolan/state.json (empty {} for now)
    4. .portolan/versions.json (minimal catalog-level versioning)
    5. catalog.json at ROOT level (valid STAC catalog via pystac) - LAST

    Write order ensures failed runs stay in FRESH state (retry-safe).

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
    # WRITE ORDER: state.json LAST to ensure atomic MANAGED transition
    # detect_state() checks for BOTH config.json AND state.json to determine
    # MANAGED state. Writing state.json last ensures that if init fails
    # partway through, the directory stays in FRESH state (retry-safe).
    # ─────────────────────────────────────────────────────────────────────────

    # Step 1: Create .portolan directory
    portolan_dir = path / ".portolan"
    try:
        portolan_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create .portolan directory: {e}") from e

    # Step 2: config.json - empty for now (first sentinel, not enough for MANAGED)
    try:
        (portolan_dir / "config.json").write_text("{}\n")
    except OSError as e:
        raise CatalogInitError(f"Cannot write config.json: {e}") from e

    # Step 3: versions.json - minimal catalog-level versioning
    # TODO: Catalog-level versions.json structure differs from ADR-0005's
    # collection-level spec. Consider creating ADR-0018 to document this.
    now = datetime.now(timezone.utc)
    versions_data = {
        "schema_version": "1.0.0",
        "catalog_id": catalog_id,
        "created": now.isoformat(),
        "collections": {},
    }
    try:
        (portolan_dir / "versions.json").write_text(json.dumps(versions_data, indent=2) + "\n")
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

    # Step 6: state.json - LAST (second sentinel, flips to MANAGED state)
    # This MUST be the final write. Once state.json exists alongside config.json,
    # detect_state() will report MANAGED. All other files must be in place first.
    try:
        (portolan_dir / "state.json").write_text("{}\n")
    except OSError as e:
        raise CatalogInitError(f"Cannot write state.json: {e}") from e

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
