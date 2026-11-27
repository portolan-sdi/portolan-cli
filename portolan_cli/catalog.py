"""Catalog management for Portolan."""

from __future__ import annotations

import contextlib
import json
import logging
import re
import sys
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, Self, overload

from portolan_cli.humanize import humanize_slug
from portolan_cli.input_hardening import validate_catalog_id
from portolan_cli.report import emit
from portolan_cli.utils import href_root, relative_href
from portolan_core.errors import CatalogAlreadyExistsError
from portolan_core.json_io import write_json_atomic, write_text_atomic
from portolan_core.models.catalog import CatalogModel
from portolan_core.stac_links import (
    contained_link_target,
    in_link_order,
    iter_links,
    iter_stac_objects,
    visible_stac_files,
)
from portolan_core.trace import count

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_core.run_state import RunState
    from portolan_core.versions import Version

# Cross-platform file locking (fcntl on Unix, msvcrt on Windows)
if sys.platform == "win32":
    import msvcrt

    def _lock_file(f: Any) -> None:
        """Lock file on Windows using msvcrt."""
        msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Windows using msvcrt."""
        # The unlock fails when the file was never locked.
        with contextlib.suppress(OSError):
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_file(f: Any) -> None:
        """Lock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)

    def _unlock_file(f: Any) -> None:
        """Unlock file on Unix using fcntl."""
        fcntl.flock(f.fileno(), fcntl.LOCK_UN)


class CatalogState(Enum):
    """The state of a directory with respect to Portolan catalog management."""

    MANAGED = "managed"
    UNMANAGED_STAC = "unmanaged_stac"
    FRESH = "fresh"


def detect_state(path: Path) -> CatalogState:
    """Detect the catalog state of a directory."""
    portolan_dir = path / ".portolan"
    config_file = portolan_dir / "config.yaml"
    root_catalog = path / "catalog.json"

    # Check for managed state first (config.yaml alone is sufficient)
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

    Args:
        start_path: Starting directory for search (defaults to cwd).
        require_operational: If True (default), require both .portolan/config.yaml and catalog.json. Pass False while init writes them.

    Returns:
        Path to catalog root if found, None otherwise.
    """
    from portolan_core.constants import MAX_CATALOG_SEARCH_DEPTH

    def _is_catalog_root(path: Path) -> bool:
        """Check if path is a valid catalog root."""
        config_yaml = path / ".portolan" / "config.yaml"
        if not config_yaml.exists():
            return False

        if not require_operational:
            # During init, config.yaml alone is sufficient
            return True

        # Require operational file: catalog.json at root (state.json removed)
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
    if depth < MAX_CATALOG_SEARCH_DEPTH and _is_catalog_root(current):
        return current

    return None


# Keep legacy exception for backward compatibility
class CatalogExistsError(Exception):
    """Raised when attempting to initialize a catalog that already exists."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"Catalog already exists at {path}")


def _sanitize_id(name: str) -> str:
    """Sanitize a string to be a valid STAC identifier."""
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


_TOOLING_ARTIFACT_IDS = frozenset(
    {
        "build",
        "data",
        "dist",
        "new",
        "out",
        "output",
        "publish",
        "staging",
        "temp",
        "tmp",
        "untitled",
        "extract",
        "arcgis-extract",
        "carto-extract",
        "services-extract",
        "wfs-extract",
    }
)


def _is_tooling_artifact_id(catalog_id: str) -> bool:
    """Report whether an id names a workflow step rather than the data."""
    return catalog_id.lower().replace("_", "-") in _TOOLING_ARTIFACT_IDS


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
    """Create a CatalogModel with auto-extracted and optional user-provided fields."""
    portolan_path = path / ".portolan"
    if portolan_path.exists():
        raise CatalogAlreadyExistsError(str(path))

    warnings: list[str] = []

    # Auto-extract id from directory name
    catalog_id = _sanitize_id(path.name)

    # Auto-generate timestamps
    now = datetime.now(UTC)

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
    """Write the catalog-level metadata.yaml carrying the license."""
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


def _resolve_catalog_identity(
    path: Path, catalog_id: str | None, title: str | None
) -> tuple[str, str, list[str]]:
    """Decide the catalog's id and title, and report what had to be guessed."""
    warnings: list[str] = []
    derived_id = catalog_id is None

    # path is already resolved, so path.name is the real directory name.
    catalog_id = _sanitize_id(path.name) if catalog_id is None else validate_catalog_id(catalog_id)

    if derived_id and _is_tooling_artifact_id(catalog_id):
        warnings.append(
            f"Derived catalog id '{catalog_id}' from the directory name. "
            "It looks like a tooling artifact, not the name of the data. "
            "Pass --id to set the catalog identity."
        )

    # A title is mandatory and human-readable, so derive one from the id.
    if not title:
        title = humanize_slug(catalog_id)
        source = "directory name" if derived_id else "catalog id"
        warnings.append(f"Derived catalog title '{title}' from {source}")

    return catalog_id, title, warnings


def _check_init_preconditions(
    path: Path,
    *,
    backend: str,
    logo: str | Path | None,
) -> None:
    """Reject an init that cannot succeed, before anything is written."""
    from portolan_core.errors import UnmanagedStacCatalogError

    state = detect_state(path)
    if state == CatalogState.MANAGED:
        raise CatalogAlreadyExistsError(str(path))
    if state == CatalogState.UNMANAGED_STAC:
        raise UnmanagedStacCatalogError(str(path))

    if logo is not None:
        from portolan_cli.logo import validate_logo_source

        validate_logo_source(logo)

    if backend != "file":
        import tempfile

        from portolan_cli.backends import get_backend

        # Probe in a scratch directory so the check writes nothing.
        try:
            with tempfile.TemporaryDirectory() as probe:
                get_backend(backend, catalog_root=Path(probe))
        except ValueError as e:
            raise CatalogInitError(str(e)) from e


def _write_conformance_files(
    path: Path,
    catalog_file: Path,
    *,
    logo: str | Path | None,
    logo_title: str | None,
) -> list[str]:
    """Write the files a catalog needs to conform, and copy the logo."""
    from portolan_cli.readme import ensure_readmes
    from portolan_core.agents_md import ensure_agents_md

    try:
        ensure_agents_md(catalog_file)
    except OSError as e:
        raise CatalogInitError(f"Cannot write AGENTS.md: {e}") from e

    try:
        ensure_schema_uris(path)
        ensure_readmes(path)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog conformance files: {e}") from e

    if logo is None:
        return []

    from portolan_cli.logo import set_catalog_logo

    try:
        return list(set_catalog_logo(path, logo, title=logo_title).warnings)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog logo: {e}") from e


class InitResult(NamedTuple):
    """The catalog that ``init_catalog`` created. It unpacks as ``(catalog_file, warnings)``."""

    catalog_file: Path
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the result as the ``init`` envelope data, read back from catalog.json."""
        from portolan_cli.logo import find_logo_link

        data = json.loads(self.catalog_file.read_text(encoding="utf-8"))
        logo_link = find_logo_link(data.get("links", []))
        return {
            "path": str(self.catalog_file.parent),
            "catalog_file": "catalog.json",
            "catalog_id": data.get("id", "unknown"),
            "logo": logo_link["href"] if logo_link else None,
            "warnings": self.warnings,
        }


def init_catalog(
    path: Path,
    *,
    catalog_id: str | None = None,
    title: str | None = None,
    description: str | None = None,
    backend: str = "file",
    license_id: str | None,
    license_url: str | None = None,
    logo: str | Path | None = None,
    logo_title: str | None = None,
) -> InitResult:
    """Initialize a new Portolan catalog with the v2 file structure."""
    import pystac

    # Resolve first: pystac normalize_hrefs drops the trailing slash of a relative root.
    path = Path(path).resolve()

    # Resolve the identity before anything touches the filesystem. A rejected
    # --id must leave no directory behind for the caller to clean up.
    catalog_id, title, identity_warnings = _resolve_catalog_identity(path, catalog_id, title)

    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create directory: {e}") from e

    _check_init_preconditions(path, backend=backend, logo=logo)

    warnings: list[str] = list(identity_warnings)

    if description is None:
        description = "A Portolan-managed STAC catalog"

    # config.yaml is written last: detect_state() reads it as MANAGED, so a failed init stays FRESH.

    # Step 1: Create .portolan directory
    portolan_dir = path / ".portolan"
    try:
        portolan_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise CatalogInitError(f"Cannot create .portolan directory: {e}") from e

    if backend == "file":
        now = datetime.now(UTC)
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
    catalog.normalize_hrefs(href_root(path))
    try:
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)
    except OSError as e:
        raise CatalogInitError(f"Cannot write catalog.json: {e}") from e

    # Steps 4b to 4c-bis: AGENTS.md, the schema URI, README.md, and the logo.
    warnings.extend(_write_conformance_files(path, catalog_file, logo=logo, logo_title=logo_title))

    warnings.extend(_seed_root_metadata(portolan_dir, license_id, license_url))

    # Step 5: config.yaml is the MANAGED sentinel and the user config file.
    config_content = "# Portolan configuration\n"
    if backend != "file":
        config_content = f"# Portolan configuration\nbackend: {backend}\n"
    try:
        write_text_atomic(portolan_dir / "config.yaml", config_content)
    except OSError as e:
        raise CatalogInitError(f"Cannot write config.yaml: {e}") from e

    return InitResult(catalog_file, warnings)


class Catalog:
    """A Portolan catalog backed by a .portolan directory.

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

        Args:
            root: The directory where the catalog should be created.
            license_id: SPDX identifier to seed into metadata.yaml. Left None, the catalog starts unlicensed and ``add`` asks for one.

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
    """Return the ancestor sub-catalog ids for a nested collection."""
    parts = collection_id.split("/")
    return ["/".join(parts[: i + 1]) for i in range(len(parts) - 1)]


def create_intermediate_catalogs(collection_id: str, catalog_root: Path) -> None:
    """Create intermediate catalog.json files for nested collection paths."""
    # intermediate_catalog_ids is shared with push discovery, so add and push agree.
    for intermediate_path in intermediate_catalog_ids(collection_id):
        catalog_dir = catalog_root / intermediate_path
        catalog_file = catalog_dir / "catalog.json"

        # Skip if already exists
        if catalog_file.exists():
            continue

        # Create directory if needed
        catalog_dir.mkdir(parents=True, exist_ok=True)

        root_href = relative_href(catalog_dir, catalog_root / "catalog.json")
        parent_href = relative_href(catalog_dir, catalog_dir.parent / "catalog.json")

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
        from portolan_core.agents_md import ensure_agents_md

        ensure_agents_md(catalog_file)


def update_catalog_links_for_nested(catalog_root: Path, collection_id: str) -> None:
    """Update catalog links for nested collection structure."""
    parts = collection_id.split("/")

    # For single-level collections, just ensure root links to collection
    if len(parts) == 1:
        _ensure_catalog_links_to_child(
            catalog_root / "catalog.json", f"./{parts[0]}/collection.json"
        )
        return

    # For nested collections:
    # 1. Root links to first-level catalog
    first_level = parts[0]
    _ensure_catalog_links_to_child(catalog_root / "catalog.json", f"./{first_level}/catalog.json")

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


def _ensure_catalog_links_to_child(catalog_file: Path, child_href: str) -> None:
    """Ensure a catalog file has a child link, in the order the links fixer writes."""
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
    content["links"] = in_link_order(links)
    write_json_atomic(catalog_file, content)


def _link_title_from_target(
    stac_file: Path, link: dict[str, object], catalog_root: Path
) -> str | None:
    """Read the human-readable title of a child/item link's target."""
    href = link.get("href")
    if not isinstance(href, str) or not href:
        return None

    # Input hardening: a crafted link cannot read a file outside the catalog.
    target = contained_link_target(catalog_root, stac_file.parent, href)
    if target is None or not target.exists():
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
    """Backfill ``title`` (and ``type``) on all child/item links."""
    changed_any = False

    for stac_file, content in iter_stac_objects(catalog_root):
        file_changed = False
        for link, _href, _path in iter_links(content, stac_file.parent, ("child", "item")):
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


#: metadata.yaml keys that a catalog copies onto its own catalog.json.
#: Kept apart from :data:`portolan_core.config.NON_INHERITED_METADATA_KEYS`, which
#: answers a different question. That constant names the keys a child never takes
#: from a parent. This one names the keys a catalog publishes about itself.
_CATALOG_HUMAN_FIELDS: tuple[str, ...] = ("title", "description")


def _apply_human_fields(
    catalog_path: Path,
    metadata: dict[str, Any],
    *,
    dry_run: bool = False,
) -> bool:
    """Copy the human-authored fields of ``metadata`` onto one catalog.json."""
    if not metadata:
        return False

    try:
        catalog_data = json.loads(catalog_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(catalog_data, dict):
        return False

    changed = False
    for field in _CATALOG_HUMAN_FIELDS:
        value = metadata.get(field)
        if not isinstance(value, str) or not value.strip():
            continue
        cleaned = value.strip()
        if catalog_data.get(field) == cleaned:
            continue
        catalog_data[field] = cleaned
        changed = True

    if changed and not dry_run:
        write_json_atomic(catalog_path, catalog_data)
    return changed


def apply_catalog_human_titles(
    catalog_root: Path, *, dry_run: bool = False, state: RunState | None = None
) -> list[Path]:
    """Apply each metadata.yaml title/description to its catalog.json."""
    from portolan_core.run_state import RunState

    state = state or RunState.open(catalog_root)
    changed: list[Path] = []
    for catalog_path in visible_stac_files(catalog_root):
        if catalog_path.name != "catalog.json":
            continue
        metadata = state.metadata(catalog_path.parent)
        if _apply_human_fields(catalog_path, metadata, dry_run=dry_run):
            changed.append(catalog_path)
    return changed


def ensure_schema_uris(catalog_root: Path) -> bool:
    """Declare the Portolan profile schema URI across a catalog tree."""
    from portolan_cli.stac import ensure_portolan_schema_uri

    changed_any = False

    for stac_file, content in iter_stac_objects(catalog_root):
        if ensure_portolan_schema_uri(content):
            write_json_atomic(stac_file, content)
            changed_any = True

    return changed_any


class CollectionSummary(NamedTuple):
    """The entry of one collection in the catalog-level ``versions.json``."""

    current_version: str
    asset_count: int
    total_size_bytes: int

    @classmethod
    def of(cls, version: Version) -> CollectionSummary:
        """Summarize a published collection version."""
        return cls(
            version.version,
            len(version.assets),
            sum(a.size_bytes for a in version.assets.values()),
        )


def _read_catalog_index(versions_path: Path) -> dict[str, Any]:
    """Read the catalog-level ``versions.json`` and check its shape."""
    try:
        count("versions_json.reads")
        content = json.loads(versions_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        msg = (
            f"Catalog versions.json is corrupted: {e}. "
            f"File: {versions_path}. "
            "Fix manually or delete to reinitialize."
        )
        emit("warn", msg)
        raise CatalogVersionsCorruptedError(msg) from e

    if not isinstance(content, dict):
        msg = (
            f"Catalog versions.json has invalid structure: expected dict, "
            f"got {type(content).__name__}. File: {versions_path}. "
            "Fix manually or delete to reinitialize."
        )
        emit("warn", msg)
        raise CatalogVersionsCorruptedError(msg)

    collections_value = content.get("collections")
    if collections_value is not None and not isinstance(collections_value, dict):
        msg = (
            f"Catalog versions.json 'collections' has invalid type: "
            f"expected dict or null, got {type(collections_value).__name__}. "
            f"File: {versions_path}. Fix manually or delete to reinitialize."
        )
        emit("warn", msg)
        raise CatalogVersionsCorruptedError(msg)
    return content


def collection_summaries(state: RunState) -> dict[str, CollectionSummary]:
    """Read file-backed collection summaries for recovery on an unchanged run."""
    from portolan_core.stac_links import versioned_collections

    summaries: dict[str, CollectionSummary] = {}
    if not (state.catalog_root / "versions.json").exists():
        return summaries
    for collection_id in versioned_collections(state.catalog_root):
        directory = state.collection_dir(collection_id)
        if state.backend(directory) != "file":
            continue
        versions = state.versions(directory, include_staged=False)
        if versions.versions:
            summaries[collection_id] = CollectionSummary.of(versions.versions[-1])
    return summaries


def update_catalog_index(
    catalog_root: Path,
    summaries: dict[str, CollectionSummary] | Callable[[], dict[str, CollectionSummary]],
) -> bool:
    """Write the entries of several collections into the catalog-level versions.json."""
    versions_path = catalog_root / "versions.json"
    if (isinstance(summaries, dict) and not summaries) or not versions_path.exists():
        # No catalog-level versions.json: not a file-backend catalog (Iceberg).
        return False

    lock_path = catalog_root / ".portolan" / ".versions.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    with lock_path.open("a") as lock_file:
        _lock_file(lock_file)
        try:
            if callable(summaries):
                summaries = summaries()
            if not summaries:
                return False
            content = _read_catalog_index(versions_path)
            collections = content.get("collections") or {}
            now = datetime.now(UTC).isoformat()
            changed = False
            for collection_id, summary in sorted(summaries.items()):
                entry = collections.get(collection_id)
                if isinstance(entry, dict) and summary == (
                    entry.get("current_version"),
                    entry.get("asset_count"),
                    entry.get("total_size_bytes"),
                ):
                    continue
                collections[collection_id] = {"updated": now, **summary._asdict()}
                changed = True
            if not changed:
                return False
            content["collections"] = collections
            content["updated"] = now
            write_json_atomic(versions_path, content)
            return True
        finally:
            _unlock_file(lock_file)


class CatalogVersionsCorruptedError(Exception):
    """Raised when catalog-level versions.json is corrupted."""


# Note: GEOSPATIAL_EXTENSIONS imported from portolan_core.constants
# Note: is_filegdb is imported from portolan_cli.scan.detect (canonical implementation).
