"""Catalog management for Portolan.

The Catalog class is the primary interface for working with Portolan catalogs.
It wraps all catalog operations as methods, following ADR-0007 (CLI wraps API).

This module also provides library functions for catalog creation:
- create_catalog(): Create a CatalogModel with auto-extracted fields
- write_catalog_json(): Serialize CatalogModel to .portolan/catalog.json
- read_catalog_json(): Load CatalogModel from .portolan/catalog.json
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal, overload

from portolan_cli.errors import CatalogAlreadyExistsError
from portolan_cli.models.catalog import CatalogModel

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


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
    auto: bool = False,
    return_warnings: Literal[False] = False,
) -> CatalogModel: ...


@overload
def create_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    auto: bool = False,
    return_warnings: Literal[True],
) -> tuple[CatalogModel, list[str]]: ...


def create_catalog(
    path: Path,
    *,
    title: str | None = None,
    description: str | None = None,
    auto: bool = False,
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
        auto: If True, skip prompts and use defaults.
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


class Catalog:
    """A Portolan catalog backed by a .portolan directory.

    The Catalog class provides the Python API for all catalog operations.
    The CLI commands are thin wrappers around these methods.

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
        """Path to the catalog.json file."""
        return self.portolan_path / self.CATALOG_FILE

    @classmethod
    def init(cls, root: Path) -> Self:
        """Initialize a new Portolan catalog.

        Creates the .portolan directory and a minimal STAC catalog.json file.

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

        # Create the .portolan directory
        portolan_path.mkdir(parents=True)

        # Create minimal STAC catalog
        catalog_data = {
            "type": "Catalog",
            "stac_version": cls.STAC_VERSION,
            "id": "portolan-catalog",
            "description": "A Portolan-managed STAC catalog",
            "links": [],
        }

        catalog_file = portolan_path / cls.CATALOG_FILE
        catalog_file.write_text(json.dumps(catalog_data, indent=2))

        return cls(root)
