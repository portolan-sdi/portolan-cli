"""Catalog management for Portolan.

The Catalog class is the primary interface for working with Portolan catalogs.
It wraps all catalog operations as methods, following ADR-0007 (CLI wraps API).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class CatalogExistsError(Exception):
    """Raised when attempting to initialize a catalog that already exists."""

    def __init__(self, path: Path) -> None:
        self.path = path
        super().__init__(f"Catalog already exists at {path}")


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
