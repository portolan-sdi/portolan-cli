"""Portolan CLI - Command-line interface for managing cloud-native geospatial data.

The CLI is a thin wrapper around the Python API (see catalog.py).
All business logic lives in the library; the CLI handles user interaction.
"""

from __future__ import annotations

from pathlib import Path

import click

from portolan_cli.catalog import Catalog, CatalogExistsError
from portolan_cli.output import error, success


@click.group()
@click.version_option()
def cli() -> None:
    """Portolan - Publish and manage cloud-native geospatial data catalogs."""
    pass


@cli.command()
@click.argument("path", type=click.Path(path_type=Path), default=".")
def init(path: Path) -> None:
    """Initialize a new Portolan catalog.

    Creates a .portolan directory with a STAC catalog.json file.

    PATH is the directory where the catalog should be created (default: current directory).
    """
    try:
        Catalog.init(path)
        success(f"Initialized Portolan catalog in {path.resolve()}")
    except CatalogExistsError as err:
        error(f"Catalog already exists at {path.resolve()}")
        raise SystemExit(1) from err
