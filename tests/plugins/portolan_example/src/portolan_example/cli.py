"""The ``portolan example`` command group.

Portolan imports this module only when the user runs ``portolan example``.
"""

from __future__ import annotations

from pathlib import Path

import click

# --8<-- [start:command]
from portolan_cli.api import info, success


@click.group()
def group() -> None:
    """Commands from the example plugin."""


@group.command()
@click.argument("collection")
@click.option(
    "--catalog",
    "catalog_root",
    type=click.Path(file_okay=False, path_type=Path),
    default=Path(),
    show_default=True,
    help="Catalog root directory.",
)
def versions(collection: str, catalog_root: Path) -> None:
    """List the versions the example backend holds for COLLECTION."""
    from portolan_example.backend import ExampleBackend

    history = ExampleBackend(catalog_root=catalog_root).list_versions(collection)
    if not history:
        info(f"No versions for {collection}.")
        return
    for version in history:
        success(f"{version.version}  {version.message}")
    # --8<-- [end:command]
