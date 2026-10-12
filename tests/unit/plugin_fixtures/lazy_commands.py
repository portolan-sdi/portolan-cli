"""A command module whose import the help test watches for."""

from __future__ import annotations

import click


@click.command()
def lazy() -> None:
    """Never shown in help."""
