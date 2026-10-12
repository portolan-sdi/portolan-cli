"""A plugin with a hook name that no hookspec declares."""

from __future__ import annotations

from typing import Any

from portolan_cli.hookspecs import hookimpl


@hookimpl
def portolan_enrich_itme(item: dict[str, Any]) -> None:
    """Misspelled on purpose."""
