"""A plugin with one valid hook and one hook with an unknown argument."""

from __future__ import annotations

from typing import Any

from portolan_cli.hookspecs import hookimpl


@hookimpl
def portolan_backends() -> dict[str, Any]:
    """Register a backend that must not survive the failed registration."""
    from portolan_cli.backends.json_file import JsonFileBackend

    return {"half": JsonFileBackend}


@hookimpl
def portolan_enrich_item(item: dict[str, Any], context: Any, bogus: Any) -> None:
    """Take an argument the hookspec does not declare."""
    del bogus
