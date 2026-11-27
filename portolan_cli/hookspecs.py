"""Hook specifications for Portolan plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pluggy

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.backends.protocol import VersioningBackend
    from portolan_cli.plugins.extractors import Extractor
    from portolan_cli.plugins.formats import FormatHandler

    BackendFactory = Callable[..., VersioningBackend]

PROJECT_NAME = "portolan"

hookspec = pluggy.HookspecMarker(PROJECT_NAME)
hookimpl = pluggy.HookimplMarker(PROJECT_NAME)


@dataclass(frozen=True)
class EnrichContext:
    """Where the STAC document that a hook enriches lives.

    Attributes:
        catalog_root: Directory that holds ``catalog.json``.
        collection_id: Collection identifier, nested ids joined with ``/``.
        document_path: Path of the item or collection JSON file.
        item_id: Item identifier. None for a collection.
    """

    catalog_root: Path
    collection_id: str
    document_path: Path
    item_id: str | None = None


@hookspec
def portolan_backends() -> dict[str, BackendFactory]:  # type: ignore[empty-body]
    """Return versioning backends by name.

    Returns:
        Mapping of backend name to factory.
    """


@hookspec
def portolan_formats() -> list[FormatHandler]:  # type: ignore[empty-body]
    """Return format handlers.

    Returns:
        Format handler objects.
    """


@hookspec
def portolan_enrich_item(item: dict[str, Any], context: EnrichContext) -> None:
    """Add STAC fields to an item before Portolan publishes it.

    Args:
        item: STAC item JSON.
        context: Where the item lives.
    """


@hookspec
def portolan_enrich_collection(collection: dict[str, Any], context: EnrichContext) -> None:
    """Add STAC fields to a collection before Portolan publishes it.

    The rules of ``portolan_enrich_item`` apply.

    Args:
        collection: STAC collection JSON.
        context: Where the collection lives.
    """


@hookspec
def portolan_extractors() -> list[Extractor]:  # type: ignore[empty-body]
    """Return data sources for ``portolan extract``.

    Returns:
        Extractors. A name that a built-in extractor uses is ignored.
    """
