"""Hook implementations of the example plugin.

Module-level imports stay light. A hook imports heavy code inside its body.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from portolan_cli.api import EnrichContext, hookimpl

if TYPE_CHECKING:
    from collections.abc import Callable

SCI_EXTENSION = "https://stac-extensions.github.io/scientific/v1.0.0/schema.json"
DOI_PREFIX = "10.1000/portolan-example"
CITATION = "Example Agency (2026). Example collections. Portolan example plugin."


def _declare_sci(document: dict[str, Any]) -> None:
    extensions: list[str] = document.setdefault("stac_extensions", [])
    if SCI_EXTENSION not in extensions:
        extensions.append(SCI_EXTENSION)


@hookimpl
def portolan_enrich_item(item: dict[str, Any], context: EnrichContext) -> None:
    """Give each item a DOI built from its collection and item ids."""
    _declare_sci(item)
    item.setdefault("properties", {})["sci:doi"] = (
        f"{DOI_PREFIX}/{context.collection_id}/{context.item_id}"
    )


@hookimpl
def portolan_enrich_collection(collection: dict[str, Any], context: EnrichContext) -> None:
    """Give each collection a citation."""
    _declare_sci(collection)
    collection["sci:citation"] = CITATION


@hookimpl
def portolan_backends() -> dict[str, Callable[..., Any]]:
    """Register the ``example`` versioning backend."""
    from portolan_example.backend import ExampleBackend

    return {"example": ExampleBackend}
