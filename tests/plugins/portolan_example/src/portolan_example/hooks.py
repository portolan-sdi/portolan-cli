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


# --8<-- [start:enrich]
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
    # --8<-- [end:enrich]


# --8<-- [start:backends]
@hookimpl
def portolan_backends() -> dict[str, Callable[..., Any]]:
    """Register the ``example`` versioning backend."""
    from portolan_example.backend import ExampleBackend

    return {"example": ExampleBackend}
    # --8<-- [end:backends]


# --8<-- [start:formats]
@hookimpl
def portolan_formats() -> list[Any]:
    """Register the ``.xyz`` format handler."""
    from portolan_example.formats import XyzHandler

    return [XyzHandler()]
    # --8<-- [end:formats]


# --8<-- [start:extractors]
@hookimpl
def portolan_extractors() -> list[Any]:
    """Register the ``example`` extractor, a LayerSource over GeoJSON files."""
    from portolan_cli.api import extractor_from
    from portolan_example.geojson_source import GeoJSONDirectory

    return [extractor_from(GeoJSONDirectory())]
    # --8<-- [end:extractors]
