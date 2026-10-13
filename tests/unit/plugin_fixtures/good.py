"""A plugin that adds one item property."""

from __future__ import annotations

from typing import Any

from portolan_cli.hookspecs import EnrichContext, hookimpl


@hookimpl
def portolan_enrich_item(item: dict[str, Any], context: EnrichContext) -> None:
    """Mark the item."""
    item.setdefault("properties", {})["fixture:good"] = context.collection_id
