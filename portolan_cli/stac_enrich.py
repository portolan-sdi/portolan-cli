"""Run the STAC enrichment hooks on a document on disk (#947).

``portolan_enrich_item`` and ``portolan_enrich_collection`` change a STAC
document before Portolan publishes it. The hooks run on the JSON file after
Portolan writes it, because ``pystac.Collection.save`` serializes every item
again from memory. A change made to an in-memory object before that save
would be lost. pystac keeps unknown fields on a round trip, so a field a
hook adds survives a later save.

A hook may add fields. It may not change the fields that hold the catalog
together. Portolan restores ``id``, ``type``, ``stac_version``, and ``links``
after each hook and warns. A hook that raises leaves no partial change.
"""

from __future__ import annotations

import copy
import json
import warnings
from typing import TYPE_CHECKING, Any

from portolan_cli.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.hookspecs import EnrichContext

STRUCTURAL_FIELDS = ("id", "type", "stac_version", "links")


def enrich_item_file(path: Path, context: EnrichContext) -> None:
    """Run ``portolan_enrich_item`` on the item JSON at ``path``.

    Args:
        path: Item JSON file.
        context: Where the item lives.
    """
    _enrich_file(path, "portolan_enrich_item", "item", context)


def enrich_collection_file(path: Path, context: EnrichContext) -> None:
    """Run ``portolan_enrich_collection`` on the collection JSON at ``path``.

    Args:
        path: Collection JSON file.
        context: Where the collection lives.
    """
    _enrich_file(path, "portolan_enrich_collection", "collection", context)


def _enrich_file(path: Path, hook_name: str, arg_name: str, context: EnrichContext) -> None:
    from portolan_cli.plugins.manager import PluginWarning, hook_calls, warn_hook_failure

    calls = hook_calls(hook_name)
    if not calls:
        return

    original: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    document = copy.deepcopy(original)
    for plugin_name, call in calls:
        before = copy.deepcopy(document)
        try:
            call(**{arg_name: document, "context": context})
        except Exception as err:  # noqa: BLE001 - one broken plugin must not stop the add
            warn_hook_failure(plugin_name, hook_name, err)
            document = before
            continue
        changed = [key for key in STRUCTURAL_FIELDS if document.get(key) != before.get(key)]
        if changed:
            for key in changed:
                if key in before:
                    document[key] = before[key]
                else:
                    document.pop(key, None)
            warnings.warn(
                f"Plugin '{plugin_name}' changed {', '.join(changed)} in {path.name}. "
                "Portolan restored them.",
                PluginWarning,
                stacklevel=2,
            )

    if document != original:
        write_json_atomic(path, document)
