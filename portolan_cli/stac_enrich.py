"""Run the STAC enrichment hooks on a document on disk."""

from __future__ import annotations

import copy
import json
import warnings
from typing import TYPE_CHECKING, Any

from portolan_core.json_io import write_json_atomic

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.hookspecs import EnrichContext

STRUCTURAL_FIELDS = ("id", "type", "stac_version", "links")


def enrich_item_file(path: Path, context: EnrichContext) -> None:
    """Run ``portolan_enrich_item`` on the item JSON at ``path``."""
    _enrich_file(path, "portolan_enrich_item", "item", context)


def enrich_collection_file(path: Path, context: EnrichContext) -> None:
    """Run ``portolan_enrich_collection`` on the collection JSON at ``path``."""
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
        except Exception as err:  # one broken plugin must not stop the add
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
