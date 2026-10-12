"""Hook specifications for Portolan plugins (#947).

A plugin is a module or object with functions marked ``@hookimpl``. A package
declares it in the ``portolan`` entry point group::

    [project.entry-points."portolan"]
    example = "portolan_example.hooks"

The plugin manager imports the module on the first hook call, never when the
CLI starts. Keep module-level imports light. Import a heavy library inside the
hook function.

This module and ``portolan_cli.api`` are the public plugin surface. Both follow
semantic versioning. A change that breaks a hook signature needs a major
release.

Deprecation policy: a hook or a hook argument stays for one minor release
after its deprecation. The hookspec carries ``warn_on_impl`` (the whole hook)
or ``warn_on_impl_args`` (one argument), so pluggy warns each plugin that
still implements it. The next major release removes it.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pluggy

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_cli.backends.protocol import VersioningBackend
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

    Each value is called as ``factory(catalog_root=path)`` and must return a
    ``VersioningBackend``. A class satisfies this. Import heavy dependencies
    inside the factory, so a catalog that does not use the backend does not
    pay for them. A catalog selects a backend with the ``backend`` setting.

    Returns:
        Mapping of backend name to factory.
    """


@hookspec
def portolan_formats() -> list[FormatHandler]:  # type: ignore[empty-body]
    """Return format handlers.

    A handler converts a source format to a cloud-native file and reads its
    metadata. See ``portolan_cli.api.FormatHandler``. For one file, a
    ``conversion.handlers`` pin wins, then a plugin handler, then a built-in
    handler.

    Returns:
        Format handler objects.
    """


@hookspec
def portolan_enrich_item(item: dict[str, Any], context: EnrichContext) -> None:
    """Add STAC fields to an item before Portolan publishes it.

    The hook receives the item JSON as a dict and changes it in place. It may
    add properties, ``stac_extensions`` entries, and asset fields. Portolan
    restores ``id``, ``type``, ``stac_version``, and ``links`` after the hook.

    The hook runs every time ``portolan add`` writes the item. It must give
    the same result when it runs twice.

    Portolan calls each implementation on its own. A hook wrapper
    (``wrapper=True`` or ``hookwrapper=True``) is skipped with a warning.

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
