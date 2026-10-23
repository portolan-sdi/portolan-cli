"""Format handlers: the protocols and the registry that picks one per file (#947).

A format handler turns a source file into a cloud-native file and reads the
metadata Portolan needs for STAC. Plugins add handlers through the
``portolan_formats`` hook. The built-in handlers come from the same hook.

Precedence for one file, first match wins:

1. ``conversion.handlers`` in ``.portolan/config.yaml`` pins a handler by name
   for an extension.
2. A plugin handler that claims the file. Two plugin handlers that claim one
   file raise ``AmbiguousFormatError`` until a pin decides.
3. A built-in handler that claims the file.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from portolan_cli.errors import AmbiguousFormatError, UnknownFormatHandlerError
from portolan_cli.format_families import VECTOR_EXTENSIONS

if TYPE_CHECKING:
    from pathlib import Path

BUILTIN_PREFIX = "builtin:"

# The manager the handler list came from, and the list.
_CACHE: list[Any] = []


@dataclass(frozen=True)
class FormatContext:
    """Where a file sits, for the handler methods that describe STAC.

    Attributes:
        catalog_root: Directory that holds ``catalog.json``.
        collection_dir: Directory that holds ``collection.json``.
        source: The file the operator added. None for a collection.
        defaults: The ``defaults`` section of the merged ``metadata.yaml``.
        merge_strategy: How ``add`` merges detected values into existing ones.
    """

    catalog_root: Path
    collection_dir: Path
    source: Path | None = None
    defaults: dict[str, Any] = field(default_factory=dict)
    merge_strategy: Any = None


@runtime_checkable
class FormatMetadata(Protocol):
    """What a handler reads from a converted file.

    Attributes:
        bbox: ``[min_x, min_y, max_x, max_y]`` in the file CRS.
        crs: The file CRS as an authority string, a PROJJSON dict, or None.
    """

    @property
    def bbox(self) -> Any:
        """Bounding box in the file CRS."""

    @property
    def crs(self) -> Any:
        """File CRS."""

    def to_stac_properties(self) -> dict[str, Any]:
        """Return the STAC item properties this format contributes."""


@runtime_checkable
class FormatHandler(Protocol):
    """Convert one file format to a cloud-native file and read its metadata.

    Attributes:
        name: Unique handler name, used by ``conversion.handlers``.
        family: Free text that groups formats, for example ``vector``.
        display_name: Name for messages, for example ``GeoParquet``.
        extensions: Lowercase source extensions with a dot, for example ``.xyz``.
        media_type: Media type of the converted file.
        asset_level: ``collection`` when a single file in a collection directory
            is a collection-level asset, ``item`` when each file is an item.
    """

    @property
    def name(self) -> str:
        """Unique handler name."""

    @property
    def family(self) -> str:
        """Format family."""

    @property
    def display_name(self) -> str:
        """Name for messages."""

    @property
    def extensions(self) -> frozenset[str]:
        """Source extensions."""

    @property
    def media_type(self) -> str:
        """Media type of the converted file."""

    @property
    def asset_level(self) -> str:
        """Either ``collection`` or ``item``."""

    def claims(self, path: Path) -> bool:
        """Return True when this handler reads ``path``. Read file contents if needed."""

    def output_path(self, source: Path, output_dir: Path) -> Path:
        """Return where ``convert`` writes the cloud-native file for ``source``.

        ``add --force`` keeps an output that exists at this path.
        """

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Write the cloud-native file into ``output_dir`` and return its path.

        Validate the result and raise ``InvalidOutputError`` when it fails.
        Write a thumbnail here when the format has one. ``add`` and
        ``check --fix`` both call this.
        """

    def read_metadata(self, path: Path) -> FormatMetadata:
        """Read the metadata of a converted file."""

    def describe_item(self, item: Any, output: Path, metadata: Any, context: FormatContext) -> None:
        """Add format fields to a new STAC item before Portolan saves it.

        ``item`` is a ``pystac.Item``. Change it in place: properties,
        assets, and ``stac_extensions``.
        """

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: FormatContext
    ) -> None:
        """Add format fields to a collection after an ``add``.

        ``collection`` is a ``pystac.Collection``. ``metadata`` holds what
        ``read_metadata`` returned for each file this ``add`` converted.
        """

    def inspect(self, path: Path, catalog_root: Path | None) -> Any:
        """Return a ``FileInfo`` for ``portolan info``. Raise ValueError if unsupported."""


# Optional handler methods, found with getattr:
#   partition(path, catalog_root) -> list[Path]
#       Split a converted file into Hive partitions. Return [] to keep it whole.
#   reoptimize(source, catalog_root) -> Path
#       Rewrite a cloud-native file for ``check --fix --force`` (issue #530).
#   reconvert(source, catalog_root) -> Path
#       Rewrite a file that is its own output for ``add --force --reconvert``.


def format_handlers() -> list[tuple[str, FormatHandler]]:
    """Return every format handler with the name of the plugin that gives it.

    A hook result that is not a list, or a list entry that is not a
    ``FormatHandler``, produces a ``PluginWarning`` and is dropped.

    The list is cached per plugin manager, because ``add`` and ``scan`` ask for
    a handler once per file. ``reset_plugin_manager`` makes a new manager, which
    drops the cache.

    Returns:
        ``(plugin_name, handler)`` pairs, built-ins first.
    """
    from portolan_cli.plugins.manager import PluginWarning, call_each, get_plugin_manager

    manager = get_plugin_manager()
    if _CACHE and _CACHE[0] is manager:
        return list(_CACHE[1])

    handlers: list[tuple[str, FormatHandler]] = []
    for plugin_name, result in call_each("portolan_formats"):
        if not isinstance(result, (list, tuple)):
            warnings.warn(
                f"Plugin '{plugin_name}' returned {type(result).__name__} from "
                "portolan_formats, which is not a list of format handlers.",
                PluginWarning,
                stacklevel=2,
            )
            continue
        for handler in result:
            if isinstance(handler, FormatHandler):
                handlers.append((plugin_name, handler))
            else:
                warnings.warn(
                    f"Plugin '{plugin_name}' returned {type(handler).__name__}, "
                    "which is not a FormatHandler.",
                    PluginWarning,
                    stacklevel=2,
                )
    _CACHE[:] = [manager, tuple(handlers)]
    return handlers


def handler_named(name: str) -> FormatHandler | None:
    """Return the handler with this ``name``, or None."""
    return next((handler for _, handler in format_handlers() if handler.name == name), None)


def is_builtin(plugin_name: str) -> bool:
    """True for the plugin name of a built-in hook module."""
    return plugin_name.startswith(BUILTIN_PREFIX)


def extension_of(path: Path) -> str:
    """Return the lowercase extension a handler is registered for."""
    name = path.name.lower()
    return ".copc.laz" if name.endswith(".copc.laz") else path.suffix.lower()


def handler_for(path: Path, catalog_root: Path | None = None) -> FormatHandler | None:
    """Pick the format handler for ``path``.

    Args:
        path: Source file or FileGDB directory.
        catalog_root: Catalog root, to read ``conversion.handlers``. None reads
            no pin.

    Returns:
        The handler, or None when no handler claims the file.

    Raises:
        AmbiguousFormatError: Two plugin handlers claim the file and no pin decides.
        UnknownFormatHandlerError: A pin names a handler that does not exist.
    """
    selected = select_handler(path, catalog_root)
    return selected[1] if selected else None


def plugin_handler_for(path: Path, catalog_root: Path | None = None) -> FormatHandler | None:
    """Like ``handler_for``, but return only a handler from an outside plugin."""
    selected = select_handler(path, catalog_root)
    return selected[1] if selected and not is_builtin(selected[0]) else None


def select_handler(
    path: Path, catalog_root: Path | None = None
) -> tuple[str, FormatHandler] | None:
    """Pick the format handler for ``path`` and the plugin that gives it.

    See ``handler_for`` for the precedence and the errors.
    """
    extension = extension_of(path)
    handlers = format_handlers()

    if catalog_root is not None:
        from portolan_cli.conversion_config import get_conversion_overrides, get_handler_pins

        pinned = get_handler_pins(catalog_root).get(extension)
        # conversion.extensions.convert sends a cloud-native vector, such as a
        # FlatGeobuf, to GeoParquet. Its own handler only copies it (#1020). A
        # listed raster keeps the COG handler, which converts it again.
        if (
            pinned is None
            and extension in VECTOR_EXTENSIONS
            and extension in get_conversion_overrides(catalog_root).extensions_convert
        ):
            pinned = "geoparquet"
        if pinned is not None:
            for plugin_name, handler in handlers:
                if handler.name == pinned:
                    return plugin_name, handler
            raise UnknownFormatHandlerError(
                extension, pinned, sorted({handler.name for _, handler in handlers})
            )

    claiming = [
        (plugin_name, handler)
        for plugin_name, handler in handlers
        if extension in handler.extensions and _claims(plugin_name, handler, path)
    ]
    plugins = [(name, handler) for name, handler in claiming if not is_builtin(name)]
    if len(plugins) > 1:
        raise AmbiguousFormatError(str(path), extension, [handler.name for _, handler in plugins])
    if plugins:
        return plugins[0]
    return claiming[0] if claiming else None


def _claims(plugin_name: str, handler: FormatHandler, path: Path) -> bool:
    """Ask ``handler`` to claim ``path``. A plugin that raises claims nothing.

    One broken plugin must not stop ``scan``, ``check``, or ``add`` for every
    file with its extension, so the error becomes a ``PluginWarning``.
    """
    if is_builtin(plugin_name):
        return handler.claims(path)
    try:
        return handler.claims(path)
    except Exception as err:  # noqa: BLE001 - one broken plugin must not stop the CLI
        from portolan_cli.plugins.manager import warn_hook_failure

        warn_hook_failure(plugin_name, f"{handler.name}.claims", err)
        return False


def builtin_extensions() -> frozenset[str]:
    """Return the extensions that the built-in format handlers register."""
    return frozenset(
        extension
        for plugin_name, handler in format_handlers()
        if is_builtin(plugin_name)
        for extension in handler.extensions
    )


def plugin_extensions() -> frozenset[str]:
    """Return the extensions that outside plugin handlers register."""
    return frozenset(
        extension
        for plugin_name, handler in format_handlers()
        if not is_builtin(plugin_name)
        for extension in handler.extensions
    )
