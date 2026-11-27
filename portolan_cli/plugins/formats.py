"""Format handlers: the protocols and the registry that picks one per file."""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from portolan_core import extension_registry as _reg
from portolan_core.errors import AmbiguousFormatError, UnknownFormatHandlerError

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_core.config import ConfigSource

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
        state: The run state of the command, or None. A handler passes it to
    """

    catalog_root: Path
    collection_dir: Path
    source: Path | None = None
    defaults: dict[str, Any] = field(default_factory=dict)
    merge_strategy: Any = None
    state: ConfigSource | None = None


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
        """Write the cloud-native file into ``output_dir`` and return its path."""

    def read_metadata(self, path: Path) -> FormatMetadata:
        """Read the metadata of a converted file."""

    def describe_item(self, item: Any, output: Path, metadata: Any, context: FormatContext) -> None:
        """Add format fields to a new STAC item before Portolan saves it."""

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: FormatContext
    ) -> None:
        """Add format fields to a collection after an ``add``."""

    def inspect(self, path: Path, catalog_root: Path | None) -> Any:
        """Return a ``FileInfo`` for ``portolan info``. Raise ValueError if unsupported."""


def format_handlers() -> list[tuple[str, FormatHandler]]:
    """Return every format handler with the name of the plugin that gives it."""
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


def handler_for(
    path: Path, catalog_root: Path | None = None, *, state: ConfigSource | None = None
) -> FormatHandler | None:
    """Pick the format handler for ``path``."""
    selected = select_handler(path, catalog_root, state=state)
    return selected[1] if selected else None


def plugin_handler_for(
    path: Path, catalog_root: Path | None = None, *, state: ConfigSource | None = None
) -> FormatHandler | None:
    """Like ``handler_for``, but return only a handler from an outside plugin."""
    selected = select_handler(path, catalog_root, state=state)
    return selected[1] if selected and not is_builtin(selected[0]) else None


def select_handler(
    path: Path, catalog_root: Path | None = None, *, state: ConfigSource | None = None
) -> tuple[str, FormatHandler] | None:
    """Pick the format handler for ``path`` and the plugin that gives it.

    Raises AmbiguousFormatError when two plugins claim it unpinned, UnknownFormatHandlerError for a bad pin.
    """
    extension = extension_of(path)
    handlers = format_handlers()

    if catalog_root is not None:
        from portolan_cli.conversion_config import get_conversion_overrides, get_handler_pins

        pinned = get_handler_pins(catalog_root, state=state).get(extension)
        if (
            pinned is None
            and extension in _reg.convert_on_request_extensions("GeoParquet")
            and extension in get_conversion_overrides(catalog_root, state=state).extensions_convert
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
    """Ask ``handler`` to claim ``path``. A plugin that raises claims nothing."""
    if is_builtin(plugin_name):
        return handler.claims(path)
    try:
        return handler.claims(path)
    except Exception as err:  # one broken plugin must not stop the CLI
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
