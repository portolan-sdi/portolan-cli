"""The Extractor protocol and the registry of extractors."""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pathlib import Path

    import click

    from portolan_cli.extract.common.report import ExtractionReport


@runtime_checkable
class Extractor(Protocol):
    """A source of data that Portolan can extract into a catalog.

    Attributes:
        name: The subcommand name, ``portolan extract <name>``.
        summary: One line that describes the source.
    """

    name: str
    summary: str

    def extract(self, source: str, output_dir: Path, **options: Any) -> ExtractionReport:
        """Extract ``source`` into a catalog in ``output_dir``.

        Args:
            source: Service URL or other source locator.
            output_dir: Directory of the catalog to write.
            **options: Source-specific options.

        Returns:
            The extraction report.
        """
        ...

    def cli_command(self) -> click.Command:
        """Return the Click command for ``portolan extract <name>``."""
        ...


# The plugin manager, the extractors by name, and their plugin names.
# ``reset_plugin_manager`` makes a new manager, which drops the cache.
_CACHE: list[Any] = []


def registered_extractors() -> dict[str, Extractor]:
    """Collect extractors from every ``portolan_extractors`` hook."""
    from portolan_cli.plugins.manager import PluginWarning, call_each, get_plugin_manager

    manager = get_plugin_manager()
    if _CACHE and _CACHE[0] is manager:
        return dict(_CACHE[1])

    found: dict[str, Extractor] = {}
    owners: dict[str, str] = {}
    for plugin_name, result in call_each("portolan_extractors"):
        if not isinstance(result, (list, tuple)):
            warnings.warn(
                f"Plugin '{plugin_name}' returned {type(result).__name__} from "
                "portolan_extractors, which is not a list of extractors.",
                PluginWarning,
                stacklevel=2,
            )
            continue
        for extractor in result:
            if not isinstance(extractor, Extractor):
                warnings.warn(
                    f"Plugin '{plugin_name}' returned {type(extractor).__name__}, which is "
                    "not an Extractor. Portolan ignored it.",
                    PluginWarning,
                    stacklevel=2,
                )
                continue
            if extractor.name in found:
                warnings.warn(
                    f"Plugin '{plugin_name}' registers extractor '{extractor.name}', which "
                    f"'{owners[extractor.name]}' already provides. The first registration stays.",
                    PluginWarning,
                    stacklevel=2,
                )
                continue
            found[extractor.name] = extractor
            owners[extractor.name] = plugin_name
    _CACHE[:] = [manager, dict(found), owners]
    return found


def extractor_owner(name: str) -> str | None:
    """Return the name of the plugin that registers extractor ``name``."""
    registered_extractors()
    owners: dict[str, str] = _CACHE[2]
    return owners.get(name)
