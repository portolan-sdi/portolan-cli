"""The Extractor protocol and the registry of extractors (#947).

An extractor turns a data source into a Portolan catalog. ``portolan
extract <name>`` runs the Click command it supplies. A library caller runs
``extract()`` directly.

The ``portolan_extractors`` hook supplies extractors. The built-in ArcGIS,
Carto, and WFS extractors come through the same hook. A built-in name wins a
collision with a plugin.

This module imports no Click at runtime. ``cli_command()`` imports the
command module when the user runs the command.
"""

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
        """Return the Click command for ``portolan extract <name>``.

        Import the command module inside this method, so ``portolan --help``
        does not pay for it.
        """
        ...


def registered_extractors() -> dict[str, Extractor]:
    """Collect extractors from every ``portolan_extractors`` hook.

    Built-ins register first, so a built-in name keeps its built-in
    extractor. A result that is not an ``Extractor``, or a name that is
    already registered, produces a ``PluginWarning``.

    Returns:
        Extractors by name.
    """
    from portolan_cli.plugins.manager import PluginWarning, call_each

    found: dict[str, Extractor] = {}
    owners: dict[str, str] = {}
    for plugin_name, result in call_each("portolan_extractors"):
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
    return found
