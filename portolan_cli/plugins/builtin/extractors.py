"""The built-in ArcGIS, Carto, and WFS extractors.

Each one wraps the catalog function in ``extract/<source>/orchestrator.py``
and the Click command in ``extract/<source>/cli.py``. Both load on first use.
"""

from __future__ import annotations

import importlib
from typing import TYPE_CHECKING, Any

from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from pathlib import Path

    import click

    from portolan_cli.extract.common.report import ExtractionReport


class BuiltinExtractor:
    """An extractor whose code is in ``portolan_cli.extract.<source>``."""

    def __init__(self, name: str, summary: str, function: str, command: str) -> None:
        self.name = name
        self.summary = summary
        self._function = function
        self._command = command

    def extract(self, source: str, output_dir: Path, **options: Any) -> ExtractionReport:
        """Run ``extract_<source>_catalog`` from the source's orchestrator."""
        module = importlib.import_module(f"portolan_cli.extract.{self.name}.orchestrator")
        result: ExtractionReport = getattr(module, self._function)(source, output_dir, **options)
        return result

    def cli_command(self) -> click.Command:
        """Return the command from ``portolan_cli.extract.<source>.cli``."""
        module = importlib.import_module(f"portolan_cli.extract.{self.name}.cli")
        command: click.Command = getattr(module, self._command)
        return command


@hookimpl
def portolan_extractors() -> list[BuiltinExtractor]:
    """Register the built-in extractors."""
    return [
        BuiltinExtractor(
            "arcgis",
            "ArcGIS FeatureServer, MapServer, and ImageServer services.",
            "extract_arcgis_catalog",
            "extract_arcgis_cmd",
        ),
        BuiltinExtractor(
            "carto", "Carto SQL API tables.", "extract_carto_catalog", "extract_carto_cmd"
        ),
        BuiltinExtractor(
            "wfs", "OGC Web Feature Service layers.", "extract_wfs_catalog", "extract_wfs_cmd"
        ),
    ]
