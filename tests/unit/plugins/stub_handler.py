"""A configurable format handler for the plugin system tests (#947)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path


@dataclass
class StubHandler:
    """A format handler that claims every file and converts nothing."""

    name: str
    extensions: frozenset[str]
    family: str = "vector"
    display_name: str = "Test"
    media_type: str = "application/octet-stream"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:
        return True

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        return source

    def read_metadata(self, path: Path) -> Any:
        return None
