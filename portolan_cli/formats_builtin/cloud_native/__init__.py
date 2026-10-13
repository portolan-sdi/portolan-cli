"""PMTiles and FlatGeobuf: cloud-native vectors that Portolan tracks as they are (#947).

Both formats keep their file. Neither adds item or collection fields beyond
what its metadata reader returns.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli import api

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class CloudNativeVectorHandler:
    """Copy a cloud-native vector file into place and read its header.

    ``reader`` names the ``portolan_cli.api`` function that reads the header.
    """

    name: str
    display_name: str
    extensions: frozenset[str]
    media_type: str
    reader: str
    family: str = "vector"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:  # noqa: ARG002 - the protocol fixes it
        """Claim every file with this handler's extension."""
        return True

    def output_path(self, source: Path, output_dir: Path) -> Path:
        """Keep the source name."""
        return output_dir / source.name

    def convert(
        self,
        source: Path,
        output_dir: Path,
        catalog_root: Path | None,  # noqa: ARG002 - the protocol fixes it
    ) -> Path:
        """Copy the file unless it is in place already."""
        target = self.output_path(source, output_dir)
        if target.resolve() != source.resolve():
            shutil.copy2(source, target)
        return target

    def read_metadata(self, path: Path) -> Any:
        """Read the file header."""
        reader: Any = getattr(api, self.reader)
        return reader(path)

    def describe_item(
        self, item: Any, output: Path, metadata: Any, context: api.FormatContext
    ) -> None:
        """A cloud-native vector adds no item fields."""

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: api.FormatContext
    ) -> None:
        """A cloud-native vector adds no collection fields."""

    def inspect(
        self,
        path: Path,
        catalog_root: Path | None,  # noqa: ARG002 - the protocol fixes it
    ) -> Any:
        """``portolan info`` does not read this format."""
        raise ValueError(f"Unsupported file format: {path.suffix}")


def make_handlers() -> list[CloudNativeVectorHandler]:
    """Build the PMTiles and FlatGeobuf handlers."""
    return [
        CloudNativeVectorHandler(
            name="pmtiles",
            display_name="PMTiles",
            extensions=frozenset({".pmtiles"}),
            media_type="application/vnd.pmtiles",
            reader="extract_pmtiles_metadata",
        ),
        CloudNativeVectorHandler(
            name="flatgeobuf",
            display_name="FlatGeobuf",
            extensions=frozenset({".fgb", ".flatgeobuf"}),
            media_type="application/vnd.flatgeobuf",
            reader="extract_flatgeobuf_metadata",
        ),
    ]
