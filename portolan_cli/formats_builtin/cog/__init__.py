"""Raster sources to Cloud-Optimized GeoTIFF through rio-cogeo (#947)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli import api

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class CogHandler:
    """Convert raster sources to COG. A source that is already a COG is kept."""

    name: str = "cog"
    family: str = "raster"
    display_name: str = "COG"
    extensions: frozenset[str] = frozenset()
    media_type: str = "image/tiff; application=geotiff; profile=cloud-optimized"
    asset_level: str = "item"

    def claims(self, path: Path) -> bool:  # noqa: ARG002 - the protocol fixes it
        """Claim every raster extension."""
        return True

    def output_path(self, source: Path, output_dir: Path) -> Path:
        """Return ``<stem>.tif`` in ``output_dir``."""
        return output_dir / f"{source.stem}.tif"

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Convert, validate, and write a thumbnail.

        The thumbnail is written only when the COG has none. A thumbnail that
        a publisher made survives a re-add.
        """
        output = api.convert_raster(source, output_dir, catalog_root)
        self._check(output)
        api.write_raster_thumbnail(output, catalog_root)
        return output

    def reoptimize(self, source: Path, catalog_root: Path | None) -> Path:
        """Rewrite a COG with the current settings (issue #530)."""
        output = api.convert_raster(source, source.parent, catalog_root, reoptimize=True)
        self._check(output)
        api.write_raster_thumbnail(output, catalog_root)
        return output

    def read_metadata(self, path: Path) -> Any:
        """Read COG metadata."""
        return api.extract_cog_metadata(path)

    def describe_item(
        self, item: Any, output: Path, metadata: Any, context: api.FormatContext
    ) -> None:
        """Add band statistics, nodata defaults, render fields, and the raster extension."""
        api.describe_raster_item(item, output, metadata, context)

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: api.FormatContext
    ) -> None:
        """A COG collection gets no collection-level format fields."""

    def inspect(self, path: Path, catalog_root: Path | None) -> Any:
        """Describe a COG for ``portolan info``."""
        return api.inspect_cog(path, catalog_root=catalog_root)

    @staticmethod
    def _check(output: Path) -> None:
        error = api.validate_cog(output)
        if error is not None:
            raise api.InvalidOutputError(str(output), error)


def make_handler() -> CogHandler:
    """Build the handler with every raster extension."""
    return CogHandler(extensions=frozenset(api.RASTER_EXTENSIONS))
