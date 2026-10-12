"""The built-in format handlers (#947).

GeoParquet through geoparquet-io, COG through rio-cogeo, and the two
cloud-native vector formats that Portolan tracks as they are: PMTiles and
FlatGeobuf. Each handler imports its libraries inside its methods, so the hook
call stays cheap.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.format_families import RASTER, RASTER_EXTENSIONS, VECTOR, VECTOR_EXTENSIONS
from portolan_cli.hookspecs import hookimpl

if TYPE_CHECKING:
    from pathlib import Path

    from portolan_cli.plugins.formats import FormatHandler

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"
_PMTILES = frozenset({".pmtiles"})
_FLATGEOBUF = frozenset({".fgb", ".flatgeobuf"})


@dataclass(frozen=True)
class GeoParquetHandler:
    """Vector sources to GeoParquet through geoparquet-io."""

    name: str = "geoparquet"
    family: str = VECTOR
    display_name: str = "GeoParquet"
    extensions: frozenset[str] = (VECTOR_EXTENSIONS - _PMTILES - _FLATGEOBUF) | {".json"}
    media_type: str = PARQUET_MEDIA_TYPE
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:
        """Claim a vector file. A ``.json`` file must look like GeoJSON."""
        from portolan_cli.format_families import is_geojson

        if path.suffix.lower() == ".json":
            return path.is_file() and is_geojson(path)
        return True

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Convert with the catalog's vector settings."""
        from portolan_cli.conversion_config import VectorSettings, get_vector_settings
        from portolan_cli.convert import _convert_vector

        settings = get_vector_settings(catalog_root) if catalog_root else VectorSettings()
        return _convert_vector(source, output_dir, settings)

    def read_metadata(self, path: Path) -> Any:
        """Read GeoParquet metadata."""
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        return extract_geoparquet_metadata(path)


@dataclass(frozen=True)
class CogHandler:
    """Raster sources to Cloud-Optimized GeoTIFF through rio-cogeo."""

    name: str = "cog"
    family: str = RASTER
    display_name: str = "COG"
    extensions: frozenset[str] = RASTER_EXTENSIONS
    media_type: str = COG_MEDIA_TYPE
    asset_level: str = "item"

    def claims(self, path: Path) -> bool:  # noqa: ARG002 - the protocol fixes it
        """Claim every raster extension."""
        return True

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Convert with the catalog's COG settings."""
        from portolan_cli.conversion_config import CogSettings, get_cog_settings
        from portolan_cli.convert import _convert_raster

        settings = get_cog_settings(catalog_root) if catalog_root else CogSettings()
        return _convert_raster(source, output_dir, settings)

    def read_metadata(self, path: Path) -> Any:
        """Read COG metadata."""
        from portolan_cli.metadata.cog import extract_cog_metadata

        return extract_cog_metadata(path)


def _copy_into(source: Path, output_dir: Path) -> Path:
    """Copy a cloud-native file into ``output_dir`` unless it is there already."""
    target = output_dir / source.name
    if target.resolve() != source.resolve():
        shutil.copy2(source, target)
    return target


@dataclass(frozen=True)
class PMTilesHandler:
    """PMTiles, tracked as they are."""

    name: str = "pmtiles"
    family: str = VECTOR
    display_name: str = "PMTiles"
    extensions: frozenset[str] = _PMTILES
    media_type: str = "application/vnd.pmtiles"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:  # noqa: ARG002 - the protocol fixes it
        """Claim every PMTiles file."""
        return True

    def convert(
        self,
        source: Path,
        output_dir: Path,
        catalog_root: Path | None,  # noqa: ARG002 - the protocol fixes it
    ) -> Path:
        """Copy the file. PMTiles is cloud-native."""
        return _copy_into(source, output_dir)

    def read_metadata(self, path: Path) -> Any:
        """Read the PMTiles header."""
        from portolan_cli.metadata.pmtiles import extract_pmtiles_metadata

        return extract_pmtiles_metadata(path)


@dataclass(frozen=True)
class FlatGeobufHandler:
    """FlatGeobuf, tracked as it is."""

    name: str = "flatgeobuf"
    family: str = VECTOR
    display_name: str = "FlatGeobuf"
    extensions: frozenset[str] = _FLATGEOBUF
    media_type: str = "application/vnd.flatgeobuf"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:  # noqa: ARG002 - the protocol fixes it
        """Claim every FlatGeobuf file."""
        return True

    def convert(
        self,
        source: Path,
        output_dir: Path,
        catalog_root: Path | None,  # noqa: ARG002 - the protocol fixes it
    ) -> Path:
        """Copy the file. FlatGeobuf is cloud-native."""
        return _copy_into(source, output_dir)

    def read_metadata(self, path: Path) -> Any:
        """Read the FlatGeobuf header."""
        from portolan_cli.metadata.flatgeobuf import extract_flatgeobuf_metadata

        return extract_flatgeobuf_metadata(path)


GEOPARQUET = GeoParquetHandler()
COG = CogHandler()
PMTILES = PMTilesHandler()
FLATGEOBUF = FlatGeobufHandler()

BUILTIN_HANDLERS = (GEOPARQUET, COG, PMTILES, FLATGEOBUF)


@hookimpl
def portolan_formats() -> list[FormatHandler]:
    """Register the built-in format handlers."""
    return list(BUILTIN_HANDLERS)
