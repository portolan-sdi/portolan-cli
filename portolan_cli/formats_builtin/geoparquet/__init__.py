"""Vector sources to GeoParquet through geoparquet-io (#947)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli import api

if TYPE_CHECKING:
    from pathlib import Path

_CLOUD_NATIVE_VECTORS = frozenset({".pmtiles", ".fgb", ".flatgeobuf"})


@dataclass(frozen=True)
class GeoParquetHandler:
    """Convert vector sources to GeoParquet. A GeoParquet source is rewritten when needed."""

    name: str = "geoparquet"
    family: str = "vector"
    display_name: str = "GeoParquet"
    extensions: frozenset[str] = frozenset()
    media_type: str = "application/vnd.apache.parquet"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:
        """Claim a vector file. A ``.json`` file must look like GeoJSON."""
        if path.suffix.lower() == ".json":
            return path.is_file() and api.is_geojson(path)
        return True

    def output_path(self, source: Path, output_dir: Path) -> Path:
        """Return ``<stem>.parquet`` in ``output_dir``."""
        return output_dir / f"{source.stem}.parquet"

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Convert, validate, and write a thumbnail when none exists.

        A GeoParquet source is copied, or rewritten in place behind the
        fidelity gate when it lacks the bbox covering column (issue #805).
        """
        output = api.convert_vector(source, output_dir, catalog_root)
        # A table with no geometry is tabular data. ``add`` reads no bbox from
        # it, removes it, and tracks the source as a tabular asset.
        if api.is_geoparquet(output):
            self._check(output)
            api.write_vector_thumbnail(output, catalog_root)
        return output

    def reoptimize(self, source: Path, catalog_root: Path | None) -> Path:
        """Rewrite a GeoParquet file with the current settings."""
        output = api.convert_vector(source, source.parent, catalog_root, reconvert=True)
        self._check(output)
        api.write_vector_thumbnail(output, catalog_root, replace=True)
        return output

    def partition(self, path: Path, catalog_root: Path | None) -> list[Path]:
        """Partition by the one partition rule. See ``partition_strategy``."""
        return api.partition_vector_file(path, catalog_root)

    def read_metadata(self, path: Path) -> Any:
        """Read GeoParquet metadata."""
        return api.extract_geoparquet_metadata(path)

    def describe_item(
        self, item: Any, output: Path, metadata: Any, context: api.FormatContext
    ) -> None:
        """Add column statistics and the vector extension."""
        api.describe_vector_item(item, output, metadata, context)

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: api.FormatContext
    ) -> None:
        """Aggregate the table extension from every tracked GeoParquet file."""
        api.aggregate_table_extension(collection, metadata, context)

    def inspect(self, path: Path, catalog_root: Path | None) -> Any:
        """Describe a GeoParquet file for ``portolan info``."""
        return api.inspect_geoparquet(path, catalog_root=catalog_root)

    @staticmethod
    def _check(output: Path) -> None:
        if output.is_file():
            error = api.validate_geoparquet(output)
            if error is not None:
                raise api.InvalidOutputError(str(output), error)


def make_handler() -> GeoParquetHandler:
    """Build the handler with every vector extension except the cloud-native ones."""
    extensions = (api.VECTOR_EXTENSIONS - _CLOUD_NATIVE_VECTORS) | {".json"}
    return GeoParquetHandler(extensions=frozenset(extensions))
