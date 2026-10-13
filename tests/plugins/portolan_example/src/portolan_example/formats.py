"""The ``.xyz`` format handler: ASCII ``x y z`` point lists to GeoParquet.

Each line holds a longitude, a latitude, and a height in WGS84, separated by
spaces. The handler writes the points as GeoParquet through geoparquet-io and
keeps the height in a ``z`` column. geoparquet-io and pyarrow come with
portolan-cli.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli import api

if TYPE_CHECKING:
    from pathlib import Path


@dataclass(frozen=True)
class XyzMetadata:
    """What Portolan reads from a converted ``.xyz`` file."""

    bbox: list[float]
    crs: str
    point_count: int

    def to_stac_properties(self) -> dict[str, Any]:
        """Add the point count as an item property."""
        return {"example:point_count": self.point_count}


def _points(path: Path) -> list[tuple[float, float, float]]:
    points = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            x, y, z = (float(part) for part in line.split())
            points.append((x, y, z))
    return points


# --8<-- [start:handler]
@dataclass(frozen=True)
class XyzHandler:
    """Convert ``.xyz`` point lists to GeoParquet."""

    name: str = "xyz"
    family: str = "vector"
    display_name: str = "XYZ points"
    extensions: frozenset[str] = frozenset({".xyz"})
    media_type: str = "application/vnd.apache.parquet"
    asset_level: str = "collection"

    def claims(self, path: Path) -> bool:
        """Claim a file whose first line holds three numbers."""
        try:
            first = next(line for line in path.read_text(encoding="utf-8").splitlines() if line)
            return len([float(part) for part in first.split()]) == 3
        except (OSError, UnicodeDecodeError, ValueError, StopIteration):
            return False
            # --8<-- [end:handler]

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Write ``<stem>.parquet`` next to the source."""
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        rows = [f"{x},{y},{z},POINT({x} {y})" for x, y, z in _points(source)]
        csv = output_dir / f".{source.stem}.xyz.csv"
        csv.write_text("x,y,z,wkt\n" + "\n".join(rows) + "\n", encoding="utf-8")
        output = self.output_path(source, output_dir)
        try:
            gpio.convert(str(csv)).add_bbox().sort_hilbert().write(str(output))
        finally:
            csv.unlink(missing_ok=True)
        return output

    def output_path(self, source: Path, output_dir: Path) -> Path:
        """Return ``<stem>.parquet`` in ``output_dir``."""
        return output_dir / f"{source.stem}.parquet"

    def describe_item(
        self, item: Any, output: Path, metadata: Any, context: api.FormatContext
    ) -> None:
        """Add column statistics, as the built-in GeoParquet handler does."""
        api.describe_vector_item(item, output, metadata, context)

    def describe_collection(
        self, collection: Any, metadata: list[Any], context: api.FormatContext
    ) -> None:
        """Record the total point count of the files this ``add`` converted."""
        total = sum(m.point_count for m in metadata if isinstance(m, XyzMetadata))
        collection.extra_fields["example:point_count"] = total

    def inspect(self, path: Path, catalog_root: Path | None) -> Any:
        """Describe the converted GeoParquet next to an ``.xyz`` file."""
        converted = self.output_path(path, path.parent) if path.suffix == ".xyz" else path
        if not converted.exists():
            raise ValueError(f"Run portolan add first. {converted.name} does not exist.")
        return api.inspect_geoparquet(converted, catalog_root=catalog_root)

    def read_metadata(self, path: Path) -> XyzMetadata:
        """Read the bbox from the GeoParquet ``geo`` key."""
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        geo = json.loads(parquet.schema_arrow.metadata[b"geo"])
        column = geo["columns"][geo["primary_column"]]
        return XyzMetadata(
            bbox=[float(v) for v in column["bbox"]],
            crs="OGC:CRS84",
            point_count=parquet.metadata.num_rows,
        )
