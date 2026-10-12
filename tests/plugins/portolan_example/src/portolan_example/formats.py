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

    def convert(self, source: Path, output_dir: Path, catalog_root: Path | None) -> Path:
        """Write ``<stem>.parquet`` next to the source."""
        import geoparquet_io as gpio  # type: ignore[import-untyped]

        rows = [f"{x},{y},{z},POINT({x} {y})" for x, y, z in _points(source)]
        csv = output_dir / f".{source.stem}.xyz.csv"
        csv.write_text("x,y,z,wkt\n" + "\n".join(rows) + "\n", encoding="utf-8")
        output = output_dir / f"{source.stem}.parquet"
        try:
            gpio.convert(str(csv)).add_bbox().sort_hilbert().write(str(output))
        finally:
            csv.unlink(missing_ok=True)
        return output

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
