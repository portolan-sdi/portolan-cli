"""COG (Cloud-Optimized GeoTIFF) metadata extraction.

Uses rasterio to read COG metadata without loading pixel data.
Extracts bbox, CRS, dimensions, bands, and resolution.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, overload

import rasterio

from portolan_cli.models.schema import BandSchema, SchemaModel


@dataclass
class COGMetadata:
    """Metadata extracted from a COG file.

    Attributes:
        bbox: Bounding box as (minx, miny, maxx, maxy).
        crs: CRS as EPSG code or WKT.
        width: Image width in pixels.
        height: Image height in pixels.
        band_count: Number of bands.
        dtype: Data type (uint8, float32, etc.).
        nodata: Nodata value or None.
        resolution: Pixel resolution as (x_res, y_res).
    """

    bbox: tuple[float, float, float, float]
    crs: str | None
    width: int
    height: int
    band_count: int
    dtype: str
    nodata: float | None
    resolution: tuple[float, float]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "bbox": list(self.bbox),
            "crs": self.crs,
            "width": self.width,
            "height": self.height,
            "band_count": self.band_count,
            "dtype": self.dtype,
            "nodata": self.nodata,
            "resolution": list(self.resolution),
        }

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item properties format."""
        props: dict[str, Any] = {
            "raster:bands": [{"data_type": self.dtype} for _ in range(self.band_count)],
        }

        if self.nodata is not None:
            for band in props["raster:bands"]:
                band["nodata"] = self.nodata

        return props


def extract_cog_metadata(path: Path) -> COGMetadata:
    """Extract metadata from a COG file.

    Uses rasterio to read file metadata without loading pixel data.

    Args:
        path: Path to COG file.

    Returns:
        COGMetadata with extracted information.

    Raises:
        FileNotFoundError: If file doesn't exist.
        rasterio.errors.RasterioIOError: If file is not valid raster.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with rasterio.open(path) as src:
        # Extract CRS
        crs = None
        if src.crs:
            epsg = src.crs.to_epsg()
            if epsg:
                crs = f"EPSG:{epsg}"
            else:
                crs = src.crs.to_wkt()

        # Extract bbox from bounds
        bbox = (
            src.bounds.left,
            src.bounds.bottom,
            src.bounds.right,
            src.bounds.top,
        )

        # Extract resolution from transform
        resolution = (abs(src.transform.a), abs(src.transform.e))

        return COGMetadata(
            bbox=bbox,
            crs=crs,
            width=src.width,
            height=src.height,
            band_count=src.count,
            dtype=str(src.dtypes[0]),
            nodata=src.nodata,
            resolution=resolution,
        )


# Overloaded signatures for extract_schema_from_cog
@overload
def extract_schema_from_cog(
    path: Path,
    *,
    return_warnings: Literal[False] = False,
) -> SchemaModel: ...


@overload
def extract_schema_from_cog(
    path: Path,
    *,
    return_warnings: Literal[True],
) -> tuple[SchemaModel, list[str]]: ...


def extract_schema_from_cog(
    path: Path,
    *,
    return_warnings: bool = False,
) -> SchemaModel | tuple[SchemaModel, list[str]]:
    """Extract SchemaModel from a COG file.

    Extracts band metadata including data types and nodata values.
    Returns a SchemaModel compatible with the Portolan metadata model.

    Args:
        path: Path to COG file.
        return_warnings: If True, return (SchemaModel, warnings) tuple.

    Returns:
        SchemaModel with band definitions as columns, or (SchemaModel, warnings)
        tuple if return_warnings is True.

    Raises:
        FileNotFoundError: If file doesn't exist.
        rasterio.errors.RasterioIOError: If file is not valid raster.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    warnings: list[str] = []
    statistics: dict[str, Any] = {}

    with rasterio.open(path) as src:
        # Check for missing CRS
        if src.crs:
            epsg = src.crs.to_epsg()
            if epsg:
                statistics["crs"] = f"EPSG:{epsg}"
            else:
                statistics["crs"] = src.crs.to_wkt()
        else:
            warnings.append("Raster has no CRS defined. Consider adding CRS metadata.")

        # Build BandSchema for each band
        bands: list[BandSchema] = []
        for i in range(1, src.count + 1):
            # Get band description if available
            description = src.descriptions[i - 1] if src.descriptions else None

            band = BandSchema(
                name=f"band_{i}",
                data_type=str(src.dtypes[i - 1]),
                nodata=src.nodata,
                description=description,
            )
            bands.append(band)

    # For COG, we store bands as "columns" in the schema
    # This is a design choice - BandSchema is compatible via to_dict/from_dict
    schema = SchemaModel(
        schema_version="1.0.0",
        format="cog",
        columns=bands,  # type: ignore[arg-type]
        statistics=statistics if statistics else None,
    )

    if return_warnings:
        return schema, warnings
    return schema
