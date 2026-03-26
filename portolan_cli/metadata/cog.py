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
        nodata: Legacy single nodata value (first band). Use nodatavals for per-band.
        resolution: Pixel resolution as (x_res, y_res).
        nodatavals: Per-band nodata values as tuple. If None, falls back to nodata.
        transform: Affine transform as 6 coefficients (a, b, c, d, e, f).
                  Maps pixel coordinates to CRS coordinates.
    """

    bbox: tuple[float, float, float, float]
    crs: str | None
    width: int
    height: int
    band_count: int
    dtype: str
    nodata: float | None
    resolution: tuple[float, float]
    nodatavals: tuple[float | None, ...] | None = None
    transform: tuple[float, float, float, float, float, float] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        result: dict[str, Any] = {
            "bbox": list(self.bbox),
            "crs": self.crs,
            "width": self.width,
            "height": self.height,
            "band_count": self.band_count,
            "dtype": self.dtype,
            "nodata": self.nodata,
            "resolution": list(self.resolution),
        }
        if self.nodatavals is not None:
            # Convert tuple to list, preserving None values
            nodatavals_list: list[float | None] = list(self.nodatavals)
            result["nodatavals"] = nodatavals_list
        if self.transform is not None:
            result["transform"] = list(self.transform)
        return result

    def to_stac_properties(self) -> dict[str, Any]:
        """Convert to STAC Item properties format (STAC v1.1.0 unified bands).

        Returns unified `bands` array (not `raster:bands`) per STAC v1.1.0 spec.
        Each band has: name, data_type, and optional nodata.

        Also includes raster:spatial_resolution from resolution tuple.
        """
        # Build unified bands array (STAC v1.1.0)
        bands: list[dict[str, Any]] = []
        for i in range(self.band_count):
            band: dict[str, Any] = {
                "name": f"band_{i + 1}",
                "data_type": self.dtype,
            }
            # Per-band nodata (preferred)
            if self.nodatavals is not None and i < len(self.nodatavals):
                if self.nodatavals[i] is not None:
                    band["nodata"] = self.nodatavals[i]
            elif self.nodata is not None:
                # Fall back to uniform nodata
                band["nodata"] = self.nodata
            bands.append(band)

        props: dict[str, Any] = {"bands": bands}

        # Add raster:spatial_resolution (average of x and y resolution)
        if self.resolution:
            x_res, y_res = self.resolution
            props["raster:spatial_resolution"] = (x_res + y_res) / 2

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

        # Extract per-band nodata values
        # src.nodatavals returns a tuple with one value per band
        nodatavals = src.nodatavals if src.nodatavals else None

        # Extract affine transform as GDAL GeoTransform format
        # GDAL format: (origin_x, pixel_width, rotation_x, origin_y, rotation_y, pixel_height)
        gdal_transform = src.transform.to_gdal()
        transform = (
            gdal_transform[0],
            gdal_transform[1],
            gdal_transform[2],
            gdal_transform[3],
            gdal_transform[4],
            gdal_transform[5],
        )

        return COGMetadata(
            bbox=bbox,
            crs=crs,
            width=src.width,
            height=src.height,
            band_count=src.count,
            dtype=str(src.dtypes[0]),
            nodata=src.nodata,  # Keep for backward compatibility
            resolution=resolution,
            nodatavals=nodatavals,
            transform=transform,
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
    crs_value: str | None = None

    with rasterio.open(path) as src:
        # Extract CRS as schema-level spatial metadata
        if src.crs:
            epsg = src.crs.to_epsg()
            if epsg:
                crs_value = f"EPSG:{epsg}"
            else:
                crs_value = src.crs.to_wkt()
        else:
            warnings.append("Raster has no CRS defined. Consider adding CRS metadata.")

        # Get per-band nodata values (fallback to uniform nodata if not available)
        nodatavals = src.nodatavals if src.nodatavals else None

        # Build BandSchema for each band
        bands: list[BandSchema] = []
        for i in range(1, src.count + 1):
            # Get band description if available
            description = src.descriptions[i - 1] if src.descriptions else None

            # Use per-band nodata if available, otherwise fall back to uniform nodata
            if nodatavals is not None and len(nodatavals) >= i:
                band_nodata = nodatavals[i - 1]
            else:
                band_nodata = src.nodata

            band = BandSchema(
                name=f"band_{i}",
                data_type=str(src.dtypes[i - 1]),
                nodata=band_nodata,
                description=description,
            )
            bands.append(band)

    # For COG, we store bands as "columns" in the schema
    # BandSchema is now properly typed in SchemaModel.columns union
    schema = SchemaModel(
        schema_version="1.0.0",
        format="cog",
        columns=list(bands),
        crs=crs_value,
    )

    if return_warnings:
        return schema, warnings
    return schema
