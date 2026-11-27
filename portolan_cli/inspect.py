"""Inspect module - file, collection, and catalog metadata inspection."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.stac_parquet import count_items
from portolan_core.stac_links import catalog_collections
from portolan_core.versions import read_versions

if TYPE_CHECKING:
    from pathlib import Path

_MIN_PATH_PARTS = 2


@dataclass
class FileInfo:
    """Metadata extracted from a geospatial file.

    Attributes:
        path: Path to the file.
        format: File format (GeoParquet, COG, etc.).
        crs: Coordinate reference system (EPSG code or WKT).
        bbox: Bounding box as [minx, miny, maxx, maxy].
        feature_count: Number of features (vector files only).
        width: Image width in pixels (raster files only).
        height: Image height in pixels (raster files only).
        band_count: Number of bands (raster files only).
        dtype: Data type (raster files only).
        version: Version string from versions.json (if tracked).
        geometry_type: Every declared geometry type (vector files only).
    """

    path: Path
    format: str
    crs: str | None = None
    bbox: list[float] | None = None
    feature_count: int | None = None
    width: int | None = None
    height: int | None = None
    band_count: int | None = None
    dtype: str | None = None
    version: str | None = None
    geometry_type: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        data: dict[str, Any] = {
            "path": str(self.path),
            "format": self.format,
            "crs": self.crs,
            "bbox": self.bbox,
            "version": self.version,
        }

        # Add format-specific fields
        if self.format == "GeoParquet":
            data["feature_count"] = self.feature_count
            data["geometry_type"] = self.geometry_type
        elif self.format == "COG":
            data["width"] = self.width
            data["height"] = self.height
            data["band_count"] = self.band_count
            data["dtype"] = self.dtype

        return data

    def format_human(self) -> list[str]:
        """Format for human-readable output.

        Returns:
            List of output lines in the format:
        """
        lines = []
        lines.append(f"Format: {self.format}")
        lines.append(f"CRS: {self.crs or 'Unknown'}")

        if self.bbox:
            from portolan_core.bbox import to_2d_bbox

            west, south, east, north = to_2d_bbox(self.bbox)
            bbox_str = f"[{west}, {south}, {east}, {north}]"
            lines.append(f"Bbox: {bbox_str}")

        if self.format == "GeoParquet" and self.feature_count is not None:
            lines.append(f"Features: {self.feature_count:,}")
        elif self.format == "COG":
            if self.width and self.height:
                lines.append(f"Dimensions: {self.width} x {self.height}")
            if self.band_count:
                lines.append(f"Bands: {self.band_count}")

        if self.version:
            lines.append(f"Version: {self.version}")

        return lines


@dataclass
class CollectionInfo:
    """Metadata for a STAC collection."""

    collection_id: str
    title: str | None = None
    description: str | None = None
    item_count: int = 0
    total_size_bytes: int = 0
    bbox: list[float] | None = None
    has_parquet: bool = False

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "collection_id": self.collection_id,
            "title": self.title,
            "description": self.description,
            "item_count": self.item_count,
            "total_size_bytes": self.total_size_bytes,
            "bbox": self.bbox,
            "has_parquet": self.has_parquet,
        }

    def format_human(self) -> list[str]:
        """Format for human-readable output."""
        lines = []
        lines.append(f"Collection: {self.collection_id}")
        if self.title:
            lines.append(f"Title: {self.title}")
        if self.description:
            lines.append(f"Description: {self.description}")
        lines.append(f"Items: {self.item_count}")
        if self.total_size_bytes:
            size_mb = self.total_size_bytes / (1024 * 1024)
            lines.append(f"Total Size: {size_mb:.2f} MB")
        if self.bbox:
            from portolan_core.bbox import to_2d_bbox

            west, south, east, north = to_2d_bbox(self.bbox)
            bbox_str = f"[{west}, {south}, {east}, {north}]"
            lines.append(f"Bbox: {bbox_str}")
        # Show parquet status
        parquet_status = "Yes" if self.has_parquet else "No"
        lines.append(f"GeoParquet Index: {parquet_status}")
        return lines


@dataclass
class CatalogInfo:
    """Metadata for a STAC catalog."""

    catalog_id: str
    title: str | None = None
    description: str | None = None
    collection_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dictionary."""
        return {
            "catalog_id": self.catalog_id,
            "title": self.title,
            "description": self.description,
            "collection_count": self.collection_count,
        }

    def format_human(self) -> list[str]:
        """Format for human-readable output."""
        lines = []
        lines.append(f"Catalog: {self.catalog_id}")
        if self.title:
            lines.append(f"Title: {self.title}")
        if self.description:
            lines.append(f"Description: {self.description}")
        lines.append(f"Collections: {self.collection_count}")
        return lines


def inspect_file(path: Path, *, catalog_root: Path | None = None) -> FileInfo:
    """Inspect a geospatial file and extract metadata."""
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    from portolan_cli.plugins.formats import handler_for

    handler = handler_for(path, catalog_root)
    if handler is None:
        raise ValueError(f"Unsupported file format: {path.suffix}")
    info: FileInfo = handler.inspect(path, catalog_root)
    return info


def inspect_geoparquet(path: Path, *, catalog_root: Path | None = None) -> FileInfo:
    """Extract metadata from a GeoParquet file.

    Args:
        path: Path to the GeoParquet file.
        catalog_root: Optional catalog root for version lookup.

    Returns:
        FileInfo with GeoParquet metadata.
    """
    from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

    metadata = extract_geoparquet_metadata(path)

    # Look up version if catalog_root provided
    version = None
    if catalog_root:
        version = _lookup_version(path, catalog_root)

    return FileInfo(
        path=path,
        format="GeoParquet",
        crs=metadata.crs
        if isinstance(metadata.crs, str)
        else str(metadata.crs)
        if metadata.crs
        else None,
        bbox=list(metadata.bbox) if metadata.bbox else None,
        feature_count=metadata.feature_count,
        geometry_type=metadata.geometry_type,
        version=version,
    )


def inspect_cog(path: Path, *, catalog_root: Path | None = None) -> FileInfo:
    """Extract metadata from a COG file.

    Args:
        path: Path to the COG file.
        catalog_root: Optional catalog root for version lookup.

    Returns:
        FileInfo with COG metadata.
    """
    from portolan_cli.metadata.cog import extract_cog_metadata

    metadata = extract_cog_metadata(path)

    # Look up version if catalog_root provided
    version = None
    if catalog_root:
        version = _lookup_version(path, catalog_root)

    return FileInfo(
        path=path,
        format="COG",
        crs=metadata.crs,
        bbox=list(metadata.bbox),
        width=metadata.width,
        height=metadata.height,
        band_count=metadata.band_count,
        dtype=metadata.dtype,
        version=version,
    )


def _lookup_version(path: Path, catalog_root: Path) -> str | None:
    """Look up version from versions.json for a tracked file."""
    # Try to determine collection from file path
    try:
        relative = path.resolve().relative_to(catalog_root.resolve())
    except ValueError:
        # File is outside catalog
        return None

    parts = relative.parts
    if len(parts) < _MIN_PATH_PARTS:
        return None

    # First part is collection ID ( subdirectory = collection)
    collection_id = parts[0]
    versions_path = catalog_root / collection_id / "versions.json"

    if not versions_path.exists():
        return None

    try:
        versions_file = read_versions(versions_path)
    except (ValueError, FileNotFoundError):
        return None

    if versions_file.current_version is None:
        return None

    # Check if file is in the current version's assets
    current_version_obj = next(
        (v for v in versions_file.versions if v.version == versions_file.current_version),
        None,
    )

    if current_version_obj is None:
        return None

    # Check if file is tracked using item-scoped key format ({item_id}/{filename})
    filename = path.name
    # Determine item_id from path (parent directory name)
    item_id = path.parent.name if path.parent != catalog_root else ""

    # The item-scoped key is the new format. The bare filename is the legacy format.
    keys = {filename, f"{item_id}/{filename}"} if item_id else {filename}
    if any(asset_name in keys for asset_name in current_version_obj.assets):
        return f"v{versions_file.current_version}"

    return None


def inspect_collection(collection_path: Path) -> CollectionInfo:
    """Inspect a STAC collection and extract metadata."""
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"Collection not found: {collection_path}")

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))

    # Count items the collection owns, descending organizing catalogs (core.md:168-170)
    item_count = count_items(collection_path)

    # Calculate total size from versions.json
    total_size = 0
    versions_path = collection_path / "versions.json"
    if versions_path.exists():
        try:
            versions_file = read_versions(versions_path)
            if versions_file.versions:
                current = versions_file.versions[-1]
                total_size = sum(asset.size_bytes for asset in current.assets.values())
        except (ValueError, FileNotFoundError):
            pass

    # Extract bbox from extent
    bbox = None
    extent = data.get("extent", {})
    spatial = extent.get("spatial", {})
    bbox_list = spatial.get("bbox", [])
    if bbox_list and len(bbox_list) > 0:
        bbox = bbox_list[0]

    # Check for items.parquet (stac-geoparquet index)
    has_parquet = (collection_path / "items.parquet").exists()

    return CollectionInfo(
        collection_id=data.get("id", collection_path.name),
        title=data.get("title"),
        description=data.get("description"),
        item_count=item_count,
        total_size_bytes=total_size,
        bbox=bbox,
        has_parquet=has_parquet,
    )


def inspect_catalog(catalog_root: Path) -> CatalogInfo:
    """Inspect a STAC catalog and extract metadata."""
    catalog_json_path = catalog_root / "catalog.json"
    if not catalog_json_path.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog_root}")

    data = json.loads(catalog_json_path.read_text(encoding="utf-8"))

    collection_count = len(catalog_collections(catalog_root))

    return CatalogInfo(
        catalog_id=data.get("id", catalog_root.name),
        title=data.get("title"),
        description=data.get("description"),
        collection_count=collection_count,
    )


def inspect_target(
    target: Path | None, *, catalog_root: Path
) -> FileInfo | CollectionInfo | CatalogInfo:
    """Inspect the file, collection, or catalog that ``target`` names."""
    if target is None:
        return inspect_catalog(catalog_root)
    if target.is_file():
        return inspect_file(target, catalog_root=catalog_root)
    if not target.is_dir():
        raise FileNotFoundError(f"Path not found: {target}")
    # A directory can hold both files. catalog.json wins, because it gives the
    # organizing structure.
    if (target / "catalog.json").exists():
        return inspect_catalog(target)
    if (target / "collection.json").exists():
        return inspect_collection(target)
    raise ValueError(f"Directory is not a catalog or collection: {target}")
