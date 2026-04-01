"""STAC generation module - wraps pystac for Portolan's conventions.

Provides opinionated helpers for creating STAC catalogs, collections, and items
with consistent defaults and conventions for Portolan-managed catalogs.

Key conventions:
- Self-contained catalog type (relative links, portable)
- WGS84 (EPSG:4326) as default CRS
- Consistent asset naming and roles
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from pathlib import Path

import pystac
from pystac.summaries import Summarizer, SummaryStrategy

# STAC version we generate (v1.1.0 has unified bands array, superseding eo:bands/raster:bands)
STAC_VERSION = "1.1.0"

# Default license when not specified
DEFAULT_LICENSE = "proprietary"


def create_collection(
    *,
    collection_id: str,
    description: str,
    title: str | None = None,
    license: str = DEFAULT_LICENSE,
    bbox: list[float] | None = None,
    temporal_extent: tuple[datetime | None, datetime | None] | None = None,
) -> pystac.Collection:
    """Create a STAC Collection with Portolan conventions.

    Args:
        collection_id: Unique identifier for the collection.
        description: Human-readable description.
        title: Optional display title (defaults to None).
        license: SPDX license identifier (default: "proprietary").
        bbox: Spatial extent as [min_x, min_y, max_x, max_y] in WGS84.
              Defaults to global extent if not specified.
        temporal_extent: Temporal extent as (start, end) datetimes.
                        Use None for open-ended intervals.

    Returns:
        A pystac.Collection object.
    """
    # Default to global extent if not specified
    if bbox is None:
        bbox = [-180, -90, 180, 90]

    # Default to open temporal interval
    if temporal_extent is None:
        temporal_interval: list[datetime | None] = [None, None]
    else:
        temporal_interval = list(temporal_extent)

    extent = pystac.Extent(
        spatial=pystac.SpatialExtent(bboxes=[bbox]),
        temporal=pystac.TemporalExtent(intervals=[temporal_interval]),
    )

    collection = pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        title=title,
        license=license,
    )

    return collection


def create_item(
    *,
    item_id: str,
    bbox: list[float],
    datetime: datetime | None = None,
    properties: dict[str, object] | None = None,
    assets: dict[str, pystac.Asset] | None = None,
) -> pystac.Item:
    """Create a STAC Item with Portolan conventions.

    Args:
        item_id: Unique identifier for the item.
        bbox: Bounding box as [min_x, min_y, max_x, max_y] in WGS84.
        datetime: Acquisition/creation datetime. If None, creates an open temporal
            interval (start/end both null) and marks as provisional (per ADR-0035).
        properties: Additional properties to include.
        assets: Asset dictionary to attach to the item.

    Returns:
        A pystac.Item object.
    """
    # Generate polygon geometry from bbox
    geometry = _bbox_to_polygon(bbox)

    # Merge any custom properties
    item_properties = dict(properties) if properties else {}

    # Per ADR-0035: If datetime not provided, use open interval (null start/end)
    # and mark as provisional so portolan check can flag incomplete items.
    # STAC requires either datetime OR start_datetime+end_datetime.
    datetime_provisional = datetime is None
    if datetime_provisional:
        # Use open temporal interval (both null = unknown temporal extent)
        item_properties["start_datetime"] = None
        item_properties["end_datetime"] = None
        item_properties["portolan:datetime_provisional"] = True

    item = pystac.Item(
        id=item_id,
        geometry=geometry,
        bbox=bbox,
        datetime=datetime,  # Will be None if provisional
        properties=item_properties,
    )

    # Add assets if provided
    if assets:
        for asset_key, asset in assets.items():
            item.add_asset(asset_key, asset)

    return item


def _bbox_to_polygon(bbox: list[float]) -> dict[str, object]:
    """Convert a bounding box to a GeoJSON Polygon geometry.

    Args:
        bbox: [min_x, min_y, max_x, max_y]

    Returns:
        GeoJSON Polygon dict.
    """
    min_x, min_y, max_x, max_y = bbox
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [min_x, min_y],
                [min_x, max_y],
                [max_x, max_y],
                [max_x, min_y],
                [min_x, min_y],  # Close the ring
            ]
        ],
    }


def _now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


def load_catalog(catalog_path: Path) -> pystac.Catalog:
    """Load an existing STAC catalog from disk.

    Args:
        catalog_path: Path to the catalog.json file.

    Returns:
        A pystac.Catalog object.

    Raises:
        FileNotFoundError: If the catalog file doesn't exist.
    """
    if not catalog_path.exists():
        raise FileNotFoundError(f"Catalog not found: {catalog_path}")

    return pystac.Catalog.from_file(str(catalog_path))


def save_catalog(catalog: pystac.Catalog, dest_dir: Path) -> None:
    """Save a STAC catalog to disk.

    Saves as a self-contained catalog with relative links.

    Args:
        catalog: The catalog to save.
        dest_dir: Directory to save the catalog to (will contain catalog.json).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    # Normalize the catalog before saving (sets up hrefs)
    catalog.normalize_hrefs(str(dest_dir))

    # Save as self-contained (relative links)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def add_collection_to_catalog(
    catalog: pystac.Catalog,
    collection: pystac.Collection,
) -> None:
    """Add a collection as a child of a catalog.

    Args:
        catalog: The parent catalog.
        collection: The collection to add.
    """
    catalog.add_child(collection)


def add_item_to_collection(
    collection: pystac.Collection,
    item: pystac.Item,
    *,
    update_extent: bool = False,
) -> None:
    """Add an item to a collection.

    Args:
        collection: The parent collection.
        item: The item to add.
        update_extent: If True, update collection's spatial extent to
                      encompass the item's bbox.
    """
    collection.add_item(item)

    if update_extent:
        _update_collection_extent(collection, item)


def _update_collection_extent(
    collection: pystac.Collection,
    item: pystac.Item,
) -> None:
    """Update a collection's spatial extent to include an item's bbox.

    Args:
        collection: The collection to update.
        item: The item whose bbox should be included.
    """
    if item.bbox is None:
        return

    current_bbox = collection.extent.spatial.bboxes[0]
    new_bbox = [
        min(current_bbox[0], item.bbox[0]),  # min_x
        min(current_bbox[1], item.bbox[1]),  # min_y
        max(current_bbox[2], item.bbox[2]),  # max_x
        max(current_bbox[3], item.bbox[3]),  # max_y
    ]

    collection.extent.spatial = pystac.SpatialExtent(bboxes=[new_bbox])


def update_collection_temporal_extent(
    collection: pystac.Collection,
    item_datetime: datetime | None,
) -> None:
    """Update a collection's temporal extent to include an item's datetime.

    Widens the collection's temporal interval to encompass the item's datetime.
    Per ADR-0035, items without datetime have null interval and are not included.

    Args:
        collection: The collection to update.
        item_datetime: The item's datetime, or None for provisional items.
    """
    from portolan_cli.temporal import ensure_utc_aware

    if item_datetime is None:
        return  # Provisional items don't affect temporal extent

    # Normalize to UTC-aware to avoid naive/aware comparison errors
    item_dt = ensure_utc_aware(item_datetime)
    assert item_dt is not None  # nosec B101 - type narrowing for mypy, runtime checked above

    # Get current interval
    current_interval = collection.extent.temporal.intervals[0]
    current_start = ensure_utc_aware(current_interval[0])
    current_end = ensure_utc_aware(current_interval[1])

    # Widen interval to include item datetime
    new_start = current_start
    new_end = current_end

    if current_start is None or item_dt < current_start:
        new_start = item_dt
    if current_end is None or item_dt > current_end:
        new_end = item_dt

    collection.extent.temporal = pystac.TemporalExtent(intervals=[[new_start, new_end]])


# STAC Extension schema URLs (v1.1.0 compatible)
# Note: "file" extension is reserved for future use (checksums, sizes)
# Currently only table, projection, and raster are actively used
EXTENSION_URLS = {
    "table": "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "projection": "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
    "raster": "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
    "file": "https://stac-extensions.github.io/file/v2.1.0/schema.json",  # Reserved for future
    "vector": "https://stac-extensions.github.io/vector/v0.1.0/schema.json",  # Proposal maturity
}


def build_stac_extensions(properties: dict[str, object]) -> list[str]:
    """Build stac_extensions array based on which extension fields are populated.

    Scans the properties dict for extension-prefixed fields (e.g., "table:", "proj:")
    and returns the corresponding extension schema URLs.

    Args:
        properties: Properties dict to scan for extension fields.

    Returns:
        List of extension schema URLs.
    """
    extensions: list[str] = []

    # Check for table extension fields
    if any(k.startswith("table:") for k in properties):
        extensions.append(EXTENSION_URLS["table"])

    # Check for projection extension fields
    if any(k.startswith("proj:") for k in properties):
        extensions.append(EXTENSION_URLS["projection"])

    # Check for raster extension fields
    if any(k.startswith("raster:") for k in properties):
        extensions.append(EXTENSION_URLS["raster"])

    # Check for file extension fields
    if any(k.startswith("file:") for k in properties):
        extensions.append(EXTENSION_URLS["file"])

    # Check for vector extension fields
    if any(k.startswith("vector:") for k in properties):
        extensions.append(EXTENSION_URLS["vector"])

    return extensions


def add_table_extension(
    collection: pystac.Collection,
    metadata: object,
) -> None:
    """Add Table extension fields to a collection from GeoParquet metadata.

    Sets table:row_count, table:primary_geometry, and table:columns based on
    the provided GeoParquet metadata object.

    Args:
        collection: The collection to add extension fields to.
        metadata: A GeoParquetMetadata-like object with feature_count,
                 geometry_column, and schema attributes.
    """
    # Set row count
    if hasattr(metadata, "feature_count") and metadata.feature_count is not None:
        collection.extra_fields["table:row_count"] = metadata.feature_count

    # Set primary geometry column
    if hasattr(metadata, "geometry_column") and metadata.geometry_column is not None:
        collection.extra_fields["table:primary_geometry"] = metadata.geometry_column

    # Set columns from schema
    if hasattr(metadata, "schema") and metadata.schema:
        columns = [{"name": name, "type": dtype} for name, dtype in metadata.schema.items()]
        collection.extra_fields["table:columns"] = columns

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["table"]
    if ext_url not in (collection.stac_extensions or []):
        if collection.stac_extensions is None:
            collection.stac_extensions = []
        collection.stac_extensions.append(ext_url)


def get_existing_table_metadata(collection: pystac.Collection) -> object | None:
    """Extract existing table extension metadata from a collection.

    Returns a GeoParquetMetadata-like object if the collection has table extension
    fields, or None if not present. Used to include existing metadata when
    incrementally adding items to a collection.

    Args:
        collection: The collection to extract metadata from.

    Returns:
        A GeoParquetMetadata object if table extension fields exist, None otherwise.
    """
    from portolan_cli.metadata.geoparquet import GeoParquetMetadata

    row_count = collection.extra_fields.get("table:row_count")
    if row_count is None:
        return None

    # Reconstruct schema from table:columns
    columns = collection.extra_fields.get("table:columns", [])
    schema = {col["name"]: col["type"] for col in columns if "name" in col and "type" in col}

    return GeoParquetMetadata(
        bbox=None,  # Not stored in table extension
        crs=None,  # Not stored in table extension
        geometry_type=None,  # Not stored in table extension
        geometry_column=collection.extra_fields.get("table:primary_geometry", "geometry"),
        feature_count=row_count,
        schema=schema,
    )


def _merge_schemas(metadata_list: Sequence[object]) -> tuple[dict[str, str], list[str]]:
    """Merge schemas from multiple metadata objects, tracking conflicts."""
    merged_schema: dict[str, str] = {}
    conflicts: list[str] = []
    for m in metadata_list:
        schema = getattr(m, "schema", None)
        if schema:
            for col_name, col_type in schema.items():
                if col_name in merged_schema and merged_schema[col_name] != col_type:
                    conflicts.append(
                        f"Column '{col_name}': {merged_schema[col_name]} vs {col_type}"
                    )
                elif col_name not in merged_schema:
                    merged_schema[col_name] = col_type
    return merged_schema, conflicts


def _compute_bbox_union(
    metadata_list: Sequence[object],
) -> tuple[float, float, float, float]:
    """Compute bounding box union from multiple metadata objects."""
    all_bboxes: list[tuple[float, float, float, float]] = []
    for m in metadata_list:
        bbox = getattr(m, "bbox", None)
        if bbox is not None:
            all_bboxes.append(bbox)
    if not all_bboxes:
        raise ValueError("Cannot aggregate metadata: no items have valid bboxes")
    return (
        min(b[0] for b in all_bboxes),
        min(b[1] for b in all_bboxes),
        max(b[2] for b in all_bboxes),
        max(b[3] for b in all_bboxes),
    )


def _warn_on_mismatches(metadata_list: Sequence[object]) -> None:
    """Warn if CRS or geometry types differ across items."""
    import warnings

    crs_values = {getattr(m, "crs", None) for m in metadata_list} - {None}
    if len(crs_values) > 1:
        warnings.warn(
            f"CRS mismatch detected across items: {crs_values}. Using first item's CRS.",
            UserWarning,
            stacklevel=3,
        )

    geometry_types = {getattr(m, "geometry_type", None) for m in metadata_list} - {None}
    if len(geometry_types) > 1:
        warnings.warn(
            f"Mixed geometry types detected: {geometry_types}. Using first item's type.",
            UserWarning,
            stacklevel=3,
        )


def aggregate_table_metadata(metadata_list: Sequence[object]) -> object:
    """Aggregate table metadata from multiple vector items for collection-level extension.

    Used to combine metadata from multiple GeoParquet files in a collection:
    - Computes union bbox (encompassing all items)
    - Sums row_count (feature_count) across all items
    - Merges schemas (union of all column names, warns on type conflicts)
    - Uses first item's geometry_column
    - Warns if CRS values differ across items

    Args:
        metadata_list: Sequence of GeoParquetMetadata objects.

    Returns:
        A GeoParquetMetadata object with aggregated values.

    Raises:
        ValueError: If metadata_list is empty or no items have valid bboxes.
    """
    import warnings

    from portolan_cli.metadata.geoparquet import GeoParquetMetadata

    if not metadata_list:
        raise ValueError("Cannot aggregate empty metadata list")

    total_row_count = sum(getattr(m, "feature_count", 0) or 0 for m in metadata_list)
    merged_schema, schema_conflicts = _merge_schemas(metadata_list)

    if schema_conflicts:
        warnings.warn(
            f"Schema type conflicts detected (first type wins): {'; '.join(schema_conflicts)}",
            UserWarning,
            stacklevel=2,
        )

    union_bbox = _compute_bbox_union(metadata_list)
    _warn_on_mismatches(metadata_list)

    first = metadata_list[0]
    return GeoParquetMetadata(
        bbox=union_bbox,
        crs=getattr(first, "crs", None) or "EPSG:4326",
        geometry_type=getattr(first, "geometry_type", None),
        geometry_column=getattr(first, "geometry_column", None) or "geometry",
        feature_count=total_row_count,
        schema=merged_schema,
    )


def add_projection_extension(
    item: pystac.Item,
    metadata: object,
) -> None:
    """Add Projection extension fields to an item from metadata.

    Always sets (when available):
    - proj:code: CRS code (EPSG or WKT)
    - proj:bbox: Bounding box in native CRS

    For raster metadata (COGMetadata), also sets:
    - proj:shape: [height, width] in pixels
    - proj:transform: GDAL GeoTransform array

    Args:
        item: The item to add extension fields to.
        metadata: A metadata object with crs and bbox attributes.
                 For rasters, should also have width, height, and transform.
    """
    if not hasattr(metadata, "crs") or metadata.crs is None:
        return

    # Set proj:code
    crs_str = metadata.crs
    if isinstance(crs_str, str):
        # Normalize EPSG codes
        if crs_str.upper().startswith("EPSG:"):
            item.properties["proj:code"] = crs_str.upper()
        else:
            # WKT or other format - store as-is
            item.properties["proj:code"] = crs_str
    elif isinstance(crs_str, dict):
        # PROJJSON format (used by some GeoParquet files)
        # Try to extract EPSG code if available
        if "id" in crs_str and "code" in crs_str["id"]:
            authority = crs_str["id"].get("authority", "EPSG")
            code = crs_str["id"]["code"]
            item.properties["proj:code"] = f"{authority}:{code}"
        else:
            # Store as WKT2 if we can't extract EPSG
            item.properties["proj:code"] = str(crs_str)

    # Set proj:bbox (native CRS bbox)
    if hasattr(metadata, "bbox") and metadata.bbox is not None:
        item.properties["proj:bbox"] = list(metadata.bbox)

    # Set raster-specific fields if available (COGMetadata)
    # proj:shape is [height, width] per the extension spec
    # Check for actual int values, not just attribute existence (MagicMock creates attributes dynamically)
    height = getattr(metadata, "height", None)
    width = getattr(metadata, "width", None)
    if isinstance(height, int) and isinstance(width, int):
        item.properties["proj:shape"] = [height, width]

    # proj:transform is GDAL GeoTransform array
    transform = getattr(metadata, "transform", None)
    if transform is not None and isinstance(transform, (list, tuple)):
        item.properties["proj:transform"] = list(transform)

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["projection"]
    if ext_url not in (item.stac_extensions or []):
        if item.stac_extensions is None:
            item.stac_extensions = []
        item.stac_extensions.append(ext_url)


def add_vector_extension(
    item: pystac.Item,
    metadata: object,
) -> None:
    """Add Vector extension fields to an item from GeoParquet metadata.

    Sets vector:geometry_types based on the geometry type(s) in the metadata.
    Per ADR-0037: Use experimental extensions (Vector v0.1.0 is Proposal maturity).

    Args:
        item: The STAC item to add extension fields to.
        metadata: A metadata object with geometry_type attribute (str or list).
    """
    if not hasattr(metadata, "geometry_type") or metadata.geometry_type is None:
        return

    # geometry_types is an array per spec
    geometry_types = metadata.geometry_type
    if isinstance(geometry_types, str):
        geometry_types = [geometry_types]

    item.properties["vector:geometry_types"] = geometry_types

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["vector"]
    if ext_url not in (item.stac_extensions or []):
        if item.stac_extensions is None:
            item.stac_extensions = []
        item.stac_extensions.append(ext_url)


# Per ADR-0036: Hybrid field detection for collection summaries
# Explicit fields with known strategies; auto-detect extension-prefixed fields
SUMMARIZED_FIELDS: dict[str, SummaryStrategy] = {
    "proj:code": SummaryStrategy.ARRAY,  # Distinct CRS codes
    "vector:geometry_types": SummaryStrategy.ARRAY,  # Distinct geometry types
    "gsd": SummaryStrategy.RANGE,  # Ground sample distance range
}


def update_collection_summaries(collection: pystac.Collection) -> None:
    """Update collection summaries from item properties.

    Uses PySTAC's Summarizer with hybrid field detection:
    - Explicit strategies for core fields (proj:code, vector:geometry_types, gsd)
    - Auto-detect extension-prefixed fields (custom:*, etc.)

    Per ADR-0036: Categorical fields only, no numeric aggregation across items.

    Args:
        collection: The collection to update summaries for.
    """
    items = list(collection.get_items(recursive=True))
    if not items:
        return

    # Build field strategies: explicit + auto-detected extension prefixes
    field_strategies = dict(SUMMARIZED_FIELDS)

    # Auto-detect extension-prefixed fields from items (not in explicit list)
    for item in items:
        for key in item.properties:
            if ":" in key and key not in field_strategies:
                # Extension-prefixed field, default to ARRAY (distinct values)
                field_strategies[key] = SummaryStrategy.ARRAY

    summarizer = Summarizer(field_strategies)
    collection.summaries = summarizer.summarize(items)
