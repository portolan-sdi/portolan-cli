"""Collection creation and management.

Provides functions to create CollectionModel from data files,
extract extent information, and write collection metadata to JSON.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

from portolan_cli.bbox import to_2d_bbox
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
    TemporalExtent,
)
from portolan_cli.models.schema import SchemaModel

logger = logging.getLogger(__name__)


def create_collection(
    collection_id: str,
    data_path: Path,
    description: str,
    *,
    title: str | None = None,
    license: str = "CC-BY-4.0",
) -> CollectionModel:
    """Create a CollectionModel from a data file.

    Extracts spatial extent from the data file metadata.

    Args:
        collection_id: Unique collection identifier.
        data_path: Path to data file (GeoParquet or COG).
        description: Collection description.
        title: Optional human-readable title.
        license: SPDX license identifier.

    Returns:
        CollectionModel with extracted metadata.

    Raises:
        FileNotFoundError: If data_path doesn't exist.
    """
    if not data_path.exists():
        raise FileNotFoundError(f"Data file not found: {data_path}")

    # Capture single timestamp for consistency across all fields
    now = datetime.now(timezone.utc)

    # Extract metadata based on file type
    extent = _extract_extent_from_file(data_path, timestamp=now)

    # Create collection with extracted metadata
    collection = CollectionModel(
        id=collection_id,
        description=description,
        extent=extent,
        title=title,
        license=license,
        created=now,
        updated=now,
    )

    return collection


def _extract_extent_from_file(
    path: Path,
    *,
    timestamp: datetime | None = None,
) -> ExtentModel:
    """Extract spatial and temporal extent from a data file.

    Args:
        path: Path to data file.
        timestamp: Optional timestamp to use for temporal interval start.
            If None, uses current UTC time.

    Returns:
        ExtentModel with spatial bbox.
    """
    suffix = path.suffix.lower()
    bbox: list[list[float]]

    if suffix in (".parquet", ".geoparquet"):
        # Deferred import: portolan_cli.metadata.update imports from this module,
        # so a module-level import here would create a circular import.
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

        gp_metadata = extract_geoparquet_metadata(path)
        if gp_metadata.bbox:
            bbox = [list(gp_metadata.bbox)]
        else:
            # Default to global extent if no bbox
            bbox = [[-180.0, -90.0, 180.0, 90.0]]
    elif suffix in (".tif", ".tiff"):
        from portolan_cli.metadata.cog import extract_cog_metadata

        cog_metadata = extract_cog_metadata(path)
        if cog_metadata and cog_metadata.bbox:
            bbox = [list(cog_metadata.bbox)]
        else:
            # Default to global extent if no bbox
            bbox = [[-180.0, -90.0, 180.0, 90.0]]
    else:
        # Default to global extent
        bbox = [[-180.0, -90.0, 180.0, 90.0]]

    # Use provided timestamp or current time
    interval_start = (timestamp or datetime.now(timezone.utc)).isoformat()

    # Create extent with spatial bbox and open temporal interval
    return ExtentModel(
        spatial=SpatialExtent(bbox=bbox),
        temporal=TemporalExtent(interval=[[interval_start, None]]),
    )


def write_collection_json(collection: CollectionModel, path: Path) -> Path:
    """Write collection metadata to JSON file.

    Args:
        collection: CollectionModel to write.
        path: Directory to write to.

    Returns:
        Path to written file.
    """
    path.mkdir(parents=True, exist_ok=True)
    output_path = path / "collection.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(collection.to_dict(), f, indent=2)

    return output_path


def write_schema_json(schema: SchemaModel, path: Path) -> Path:
    """Write schema metadata to JSON file.

    Args:
        schema: SchemaModel to write.
        path: Directory to write to.

    Returns:
        Path to written file.
    """
    path.mkdir(parents=True, exist_ok=True)
    output_path = path / "schema.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(schema.to_dict(), f, indent=2)

    return output_path


def read_collection_json(path: Path) -> CollectionModel:
    """Read collection metadata from JSON file.

    Args:
        path: Path to collection.json file.

    Returns:
        CollectionModel loaded from file.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as f:
        data = json.load(f)

    return CollectionModel.from_dict(data)


def read_schema_json(path: Path) -> SchemaModel:
    """Read schema metadata from JSON file.

    Args:
        path: Path to schema.json file.

    Returns:
        SchemaModel loaded from file.

    Raises:
        FileNotFoundError: If file doesn't exist.
    """
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")

    with open(path) as f:
        data = json.load(f)

    return SchemaModel.from_dict(data)


def _get_sibling_collection_bboxes(catalog_root: Path) -> list[list[float]]:
    """Get bounding boxes from all sibling collections in the catalog (Issue #432).

    Scans the catalog for child collection links and extracts their spatial extents.
    Used for AOI inheritance when creating tabular-only collections.

    Args:
        catalog_root: Root directory of the catalog.

    Returns:
        List of bboxes [west, south, east, north] from sibling collections.
        Empty list if no collections with valid extents found.
    """
    catalog_path = catalog_root / "catalog.json"
    if not catalog_path.exists():
        return []

    try:
        with open(catalog_path) as f:
            catalog_data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return []

    bboxes: list[list[float]] = []

    # Find child links to collections
    for link in catalog_data.get("links", []):
        if link.get("rel") != "child":
            continue

        href = link.get("href", "")
        if not href.endswith("collection.json"):
            continue

        # Security: Validate path is within catalog_root (ADR-0030 path hardening)
        # Prevents path traversal via malicious hrefs like "../../../etc/passwd"
        try:
            collection_path = (catalog_root / href).resolve()
            # Ensure resolved path is within catalog_root
            collection_path.relative_to(catalog_root.resolve())
        except ValueError:
            # Path is outside catalog_root - skip silently (path traversal attempt)
            continue

        if not collection_path.exists():
            continue

        try:
            with open(collection_path) as f:
                collection_data = json.load(f)

            # Extract bbox from extent
            extent = collection_data.get("extent", {})
            spatial = extent.get("spatial", {})
            bbox_list = spatial.get("bbox", [])

            if bbox_list and len(bbox_list) > 0:
                bbox = bbox_list[0]
                # Validate bbox format: [west, south, east, north] or 3D variant
                # STAC allows 6-element bboxes for 3D: [west, south, min_z, east, north, max_z]
                # We use only the 2D components for union computation
                if (
                    isinstance(bbox, list)
                    and len(bbox) in (4, 6)
                    and all(isinstance(x, (int, float)) for x in bbox)
                ):
                    bboxes.append(to_2d_bbox(bbox))

        except (json.JSONDecodeError, OSError, KeyError):
            continue

    return bboxes


def _compute_union_bbox(bboxes: list[list[float]]) -> list[float]:
    """Compute the union (enclosing) bounding box from multiple bboxes.

    Filters out invalid bboxes (inf/nan/out-of-range) with warnings (issue #516).
    Handles antimeridian-crossing bboxes properly.

    Args:
        bboxes: List of bboxes, each [west, south, east, north].

    Returns:
        Union bbox [min_west, min_south, max_east, max_north].
        Returns global fallback if all bboxes are invalid.
    """
    from portolan_cli.bbox import compute_bbox_union

    if not bboxes:
        return [-180.0, -90.0, 180.0, 90.0]  # Global fallback

    result = compute_bbox_union(bboxes)
    if result.bbox is None:
        # All bboxes were invalid - return global fallback
        return [-180.0, -90.0, 180.0, 90.0]

    return result.bbox


def _get_metadata_yaml_bbox(collection_dir: Path) -> list[float] | None:
    """Check metadata.yaml for explicit bbox (ADR-0047 priority 1).

    Args:
        collection_dir: Path to the collection directory.

    Returns:
        Bbox [west, south, east, north] if found in metadata.yaml, None otherwise.
    """
    metadata_path = collection_dir / "metadata.yaml"
    if not metadata_path.exists():
        return None

    try:
        import yaml

        with open(metadata_path) as f:
            metadata = yaml.safe_load(f) or {}

        # Check for explicit bbox in metadata.yaml
        # Supported formats: extent.bbox or just bbox at top level
        bbox = metadata.get("bbox")
        if bbox is None:
            extent = metadata.get("extent", {})
            if isinstance(extent, dict):
                bbox = extent.get("bbox")

        # Validate bbox format (4-element 2D or 6-element 3D)
        if (
            isinstance(bbox, list)
            and len(bbox) in (4, 6)
            and all(isinstance(x, (int, float)) for x in bbox)
        ):
            # Return 2D [west, south, east, north] for consistency
            return to_2d_bbox(bbox)

    except Exception as e:
        # Any error reading/parsing metadata.yaml - fall back to inheritance
        logger.debug("Error reading bbox from %s: %s", metadata_path, e)

    return None
