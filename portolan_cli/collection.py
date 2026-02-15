"""Collection creation and management.

Provides functions to create CollectionModel from data files,
extract extent information, and write collection metadata to JSON.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
from portolan_cli.models.collection import (
    CollectionModel,
    ExtentModel,
    SpatialExtent,
    TemporalExtent,
)
from portolan_cli.models.schema import SchemaModel


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
