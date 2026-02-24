"""Dataset orchestration module - manages the dataset add/list/info/remove workflow.

This module orchestrates the complete workflow for managing datasets in a
Portolan catalog:
1. Format detection (route to vector or raster handler)
2. Conversion to cloud-native format (GeoParquet or COG)
3. Metadata extraction
4. STAC item/collection creation
5. versions.json update
6. File staging

Per ADR-0007, all logic lives here; the CLI is a thin wrapper.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import pystac

from portolan_cli.formats import FormatType, detect_format
from portolan_cli.metadata import (
    extract_cog_metadata,
    extract_geoparquet_metadata,
)
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.geoparquet import GeoParquetMetadata
from portolan_cli.stac import (
    add_item_to_collection,
    create_collection,
    create_item,
    load_catalog,
)
from portolan_cli.versions import (
    Asset,
    VersionsFile,
    add_version,
    read_versions,
    write_versions,
)


@dataclass
class DatasetInfo:
    """Information about a dataset in the catalog.

    Attributes:
        item_id: STAC item identifier.
        collection_id: Parent collection identifier.
        format_type: Vector or raster format.
        bbox: Bounding box [min_x, min_y, max_x, max_y].
        asset_paths: Paths to data assets.
        title: Optional display title.
        description: Optional description.
        datetime: Acquisition/creation datetime.
    """

    item_id: str
    collection_id: str
    format_type: FormatType
    bbox: list[float]
    asset_paths: list[str] = field(default_factory=list)
    title: str | None = None
    description: str | None = None
    datetime: datetime | None = None


def add_dataset(
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    title: str | None = None,
    description: str | None = None,
    item_id: str | None = None,
) -> DatasetInfo:
    """Add a dataset to a Portolan catalog.

    This is the main orchestration function that:
    1. Detects the format type
    2. Converts to cloud-native format if needed
    3. Extracts metadata
    4. Creates/updates STAC collection and item
    5. Updates versions.json
    6. Stages files to .portolan/

    Args:
        path: Path to the source file.
        catalog_root: Root directory containing .portolan/.
        collection_id: Collection to add the dataset to.
        title: Optional display title for the dataset.
        description: Optional description.
        item_id: Optional item ID (defaults to filename stem).

    Returns:
        DatasetInfo with details about the added dataset.

    Raises:
        ValueError: If the format is unsupported.
        FileNotFoundError: If the source file doesn't exist.
    """
    # Step 1: Detect format
    format_type = detect_format(path)
    if format_type == FormatType.UNKNOWN:
        raise ValueError(f"Unsupported format: {path.suffix}")

    # Generate item ID from filename if not provided
    if item_id is None:
        item_id = path.stem

    # Set up paths
    portolan_dir = catalog_root / ".portolan"
    collection_dir = portolan_dir / "collections" / collection_id
    item_dir = collection_dir / item_id

    # Ensure directories exist
    item_dir.mkdir(parents=True, exist_ok=True)

    # Step 2: Convert to cloud-native format
    metadata: GeoParquetMetadata | COGMetadata
    if format_type == FormatType.VECTOR:
        output_path = convert_vector(path, item_dir)
        metadata = extract_geoparquet_metadata(output_path)
        media_type = "application/x-parquet"
    else:  # RASTER
        output_path = convert_raster(path, item_dir)
        metadata = extract_cog_metadata(output_path)
        media_type = "image/tiff; application=geotiff; profile=cloud-optimized"

    # Step 3: Compute checksum
    checksum = compute_checksum(output_path)

    # Step 4: Extract bbox (handle tuple -> list conversion)
    if not metadata.bbox:
        raise ValueError(
            f"Cannot create STAC item for '{metadata.id if hasattr(metadata, 'id') else path.stem}': "
            f"missing bounding box. The source file may have no valid geometry."
        )
    bbox = list(metadata.bbox)

    # Step 5: Create STAC item
    stac_properties = metadata.to_stac_properties()
    if title:
        stac_properties["title"] = title
    if description:
        stac_properties["description"] = description

    item = create_item(
        item_id=item_id,
        bbox=bbox,
        properties=stac_properties,
        assets={
            "data": pystac.Asset(
                href=output_path.name,
                media_type=media_type,
                roles=["data"],
            )
        },
    )

    # Step 6: Load or create collection
    collection = _get_or_create_collection(
        portolan_dir=portolan_dir,
        collection_id=collection_id,
        initial_bbox=bbox,
    )

    # Add item to collection
    add_item_to_collection(collection, item, update_extent=True)

    # Step 7: Save collection
    collection.normalize_hrefs(str(collection_dir))
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # Step 8: Update catalog to link to collection (if new)
    _update_catalog_links(portolan_dir, collection_id)

    # Step 9: Update versions.json
    _update_versions(
        collection_dir=collection_dir,
        item_id=item_id,
        output_path=output_path,
        checksum=checksum,
    )

    return DatasetInfo(
        item_id=item_id,
        collection_id=collection_id,
        format_type=format_type,
        bbox=bbox,
        asset_paths=[str(output_path)],
        title=title,
        description=description,
    )


def convert_vector(source: Path, dest_dir: Path) -> Path:
    """Convert vector file to GeoParquet.

    Args:
        source: Source vector file.
        dest_dir: Destination directory.

    Returns:
        Path to the output GeoParquet file.
    """
    import geoparquet_io as gpio  # type: ignore[import-untyped]

    output_path = dest_dir / f"{source.stem}.parquet"

    # Check if already GeoParquet
    if source.suffix.lower() == ".parquet":
        shutil.copy2(source, output_path)
        return output_path

    # Convert using geoparquet-io fluent API
    gpio.convert(str(source)).write(str(output_path))

    return output_path


def convert_raster(source: Path, dest_dir: Path) -> Path:
    """Convert raster file to COG.

    Uses Portolan's opinionated COG defaults (see convert command design):
    - DEFLATE compression (universal compatibility, lossless)
    - Predictor=2 (horizontal differencing, improves compression)
    - 512x512 tiles (matches rio-cogeo default, fewer HTTP requests)
    - Nearest resampling (safe for all data types: categorical, imagery, elevation)

    For fine-tuned control, power users should use rio_cogeo.cog_translate() directly.

    Args:
        source: Source raster file.
        dest_dir: Destination directory.

    Returns:
        Path to the output COG file.
    """
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    output_path = dest_dir / f"{source.stem}.tif"

    # Check if already COG (simple heuristic: .tif extension)
    # In production, should validate with rio_cogeo.cogeo.cog_validate
    if source.suffix.lower() in (".tif", ".tiff"):
        # For now, assume .tif files need conversion
        # TODO: Add proper COG validation
        pass

    # Convert using rio-cogeo with Portolan's opinionated defaults
    profile = cog_profiles.get("deflate")  # type: ignore[no-untyped-call]

    # Apply predictor=2 for better compression
    # Note: profile is a copy of the deflate profile dict
    profile["predictor"] = 2

    cog_translate(
        str(source),
        str(output_path),
        profile,
        quiet=True,
        overview_resampling="nearest",  # Safe for all data types
    )

    return output_path


def compute_checksum(path: Path) -> str:
    """Compute SHA-256 checksum of a file securely.

    Security: Validates the resolved path is a regular file to prevent
    symlink attacks (MAJOR #5 - symlink security vulnerability).

    Args:
        path: Path to the file.

    Returns:
        Hex-encoded SHA-256 checksum.

    Raises:
        ValueError: If path is not a regular file (e.g., symlink to directory,
            device file, or other non-regular file).
        FileNotFoundError: If path does not exist.
    """
    # Resolve symlinks and check it's a regular file (MAJOR #5)
    resolved = path.resolve()
    if not resolved.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not resolved.is_file():
        raise ValueError(f"Not a regular file: {path} (resolves to {resolved})")

    sha256 = hashlib.sha256()
    with open(resolved, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def _get_or_create_collection(
    portolan_dir: Path,
    collection_id: str,
    initial_bbox: list[float],
) -> pystac.Collection:
    """Load existing collection or create new one.

    Args:
        portolan_dir: Path to .portolan directory.
        collection_id: Collection identifier.
        initial_bbox: Initial bounding box for new collections.

    Returns:
        pystac.Collection object.
    """
    collection_path = portolan_dir / "collections" / collection_id / "collection.json"

    if collection_path.exists():
        return pystac.Collection.from_file(str(collection_path))

    # Create new collection
    return create_collection(
        collection_id=collection_id,
        description=f"Collection: {collection_id}",
        bbox=initial_bbox,
    )


def _update_catalog_links(portolan_dir: Path, collection_id: str) -> None:
    """Ensure catalog has link to collection.

    Args:
        portolan_dir: Path to .portolan directory.
        collection_id: Collection identifier.
    """
    catalog_path = portolan_dir / "catalog.json"
    catalog = load_catalog(catalog_path)

    # Normalize hrefs to ensure consistent comparison
    catalog.normalize_hrefs(str(portolan_dir))

    # Extract collection IDs from existing child links
    # Links may be in various formats: "./collections/{id}/collection.json" or absolute paths
    existing_collection_ids: set[str] = set()
    for link in catalog.links:
        if link.rel != "child":
            continue
        href = link.href or ""
        # Extract collection ID from href pattern: .../collections/{id}/collection.json
        if "/collections/" in href and href.endswith("/collection.json"):
            # Parse: anything/collections/{collection_id}/collection.json
            parts = href.split("/collections/")[-1]
            coll_id = parts.split("/")[0]
            existing_collection_ids.add(coll_id)

    if collection_id not in existing_collection_ids:
        collection_href = f"./collections/{collection_id}/collection.json"
        catalog.add_link(pystac.Link(rel="child", target=collection_href))
        # Re-save catalog
        catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def _update_versions(
    collection_dir: Path,
    item_id: str,
    output_path: Path,
    checksum: str,
) -> None:
    """Update versions.json with new asset.

    Args:
        collection_dir: Path to collection directory.
        item_id: Item identifier.
        output_path: Path to the output file.
        checksum: SHA-256 checksum.
    """
    versions_path = collection_dir / "versions.json"

    if versions_path.exists():
        versions_file = read_versions(versions_path)
    else:
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )

    # Compute new version
    if versions_file.current_version is None:
        new_version = "1.0.0"
    else:
        # Simple version increment (could be smarter)
        parts = versions_file.current_version.split(".")
        parts[-1] = str(int(parts[-1]) + 1)
        new_version = ".".join(parts)

    assets = {
        output_path.name: Asset(
            sha256=checksum,
            size_bytes=output_path.stat().st_size,
            href=output_path.name,
        )
    }

    updated = add_version(
        versions_file,
        version=new_version,
        assets=assets,
        breaking=False,
    )

    write_versions(versions_path, updated)


def list_datasets(
    catalog_root: Path,
    collection_id: str | None = None,
) -> list[DatasetInfo]:
    """List datasets in a Portolan catalog.

    Args:
        catalog_root: Root directory containing .portolan/.
        collection_id: Optional collection to filter by.

    Returns:
        List of DatasetInfo objects.
    """
    portolan_dir = catalog_root / ".portolan"
    catalog_path = portolan_dir / "catalog.json"

    if not catalog_path.exists():
        return []

    datasets: list[DatasetInfo] = []

    # Scan collections
    collections_dir = portolan_dir / "collections"
    if not collections_dir.exists():
        return []

    for col_dir in collections_dir.iterdir():
        if not col_dir.is_dir():
            continue

        col_id = col_dir.name

        # Filter by collection if specified
        if collection_id and col_id != collection_id:
            continue

        collection_path = col_dir / "collection.json"
        if not collection_path.exists():
            continue

        # Load collection to get items
        collection_data = json.loads(collection_path.read_text())

        for link in collection_data.get("links", []):
            if link.get("rel") != "item":
                continue

            # Parse item href to get item ID
            item_href = link.get("href", "")
            # href is like ./item-id/item-id.json
            item_id = item_href.split("/")[1] if "/" in item_href else item_href

            # Load item
            item_path = col_dir / item_href.removeprefix("./")
            if not item_path.exists():
                continue

            item_data = json.loads(item_path.read_text())

            # Determine format from assets
            format_type = FormatType.UNKNOWN
            asset_paths: list[str] = []
            for _asset_key, asset in item_data.get("assets", {}).items():
                href = asset.get("href", "")
                asset_paths.append(href)
                if href.endswith(".parquet"):
                    format_type = FormatType.VECTOR
                elif href.endswith(".tif"):
                    format_type = FormatType.RASTER

            datasets.append(
                DatasetInfo(
                    item_id=item_data.get("id", item_id),
                    collection_id=col_id,
                    format_type=format_type,
                    bbox=item_data.get("bbox", [0, 0, 0, 0]),
                    asset_paths=asset_paths,
                    title=item_data.get("properties", {}).get("title"),
                    description=item_data.get("properties", {}).get("description"),
                )
            )

    return datasets


def get_dataset_info(
    catalog_root: Path,
    dataset_id: str,
) -> DatasetInfo:
    """Get information about a specific dataset.

    Args:
        catalog_root: Root directory containing .portolan/.
        dataset_id: Dataset identifier in format "collection/item".

    Returns:
        DatasetInfo for the requested dataset.

    Raises:
        KeyError: If the dataset doesn't exist.
    """
    if "/" not in dataset_id:
        raise KeyError(f"Dataset not found: {dataset_id} (expected format: collection/item)")

    collection_id, item_id = dataset_id.split("/", 1)

    portolan_dir = catalog_root / ".portolan"
    item_path = portolan_dir / "collections" / collection_id / item_id / f"{item_id}.json"

    if not item_path.exists():
        raise KeyError(f"Dataset not found: {dataset_id}")

    item_data = json.loads(item_path.read_text())

    # Determine format from assets
    format_type = FormatType.UNKNOWN
    asset_paths: list[str] = []
    for asset in item_data.get("assets", {}).values():
        href = asset.get("href", "")
        asset_paths.append(href)
        if href.endswith(".parquet"):
            format_type = FormatType.VECTOR
        elif href.endswith(".tif"):
            format_type = FormatType.RASTER

    return DatasetInfo(
        item_id=item_data.get("id", item_id),
        collection_id=collection_id,
        format_type=format_type,
        bbox=item_data.get("bbox", [0, 0, 0, 0]),
        asset_paths=asset_paths,
        title=item_data.get("properties", {}).get("title"),
        description=item_data.get("properties", {}).get("description"),
    )


def remove_dataset(
    catalog_root: Path,
    dataset_id: str,
    *,
    remove_collection: bool = False,
) -> None:
    """Remove a dataset from a Portolan catalog.

    Args:
        catalog_root: Root directory containing .portolan/.
        dataset_id: Dataset identifier in format "collection/item" or just "collection".
        remove_collection: If True, remove entire collection.

    Raises:
        KeyError: If the dataset doesn't exist.
    """
    portolan_dir = catalog_root / ".portolan"

    if remove_collection or "/" not in dataset_id:
        # Remove entire collection
        collection_id = dataset_id.split("/")[0]
        collection_dir = portolan_dir / "collections" / collection_id

        if not collection_dir.exists():
            raise KeyError(f"Dataset not found: {dataset_id}")

        # Remove collection directory
        shutil.rmtree(collection_dir)

        # Update catalog links
        catalog_path = portolan_dir / "catalog.json"
        if catalog_path.exists():
            catalog_data = json.loads(catalog_path.read_text())
            catalog_data["links"] = [
                link
                for link in catalog_data.get("links", [])
                if not link.get("href", "").endswith(f"/{collection_id}/collection.json")
            ]
            catalog_path.write_text(json.dumps(catalog_data, indent=2))
    else:
        # Remove single item
        collection_id, item_id = dataset_id.split("/", 1)
        item_dir = portolan_dir / "collections" / collection_id / item_id

        if not item_dir.exists():
            raise KeyError(f"Dataset not found: {dataset_id}")

        # Remove item directory
        shutil.rmtree(item_dir)

        # Update collection links
        collection_path = portolan_dir / "collections" / collection_id / "collection.json"
        if collection_path.exists():
            collection_data = json.loads(collection_path.read_text())
            collection_data["links"] = [
                link
                for link in collection_data.get("links", [])
                if not link.get("href", "").startswith(f"./{item_id}/")
            ]
            collection_path.write_text(json.dumps(collection_data, indent=2))


# ─────────────────────────────────────────────────────────────────────────────
# Directory handling
# ─────────────────────────────────────────────────────────────────────────────

# Extensions we recognize as geospatial
GEOSPATIAL_EXTENSIONS: frozenset[str] = frozenset(
    {
        ".geojson",
        ".parquet",
        ".shp",
        ".gpkg",
        ".fgb",
        ".tif",
        ".tiff",
        ".jp2",
    }
)


def iter_geospatial_files(
    path: Path,
    *,
    recursive: bool = True,
) -> list[Path]:
    """Iterate over geospatial files in a directory.

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of paths to geospatial files.
    """
    if not path.is_dir():
        return []

    files: list[Path] = []

    if recursive:
        for item in path.rglob("*"):
            if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                files.append(item)
    else:
        for item in path.iterdir():
            if item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                files.append(item)

    return sorted(files)


def add_directory(
    *,
    path: Path,
    catalog_root: Path,
    collection_id: str,
    recursive: bool = True,
) -> list[DatasetInfo]:
    """Add all geospatial files in a directory to a collection.

    Args:
        path: Directory containing geospatial files.
        catalog_root: Root directory containing .portolan/.
        collection_id: Collection to add datasets to.
        recursive: If True, process subdirectories recursively.

    Returns:
        List of DatasetInfo for each added dataset.
    """
    files = iter_geospatial_files(path, recursive=recursive)

    results: list[DatasetInfo] = []
    for file_path in files:
        result = add_dataset(
            path=file_path,
            catalog_root=catalog_root,
            collection_id=collection_id,
        )
        results.append(result)

    return results
