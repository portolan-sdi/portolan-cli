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


def find_catalog_root(start_path: Path | None = None) -> Path | None:
    """Find the catalog root by walking up from the given path.

    Searches for catalog.json starting from start_path (or cwd if None)
    and walking up parent directories. This provides git-style behavior
    where commands work from any subdirectory within a catalog.

    Args:
        start_path: Starting directory for search (defaults to cwd).

    Returns:
        Path to catalog root if found, None otherwise.
    """
    current = (start_path or Path.cwd()).resolve()

    # Walk up until we find catalog.json or hit the filesystem root
    while current != current.parent:
        if (current / "catalog.json").exists():
            return current
        current = current.parent

    # Check the root itself
    if (current / "catalog.json").exists():
        return current

    return None


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

    STAC structure (per ADR-0023):
    - Collection: {catalog_root}/{collection_id}/collection.json
    - Item: {catalog_root}/{collection_id}/{item_id}/{item_id}.json
    - Versions: {catalog_root}/{collection_id}/versions.json

    Args:
        path: Path to the source file.
        catalog_root: Root directory of the catalog.
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

    # Set up paths (STAC at root, per ADR-0023)
    collection_dir = catalog_root / collection_id
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
        catalog_root=catalog_root,
        collection_id=collection_id,
        initial_bbox=bbox,
    )

    # Add item to collection
    add_item_to_collection(collection, item, update_extent=True)

    # Step 7: Save collection
    collection.normalize_hrefs(str(collection_dir))
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # Step 8: Update catalog to link to collection (if new)
    _update_catalog_links(catalog_root, collection_id)

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
        # If source is already at the destination, no copy needed
        if source.resolve() == output_path.resolve():
            return output_path
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
    catalog_root: Path,
    collection_id: str,
    initial_bbox: list[float],
) -> pystac.Collection:
    """Load existing collection or create new one.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        initial_bbox: Initial bounding box for new collections.

    Returns:
        pystac.Collection object.
    """
    # STAC at root level (per ADR-0023)
    collection_path = catalog_root / collection_id / "collection.json"

    if collection_path.exists():
        return pystac.Collection.from_file(str(collection_path))

    # Create new collection
    return create_collection(
        collection_id=collection_id,
        description=f"Collection: {collection_id}",
        bbox=initial_bbox,
    )


def _update_catalog_links(catalog_root: Path, collection_id: str) -> None:
    """Ensure catalog has link to collection.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
    """
    # Catalog at root level (per ADR-0023)
    catalog_path = catalog_root / "catalog.json"
    catalog = load_catalog(catalog_path)

    # Normalize hrefs to ensure consistent comparison
    catalog.normalize_hrefs(str(catalog_root))

    # Extract collection IDs from existing child links
    # Links are in format: "./{collection_id}/collection.json"
    existing_collection_ids: set[str] = set()
    for link in catalog.links:
        if link.rel != "child":
            continue
        href = link.href or ""
        # Extract collection ID from href pattern: ./{collection_id}/collection.json
        if href.endswith("/collection.json"):
            # Parse: ./{collection_id}/collection.json or {collection_id}/collection.json
            parts = href.replace("./", "").split("/")
            if len(parts) >= 2:
                coll_id = parts[0]
                existing_collection_ids.add(coll_id)

    if collection_id not in existing_collection_ids:
        collection_href = f"./{collection_id}/collection.json"
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
        catalog_root: Root directory of the catalog.
        collection_id: Optional collection to filter by.

    Returns:
        List of DatasetInfo objects.
    """
    # Catalog at root level (per ADR-0023)
    catalog_path = catalog_root / "catalog.json"

    if not catalog_path.exists():
        return []

    datasets: list[DatasetInfo] = []

    # Scan root-level directories for collections (per ADR-0023)
    for col_dir in catalog_root.iterdir():
        if not col_dir.is_dir():
            continue

        # Skip .portolan and hidden directories
        if col_dir.name.startswith("."):
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
        catalog_root: Root directory of the catalog.
        dataset_id: Dataset identifier in format "collection/item".

    Returns:
        DatasetInfo for the requested dataset.

    Raises:
        KeyError: If the dataset doesn't exist.
    """
    if "/" not in dataset_id:
        raise KeyError(f"Dataset not found: {dataset_id} (expected format: collection/item)")

    collection_id, item_id = dataset_id.split("/", 1)

    # STAC at root level (per ADR-0023)
    item_path = catalog_root / collection_id / item_id / f"{item_id}.json"

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
        catalog_root: Root directory of the catalog.
        dataset_id: Dataset identifier in format "collection/item" or just "collection".
        remove_collection: If True, remove entire collection.

    Raises:
        KeyError: If the dataset doesn't exist.
    """
    # STAC at root level (per ADR-0023)
    if remove_collection or "/" not in dataset_id:
        # Remove entire collection
        collection_id = dataset_id.split("/")[0]
        collection_dir = catalog_root / collection_id

        if not collection_dir.exists():
            raise KeyError(f"Dataset not found: {dataset_id}")

        # Remove collection directory
        shutil.rmtree(collection_dir)

        # Update catalog links
        catalog_path = catalog_root / "catalog.json"
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
        item_dir = catalog_root / collection_id / item_id

        if not item_dir.exists():
            raise KeyError(f"Dataset not found: {dataset_id}")

        # Remove item directory
        shutil.rmtree(item_dir)

        # Update collection links
        collection_path = catalog_root / collection_id / "collection.json"
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


# ─────────────────────────────────────────────────────────────────────────────
# Sidecar auto-detection (per issue #97)
# ─────────────────────────────────────────────────────────────────────────────

# Sidecar file patterns by primary file extension
SIDECAR_PATTERNS: dict[str, list[str]] = {
    ".shp": [".dbf", ".shx", ".prj", ".cpg", ".sbn", ".sbx", ".qix"],
    ".tif": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".tiff": [".tfw", ".xml", ".aux.xml", ".ovr"],
    ".img": [".ige", ".rrd", ".rde", ".xml"],
}


def get_sidecars(path: Path) -> list[Path]:
    """Detect sidecar files for a given primary file.

    Automatically finds associated files like .dbf/.shx/.prj for shapefiles,
    or .tfw/.xml for GeoTIFFs.

    Args:
        path: Path to the primary file.

    Returns:
        List of existing sidecar file paths (may be empty).
    """
    suffix_lower = path.suffix.lower()
    patterns = SIDECAR_PATTERNS.get(suffix_lower, [])

    sidecars: list[Path] = []
    stem = path.stem
    parent = path.parent

    for ext in patterns:
        sidecar_path = parent / f"{stem}{ext}"
        if sidecar_path.exists():
            sidecars.append(sidecar_path)

    return sidecars


def resolve_collection_id(path: Path, catalog_root: Path) -> str:
    """Resolve collection ID from a file path.

    Per ADR-0022: First path component (relative to catalog root) = collection ID.

    Args:
        path: Path to the file.
        catalog_root: Root directory of the catalog.

    Returns:
        Collection ID (first directory component relative to catalog).

    Raises:
        ValueError: If path is not inside catalog root.
    """
    # Get path relative to catalog root
    try:
        relative = path.resolve().relative_to(catalog_root.resolve())
    except ValueError as err:
        raise ValueError(f"Path {path} is outside catalog root {catalog_root}") from err

    # First component is the collection ID
    parts = relative.parts
    if not parts:
        raise ValueError(f"Cannot determine collection from path: {path}")

    # Skip the filename if path is a file
    if path.is_file() and len(parts) == 1:
        raise ValueError(f"File {path} must be in a subdirectory (collection)")

    return parts[0]


def is_current(
    path: Path,
    versions_path: Path,
) -> bool:
    """Check if a file is unchanged compared to versions.json.

    Uses mtime as fast-path (per ADR-0017), falls back to sha256 if mtime changed.

    Args:
        path: Path to the file to check.
        versions_path: Path to versions.json for this collection.

    Returns:
        True if file is unchanged (already tracked at current state),
        False if new or modified.
    """
    if not versions_path.exists():
        return False

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return False

    current_version = versions_file.versions[-1]

    # Look for this file in current version assets
    filename = path.name
    asset = current_version.assets.get(filename)
    if asset is None:
        # Also check for stem.parquet (converted name)
        parquet_name = f"{path.stem}.parquet"
        asset = current_version.assets.get(parquet_name)
        if asset is None:
            return False

    # Fast path: check mtime
    if asset.mtime is not None:
        current_mtime = path.stat().st_mtime
        if abs(current_mtime - asset.mtime) < 0.001:  # Within 1ms tolerance
            return True

    # Slow path: compare sha256
    current_checksum = compute_checksum(path)
    return current_checksum == asset.sha256


def add_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None = None,
    verbose: bool = False,
) -> tuple[list[DatasetInfo], list[Path]]:
    """Add files to a Portolan catalog.

    This is the main entry point for the `portolan add` command.
    Handles single files, directories, and sidecar auto-detection.

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
            If not provided, inferred from first path component.
        verbose: If True, return skipped files info.

    Returns:
        Tuple of (added_datasets, skipped_paths).
        added_datasets: List of DatasetInfo for newly added/updated files.
        skipped_paths: List of paths that were skipped (unchanged).
    """
    added: list[DatasetInfo] = []
    skipped: list[Path] = []
    processed_paths: set[Path] = set()

    for path in paths:
        if path.is_dir():
            # Add all files in directory
            files = iter_files_with_sidecars(path)
        else:
            # Single file + sidecars
            files = [path] + get_sidecars(path)

        for file_path in files:
            if file_path in processed_paths:
                continue
            processed_paths.add(file_path)

            # Skip non-geospatial files (sidecars are handled separately)
            if file_path.suffix.lower() not in GEOSPATIAL_EXTENSIONS:
                continue

            # Determine collection ID
            coll_id = collection_id
            if coll_id is None:
                coll_id = resolve_collection_id(file_path, catalog_root)

            # Check if unchanged
            versions_path = catalog_root / coll_id / "versions.json"
            if is_current(file_path, versions_path):
                skipped.append(file_path)
                continue

            # Add the file
            try:
                result = add_dataset(
                    path=file_path,
                    catalog_root=catalog_root,
                    collection_id=coll_id,
                )
                added.append(result)
            except (ValueError, FileNotFoundError) as err:
                # Re-raise with context
                raise type(err)(f"Failed to add {file_path}: {err}") from err

    return added, skipped


def iter_files_with_sidecars(path: Path, *, recursive: bool = True) -> list[Path]:
    """Iterate over all files in a directory (including sidecars).

    Unlike iter_geospatial_files, this returns ALL files in the directory,
    which is needed for directory add (per issue #97 design).

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of all file paths in the directory.
    """
    if not path.is_dir():
        return []

    files: list[Path] = []

    if recursive:
        for item in path.rglob("*"):
            if item.is_file():
                files.append(item)
    else:
        for item in path.iterdir():
            if item.is_file():
                files.append(item)

    return sorted(files)


def remove_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    keep: bool = False,
) -> list[Path]:
    """Remove files from Portolan catalog tracking.

    This is the main entry point for the `portolan rm` command.
    By default, deletes the file AND removes from tracking (git-style).
    With keep=True, removes from tracking but preserves the file.

    Args:
        paths: List of paths to remove (files or directories).
        catalog_root: Root directory of the catalog.
        keep: If True, preserve file on disk (only untrack).

    Returns:
        List of paths that were removed from tracking.
    """
    removed: list[Path] = []

    for path in paths:
        if path.is_dir():
            # Remove all files in directory
            files = list(path.rglob("*")) if path.exists() else []
            files = [f for f in files if f.is_file()]
        else:
            files = [path]

        for file_path in files:
            if not file_path.exists() and not keep:
                continue

            # Determine collection ID
            try:
                coll_id = resolve_collection_id(file_path, catalog_root)
            except ValueError:
                continue

            # Remove from versions.json
            versions_path = catalog_root / coll_id / "versions.json"
            if versions_path.exists():
                _remove_from_versions(file_path, versions_path)

            # Remove STAC item and files (unless --keep)
            if not keep:
                item_id = file_path.stem
                item_dir = catalog_root / coll_id / item_id
                if item_dir.exists() and item_dir.is_dir():
                    shutil.rmtree(item_dir)

                # Delete file from disk
                if file_path.exists():
                    file_path.unlink()

            removed.append(file_path)

    return removed


def _remove_from_versions(file_path: Path, versions_path: Path) -> None:
    """Remove a file from versions.json tracking.

    This creates a new version entry without the specified file.

    Args:
        file_path: Path to the file to untrack.
        versions_path: Path to the versions.json file.
    """
    if not versions_path.exists():
        return

    versions_file = read_versions(versions_path)
    if not versions_file.versions:
        return

    # Get current assets, removing the file
    current = versions_file.versions[-1]
    filename = file_path.name
    parquet_name = f"{file_path.stem}.parquet"

    new_assets = {
        name: asset
        for name, asset in current.assets.items()
        if name != filename and name != parquet_name
    }

    if len(new_assets) == len(current.assets):
        # File wasn't tracked, nothing to do
        return

    if not new_assets:
        # No assets left, could clean up versions.json
        # For now, just leave it with empty latest version
        pass

    # Compute new version number
    parts = (
        versions_file.current_version.split(".")
        if versions_file.current_version
        else ["0", "0", "0"]
    )
    parts[-1] = str(int(parts[-1]) + 1)
    new_version = ".".join(parts)

    # Create new version entry
    new_assets_typed = {
        name: Asset(
            sha256=asset.sha256,
            size_bytes=asset.size_bytes,
            href=asset.href,
            source_path=asset.source_path,
            source_mtime=asset.source_mtime,
            mtime=asset.mtime,
        )
        for name, asset in new_assets.items()
    }

    updated = add_version(
        versions_file,
        version=new_version,
        assets=new_assets_typed,
        breaking=False,
        message=f"Removed {filename}",
    )

    write_versions(versions_path, updated)
