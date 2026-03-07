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
import logging
import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import click
import pystac

from portolan_cli.collection_id import normalize_collection_id, validate_collection_id
from portolan_cli.constants import (
    GEOSPATIAL_EXTENSIONS,
    MTIME_TOLERANCE_SECONDS,
    SIDECAR_PATTERNS,
    TABULAR_EXTENSIONS,
)
from portolan_cli.formats import FormatType, detect_format, is_cloud_optimized_geotiff
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

logger = logging.getLogger(__name__)

# Error message patterns from geoparquet-io for non-geospatial CSV/TSV files.
# These specific patterns indicate the file lacks geometry columns (not other errors
# like permission denied, encoding issues, or memory errors).
# See: https://github.com/geoparquet/geoparquet-io (geometry detection logic)
_GEOPARQUET_IO_NO_GEOMETRY_PATTERNS: tuple[str, ...] = (
    "could not detect geometry columns",
    "geometry columns in csv",
    "geometry columns in tsv",
)

# Files to ignore when scanning item directories for assets.
# These are STAC/Portolan structural files, not user data.
IGNORED_FILES: frozenset[str] = frozenset(
    {
        "catalog.json",
        "collection.json",
        "versions.json",
    }
)

# Extension-to-MIME-type mapping for asset files.
_MEDIA_TYPE_MAP: dict[str, str] = {
    ".parquet": "application/x-parquet",
    ".tif": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".tiff": "image/tiff; application=geotiff; profile=cloud-optimized",
    ".geojson": "application/geo+json",
    ".json": "application/json",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".svg": "image/svg+xml",
    ".xml": "application/xml",
    ".csv": "text/csv",
    ".gpkg": "application/geopackage+sqlite3",
    ".fgb": "application/flatgeobuf",
    ".pmtiles": "application/vnd.pmtiles",
    ".shp": "application/x-shapefile",
    ".pdf": "application/pdf",
    ".txt": "text/plain",
    ".md": "text/markdown",
    ".html": "text/html",
}

# Extension-to-role mapping for asset files.
# Data formats get "data", images get "thumbnail", metadata gets "metadata".
_ROLE_MAP: dict[str, str] = {
    ".parquet": "data",
    ".tif": "data",
    ".tiff": "data",
    ".geojson": "data",
    ".gpkg": "data",
    ".fgb": "data",
    ".csv": "data",
    ".shp": "data",
    ".pmtiles": "data",
    ".png": "thumbnail",
    ".jpg": "thumbnail",
    ".jpeg": "thumbnail",
    ".svg": "thumbnail",
    ".xml": "metadata",
    ".json": "metadata",
    ".pdf": "documentation",
    ".txt": "documentation",
    ".md": "documentation",
    ".html": "documentation",
}


def _get_media_type(path: Path) -> str:
    """Determine MIME type from file extension.

    Args:
        path: Path to the file.

    Returns:
        MIME type string. Defaults to "application/octet-stream" for
        unknown extensions.
    """
    return _MEDIA_TYPE_MAP.get(path.suffix.lower(), "application/octet-stream")


def _get_asset_role(path: Path) -> str:
    """Determine STAC asset role from file extension.

    Args:
        path: Path to the file.

    Returns:
        Role string: "data", "thumbnail", "metadata", or "documentation".
        Defaults to "data" for unknown extensions.
    """
    return _ROLE_MAP.get(path.suffix.lower(), "data")


def _scan_item_assets(
    item_dir: Path,
    item_id: str,
    primary_file: Path,
) -> tuple[dict[str, pystac.Asset], dict[str, tuple[Path, str]], list[str]]:
    """Scan an item directory for all trackable assets.

    Per issue #133, ALL files in item directories are tracked as assets.
    Skips: directories, symlinks, hidden files, STAC structural files.

    Args:
        item_dir: Path to the item directory.
        item_id: Item identifier (for skipping item.json).
        primary_file: Path to the primary data file (gets "data" key).

    Returns:
        Tuple of (stac_assets, asset_files, asset_paths):
        - stac_assets: Dict mapping asset key to pystac.Asset
        - asset_files: Dict mapping filename to (path, checksum) tuples
        - asset_paths: List of absolute path strings
    """
    stac_assets: dict[str, pystac.Asset] = {}
    asset_files: dict[str, tuple[Path, str]] = {}
    asset_paths: list[str] = []

    for file_path in item_dir.iterdir():
        # Skip non-files, symlinks, hidden files, and structural files
        if not file_path.is_file():
            continue
        if file_path.is_symlink():
            continue
        if file_path.name.startswith("."):
            continue
        if file_path.name in IGNORED_FILES:
            continue
        if file_path.name == f"{item_id}.json":
            continue

        file_checksum = compute_checksum(file_path)
        file_media_type = _get_media_type(file_path)
        file_role = _get_asset_role(file_path)

        # Primary geo file gets "data" key, others use stem with disambiguation
        if file_path == primary_file:
            asset_key = "data"
        else:
            # Use stem, but disambiguate on collision (e.g., metadata.json vs metadata.xml)
            base_key = file_path.stem
            asset_key = base_key
            if asset_key in stac_assets or asset_key == "data":
                # Collision: use full filename instead
                asset_key = file_path.name
        stac_assets[asset_key] = pystac.Asset(
            href=file_path.name,
            media_type=file_media_type,
            roles=[file_role],
        )
        asset_files[file_path.name] = (file_path, file_checksum)
        asset_paths.append(str(file_path))

    return stac_assets, asset_files, asset_paths


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


def _pre_validate_geometry(path: Path, format_type: FormatType) -> None:
    """Pre-validate that a file has valid geometry BEFORE any filesystem operations.

    Issue #163: Failed add operations should be atomic. This function checks for
    geometry/features before any conversion or copying happens, preventing partial
    artifacts from being created.

    Args:
        path: Path to the source file.
        format_type: Detected format type (VECTOR or RASTER).

    Raises:
        ValueError: If the file has no valid geometry/features.
    """
    ext = path.suffix.lower()

    # Parquet: check GeoParquet metadata
    if ext == ".parquet":
        from portolan_cli.scan import is_geoparquet

        if not is_geoparquet(path):
            raise ValueError(
                f"Cannot create STAC item for '{path.stem}': "
                "missing bounding box. The source file may have no valid geometry."
            )
        return

    # GeoJSON: check for features with geometry
    if ext in {".geojson", ".json"}:
        import json

        try:
            # Per RFC 7946: GeoJSON MUST be encoded as UTF-8
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

            # Check for features
            if data.get("type") == "FeatureCollection":
                features = data.get("features", [])
                if not features:
                    raise ValueError(
                        f"Cannot create STAC item for '{path.stem}': "
                        "missing bounding box. The source file has no features."
                    )
                # Check that at least one feature has geometry
                has_geometry = any(f.get("geometry") is not None for f in features)
                if not has_geometry:
                    raise ValueError(
                        f"Cannot create STAC item for '{path.stem}': "
                        "missing bounding box. No features have geometry."
                    )
            elif data.get("type") == "Feature":
                if data.get("geometry") is None:
                    raise ValueError(
                        f"Cannot create STAC item for '{path.stem}': "
                        "missing bounding box. Feature has no geometry."
                    )
        except json.JSONDecodeError as err:
            raise ValueError(f"Invalid JSON in '{path}': {err}") from err
        return

    # Shapefile: existence of .shp implies geometry (inherent to format)
    # Rasters: inherently have bbox (extent is required for geotiff)
    # Other formats: let conversion handle validation
    # (We can't easily pre-validate without heavy dependencies)


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
        item_id: Optional item ID (defaults to parent directory name).

    Returns:
        DatasetInfo with details about the added dataset.

    Raises:
        ValueError: If the format is unsupported or collection_id is invalid.
        FileNotFoundError: If the source file doesn't exist.
    """
    # First check: reject path-like collection IDs (security check)
    if (
        not collection_id
        or "/" in collection_id
        or "\\" in collection_id
        or collection_id in {".", ".."}
    ):
        raise ValueError(f"Invalid collection_id '{collection_id}': must be a single path segment")

    # Second check: validate collection ID format per STAC spec
    is_valid, error_msg = validate_collection_id(collection_id)
    if not is_valid:
        suggestion = ""
        try:
            normalized = normalize_collection_id(collection_id)
            suggestion = f" Suggested: '{normalized}'"
        except ValueError:
            # Cannot normalize (e.g., all special characters)
            pass
        raise ValueError(f"Invalid collection ID '{collection_id}': {error_msg}.{suggestion}")

    # Step 1: Detect format
    format_type = detect_format(path)
    if format_type == FormatType.UNKNOWN:
        raise ValueError(f"Unsupported format: {path.suffix}")

    # Step 2: Pre-validate BEFORE any filesystem operations (Issue #163 atomicity)
    # Check for valid geometry/features before any conversion or copying
    _pre_validate_geometry(path, format_type)

    # Generate item ID from PARENT DIRECTORY name (Issue #163)
    # Item boundaries are directories, not filenames.
    # Example: collection/item_dir/file.parquet -> item_id = "item_dir"
    if item_id is None:
        item_id = path.parent.name

    # Validate IDs are safe single path segments
    for label, value in [("collection_id", collection_id), ("item_id", item_id)]:
        if not value or "/" in value or "\\" in value or value in {".", ".."}:
            raise ValueError(f"Invalid {label} '{value}': must be a single path segment")

    # Set up paths - track files IN-PLACE (Issue #163)
    # The file's parent directory IS the item directory.
    # No copying needed; assets stay where they are.
    collection_dir = catalog_root / collection_id
    item_dir = path.parent  # Use existing directory, don't create new one

    # Verify structural consistency: item_dir should be inside collection_dir
    # Use .resolve() to prevent symlink bypass attacks (CodeRabbit review)
    try:
        item_dir.resolve().relative_to(collection_dir.resolve())
    except ValueError as err:
        raise ValueError(
            f"File '{path}' is not inside collection '{collection_id}'. "
            f"Expected path under '{collection_dir}'."
        ) from err

    # Step 3: Convert to cloud-native format (in-place)
    metadata: GeoParquetMetadata | COGMetadata
    if format_type == FormatType.VECTOR:
        output_path = convert_vector(path, item_dir)
        metadata = extract_geoparquet_metadata(output_path)
    else:  # RASTER
        output_path = convert_raster(path, item_dir)
        metadata = extract_cog_metadata(output_path)

    # Step 4: Extract bbox (handle tuple -> list conversion)
    if not metadata.bbox:
        raise ValueError(
            f"Cannot create STAC item for '{metadata.id if hasattr(metadata, 'id') else path.stem}': "
            f"missing bounding box. The source file may have no valid geometry."
        )
    bbox = list(metadata.bbox)

    # Step 5: Scan ALL files in item_dir for assets (per issue #133)
    stac_assets, asset_files, asset_paths = _scan_item_assets(
        item_dir=item_dir,
        item_id=item_id,
        primary_file=output_path,
    )

    # Step 6: Create STAC item with ALL assets
    stac_properties = metadata.to_stac_properties()
    if title:
        stac_properties["title"] = title
    if description:
        stac_properties["description"] = description

    item = create_item(
        item_id=item_id,
        bbox=bbox,
        properties=stac_properties,
        assets=stac_assets,
    )

    # Step 7: Load or create collection
    collection = _get_or_create_collection(
        catalog_root=catalog_root,
        collection_id=collection_id,
        initial_bbox=bbox,
    )

    # Add item to collection
    add_item_to_collection(collection, item, update_extent=True)

    # Step 8: Save collection
    collection.normalize_hrefs(str(collection_dir))
    collection.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    # Step 9: Update catalog to link to collection (if new)
    _update_catalog_links(catalog_root, collection_id)

    # Step 10: Update versions.json with ALL assets
    _update_versions(
        collection_dir=collection_dir,
        item_id=item_id,
        asset_files=asset_files,
    )

    return DatasetInfo(
        item_id=item_id,
        collection_id=collection_id,
        format_type=format_type,
        bbox=bbox,
        asset_paths=asset_paths,
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

    # Check if already a valid COG — skip conversion if so
    if source.suffix.lower() in (".tif", ".tiff") and is_cloud_optimized_geotiff(source):
        # If source is already at the destination, no copy needed (CodeRabbit review)
        if source.resolve() == output_path.resolve():
            return output_path
        # Already a COG, just copy to destination
        shutil.copy2(source, output_path)
        return output_path

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
    output_path: Path | None = None,
    checksum: str | None = None,
    *,
    asset_files: dict[str, tuple[Path, str]] | None = None,
) -> None:
    """Update versions.json with assets.

    Supports both single-file (backward compat) and multi-file modes.

    Args:
        collection_dir: Path to collection directory.
        item_id: Item identifier.
        output_path: Path to single output file (legacy mode).
        checksum: SHA-256 checksum for single file (legacy mode).
        asset_files: Dict mapping filename to (path, checksum) tuples.
            If provided, output_path/checksum are ignored.
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

    collection_id = collection_dir.name

    # Build assets dict - support both single-file and multi-file modes
    assets: dict[str, Asset] = {}
    if asset_files is not None:
        # Multi-asset mode (per issue #133)
        # Use item-scoped keys ({item_id}/{filename}) for multi-asset tracking
        for filename, (file_path, file_checksum) in asset_files.items():
            href = f"{collection_id}/{item_id}/{filename}"
            asset_key = f"{item_id}/{filename}"
            assets[asset_key] = Asset(
                sha256=file_checksum,
                size_bytes=file_path.stat().st_size,
                href=href,
            )
    elif output_path is not None and checksum is not None:
        # Legacy single-file mode (backward compatibility)
        href = f"{collection_id}/{item_id}/{output_path.name}"
        assets[output_path.name] = Asset(
            sha256=checksum,
            size_bytes=output_path.stat().st_size,
            href=href,
        )
    else:
        raise ValueError("Either asset_files or (output_path, checksum) must be provided")

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

# Note: GEOSPATIAL_EXTENSIONS imported from portolan_cli.constants


def _is_filegdb(path: Path) -> bool:
    """Check if a path is a FileGDB directory.

    A FileGDB is a directory with .gdb extension containing .gdbtable files.
    """
    if not path.is_dir() or path.suffix.lower() != ".gdb":
        return False
    # Check for at least one .gdbtable file (marker of valid FileGDB)
    try:
        return any(f.suffix.lower() == ".gdbtable" for f in path.iterdir())
    except OSError:
        return False


def iter_geospatial_files(
    path: Path,
    *,
    recursive: bool = True,
) -> list[Path]:
    """Iterate over geospatial files in a directory.

    Includes both regular files and FileGDB directories (.gdb).
    FileGDB directories are treated as single geospatial assets.

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of paths to geospatial files (including FileGDB directories).
    """
    # Special case: if path itself is a FileGDB, return it directly
    if _is_filegdb(path):
        return [path]

    if not path.is_dir():
        return []

    files: list[Path] = []
    seen_filegdbs: set[Path] = set()  # Track FileGDBs to avoid recursing into them

    if recursive:
        for item in path.rglob("*"):
            # Skip items inside FileGDB directories (they're internal files)
            if any(parent in seen_filegdbs for parent in item.parents):
                continue

            # Check for FileGDB directory
            if item.is_dir() and _is_filegdb(item):
                files.append(item)
                seen_filegdbs.add(item)
            elif item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
                files.append(item)
    else:
        for item in path.iterdir():
            # Check for FileGDB directory
            if item.is_dir() and _is_filegdb(item):
                files.append(item)
            elif item.is_file() and item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
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

# Note: SIDECAR_PATTERNS imported from portolan_cli.constants


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
    *,
    asset_key: str | None = None,
) -> bool:
    """Check if a file is unchanged compared to versions.json.

    Uses mtime as fast-path (per ADR-0017), falls back to sha256 if mtime changed.

    Args:
        path: Path to the file to check.
        versions_path: Path to versions.json for this collection.
        asset_key: Optional explicit key to look up in versions.json.
            If not provided, looks up by filename alone (legacy behavior).

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
    # Try explicit key first, then filename, then converted name
    asset = None
    if asset_key is not None:
        asset = current_version.assets.get(asset_key)
    if asset is None:
        filename = path.name
        asset = current_version.assets.get(filename)
    if asset is None:
        # Also check for stem.parquet (converted name)
        parquet_name = f"{path.stem}.parquet"
        asset = current_version.assets.get(parquet_name)
    if asset is None:
        return False

    # Get file stats once (used for both mtime and size checks)
    file_stat = path.stat()

    # Fast path: check mtime (2s tolerance for NFS/CIFS compatibility)
    if asset.mtime is not None:
        if abs(file_stat.st_mtime - asset.mtime) < MTIME_TOLERANCE_SECONDS:
            return True

    # Medium path: size check before expensive SHA256
    # If size differs, file definitely changed - skip checksum
    if asset.size_bytes is not None and file_stat.st_size != asset.size_bytes:
        return False

    # Slow path: compare sha256 (only if mtime changed but size matches)
    current_checksum = compute_checksum(path)
    return current_checksum == asset.sha256


def _is_no_geometry_error(err: click.ClickException) -> bool:
    """Check if a ClickException is specifically a 'no geometry columns' error from geoparquet-io.

    This narrows the exception handling to ONLY geometry detection errors,
    avoiding accidentally catching permission errors, encoding issues, or
    memory errors that might also be wrapped in ClickException.

    Args:
        err: The ClickException to check.

    Returns:
        True if the error is specifically about missing geometry columns.
    """
    err_msg = (str(err.message) if hasattr(err, "message") else str(err)).lower()
    return any(pattern in err_msg for pattern in _GEOPARQUET_IO_NO_GEOMETRY_PATTERNS)


def _copy_non_geo_to_item_dir(
    file_path: Path,
    item_dir: Path,
) -> Path:
    """Copy a non-geospatial file to an item directory as a companion asset.

    Per ADR-0028, ALL files in item directories should be tracked as STAC assets.
    Non-geospatial CSV/TSV files are copied (not converted) and tracked alongside
    the primary geospatial data.

    Args:
        file_path: Source file path.
        item_dir: Destination item directory.

    Returns:
        Path to the copied file in item_dir.
    """
    dest_path = item_dir / file_path.name
    if dest_path.exists() and dest_path.resolve() == file_path.resolve():
        # Already in place
        return dest_path
    shutil.copy2(file_path, dest_path)
    return dest_path


def add_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    collection_id: str | None = None,
    item_id: str | None = None,
    verbose: bool = False,
) -> tuple[list[DatasetInfo], list[Path]]:
    """Add files to a Portolan catalog.

    This is the main entry point for the `portolan add` command.
    Handles single files, directories, and sidecar auto-detection.

    Per ADR-0028 ("Track ALL files in item directories as assets"):
    - Geospatial files (with geometry) are converted to cloud-native format
    - Non-geospatial CSV/TSV files are tracked as companion assets (no conversion)
    - Files must be in a directory with at least one geospatial file to be tracked

    Args:
        paths: List of paths to add (files or directories).
        catalog_root: Root directory of the catalog.
        collection_id: Optional explicit collection ID.
            If not provided (None), the collection is inferred per-file from
            the first directory component relative to catalog_root. This is
            used by `portolan add .` to process multiple collections at once.
            Files at the catalog root level (not in a subdirectory) are skipped
            with a warning when collection_id=None.
        item_id: Optional explicit item ID. If provided, overrides automatic
            derivation from parent directory name. Must be a single path segment
            (no '/', '\\', '.', or '..').
        verbose: If True, return skipped files info.

    Returns:
        Tuple of (added_datasets, skipped_paths).
        added_datasets: List of DatasetInfo for newly added/updated files.
        skipped_paths: List of paths that were skipped (unchanged or non-geospatial).
    """
    added: list[DatasetInfo] = []
    skipped: list[Path] = []
    processed_paths: set[Path] = set()

    # Track source_dir -> item_dir mappings for non-geo file placement (ADR-0028)
    # Key: source directory, Value: (item_dir, collection_id, item_id)
    source_to_item_dir: dict[Path, tuple[Path, str, str]] = {}

    # Deferred non-geo files: (file_path, source_dir, collection_id)
    # These are processed after geo files to ensure item directories exist
    deferred_non_geo: list[tuple[Path, Path, str]] = []

    for path in paths:
        if path.is_dir():
            # Add all files in directory
            files = iter_files_with_sidecars(path)
        else:
            # Single file + sidecars
            files = [path] + get_sidecars(path)

        for file_path in files:
            # Resolve symlinks to track the real file
            # This ensures we track actual data, not ephemeral links
            if file_path.is_symlink():
                file_path = file_path.resolve()

            if file_path in processed_paths:
                continue
            processed_paths.add(file_path)

            # Skip non-geospatial files (sidecars are handled separately)
            if file_path.suffix.lower() not in GEOSPATIAL_EXTENSIONS:
                continue

            # Determine collection ID
            coll_id = collection_id
            if coll_id is None:
                try:
                    coll_id = resolve_collection_id(file_path, catalog_root)
                except ValueError:
                    # File is at catalog root level (not in a collection subdirectory).
                    # During recursive add (collection_id=None), skip with a warning
                    # rather than crashing the entire operation. Files must be inside
                    # a collection subdirectory to be tracked.
                    from portolan_cli.output import warn as warn_output

                    warn_output(
                        f"Skipping {file_path.name}: files at catalog root must be "
                        "in a collection subdirectory"
                    )
                    skipped.append(file_path)
                    continue

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
                    item_id=item_id,
                )
                added.append(result)

                # Track source_dir -> item_dir mapping for non-geo file placement
                source_dir = file_path.parent
                item_dir = catalog_root / coll_id / result.item_id
                source_to_item_dir[source_dir] = (item_dir, coll_id, result.item_id)

            except click.ClickException as err:
                # Handle geometry detection errors from geoparquet-io gracefully
                # for CSV/TSV files that don't have geometry columns (Issue #140)
                #
                # IMPORTANT: Only catch SPECIFIC geometry-related errors.
                # Other ClickExceptions (permission, encoding, memory) should propagate.
                if not _is_no_geometry_error(err):
                    raise

                # Only tabular formats can be non-geospatial assets
                if file_path.suffix.lower() not in TABULAR_EXTENSIONS:
                    # Non-tabular format without geometry is a real error
                    raise

                # Defer non-geo tabular files until geo files are processed
                # This ensures we have an item_dir to place them in
                source_dir = file_path.parent
                deferred_non_geo.append((file_path, source_dir, coll_id))

            except (ValueError, FileNotFoundError) as err:
                # Re-raise with context
                raise type(err)(f"Failed to add {file_path}: {err}") from err

    # Process deferred non-geo files (ADR-0028: track as assets, skip conversion)
    for file_path, source_dir, coll_id in deferred_non_geo:
        if source_dir in source_to_item_dir:
            resolved_item_dir, _, resolved_item_id = source_to_item_dir[source_dir]

            # Copy non-geo file to item directory as companion asset
            dest_path = _copy_non_geo_to_item_dir(file_path, resolved_item_dir)

            # Log info message (not warning - this is expected behavior per ADR-0028)
            ext = file_path.suffix.upper().lstrip(".")
            logger.info(
                "Tracking %s as non-geospatial %s asset (no conversion): %s",
                file_path,
                ext,
                dest_path.name,
            )

            # Update the STAC item to include this new asset
            _update_item_with_asset(
                catalog_root=catalog_root,
                collection_id=coll_id,
                item_id=resolved_item_id,
                asset_path=dest_path,
            )

            # Add to skipped (tracked but not converted)
            skipped.append(file_path)
        else:
            # No geo file in same directory - cannot create item without bbox
            ext = file_path.suffix.upper().lstrip(".")
            logger.warning(
                "Cannot track non-geospatial %s file %s: no geospatial file in same directory. "
                "Non-geospatial files require a companion geospatial file to create a STAC item.",
                ext,
                file_path,
            )
            skipped.append(file_path)

    return added, skipped


def _update_item_with_asset(
    catalog_root: Path,
    collection_id: str,
    item_id: str,
    asset_path: Path,
) -> None:
    """Update a STAC item to include a new asset file.

    Re-scans the item directory and updates the item.json with all assets.
    This is used to add non-geospatial companion files to existing items.

    Args:
        catalog_root: Root directory of the catalog.
        collection_id: Collection identifier.
        item_id: Item identifier.
        asset_path: Path to the new asset file.
    """
    collection_dir = catalog_root / collection_id
    item_dir = collection_dir / item_id
    item_json_path = item_dir / f"{item_id}.json"

    if not item_json_path.exists():
        logger.warning("Item JSON not found: %s", item_json_path)
        return

    # Load existing item
    with open(item_json_path) as f:
        item_data = json.load(f)

    # Find the primary data file (look for .parquet or .tif)
    primary_file: Path | None = None
    for file in item_dir.iterdir():
        if file.suffix.lower() in {".parquet", ".tif", ".tiff"}:
            primary_file = file
            break

    if primary_file is None:
        # Use the first non-json file as primary
        for file in item_dir.iterdir():
            if file.is_file() and file.suffix.lower() != ".json":
                primary_file = file
                break

    if primary_file is None:
        logger.warning("No primary file found in item directory: %s", item_dir)
        return

    # Re-scan assets
    stac_assets, asset_files, _ = _scan_item_assets(
        item_dir=item_dir,
        item_id=item_id,
        primary_file=primary_file,
    )

    # Update item assets
    item_data["assets"] = {
        key: {
            "href": asset.href,
            "type": asset.media_type,
            "roles": asset.roles,
        }
        for key, asset in stac_assets.items()
    }

    # Write updated item
    with open(item_json_path, "w") as f:
        json.dump(item_data, f, indent=2)

    # Update versions.json with new asset
    _update_versions(
        collection_dir=collection_dir,
        item_id=item_id,
        asset_files=asset_files,
    )


def iter_files_with_sidecars(path: Path, *, recursive: bool = True) -> list[Path]:
    """Iterate over geospatial files in a directory (including their sidecars).

    Returns geospatial files and their associated sidecars (e.g., .dbf/.shx for shapefiles).
    FileGDB directories (.gdb) are treated as single geospatial assets.
    Filters by GEOSPATIAL_EXTENSIONS while iterating for efficiency.

    Args:
        path: Directory to scan.
        recursive: If True, scan subdirectories recursively.

    Returns:
        List of geospatial file paths (including FileGDB directories) and their sidecars.
    """
    # Special case: if path itself is a FileGDB, return it directly
    if _is_filegdb(path):
        return [path]

    if not path.is_dir():
        return []

    files: list[Path] = []
    seen: set[Path] = set()
    seen_filegdbs: set[Path] = set()  # Track FileGDBs to avoid recursing into them

    iterator = path.rglob("*") if recursive else path.iterdir()

    for item in iterator:
        # Skip items inside FileGDB directories (they're internal files)
        if any(parent in seen_filegdbs for parent in item.parents):
            continue

        # Check for FileGDB directory (treat as single asset)
        if item.is_dir() and _is_filegdb(item):
            if item not in seen:
                files.append(item)
                seen.add(item)
                seen_filegdbs.add(item)
            continue

        if not item.is_file():
            continue

        # Only process geospatial files (not sidecars directly)
        if item.suffix.lower() in GEOSPATIAL_EXTENSIONS:
            if item not in seen:
                files.append(item)
                seen.add(item)

            # Also include any sidecars for this file
            for sidecar in get_sidecars(item):
                if sidecar not in seen:
                    files.append(sidecar)
                    seen.add(sidecar)

    return sorted(files)


def remove_files(
    *,
    paths: list[Path],
    catalog_root: Path,
    keep: bool = False,
    dry_run: bool = False,
) -> tuple[list[Path], list[Path]]:
    """Remove files from Portolan catalog tracking.

    This is the main entry point for the `portolan rm` command.
    By default, deletes the file AND removes from tracking (git-style).
    With keep=True, removes from tracking but preserves the file.

    Args:
        paths: List of paths to remove (files or directories).
        catalog_root: Root directory of the catalog.
        keep: If True, preserve file on disk (only untrack).
        dry_run: If True, preview what would be removed without actually removing.

    Returns:
        Tuple of (removed_paths, skipped_paths).
        removed_paths: Paths that were removed from tracking.
        skipped_paths: Paths that were skipped (not in catalog, errors).
    """
    removed: list[Path] = []
    skipped: list[Path] = []

    for path in paths:
        if path.is_dir():
            # Remove all files in directory
            files = list(path.rglob("*")) if path.exists() else []
            files = [f for f in files if f.is_file()]
        else:
            # Include sidecars for single file removal
            sidecars = get_sidecars(path) if path.exists() else []
            files = [path] + sidecars

        for file_path in files:
            if not file_path.exists() and not keep:
                skipped.append(file_path)
                continue

            # Refuse to delete symlinks - they might point outside the catalog
            # and deleting them could have unintended consequences. Users should
            # resolve symlinks manually or use --keep to just untrack.
            if file_path.is_symlink() and not keep:
                skipped.append(file_path)
                continue

            # Determine collection ID
            try:
                coll_id = resolve_collection_id(file_path, catalog_root)
            except ValueError:
                # File is outside catalog - skip with warning
                skipped.append(file_path)
                continue

            # In dry-run mode, just record what would be removed
            if dry_run:
                removed.append(file_path)
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

                # Delete file from disk (missing_ok handles race conditions)
                file_path.unlink(missing_ok=True)

                # Also delete sidecars if this is the primary file
                # Use missing_ok=True to handle race conditions where another
                # process might delete the file between exists() and unlink()
                for sidecar in get_sidecars(file_path):
                    sidecar.unlink(missing_ok=True)

            removed.append(file_path)

    return removed, skipped


def _increment_version(version: str) -> str:
    """Safely increment a semantic version string.

    Handles standard semver (1.2.3) and pre-release versions (1.0.0-beta.1).

    Args:
        version: Current version string.

    Returns:
        Incremented version string.
    """
    if not version:
        return "0.0.1"

    # Handle pre-release versions (e.g., 1.0.0-beta.1)
    if "-" in version:
        base, prerelease = version.split("-", 1)
        # Try to increment the prerelease number
        prerelease_parts = prerelease.rsplit(".", 1)
        if len(prerelease_parts) == 2 and prerelease_parts[1].isdigit():
            prerelease_parts[1] = str(int(prerelease_parts[1]) + 1)
            return f"{base}-{'.'.join(prerelease_parts)}"
        else:
            # No numeric suffix: 1.0.0-beta → 1.0.0-beta.1
            # Preserve the prerelease tag by appending .1
            return f"{base}-{prerelease}.1"

    # Standard semver: increment patch
    parts = version.split(".")
    if len(parts) >= 3 and parts[-1].isdigit():
        parts[-1] = str(int(parts[-1]) + 1)
    elif len(parts) < 3:
        # Pad to 3 parts if needed
        while len(parts) < 3:
            parts.append("0")
        parts[-1] = "1"
    return ".".join(parts)


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

    # Compute new version number safely
    new_version = _increment_version(versions_file.current_version or "0.0.0")

    # Note: Even if new_assets is empty, we preserve version history by creating
    # an empty version entry rather than deleting versions.json entirely.
    # This maintains collection state and allows seeing what files were removed.

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

    # Pass removed= so add_version excludes these from the snapshot
    # (otherwise the snapshot model would re-add them from previous version)
    removed_keys = {filename, parquet_name}

    updated = add_version(
        versions_file,
        version=new_version,
        assets=new_assets_typed,
        breaking=False,
        message=f"Removed {filename}",
        removed=removed_keys,
    )

    write_versions(versions_path, updated)
