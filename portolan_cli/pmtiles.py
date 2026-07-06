"""PMTiles generation from GeoParquet collections.

This module provides functionality to generate PMTiles (vector tiles) from
GeoParquet files in STAC collections. PMTiles are stored as sibling files
to the source GeoParquet, registered as collection-level assets with role
["visual"], and tracked in versions.json for push.

Requires:
- gpio-pmtiles package (optional dependency: `pip install portolan-cli[pmtiles]`)
- tippecanoe binary installed and in PATH

Usage:
    from portolan_cli.pmtiles import generate_pmtiles_for_collection

    result = generate_pmtiles_for_collection(
        collection_path=Path("municipalities"),
        catalog_root=Path("."),
        force=False,
    )
    print(f"Generated: {len(result.generated)}, Skipped: {len(result.skipped)}")
"""

from __future__ import annotations

import hashlib
import json
import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from portolan_cli.errors import PortolanError
from portolan_cli.output import warn
from portolan_cli.thumbnail import (
    generate_vector_thumbnail,
    get_thumbnail_config,
    thumbnail_path_for,
)

logger = logging.getLogger(__name__)

# MIME type for PMTiles (matches add.py)
PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"

# web-map-links STAC extension declared for the rel="pmtiles" collection link
# (Issue #569). v1.3.0 defines the pmtiles rel, the application/vnd.pmtiles media
# type, and the pmtiles:layers field for default-visible vector layers.
WEB_MAP_LINKS_EXTENSION = "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json"


# --- Errors ---


class PMTilesError(PortolanError):
    """Base class for PMTiles-related errors."""

    code = "PRTLN-PMT000"


class PMTilesNotAvailableError(PMTilesError):
    """Raised when gpio-pmtiles package is not installed.

    Error code: PRTLN-PMT001
    """

    code = "PRTLN-PMT001"

    def __init__(self) -> None:
        super().__init__(
            "gpio-pmtiles package not installed. Install with: pip install portolan-cli[pmtiles]"
        )


class TippecanoeNotFoundError(PMTilesError):
    """Raised when tippecanoe binary is not found in PATH.

    Error code: PRTLN-PMT002
    """

    code = "PRTLN-PMT002"

    def __init__(self) -> None:
        super().__init__(
            "tippecanoe not found in PATH. PMTiles generation requires tippecanoe. "
            "Install: brew install tippecanoe (macOS) or apt install tippecanoe (Ubuntu)"
        )


class PMTilesGenerationError(PMTilesError):
    """Raised when PMTiles generation fails.

    Error code: PRTLN-PMT003
    """

    code = "PRTLN-PMT003"

    def __init__(self, source_path: str, original_error: Exception) -> None:
        super().__init__(
            f"PMTiles generation failed for {source_path}: {original_error}",
            source_path=source_path,
            original_error_type=type(original_error).__name__,
            original_error_message=str(original_error),
        )
        self.original_exception = original_error


# --- Result dataclass ---


@dataclass
class PMTilesResult:
    """Result of PMTiles generation for a collection.

    Attributes:
        generated: Paths to successfully generated PMTiles files.
        skipped: Paths to PMTiles that were skipped (already exist and up-to-date).
        failed: List of (source_path, error_message) for failed generations.
    """

    generated: list[Path] = field(default_factory=list)
    skipped: list[Path] = field(default_factory=list)
    failed: list[tuple[Path, str]] = field(default_factory=list)

    @property
    def total(self) -> int:
        """Total number of files processed."""
        return len(self.generated) + len(self.skipped) + len(self.failed)

    @property
    def success(self) -> bool:
        """True if no failures occurred."""
        return len(self.failed) == 0


# --- Core functions ---


def check_pmtiles_available() -> None:
    """Check that PMTiles generation dependencies are available.

    Raises:
        PMTilesNotAvailableError: If gpio-pmtiles is not installed.
        TippecanoeNotFoundError: If tippecanoe is not in PATH.
    """
    # Check for gpio-pmtiles
    try:
        import gpio_pmtiles  # type: ignore[import-untyped]  # noqa: F401
    except ImportError as e:
        raise PMTilesNotAvailableError() from e

    # Check for tippecanoe
    if shutil.which("tippecanoe") is None:
        raise TippecanoeNotFoundError()


def _find_geoparquet_assets(collection_path: Path) -> list[tuple[str, Path]]:
    """Find all GeoParquet assets in a collection.

    Args:
        collection_path: Path to collection directory.

    Returns:
        List of (asset_key, asset_path) tuples for GeoParquet assets.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return []

    data = json.loads(collection_json_path.read_text())
    assets = data.get("assets", {})

    geoparquet_assets = []
    for key, asset in assets.items():
        href = asset.get("href", "")
        media_type = asset.get("type", "")

        # Check if it's a GeoParquet asset
        is_geoparquet = (
            media_type == "application/vnd.apache.parquet"
            or media_type == "application/x-parquet"
            or href.endswith(".parquet")
        )

        # Skip stac-items parquet (that's metadata, not geodata)
        roles = asset.get("roles", [])
        if "stac-items" in roles:
            continue

        if is_geoparquet:
            # Resolve href relative to collection
            if href.startswith("./"):
                href = href[2:]
            asset_path = collection_path / href
            if asset_path.exists():
                geoparquet_assets.append((key, asset_path))

    return geoparquet_assets


def _should_generate(parquet_path: Path, pmtiles_path: Path, force: bool) -> bool:
    """Determine if PMTiles should be generated.

    Args:
        parquet_path: Path to source GeoParquet file.
        pmtiles_path: Path to target PMTiles file.
        force: If True, always regenerate.

    Returns:
        True if PMTiles should be generated.
    """
    if force:
        return True

    if not pmtiles_path.exists():
        return True

    # Regenerate if source is newer than target
    return parquet_path.stat().st_mtime > pmtiles_path.stat().st_mtime


def _discover_style_for_thumbnail(collection_dir: Path) -> Path | None:
    """Find a style file for thumbnail generation.

    Searches for styles/default.json or styles/source.json in the collection
    directory, preferring default.json (the extracted style, per Issue #497).

    Args:
        collection_dir: Path to collection directory.

    Returns:
        Path to style file if found, None otherwise.
    """
    styles_dir = collection_dir / "styles"
    if not styles_dir.exists():
        return None

    # Prefer default.json (extracted style), then source.json
    for name in ("default.json", "source.json"):
        style_path = styles_dir / name
        if style_path.exists():
            return style_path

    return None


def generate_pmtiles(
    parquet_path: Path,
    pmtiles_path: Path,
    *,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    layer: str | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    attribution: str | None = None,
    src_crs: str | None = None,
) -> None:
    """Generate a single PMTiles file from GeoParquet.

    Args:
        parquet_path: Path to source GeoParquet file.
        pmtiles_path: Path to output PMTiles file.
        min_zoom: Minimum zoom level (None = auto-detect).
        max_zoom: Maximum zoom level (None = auto-detect).
        layer: Layer name in PMTiles (None = use filename).
        bbox: Bounding box filter as "minx,miny,maxx,maxy".
        where: SQL WHERE clause for filtering features.
        include_cols: Comma-separated columns to include in tiles.
        precision: Coordinate decimal precision (default: 6).
        attribution: Attribution HTML for tiles.
        src_crs: Override source CRS if metadata is incorrect.

    Raises:
        PMTilesNotAvailableError: If gpio-pmtiles not installed.
        TippecanoeNotFoundError: If tippecanoe not in PATH.
        PMTilesGenerationError: If generation fails.
    """
    check_pmtiles_available()

    from gpio_pmtiles import create_pmtiles_from_geoparquet

    try:
        create_pmtiles_from_geoparquet(
            input_path=str(parquet_path),
            output_path=str(pmtiles_path),
            min_zoom=min_zoom,
            max_zoom=max_zoom,
            layer=layer,
            bbox=bbox,
            where=where,
            include_cols=include_cols,
            precision=precision,
            attribution=attribution,
            src_crs=src_crs,
        )
    except Exception as e:
        raise PMTilesGenerationError(str(parquet_path), e) from e


def _write_default_style_for_geoparquet(
    parquet_path: Path,
    layer_name: str,
    collection_path: Path,
    pmtiles_relative_path: str,
    catalog_path: Path | None = None,
) -> Path | None:
    """Write a default style file for a PMTiles asset.

    Args:
        parquet_path: Path to source GeoParquet (for geometry type detection).
        layer_name: Layer name in the PMTiles.
        collection_path: Path to the collection directory.
        pmtiles_relative_path: PMTiles path relative to collection (e.g., "data.pmtiles").
        catalog_path: Optional catalog path for loading style config.

    Returns:
        Path to the written style file, or None if skipped.
    """
    # Check existence before expensive metadata extraction
    default_path = collection_path / "styles" / "default.json"
    if default_path.exists():
        return None

    try:
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
        from portolan_cli.style import (
            VectorStyleConfig,
            get_vector_style_config,
            write_default_style,
        )
    except ImportError:
        logger.debug("Style dependencies not available")
        return None

    try:
        metadata = extract_geoparquet_metadata(parquet_path)
        geometry_type = metadata.geometry_type
        if not geometry_type:
            logger.debug("No geometry type found in %s", parquet_path)
            return None

        config = get_vector_style_config(catalog_path) if catalog_path else VectorStyleConfig()

        return write_default_style(
            collection_path=collection_path,
            geometry_type=geometry_type,
            source_layer=layer_name,
            pmtiles_relative_path=pmtiles_relative_path,
            config=config,
        )
    except Exception as e:
        logger.debug("Failed to write default style for %s: %s", parquet_path, e)
        return None


def add_pmtiles_asset_to_collection(
    collection_path: Path,
    parquet_key: str,
    pmtiles_href: str,
    *,
    extra_properties: dict[str, Any] | None = None,
) -> None:
    """Add PMTiles asset to collection.json.

    Adds a collection-level asset with role ["visual"] for the PMTiles file.
    The asset key is derived from the source parquet key with "-tiles" suffix.

    Args:
        collection_path: Path to collection directory.
        parquet_key: Asset key of the source GeoParquet.
        pmtiles_href: Relative href to PMTiles file (e.g., "./data.pmtiles").
        extra_properties: Additional properties to add to the asset.

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text())
    assets = data.get("assets", {})

    # Generate asset key from parquet key
    pmtiles_key = f"{parquet_key}-tiles"

    # Get title from source asset if available
    source_asset = assets.get(parquet_key, {})
    source_title = source_asset.get("title", parquet_key)

    # Check if already exists - update extra properties if changed, otherwise skip
    if pmtiles_key in assets:
        existing = assets[pmtiles_key]
        needs_update = False

        # Update extra properties if provided
        if extra_properties:
            for key, value in extra_properties.items():
                if existing.get(key) != value:
                    existing[key] = value
                    needs_update = True

        if needs_update:
            collection_json_path.write_text(json.dumps(data, indent=2))
        return

    asset_dict: dict[str, Any] = {
        "href": pmtiles_href,
        "type": PMTILES_MEDIA_TYPE,
        "title": f"{source_title} (vector tiles)",
        "roles": ["visual"],
    }

    # Add any extra properties
    if extra_properties:
        asset_dict.update(extra_properties)

    assets[pmtiles_key] = asset_dict
    data["assets"] = assets

    collection_json_path.write_text(json.dumps(data, indent=2))


def pmtiles_asset_hrefs(assets: dict[str, Any]) -> list[str]:
    """Return the hrefs of all PMTiles assets in a collection's asset dict.

    An asset is a PMTiles asset when its ``type`` is ``application/vnd.pmtiles``
    or its ``href`` ends in ``.pmtiles``. Shared by the RULE-0061 check and its
    ``--fix`` repair so both classify assets identically.
    """
    return [
        str(asset["href"])
        for asset in assets.values()
        if isinstance(asset, dict)
        and (
            asset.get("type") == PMTILES_MEDIA_TYPE
            or str(asset.get("href", "")).endswith(".pmtiles")
        )
        and asset.get("href")
    ]


def pmtiles_link_hrefs(links: list[Any]) -> set[str]:
    """Return the hrefs of all ``rel='pmtiles'`` links in a collection's links list."""
    return {
        str(link["href"])
        for link in links
        if isinstance(link, dict) and link.get("rel") == "pmtiles" and link.get("href")
    }


def ensure_web_map_links_extension(collection_path: Path) -> bool:
    """Declare the web-map-links extension in a collection idempotently.

    Adds ``WEB_MAP_LINKS_EXTENSION`` to ``stac_extensions`` if absent, without
    touching any links (so existing ``pmtiles:layers`` overrides are preserved).
    Used by ``check --fix`` to repair a collection that carries the PMTiles link
    but omits the extension declaration (RULE-0061 assertion 3).

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if the extension was added and the file rewritten, else False.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text())
    extensions = data.get("stac_extensions", [])
    if WEB_MAP_LINKS_EXTENSION in extensions:
        return False

    extensions.append(WEB_MAP_LINKS_EXTENSION)
    data["stac_extensions"] = extensions
    collection_json_path.write_text(json.dumps(data, indent=2))
    return True


def add_pmtiles_link_to_collection(
    collection_path: Path,
    pmtiles_href: str,
    *,
    layers: list[str],
) -> None:
    """Add a ``rel="pmtiles"`` collection link following web-map-links (Issue #569).

    The link coexists with the PMTiles *asset* (RULE-0060) and satisfies RULE-0061.
    It declares the web-map-links extension in ``stac_extensions`` and carries the
    ``pmtiles:layers`` array of default-visible vector layers. The link is keyed by
    its ``href`` so multiple PMTiles in one collection each get their own link, and
    re-running keeps ``pmtiles:layers`` in sync without duplicating the link.

    Args:
        collection_path: Path to collection directory.
        pmtiles_href: Relative href to the PMTiles file (e.g., "./data.pmtiles").
        layers: Default-visible vector layer names inside the PMTiles.

    Raises:
        FileNotFoundError: If collection.json doesn't exist.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text())

    changed = False

    # Declare the web-map-links extension (idempotently).
    extensions = data.get("stac_extensions", [])
    if WEB_MAP_LINKS_EXTENSION not in extensions:
        extensions.append(WEB_MAP_LINKS_EXTENSION)
        data["stac_extensions"] = extensions
        changed = True

    links = data.get("links", [])
    existing = next(
        (
            link
            for link in links
            if link.get("rel") == "pmtiles" and link.get("href") == pmtiles_href
        ),
        None,
    )
    if existing is None:
        links.append(
            {
                "rel": "pmtiles",
                "href": pmtiles_href,
                "type": PMTILES_MEDIA_TYPE,
                "pmtiles:layers": layers,
            }
        )
        data["links"] = links
        changed = True
    else:
        # Keep type and layers aligned if a prior link is stale.
        if existing.get("type") != PMTILES_MEDIA_TYPE:
            existing["type"] = PMTILES_MEDIA_TYPE
            changed = True
        if existing.get("pmtiles:layers") != layers:
            existing["pmtiles:layers"] = layers
            changed = True

    if changed:
        collection_json_path.write_text(json.dumps(data, indent=2))


def add_thumbnail_asset_to_collection(
    collection_path: Path,
    pmtiles_key: str,
    thumbnail_path: Path,
) -> None:
    """Add thumbnail asset to collection.json.

    Args:
        collection_path: Path to collection directory.
        pmtiles_key: Asset key of the PMTiles file (thumbnail key will be pmtiles_key + "-thumbnail").
        thumbnail_path: Path to thumbnail file.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return

    data = json.loads(collection_json_path.read_text())
    assets = data.get("assets", {})

    thumb_key = f"{pmtiles_key}-thumbnail"
    thumb_href = f"./{thumbnail_path.name}"

    # Get title from PMTiles asset if available
    pmtiles_asset = assets.get(pmtiles_key, {})
    pmtiles_title = pmtiles_asset.get("title", pmtiles_key)

    assets[thumb_key] = {
        "href": thumb_href,
        "type": "image/jpeg",
        "title": f"{pmtiles_title} (thumbnail)",
        "roles": ["thumbnail"],
    }
    data["assets"] = assets

    collection_json_path.write_text(json.dumps(data, indent=2))


def _compute_sha256(path: Path) -> str:
    """Stream a file in 64KB chunks and return its SHA-256 hex digest.

    Chunked to avoid loading large PMTiles/thumbnail files fully into memory.
    """
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):  # 64KB chunks
            hasher.update(chunk)
    return hasher.hexdigest()


def _track_generated_assets_in_versions(
    collection_path: Path,
    asset_paths: list[Path],
    catalog_root: Path,
    *,
    message: str,
    only_if_missing: bool = False,
) -> None:
    """Track generated side-step assets (PMTiles, thumbnail) in versions.json.

    Computes SHA-256, size, and mtime for each path and records them in a *single*
    new version snapshot. The PMTiles and its thumbnail come from the same
    side-step for the same source asset, so they belong in one version, not two
    (Issue #519). ``add_version`` carries forward the previous version's assets,
    so the result is a complete snapshot with these assets added/updated.

    Args:
        collection_path: Path to collection directory.
        asset_paths: Paths to the generated files to track.
        catalog_root: Path to catalog root (hrefs are catalog-root-relative).
        message: Human-readable description of the change.
        only_if_missing: When True, only track assets whose filename is not
            already present in the latest version snapshot, and create no version
            at all if every asset is already tracked. Used by the skip path to
            backfill artifacts generated before this tracking existed without
            bumping a version on every unchanged ``add`` (Issue #519).

    Raises:
        FileNotFoundError: If any asset path doesn't exist.
    """
    from portolan_cli.versions import (
        Asset,
        VersionsFile,
        add_version,
        parse_version,
        read_versions,
        write_versions,
    )

    for asset_path in asset_paths:
        if not asset_path.exists():
            raise FileNotFoundError(f"File not found at {asset_path}")

    versions_path = collection_path / "versions.json"

    # If no versions.json, create a minimal one
    if not versions_path.exists():
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version=None,
            versions=[],
        )
    else:
        versions_file = read_versions(versions_path)

    # Backfill mode: skip assets already tracked, and create no version if none
    # are missing (otherwise the message would force a no-op version bump).
    paths_to_track = asset_paths
    if only_if_missing and versions_file.versions:
        tracked = versions_file.versions[-1].assets
        paths_to_track = [p for p in asset_paths if p.name not in tracked]
    if not paths_to_track:
        return

    assets: dict[str, Asset] = {}
    for asset_path in paths_to_track:
        stat = asset_path.stat()
        # Href is relative to catalog root
        try:
            rel_path = asset_path.relative_to(catalog_root)
        except ValueError:
            # Fallback if not relative
            rel_path = asset_path.relative_to(collection_path.parent)
        assets[asset_path.name] = Asset(
            sha256=_compute_sha256(asset_path),
            size_bytes=stat.st_size,
            href=rel_path.as_posix(),
            mtime=stat.st_mtime,
        )

    # Determine next version
    if versions_file.current_version:
        major, minor, patch = parse_version(versions_file.current_version)
        new_version = f"{major}.{minor}.{patch + 1}"
    else:
        new_version = "1.0.0"

    updated = add_version(
        versions_file,
        version=new_version,
        assets=assets,
        breaking=False,
        message=message,
    )

    write_versions(versions_path, updated)


def _backfill_skipped_assets(
    collection_path: Path, asset_key: str, pmtiles_path: Path, catalog_root: Path
) -> None:
    """Track an up-to-date PMTiles and its thumbnail if not already tracked.

    Runs on the skip path to heal catalogs whose artifacts were generated before
    versions.json tracking existed (Issue #519), without bumping a version when
    everything is already tracked. When a thumbnail exists on disk it is also
    re-registered as a STAC asset (mirroring the PMTiles), so the backfilled
    versions.json entry can never be orphaned from collection.json.
    """
    backfill = [pmtiles_path]
    existing_thumb = thumbnail_path_for(pmtiles_path)
    if existing_thumb.exists():
        # Ensure the thumbnail is a STAC asset too, even when skipping generation.
        add_thumbnail_asset_to_collection(collection_path, f"{asset_key}-tiles", existing_thumb)
        backfill.append(existing_thumb)
    try:
        _track_generated_assets_in_versions(
            collection_path,
            backfill,
            catalog_root,
            message="Backfilled visualization asset tracking",
            only_if_missing=True,
        )
    except Exception as e:
        warn(f"Failed to backfill versions.json tracking for {pmtiles_path.name}: {e}")


def _generate_thumbnail_asset(
    collection_path: Path,
    parquet_path: Path,
    pmtiles_path: Path,
    asset_key: str,
    catalog_root: Path,
) -> Path | None:
    """Generate the vector thumbnail and register it as a STAC asset.

    Returns the thumbnail path on success, or None if disabled or failed. Failure
    is non-fatal: it must not affect PMTiles success (Issue #13).
    """
    try:
        thumb_config = get_thumbnail_config(catalog_root)
        if not thumb_config.enabled:
            return None
        # Discover style for thumbnail (Issue #495)
        style_path = _discover_style_for_thumbnail(collection_path)
        thumb_path = generate_vector_thumbnail(
            pmtiles_path=pmtiles_path,
            geoparquet_path=parquet_path,  # fallback
            config=thumb_config,
            style_path=style_path,
        )
        if thumb_path:
            add_thumbnail_asset_to_collection(collection_path, f"{asset_key}-tiles", thumb_path)
        return thumb_path
    except Exception as e:
        warn(f"Thumbnail generation failed for {pmtiles_path.name}: {e}")
        return None


def _track_side_step_assets(
    collection_path: Path,
    pmtiles_path: Path,
    thumb_path: Path | None,
    catalog_root: Path,
) -> None:
    """Track the PMTiles and its thumbnail in a SINGLE versions.json snapshot.

    One side-step is one version bump, not two (Issue #519). Called outside the
    generation try/finally so a versions.json write error cannot trigger the
    partial-file cleanup that deletes the freshly generated PMTiles.
    """
    generated_assets = [pmtiles_path]
    if thumb_path:
        generated_assets.append(thumb_path)
        message = f"Generated PMTiles and thumbnail: {pmtiles_path.name}, {thumb_path.name}"
    else:
        message = f"Generated PMTiles: {pmtiles_path.name}"
    try:
        _track_generated_assets_in_versions(
            collection_path, generated_assets, catalog_root, message=message
        )
    except Exception as e:
        warn(f"Failed to track generated assets in versions.json for {pmtiles_path.name}: {e}")


def generate_pmtiles_for_collection(
    collection_path: Path,
    catalog_root: Path,
    *,
    force: bool = False,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    layer: str | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    attribution: str | None = None,
    src_crs: str | None = None,
) -> PMTilesResult:
    """Generate PMTiles for all GeoParquet assets in a collection.

    For each GeoParquet asset in collection.json, generates a sibling PMTiles
    file if it doesn't exist or if the source is newer. Updates collection.json
    with PMTiles assets and tracks them in versions.json.

    Args:
        collection_path: Path to collection directory.
        catalog_root: Path to catalog root.
        force: If True, regenerate even if PMTiles exists and is up-to-date.
        min_zoom: Minimum zoom level (None = auto-detect via tippecanoe).
        max_zoom: Maximum zoom level (None = auto-detect via tippecanoe).
        layer: Layer name in PMTiles (None = use filename).
        bbox: Bounding box filter as "minx,miny,maxx,maxy".
        where: SQL WHERE clause for filtering features.
        include_cols: Comma-separated columns to include in tiles.
        precision: Coordinate decimal precision (default: 6).
        attribution: Attribution HTML for tiles.
        src_crs: Override source CRS if metadata is incorrect.

    Returns:
        PMTilesResult with generated, skipped, and failed counts.

    Raises:
        PMTilesNotAvailableError: If gpio-pmtiles not installed.
        TippecanoeNotFoundError: If tippecanoe not in PATH.
    """
    # Check dependencies upfront
    check_pmtiles_available()

    result = PMTilesResult()

    # Find all GeoParquet assets
    geoparquet_assets = _find_geoparquet_assets(collection_path)

    for asset_key, parquet_path in geoparquet_assets:
        pmtiles_path = parquet_path.with_suffix(".pmtiles")

        # Compute href relative to collection (preserves subdirectory structure)
        # Use as_posix() for STAC-compliant forward slashes on all platforms
        try:
            pmtiles_rel = pmtiles_path.relative_to(collection_path)
            pmtiles_href = f"./{pmtiles_rel.as_posix()}"
        except ValueError:
            pmtiles_href = f"./{pmtiles_path.name}"

        # Determine layer name (Issue #13)
        layer_name = layer if layer else parquet_path.stem

        # Compute collection-relative PMTiles path for style source URLs
        try:
            pmtiles_col_rel = pmtiles_path.relative_to(collection_path).as_posix()
        except ValueError:
            pmtiles_col_rel = pmtiles_path.name

        if not _should_generate(parquet_path, pmtiles_path, force):
            # Ensure asset is registered/updated in collection.json even when skipping
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)
            # Emit the rel="pmtiles" web-map-links link alongside the asset (#569)
            add_pmtiles_link_to_collection(collection_path, pmtiles_href, layers=[layer_name])
            # Ensure default style exists even when PMTiles generation is skipped
            _write_default_style_for_geoparquet(
                parquet_path=parquet_path,
                layer_name=layer_name,
                collection_path=collection_path,
                pmtiles_relative_path=pmtiles_col_rel,
                catalog_path=catalog_root,
            )
            # Backfill versions.json for artifacts generated before this tracking
            # existed (the original #519 bug state), idempotently.
            _backfill_skipped_assets(collection_path, asset_key, pmtiles_path, catalog_root)
            result.skipped.append(pmtiles_path)
            continue

        # Track success to clean up partial files on any failure (Issue #385)
        # Using finally ensures cleanup even on KeyboardInterrupt/SystemExit
        generation_succeeded = False
        try:
            # Delete existing file if forcing regeneration
            # (tippecanoe requires this since it doesn't have a --force option)
            if force and pmtiles_path.exists():
                pmtiles_path.unlink()

            generate_pmtiles(
                parquet_path,
                pmtiles_path,
                min_zoom=min_zoom,
                max_zoom=max_zoom,
                layer=layer,
                bbox=bbox,
                where=where,
                include_cols=include_cols,
                precision=precision,
                attribution=attribution,
                src_crs=src_crs,
            )

            # Register asset in collection.json (Issue #13)
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)
            # Emit the rel="pmtiles" web-map-links link alongside the asset (#569)
            add_pmtiles_link_to_collection(collection_path, pmtiles_href, layers=[layer_name])

            result.generated.append(pmtiles_path)
            generation_succeeded = True

            # Generate default style file (ADR-0045)
            _write_default_style_for_geoparquet(
                parquet_path=parquet_path,
                layer_name=layer_name,
                collection_path=collection_path,
                pmtiles_relative_path=pmtiles_col_rel,
                catalog_path=catalog_root,
            )

        except PMTilesGenerationError as e:
            result.failed.append((parquet_path, str(e)))
        except Exception as e:
            result.failed.append((parquet_path, f"Unexpected error: {e}"))
        finally:
            # Clean up partial output to prevent phantom assets (Issue #385)
            # missing_ok=True avoids TOCTOU race condition
            if not generation_succeeded and pmtiles_path.exists():
                pmtiles_path.unlink(missing_ok=True)
                warn(f"Cleaned up partial file after failure: {pmtiles_path.name}")

        # Generate thumbnail separately - failure shouldn't affect PMTiles success
        # (Issue #13) - then track the PMTiles and thumbnail in a SINGLE version
        # snapshot (one side-step == one version, not two, Issue #519).
        if generation_succeeded:
            thumb_path = _generate_thumbnail_asset(
                collection_path, parquet_path, pmtiles_path, asset_key, catalog_root
            )
            _track_side_step_assets(collection_path, pmtiles_path, thumb_path, catalog_root)

    # Discover and register style assets (ADR-0045)
    from portolan_cli.style import discover_styles, register_style_assets

    styles = discover_styles(collection_path)
    register_style_assets(collection_path, styles)

    return result
