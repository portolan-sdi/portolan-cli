"""PMTiles generation from GeoParquet collections.

This module provides functionality to generate PMTiles (vector tiles) from
GeoParquet files in STAC collections. PMTiles are stored as sibling files
to the source GeoParquet, registered as collection-level assets with role
["overview"], and tracked in versions.json for push.

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
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from portolan_cli.errors import PortolanError

# MIME type for PMTiles (matches dataset.py)
PMTILES_MEDIA_TYPE = "application/vnd.pmtiles"


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
        import gpio_pmtiles  # type: ignore[import-not-found]  # noqa: F401
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


def generate_pmtiles(
    parquet_path: Path,
    pmtiles_path: Path,
    *,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
) -> None:
    """Generate a single PMTiles file from GeoParquet.

    Args:
        parquet_path: Path to source GeoParquet file.
        pmtiles_path: Path to output PMTiles file.
        min_zoom: Minimum zoom level (None = auto-detect).
        max_zoom: Maximum zoom level (None = auto-detect).

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
        )
    except Exception as e:
        raise PMTilesGenerationError(str(parquet_path), e) from e


def add_pmtiles_asset_to_collection(
    collection_path: Path,
    parquet_key: str,
    pmtiles_href: str,
) -> None:
    """Add PMTiles asset to collection.json.

    Adds a collection-level asset with role ["overview"] for the PMTiles file.
    The asset key is derived from the source parquet key with "-tiles" suffix.

    Args:
        collection_path: Path to collection directory.
        parquet_key: Asset key of the source GeoParquet.
        pmtiles_href: Relative href to PMTiles file (e.g., "./data.pmtiles").

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

    # Check if already exists
    if pmtiles_key in assets:
        return

    # Get title from source asset if available
    source_asset = assets.get(parquet_key, {})
    source_title = source_asset.get("title", parquet_key)

    assets[pmtiles_key] = {
        "href": pmtiles_href,
        "type": PMTILES_MEDIA_TYPE,
        "title": f"{source_title} (vector tiles)",
        "roles": ["overview"],
    }
    data["assets"] = assets

    collection_json_path.write_text(json.dumps(data, indent=2))


def track_pmtiles_in_versions(
    collection_path: Path,
    pmtiles_path: Path,
    catalog_root: Path,
) -> None:
    """Track PMTiles file in versions.json.

    Args:
        collection_path: Path to collection directory.
        pmtiles_path: Path to PMTiles file.
        catalog_root: Path to catalog root.

    Raises:
        FileNotFoundError: If PMTiles file doesn't exist.
    """
    from portolan_cli.versions import (
        Asset,
        VersionsFile,
        add_version,
        parse_version,
        read_versions,
        write_versions,
    )

    if not pmtiles_path.exists():
        raise FileNotFoundError(f"PMTiles file not found at {pmtiles_path}")

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

    # Compute checksum and stats
    stat = pmtiles_path.stat()
    sha256 = hashlib.sha256(pmtiles_path.read_bytes()).hexdigest()

    # Href is relative to catalog root
    try:
        rel_path = pmtiles_path.relative_to(catalog_root)
    except ValueError:
        # Fallback if not relative
        rel_path = pmtiles_path.relative_to(collection_path.parent)

    pmtiles_asset = Asset(
        sha256=sha256,
        size_bytes=stat.st_size,
        href=str(rel_path),
        mtime=stat.st_mtime,
    )

    # Determine next version
    if versions_file.current_version:
        major, minor, patch = parse_version(versions_file.current_version)
        new_version = f"{major}.{minor}.{patch + 1}"
    else:
        new_version = "1.0.0"

    # Add version with pmtiles asset
    updated = add_version(
        versions_file,
        version=new_version,
        assets={pmtiles_path.name: pmtiles_asset},
        breaking=False,
        message=f"Generated PMTiles: {pmtiles_path.name}",
    )

    write_versions(versions_path, updated)


def generate_pmtiles_for_collection(
    collection_path: Path,
    catalog_root: Path,
    *,
    force: bool = False,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
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

        if not _should_generate(parquet_path, pmtiles_path, force):
            result.skipped.append(pmtiles_path)
            continue

        try:
            generate_pmtiles(
                parquet_path,
                pmtiles_path,
                min_zoom=min_zoom,
                max_zoom=max_zoom,
            )

            # Register asset in collection.json
            pmtiles_href = f"./{pmtiles_path.name}"
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)

            # Track in versions.json
            track_pmtiles_in_versions(collection_path, pmtiles_path, catalog_root)

            result.generated.append(pmtiles_path)

        except PMTilesGenerationError as e:
            result.failed.append((parquet_path, str(e)))
        except Exception as e:
            result.failed.append((parquet_path, f"Unexpected error: {e}"))

    return result
