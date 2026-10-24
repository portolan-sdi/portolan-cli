"""PMTiles generation from GeoParquet collections.

This module provides functionality to generate PMTiles (vector tiles) from
GeoParquet files in STAC collections. PMTiles are stored as sibling files
to the source GeoParquet, registered as collection-level assets with role
["visual"], and tracked in versions.json for push.

Requires:
- geoparquet-io (a core dependency) for `create_pmtiles`
- tippecanoe binary installed and in PATH

Usage:
    from portolan_cli.viz.pmtiles import generate_pmtiles_for_collection

    result = generate_pmtiles_for_collection(
        collection_path=Path("municipalities"),
        catalog_root=Path("."),
        force=False,
    )
    print(f"Generated: {len(result.generated)}, Skipped: {len(result.skipped)}")
"""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_cli.constants import ROLE_VISUAL
from portolan_cli.errors import PortolanError
from portolan_cli.formats import is_geoparquet
from portolan_cli.json_io import write_json_atomic
from portolan_cli.output import error, info, success, warn
from portolan_cli.stac_parquet import stamp_file_fields, sync_file_extension
from portolan_cli.versions import track_generated_assets

# The PMTiles constants and pure asset/link classifiers now live in the
# framework-free leaf pmtiles_links.py so validation/ can reach them without
# importing this module (which pulls in output/thumbnail/style). Re-imported
# here — both constants are used below — so pmtiles.py's public API is unchanged
# for existing callers/tests (see pmtiles_links.py, issue #563).
from portolan_cli.viz.pmtiles_links import (
    PMTILES_MEDIA_TYPE,
    PMTILES_SUFFIX,
    WEB_MAP_LINKS_EXTENSION,
    pmtiles_asset_key,
)
from portolan_cli.viz.thumbnail import (
    generate_vector_thumbnail,
    get_thumbnail_config,
    thumbnail_path_for,
)

if TYPE_CHECKING:
    from pathlib import Path

logger = logging.getLogger(__name__)

PARQUET_SUFFIX = ".parquet"


# --- Errors ---


class PMTilesError(PortolanError):
    """Base class for PMTiles-related errors."""

    code = "PRTLN-PMT000"


class PMTilesNotAvailableError(PMTilesError):
    """Raised when geoparquet-io does not provide PMTiles generation.

    Error code: PRTLN-PMT001
    """

    code = "PRTLN-PMT001"

    def __init__(self) -> None:
        super().__init__(
            "geoparquet-io does not provide PMTiles generation. "
            "Install with: pip install 'geoparquet-io>=1.3.0'"
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
        skipped: Paths to PMTiles that this run did not build.
        failed: List of (source_path, error_message) for failed generations.
        sources: The GeoParquet of each PMTiles in the plan.
        older: PMTiles that were older than their GeoParquet at the start.
    """

    generated: list[Path] = field(default_factory=list)
    skipped: list[Path] = field(default_factory=list)
    failed: list[tuple[Path, str]] = field(default_factory=list)
    sources: dict[Path, Path] = field(default_factory=dict)
    older: list[Path] = field(default_factory=list)

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
        PMTilesNotAvailableError: If geoparquet-io has no PMTiles support.
        TippecanoeNotFoundError: If tippecanoe is not in PATH.
    """
    # Check for the geoparquet-io PMTiles API. geoparquet-io is a core
    # dependency, so this only fails on a version below 1.3.0 (Issue #754).
    try:
        from geoparquet_io.api.ops import (  # type: ignore[import-untyped] # noqa: F401
            create_pmtiles,
        )
    except ImportError as e:
        raise PMTilesNotAvailableError from e

    # Check for tippecanoe
    if shutil.which("tippecanoe") is None:
        raise TippecanoeNotFoundError


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

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    assets = data.get("assets", {})

    geoparquet_assets = []
    for key, asset in assets.items():
        # Only the data is a tile source. This skips the item mirror
        # (collection-mirror), which is a Parquet of STAC items, and a source.
        if "data" not in asset.get("roles", []):
            continue
        asset_path = collection_path / str(asset.get("href", "")).removeprefix("./")
        # The footer decides, not the suffix: a plain table is no tile source.
        # A partition glob such as kdtree_cell=*/*.parquet is no file either.
        if (
            asset_path.suffix.lower() == PARQUET_SUFFIX
            and asset_path.is_file()
            and is_geoparquet(asset_path)
        ):
            geoparquet_assets.append((key, asset_path))

    return geoparquet_assets


def _older_than_data(parquet_path: Path, pmtiles_path: Path) -> bool:
    """Report whether an existing PMTiles is older than its GeoParquet.

    This is the one up-to-date rule. Tiles that are not older than the
    GeoParquet are current. No PMTiles writer records the checksum of its
    source, and geoparquet-io gives no way to write one, so the mtime is the
    only comparison.
    """
    return pmtiles_path.exists() and parquet_path.stat().st_mtime > pmtiles_path.stat().st_mtime


def _should_generate(parquet_path: Path, pmtiles_path: Path, force: bool) -> bool:
    """Determine if PMTiles should be generated.

    Args:
        parquet_path: Path to source GeoParquet file.
        pmtiles_path: Path to target PMTiles file.
        force: If True, always regenerate.

    Returns:
        True if PMTiles should be generated.
    """
    return force or not pmtiles_path.exists() or _older_than_data(parquet_path, pmtiles_path)


def _set_aside(pmtiles_path: Path) -> Path | None:
    """Move an existing PMTiles to a hidden name for the length of a build.

    tippecanoe refuses to overwrite a tileset (exit code 104). The caller puts
    the file back when the build fails, so a failed rebuild keeps valid data
    (Issue #814). A hidden file is never an asset.
    """
    if not pmtiles_path.exists():
        return None
    previous = pmtiles_path.with_name(f".{pmtiles_path.name}.previous")
    pmtiles_path.replace(previous)
    return previous


def _end_build(pmtiles_path: Path, previous: Path | None, *, succeeded: bool) -> None:
    """Remove the partial output of a failed build and put the old tileset back.

    A partial file is a phantom asset (Issue #385). The tileset of an earlier
    run is valid data, so a failed rebuild keeps it (Issue #814). After a
    build that succeeds, the old tileset goes.
    """
    if succeeded:
        if previous is not None:
            previous.unlink(missing_ok=True)
        return
    # missing_ok=True avoids TOCTOU race condition
    if pmtiles_path.exists():
        pmtiles_path.unlink(missing_ok=True)
        warn(f"Cleaned up partial file after failure: {pmtiles_path.name}")
    if previous is not None:
        previous.replace(pmtiles_path)
        warn(f"Kept the previous {pmtiles_path.name}: the rebuild failed.")


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
        PMTilesNotAvailableError: If geoparquet-io has no PMTiles support.
        TippecanoeNotFoundError: If tippecanoe not in PATH.
        PMTilesGenerationError: If generation fails.
    """
    check_pmtiles_available()

    from geoparquet_io.api.ops import create_pmtiles

    try:
        create_pmtiles(
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
        from portolan_cli.viz.style import (
            VectorStyleConfig,
            get_vector_style_config,
            write_default_style,
        )
    except ImportError:
        logger.debug("Style dependencies not available")
        return None

    try:
        metadata = extract_geoparquet_metadata(parquet_path)
        if not metadata.geometry_type:
            logger.debug("No geometry type found in %s", parquet_path)
            return None
        geometry_type = _style_geometry_type(metadata.geometry_type)

        config = get_vector_style_config(catalog_path) if catalog_path else VectorStyleConfig()

        # Thread the archive's zoom range into the style source so MapLibre
        # never requests tiles beyond what tippecanoe produced (issue #756).
        min_zoom, max_zoom = _pmtiles_zoom_range(collection_path / pmtiles_relative_path)

        return write_default_style(
            collection_path=collection_path,
            geometry_type=geometry_type,
            source_layer=layer_name,
            pmtiles_relative_path=pmtiles_relative_path,
            config=config,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
        )
    except Exception as e:
        logger.debug("Failed to write default style for %s: %s", parquet_path, e)
        return None


def _style_geometry_type(geometry_types: list[str]) -> str:
    """Pick the one geometry type the default style draws (issue #1010).

    The default style holds one layer, so a layer that declares several types
    gets the layer of its highest dimension: polygon, then line, then point.
    A fill or line layer shows where the data lies. A circle layer drawn over
    polygons shows only their vertices.
    """
    for family in ("polygon", "line"):
        for geometry_type in geometry_types:
            if family in geometry_type.lower():
                return geometry_type
    return geometry_types[0]


def _archive_layer(pmtiles_path: Path) -> str | None:
    """The first vector layer a PMTiles archive names, or None if unreadable."""
    try:
        from portolan_cli.metadata.pmtiles import extract_pmtiles_metadata

        return extract_pmtiles_metadata(pmtiles_path).layer_name
    except Exception as e:  # noqa: BLE001 - the layer name falls back to the stem
        logger.debug("Could not read the PMTiles layer of %s: %s", pmtiles_path, e)
        return None


def _pmtiles_zoom_range(pmtiles_path: Path) -> tuple[int | None, int | None]:
    """Read (min_zoom, max_zoom) from a PMTiles archive header, or (None, None).

    Zoom range is advisory for styles: a style without it still loads, so any
    failure to read the header degrades to omitting the range rather than
    failing style generation.
    """
    try:
        from portolan_cli.metadata.pmtiles import extract_pmtiles_metadata

        meta = extract_pmtiles_metadata(pmtiles_path)
    except Exception as e:  # pragma: no cover - defensive, header read is best-effort
        logger.debug("Could not read PMTiles zoom range from %s: %s", pmtiles_path, e)
        return None, None
    return meta.min_zoom, meta.max_zoom


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
    An asset that already points at ``pmtiles_href`` keeps its key, so the file
    never gets a second entry (Issue #933).

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

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    assets = data.get("assets", {})

    # An asset that already tracks this file keeps its key. Otherwise derive the
    # key from the parquet key (Issue #933).
    pmtiles_key = next(
        (key for key, asset in assets.items() if asset.get("href") == pmtiles_href),
        pmtiles_asset_key(parquet_key),
    )

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

        # Regeneration rewrites the tiles, so a stale digest is worse than none.
        if stamp_file_fields(existing, collection_path):
            needs_update = True
        if sync_file_extension(data, assets, collection_json_path):
            needs_update = True

        if needs_update:
            write_json_atomic(collection_json_path, data)
        return

    asset_dict: dict[str, Any] = {
        "href": pmtiles_href,
        "type": PMTILES_MEDIA_TYPE,
        "title": f"{source_title} (vector tiles)",
        "roles": [ROLE_VISUAL],
    }

    # Add any extra properties
    if extra_properties:
        asset_dict.update(extra_properties)

    assets[pmtiles_key] = asset_dict
    data["assets"] = assets

    # PORTO-CORE-028 asks every asset for file:size and file:checksum. The style
    # and thumbnail writers stamp theirs; PMTiles was the one visual artifact
    # that shipped without them, drawing a PTL-AST-003 warning nothing caught
    # until the gate started reading warnings (issue #746).
    stamp_file_fields(asset_dict, collection_path)
    sync_file_extension(data, assets, collection_json_path)

    write_json_atomic(collection_json_path, data)


def ensure_web_map_links_extension(collection_path: Path) -> bool:
    """Declare the web-map-links extension in a collection idempotently.

    Adds ``WEB_MAP_LINKS_EXTENSION`` to ``stac_extensions`` if absent, without
    touching any links (so existing ``pmtiles:layers`` overrides are preserved).
    Used by ``check --fix`` to repair a collection that carries the PMTiles link
    but omits the extension declaration (part of rashid PTL-VIZ-003).

    Args:
        collection_path: Path to collection directory.

    Returns:
        True if the extension was added and the file rewritten, else False.
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return False

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    extensions = data.get("stac_extensions", [])
    if WEB_MAP_LINKS_EXTENSION in extensions:
        return False

    extensions.append(WEB_MAP_LINKS_EXTENSION)
    data["stac_extensions"] = extensions
    write_json_atomic(collection_json_path, data)
    return True


def add_pmtiles_link_to_collection(
    collection_path: Path,
    pmtiles_href: str,
    *,
    layers: list[str],
) -> None:
    """Add a ``rel="pmtiles"`` collection link following web-map-links (Issue #569).

    The link coexists with the PMTiles *asset* and satisfies rashid PTL-VIZ-003.
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

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))

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
        write_json_atomic(collection_json_path, data)


def _backfill_skipped_assets(collection_path: Path, pmtiles_path: Path, catalog_root: Path) -> None:
    """Track an up-to-date PMTiles and its thumbnail if not already tracked.

    Runs on the skip path to heal catalogs whose artifacts were generated before
    versions.json tracking existed (Issue #519), without bumping a version when
    everything is already tracked.

    Registering the thumbnail as a STAC asset is not done here.
    ``collection_thumbnail.ensure_collection_thumbnails`` adopts any
    ``.thumb.jpg`` on disk and is the single writer of the thumbnail asset
    (Issue #683), so a backfilled versions.json entry still cannot be orphaned
    from collection.json.
    """
    backfill = [pmtiles_path]
    existing_thumb = thumbnail_path_for(pmtiles_path)
    if existing_thumb.exists():
        backfill.append(existing_thumb)
    try:
        track_generated_assets(
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
    catalog_root: Path,
) -> Path | None:
    """Render the vector thumbnail, preferring the exact GeoParquet geometry.

    Writes the file and returns its path, or None if disabled or failed. Failure
    is non-fatal: it must not affect PMTiles success (Issue #13). PMTiles are
    only the fallback source: at thumbnail zoom they carry tippecanoe's
    simplification and tiny-polygon placeholder squares (issue #786).

    Registering the STAC asset is deliberately not done here.
    ``collection_thumbnail.ensure_collection_thumbnails`` runs after this and
    adopts the ``.thumb.jpg`` this writes, so one writer owns the thumbnail
    asset shape (Issue #683). This render still wins, because it runs first
    and adoption never overwrites.
    """
    try:
        thumb_config = get_thumbnail_config(catalog_root)
        if not thumb_config.enabled:
            return None
        # Discover style for thumbnail (Issue #495)
        style_path = _discover_style_for_thumbnail(collection_path)
        return generate_vector_thumbnail(
            pmtiles_path=pmtiles_path,  # fallback
            geoparquet_path=parquet_path,
            config=thumb_config,
            style_path=style_path,
        )
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
        track_generated_assets(collection_path, generated_assets, catalog_root, message=message)
    except Exception as e:
        warn(f"Failed to track generated assets in versions.json for {pmtiles_path.name}: {e}")


def generate_pmtiles_for_collection(
    collection_path: Path,
    catalog_root: Path,
    *,
    generate: bool = True,
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

    A sibling PMTiles that this run does not build is registered as it is: the
    asset, the ``rel: pmtiles`` link, and the default style (rashid PTL-VIZ-002
    and PTL-VIZ-003). tippecanoe is checked only when a tile is built, so
    pre-built tiles need no tippecanoe (Issue #934). With ``generate`` True,
    a sibling PMTiles older than its GeoParquet is built again. With
    ``generate`` False, it is registered as it is and ``older`` names it.
    ``add`` rewrites a GeoParquet in place for the bbox column (#805), so
    user tiles (#929) look older although the data is the same. A rebuild
    with the default settings would replace them.

    Args:
        collection_path: Path to collection directory.
        catalog_root: Path to catalog root.
        force: If True, regenerate even if PMTiles exists and is up-to-date.
        generate: If False, build nothing and only register the sibling PMTiles
            that exist. ``add`` runs this without ``--pmtiles``, so tiles a user
            built with tippecanoe get the same registration (Issue #929).
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
        PMTilesNotAvailableError: If a tile must be built and geoparquet-io has
            no PMTiles support.
        TippecanoeNotFoundError: If a tile must be built and tippecanoe is not
            in PATH.
    """
    plan = [
        (asset_key, parquet_path, parquet_path.with_suffix(PMTILES_SUFFIX))
        for asset_key, parquet_path in _find_geoparquet_assets(collection_path)
    ]
    if not generate:
        plan = [entry for entry in plan if entry[2].exists()]
        if not plan:
            return PMTilesResult()
    result = PMTilesResult(
        sources={pmtiles_path: parquet_path for _, parquet_path, pmtiles_path in plan},
        older=[pm for _, parquet, pm in plan if _older_than_data(parquet, pm)],
    )
    builds = {
        pmtiles_path
        for _, parquet_path, pmtiles_path in plan
        if generate and _should_generate(parquet_path, pmtiles_path, force)
    }
    if builds:
        check_pmtiles_available()

    for asset_key, parquet_path, pmtiles_path in plan:
        # Compute href relative to collection (preserves subdirectory structure)
        # Use as_posix() for STAC-compliant forward slashes on all platforms
        try:
            pmtiles_rel = pmtiles_path.relative_to(collection_path)
            pmtiles_href = f"./{pmtiles_rel.as_posix()}"
        except ValueError:
            pmtiles_href = f"./{pmtiles_path.name}"

        # Compute collection-relative PMTiles path for style source URLs
        try:
            pmtiles_col_rel = pmtiles_path.relative_to(collection_path).as_posix()
        except ValueError:
            pmtiles_col_rel = pmtiles_path.name

        if pmtiles_path not in builds:
            # Tiles built elsewhere name their layer themselves (Issue #929).
            layer_name = layer or _archive_layer(pmtiles_path) or parquet_path.stem
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
            _backfill_skipped_assets(collection_path, pmtiles_path, catalog_root)
            result.skipped.append(pmtiles_path)
            continue

        # Determine layer name (Issue #13)
        layer_name = layer or parquet_path.stem
        # Track success to clean up partial files on any failure (Issue #385)
        # Using finally ensures cleanup even on KeyboardInterrupt/SystemExit
        generation_succeeded = False
        previous = _set_aside(pmtiles_path)
        try:
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

            # Generate default style file
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
            _end_build(pmtiles_path, previous, succeeded=generation_succeeded)

        # Generate thumbnail separately - failure shouldn't affect PMTiles success
        # (Issue #13) - then track the PMTiles and thumbnail in a SINGLE version
        # snapshot (one side-step == one version, not two, Issue #519).
        if generation_succeeded:
            thumb_path = _generate_thumbnail_asset(
                collection_path, parquet_path, pmtiles_path, catalog_root
            )
            _track_side_step_assets(collection_path, pmtiles_path, thumb_path, catalog_root)

    # Discover and register style assets
    from portolan_cli.viz.style import (
        complete_style_sources,
        discover_styles,
        register_style_assets,
    )

    # Repair styles written before the archive existed (extract-produced SLD
    # styles have a vector source with no URL) and normalize bare-path URLs to
    # the loadable pmtiles:// form, filling the archive's zoom range (#756).
    if result.generated:
        newest = result.generated[-1]
        min_zoom, max_zoom = _pmtiles_zoom_range(newest)
        complete_style_sources(
            collection_path,
            pmtiles_relative_path=newest.name,
            min_zoom=min_zoom,
            max_zoom=max_zoom,
        )

    styles = discover_styles(collection_path)
    register_style_assets(collection_path, styles)

    return result


@dataclass
class PMTilesSettings:
    """Per-collection PMTiles generation settings resolved from config."""

    enabled: bool = False
    min_zoom: int | None = None
    max_zoom: int | None = None
    layer: str | None = None
    bbox: str | None = None
    where: str | None = None
    include_cols: str | None = None
    precision: int = 6
    attribution: str | None = None
    src_crs: str | None = None


def get_pmtiles_settings(catalog_root: Path, coll_id: str, coll_path: Path) -> PMTilesSettings:
    """Resolve PMTiles settings for a collection via hierarchical config."""
    from portolan_cli.config import coerce_bool, coerce_int, get_setting

    def get(key: str) -> Any:
        return get_setting(
            f"pmtiles.{key}",
            catalog_path=catalog_root,
            collection=coll_id,
            collection_path=coll_path,
        )

    return PMTilesSettings(
        enabled=coerce_bool(get("enabled"), default=False),
        min_zoom=None if get("min_zoom") is None else coerce_int(get("min_zoom"), default=0),
        max_zoom=None if get("max_zoom") is None else coerce_int(get("max_zoom"), default=14),
        layer=get("layer"),
        bbox=get("bbox"),
        where=get("where"),
        include_cols=get("include_cols"),
        precision=coerce_int(get("precision"), default=6),
        attribution=get("attribution"),
        src_crs=get("src_crs"),
    )


def generate_or_suggest_pmtiles(
    catalog_root: Path,
    affected_collections: set[str],
    *,
    generate_pmtiles: bool,
    force: bool,
    verbose: bool,
    use_json: bool = False,
) -> None:
    """Generate PMTiles for affected collections when requested or configured.

    For each affected collection, resolves PMTiles settings and generates when
    ``--pmtiles`` or ``--force-pmtiles`` was passed, or ``pmtiles.enabled`` is
    configured. Otherwise it only registers the sibling PMTiles that exist
    (Issue #929). Generation runs
    regardless of output mode so the JSON envelope reflects the final state; an explicitly-requested generation that fails exits non-zero.

    Args:
        catalog_root: Catalog root directory.
        affected_collections: Collection IDs modified by the add command.
        generate_pmtiles: Whether ``--pmtiles`` was passed.
        force: Whether to regenerate up-to-date PMTiles.
        verbose: Whether to emit per-file detail.
        use_json: Whether the command runs in JSON output mode.
    """
    if not affected_collections:
        return

    # --force-pmtiles asks for a rebuild, so it also turns generation on. The
    # default is pmtiles.enabled false, so the flag alone used to exit 0 and
    # leave the old tiles in place (Issue #760). `requested` also drives the
    # reports below: an explicit request that fails must exit non-zero.
    requested = generate_pmtiles or force

    for coll_id in affected_collections:
        coll_path = catalog_root / coll_id
        if not (coll_path / "collection.json").exists():
            continue

        settings = get_pmtiles_settings(catalog_root, coll_id, coll_path)
        # Without a request, register the tiles a user built and build none.
        generate = requested or settings.enabled
        try:
            result = generate_pmtiles_for_collection(
                coll_path,
                catalog_root,
                force=force,
                generate=generate,
                min_zoom=settings.min_zoom,
                max_zoom=settings.max_zoom,
                layer=settings.layer,
                bbox=settings.bbox,
                where=settings.where,
                include_cols=settings.include_cols,
                precision=settings.precision,
                attribution=settings.attribution,
                src_crs=settings.src_crs,
            )
            _report_pmtiles_result(result, verbose, requested, use_json=use_json)
        except PMTilesNotAvailableError as e:
            _report_pmtiles_unavailable(
                e, requested, settings.enabled, verbose, coll_id, use_json=use_json
            )
        except TippecanoeNotFoundError as e:
            _report_tippecanoe_missing(e, requested, coll_id, use_json=use_json)


def _report_pmtiles_result(
    result: PMTilesResult, verbose: bool, explicit_flag: bool, *, use_json: bool = False
) -> None:
    """Report PMTiles generation results; exit non-zero on explicit-flag failure."""
    if not use_json:
        for p in result.generated:
            if p in result.older:
                success(f"Rebuilt PMTiles: {p.name} was older than {result.sources[p].name}")
            else:
                success(f"Generated PMTiles: {p.name}")
        for p in result.skipped:
            if p in result.older:
                warn(
                    f"Kept PMTiles: {p.name} is older than {result.sources[p].name}. "
                    "Pass --pmtiles to rebuild it."
                )
            elif verbose:
                info(f"Skipped PMTiles: {p.name} is not older than {result.sources[p].name}")
    for path, error_msg in result.failed:
        if explicit_flag:
            if not use_json:
                error(f"PMTiles generation failed: {error_msg}")
            raise SystemExit(1)
        if not use_json:
            warn(f"Failed to generate PMTiles for '{path}': {error_msg}")


def _report_pmtiles_unavailable(
    e: Exception,
    explicit_flag: bool,
    config_enabled: bool,
    verbose: bool,
    coll_id: str,
    *,
    use_json: bool = False,
) -> None:
    """Report a PMTilesNotAvailableError; exit non-zero when explicitly requested."""
    if explicit_flag:
        if not use_json:
            error(str(e))
        raise SystemExit(1) from e
    if use_json:
        return
    # Warn if pmtiles.enabled in config (user expectation), or info if just verbose
    if config_enabled:
        warn(f"PMTiles enabled for '{coll_id}' but geoparquet-io has no PMTiles support")
    elif verbose:
        info(f"Skipping PMTiles for '{coll_id}': geoparquet-io has no PMTiles support")


def _report_tippecanoe_missing(
    e: Exception, explicit_flag: bool, coll_id: str, *, use_json: bool = False
) -> None:
    """Report a TippecanoeNotFoundError; exit non-zero when explicitly requested."""
    if explicit_flag:
        if not use_json:
            error(str(e))
        raise SystemExit(1) from e
    if not use_json:
        warn(f"Skipping PMTiles for '{coll_id}': tippecanoe not installed")
