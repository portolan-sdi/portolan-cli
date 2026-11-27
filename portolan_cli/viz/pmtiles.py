"""PMTiles generation from GeoParquet collections."""

from __future__ import annotations

import json
import logging
import shutil
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from portolan_cli.formats import is_geoparquet, run_scope
from portolan_cli.report import emit
from portolan_cli.stac_parquet import stamp_file_fields, sync_file_extension
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
from portolan_core.constants import ROLE_VISUAL
from portolan_core.errors import PortolanError
from portolan_core.json_io import write_json_atomic
from portolan_core.versions import track_generated_assets

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

    from portolan_core.config import ConfigSource
    from portolan_core.run_state import RunState

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
    """Result of PMTiles generation for a collection."""

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
    """Check that PMTiles generation dependencies are available."""
    # geoparquet-io below 1.3.0 has no PMTiles API.
    try:
        from geoparquet_io.api.ops import (  # noqa: F401 - probes that the PMTiles API exists
            create_pmtiles,
        )
    except ImportError as e:
        raise PMTilesNotAvailableError from e

    # Check for tippecanoe
    if shutil.which("tippecanoe") is None:
        raise TippecanoeNotFoundError


def _find_geoparquet_assets(collection_path: Path) -> list[tuple[str, Path]]:
    """Find all GeoParquet assets in a collection."""
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
    """Report whether an existing PMTiles is older than its GeoParquet."""
    return pmtiles_path.exists() and parquet_path.stat().st_mtime > pmtiles_path.stat().st_mtime


def _should_generate(parquet_path: Path, pmtiles_path: Path, force: bool) -> bool:
    """Determine if PMTiles should be generated."""
    return force or not pmtiles_path.exists() or _older_than_data(parquet_path, pmtiles_path)


def _set_aside(pmtiles_path: Path) -> Path | None:
    """Move an existing PMTiles to a hidden name for the length of a build."""
    if not pmtiles_path.exists():
        return None
    previous = pmtiles_path.with_name(f".{pmtiles_path.name}.previous")
    pmtiles_path.replace(previous)
    return previous


def _end_build(pmtiles_path: Path, previous: Path | None, *, succeeded: bool) -> None:
    """Remove the partial output of a failed build and put the old tileset back."""
    if succeeded:
        if previous is not None:
            previous.unlink(missing_ok=True)
        return
    # missing_ok=True avoids TOCTOU race condition
    if pmtiles_path.exists():
        pmtiles_path.unlink(missing_ok=True)
        emit("warn", f"Cleaned up partial file after failure: {pmtiles_path.name}")
    if previous is not None:
        previous.replace(pmtiles_path)
        emit("warn", f"Kept the previous {pmtiles_path.name}: the rebuild failed.")


def _discover_style_for_thumbnail(collection_dir: Path) -> Path | None:
    """Find a style file for thumbnail generation."""
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
    """Generate a single PMTiles file from GeoParquet."""
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
    state: RunState | None = None,
) -> Path | None:
    """Write a default style file for a PMTiles asset."""
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
        with run_scope(state):
            metadata = extract_geoparquet_metadata(parquet_path)
        if not metadata.geometry_type:
            logger.debug("No geometry type found in %s", parquet_path)
            return None
        geometry_type = _style_geometry_type(metadata.geometry_type)

        config = (
            get_vector_style_config(catalog_path, state=state)
            if catalog_path
            else VectorStyleConfig()
        )

        # Pass the archive's zoom range so MapLibre never requests tiles tippecanoe did not make.
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
    """Pick the one geometry type the default style draws."""
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
    except Exception as e:  # the layer name falls back to the stem
        logger.debug("Could not read the PMTiles layer of %s: %s", pmtiles_path, e)
        return None


def _pmtiles_zoom_range(pmtiles_path: Path) -> tuple[int | None, int | None]:
    """Read (min_zoom, max_zoom) from a PMTiles archive header, or (None, None)."""
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
    """Add PMTiles asset to collection.json."""
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        raise FileNotFoundError(f"collection.json not found in {collection_path}")

    data = json.loads(collection_json_path.read_text(encoding="utf-8"))
    assets = data.get("assets", {})

    # An asset that already tracks this file keeps its key; otherwise derive it from the parquet key.
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

    # PORTO-CORE-028 asks every asset for file:size and file:checksum.
    stamp_file_fields(asset_dict, collection_path)
    sync_file_extension(data, assets, collection_json_path)

    write_json_atomic(collection_json_path, data)


def ensure_web_map_links_extension(collection_path: Path) -> bool:
    """Declare the web-map-links extension in a collection idempotently."""
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
    """Add a ``rel="pmtiles"`` collection link following web-map-links."""
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


def _backfill_skipped_assets(
    collection_path: Path, pmtiles_path: Path, catalog_root: Path, state: RunState | None
) -> None:
    """Track an up-to-date PMTiles and its thumbnail if not already tracked."""
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
            state=state,
        )
    except Exception as e:
        emit("warn", f"Failed to backfill versions.json tracking for {pmtiles_path.name}: {e}")


def _generate_thumbnail_asset(
    collection_path: Path,
    parquet_path: Path,
    pmtiles_path: Path,
    catalog_root: Path,
    state: RunState | None,
) -> Path | None:
    """Render the vector thumbnail, preferring the exact GeoParquet geometry."""
    try:
        thumb_config = get_thumbnail_config(catalog_root, state=state)
        if not thumb_config.enabled:
            return None
        # Discover the style for the thumbnail
        style_path = _discover_style_for_thumbnail(collection_path)
        return generate_vector_thumbnail(
            pmtiles_path=pmtiles_path,  # fallback
            geoparquet_path=parquet_path,
            config=thumb_config,
            style_path=style_path,
        )
    except Exception as e:
        emit("warn", f"Thumbnail generation failed for {pmtiles_path.name}: {e}")
        return None


def _track_side_step_assets(
    collection_path: Path,
    pmtiles_path: Path,
    thumb_path: Path | None,
    catalog_root: Path,
    state: RunState | None,
) -> None:
    """Track the PMTiles and its thumbnail in a SINGLE versions.json snapshot."""
    generated_assets = [pmtiles_path]
    if thumb_path:
        generated_assets.append(thumb_path)
        message = f"Generated PMTiles and thumbnail: {pmtiles_path.name}, {thumb_path.name}"
    else:
        message = f"Generated PMTiles: {pmtiles_path.name}"
    try:
        track_generated_assets(
            collection_path, generated_assets, catalog_root, message=message, state=state
        )
    except Exception as e:
        emit(
            "warn",
            f"Failed to track generated assets in versions.json for {pmtiles_path.name}: {e}",
        )


def generate_pmtiles_for_collection(  # noqa: PLR0913, PLR0915 - each tippecanoe option is an argument
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
    state: RunState | None = None,
) -> PMTilesResult:
    """Generate PMTiles for all GeoParquet assets in a collection."""
    # The footer read of each asset goes through the run memo.
    with run_scope(state):
        assets = _find_geoparquet_assets(collection_path)
    plan = [
        (asset_key, parquet_path, parquet_path.with_suffix(PMTILES_SUFFIX))
        for asset_key, parquet_path in assets
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
            # Tiles built elsewhere name their layer themselves.
            layer_name = layer or _archive_layer(pmtiles_path) or parquet_path.stem
            # Ensure asset is registered/updated in collection.json even when skipping
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)
            # Emit the rel="pmtiles" web-map-links link alongside the asset
            add_pmtiles_link_to_collection(collection_path, pmtiles_href, layers=[layer_name])
            # Ensure default style exists even when PMTiles generation is skipped
            _write_default_style_for_geoparquet(
                parquet_path=parquet_path,
                layer_name=layer_name,
                collection_path=collection_path,
                pmtiles_relative_path=pmtiles_col_rel,
                catalog_path=catalog_root,
                state=state,
            )
            # Backfill versions.json for artifacts generated before tracking existed.
            _backfill_skipped_assets(collection_path, pmtiles_path, catalog_root, state)
            result.skipped.append(pmtiles_path)
            continue

        # Determine layer name
        layer_name = layer or parquet_path.stem
        # Clean up partial files on any failure, including KeyboardInterrupt.
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

            # Register asset in collection.json
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)
            # Emit the rel="pmtiles" web-map-links link alongside the asset
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
                state=state,
            )

        except PMTilesGenerationError as e:
            result.failed.append((parquet_path, str(e)))
        except Exception as e:
            result.failed.append((parquet_path, f"Unexpected error: {e}"))
        finally:
            _end_build(pmtiles_path, previous, succeeded=generation_succeeded)

        # A thumbnail failure does not fail PMTiles; track both in one version snapshot.
        if generation_succeeded:
            thumb_path = _generate_thumbnail_asset(
                collection_path, parquet_path, pmtiles_path, catalog_root, state
            )
            _track_side_step_assets(collection_path, pmtiles_path, thumb_path, catalog_root, state)

    # Discover and register style assets
    from portolan_cli.viz.style import (
        complete_style_sources,
        discover_styles,
        register_style_assets,
    )

    # Repair styles with no source URL; rewrite bare-path URLs to pmtiles:// and fill the zoom range.
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


def get_pmtiles_settings(
    catalog_root: Path,
    coll_id: str,
    coll_path: Path,
    *,
    state: ConfigSource | None = None,
) -> PMTilesSettings:
    """Resolve PMTiles settings for a collection via hierarchical config.

    With a run state, the ten keys share one merge of the collection config.
    """
    from portolan_core.config import coerce_bool, coerce_int, get_setting

    def get(key: str) -> Any:
        if state is not None:
            return state.setting(f"pmtiles.{key}", collection_dir=coll_path)
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


@dataclass
class PMTilesOutcome:
    """What the PMTiles side-step did for one collection."""

    collection_id: str
    config_enabled: bool
    requested: bool
    result: PMTilesResult | None = None
    missing: PMTilesNotAvailableError | TippecanoeNotFoundError | None = None
    failure: str | None = None


@dataclass
class PMTilesReport:
    """The outcome of one ``generate_or_suggest_pmtiles`` run."""

    outcomes: list[PMTilesOutcome] = field(default_factory=list)

    @property
    def failure(self) -> str | None:
        """The message of the explicit request that failed, or None."""
        return next((o.failure for o in self.outcomes if o.failure is not None), None)


def generate_or_suggest_pmtiles(
    catalog_root: Path,
    affected_collections: set[str],
    *,
    generate_pmtiles: bool,
    force: bool,
    state: RunState | None = None,
    on_outcome: Callable[[PMTilesOutcome], None] | None = None,
) -> PMTilesReport:
    """Generate PMTiles for affected collections when requested or configured."""
    # --force-pmtiles also turns generation on. `requested` makes a failed explicit request exit non-zero.
    requested = generate_pmtiles or force
    report = PMTilesReport()

    for coll_id in affected_collections:
        coll_path = catalog_root / coll_id
        if not (coll_path / "collection.json").exists():
            continue

        settings = get_pmtiles_settings(catalog_root, coll_id, coll_path, state=state)
        outcome = PMTilesOutcome(coll_id, settings.enabled, requested)
        report.outcomes.append(outcome)
        try:
            outcome.result = generate_pmtiles_for_collection(
                coll_path,
                catalog_root,
                force=force,
                # Without a request, register the tiles a user built and build none.
                generate=requested or settings.enabled,
                min_zoom=settings.min_zoom,
                max_zoom=settings.max_zoom,
                layer=settings.layer,
                bbox=settings.bbox,
                where=settings.where,
                include_cols=settings.include_cols,
                precision=settings.precision,
                attribution=settings.attribution,
                src_crs=settings.src_crs,
                state=state,
            )
        except (PMTilesNotAvailableError, TippecanoeNotFoundError) as e:
            outcome.missing = e
        if requested:
            outcome.failure = _explicit_failure(outcome)
        if on_outcome is not None:
            on_outcome(outcome)
        if outcome.failure is not None:
            break
    return report


def _explicit_failure(outcome: PMTilesOutcome) -> str | None:
    """Return the message of an explicit request that failed for ``outcome``."""
    if outcome.missing is not None:
        return str(outcome.missing)
    if outcome.result is not None and outcome.result.failed:
        return f"PMTiles generation failed: {outcome.result.failed[0][1]}"
    return None
