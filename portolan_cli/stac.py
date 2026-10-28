"""STAC generation module - wraps pystac for Portolan's conventions.

Provides opinionated helpers for creating STAC catalogs, collections, and items
with consistent defaults and conventions for Portolan-managed catalogs.

Key conventions:
- Self-contained catalog type (relative links, portable)
- WGS84 (EPSG:4326) as default CRS
- Consistent asset naming and roles
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

import pystac

from portolan_cli.bbox import compute_bbox_union, to_2d_bbox
from portolan_cli.constants import PARTITION_EXTENSION_URI, PORTOLAN_SCHEMA_URI, ROLE_VISUAL
from portolan_cli.errors import AssetKeyCollisionError, InvalidBboxError
from portolan_cli.extension_registry import EXTENSION_REGISTRY
from portolan_cli.humanize import humanize_slug
from portolan_cli.json_io import write_json_atomic
from portolan_cli.providers import derive_provenance, resolve_providers
from portolan_cli.viz.pmtiles_links import PMTILES_MEDIA_TYPE, pmtiles_asset_key

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Mapping, Sequence

    from portolan_cli.metadata.tabular import TabularMetadata

logger = logging.getLogger(__name__)

# Any versioned Portolan profile URI, not just the current one: matching the
# whole family is what lets a stale claim be rewritten rather than duplicated.
# The capture groups read the SemVer triple, so the stamper can compare versions
# and keep the higher one (issue #755).
PORTOLAN_SCHEMA_URI_PATTERN = re.compile(
    r"^https://schemas\.portolan-sdi\.org/portolan/v(\d+)\.(\d+)\.(\d+)/schema\.json$"
)


def _profile_version(uri: str) -> tuple[int, int, int] | None:
    """Return the SemVer triple of a Portolan profile URI, or None when it does not match."""
    match = PORTOLAN_SCHEMA_URI_PATTERN.match(uri)
    if match is None:
        return None
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


class MergeStrategy(Enum):
    """Strategy for merging existing metadata with auto-detected values.

    Issue #446: Controls how `portolan add` handles conflicts between
    human-authored metadata and auto-detected values.

    The strategy decides only the fields a person writes: an asset's title,
    description and custom fields, and a column's description. Every field
    Portolan reads from the data (href, media type, roles, ``file:*``,
    ``proj:*``, column names and types, row counts, extents, summaries) comes
    from the data under every strategy, so a removed column or a stale extent
    cannot survive a rebuild (issues #801, #931).

    Strategies:
        SMART: Keep a human value, and fill a missing one from the data.
        KEEP: Keep a human value, and never fill it from the data.
        OVERWRITE: Drop the human values.
        INTERACTIVE: Prompt per-conflict (not yet implemented).
    """

    SMART = "smart"
    KEEP = "keep"
    OVERWRITE = "overwrite"
    INTERACTIVE = "interactive"


# Fields that are human-enrichable (preserve existing by default in SMART mode)
HUMAN_ENRICHABLE_ASSET_FIELDS = frozenset({"title", "description"})

# Extra fields that are machine-derivable (update in SMART mode)
# All extension prefixes that are auto-detected from file metadata
MACHINE_DERIVABLE_EXTRA_FIELD_PREFIXES = frozenset(
    {
        "file:",  # file:size, file:checksum
        "proj:",  # proj:code, proj:wkt2
        "pmtiles:",  # pmtiles:min_zoom, pmtiles:max_zoom, etc.
        "flatgeobuf:",  # flatgeobuf:feature_count, etc.
        "raster:",  # raster:spatial_resolution, etc.
        "partition:",  # partition:glob (auto-generated on push)
    }
)

# Specific extra fields that are machine-derivable (not prefix-based)
MACHINE_DERIVABLE_EXTRA_FIELDS = frozenset(
    {
        "bands",  # Unified bands array (STAC v1.1.0)
    }
)

# Default license when not specified.
# STAC 1.1 deprecates "proprietary" (it is not an SPDX identifier); "other" is
# the spec keyword for a license not covered by an SPDX expression. A rel="license"
# link SHOULD be added by the user once the concrete license is known (issue #568).
DEFAULT_LICENSE = "other"

# Sentinel datetime values for provisional items (STAC 1.1.0 compliance)
# STAC 1.1.0 and pystac require start_datetime/end_datetime to be valid ISO 8601 strings
# when datetime is null. These sentinel values indicate "unknown temporal extent" while
# remaining parseable. The range is the marker: an item carrying it has no real
# temporal extent yet, which is readable without a custom field (issue #654).
PROVISIONAL_START_DATETIME = "1900-01-01T00:00:00Z"
PROVISIONAL_END_DATETIME = "9999-12-31T23:59:59Z"


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
        title: Optional display title. Defaults to a human-readable title
            derived from ``collection_id`` (Issue #502: titles are mandatory).
        license: SPDX license expression, or "other" for a non-SPDX license
            (default: "other"). STAC 1.1 no longer accepts "proprietary".
        bbox: Spatial extent as [min_x, min_y, max_x, max_y] in WGS84.
              Defaults to global extent if not specified.
        temporal_extent: Temporal extent as (start, end) datetimes.
                        Use None for open-ended intervals.

    Returns:
        A pystac.Collection object.
    """
    # Issue #502: titles and descriptions are mandatory and must be
    # human-readable. Derive a title from the id when none is supplied, and
    # fall back to the title for an empty description rather than leaving a
    # placeholder like "Collection: <slug>".
    if not title:
        title = humanize_slug(collection_id)
    if not description:
        description = title

    # Default to global extent if not specified
    if bbox is None:
        bbox = [-180, -90, 180, 90]

    # Default to open temporal interval
    if temporal_extent is None:
        temporal_interval: list[datetime | None] = [None, None]
    else:
        temporal_interval = list(temporal_extent)

    extent = pystac.Extent(
        spatial=build_spatial_extent([bbox]),
        temporal=pystac.TemporalExtent(intervals=[temporal_interval]),
    )

    return pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        title=title,
        license=license,
    )


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
        datetime: Acquisition/creation datetime. If None, the item carries a null
            datetime and the sentinel start/end range that stands for an unknown
            temporal extent.
        properties: Additional properties to include.
        assets: Asset dictionary to attach to the item.

    Returns:
        A pystac.Item object.
    """
    # Generate polygon geometry from bbox
    geometry = _bbox_to_polygon(bbox)

    # Merge any custom properties
    item_properties = dict(properties) if properties else {}

    # Issue #502: items must carry a human-readable title for STAC Browser.
    # Derive one from the item id when source metadata didn't supply a usable
    # title (humanize_slug also normalizes technical ids).
    if not item_properties.get("title") or is_technical_name(str(item_properties.get("title"))):
        item_properties["title"] = humanize_slug(item_id)

    # If datetime not provided, publish the sentinel range instead.
    # STAC 1.1.0 and pystac require start_datetime/end_datetime to be valid
    # ISO 8601 strings when datetime is null. We use an open-ended range
    # to indicate unknown temporal extent. The range says so on its own, so no
    # marker field travels with it (issue #654).
    if datetime is None:
        item_properties["start_datetime"] = PROVISIONAL_START_DATETIME
        item_properties["end_datetime"] = PROVISIONAL_END_DATETIME

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


def add_item_to_collection(collection: pystac.Collection, item: pystac.Item) -> None:
    """Add an item to a collection.

    The collection extent is not touched here. ``derive_collection_fields``
    computes it from every member once the collection holds them all.

    The item keeps the file path ``add`` wrote it to. pystac's default layout
    would move the item of a saved collection to ``<collection>/<id>/``, which
    for ``--item-id`` is a second copy away from the data (issue #840).

    Args:
        collection: The parent collection.
        item: The item to add.
    """
    from pystac.layout import AsIsLayoutStrategy

    strategy = AsIsLayoutStrategy() if item.get_self_href() is not None else None
    collection.add_item(item, strategy=strategy)


def _is_machine_derivable_extra_field(field_name: str) -> bool:
    """Check if an extra_field key is machine-derivable.

    Machine-derivable fields are updated in SMART merge mode.
    Human-authored custom fields are preserved.
    """
    if field_name in MACHINE_DERIVABLE_EXTRA_FIELDS:
        return True
    return any(field_name.startswith(prefix) for prefix in MACHINE_DERIVABLE_EXTRA_FIELD_PREFIXES)


def _merge_asset(
    existing: pystac.Asset,
    new: pystac.Asset,
    strategy: MergeStrategy,
) -> pystac.Asset:
    """Merge an existing asset with a new asset based on strategy.

    Args:
        existing: The existing asset in the collection.
        new: The new asset from auto-detection.
        strategy: The merge strategy to apply.

    Returns:
        The merged asset.
    """
    if strategy == MergeStrategy.OVERWRITE:
        return new

    # The strategy decides the human fields only. Machine fields always come
    # from the new asset, so KEEP no longer pins a stale href or checksum.
    fill = strategy != MergeStrategy.KEEP
    merged = pystac.Asset(
        href=new.href,
        media_type=new.media_type,
        roles=new.roles,
        title=existing.title or (new.title if fill else None),
        description=existing.description or (new.description if fill else None),
    )

    # A human custom field survives. A machine field the new asset no longer
    # reports is dropped, because the data no longer carries it.
    merged_extra: dict[str, object] = {
        key: value
        for key, value in (existing.extra_fields or {}).items()
        if not _is_machine_derivable_extra_field(key)
    }
    for key, value in (new.extra_fields or {}).items():
        if _is_machine_derivable_extra_field(key) or (fill and key not in merged_extra):
            merged_extra[key] = value

    if merged_extra:
        merged.extra_fields = merged_extra

    return merged


def add_asset_to_collection(
    collection: pystac.Collection,
    asset_key: str,
    asset: pystac.Asset,
    *,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add an asset directly to a collection (collection-level asset).

    Single vector files (GeoParquet, Shapefile, GeoPackage) are
    collection-level assets—no item.json, asset directly in collection.json.

    Issue #447 FIX: Before adding, check if ANY existing asset points to the same
    href. If so, use that key (preserving human-authored keys) instead of creating
    a duplicate entry.

    When an asset with the same key already exists, the merge_strategy decides
    its human fields (Issue #446, see :class:`MergeStrategy`). The machine
    fields always come from ``asset``.

    Issue #933: when the key holds another data file, the two files are never
    merged. A same-stem file that ranks below the incoming file moves to the key
    a fresh add gives it (see ``preparation._resolve_assets``). Another data
    file of a different media type raises ``AssetKeyCollisionError`` under every
    strategy, because every strategy loses one of the two files. The rule
    covers assets with the ``data`` or ``visual`` role. Assets under a role key,
    such as ``thumbnail``, merge as before.

    Args:
        collection: The collection to add the asset to.
        asset_key: Key for the asset (e.g., "data", "boundaries").
        asset: The pystac.Asset to add.
        merge_strategy: How to handle conflicts with existing assets.

    Raises:
        AssetKeyCollisionError: The key holds a different file of another media
            type.
    """
    # Issue #447: Check if any existing asset already points to the same href
    # This prevents duplicate entries when human-authored key differs from auto-key
    existing_key_by_href = None
    for key, existing in collection.assets.items():
        if _same_href(existing.href, asset.href):
            existing_key_by_href = key
            break

    # Use the existing key if found (preserves human-authored keys like "census_2020")
    # Otherwise use the provided key (e.g., stem-based "data")
    final_key = existing_key_by_href if existing_key_by_href is not None else asset_key
    if (
        existing_key_by_href is not None
        and asset_key not in collection.assets
        and _gives_up_stem(existing_key_by_href, asset_key, asset)
    ):
        # The file held its stem key, and the add now keys it by its filename,
        # because a same-stem file of a higher rank takes the stem. A companion
        # CSV that gains geometry is the case: its GeoParquet takes the stem.
        # The stem key was the add's own, not a human's, so the file moves.
        collection.add_asset(asset_key, collection.assets.pop(existing_key_by_href))
        final_key = asset_key
    existing_asset = collection.assets.get(final_key)

    if (
        existing_asset is not None
        and not _same_href(existing_asset.href, asset.href)
        and _both_data(existing_asset, asset)
    ):
        if _is_displaced_by(collection, existing_asset, asset):
            asset = _move_displaced_aside(collection, final_key, existing_asset, asset)
            existing_asset = None
        elif _media_types_differ(existing_asset.media_type, asset.media_type):
            raise AssetKeyCollisionError(collection.id, final_key, existing_asset.href, asset.href)

    if existing_asset is not None:
        asset = _merge_asset(existing_asset, asset, merge_strategy)

    collection.add_asset(final_key, asset)


_PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"

# Media types an older catalog or a hand edit wrote for GeoParquet.
_LEGACY_MEDIA_TYPES: dict[str, str] = {
    "application/x-parquet": _PARQUET_MEDIA_TYPE,
    "application/geo+parquet": _PARQUET_MEDIA_TYPE,
    "application/x-geoparquet": _PARQUET_MEDIA_TYPE,
}

# Rank of a file when same-stem files compete for the stem key (Issues #933,
# #960). The lowest rank owns the stem. ``preparation`` ranks the scanned files
# and ``add_asset_to_collection`` the stored assets, both with :func:`stem_rank`.
STEM_RANK_GEOPARQUET = 0
STEM_RANK_CLOUD_NATIVE = 1
STEM_RANK_PMTILES = 2
STEM_RANK_TABLE = 3
STEM_RANK_SOURCE = 4

# Extensions whose files can be the catalog's own cloud-native data.
_CLOUD_NATIVE_EXTENSIONS: tuple[str, ...] = tuple(
    spec.ext for spec in EXTENSION_REGISTRY if spec.cloud_native != "no"
)


def stem_rank(media_type: str | None, name: str, is_plain_table: Callable[[], bool]) -> int:
    """Rank one data file for the stem key.

    GeoParquet, then COG, FlatGeobuf and other cloud-native data, then PMTiles,
    then a Parquet table without GeoParquet metadata, then any other file, such
    as a CSV or GeoJSON source.

    Args:
        media_type: The media type of the file. A legacy Parquet type counts.
        name: The file name or href.
        is_plain_table: Reads the Parquet footer. Called for Parquet only.
    """
    canonical = _canonical_media_type(media_type or "")
    if canonical == PMTILES_MEDIA_TYPE:
        return STEM_RANK_PMTILES
    if canonical == _PARQUET_MEDIA_TYPE:
        return STEM_RANK_TABLE if is_plain_table() else STEM_RANK_GEOPARQUET
    if name.lower().endswith(_CLOUD_NATIVE_EXTENSIONS):
        return STEM_RANK_CLOUD_NATIVE
    return STEM_RANK_SOURCE


def _canonical_media_type(media_type: str) -> str:
    return _LEGACY_MEDIA_TYPES.get(media_type, media_type)


def _same_href(first: str, second: str) -> bool:
    """Report whether two collection-relative hrefs name one file."""
    return first.removeprefix("./") == second.removeprefix("./")


def _gives_up_stem(stored_key: str, asset_key: str, asset: pystac.Asset) -> bool:
    """Report whether a file stored under its stem key now takes its filename key.

    ``preparation._resolve_assets`` keys a file by its filename only when a
    same-stem file of a higher rank holds the stem. A human key is neither of
    the two, so it stays.
    """
    path = PurePosixPath(asset.href)
    return stored_key == path.stem and asset_key == path.name and stored_key != asset_key


# Roles of the assets that #933 could replace: the data and its tile derivative.
_GUARDED_ROLES = frozenset({"data", ROLE_VISUAL})


def _both_data(existing: pystac.Asset, new: pystac.Asset) -> bool:
    """Report whether both assets are data files or their tile derivatives."""
    return bool(_GUARDED_ROLES & set(existing.roles or [])) and bool(
        _GUARDED_ROLES & set(new.roles or [])
    )


def _media_types_differ(existing: str | None, new: str | None) -> bool:
    """Report whether two media types name different formats.

    An absent type matches every type. A legacy type matches its current type.
    """
    if existing is None or new is None:
        return False
    return _canonical_media_type(existing) != _canonical_media_type(new)


def _stem_rank(collection: pystac.Collection, asset: pystac.Asset) -> int:
    """Rank a stored asset with :func:`stem_rank`.

    The tabular add path keys a plain table by its stem, so the footer decides.
    Only a same-stem collision reaches this read.
    """
    return stem_rank(asset.media_type, asset.href, lambda: _is_plain_table(collection, asset.href))


def _is_plain_table(collection: pystac.Collection, href: str) -> bool:
    """Report whether a local Parquet asset has no GeoParquet metadata.

    An asset with no local file, or a collection with no file location, counts
    as GeoParquet, as its media type says.
    """
    self_href = collection.get_self_href()
    if self_href is None or "://" in self_href:
        return False
    path = Path(self_href).parent / href.removeprefix("./")
    if not path.is_file():
        return False

    from portolan_cli.formats import is_geoparquet

    return not is_geoparquet(path)


def _is_displaced_by(
    collection: pystac.Collection, existing: pystac.Asset, new: pystac.Asset
) -> bool:
    """Report whether ``existing`` is a same-stem file that ranks below ``new``.

    portolan-cli 0.8.0 renamed the primary ``data`` key to the stem after the
    scan had keyed a same-stem file by that stem. ``iterdir`` order then left
    the PMTiles, the converted source, or a sidecar such as ``.dbf`` under the
    GeoParquet key. A re-add of the GeoParquet finds that file under its own
    key. An add of two same-stem files in two runs meets the same layout.
    """
    if existing.media_type is None or new.media_type is None:
        return False
    return PurePosixPath(existing.href).stem == PurePosixPath(new.href).stem and _stem_rank(
        collection, existing
    ) > _stem_rank(collection, new)


def _move_displaced_aside(
    collection: pystac.Collection,
    key: str,
    displaced: pystac.Asset,
    data: pystac.Asset,
) -> pystac.Asset:
    """Move a displaced same-stem asset from ``key`` to the key it gets today.

    The key follows ``preparation._resolve_assets``. A PMTiles beside a
    GeoParquet, COG or FlatGeobuf moves to its ``-tiles`` key with the
    ``visual`` role. Any other
    file moves to its filename. When that key already holds the same file, the copy under ``key``
    is dropped. The human title and description under ``key`` described the
    data, so they go back to the data asset.

    Args:
        collection: The collection that holds the assets.
        key: The GeoParquet key that the displaced asset occupies.
        displaced: The asset under ``key``.
        data: The incoming GeoParquet asset.

    Returns:
        The GeoParquet asset to store under ``key``.

    Raises:
        AssetKeyCollisionError: The target key holds a different file.
    """
    roles: list[str] | None
    if displaced.media_type == PMTILES_MEDIA_TYPE and _stem_rank(collection, data) <= 1:
        target_key = pmtiles_asset_key(key)
        roles = [ROLE_VISUAL]
    else:
        target_key = PurePosixPath(displaced.href).name
        roles = displaced.roles
    holder = collection.assets.get(target_key)
    if holder is not None and not _same_href(holder.href, displaced.href):
        raise AssetKeyCollisionError(collection.id, target_key, holder.href, displaced.href)

    del collection.assets[key]
    if holder is None:
        collection.add_asset(
            target_key,
            pystac.Asset(
                href=displaced.href,
                media_type=displaced.media_type,
                roles=roles,
                extra_fields=dict(displaced.extra_fields),
            ),
        )

    restored = data.clone()
    restored.title = data.title or displaced.title
    restored.description = data.description or displaced.description
    return restored


def add_collection_properties_from_metadata(
    collection: pystac.Collection,
    metadata: object,
    asset_keys: Iterable[str] | None = None,
) -> None:
    """Add STAC properties from metadata to a collection.

    Used for collection-level assets where metadata properties
    should be applied directly to the collection instead of an item.

    Handles:
    - PMTilesMetadata: pmtiles:* properties (no projection contribution; the
      hardcoded Web-Mercator tile CRS describes the tiles, not the source data)
    - FlatGeobufMetadata: proj:code onto the data asset, flatgeobuf:* properties
    - GeoParquetMetadata: proj:code onto the data asset (table extension
      handled separately)

    Projection v2.0.0 removed ``proj:epsg``; the reference catalog in
    portolan-spec carries ``proj:code`` (``"EPSG:4269"``) on the collection's
    *data asset*, not on the collection top level (issue #654). A stale
    top-level ``proj:epsg`` from an older catalog is stripped on re-add.

    Args:
        collection: The collection to add properties to.
        metadata: Metadata object with to_stac_properties() method.
        asset_keys: Keys of the collection assets this metadata describes.
            ``proj:code`` lands on those of them carrying the ``data`` role.
            When omitted, every collection asset with the ``data`` role
            receives it.
    """
    if not hasattr(metadata, "to_stac_properties"):
        return

    props = metadata.to_stac_properties()
    if not props:
        return

    # proj:epsg is the extractors' internal CRS handoff; it never lands in
    # published STAC. Translate to proj:code on the data asset below.
    epsg = props.pop("proj:epsg", None)

    for key, value in props.items():
        collection.extra_fields[key] = value

    # Migration: older catalogs carried proj:epsg on the collection top level.
    collection.extra_fields.pop("proj:epsg", None)

    if epsg is None:
        return

    if asset_keys is None:
        targets = list(collection.assets.keys())
    else:
        targets = [key for key in asset_keys if key in collection.assets]

    wrote_proj_code = False
    for key in targets:
        asset = collection.assets[key]
        if "data" in (asset.roles or []):
            asset.extra_fields["proj:code"] = f"EPSG:{epsg}"
            wrote_proj_code = True

    if wrote_proj_code:
        proj_ext_url = EXTENSION_URLS["projection"]
        if proj_ext_url not in collection.stac_extensions:
            collection.stac_extensions.append(proj_ext_url)


def apply_human_titles(collection: pystac.Collection, metadata: object) -> None:
    """Apply human-authored title/description from metadata.yaml (Issue #502).

    ``metadata.yaml`` may carry optional ``title`` and
    ``description`` keys as the human override for the auto-derived values.
    These are the highest-precedence source: a human-authored title always wins
    over the slug-humanized default. Missing/blank values leave the existing
    collection title/description untouched.

    Args:
        collection: The collection to update in place.
        metadata: The merged metadata.yaml mapping (other types are ignored).
    """
    if not isinstance(metadata, dict):
        return

    title = metadata.get("title")
    if isinstance(title, str) and title.strip():
        collection.title = title.strip()

    description = metadata.get("description")
    if isinstance(description, str) and description.strip():
        collection.description = description.strip()


def apply_human_license(collection: pystac.Collection, metadata: object) -> None:
    """Apply the human-authored license from metadata.yaml (issue #654).

    ``license`` is a required metadata.yaml field; without this the
    collection kept the ``other`` placeholder even when the human had declared an
    SPDX identifier. ``license_url``, when present, becomes the ``rel="license"``
    link that a non-SPDX license needs to be resolvable.

    Args:
        collection: The collection to update in place.
        metadata: The merged metadata.yaml mapping (other types are ignored).
    """
    if not isinstance(metadata, dict):
        return

    license_id = metadata.get("license")
    if isinstance(license_id, str) and license_id.strip():
        collection.license = license_id.strip()

    license_url = metadata.get("license_url")
    if not isinstance(license_url, str) or not license_url.strip():
        return
    href = license_url.strip()
    for link in collection.links:
        if link.rel == "license" and link.href == href:
            return
    collection.add_link(pystac.Link(rel="license", target=href, title="License"))


def apply_human_providers(collection: pystac.Collection, metadata: object) -> None:
    """Apply the human-authored providers array from metadata.yaml (issue #684).

    Every collection must name a producer and exactly one host, listed last
    (PTL-PRV-001, PTL-PRV-002). ``providers.resolve_providers`` does the ordering
    and seeds the host from ``contact``; this writes the result onto the
    collection. Metadata that declares nothing leaves any existing array alone,
    so a re-add never wipes providers a human wrote straight into
    ``collection.json``.

    Args:
        collection: The collection to update in place.
        metadata: The merged metadata.yaml mapping (other types ignored).

    Raises:
        InvalidProvidersError: When metadata.yaml declares a providers array
            Portolan cannot put in conformant shape.
    """
    resolved = resolve_providers(metadata)
    if not resolved:
        return

    collection.providers = [
        pystac.Provider(
            name=str(provider["name"]),
            description=provider.get("description"),
            roles=provider.get("roles"),
            url=provider.get("url"),
            extra_fields={"email": provider["email"]} if "email" in provider else None,
        )
        for provider in resolved
    ]


def _collection_provenance(collection: pystac.Collection) -> str | None:
    """Derive official vs mirror from the providers already on the collection."""
    return derive_provenance([provider.to_dict() for provider in collection.providers or []])


def apply_provenance(
    collection: pystac.Collection,
    metadata: object,
    *,
    synced_at: datetime | None = None,
) -> None:
    """Record how this collection relates to the data's original source (issue #684).

    Source provenance is derived from the providers, never declared separately: a
    collection is official when its producer also hosts it, and a mirror when
    they differ. A mirror links back to the source with ``rel="via"``
    (PTL-PRO-001) and records the sync in a top-level RFC 3339 ``updated`` field
    (PTL-PRO-003). An official collection is the source, so it gets neither
    (PTL-PRO-004).

    The sync a mirror records is the sync *from its source*, which for Portolan
    is the moment ``add`` copied or converted the data, not a later ``push`` to
    object storage.

    Args:
        collection: The collection to update in place.
        metadata: The merged metadata.yaml mapping (other types ignored).
        synced_at: The sync time to record. Defaults to now, in UTC.
    """
    provenance = _collection_provenance(collection)
    if provenance is None:
        return

    source_url = metadata.get("source_url") if isinstance(metadata, dict) else None
    source_url = source_url.strip() if isinstance(source_url, str) and source_url.strip() else None

    if provenance == "official":
        if source_url is not None:
            from portolan_cli.output import warn as warn_output

            warn_output(
                f"{collection.id}: producer and host are the same organization, so the "
                f"collection is official and carries no via link to {source_url}"
            )
        return

    collection.extra_fields["updated"] = to_rfc3339(synced_at or datetime.now(UTC))

    if source_url is None:
        import logging

        logging.getLogger(__name__).debug(
            "%s mirrors data it did not produce but metadata.yaml declares no source_url",
            collection.id,
        )
        return
    for link in collection.links:
        if link.rel == "via" and link.href == source_url:
            return
    collection.add_link(
        pystac.Link(
            rel="via",
            target=source_url,
            media_type="text/html",
            title="Original source",
        )
    )


def to_rfc3339(moment: datetime) -> str:
    """Format a datetime as the RFC 3339 date-time STAC's ``updated`` field wants."""
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return moment.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def add_partition_metadata_to_collection(
    collection: pystac.Collection,
    partition_metadata: dict[str, object],
) -> None:
    """Add partition extension fields to a collection.

    Adds partition:* fields from the provided metadata dict and registers
    the partition extension URL in stac_extensions.

    Per Issue #443: Preserves existing hand-authored descriptions on partition:keys
    when merging new partition metadata.

    Args:
        collection: The collection to add partition metadata to.
        partition_metadata: Dict with partition:* fields from get_partition_metadata().
    """
    # Get existing partition keys descriptions to preserve
    existing_descriptions: dict[str, str] = {}
    existing_keys = collection.extra_fields.get("partition:keys", [])
    if isinstance(existing_keys, list):
        for key in existing_keys:
            if isinstance(key, dict) and "name" in key and "description" in key:
                existing_descriptions[key["name"]] = key["description"]

    # Add partition:* fields to collection extra_fields
    for key, value in partition_metadata.items():
        if key.startswith("partition:"):
            # Special handling for partition:keys to preserve descriptions
            if key == "partition:keys" and isinstance(value, list):
                merged_keys: list[dict[str, str]] = []
                for new_key in value:
                    if isinstance(new_key, dict) and "name" in new_key:
                        key_name = new_key["name"]
                        merged_key = dict(new_key)
                        # Preserve existing hand-authored description if:
                        # 1. New key has no description at all
                        # 2. New key has empty/whitespace description
                        # 3. New key has auto-generated generic description (ends with "identifier")
                        new_desc = merged_key.get("description", "").strip()
                        is_generic_desc = new_desc.endswith("identifier") or new_desc == ""
                        if key_name in existing_descriptions and (
                            "description" not in merged_key or is_generic_desc
                        ):
                            merged_key["description"] = existing_descriptions[key_name]
                        merged_keys.append(merged_key)
                collection.extra_fields[key] = merged_keys
            else:
                collection.extra_fields[key] = value

    # Register partition extension
    ext_url = EXTENSION_URLS["partition"]
    if ext_url not in collection.stac_extensions:
        collection.stac_extensions.append(ext_url)


# STAC Extension schema URLs (v1.1.0 compatible)
# "vector" is the only entry still unused; the rest are declared by the writers
# named beside them.
EXTENSION_URLS = {
    "table": "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "projection": "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
    "raster": "https://stac-extensions.github.io/raster/v2.0.0/schema.json",
    # file:size / file:checksum, declared by declare_file_extension
    # and by stac_parquet.sync_file_extension.
    "file": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    "vector": "https://stac-extensions.github.io/vector/v0.1.0/schema.json",  # Proposal maturity
    "partition": PARTITION_EXTENSION_URI,
}


def ensure_portolan_schema_uri(document: dict[str, Any]) -> bool:
    """Declare the versioned Portolan profile schema URI on a catalog or collection.

    The profile URI is the machine-readable conformance claim: every catalog and
    collection MUST carry exactly one (issue #654). This appends
    :data:`~portolan_cli.constants.PORTOLAN_SCHEMA_URI` when absent, and rewrites
    a URI left over from an older spec version in place, so re-stamping a catalog
    upgrades it instead of accumulating claims. Other extension declarations keep
    their relative order.

    The URI never moves down a version. A catalog that already declares a newer
    spec release than the one the installed validator bundles keeps its version,
    so ``add`` does not stamp an older schema over a newer one (issue #755).

    Args:
        document: Parsed ``catalog.json`` or ``collection.json``, mutated in place.

    Returns:
        True when ``stac_extensions`` changed, False when it already conformed.
    """
    existing = document.get("stac_extensions")
    # JSON may hold anything, so each entry is checked before it is read.
    declared: list[Any] = list(existing) if isinstance(existing, list) else []

    # Keep the highest version between what is declared and what this CLI stamps,
    # so a re-add upgrades an older claim but never downgrades a newer one.
    canonical = PORTOLAN_SCHEMA_URI
    canonical_version = _profile_version(PORTOLAN_SCHEMA_URI)
    for uri in declared:
        if not isinstance(uri, str):
            continue
        version = _profile_version(uri)
        if version is not None and canonical_version is not None and version > canonical_version:
            canonical = uri
            canonical_version = version

    kept: list[str] = []
    stamped = False
    for uri in declared:
        if isinstance(uri, str) and PORTOLAN_SCHEMA_URI_PATTERN.match(uri):
            # Collapse every profile claim (stale or duplicate) onto the first.
            if not stamped:
                kept.append(canonical)
                stamped = True
            continue
        kept.append(uri)
    if not stamped:
        kept.append(canonical)

    if kept == declared and isinstance(existing, list):
        return False
    document["stac_extensions"] = kept
    return True


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

    # Check for raster extension fields.
    # The unified `bands` array (with its `statistics`) is core STAC v1.1.0, so
    # it does not imply the raster extension — only genuinely `raster:`-prefixed
    # fields such as raster:spatial_resolution do (issue #654).
    if any(k.startswith("raster:") for k in properties):
        extensions.append(EXTENSION_URLS["raster"])

    # Check for file extension fields
    if any(k.startswith("file:") for k in properties):
        extensions.append(EXTENSION_URLS["file"])

    # Check for vector extension fields
    if any(k.startswith("vector:") for k in properties):
        extensions.append(EXTENSION_URLS["vector"])

    # Check for partition extension fields
    if any(k.startswith("partition:") for k in properties):
        extensions.append(EXTENSION_URLS["partition"])

    return extensions


def _merge_table_columns(
    existing_columns: list[dict[str, object]],
    new_schema: dict[str, str],
    strategy: MergeStrategy,
) -> list[dict[str, object]]:
    """Build table:columns from the schema, keeping the human fields of each column.

    The columns, their order, and their types come from ``new_schema`` under
    every strategy, so a column the data no longer has is gone (issue #931).
    The strategy decides only the fields a person wrote on a surviving column,
    such as its description: SMART and KEEP keep them, OVERWRITE drops them.

    Args:
        existing_columns: Existing table:columns array from collection.
        new_schema: New schema dict mapping column name to type.
        strategy: The merge strategy to apply.

    Returns:
        Merged columns array.
    """
    if strategy == MergeStrategy.OVERWRITE:
        return [{"name": name, "type": dtype} for name, dtype in new_schema.items()]

    existing_by_name = {col["name"]: col for col in existing_columns if "name" in col}

    merged_columns = []
    for name, dtype in new_schema.items():
        if name in existing_by_name:
            # Column exists: preserve description, update type
            existing = existing_by_name[name]
            merged_col: dict[str, object] = {"name": name, "type": dtype}
            if "description" in existing:
                merged_col["description"] = existing["description"]
            # Preserve any other human-authored fields
            for key, value in existing.items():
                if key not in ("name", "type", "statistics"):
                    merged_col.setdefault(key, value)
            merged_columns.append(merged_col)
        else:
            # New column: just name and type
            merged_columns.append({"name": name, "type": dtype})

    return merged_columns


def add_table_extension(
    collection: pystac.Collection,
    metadata: object,
    *,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add Table extension fields to a collection from GeoParquet metadata.

    Sets table:row_count, table:primary_geometry, and table:columns based on
    the provided GeoParquet metadata object.

    The fields come from ``metadata`` under every strategy. The merge strategy
    decides only the column descriptions (Issue #446, #931).

    Args:
        collection: The collection to add extension fields to.
        metadata: A GeoParquetMetadata-like object with feature_count,
                 geometry_column, and schema attributes.
        merge_strategy: How to handle conflicts with existing metadata.
    """
    # Set row count (machine-derivable: always update in SMART/OVERWRITE)
    if hasattr(metadata, "feature_count") and metadata.feature_count is not None:
        collection.extra_fields["table:row_count"] = metadata.feature_count

    # Set primary geometry column (machine-derivable: always update)
    if hasattr(metadata, "geometry_column") and metadata.geometry_column is not None:
        collection.extra_fields["table:primary_geometry"] = metadata.geometry_column

    # Set columns from schema with merge logic
    if hasattr(metadata, "schema") and metadata.schema:
        existing_columns = collection.extra_fields.get("table:columns", [])
        collection.extra_fields["table:columns"] = _merge_table_columns(
            existing_columns if isinstance(existing_columns, list) else [],
            metadata.schema,
            merge_strategy,
        )

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["table"]
    if ext_url not in collection.stac_extensions:
        collection.stac_extensions.append(ext_url)


def document_tabular_table(
    collection_data: dict[str, Any],
    asset_key: str,
    metadata: TabularMetadata,
    merge_strategy: MergeStrategy,
) -> None:
    """Document a plain-Parquet asset's columns with the table extension (issue #749).

    The tabular writer builds ``collection.json`` as raw JSON outside
    ``finalize_items``, so it cannot reach :func:`add_table_extension`, which
    takes a pystac Collection. This is the dict-level counterpart, and it is the
    only writer of ``table:*`` on that path.

    Where the columns land depends on what else the collection holds. When this
    is its only Parquet data asset, the collection *is* the table — the
    single-file collection pattern the spec's Tabular Data section describes —
    so the fields go on the collection, where ``readme`` and ``metadata.yaml``
    already read them. When another Parquet data asset is present, that one owns
    the collection-level schema, so these columns go on the asset instead of
    overwriting it. The table extension permits both placements and rashid
    (PTL-DAT-015) accepts either.

    The columns come from ``metadata``. ``merge_strategy`` decides only whether
    a human description on a surviving column stays (issue #931).

    Args:
        collection_data: Parsed ``collection.json``, mutated in place.
        asset_key: Key of the Parquet asset under ``assets``.
        metadata: Schema and row count read from that asset.
        merge_strategy: The strategy of the ``add`` run.
    """
    assets = collection_data.get("assets", {})
    others_hold_the_collection = any(
        key != asset_key
        and "data" in (asset.get("roles") or [])
        and str(asset.get("href", "")).lower().endswith(".parquet")
        for key, asset in assets.items()
    )
    target: dict[str, Any] = assets[asset_key] if others_hold_the_collection else collection_data

    existing = target.get("table:columns")
    target["table:columns"] = _merge_table_columns(
        existing if isinstance(existing, list) else [],
        metadata.schema,
        merge_strategy,
    )
    target["table:row_count"] = metadata.row_count

    declared = collection_data.setdefault("stac_extensions", [])
    if EXTENSION_URLS["table"] not in declared:
        declared.append(EXTENSION_URLS["table"])


def set_temporal_extent(
    collection_data: dict[str, Any],
    interval: tuple[datetime, datetime],
) -> None:
    """Populate a collection's temporal extent from a derived interval (issue #749).

    Only fills an extent that is still open at both ends, the sentinel
    ``[[null, null]]`` a tabular collection is created with. A bound already
    present came from a human or from the collection's items, and machine-read
    column statistics do not outrank either.

    Args:
        collection_data: Parsed ``collection.json``, mutated in place.
        interval: Start and end datetimes, each serialized to RFC 3339.
    """
    extent = collection_data.setdefault("extent", {})
    temporal = extent.setdefault("temporal", {})
    bounds = temporal.get("interval")
    if isinstance(bounds, list) and any(
        isinstance(pair, list) and any(bound is not None for bound in pair) for pair in bounds
    ):
        return
    temporal["interval"] = [[_rfc3339(interval[0]), _rfc3339(interval[1])]]


def _rfc3339(moment: datetime) -> str:
    """Serialize a UTC datetime the way STAC spells it, with a trailing ``Z``."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


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
    """Compute bounding box union from multiple metadata objects.

    These bboxes are in the source CRS (reprojection to WGS84 happens later in
    the add flow), and geoparquet-io does not preserve the CRS in the converted
    file's metadata, so WGS84 range validation cannot be applied here. Bboxes are
    filtered for finiteness and a sane coordinate magnitude only, which still
    rejects inf/nan and "effectively infinite" sentinel poison values such as
    ±1.79e308 while accepting legitimate projected coordinates (issue #516).
    """
    from portolan_cli.bbox import compute_bbox_union

    all_bboxes: list[list[float]] = []
    for m in metadata_list:
        bbox = getattr(m, "bbox", None)
        if bbox is not None:
            all_bboxes.append(list(bbox))

    if not all_bboxes:
        raise ValueError("Cannot aggregate metadata: no items have valid bboxes")

    result = compute_bbox_union(all_bboxes, wgs84_only=False)
    if result.bbox is None:
        raise ValueError("Cannot aggregate metadata: all bboxes are invalid (inf/nan/out of range)")

    return (result.bbox[0], result.bbox[1], result.bbox[2], result.bbox[3])


def _canonicalize_crs(crs: object) -> str | None:
    """Convert CRS to a canonical hashable string for comparison.

    GeoParquetMetadata.crs can be a dict (PROJJSON), which isn't hashable.
    This converts any CRS to a stable string form.

    Args:
        crs: CRS value (string, dict/PROJJSON, or None)

    Returns:
        Canonical string representation, or None if crs is None.
    """
    import json

    if crs is None:
        return None
    if isinstance(crs, dict):
        # PROJJSON - convert to stable JSON string
        return json.dumps(crs, sort_keys=True)
    return str(crs)


def _warn_on_mismatches(metadata_list: Sequence[object]) -> None:
    """Warn if CRS or geometry types differ across items."""
    import warnings

    # Canonicalize CRS values to handle dict (PROJJSON) CRS
    crs_values = {_canonicalize_crs(getattr(m, "crs", None)) for m in metadata_list} - {None}
    if len(crs_values) > 1:
        # For display, show original CRS values (truncate PROJJSON to avoid huge messages)
        display_values = set()
        for m in metadata_list:
            crs = getattr(m, "crs", None)
            if crs is not None:
                if isinstance(crs, dict):
                    display_values.add("<PROJJSON>")
                else:
                    display_values.add(str(crs))
        warnings.warn(
            f"CRS mismatch detected across items: {display_values}. Using first item's CRS.",
            UserWarning,
            stacklevel=3,
        )

    # GeoParquet metadata lists every declared type (#1010). A set needs tuples.
    geometry_types = {
        tuple(declared) if isinstance(declared, list) else declared
        for declared in (getattr(m, "geometry_type", None) for m in metadata_list)
    } - {None}
    if len(geometry_types) > 1:
        shown = sorted(list(t) if isinstance(t, tuple) else [t] for t in geometry_types)
        warnings.warn(
            f"Mixed geometry types detected: {shown}. The collection lists every type.",
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
        geometry_type=list(
            dict.fromkeys(
                name
                for m in metadata_list
                for name in geometry_type_list(getattr(m, "geometry_type", None))
            )
        )
        or None,
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
    if ext_url not in item.stac_extensions:
        item.stac_extensions.append(ext_url)


def geometry_type_list(value: object) -> list[str]:
    """Every geometry type a metadata value names, deduplicated in order.

    GeoParquet declares a list. An older reader returned one string.
    """
    names = [value] if isinstance(value, str) else value if isinstance(value, list) else []
    return list(dict.fromkeys(str(name) for name in names if name))


def add_vector_extension(
    item: pystac.Item,
    metadata: object,
) -> None:
    """Add Vector extension fields to an item from GeoParquet metadata.

    Sets vector:geometry_types based on the geometry type(s) in the metadata.
    Use experimental extensions (Vector v0.1.0 is Proposal maturity).

    Args:
        item: The STAC item to add extension fields to.
        metadata: A metadata object with geometry_type attribute (str or list).
    """
    geometry_types = geometry_type_list(getattr(metadata, "geometry_type", None))
    if not geometry_types:
        return

    # Every type the file declares, in order (issue #1010).
    item.properties["vector:geometry_types"] = geometry_types

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["vector"]
    if ext_url not in item.stac_extensions:
        item.stac_extensions.append(ext_url)


def _get_stac_properties(metadata: object) -> dict[str, object]:
    """Extract STAC properties from metadata if to_stac_properties() is available."""
    if hasattr(metadata, "to_stac_properties") and callable(metadata.to_stac_properties):
        result = metadata.to_stac_properties()
        if isinstance(result, dict):
            return result
    return {}


def _compute_spatial_resolution(metadata: object) -> float | None:
    """Extract spatial resolution from metadata, returning None if unavailable."""
    resolution = getattr(metadata, "resolution", None)
    if not isinstance(resolution, (list, tuple)) or len(resolution) < 2:
        return None
    x_res, y_res = resolution[0], resolution[1]
    if isinstance(x_res, (int, float)) and isinstance(y_res, (int, float)):
        return (abs(x_res) + abs(y_res)) / 2
    return None


def _build_bands_from_metadata(metadata: object) -> list[dict[str, object]] | None:
    """Build bands array from metadata, returning None if not possible."""
    band_count = getattr(metadata, "band_count", None)
    if not isinstance(band_count, int) or band_count <= 0:
        return None

    # Get dtype and nodata, validating they're real values (not MagicMock)
    dtype = getattr(metadata, "dtype", None)
    if not isinstance(dtype, str):
        dtype = "unknown"
    nodata = getattr(metadata, "nodata", None)
    if not isinstance(nodata, (int, float, type(None))):
        nodata = None

    bands: list[dict[str, object]] = []
    for i in range(band_count):
        band: dict[str, object] = {"name": f"band_{i + 1}", "data_type": dtype}
        if nodata is not None:
            band["nodata"] = nodata
        bands.append(band)
    return bands


def _set_bands_on_data_assets(
    item: pystac.Item,
    bands: list[dict[str, object]],
) -> None:
    """Attach the STAC v1.1.0 unified ``bands`` array to the item's data asset.

    Per STAC v1.1.0, ``bands`` is an asset-level field. Targets the conventional
    primary-data asset (key ``"data"``), falling back to the first asset whose
    roles include ``"data"``. No-op if the item has no data asset.

    Args:
        item: The STAC item whose data asset should carry the bands array.
        bands: The unified bands array (with data_type/nodata/statistics).
    """
    data_asset = item.assets.get("data")
    if data_asset is None:
        data_asset = next(
            (asset for asset in item.assets.values() if "data" in (asset.roles or [])),
            None,
        )
    if data_asset is not None:
        data_asset.extra_fields["bands"] = bands


def add_raster_extension(
    item: pystac.Item,
    metadata: object,
) -> None:
    """Add Raster extension fields to an item from COG metadata.

    Sets raster:spatial_resolution on the item and attaches the unified ``bands``
    array to the item's data asset.

    Per STAC v1.1.0, ``bands`` is an asset-level field — the core item schema
    forbids it on ``item.properties``. Any item-level bands (carrying statistics
    and nodata defaults applied earlier in the pipeline) are relocated onto the
    data asset; otherwise the array is built from metadata.

    Args:
        item: The STAC item to add extension fields to.
        metadata: A metadata object with resolution and band information.
    """
    stac_props = _get_stac_properties(metadata)

    # Set raster:spatial_resolution
    spatial_res = _compute_spatial_resolution(metadata)
    if spatial_res is not None:
        item.properties["raster:spatial_resolution"] = spatial_res
    elif "raster:spatial_resolution" in stac_props:
        item.properties["raster:spatial_resolution"] = stac_props["raster:spatial_resolution"]

    # STAC v1.1.0 unifies bands as an ASSET-level field; the core item schema
    # forbids `bands` on item.properties. Relocate any item-level bands (which
    # carry statistics/nodata applied earlier in the pipeline) onto the data
    # asset, falling back to building the array from metadata.
    bands: list[dict[str, object]] | None
    item_bands = item.properties.pop("bands", None)
    if isinstance(item_bands, list) and item_bands:
        bands = item_bands
    else:
        stac_bands = stac_props.get("bands")
        if isinstance(stac_bands, list) and stac_bands:
            bands = stac_bands
        else:
            bands = _build_bands_from_metadata(metadata)
    if bands:
        _set_bands_on_data_assets(item, bands)

    # Declare the extension only when a raster:-prefixed field was actually
    # written (issue #654). Raster v2.0.0 requires a declared item to carry at
    # least one raster: field in properties, bands, or an asset; the unified
    # `bands` array holds only core fields, so an empty declaration fails
    # v2.0.0 validation. The spec registry condition is "When band-level
    # detail is provided".
    has_raster_field = any(key.startswith("raster:") for key in item.properties)
    if has_raster_field:
        ext_url = EXTENSION_URLS["raster"]
        if ext_url not in item.stac_extensions:
            item.stac_extensions.append(ext_url)


# Fields that describe the grid of one item. A collection summary would merge
# unrelated grids into one meaningless value, so they are not summarized (#1023).
_PER_ITEM_GRID_FIELDS = frozenset(
    {"proj:bbox", "proj:centroid", "proj:geometry", "proj:shape", "proj:transform"}
)

# Numeric fields summarized as a range rather than a set of values.
_RANGE_FIELDS = frozenset({"gsd"})


def _is_summarized(key: str) -> bool:
    """Whether derivation owns the summary of ``key``.

    Derivation owns every extension-prefixed key, plus ``gsd``. It writes no
    summary for a per-item grid field, so a stored one is dropped. A summary
    under any other key came from elsewhere and is kept.
    """
    return key in _RANGE_FIELDS or ":" in key


def _summary_values(value: object) -> list[object]:
    """The values one item contributes to a set-of-values summary.

    A list of strings is a set already, such as ``vector:geometry_types``, so
    each string counts. Any other list is one value: a list of numbers keeps
    its shape, and repeated numbers in it are not merged away (issue #1023).
    """
    if isinstance(value, list) and all(isinstance(entry, str) for entry in value):
        return list(value)
    return [value]


def summarize_properties(properties: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize item properties the way STAC 1.1.0 describes a collection.

    Args:
        properties: The properties of each item.

    Returns:
        A set of distinct values per field, in first-seen order, or a
        ``minimum``/``maximum`` range for :data:`_RANGE_FIELDS`.
    """
    import json

    distinct: dict[str, dict[str, object]] = {}
    ranges: dict[str, list[float]] = {}
    for props in properties:
        for key, value in props.items():
            if value is None or not _is_summarized(key) or key in _PER_ITEM_GRID_FIELDS:
                continue
            if key in _RANGE_FIELDS:
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    ranges.setdefault(key, []).append(value)
                continue
            seen = distinct.setdefault(key, {})
            for entry in _summary_values(value):
                seen.setdefault(json.dumps(entry, sort_keys=True, default=str), entry)
    summaries: dict[str, Any] = {key: list(seen.values()) for key, seen in distinct.items()}
    for key, values in ranges.items():
        summaries[key] = {"minimum": min(values), "maximum": max(values)}
    return summaries


@dataclass(frozen=True)
class CollectionMember:
    """One member of a collection: an item, or a collection-level data asset.

    Attributes:
        bbox: WGS84 bbox of the member, or None when it has no footprint.
        start: Earliest observation time, or None when unknown.
        end: Latest observation time, or None when unknown.
        properties: Item properties for the summaries. Empty for an asset.
    """

    bbox: list[float] | None = None
    start: datetime | None = None
    end: datetime | None = None
    properties: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CollectionFields:
    """The collection fields Portolan computes from the members.

    Attributes:
        spatial: The spatial extent, or None when no member has a footprint and
            metadata.yaml declares none.
        temporal: The temporal extent, or None when no member has a time.
        summaries: The summaries of the item properties.
    """

    spatial: pystac.SpatialExtent | None
    temporal: pystac.TemporalExtent | None
    summaries: dict[str, Any]


def build_spatial_extent(
    bboxes: Sequence[Sequence[float]], *, wgs84_only: bool = True
) -> pystac.SpatialExtent:
    """Build the STAC 1.1.0 spatial extent that covers ``bboxes``.

    This is the one constructor of a collection spatial extent. The extent is
    one bbox, or three or more with the envelope first: a union that crosses
    the antimeridian is the envelope, then its two parts (issue #852).

    Args:
        bboxes: WGS84 bboxes, 2D or 3D. Invalid ones are skipped with a warning.
        wgs84_only: Also reject a bbox outside the WGS84 ranges. False accepts
            any finite bbox, for a format sample in its own CRS.

    Returns:
        The spatial extent.

    Raises:
        InvalidBboxError: No bbox is valid.
    """
    result = compute_bbox_union([list(bbox) for bbox in bboxes], wgs84_only=wgs84_only)
    if result.bboxes is None:
        raise InvalidBboxError(f"no valid bbox among {len(bboxes)} to build an extent from")
    return pystac.SpatialExtent(bboxes=result.bboxes)


def declared_extent_bbox(metadata: Mapping[str, Any]) -> list[float] | None:
    """The bbox a human declares in metadata.yaml.

    The STAC form ``extent.spatial.bbox`` holds one bbox or a list of them, and
    the first one counts. ``extent.bbox`` and a top-level ``bbox`` also work.
    It is the only override of the derived extent, and it can only widen it:
    the derived extent is the union of this bbox and the members.
    """
    bbox = metadata.get("bbox")
    extent = metadata.get("extent")
    if bbox is None and isinstance(extent, dict):
        spatial = extent.get("spatial")
        bbox = spatial.get("bbox") if isinstance(spatial, dict) else extent.get("bbox")
    if isinstance(bbox, list) and bbox and isinstance(bbox[0], list):
        bbox = bbox[0]
    if (
        isinstance(bbox, list)
        and len(bbox) in (4, 6)
        and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in bbox)
    ):
        return to_2d_bbox(bbox)
    return None


def _temporal_extent(members: Sequence[CollectionMember]) -> pystac.TemporalExtent | None:
    """The interval from the earliest start to the latest end of the members."""
    from portolan_cli.temporal import ensure_utc_aware

    starts = [start for m in members if (start := ensure_utc_aware(m.start)) is not None]
    ends = [end for m in members if (end := ensure_utc_aware(m.end)) is not None]
    if not starts and not ends:
        return None
    return pystac.TemporalExtent(
        intervals=[[min(starts) if starts else None, max(ends) if ends else None]]
    )


def derive_collection_fields(
    members: Sequence[CollectionMember],
    metadata: Mapping[str, Any],
) -> CollectionFields:
    """Compute the collection fields from the members and metadata.yaml.

    Pure: the result depends on the arguments only, never on the values the
    collection stores. A wrong extent or a stale summary therefore cannot
    survive a rebuild (issues #801, #1023). Every writer of ``collection.json``
    calls this and replaces the stored values with the result.

    Args:
        members: The items and collection-level data assets of the collection.
        metadata: The collection's own metadata.yaml. Its ``bbox`` (or
            ``extent.bbox``) widens the spatial extent.

    Returns:
        The derived fields.
    """
    bboxes = [m.bbox for m in members if m.bbox is not None]
    declared = declared_extent_bbox(metadata)
    if declared is not None:
        bboxes.append(declared)
    spatial: pystac.SpatialExtent | None = None
    if bboxes:
        try:
            spatial = build_spatial_extent(bboxes)
        except InvalidBboxError:
            logger.warning("No member reports a valid bbox; the extent is not derived")
    return CollectionFields(
        spatial=spatial,
        temporal=_temporal_extent(members),
        summaries=summarize_properties(m.properties for m in members if m.properties),
    )


def _merged_summaries(stored: Mapping[str, Any], derived: Mapping[str, Any]) -> dict[str, Any]:
    """Replace every summary derivation owns, and keep the others."""
    kept = {key: value for key, value in stored.items() if not _is_summarized(key)}
    return {**kept, **derived}


def _declared_extensions(
    declared: Sequence[str],
    stored: Mapping[str, Any],
    derived: Mapping[str, Any],
    other_keys: Iterable[str],
) -> list[str]:
    """The extension URLs a collection declares once its summaries are derived.

    The derived summaries add the extensions they use. An extension only a
    summary that is gone needed is withdrawn, unless another field of the
    collection or of an asset still uses it. A stale declaration otherwise
    makes the schema check fail on a field nobody writes any more.
    """
    needed = build_stac_extensions(dict(derived))
    in_use = set(needed) | set(build_stac_extensions(dict.fromkeys(other_keys)))
    stale = set(build_stac_extensions(dict(stored))) - in_use
    result = [url for url in declared if url not in stale]
    result.extend(url for url in needed if url not in result)
    return result


def apply_collection_fields(collection: pystac.Collection, fields: CollectionFields) -> None:
    """Replace the derived fields of a pystac collection.

    A field the members cannot derive (no footprint, no time) keeps its stored
    value, which came from metadata.yaml, the AOI of a tabular collection, or
    the column statistics of a table.
    """
    if fields.spatial is not None:
        collection.extent.spatial = fields.spatial
    if fields.temporal is not None:
        collection.extent.temporal = fields.temporal
    stored = collection.summaries.to_dict() if collection.summaries is not None else {}
    collection.summaries = pystac.Summaries(_merged_summaries(stored, fields.summaries))
    other_keys = [
        *collection.extra_fields,
        *(key for asset in collection.assets.values() for key in asset.extra_fields),
    ]
    collection.stac_extensions = _declared_extensions(
        collection.stac_extensions, stored, fields.summaries, other_keys
    )


def apply_collection_fields_to_dict(data: dict[str, Any], fields: CollectionFields) -> None:
    """Replace the derived fields of a parsed ``collection.json`` in place.

    The dict counterpart of :func:`apply_collection_fields`, for writers that
    keep the file as JSON so the enrichment hook fields survive.
    """
    extent = data.setdefault("extent", {})
    if fields.spatial is not None:
        extent["spatial"] = fields.spatial.to_dict()
    if fields.temporal is not None:
        extent["temporal"] = fields.temporal.to_dict()
    stored = data.get("summaries") or {}
    summaries = _merged_summaries(stored, fields.summaries)
    if summaries:
        data["summaries"] = summaries
    else:
        data.pop("summaries", None)
    assets = data.get("assets") or {}
    other_keys = [
        *(key for key in data if key != "summaries"),
        *(key for asset in assets.values() if isinstance(asset, dict) for key in asset),
    ]
    declared = _declared_extensions(
        data.get("stac_extensions") or [], stored, fields.summaries, other_keys
    )
    if declared or "stac_extensions" in data:
        data["stac_extensions"] = declared


def declare_file_extension(collection: pystac.Collection) -> None:
    """Declare the STAC file extension when an asset under the collection uses it.

    Scope is the collection *and* its items: the extension a collection declares
    covers everything it contains, and ``stac_parquet.sync_file_extension``
    withdraws the URI on the same scope, so the two writers agree instead of
    trading edits on every run.

    This once also wrote ``portolan:total_size_bytes`` and
    ``portolan:asset_count``. Issue #654 removed both: the spec defines no
    ``portolan:`` field, and the per-asset ``file:size`` values the totals were
    summed from are published on the assets themselves.

    Args:
        collection: The collection to declare the extension on.
    """
    if not _any_asset_declares_file_fields(collection):
        return

    file_ext_url = EXTENSION_URLS["file"]
    if file_ext_url not in collection.stac_extensions:
        collection.stac_extensions.append(file_ext_url)


def _any_asset_declares_file_fields(collection: pystac.Collection) -> bool:
    """Whether any asset on the collection or its items carries ``file:size``."""
    if any(_has_file_size(asset) for asset in collection.assets.values()):
        return True
    return any(
        _has_file_size(asset)
        for item in collection.get_items(recursive=True)
        for asset in item.assets.values()
    )


def _has_file_size(asset: pystac.Asset) -> bool:
    """Whether an asset declares a usable ``file:size``.

    A boolean is not a size even though ``bool`` is an ``int``, and a string is
    only one when it parses; anything else is a malformed value that must not
    make the collection claim an extension it does not use.
    """
    size = asset.extra_fields.get("file:size") if asset.extra_fields else None
    if isinstance(size, bool):
        return False
    if isinstance(size, int):
        return True
    if isinstance(size, str):
        return size.strip().lstrip("+-").isdigit()
    return False


def update_catalog_provenance(catalog_root: Path) -> None:
    """Stamp the root catalog's sync time when the whole tree is a mirror (issue #684).

    PTL-PRO-003 requires the top-level ``updated`` field on every mirror
    collection, and on the root catalog too when every collection under it is a
    mirror — at that point the catalog as a whole is a copy of data published
    elsewhere. A tree that mixes official and mirrored collections leaves the
    root alone, since the catalog is not wholly either one.

    The value is the newest sync time among the collections, so the root reports
    the freshness a consumer would compare against the source.

    Args:
        catalog_root: Root directory of the catalog.
    """
    import json

    catalog_path = catalog_root / "catalog.json"
    if not catalog_path.exists():
        return

    stamps: list[str] = []
    for collection_json in catalog_root.rglob("collection.json"):
        try:
            data = json.loads(collection_json.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        if data.get("type") != "Collection":
            continue
        if derive_provenance(data.get("providers")) != "mirror":
            return
        updated = data.get("updated")
        if isinstance(updated, str):
            stamps.append(updated)

    if not stamps:
        return

    try:
        catalog_data = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog_data["updated"] = max(stamps)
        write_json_atomic(catalog_path, catalog_data)
    except (json.JSONDecodeError, OSError):
        pass


def add_via_link(
    collection_path: Path,
    source_url: str,
    *,
    title: str | None = None,
) -> None:
    """Add a `via` provenance link to a collection.json file.

    The `via` link relation points to the original data source from which
    the collection was extracted. This is useful for provenance tracking
    and data lineage.

    Per STAC spec, `via` links indicate "the source from which the data
    was originally obtained."

    Args:
        collection_path: Path to the collection.json file.
        source_url: URL of the original data source (e.g., ArcGIS FeatureServer).
        title: Optional title for the link. Defaults to "Source data service".

    Note:
        This function is idempotent - adding the same URL twice will not
        create duplicate links.
    """
    import json

    if not collection_path.exists():
        return

    collection_data = json.loads(collection_path.read_text(encoding="utf-8"))
    links = collection_data.setdefault("links", [])

    # Check if via link already exists with same href
    for link in links:
        if link.get("rel") == "via" and link.get("href") == source_url:
            return  # Already exists, idempotent

    # Add via link
    via_link = {
        "rel": "via",
        "href": source_url,
        "type": "text/html",
        "title": title or "Source data service",
    }
    links.append(via_link)

    write_json_atomic(collection_path, collection_data)


def is_technical_name(text: str | None) -> bool:
    """Check if text looks like a technical/internal name rather than description.

    Technical names are typically identifiers that aren't useful as metadata:
    - Pure snake_case names without spaces (e.g., "bu_building_emprise_v2")
    - Namespace-prefixed (e.g., "ns:LayerName")
    - Short all-lowercase names without spaces (e.g., "layer", "parcels")
    - Short names carrying digits (e.g., "layer1", "parcels2024", "Q4")

    Valid titles include:
    - CamelCase names (e.g., "DenHaagHousing")
    - All-caps acronyms (e.g., "USA", "IGN")
    - Capitalized, digit-free single words (e.g., "Provincia", "País") — Issue
      #513. Note: this also keeps capitalized single-word source/layer titles
      ("Buildings", "Parcels") that were previously filtered, which is the
      intended behavior across all callers (extraction, metadata seeding).
    - Titles with spaces, even if they contain underscores (e.g., "Building - building_emprise")

    Args:
        text: Text to check.

    Returns:
        True if text looks like a technical name.
    """
    import re

    if not text:
        return True

    text = text.strip()

    # Has spaces → probably human-readable, even if it contains underscores
    if " " in text:
        return False

    # Contains namespace prefix (ns:name pattern) → technical
    if re.match(r"^[a-z_]+:[A-Za-z]", text):
        return True

    # No spaces + underscores → snake_case identifier
    if "_" in text:
        return True

    # Short tokens without an internal capital are ambiguous: distinguish
    # technical identifiers from ordinary one-word proper titles (Issue #513).
    # Identifiers are all-lowercase ("layer", "parcels") or carry digits
    # ("layer1", "parcels2024"). A capitalized, digit-free word ("Provincia",
    # "Localidad", "País") is a legitimate title and must be preserved.
    # CamelCase and all-caps acronyms ("DenHaagHousing", "USA", "IGN") have an
    # uppercase letter after the first char, so they fail this branch's guard
    # and fall through to the final non-technical return below.
    if not re.search(r"[A-Z]", text[1:]) and len(text) < 20:
        has_digit = any(char.isdigit() for char in text)
        starts_capitalized = text[:1].isupper()
        return bool(has_digit or not starts_capitalized)

    return False


def update_stac_metadata(
    path: Path,
    title: str | None = None,
    description: str | None = None,
) -> bool:
    """Update title and/or description in a STAC catalog.json or collection.json.

    This function patches existing STAC files with metadata extracted from
    external sources (WFS GetCapabilities, ArcGIS REST API, ISO 19139).
    Used by extraction --auto mode to propagate rich metadata to STAC.

    Per Issue #369: Extraction should populate STAC with meaningful metadata,
    not leave generic placeholders like "Collection: layer_name_abc123".

    Skips technical-looking names (underscore identifiers, namespace prefixes)
    to avoid replacing human-readable content with machine identifiers.

    Args:
        path: Path to catalog.json or collection.json file.
        title: New title (None to skip updating title).
        description: New description (None to skip updating description).

    Returns:
        True if file was updated, False if no changes made or file missing.

    Note:
        This function is idempotent. Calling multiple times with the same
        values produces the same result.
    """
    import json

    if not path.exists():
        return False

    # Filter out technical names
    effective_title = title if title and not is_technical_name(title) else None
    effective_description = (
        description if description and not is_technical_name(description) else None
    )

    # Nothing to update
    if effective_title is None and effective_description is None:
        return False

    try:
        stac_data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        import logging

        logging.getLogger(__name__).warning(
            "Failed to parse %s: %s — skipping metadata update", path, e
        )
        return False

    updated = False

    if effective_title is not None:
        stac_data["title"] = effective_title
        updated = True

    if effective_description is not None:
        stac_data["description"] = effective_description
        updated = True

    if updated:
        write_json_atomic(path, stac_data)

    return updated
