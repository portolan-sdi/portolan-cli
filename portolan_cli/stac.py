"""STAC generation module - wraps pystac for Portolan's conventions.

Provides opinionated helpers for creating STAC catalogs, collections, and items
with consistent defaults and conventions for Portolan-managed catalogs.

Key conventions:
- Self-contained catalog type (relative links, portable)
- WGS84 (EPSG:4326) as default CRS
- Consistent asset naming and roles
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pystac
from pystac.summaries import Summarizer, SummaryStrategy

from portolan_cli.constants import PARTITION_EXTENSION_URI, PORTOLAN_SCHEMA_URI
from portolan_cli.humanize import humanize_slug
from portolan_cli.json_io import write_json_atomic
from portolan_cli.providers import derive_provenance, resolve_providers
from portolan_cli.utils import href_root

if TYPE_CHECKING:
    from portolan_cli.metadata.tabular import TabularMetadata

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

    Strategies:
        SMART: Preserve human-enrichable fields (title, description),
               update machine-derivable fields (href, type, row_count).
               This is the default and recommended for most use cases.
        KEEP: Preserve all existing fields, only add missing ones.
              Use for legacy catalog imports where existing metadata is trusted.
        OVERWRITE: Replace everything with auto-detected values.
                   Use for regenerating metadata from scratch.
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

# STAC version we generate (v1.1.0 has unified bands array, superseding eo:bands/raster:bands)
STAC_VERSION = "1.1.0"

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
        spatial=pystac.SpatialExtent(bboxes=[bbox]),
        temporal=pystac.TemporalExtent(intervals=[temporal_interval]),
    )

    collection = pystac.Collection(
        id=collection_id,
        description=description,
        extent=extent,
        title=title,
        license=license,
    )

    return collection


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


def _now_utc() -> datetime:
    """Get current UTC datetime."""
    return datetime.now(timezone.utc)


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


def save_catalog(catalog: pystac.Catalog, dest_dir: Path) -> None:
    """Save a STAC catalog to disk.

    Saves as a self-contained catalog with relative links.

    Args:
        catalog: The catalog to save.
        dest_dir: Directory to save the catalog to (will contain catalog.json).
    """
    dest_dir.mkdir(parents=True, exist_ok=True)

    catalog.normalize_hrefs(href_root(dest_dir))

    # Save as self-contained (relative links)
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)


def add_collection_to_catalog(
    catalog: pystac.Catalog,
    collection: pystac.Collection,
) -> None:
    """Add a collection as a child of a catalog.

    Args:
        catalog: The parent catalog.
        collection: The collection to add.
    """
    catalog.add_child(collection)


def add_item_to_collection(
    collection: pystac.Collection,
    item: pystac.Item,
    *,
    update_extent: bool = False,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add an item to a collection.

    Args:
        collection: The parent collection.
        item: The item to add.
        update_extent: If True, update collection's spatial extent to
                      encompass the item's bbox.
        merge_strategy: How to handle conflicts with existing extent.
    """
    collection.add_item(item)

    if update_extent:
        _update_collection_extent(collection, item, merge_strategy)


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

    if strategy == MergeStrategy.KEEP:
        return existing

    # SMART strategy: preserve human-enrichable, update machine-derivable
    merged = pystac.Asset(
        href=new.href,  # Machine-derivable: always update
        media_type=new.media_type,  # Machine-derivable: always update
        roles=new.roles,  # Machine-derivable: always update
        title=existing.title if existing.title else new.title,
        description=existing.description if existing.description else new.description,
    )

    # Merge extra_fields
    merged_extra: dict[str, object] = {}

    # Start with existing extra_fields (preserve custom fields)
    if existing.extra_fields:
        merged_extra.update(existing.extra_fields)

    # Update machine-derivable extra_fields from new asset
    if new.extra_fields:
        for key, value in new.extra_fields.items():
            if _is_machine_derivable_extra_field(key):
                merged_extra[key] = value
            elif key not in merged_extra:
                # Add new fields that don't exist
                merged_extra[key] = value

    if merged_extra:
        merged.extra_fields = merged_extra

    return merged


def add_asset_to_collection(
    collection: pystac.Collection,
    asset_key: str,
    asset: pystac.Asset,
    *,
    update_extent_from_bbox: list[float] | None = None,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Add an asset directly to a collection (collection-level asset).

    Single vector files (GeoParquet, Shapefile, GeoPackage) are
    collection-level assets—no item.json, asset directly in collection.json.

    Issue #447 FIX: Before adding, check if ANY existing asset points to the same
    href. If so, use that key (preserving human-authored keys) instead of creating
    a duplicate entry.

    When an asset with the same key already exists, the merge_strategy controls
    how fields are combined (Issue #446):
    - SMART (default): Preserve human-enrichable fields (title, description),
                       update machine-derivable fields (href, media_type, roles).
    - KEEP: Preserve all existing fields, only add the asset if missing.
    - OVERWRITE: Replace the existing asset entirely.
    - INTERACTIVE: Not yet implemented.

    Args:
        collection: The collection to add the asset to.
        asset_key: Key for the asset (e.g., "data", "boundaries").
        asset: The pystac.Asset to add.
        update_extent_from_bbox: If provided, update collection's spatial extent
            to encompass this bbox [min_x, min_y, max_x, max_y].
        merge_strategy: How to handle conflicts with existing assets.
    """
    # Issue #447: Check if any existing asset already points to the same href
    # This prevents duplicate entries when human-authored key differs from auto-key
    existing_key_by_href = None
    for key, existing in collection.assets.items():
        if existing.href == asset.href:
            existing_key_by_href = key
            break

    # Use the existing key if found (preserves human-authored keys like "census_2020")
    # Otherwise use the provided key (e.g., stem-based "data")
    final_key = existing_key_by_href if existing_key_by_href is not None else asset_key
    existing_asset = collection.assets.get(final_key)

    if existing_asset is not None:
        asset = _merge_asset(existing_asset, asset, merge_strategy)

    collection.add_asset(final_key, asset)

    if update_extent_from_bbox:
        _update_collection_extent_from_bbox(collection, update_extent_from_bbox, merge_strategy)


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
        if collection.stac_extensions is None:
            collection.stac_extensions = []
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

    collection.extra_fields["updated"] = to_rfc3339(synced_at or datetime.now(timezone.utc))

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
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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
    if collection.stac_extensions is None:
        collection.stac_extensions = []
    if ext_url not in collection.stac_extensions:
        collection.stac_extensions.append(ext_url)


def _is_placeholder_extent(bbox: list[float]) -> bool:
    """Check if a bbox is the whole-world placeholder extent.

    Issue #447: Placeholder extents like [-180, -90, 180, 90] should be
    replaced with actual data extent, not expanded.
    """
    from portolan_cli.bbox import to_2d_bbox

    # Reduce to 2D so a 6-element placeholder ([−180,−90,z,180,90,z]) is still
    # recognized; east/north live at indices 3/4 there, not 2/3 (issue #592).
    west, south, east, north = to_2d_bbox(bbox)

    # Allow small tolerance for floating point comparison
    return (
        abs(west - (-180)) < 0.001
        and abs(south - (-90)) < 0.001
        and abs(east - 180) < 0.001
        and abs(north - 90) < 0.001
    )


def _update_collection_extent_from_bbox(
    collection: pystac.Collection,
    bbox: list[float],
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Update a collection's spatial extent to include a bounding box.

    Issue #447 FIX: Respects merge strategy and handles placeholder extents:
    - KEEP: Don't modify extent at all (preserve manual settings)
    - SMART/OVERWRITE: Replace placeholder extent, expand otherwise

    Args:
        collection: The collection to update.
        bbox: Bounding box [min_x, min_y, max_x, max_y] to include.
        merge_strategy: How to handle conflicts with existing extent.
    """
    # KEEP strategy: preserve existing extent entirely
    if merge_strategy == MergeStrategy.KEEP:
        return

    # Never let an invalid bbox (inf/nan/effectively-infinite sentinel) poison the
    # collection extent. Use finiteness + sane-magnitude only, since this bbox may
    # be in a projected source CRS (issue #516).
    import logging

    from portolan_cli.bbox import get_bbox_validation_reason

    reason = get_bbox_validation_reason(list(bbox), wgs84_only=False)
    if reason is not None:
        logging.getLogger(__name__).warning(
            "Skipping invalid bbox for collection '%s' extent: %s", collection.id, reason
        )
        return

    from portolan_cli.bbox import to_2d_bbox

    current_bbox = to_2d_bbox(collection.extent.spatial.bboxes[0])
    incoming = to_2d_bbox(list(bbox))

    # If current extent is placeholder, replace entirely with actual data
    if _is_placeholder_extent(current_bbox):
        collection.extent.spatial = pystac.SpatialExtent(bboxes=[incoming])
        return

    # Otherwise expand to include new bbox. Reduce both to 2D first so a
    # 6-element bbox's min_z can't be mistaken for the easting (issue #592).
    new_bbox = [
        min(current_bbox[0], incoming[0]),  # min_x
        min(current_bbox[1], incoming[1]),  # min_y
        max(current_bbox[2], incoming[2]),  # max_x
        max(current_bbox[3], incoming[3]),  # max_y
    ]

    collection.extent.spatial = pystac.SpatialExtent(bboxes=[new_bbox])


def _update_collection_extent(
    collection: pystac.Collection,
    item: pystac.Item,
    merge_strategy: MergeStrategy = MergeStrategy.SMART,
) -> None:
    """Update a collection's spatial extent to include an item's bbox.

    Issue #447 FIX: Respects merge strategy and handles placeholder extents
    (consistent with _update_collection_extent_from_bbox for collection-level assets).

    Args:
        collection: The collection to update.
        item: The item whose bbox should be included.
        merge_strategy: How to handle conflicts with existing extent.
    """
    if item.bbox is None:
        return

    # KEEP strategy: preserve existing extent entirely
    if merge_strategy == MergeStrategy.KEEP:
        return

    # Never let an invalid item bbox (inf/nan/effectively-infinite sentinel)
    # poison the collection extent (issue #516).
    import logging

    from portolan_cli.bbox import get_bbox_validation_reason

    reason = get_bbox_validation_reason(list(item.bbox), wgs84_only=False)
    if reason is not None:
        logging.getLogger(__name__).warning(
            "Skipping invalid bbox for collection '%s' extent: %s", collection.id, reason
        )
        return

    from portolan_cli.bbox import to_2d_bbox

    current_bbox = to_2d_bbox(collection.extent.spatial.bboxes[0])
    item_bbox = to_2d_bbox(list(item.bbox))

    # If current extent is placeholder, replace entirely with actual data
    if _is_placeholder_extent(current_bbox):
        collection.extent.spatial = pystac.SpatialExtent(bboxes=[item_bbox])
        return

    # Otherwise expand to include item bbox. Reduce both to 2D first so a
    # 6-element bbox's min_z can't be mistaken for the easting (issue #592).
    new_bbox = [
        min(current_bbox[0], item_bbox[0]),  # min_x
        min(current_bbox[1], item_bbox[1]),  # min_y
        max(current_bbox[2], item_bbox[2]),  # max_x
        max(current_bbox[3], item_bbox[3]),  # max_y
    ]

    collection.extent.spatial = pystac.SpatialExtent(bboxes=[new_bbox])


def update_collection_temporal_extent(
    collection: pystac.Collection,
    item_datetime: datetime | None,
) -> None:
    """Update a collection's temporal extent to include an item's datetime.

    Widens the collection's temporal interval to encompass the item's datetime.
    Items without datetime have null interval and are not included.

    Args:
        collection: The collection to update.
        item_datetime: The item's datetime, or None for provisional items.
    """
    from portolan_cli.temporal import ensure_utc_aware

    if item_datetime is None:
        return  # Provisional items don't affect temporal extent

    # Normalize to UTC-aware to avoid naive/aware comparison errors
    item_dt = ensure_utc_aware(item_datetime)
    assert item_dt is not None  # nosec B101 - type narrowing for mypy, runtime checked above

    # Get current interval
    current_interval = collection.extent.temporal.intervals[0]
    current_start = ensure_utc_aware(current_interval[0])
    current_end = ensure_utc_aware(current_interval[1])

    # Widen interval to include item datetime
    new_start = current_start
    new_end = current_end

    if current_start is None or item_dt < current_start:
        new_start = item_dt
    if current_end is None or item_dt > current_end:
        new_end = item_dt

    collection.extent.temporal = pystac.TemporalExtent(intervals=[[new_start, new_end]])


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
    declared: list[str] = list(existing) if isinstance(existing, list) else []

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
    """Merge existing table:columns with new schema based on strategy.

    Args:
        existing_columns: Existing table:columns array from collection.
        new_schema: New schema dict mapping column name to type.
        strategy: The merge strategy to apply.

    Returns:
        Merged columns array.
    """
    if strategy == MergeStrategy.OVERWRITE:
        return [{"name": name, "type": dtype} for name, dtype in new_schema.items()]

    if strategy == MergeStrategy.KEEP:
        return existing_columns

    # SMART strategy: preserve descriptions, update types, handle adds/removes
    # Build lookup of existing columns by name
    existing_by_name = {col["name"]: col for col in existing_columns}

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

    When the collection already has table:columns, the merge_strategy controls
    how column metadata is combined (Issue #446):
    - SMART (default): Preserve column descriptions, update types.
    - KEEP: Preserve all existing table extension fields.
    - OVERWRITE: Replace everything with auto-detected values.

    Args:
        collection: The collection to add extension fields to.
        metadata: A GeoParquetMetadata-like object with feature_count,
                 geometry_column, and schema attributes.
        merge_strategy: How to handle conflicts with existing metadata.
    """
    # KEEP strategy: don't modify existing table extension fields
    if merge_strategy == MergeStrategy.KEEP:
        if "table:row_count" in collection.extra_fields:
            # Already has table extension, don't overwrite
            return

    # Set row count (machine-derivable: always update in SMART/OVERWRITE)
    if hasattr(metadata, "feature_count") and metadata.feature_count is not None:
        collection.extra_fields["table:row_count"] = metadata.feature_count

    # Set primary geometry column (machine-derivable: always update)
    if hasattr(metadata, "geometry_column") and metadata.geometry_column is not None:
        collection.extra_fields["table:primary_geometry"] = metadata.geometry_column

    # Set columns from schema with merge logic
    if hasattr(metadata, "schema") and metadata.schema:
        existing_columns = collection.extra_fields.get("table:columns", [])
        if existing_columns and merge_strategy != MergeStrategy.OVERWRITE:
            columns = _merge_table_columns(existing_columns, metadata.schema, merge_strategy)
        else:
            columns = [{"name": name, "type": dtype} for name, dtype in metadata.schema.items()]
        collection.extra_fields["table:columns"] = columns

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["table"]
    if ext_url not in (collection.stac_extensions or []):
        if collection.stac_extensions is None:
            collection.stac_extensions = []
        collection.stac_extensions.append(ext_url)


def document_tabular_table(
    collection_data: dict[str, Any],
    asset_key: str,
    metadata: TabularMetadata,
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

    Descriptions a human wrote on existing columns survive, matching
    :data:`MergeStrategy.SMART`; only names and types are refreshed.

    Args:
        collection_data: Parsed ``collection.json``, mutated in place.
        asset_key: Key of the Parquet asset under ``assets``.
        metadata: Schema and row count read from that asset.
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
        MergeStrategy.SMART,
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
    return moment.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def get_existing_table_metadata(collection: pystac.Collection) -> object | None:
    """Extract existing table extension metadata from a collection.

    Returns a GeoParquetMetadata-like object if the collection has table extension
    fields, or None if not present. Used to include existing metadata when
    incrementally adding items to a collection.

    Args:
        collection: The collection to extract metadata from.

    Returns:
        A GeoParquetMetadata object if table extension fields exist, None otherwise.
    """
    from portolan_cli.metadata.geoparquet import GeoParquetMetadata

    row_count = collection.extra_fields.get("table:row_count")
    if row_count is None:
        return None

    # Reconstruct schema from table:columns
    columns = collection.extra_fields.get("table:columns", [])
    schema = {col["name"]: col["type"] for col in columns if "name" in col and "type" in col}

    return GeoParquetMetadata(
        bbox=None,  # Not stored in table extension
        crs=None,  # Not stored in table extension
        geometry_type=None,  # Not stored in table extension
        geometry_column=collection.extra_fields.get("table:primary_geometry", "geometry"),
        feature_count=row_count,
        schema=schema,
    )


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

    geometry_types = {getattr(m, "geometry_type", None) for m in metadata_list} - {None}
    if len(geometry_types) > 1:
        warnings.warn(
            f"Mixed geometry types detected: {geometry_types}. Using first item's type.",
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
        geometry_type=getattr(first, "geometry_type", None),
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
    if ext_url not in (item.stac_extensions or []):
        if item.stac_extensions is None:
            item.stac_extensions = []
        item.stac_extensions.append(ext_url)


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
    if not hasattr(metadata, "geometry_type") or metadata.geometry_type is None:
        return

    # geometry_types is an array per spec
    geometry_types = metadata.geometry_type
    if isinstance(geometry_types, str):
        geometry_types = [geometry_types]

    item.properties["vector:geometry_types"] = geometry_types

    # Update stac_extensions if not already present
    ext_url = EXTENSION_URLS["vector"]
    if ext_url not in (item.stac_extensions or []):
        if item.stac_extensions is None:
            item.stac_extensions = []
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
        if ext_url not in (item.stac_extensions or []):
            if item.stac_extensions is None:
                item.stac_extensions = []
            item.stac_extensions.append(ext_url)


def add_collection_extensions_from_summaries(
    collection: pystac.Collection,
    summaries: dict[str, object],
) -> None:
    """Add extension URLs to collection based on fields in summaries.

    Scans summary keys for extension-prefixed fields and adds the corresponding
    extension schema URLs to the collection's stac_extensions array.

    This ensures collections declare extensions used by their items (Issue #336).

    Args:
        collection: The collection to add extension URLs to.
        summaries: Summary dict to scan for extension fields.
    """
    # Use build_stac_extensions to detect which extensions are needed
    extensions_needed = build_stac_extensions(summaries)

    # Ensure collection has stac_extensions list
    if collection.stac_extensions is None:
        collection.stac_extensions = []

    # Add any missing extensions
    for ext_url in extensions_needed:
        if ext_url not in collection.stac_extensions:
            collection.stac_extensions.append(ext_url)


# Hybrid field detection for collection summaries
# Explicit fields with known strategies; auto-detect extension-prefixed fields
SUMMARIZED_FIELDS: dict[str, SummaryStrategy] = {
    "proj:code": SummaryStrategy.ARRAY,  # Distinct CRS codes
    "vector:geometry_types": SummaryStrategy.ARRAY,  # Distinct geometry types
    "gsd": SummaryStrategy.RANGE,  # Ground sample distance range
}


def update_collection_summaries(collection: pystac.Collection) -> None:
    """Update collection summaries from item properties.

    Uses PySTAC's Summarizer with hybrid field detection:
    - Explicit strategies for core fields (proj:code, vector:geometry_types, gsd)
    - Auto-detect extension-prefixed fields (custom:*, etc.)

    Categorical fields only, no numeric aggregation across items.

    Args:
        collection: The collection to update summaries for.
    """
    items = list(collection.get_items(recursive=True))
    if not items:
        return

    # Build field strategies: explicit + auto-detected extension prefixes
    field_strategies = dict(SUMMARIZED_FIELDS)

    # Auto-detect extension-prefixed fields from items (not in explicit list)
    for item in items:
        for key in item.properties:
            if ":" in key and key not in field_strategies:
                # Extension-prefixed field, default to ARRAY (distinct values)
                field_strategies[key] = SummaryStrategy.ARRAY

    summarizer = Summarizer(field_strategies)
    collection.summaries = summarizer.summarize(items)


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
    if collection.stac_extensions is None:
        collection.stac_extensions = []
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
        if has_digit or not starts_capitalized:
            return True
        return False

    return False


# Alias for internal use (maintains backward compatibility)
_is_technical_name = is_technical_name


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
    effective_title = title if title and not _is_technical_name(title) else None
    effective_description = (
        description if description and not _is_technical_name(description) else None
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
