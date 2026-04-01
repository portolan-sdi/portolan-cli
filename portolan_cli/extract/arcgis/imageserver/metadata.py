"""STAC metadata generation from ImageServer metadata.

This module converts ImageServer metadata into STAC Collection and Item
JSON structures. The output is ready for json.dump() and follows the
STAC 1.1.0 specification.

Key mappings:
- ImageServerMetadata.name → Collection.id (sanitized)
- ImageServerMetadata.full_extent → Collection.extent.spatial (transformed to WGS84)
- TileSpec.bbox → Item.bbox and Item.geometry
- COG path → Item.assets.data.href

References:
- STAC Spec: https://stacspec.org/
- STAC Raster Extension: https://github.com/stac-extensions/raster
- ADR-0035: Temporal extent handling (use null for unknown dates)
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from portolan_cli.crs import transform_bbox_to_wgs84
from portolan_cli.models._stac_version import get_stac_version

if TYPE_CHECKING:
    from portolan_cli.extract.arcgis.imageserver.discovery import ImageServerMetadata
    from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec

# COG media type per STAC best practices
COG_MEDIA_TYPE = "image/tiff; application=geotiff; profile=cloud-optimized"


def create_collection_metadata(
    service_metadata: ImageServerMetadata,
    service_url: str,
) -> dict[str, Any]:
    """Create STAC Collection JSON from ImageServer metadata.

    Args:
        service_metadata: Parsed ImageServer metadata.
        service_url: URL of the source ImageServer.

    Returns:
        Dict ready for json.dump() with STAC Collection structure:
        - type: "Collection"
        - stac_version: from portolan_cli.models._stac_version
        - id: derived from service name (sanitized)
        - extent: spatial from fullExtent (WGS84), temporal open interval
        - summaries: band count, pixel type
        - links: source link to ImageServer, self link
        - license: "proprietary" (default for unknown sources)
    """
    collection_id = _sanitize_id(service_metadata.name)

    # Transform extent to WGS84
    crs_string = service_metadata.get_crs_string()
    wgs84_bbox = transform_bbox_to_wgs84(
        service_metadata.get_bbox_tuple(),
        crs_string,
        allow_guess=True,  # Log warning but don't fail
    )

    # Build description with fallback
    description = _get_description(service_metadata)

    # Build summaries
    summaries = _build_summaries(service_metadata)

    # Build links (no root link - catalog.json is not created by extraction)
    links = [
        {
            "rel": "self",
            "href": "./collection.json",
            "type": "application/json",
        },
        {
            "rel": "source",
            "href": service_url,
            "type": "application/json",
            "title": "Source ImageServer",
        },
    ]

    # Build providers if copyright available
    providers = []
    if service_metadata.copyright_text:
        providers.append(
            {
                "name": service_metadata.copyright_text,
                "roles": ["producer", "licensor"],
            }
        )

    collection: dict[str, Any] = {
        "type": "Collection",
        "stac_version": get_stac_version(),
        "stac_extensions": [
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
        ],
        "id": collection_id,
        "title": service_metadata.name,
        "description": description,
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [list(wgs84_bbox)],
            },
            "temporal": {
                # Open interval per ADR-0035: unknown acquisition dates
                "interval": [[None, None]],
            },
        },
        "summaries": summaries,
        "links": links,
    }

    if providers:
        collection["providers"] = providers

    return collection


def create_item_metadata(
    tile: TileSpec,
    service_metadata: ImageServerMetadata,
    cog_path: str,
    collection_id: str | None = None,
) -> dict[str, Any]:
    """Create STAC Item JSON for a single tile.

    Args:
        tile: Tile specification with bbox and ID.
        service_metadata: Parent ImageServer metadata.
        cog_path: Relative or absolute path to the COG file.
        collection_id: ID of the parent collection (required by STAC spec).

    Returns:
        Dict ready for json.dump() with STAC Item structure:
        - type: "Feature"
        - stac_version: from portolan_cli.models._stac_version
        - id: from tile ID
        - geometry: Polygon from tile bbox
        - bbox: tile bbox (transformed to WGS84 if needed)
        - properties: datetime (null), created timestamp
        - collection: parent collection ID
        - assets: COG asset with href and media type
    """
    item_id = tile.get_id()

    # Derive collection_id from service name if not provided
    if collection_id is None:
        collection_id = _sanitize_id(service_metadata.name)

    # Transform bbox to WGS84
    crs_string = service_metadata.get_crs_string()
    wgs84_bbox = transform_bbox_to_wgs84(
        tile.bbox,
        crs_string,
        allow_guess=True,
    )

    # Build geometry from bbox
    geometry = _bbox_to_polygon(wgs84_bbox)

    # Current timestamp for created field
    now = datetime.now(timezone.utc).isoformat()

    item: dict[str, Any] = {
        "type": "Feature",
        "stac_version": get_stac_version(),
        "stac_extensions": [
            "https://stac-extensions.github.io/raster/v1.1.0/schema.json",
        ],
        "id": item_id,
        "geometry": geometry,
        "bbox": list(wgs84_bbox),
        "properties": {
            # datetime is null for imagery without known acquisition date
            "datetime": None,
            "created": now,
        },
        "collection": collection_id,
        "links": [
            {
                "rel": "self",
                "href": f"./{item_id}.json",
                "type": "application/geo+json",
            },
            {
                "rel": "parent",
                "href": "./collection.json",
                "type": "application/json",
            },
            {
                "rel": "collection",
                "href": "./collection.json",
                "type": "application/json",
            },
        ],
        "assets": {
            "data": {
                "href": cog_path,
                "type": COG_MEDIA_TYPE,
                "title": "Cloud-Optimized GeoTIFF",
                "roles": ["data"],
                "raster:bands": _build_raster_bands(service_metadata),
            },
        },
    }

    return item


def _sanitize_id(name: str) -> str:
    """Sanitize service name for use as STAC ID.

    STAC IDs should be URL-safe, lowercase, and contain only
    alphanumeric characters, underscores, and hyphens.

    Args:
        name: Original service name.

    Returns:
        Sanitized ID string.
    """
    # Convert to lowercase
    sanitized = name.lower()

    # Replace spaces and special characters with underscores
    sanitized = re.sub(r"[^a-z0-9_-]", "_", sanitized)

    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)

    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")

    # Ensure not empty
    return sanitized or "unnamed"


def _get_description(metadata: ImageServerMetadata) -> str:
    """Get description with fallback.

    Priority:
    1. description
    2. Generated fallback

    Args:
        metadata: ImageServer metadata.

    Returns:
        Description string.
    """
    if metadata.description:
        return metadata.description
    return (
        f"Raster imagery from {metadata.name} ({metadata.band_count} bands, {metadata.pixel_type})"
    )


def _build_summaries(metadata: ImageServerMetadata) -> dict[str, Any]:
    """Build STAC Collection summaries.

    Args:
        metadata: ImageServer metadata.

    Returns:
        Summaries dict with band info and pixel type.
    """
    summaries: dict[str, Any] = {}

    # Band information using raster extension
    raster_bands = _build_raster_bands(metadata)
    if raster_bands:
        summaries["raster:bands"] = raster_bands

    # Add basic band count info
    summaries["eo:bands"] = [{"name": f"band_{i + 1}"} for i in range(metadata.band_count)]

    return summaries


def _build_raster_bands(metadata: ImageServerMetadata) -> list[dict[str, Any]]:
    """Build raster:bands array for STAC raster extension.

    Args:
        metadata: ImageServer metadata.

    Returns:
        List of band objects with data_type.
    """
    # Map ArcGIS pixel types to STAC data types
    pixel_type_map = {
        "U1": "uint8",
        "U2": "uint8",
        "U4": "uint8",
        "U8": "uint8",
        "S8": "int8",
        "U16": "uint16",
        "S16": "int16",
        "U32": "uint32",
        "S32": "int32",
        "F32": "float32",
        "F64": "float64",
        "C64": "complex64",
        "C128": "complex128",
    }

    data_type = pixel_type_map.get(metadata.pixel_type, "other")

    # Generate generic band names based on band_count
    # (ImageServer REST API doesn't provide per-band metadata)
    return [
        {
            "name": f"band_{i + 1}",
            "data_type": data_type,
        }
        for i in range(metadata.band_count)
    ]


def _bbox_to_polygon(
    bbox: tuple[float, float, float, float],
) -> dict[str, Any]:
    """Convert bbox to GeoJSON Polygon geometry.

    Args:
        bbox: Bounding box as (minx, miny, maxx, maxy).

    Returns:
        GeoJSON Polygon geometry dict.
    """
    minx, miny, maxx, maxy = bbox

    # Create closed ring following the right-hand rule (RFC 7946):
    # For exterior rings, vertices are in counter-clockwise order when
    # viewed from above (i.e., the interior is on the left as you traverse).
    # This ordering: SW -> SE -> NE -> NW -> SW traces counter-clockwise.
    coordinates = [
        [
            [minx, miny],  # SW
            [maxx, miny],  # SE
            [maxx, maxy],  # NE
            [minx, maxy],  # NW
            [minx, miny],  # SW (close ring)
        ]
    ]

    return {
        "type": "Polygon",
        "coordinates": coordinates,
    }
