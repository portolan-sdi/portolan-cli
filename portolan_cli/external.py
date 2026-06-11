"""Register external / remote datasets as catalog collections.

Some valuable datasets are already published cloud-natively at a remote
location (e.g. Overture Maps places as planet-scale GeoParquet on Overture's
own public S3). Such data should be *referenced in place* — added to a Portolan
catalog as an external collection that points at the remote URL — rather than
downloaded and re-converted.

This module creates a STAC ``collection.json`` whose collection-level ``data``
asset ``href`` is the remote URL, marked as external / not-managed, plus a
``rel:"via"`` provenance link to the source. No bytes are downloaded and no
conversion runs.

Per ADR-0031 a single vector file is a collection-level asset (no item.json),
so an external single-file dataset maps cleanly onto one collection with one
collection-level asset.

The metadata scanner (``portolan_cli.metadata.scan``) already skips
scheme-qualified hrefs (``s3://``, ``https://``, ...) for local existence and
freshness checks, so ``portolan check`` does not trip on the remote asset.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import pystac

from portolan_cli.dataset import (
    _save_collection_with_links,
    _validate_collection_id,
)
from portolan_cli.stac import add_asset_to_collection, add_via_link, create_collection
from portolan_cli.validation import InputValidationError, validate_remote_url

# Marks an asset as referenced in place rather than managed (downloaded/
# converted) by Portolan. Consumers can use this to distinguish in-place
# remote data from catalog-owned data.
MANAGED_FIELD = "portolan:managed"

# Role applied to external assets in addition to "data".
EXTERNAL_ROLE = "external"

# URI scheme matcher for is_external_href (used by scanner/check to skip
# remote assets). Note: validate_remote_url() is stricter and should be
# used for input validation; this is for classification only.
_URI_SCHEME_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")

# Media type inference from the URL's apparent extension. Glob patterns and
# query strings are tolerated by stripping to the last "real" path segment.
_MEDIA_TYPE_BY_SUFFIX: dict[str, str] = {
    ".parquet": pystac.MediaType.VND_APACHE_PARQUET,
    ".geoparquet": pystac.MediaType.VND_APACHE_PARQUET,
    ".pmtiles": pystac.MediaType.VND_PMTILES,
    ".geojson": pystac.MediaType.GEOJSON,
    ".json": pystac.MediaType.JSON,
    ".fgb": pystac.MediaType.FLATGEOBUF,
    ".gpkg": pystac.MediaType.GEOPACKAGE,
    ".tif": pystac.MediaType.COG,
    ".tiff": pystac.MediaType.COG,
    ".zarr": pystac.MediaType.VND_ZARR,
}

DEFAULT_MEDIA_TYPE = "application/octet-stream"


@dataclass
class ExternalAddResult:
    """Result of registering an external dataset.

    Attributes:
        collection_id: ID of the collection that was created.
        collection_path: Path to the written collection.json.
        href: Remote URL the asset points at.
        media_type: Media type recorded for the asset.
        via_url: Provenance URL recorded as a ``rel:"via"`` link (if any).
    """

    collection_id: str
    collection_path: Path
    href: str
    media_type: str
    via_url: str | None


def is_external_href(href: str) -> bool:
    """Return True if ``href`` is an absolute URI (so it lives off the local FS)."""
    return bool(_URI_SCHEME_RE.match(href))


def infer_media_type(url: str) -> str:
    """Infer an IANA media type from a remote URL's apparent file extension.

    Handles glob patterns (``.../*``), Hive-partition paths and query strings
    by inspecting each path segment from the end and using the first one that
    carries a recognised suffix. Falls back to ``application/octet-stream``.
    """
    path = urlparse(url).path
    for segment in reversed([s for s in path.split("/") if s]):
        # Tolerate glob segments like "*.parquet" by matching the trailing
        # ".<ext>" directly (Path(".parquet").suffix is "", so we use a regex).
        # A bare "*" has no extension and is skipped.
        match = re.search(r"(\.[A-Za-z0-9]+)$", segment)
        if match:
            suffix = match.group(1).lower()
            if suffix in _MEDIA_TYPE_BY_SUFFIX:
                return _MEDIA_TYPE_BY_SUFFIX[suffix]
    return DEFAULT_MEDIA_TYPE


def derive_collection_id_from_url(url: str) -> str:
    """Best-effort collection ID from a remote URL.

    Picks the last meaningful path segment, strips any extension and glob
    characters, and normalises to the STAC id charset. Raises ``ValueError``
    if nothing usable can be derived (caller should require an explicit
    ``--collection``).
    """
    path = urlparse(url).path
    candidates = [s for s in path.split("/") if s and "*" not in s]
    for segment in reversed(candidates):
        stem = Path(segment).stem
        # Hive partition segments like "theme=places" -> "places"
        if "=" in stem:
            stem = stem.split("=", 1)[1]
        normalized = re.sub(r"[^a-zA-Z0-9_-]+", "-", stem).strip("-")
        if normalized:
            return normalized
    raise ValueError(
        f"Could not derive a collection ID from URL '{url}'. Pass --collection explicitly."
    )


def _validate_bbox(bbox: list[float]) -> None:
    """Validate WGS84 bounding box coordinates.

    Args:
        bbox: [min_x, min_y, max_x, max_y] in WGS84.

    Raises:
        ValueError: If coordinates are out of range or min > max.
    """
    if len(bbox) != 4:
        raise ValueError("bbox must have exactly 4 values: min_x, min_y, max_x, max_y")

    min_x, min_y, max_x, max_y = bbox

    if not (-180 <= min_x <= 180 and -180 <= max_x <= 180):
        raise ValueError(
            f"Longitude must be between -180 and 180, got min_x={min_x}, max_x={max_x}"
        )
    if not (-90 <= min_y <= 90 and -90 <= max_y <= 90):
        raise ValueError(f"Latitude must be between -90 and 90, got min_y={min_y}, max_y={max_y}")
    if min_x > max_x:
        raise ValueError(f"min_x ({min_x}) must be <= max_x ({max_x})")
    if min_y > max_y:
        raise ValueError(f"min_y ({min_y}) must be <= max_y ({max_y})")


def add_external_dataset(
    *,
    catalog_root: Path,
    url: str,
    collection_id: str | None = None,
    title: str | None = None,
    description: str | None = None,
    media_type: str | None = None,
    license: str = "proprietary",
    via_url: str | None = None,
    bbox: list[float] | None = None,
    asset_key: str = "data",
    force: bool = False,
) -> ExternalAddResult:
    """Register a remote dataset as an external catalog collection.

    Creates ``<catalog_root>/<collection_id>/collection.json`` with a
    collection-level ``data`` asset whose ``href`` is ``url`` (kept as-is,
    not downloaded), marked external / not-managed, plus a ``rel:"via"``
    provenance link. The collection is linked into the root catalog.

    Args:
        catalog_root: Catalog root (the directory containing catalog.json).
        url: Remote URL of the data (e.g. an ``s3://`` or ``https://`` href).
        collection_id: Collection ID. Derived from the URL when omitted.
        title: Optional human-readable collection title.
        description: Optional description (defaults to a generated one).
        media_type: Asset media type. Inferred from the URL when omitted.
        license: SPDX license identifier (default: "proprietary").
        via_url: Provenance URL for the ``rel:"via"`` link. Defaults to ``url``.
        bbox: Optional WGS84 bbox [min_x, min_y, max_x, max_y]. Global if omitted.
        asset_key: Key for the asset entry in collection.json (default "data").
        force: If True, overwrite existing collection. Default False.

    Returns:
        ExternalAddResult describing what was written.

    Raises:
        InputValidationError: If the URL fails validation (unsupported scheme,
            path traversal, control characters, etc.).
        ValueError: If the collection ID is missing/invalid, or bbox is invalid.
        FileNotFoundError: If ``catalog_root`` is not an initialised catalog.
        FileExistsError: If collection already exists and force=False.
    """
    # ADR-0030: validate remote URL (rejects file://, path traversals, etc.)
    try:
        validate_remote_url(url)
    except InputValidationError as e:
        raise InputValidationError(f"{e}. Use 'portolan add' for local files.") from e

    if not (catalog_root / "catalog.json").exists():
        raise FileNotFoundError(
            f"Not a Portolan catalog: {catalog_root} (run 'portolan init' first)"
        )

    resolved_id = collection_id or derive_collection_id_from_url(url)
    _validate_collection_id(resolved_id)

    # Check for existing collection (overwrite protection)
    collection_dir = catalog_root / resolved_id
    collection_json = collection_dir / "collection.json"
    if collection_json.exists() and not force:
        raise FileExistsError(
            f"Collection '{resolved_id}' already exists at {collection_json}. "
            "Use --force to overwrite."
        )

    # Validate bbox if provided
    if bbox is not None:
        _validate_bbox(bbox)

    resolved_media_type = media_type or infer_media_type(url)
    resolved_description = description or f"External dataset referenced in place from {url}"

    collection = create_collection(
        collection_id=resolved_id,
        description=resolved_description,
        title=title,
        license=license,
        bbox=bbox,
    )

    asset = pystac.Asset(
        href=url,
        media_type=resolved_media_type,
        roles=["data", EXTERNAL_ROLE],
        title=title or "External data",
        extra_fields={MANAGED_FIELD: False},
    )
    add_asset_to_collection(collection, asset_key, asset)

    # Collection dir mirrors the layout 'add' produces: <root>/<collection_id>/.
    collection_dir.mkdir(parents=True, exist_ok=True)
    _save_collection_with_links(collection, collection_dir, catalog_root, resolved_id)

    # Issue #502: backfill the human-readable title onto the new child link.
    from portolan_cli.catalog import ensure_link_titles

    ensure_link_titles(catalog_root)

    collection_path = collection_dir / "collection.json"

    resolved_via = via_url or url
    add_via_link(collection_path, resolved_via, title=title or "Source dataset")

    return ExternalAddResult(
        collection_id=resolved_id,
        collection_path=collection_path,
        href=url,
        media_type=resolved_media_type,
        via_url=resolved_via,
    )
