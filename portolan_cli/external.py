"""Register external / remote data as catalog collections."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pystac

from portolan_cli.finalization import _save_collection_with_links
from portolan_cli.hookspecs import EnrichContext
from portolan_cli.input_hardening import InputValidationError, validate_remote_url
from portolan_cli.preparation import _validate_collection_id
from portolan_cli.stac import (
    add_asset_to_collection,
    add_via_link,
    apply_human_license,
    create_collection,
)
from portolan_cli.stac_enrich import enrich_collection_file
from portolan_core.bbox import BBOX_2D_LEN, LAT_MAX, LAT_MIN, LON_MAX, LON_MIN

EXTERNAL_ROLE = "external"

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
    """Result of registering external data."""

    collection_id: str
    collection_path: Path
    href: str
    media_type: str
    via_url: str | None

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope ``data`` of ``portolan add-external``."""
        return {
            "collection_id": self.collection_id,
            "collection_path": str(self.collection_path),
            "href": self.href,
            "media_type": self.media_type,
            "via": self.via_url,
            "managed": False,
        }


def is_external_href(href: str) -> bool:
    """Return True if ``href`` is an absolute URI (so it lives off the local FS)."""
    return bool(_URI_SCHEME_RE.match(href))


def infer_media_type(url: str) -> str:
    """Infer an IANA media type from a remote URL's apparent file extension."""
    path = urlparse(url).path
    for segment in reversed([s for s in path.split("/") if s]):
        match = re.search(r"(\.[A-Za-z0-9]+)$", segment)
        if match:
            suffix = match.group(1).lower()
            if suffix in _MEDIA_TYPE_BY_SUFFIX:
                return _MEDIA_TYPE_BY_SUFFIX[suffix]
    return DEFAULT_MEDIA_TYPE


def derive_collection_id_from_url(url: str) -> str:
    """Best-effort collection ID from a remote URL."""
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
    """Validate WGS84 bounding box coordinates."""
    if len(bbox) != BBOX_2D_LEN:
        raise ValueError("bbox must have exactly 4 values: min_x, min_y, max_x, max_y")

    min_x, min_y, max_x, max_y = bbox

    if not (LON_MIN <= min_x <= LON_MAX and LON_MIN <= max_x <= LON_MAX):
        raise ValueError(
            f"Longitude must be between -180 and 180, got min_x={min_x}, max_x={max_x}"
        )
    if not (LAT_MIN <= min_y <= LAT_MAX and LAT_MIN <= max_y <= LAT_MAX):
        raise ValueError(f"Latitude must be between -90 and 90, got min_y={min_y}, max_y={max_y}")
    if min_x > max_x:
        raise ValueError(f"min_x ({min_x}) must be <= max_x ({max_x})")
    if min_y > max_y:
        raise ValueError(f"min_y ({min_y}) must be <= max_y ({max_y})")


def add_external(
    *,
    catalog_root: Path,
    url: str,
    collection_id: str | None = None,
    title: str | None = None,
    description: str | None = None,
    media_type: str | None = None,
    license: str,
    license_url: str | None = None,
    via_url: str | None = None,
    bbox: list[float] | None = None,
    asset_key: str = "data",
    force: bool = False,
) -> ExternalAddResult:
    """Register remote data as an external catalog collection."""
    # validate remote URL (rejects file://, path traversals, etc.)
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
    resolved_description = description or f"External data referenced in place from {url}"

    collection = create_collection(
        collection_id=resolved_id,
        description=resolved_description,
        title=title,
        license=license,
        bbox=bbox,
    )

    apply_human_license(collection, {"license": license, "license_url": license_url})

    asset = pystac.Asset(
        href=url,
        media_type=resolved_media_type,
        roles=["data", EXTERNAL_ROLE],
        title=title or "External data",
    )
    add_asset_to_collection(collection, asset_key, asset)

    # Collection dir mirrors the layout 'add' produces: <root>/<collection_id>/.
    collection_dir.mkdir(parents=True, exist_ok=True)
    _save_collection_with_links(
        collection, collection_dir, catalog_root, resolved_id, readme_link=False
    )

    from portolan_cli.catalog import apply_catalog_human_titles, ensure_link_titles

    apply_catalog_human_titles(catalog_root)

    ensure_link_titles(catalog_root)

    collection_path = collection_dir / "collection.json"

    resolved_via = via_url or url
    add_via_link(collection_path, resolved_via, title=title or "Source data")

    enrich_collection_file(
        collection_path,
        EnrichContext(
            catalog_root=catalog_root,
            collection_id=resolved_id,
            document_path=collection_path,
        ),
    )

    return ExternalAddResult(
        collection_id=resolved_id,
        collection_path=collection_path,
        href=url,
        media_type=resolved_media_type,
        via_url=resolved_via,
    )
