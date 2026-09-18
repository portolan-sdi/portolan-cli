"""Discover provider-independent server resources from a Portolan catalog."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

from portolan_cli.server.model import (
    CollectionCandidate,
    ResourceFormat,
    ResourceType,
    ServerCatalog,
    ServerResourceSpec,
)

_GEOPARQUET_MEDIA_TYPES = frozenset({"application/vnd.apache.parquet"})
_COG_MEDIA_TYPES = frozenset({"image/tiff; application=geotiff; profile=cloud-optimized"})
_PMTILES_MEDIA_TYPES = frozenset({"application/vnd.pmtiles"})


def load_server_catalog(catalog_root: Path) -> ServerCatalog:
    """Load a Portolan catalog into generic server candidates."""
    root = catalog_root.resolve()
    catalog_path = root / "catalog.json"
    catalog = _read_json(catalog_path)
    catalog_id = str(catalog.get("id") or root.name or "portolan")
    candidates = list(_walk_catalog(root, catalog_path, str(catalog_path)))
    return ServerCatalog(
        root=root,
        catalog_id=catalog_id,
        catalog_href=str(catalog_path),
        candidates=candidates,
    )


def discover_server_resources(catalog_root: Path) -> list[ServerResourceSpec]:
    """Return publishable server resources for a Portolan catalog."""
    return [
        candidate.resource
        for candidate in load_server_catalog(catalog_root).candidates
        if candidate.resource is not None
    ]


def load_server_catalog_url(
    catalog_url: str,
    *,
    fetch_json: Callable[[str], dict[str, Any]] | None = None,
) -> ServerCatalog:
    """Load a published STAC catalog URL into generic server candidates."""
    fetch = fetch_json or _fetch_json
    catalog = fetch(catalog_url)
    catalog_id = str(catalog.get("id") or "catalog")
    candidates = list(_walk_catalog_url(catalog_url, catalog, catalog_url, fetch))
    return ServerCatalog(
        root=None,
        catalog_id=catalog_id,
        catalog_href=catalog_url,
        candidates=candidates,
    )


def _walk_catalog(root: Path, catalog_path: Path, source_catalog: str) -> list[CollectionCandidate]:
    data = _read_json(catalog_path)
    candidates: list[CollectionCandidate] = []
    for link in data.get("links", []):
        if not isinstance(link, dict) or link.get("rel") != "child":
            continue
        href = link.get("href")
        if not isinstance(href, str) or _is_remote_href(href):
            continue
        child_path = (catalog_path.parent / href).resolve()
        if not _inside(child_path, root) or not child_path.exists():
            continue
        if child_path.name == "collection.json":
            candidates.append(_collection_candidate(child_path, source_catalog))
        elif child_path.name == "catalog.json":
            candidates.extend(_walk_catalog(root, child_path, source_catalog))
    return candidates


def _walk_catalog_url(
    catalog_url: str,
    data: dict[str, Any],
    source_catalog: str,
    fetch_json: Callable[[str], dict[str, Any]],
) -> list[CollectionCandidate]:
    candidates: list[CollectionCandidate] = []
    for link in data.get("links", []):
        if not isinstance(link, dict) or link.get("rel") != "child":
            continue
        href = link.get("href")
        if not isinstance(href, str):
            continue
        child_url = urljoin(catalog_url, href)
        child = fetch_json(child_url)
        child_type = child.get("type")
        if child_type == "Collection":
            candidates.append(_collection_candidate_url(child_url, child, source_catalog))
        elif child_type == "Catalog":
            candidates.extend(_walk_catalog_url(child_url, child, source_catalog, fetch_json))
    return candidates


def _collection_candidate(collection_path: Path, source_catalog: str) -> CollectionCandidate:
    collection = _read_json(collection_path)
    collection_id = str(collection["id"])
    asset_key, asset = _primary_asset(collection.get("assets", {}))
    if asset is None:
        return CollectionCandidate(collection=collection_id, resource=None, reason="no data asset")

    media_type = str(asset.get("type") or asset.get("media_type") or "")
    if _is_parquet_asset(asset, media_type) and not _has_geometry(collection):
        return CollectionCandidate(
            collection=collection_id,
            resource=None,
            reason="non-spatial Parquet asset",
        )
    resource_type, resource_format = _classify_asset(collection, asset, media_type)
    if resource_type is None or resource_format is None:
        return CollectionCandidate(
            collection=collection_id,
            resource=None,
            reason=f"unsupported media type: {media_type or 'unknown'}",
        )

    href = str(asset["href"])
    metadata = _resource_metadata(collection, asset)
    resolved_href = _resolve_asset_href(collection_path, href)
    return CollectionCandidate(
        collection=collection_id,
        resource=ServerResourceSpec(
            id=collection_id,
            resource_type=resource_type,
            format=resource_format,
            href=resolved_href,
            title=collection.get("title"),
            description=collection.get("description"),
            extent=collection.get("extent"),
            crs=_crs(collection),
            primary_key=_primary_key(collection),
            native_name=_native_name(asset_key, resolved_href, resource_format),
            metadata=metadata,
            source_catalog=source_catalog,
            source_collection=collection_id,
            source_asset=str(asset_key),
        ),
    )


def _collection_candidate_url(
    collection_url: str, collection: dict[str, Any], source_catalog: str
) -> CollectionCandidate:
    collection_id = str(collection["id"])
    asset_key, asset = _primary_asset(collection.get("assets", {}))
    if asset is None:
        return CollectionCandidate(collection=collection_id, resource=None, reason="no data asset")

    media_type = str(asset.get("type") or asset.get("media_type") or "")
    if _is_parquet_asset(asset, media_type) and not _has_geometry(collection):
        return CollectionCandidate(
            collection=collection_id,
            resource=None,
            reason="non-spatial Parquet asset",
        )
    resource_type, resource_format = _classify_asset(collection, asset, media_type)
    if resource_type is None or resource_format is None:
        return CollectionCandidate(
            collection=collection_id,
            resource=None,
            reason=f"unsupported media type: {media_type or 'unknown'}",
        )

    href = str(asset["href"])
    metadata = _resource_metadata(collection, asset)
    resolved_href = urljoin(collection_url, href)
    return CollectionCandidate(
        collection=collection_id,
        resource=ServerResourceSpec(
            id=collection_id,
            resource_type=resource_type,
            format=resource_format,
            href=resolved_href,
            title=collection.get("title"),
            description=collection.get("description"),
            extent=collection.get("extent"),
            crs=_crs(collection),
            primary_key=_primary_key(collection),
            native_name=_native_name(asset_key, resolved_href, resource_format),
            metadata=metadata,
            source_catalog=source_catalog,
            source_collection=collection_id,
            source_asset=str(asset_key),
        ),
    )


def _primary_asset(assets: Any) -> tuple[str | None, dict[str, Any] | None]:
    if not isinstance(assets, dict):
        return None, None
    data_assets = [
        (key, asset)
        for key, asset in assets.items()
        if isinstance(asset, dict) and "data" in (asset.get("roles") or [])
    ]
    if not data_assets:
        data_assets = [(key, asset) for key, asset in assets.items() if isinstance(asset, dict)]
    if not data_assets:
        return None, None
    return data_assets[0]


def _native_name(asset_key: str | None, href: str, resource_format: ResourceFormat) -> str | None:
    if resource_format == ResourceFormat.GEOPARQUET:
        stem = Path(urlparse(href).path).stem
        if stem:
            return stem
    return str(asset_key) if asset_key is not None else None


def _classify_asset(
    collection: dict[str, Any], asset: dict[str, Any], media_type: str
) -> tuple[ResourceType | None, ResourceFormat | None]:
    href = str(asset.get("href") or "").lower()
    if _is_parquet_asset(asset, media_type):
        return ResourceType.VECTOR, ResourceFormat.GEOPARQUET
    if media_type in _COG_MEDIA_TYPES:
        return ResourceType.RASTER, ResourceFormat.COG
    if media_type in _PMTILES_MEDIA_TYPES or href.endswith(".pmtiles"):
        return ResourceType.TILES, ResourceFormat.PMTILES
    return None, None


def _is_parquet_asset(asset: dict[str, Any], media_type: str) -> bool:
    href = str(asset.get("href") or "").lower()
    return media_type in _GEOPARQUET_MEDIA_TYPES or href.endswith(".parquet")


def _has_geometry(collection: dict[str, Any]) -> bool:
    if isinstance(collection.get("table:primary_geometry"), str):
        return True
    if collection.get("geoparquet:geometry_type") is not None:
        return True
    columns = collection.get("table:columns")
    if not isinstance(columns, list):
        return False
    for column in columns:
        if not isinstance(column, dict):
            continue
        name = column.get("name")
        if name in {"geom", "geometry", "the_geom"}:
            return True
    return False


def _resource_metadata(collection: dict[str, Any], asset: dict[str, Any]) -> dict[str, object]:
    metadata: dict[str, object] = {}
    for key in ("license", "providers", "keywords", "summaries"):
        value = collection.get(key)
        if value is not None:
            metadata[key] = value
    for key in ("file:checksum", "checksum:multihash", "type", "roles"):
        value = asset.get(key)
        if value is not None:
            metadata[key] = value
    return metadata


def _crs(collection: dict[str, Any]) -> str | None:
    summaries = collection.get("summaries")
    if not isinstance(summaries, dict):
        return None
    epsg = summaries.get("proj:epsg")
    if isinstance(epsg, list) and epsg:
        return f"EPSG:{epsg[0]}"
    if isinstance(epsg, int):
        return f"EPSG:{epsg}"
    return None


def _primary_key(collection: dict[str, Any]) -> str | None:
    columns = collection.get("table:columns")
    if not isinstance(columns, list):
        return None
    names: list[str] = []
    for column in columns:
        if not isinstance(column, dict):
            continue
        name = column.get("name")
        if isinstance(name, str):
            names.append(name)
    lowered = {name.lower(): name for name in names}
    for candidate in ("id", "objectid", "_id", "fid", "ogc_fid", "gid"):
        match = lowered.get(candidate)
        if match is not None:
            return match
    return None


def _resolve_asset_href(collection_path: Path, href: str) -> str:
    if _is_remote_href(href):
        return href
    return str((collection_path.parent / href).resolve())


def _is_remote_href(href: str) -> bool:
    parsed = urlparse(href)
    return bool(parsed.scheme and parsed.scheme != "file")


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _read_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def _fetch_json(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"Unsupported catalog URL scheme: {parsed.scheme or 'missing'}")
    request = Request(url, headers={"User-Agent": "portolan-cli/0.8"})
    with urlopen(request, timeout=30) as response:  # nosec B310 - URL scheme checked above.
        data = json.loads(response.read().decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{url} must contain a JSON object")
    return data
