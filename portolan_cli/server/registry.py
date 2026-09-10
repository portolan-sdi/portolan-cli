"""Load Portolan registry exports."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from portolan_cli.server.model import RegistryCatalogEntry
from portolan_cli.server.planner import _fetch_json

DEFAULT_REGISTRY_URL = (
    "https://raw.githubusercontent.com/portolan-sdi/portolan-registry/"
    "refs/heads/main/exports/catalogs.json"
)


def load_registry_entries(
    registry_url: str = DEFAULT_REGISTRY_URL,
    *,
    fetch_json: Callable[[str], dict[str, Any]] | None = None,
    catalog_ids: set[str] | None = None,
    include_stale: bool = False,
    limit: int | None = None,
) -> list[RegistryCatalogEntry]:
    """Load catalog entries from a Portolan registry export."""
    fetch = fetch_json or _fetch_json
    registry = fetch(registry_url)
    entries: list[RegistryCatalogEntry] = []
    for link in registry.get("links", []):
        if not isinstance(link, dict) or link.get("rel") != "child":
            continue
        href = link.get("href")
        registry_id = link.get("portolan_registry:id")
        if not isinstance(href, str) or not isinstance(registry_id, str):
            continue
        status = link.get("portolan_registry:status")
        if status != "valid" and not include_stale:
            continue
        if catalog_ids is not None and registry_id not in catalog_ids:
            continue
        title = link.get("title")
        entries.append(
            RegistryCatalogEntry(
                id=registry_id,
                url=href,
                title=title if isinstance(title, str) else None,
                status=status if isinstance(status, str) else None,
            )
        )
        if limit is not None and len(entries) >= limit:
            break
    return entries


def download_registry_catalog(
    catalog_url: str,
    output_dir: Path,
    *,
    fetch_json: Callable[[str], dict[str, Any]] | None = None,
) -> Path:
    """Download a published catalog snapshot for local server commands."""
    fetch = fetch_json or _fetch_json
    catalog = fetch(catalog_url)
    catalog_id = str(catalog.get("id") or _fallback_catalog_id(catalog_url))
    catalog_root = output_dir / catalog_id
    _write_catalog_tree(catalog_url, catalog, catalog_url, catalog_root, fetch)
    return catalog_root


def _write_catalog_tree(
    document_url: str,
    document: dict[str, Any],
    root_url: str,
    output_root: Path,
    fetch_json: Callable[[str], dict[str, Any]],
) -> None:
    relative_path = _relative_document_path(root_url, document_url)
    target = output_root / relative_path
    target.parent.mkdir(parents=True, exist_ok=True)
    if document.get("type") == "Collection":
        document = _with_absolute_asset_hrefs(document_url, document)
    target.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")

    for link in document.get("links", []):
        if not isinstance(link, dict) or link.get("rel") != "child":
            continue
        href = link.get("href")
        if not isinstance(href, str):
            continue
        child_url = urljoin(document_url, href)
        child = fetch_json(child_url)
        if child.get("type") in {"Catalog", "Collection"}:
            _write_catalog_tree(child_url, child, root_url, output_root, fetch_json)


def _with_absolute_asset_hrefs(document_url: str, collection: dict[str, Any]) -> dict[str, Any]:
    assets = collection.get("assets")
    if not isinstance(assets, dict):
        return collection
    updated = dict(collection)
    updated_assets: dict[str, Any] = {}
    for key, asset in assets.items():
        if not isinstance(asset, dict):
            updated_assets[key] = asset
            continue
        href = asset.get("href")
        if isinstance(href, str):
            rewritten = dict(asset)
            rewritten["href"] = urljoin(document_url, href)
            updated_assets[key] = rewritten
        else:
            updated_assets[key] = asset
    updated["assets"] = updated_assets
    return updated


def _relative_document_path(root_url: str, document_url: str) -> Path:
    root_path = Path(urlparse(root_url).path).parent
    document_path = Path(urlparse(document_url).path)
    try:
        relative = document_path.relative_to(root_path)
    except ValueError:
        return Path(document_path.name or "catalog.json")
    return relative


def _fallback_catalog_id(catalog_url: str) -> str:
    parent = Path(urlparse(catalog_url).path).parent.name
    return parent or "catalog"
