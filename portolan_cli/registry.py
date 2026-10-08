"""Read and fetch Portolan registry exports."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin, urlparse

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

DEFAULT_REGISTRY_URL = (
    "https://raw.githubusercontent.com/portolan-sdi/portolan-registry/"
    "refs/heads/main/exports/catalogs.json"
)


@dataclass(frozen=True)
class RegistryCatalogEntry:
    """One catalog entry from a Portolan registry export."""

    id: str
    url: str
    title: str | None = None
    status: str | None = None


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
        resolved_url = urljoin(registry_url, href)
        if not _is_remote_url(resolved_url):
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
                url=resolved_url,
                title=title if isinstance(title, str) else None,
                status=status if isinstance(status, str) else None,
            )
        )
        if limit is not None and len(entries) >= limit:
            break
    return entries


def _is_remote_url(url: str) -> bool:
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def download_registry_catalog(
    catalog_url: str,
    output_dir: Path,
    *,
    expected_catalog_id: str | None = None,
    fetch_json: Callable[[str], dict[str, Any]] | None = None,
) -> Path:
    """Download a published catalog snapshot for local workflows."""
    fetch = fetch_json or _fetch_json
    catalog = fetch(catalog_url)
    catalog_id = str(catalog.get("id") or _fallback_catalog_id(catalog_url))
    _validate_catalog_id(catalog_id)
    if expected_catalog_id is not None:
        _validate_catalog_id(expected_catalog_id)
        if catalog_id != expected_catalog_id:
            raise ValueError(
                f"Catalog id '{catalog_id}' does not match registry id '{expected_catalog_id}'"
            )
    catalog_root = output_dir / catalog_id
    output_dir.mkdir(parents=True, exist_ok=True)
    with _catalog_lock(output_dir, catalog_id):
        _validate_catalog_root(output_dir, catalog_root)
        staging_root = Path(
            tempfile.mkdtemp(prefix=f".{catalog_id}.staging-", dir=output_dir.resolve())
        )
        try:
            _write_catalog_tree(catalog_url, catalog, catalog_url, staging_root, fetch)
            _publish_snapshot(staging_root, catalog_root, output_dir)
        except BaseException:
            shutil.rmtree(staging_root, ignore_errors=True)
            raise
    return catalog_root


def _fetch_json(url: str) -> dict[str, Any]:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError(f"Registry URL must use HTTP or HTTPS: {url}")
    with httpx.Client(
        headers={"User-Agent": "portolan-cli"},
        follow_redirects=True,
        timeout=30,
        event_hooks={"request": [_same_origin_request_validator(url)]},
    ) as client:
        response = client.get(url)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        raise TypeError(f"Expected JSON object from {url}")
    return data


def _same_origin_request_validator(original_url: str) -> Callable[[httpx.Request], None]:
    origin = _url_origin(original_url)

    def validate(request: httpx.Request) -> None:
        request_url = str(request.url)
        if _url_origin(request_url) != origin:
            raise ValueError(f"Redirect changed origin: {request_url}")

    return validate


def _url_origin(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    return parsed.scheme.lower(), parsed.netloc.lower()


def _validate_catalog_id(catalog_id: str) -> None:
    if catalog_id in {"", ".", ".."} or Path(catalog_id).name != catalog_id or "\\" in catalog_id:
        raise ValueError(f"Catalog id must be a safe directory name: {catalog_id}")


@contextmanager
def _catalog_lock(output_dir: Path, catalog_id: str) -> Iterator[None]:
    lock_path = output_dir.resolve() / f".{catalog_id}.lock"
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as err:
        raise RuntimeError(f"Catalog download is already in progress: {catalog_id}") from err
    os.close(descriptor)
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def _validate_catalog_root(output_dir: Path, catalog_root: Path) -> None:
    if catalog_root.is_symlink():
        raise ValueError(f"Catalog directory must not be a symlink: {catalog_root}")
    if not catalog_root.resolve().is_relative_to(output_dir.resolve()):
        raise ValueError(f"Catalog directory escapes output directory: {catalog_root}")


def _publish_snapshot(staging_root: Path, catalog_root: Path, output_dir: Path) -> None:
    _validate_catalog_root(output_dir, catalog_root)
    backup_root = Path(
        tempfile.mkdtemp(prefix=f".{catalog_root.name}.backup-", dir=output_dir.resolve())
    )
    backup_root.rmdir()
    had_previous = catalog_root.exists()
    if had_previous:
        catalog_root.rename(backup_root)
    try:
        staging_root.rename(catalog_root)
    except BaseException:
        if had_previous:
            backup_root.rename(catalog_root)
        raise
    if had_previous:
        shutil.rmtree(backup_root)


def _write_catalog_tree(
    document_url: str,
    document: dict[str, Any],
    root_url: str,
    output_root: Path,
    fetch_json: Callable[[str], dict[str, Any]],
    visited: set[str] | None = None,
    targets: dict[Path, str] | None = None,
) -> None:
    if visited is None:
        visited = set()
    if targets is None:
        targets = {}
    visited.add(document_url)

    target = _target_document_path(root_url, document_url, output_root)
    owner = targets.get(target)
    if owner is not None and owner != document_url:
        raise ValueError(f"Registry documents map to the same local path: {owner}, {document_url}")
    targets[target] = document_url
    target.parent.mkdir(parents=True, exist_ok=True)
    if document.get("type") == "Collection":
        document = _with_absolute_asset_hrefs(document_url, document)

    links = document.get("links")
    if isinstance(links, list):
        rewritten_links: list[Any] = []
        for link in links:
            rewritten_link = link
            if isinstance(link, dict) and isinstance(link.get("href"), str):
                href = link["href"]
                linked_url = urljoin(document_url, href)
                rewritten_link = {**link, "href": linked_url}
                if link.get("rel") in {"root", "parent"}:
                    linked_target = next(
                        (path for path, owner_url in targets.items() if owner_url == linked_url),
                        None,
                    )
                    if linked_target is not None:
                        rewritten_link = {
                            **link,
                            "href": _relative_local_href(target, linked_target),
                        }
                if link.get("rel") == "child":
                    child_url = linked_url
                    child_target = _target_document_path(root_url, child_url, output_root)
                    owner = targets.get(child_target)
                    if owner is not None and owner != child_url:
                        raise ValueError(
                            f"Registry documents map to the same local path: {owner}, {child_url}"
                        )
                    if child_url in visited:
                        if owner == child_url:
                            rewritten_link = {
                                **link,
                                "href": _relative_local_href(target, child_target),
                            }
                    else:
                        child = fetch_json(child_url)
                        if child.get("type") in {"Catalog", "Collection"}:
                            _write_catalog_tree(
                                child_url,
                                child,
                                root_url,
                                output_root,
                                fetch_json,
                                visited,
                                targets,
                            )
                            rewritten_link = {
                                **link,
                                "href": _relative_local_href(target, child_target),
                            }
            rewritten_links.append(rewritten_link)
        document = {**document, "links": rewritten_links}

    target.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")


def _relative_local_href(parent_target: Path, child_target: Path) -> str:
    relative_path = os.path.relpath(child_target, start=parent_target.parent)
    return Path(relative_path).as_posix()


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
    root = urlparse(root_url)
    document = urlparse(document_url)
    if (document.scheme.lower(), document.netloc.lower()) != (
        root.scheme.lower(),
        root.netloc.lower(),
    ):
        raise ValueError(f"Child document has a different origin: {document_url}")
    root_path = Path(root.path).parent
    document_path = Path(document.path)
    try:
        relative_path = document_path.relative_to(root_path)
    except ValueError as err:
        raise ValueError(f"Child document is outside catalog root: {document_url}") from err
    if ".." in relative_path.parts:
        raise ValueError(f"Child document is outside catalog root: {document_url}")
    return relative_path


def _target_document_path(root_url: str, document_url: str, output_root: Path) -> Path:
    relative_path = _relative_document_path(root_url, document_url)
    resolved_root = output_root.resolve()
    target = (output_root / relative_path).resolve()
    if not target.is_relative_to(resolved_root):
        raise ValueError(f"Child document escapes catalog root: {document_url}")
    return target


def _fallback_catalog_id(catalog_url: str) -> str:
    parent = Path(urlparse(catalog_url).path).parent.name
    return parent or "catalog"
