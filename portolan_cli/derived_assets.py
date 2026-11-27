"""Tell an optional derivative from data nothing can rebuild."""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePosixPath
from typing import Any

from portolan_cli.stac_parquet import PARQUET_FILENAME
from portolan_cli.viz.pmtiles_links import PMTILES_SUFFIX
from portolan_cli.viz.thumbnail import is_generated_thumbnail
from portolan_core.constants import OPTIONAL_DERIVATIVE_ROLES, ROLE_DEFAULT

logger = logging.getLogger(__name__)

__all__ = ["is_derived_asset", "is_optional_derivative", "resolve_asset_roles"]

_COLLECTION_JSON = "collection.json"


def _candidate_stac_files(catalog_root: Path, asset_path: Path) -> list[Path]:
    """STAC objects that could register ``asset_path``, nearest first."""
    candidates: list[Path] = []
    directory = asset_path.parent
    if directory.is_dir():
        candidates.extend(sorted(p for p in directory.glob("*.json") if p.is_file()))
    while True:
        collection_json = directory / _COLLECTION_JSON
        if collection_json.is_file() and collection_json not in candidates:
            candidates.append(collection_json)
        if directory == catalog_root or catalog_root not in directory.parents:
            break
        directory = directory.parent
    return candidates


def _roles_from_stac_file(stac_file: Path, asset_path: Path) -> set[str] | None:
    """Roles ``stac_file`` gives ``asset_path``, or None when it claims no such href."""
    try:
        data: dict[str, Any] = json.loads(stac_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        logger.debug("Cannot read %s: %s", stac_file, exc)
        return None
    assets = data.get("assets")
    if not isinstance(assets, dict):
        return None
    for asset in assets.values():
        if not isinstance(asset, dict):
            continue
        href = str(asset.get("href", ""))
        if not href or "://" in href or href.startswith("/"):
            continue
        # Resolve both sides so a symlinked catalog (macOS /var) still matches.
        candidate = (stac_file.parent / href.removeprefix("./")).resolve()
        if candidate != asset_path.resolve():
            continue
        roles = asset.get("roles")
        return {str(role) for role in roles} if isinstance(roles, list) else set()
    return None


def resolve_asset_roles(catalog_root: Path, href: str) -> set[str]:
    """The roles the owning STAC object records for ``href``."""
    asset_path = catalog_root / href
    for stac_file in _candidate_stac_files(catalog_root, asset_path):
        roles = _roles_from_stac_file(stac_file, asset_path)
        if roles is not None:
            return roles
    return set()


def is_optional_derivative(catalog_root: Path, href: str) -> bool:
    """True when the catalog can lose ``href`` and stay sound."""
    decisive = resolve_asset_roles(catalog_root, href) - {ROLE_DEFAULT}
    if decisive:
        return decisive <= OPTIONAL_DERIVATIVE_ROLES
    return is_derived_asset(href)


def is_derived_asset(href: str) -> bool:
    """True when ``href`` carries the name of an artifact Portolan generates."""
    name = PurePosixPath(href).name.lower()
    if name == PARQUET_FILENAME:
        return True
    if name.endswith(PMTILES_SUFFIX):
        return True
    return is_generated_thumbnail(name)
