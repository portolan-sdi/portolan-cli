"""Tell an optional derivative from data nothing can rebuild (Issue #735).

``versions.json`` records every file a collection publishes. ``push`` treated
each entry as a hard requirement. One absent file then failed the whole catalog.
That rule fits the user's data. It does not fit a derivative Portolan generates.
One ``portolan add`` on a single COG records two of them. A deleted thumbnail
then broke push on a sound catalog.

The spec supplies the vocabulary. Section Assets in specs/portolan/core.md gives
every asset a ``type`` and at least one role. It also names each artifact here.
Role ``data`` marks the primary GeoParquet, COG or Parquet. Role ``visual`` marks
the PMTiles derivative a client draws. Role ``thumbnail`` marks the preview
image. Role ``style`` marks a MapLibre style file. Role ``collection-mirror``
marks the items.parquet copy. Section Single-File Collections then says a
collection "may optionally carry a ``.pmtiles``, a ``thumbnail.png``, and a
``styles/`` directory". The item mirror in formats.md is a SHOULD and "a derived
Parquet copy". The item JSON stays the normative representation. So an absent
optional derivative is no reason to abort a push. Conformance gaps belong to
``portolan check``.

The role decides. Function :func:`resolve_asset_roles` reads the collection.json
or item.json that registers the href. It returns the roles that object records.
The lookup runs only for an asset that is already absent, so the common path pays
nothing.

Filenames are a fallback for an href no STAC object claims. That happens when the
manifest outlives the metadata. A name cannot replace a role. A publisher may ship
PMTiles as the primary asset. The extension registry gives ``.pmtiles`` the role
``data``, so a name alone would skip a file nothing can rebuild. Each pattern
comes from the module that writes the file.

* ``items.parquet``, from ``stac_parquet.PARQUET_FILENAME``
* ``*.pmtiles``, from ``viz.pmtiles_links.PMTILES_SUFFIX``
* ``*.thumb.*``, from ``viz.thumbnail.is_generated_thumbnail``

An asset with role ``source`` stays a hard failure. The spec says a publisher
"does not need to retain, rehost, or redistribute" the upstream original. Such an
asset carries an absolute upstream href. versions.json tracks catalog-relative
paths, so that asset never reaches this code. What does reach it is a local
original the publisher kept and then lost. Portolan cannot rebuild that file, so
push says so.

Classification never reads a versions.json field. A data asset and a derived
asset carry the same fields there. An item-level asset from ``finalization``
carries no ``source_path``, no ``feature_count`` and no ``schema_fingerprint``.
That is the same shape ``track_generated_assets`` writes. Published beta catalogs
also predate any new flag. Such a flag would keep the hard failure on the
catalogs that hit this bug.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path, PurePosixPath
from typing import Any

from portolan_cli.constants import OPTIONAL_DERIVATIVE_ROLES, ROLE_DEFAULT
from portolan_cli.stac_parquet import PARQUET_FILENAME
from portolan_cli.viz.pmtiles_links import PMTILES_SUFFIX
from portolan_cli.viz.thumbnail import is_generated_thumbnail

logger = logging.getLogger(__name__)

__all__ = ["is_derived_asset", "is_optional_derivative", "resolve_asset_roles"]

_COLLECTION_JSON = "collection.json"


def _candidate_stac_files(catalog_root: Path, asset_path: Path) -> list[Path]:
    """STAC objects that could register ``asset_path``, nearest first.

    An item JSON takes the name of its item directory. The search therefore reads
    every JSON beside the asset. It then walks up to each ``collection.json``,
    because a collection thumbnail may point into an item directory.
    """
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
        # Resolve both sides. The asset is absent, so resolve() cannot verify it,
        # but it still normalizes "..". Resolving one side only would break every
        # catalog reached through a symlink, macOS /var among them.
        candidate = (stac_file.parent / href.removeprefix("./")).resolve()
        if candidate != asset_path.resolve():
            continue
        roles = asset.get("roles")
        return {str(role) for role in roles} if isinstance(roles, list) else set()
    return None


def resolve_asset_roles(catalog_root: Path, href: str) -> set[str]:
    """The roles the owning STAC object records for ``href``.

    Args:
        catalog_root: Path to the catalog root.
        href: Catalog-root-relative asset href from versions.json.

    Returns:
        The roles, or an empty set when no readable STAC object claims the href.
    """
    asset_path = catalog_root / href
    for stac_file in _candidate_stac_files(catalog_root, asset_path):
        roles = _roles_from_stac_file(stac_file, asset_path)
        if roles is not None:
            return roles
    return set()


def is_optional_derivative(catalog_root: Path, href: str) -> bool:
    """True when the catalog can lose ``href`` and stay sound.

    The role decides. An asset is optional when the spec calls every role it
    carries an optional derivative. Role ``default`` does not count, because it
    only qualifies a style. An asset with no role on record falls back to
    :func:`is_derived_asset`.

    Args:
        catalog_root: Path to the catalog root.
        href: Catalog-root-relative asset href from versions.json.

    Returns:
        True for an artifact push may skip, False for data it must not skip.
    """
    decisive = resolve_asset_roles(catalog_root, href) - {ROLE_DEFAULT}
    if decisive:
        return decisive <= OPTIONAL_DERIVATIVE_ROLES
    return is_derived_asset(href)


def is_derived_asset(href: str) -> bool:
    """True when ``href`` carries the name of an artifact Portolan generates.

    The fallback for an href no STAC object claims. Prefer
    :func:`is_optional_derivative`, which asks the role first.

    Args:
        href: Catalog-root-relative asset href from versions.json, POSIX-style,
            for example ``imagery/scene1/scene1.thumb.jpg``.

    Returns:
        True for the item mirror, a PMTiles archive, or a thumbnail we drew.
        False for anything else. The caller then treats it as the user's data.
    """
    name = PurePosixPath(href).name.lower()
    if name == PARQUET_FILENAME:
        return True
    if name.endswith(PMTILES_SUFFIX):
        return True
    return is_generated_thumbnail(name)
