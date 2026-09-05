"""Remote object pruning for ``portolan push --prune`` (Issue #753)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import obstore as obs

from portolan_cli.output import detail, info, warn
from portolan_cli.sync.upload import ObjectStore

# Regenerated on every push; never flagged as stale on its own.
_METADATA_BASENAMES = frozenset(
    {"versions.json", "collection.json", "catalog.json", "item.json", "README.md", "AGENTS.md"}
)


@dataclass
class PruneGroup:
    """Remote keys sharing one collection prefix."""

    prefix: str
    keys: list[str]


@dataclass
class PrunePlan:
    """Delete/refuse groups from reconciling remote objects against local state."""

    delete: list[PruneGroup] = field(default_factory=list)
    refuse: list[PruneGroup] = field(default_factory=list)

    @property
    def delete_count(self) -> int:
        """Total remote keys eligible for deletion."""
        return sum(len(g.keys) for g in self.delete)

    @property
    def refuse_count(self) -> int:
        """Total remote keys refused (reported, never deleted)."""
        return sum(len(g.keys) for g in self.refuse)


def _list_remote_keys(store: ObjectStore, prefix: str) -> list[str]:
    """List every object key under a remote prefix."""
    # A trailing "/" stops a bare prefix like "acme" from also matching
    # an unrelated sibling such as "acme-archive".
    list_prefix = f"{prefix}/" if prefix else prefix
    keys: list[str] = []
    for batch in obs.list(store, prefix=list_prefix):
        for meta in batch:
            keys.append(str(meta["path"]))
    return keys


def _remote_collection_prefixes(rel_keys: list[str]) -> set[str]:
    """Return every dir (relative to the remote root) holding a versions.json.

    A bare root versions.json is catalog-level state, not a collection prefix.
    """
    prefixes: set[str] = set()
    for rel_key in rel_keys:
        if rel_key.rsplit("/", 1)[-1] == "versions.json" and "/" in rel_key:
            prefixes.add(rel_key.rsplit("/", 1)[0])
    return prefixes


def _local_structural_dirs(local_collections: list[str]) -> set[str]:
    """Ancestor directories of every local collection, including the catalog root."""
    dirs: set[str] = {""}
    for collection in local_collections:
        parts = collection.split("/")
        for i in range(len(parts)):
            dirs.add("/".join(parts[:i]))
    return dirs


def _matches_prefix(rel_key: str, prefix: str) -> bool:
    """True if ``rel_key`` lives under ``prefix``. The root prefix "" matches everything."""
    return prefix == "" or rel_key == prefix or rel_key.startswith(f"{prefix}/")


def _owning_prefix(rel_key: str, prefixes: set[str]) -> str | None:
    """Return the most specific collection prefix owning ``rel_key``, or None if untracked."""
    matches = [prefix for prefix in prefixes if _matches_prefix(rel_key, prefix)]
    if not matches:
        return None
    return max(matches, key=len)


def _local_asset_hrefs(catalog_root: Path, collection: str) -> set[str]:
    """Return every href tracked by any local version of ``collection``."""
    from portolan_cli.sync.push import _read_local_versions

    try:
        data: dict[str, Any] = _read_local_versions(catalog_root, collection)
    except (FileNotFoundError, ValueError):
        return set()

    hrefs: set[str] = set()
    for version_entry in data.get("versions", []):
        for asset_name, asset_data in version_entry.get("assets", {}).items():
            href = asset_data.get("href", asset_name)
            if href:
                hrefs.add(href)
    return hrefs


def build_prune_plan(
    store: ObjectStore,
    remote_prefix: str,
    catalog_root: Path,
    local_collections: list[str],
) -> PrunePlan:
    """Reconcile remote objects under ``remote_prefix`` against the local catalog."""
    # An empty local catalog is never a legitimate reason to prune everything remote.
    if not local_collections:
        return PrunePlan()

    root = remote_prefix.strip("/")
    remote_keys = _list_remote_keys(store, root)
    rel_keys = [key[len(root) :].lstrip("/") if root else key for key in remote_keys]
    local_set = set(local_collections)
    # A collection still counts as "known" even before its own versions.json has
    # reached the remote (push uploads it last), so it is never treated as orphaned.
    known_prefixes = _remote_collection_prefixes(rel_keys) | local_set
    structural_dirs = _local_structural_dirs(local_collections)

    delete_by_prefix: dict[str, list[str]] = {}
    refuse_by_prefix: dict[str, list[str]] = {}
    hrefs_by_collection: dict[str, set[str]] = {}

    for key, rel_key in zip(remote_keys, rel_keys, strict=True):
        owner = _owning_prefix(rel_key, known_prefixes)

        if owner is None:
            basename = rel_key.rsplit("/", 1)[-1]
            directory = rel_key.rsplit("/", 1)[0] if "/" in rel_key else ""
            # Root/subcatalog metadata is regenerated by every push, not an orphaned collection.
            if basename in _METADATA_BASENAMES and directory in structural_dirs:
                continue
            delete_by_prefix.setdefault("(unrecognized)", []).append(key)
            continue

        # Prefix unused locally: safe to delete (Issue #753's safety rule).
        if owner not in local_set:
            delete_by_prefix.setdefault(owner, []).append(key)
            continue

        # hrefs in versions.json are catalog-root-relative, same as rel_key.
        basename = rel_key.rsplit("/", 1)[-1]
        if basename in _METADATA_BASENAMES:
            continue
        if owner not in hrefs_by_collection:
            hrefs_by_collection[owner] = _local_asset_hrefs(catalog_root, owner)
        if rel_key in hrefs_by_collection[owner]:
            continue
        refuse_by_prefix.setdefault(owner, []).append(key)

    plan = PrunePlan()
    plan.delete = [
        PruneGroup(prefix=p, keys=sorted(k)) for p, k in sorted(delete_by_prefix.items())
    ]
    plan.refuse = [
        PruneGroup(prefix=p, keys=sorted(k)) for p, k in sorted(refuse_by_prefix.items())
    ]
    return plan


def print_prune_plan(plan: PrunePlan) -> None:
    """Print a PrunePlan grouped by prefix."""
    if not plan.delete and not plan.refuse:
        info("Nothing to prune: remote matches the local publication.")
        return

    if plan.delete:
        warn(f"Would delete {plan.delete_count} remote object(s) under unused prefix(es):")
        for group in plan.delete:
            detail(f"  {group.prefix}/ ({len(group.keys)} object(s))")

    if plan.refuse:
        warn(
            f"Refusing to delete {plan.refuse_count} remote object(s) under "
            "prefix(es) still used locally (possible incomplete local build):"
        )
        for group in plan.refuse:
            detail(f"  {group.prefix}/ ({len(group.keys)} object(s))")


def delete_prune_plan(store: ObjectStore, plan: PrunePlan) -> tuple[int, list[str]]:
    """Delete every key in ``plan.delete``. Never touches ``plan.refuse``.

    Returns the number of keys deleted and any per-key error messages; the
    caller decides how to surface those (styled text vs. a JSON envelope).
    """
    deleted = 0
    errors: list[str] = []
    for group in plan.delete:
        for key in group.keys:
            try:
                obs.delete(store, key)
                deleted += 1
            except Exception as e:
                errors.append(f"Failed to delete {key}: {e}")
    return deleted, errors
