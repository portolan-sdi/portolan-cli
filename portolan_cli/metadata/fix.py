"""Metadata fix functions.

Provides the fix_metadata orchestration function that applies
fixes for all issues in a MetadataReport:
- Creates missing STAC items
- Updates stale items with fresh metadata
- Handles breaking schema changes

Plus the catalog-wide repairs `check --fix` runs that no rashid rule reports,
including :func:`strip_removed_fields`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from portolan_cli.agents_md import visible_stac_files
from portolan_cli.constants import LEGACY_MANAGED_FIELD, REMOVED_PORTOLAN_FIELDS
from portolan_cli.json_io import write_json_atomic
from portolan_cli.metadata.models import (
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)
from portolan_cli.metadata.update import (
    create_missing_item,
    update_item_metadata,
    update_versions_tracking,
)


class FixAction(Enum):
    """Type of fix action performed.

    Attributes:
        CREATED: New STAC item was created.
        UPDATED: Existing STAC item was updated.
        SKIPPED: No action needed (file was FRESH).
    """

    CREATED = "created"
    UPDATED = "updated"
    SKIPPED = "skipped"


@dataclass
class FixResult:
    """Result from fixing a single file's metadata.

    Attributes:
        file_path: Path to the fixed file.
        action: Type of fix action performed.
        success: Whether the fix succeeded.
        message: Description of what was done or error message.
    """

    file_path: Path
    action: FixAction
    success: bool
    message: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "file_path": str(self.file_path),
            "action": self.action.value,
            "success": self.success,
            "message": self.message,
        }


@dataclass
class FixReport:
    """Aggregate report of fix results.

    A SKIPPED result is not a fix: an ORPHANED file or a collection-level asset
    is *reported* with an explanation and left alone. Counting it as a fix made
    ``check --fix`` claim work it had not done, so the counts below derive from
    each result's action rather than from the length of the list.

    Attributes:
        results: List of individual fix results.
        fresh_skipped: Number of files skipped before any fix was attempted
            (already FRESH, so no FixResult was produced for them).
    """

    results: list[FixResult] = field(default_factory=list)
    fresh_skipped: int = 0

    @property
    def _fixes(self) -> list[FixResult]:
        """Results that actually changed something (CREATED or UPDATED)."""
        return [r for r in self.results if r.action in (FixAction.CREATED, FixAction.UPDATED)]

    @property
    def total_count(self) -> int:
        """Total number of files that were fixed (not skipped)."""
        return len(self._fixes)

    @property
    def success_count(self) -> int:
        """Number of successful fixes."""
        return sum(1 for r in self._fixes if r.success)

    @property
    def failure_count(self) -> int:
        """Number of failed fixes."""
        return sum(1 for r in self.results if not r.success)

    @property
    def skipped_count(self) -> int:
        """Files left alone: FRESH ones plus every SKIPPED result."""
        return self.fresh_skipped + sum(1 for r in self.results if r.action is FixAction.SKIPPED)

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
        return {
            "total_count": self.total_count,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "skipped_count": self.skipped_count,
            "results": [r.to_dict() for r in self.results],
        }


def fix_metadata(
    directory: Path,
    report: MetadataReport,
    *,
    dry_run: bool = False,
) -> FixReport:
    """Apply fixes for all issues in a MetadataReport.

    For each non-FRESH result in the report:
    - MISSING: Create a new STAC item
    - STALE: Update the existing STAC item
    - BREAKING: Update the item (same as STALE, but logged differently)

    Args:
        directory: Root directory of the catalog/collection.
        report: MetadataReport with check results.
        dry_run: If True, don't actually make changes.

    Returns:
        FixReport with results of all fix operations.
    """
    fix_results: list[FixResult] = []
    fresh_skipped = 0

    for check_result in report.results:
        if check_result.status == MetadataStatus.FRESH:
            fresh_skipped += 1
            continue

        result = _fix_single_file(check_result, directory, dry_run=dry_run)
        fix_results.append(result)

    return FixReport(results=fix_results, fresh_skipped=fresh_skipped)


def repair_titles_and_links(catalog_root: Path, *, dry_run: bool = False) -> list[FixResult]:
    """Populate human-readable titles/descriptions and link titles (Issue #502).

    Repairs what rashid's mandatory-title rules (PTL-TTL-001/-002/-003) flag:

    - every catalog/collection gets a human-readable title (derived from its id
      when missing or technical) and a description (defaulting to the title);
    - every item referenced by an item link gets a title in its properties;
    - every ``child``/``item`` link gets its target's title backfilled.

    Existing human-authored titles/descriptions are preserved.

    Args:
        catalog_root: Root directory of the catalog.
        dry_run: If True, report what would change without writing.

    Returns:
        FixResults for each file that was (or would be) modified.
    """
    from portolan_cli.catalog import ensure_link_titles
    from portolan_cli.humanize import derive_title

    results: list[FixResult] = []

    stac_files = visible_stac_files(catalog_root)
    for stac_file in stac_files:
        try:
            data = json.loads(stac_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue

        obj_id = str(data.get("id") or stac_file.parent.name)
        new_title = derive_title(data.get("title"), obj_id)

        changed = False
        if data.get("title") != new_title:
            if not dry_run:
                data["title"] = new_title
            changed = True

        description = data.get("description")
        if not isinstance(description, str) or not description.strip():
            if not dry_run:
                data["description"] = new_title
            changed = True

        if changed:
            if not dry_run:
                write_json_atomic(stac_file, data)
            results.append(
                FixResult(
                    file_path=stac_file,
                    action=FixAction.UPDATED,
                    success=True,
                    message="Set human-readable title/description",
                )
            )

        # Repair item titles referenced by this collection's item links so the
        # link-title backfill below has a title to copy.
        results.extend(_repair_item_titles(stac_file, data, dry_run=dry_run))

    # Backfill child/item link titles from their (now-titled) targets.
    if not dry_run:
        ensure_link_titles(catalog_root)

    return results


def _every_stac_object(catalog_root: Path) -> Iterator[Path]:
    """Every published object in the tree: containers, then the items they own.

    Items are reached through each collection's ``rel="item"`` links rather than
    by globbing, because Portolan names an item file after the item
    (``scene-001/scene-001.json``), so no filename pattern finds them all.
    Yielded once each: a collection and an organizing catalog beneath it can
    both link the same item. A collection that will not parse yields no items;
    the sweep reports nothing about it rather than raising over a file some
    other check already flags.
    """
    from portolan_cli.stac_parquet import owned_item_hrefs

    seen: set[Path] = set()
    for path in visible_stac_files(catalog_root):
        if path not in seen:
            seen.add(path)
            yield path
        if path.name != "collection.json":
            continue
        try:
            owned = owned_item_hrefs(path)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        for _href, item_path in owned:
            if item_path not in seen:
                seen.add(item_path)
                yield item_path


def _strip_from_mapping(container: Any) -> bool:
    """Delete every removed field from one JSON object, reporting whether any went."""
    if not isinstance(container, dict):
        return False
    present = [field for field in REMOVED_PORTOLAN_FIELDS if field in container]
    for field_name in present:
        del container[field_name]
    return bool(present)


def _migrate_managed_marker(asset: dict[str, Any]) -> None:
    """Carry ``portolan:managed: false`` over to the ``external`` role before it goes.

    The marker's whole job was telling a reader that Portolan does not own the
    bytes behind the href. The ``external`` role says the same thing in
    spec-defined terms, and ``add-external`` has always written both, so this
    only fires for an asset some other path produced. Read once, never written
    back (issue #654).
    """
    from portolan_cli.external import EXTERNAL_ROLE

    if asset.get(LEGACY_MANAGED_FIELD) is not False:
        return
    roles = asset.get("roles")
    if not isinstance(roles, list):
        asset["roles"] = [EXTERNAL_ROLE]
    elif EXTERNAL_ROLE not in roles:
        roles.append(EXTERNAL_ROLE)


def _strip_removed_fields_from_object(path: Path, *, dry_run: bool) -> FixResult | None:
    """Strip every removed field from one STAC object, or return None if it had none."""
    data = _read_json_object(path)
    if data is None:
        return None

    assets = data.get("assets")
    if isinstance(assets, dict):
        for asset in assets.values():
            if isinstance(asset, dict):
                _migrate_managed_marker(asset)

    changed = _strip_from_mapping(data)
    changed |= _strip_from_mapping(data.get("properties"))
    if isinstance(assets, dict):
        for asset in assets.values():
            changed |= _strip_from_mapping(asset)

    if not changed:
        return None
    if not dry_run:
        write_json_atomic(path, data)
    return FixResult(
        file_path=path,
        action=FixAction.UPDATED,
        success=True,
        message="Removed the portolan: fields the spec does not define",
    )


def _read_json_object(path: Path) -> dict[str, Any] | None:
    """Parse a STAC file, or None when it is unreadable or not an object."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    return data if isinstance(data, dict) else None


def strip_removed_fields(catalog_root: Path, *, dry_run: bool = False) -> list[FixResult]:
    """Delete the ``portolan:`` fields no spec version defines (issue #654).

    Runs from the fix workflow rather than from the fixer registry, for the same
    reason item freshness does: no rashid rule reports these. A rule fires on a
    requirement an object fails, and no requirement names a field the spec does
    not have, so there is no finding to dispatch on and the sweep has to run on
    its own.

    Sweeps catalogs, collections, and items, and strips each field from all
    three places one was ever written: the object, an item's ``properties``, and
    an asset. ``portolan:managed`` is read before it is deleted, so an external
    asset keeps the recognition the marker used to provide.

    Args:
        catalog_root: Root directory of the catalog.
        dry_run: If True, report what would change without writing.

    Returns:
        One FixResult per object that carried at least one removed field.
    """
    return [
        result
        for path in _every_stac_object(catalog_root)
        if (result := _strip_removed_fields_from_object(path, dry_run=dry_run)) is not None
    ]


def repair_pmtiles_links(catalog_root: Path, *, dry_run: bool = False) -> list[FixResult]:
    """Backfill the ``rel="pmtiles"`` web-map-links link on collections (rashid PTL-VIZ-003).

    Repairs what rashid PTL-VIZ-003 flags: a
    collection that registers a PMTiles asset but does not emit a collection-level
    ``rel="pmtiles"`` link. For each PMTiles asset lacking a matching link, the link
    is added (with the web-map-links extension declared and ``pmtiles:layers`` set
    from the asset filename). Collections whose links are already present are left
    untouched.

    Args:
        catalog_root: Root directory of the catalog.
        dry_run: If True, report what would change without writing.

    Returns:
        FixResults for each collection that was (or would be) modified.
    """
    results: list[FixResult] = []

    for collection_json in sorted(catalog_root.rglob("collection.json")):
        rel_parts = collection_json.parent.relative_to(catalog_root).parts
        if any(part.startswith(".") for part in rel_parts):
            continue
        result = _repair_pmtiles_collection(collection_json, dry_run=dry_run)
        if result is not None:
            results.append(result)

    return results


def _repair_pmtiles_collection(collection_json: Path, *, dry_run: bool) -> FixResult | None:
    """Backfill PMTiles links / extension for one collection (see repair_pmtiles_links)."""
    from portolan_cli.viz.pmtiles import (
        add_pmtiles_link_to_collection,
        ensure_web_map_links_extension,
    )
    from portolan_cli.viz.pmtiles_links import (
        WEB_MAP_LINKS_EXTENSION,
        pmtiles_asset_hrefs,
        pmtiles_link_hrefs,
    )

    try:
        data = json.loads(collection_json.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None

    pmtiles_hrefs = pmtiles_asset_hrefs(data.get("assets", {}))
    if not pmtiles_hrefs:
        return None

    linked_hrefs = pmtiles_link_hrefs(data.get("links", []))
    missing = [href for href in pmtiles_hrefs if href not in linked_hrefs]
    # A collection may have all its links yet still lack the web-map-links
    # extension declaration (e.g. a hand-edited collection.json). The PMTiles-link rule
    # flags that too, so repair it here to keep check and --fix in agreement.
    missing_extension = WEB_MAP_LINKS_EXTENSION not in data.get("stac_extensions", [])
    if not missing and not missing_extension:
        return None

    if not dry_run:
        collection_dir = collection_json.parent
        for href in missing:
            # Best-effort: the layer name is derived from the PMTiles file
            # stem. If the tiles were generated with a custom ``--layer``
            # override differing from the file name, the override is not
            # recorded anywhere the backfill can read, so pmtiles:layers may
            # not match the actual layer name inside the PMTiles. The
            # generate path (pmtiles.generate_pmtiles_for_collection) honors
            # the override; this fallback cannot.
            add_pmtiles_link_to_collection(collection_dir, href, layers=[Path(href).stem])
        if missing_extension and not missing:
            # Links are complete but the extension declaration is absent;
            # declare it without touching links (preserves custom layers).
            ensure_web_map_links_extension(collection_dir)

    message = (
        "Added rel='pmtiles' web-map-links link" if missing else "Declared web-map-links extension"
    )
    return FixResult(
        file_path=collection_json,
        action=FixAction.UPDATED,
        success=True,
        message=message,
    )


def repair_agents_md(catalog_root: Path, *, dry_run: bool = False) -> list[FixResult]:
    """Backfill missing ``AGENTS.md`` files and ``rel="agents"`` links (rashid PTL-FIL-001/-002).

    Repairs what rashid PTL-FIL-001 (required files) and PTL-FIL-002
    (``rel="agents"`` link) flag: a
    catalog or collection missing its ``AGENTS.md`` file or the link that
    references it. For each affected STAC object the file is scaffolded (never
    overwriting an existing, human-authored one) and the link is added or
    normalized. Compliant objects are left untouched.

    Args:
        catalog_root: Root directory of the catalog.
        dry_run: If True, report what would change without writing.

    Returns:
        FixResults for each catalog/collection that was (or would be) modified.
    """
    from portolan_cli.agents_md import agents_md_gap, ensure_agents_md

    results: list[FixResult] = []

    for stac_json in visible_stac_files(catalog_root):
        if agents_md_gap(stac_json) is None:
            continue
        if not dry_run:
            ensure_agents_md(stac_json)
        results.append(
            FixResult(
                file_path=stac_json,
                action=FixAction.UPDATED,
                success=True,
                message="Scaffolded AGENTS.md and added rel='agents' link",
            )
        )

    return results


def _repair_item_titles(
    stac_file: Path,
    data: dict[str, Any],
    *,
    dry_run: bool,
) -> list[FixResult]:
    """Ensure items referenced by ``item`` links have a human-readable title."""
    from portolan_cli.humanize import derive_title

    results: list[FixResult] = []
    links = data.get("links", [])
    if not isinstance(links, list):
        return results

    for link in links:
        if not isinstance(link, dict) or link.get("rel") != "item":
            continue
        href = link.get("href")
        if not isinstance(href, str) or not href:
            continue
        item_file = (stac_file.parent / href).resolve()
        if not item_file.exists():
            continue
        try:
            item_data = json.loads(item_file.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue

        properties = item_data.setdefault("properties", {})
        if not isinstance(properties, dict):
            continue
        item_id = str(item_data.get("id") or item_file.stem)
        new_title = derive_title(properties.get("title"), item_id)
        if properties.get("title") != new_title:
            if not dry_run:
                properties["title"] = new_title
                write_json_atomic(item_file, item_data)
            results.append(
                FixResult(
                    file_path=item_file,
                    action=FixAction.UPDATED,
                    success=True,
                    message="Set human-readable item title",
                )
            )

    return results


def _fix_single_file(
    check_result: MetadataCheckResult,
    directory: Path,
    *,
    dry_run: bool = False,
) -> FixResult:
    """Fix metadata for a single file based on its check result.

    Args:
        check_result: The check result indicating what needs fixing.
        directory: Root directory for context.
        dry_run: If True, don't actually make changes.

    Returns:
        FixResult describing what was done.
    """
    file_path = check_result.file_path
    status = check_result.status

    if dry_run:
        # Determine action the same way as real execution for consistency
        if status == MetadataStatus.MISSING:
            action = FixAction.CREATED
        elif status in (MetadataStatus.STALE, MetadataStatus.BREAKING):
            action = FixAction.UPDATED
        else:
            action = FixAction.SKIPPED
        if status == MetadataStatus.ORPHANED:
            message = (
                "Cannot auto-fix orphan: register in collection.json/item.json "
                "(e.g., via 'portolan add'), or delete the file"
            )
        else:
            message = f"Would {action.value} item (dry run)"
        return FixResult(
            file_path=file_path,
            action=action,
            success=True,
            message=message,
        )

    collection_dir = _resolve_collection_dir(file_path, directory)

    try:
        if status == MetadataStatus.MISSING:
            create_missing_item(file_path, collection_dir)
            return FixResult(
                file_path=file_path,
                action=FixAction.CREATED,
                success=True,
                message="Created STAC item",
            )

        elif status in (MetadataStatus.STALE, MetadataStatus.BREAKING):
            # Item.json sits next to the data file in the hierarchical layout
            # produced by `add` ({item_dir}/{item_id}.json). Per
            # only this layout is supported — the legacy flat sibling-JSON
            # layout is reported as ORPHANED upstream, never STALE.
            item_path = file_path.parent / f"{file_path.stem}.json"

            # Collection-level assets have no companion item.json
            # by design; they are regenerated by re-running `portolan add`.
            if file_path.parent == collection_dir:
                return FixResult(
                    file_path=file_path,
                    action=FixAction.SKIPPED,
                    success=True,
                    message=(
                        "Cannot auto-fix collection-level asset: "
                        "re-run 'portolan add' to refresh it"
                    ),
                )

            update_item_metadata(item_path, file_path)

            versions_path = collection_dir / "versions.json"
            if versions_path.exists():
                try:
                    update_versions_tracking(file_path, versions_path)
                except (KeyError, FileNotFoundError):
                    pass

            action_desc = "Updated STAC item"
            if status == MetadataStatus.BREAKING:
                action_desc = "Updated STAC item (breaking schema change)"

            return FixResult(
                file_path=file_path,
                action=FixAction.UPDATED,
                success=True,
                message=action_desc,
            )

        elif status == MetadataStatus.ORPHANED:
            return FixResult(
                file_path=file_path,
                action=FixAction.SKIPPED,
                success=True,
                message=(
                    "Cannot auto-fix orphan: register in "
                    "collection.json/item.json (e.g., via 'portolan add'), "
                    "or delete the file"
                ),
            )

        else:
            return FixResult(
                file_path=file_path,
                action=FixAction.SKIPPED,
                success=True,
                message=f"Unknown status: {status}",
            )

    except Exception as e:
        action = FixAction.CREATED if status == MetadataStatus.MISSING else FixAction.UPDATED
        return FixResult(
            file_path=file_path,
            action=action,
            success=False,
            message=f"Failed to fix: {e}",
        )


def _resolve_collection_dir(file_path: Path, fallback: Path) -> Path:
    """Find the nearest ancestor of `file_path` containing collection.json.

    Callers may pass either a collection directory or a catalog root as
    `fallback`. Walking up from the data file lets fix_metadata work
    correctly in both cases (and across nested-catalog hierarchies per.
    """
    if (fallback / "collection.json").exists():
        return fallback
    for ancestor in file_path.resolve().parents:
        if (ancestor / "collection.json").exists():
            return ancestor
        if ancestor == fallback.resolve():
            break
    return fallback
