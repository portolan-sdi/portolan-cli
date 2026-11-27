"""Metadata fix functions."""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.metadata.models import (
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)
from portolan_cli.metadata.update import (
    prepare_missing_item,
    update_item_metadata,
    update_versions_tracking,
)
from portolan_core.constants import LEGACY_MANAGED_FIELD, REMOVED_PORTOLAN_FIELDS
from portolan_core.json_io import write_json_atomic
from portolan_core.stac_links import contained_link_target, visible_stac_files

if TYPE_CHECKING:
    from collections.abc import Iterator

    from portolan_cli.preparation import PreparedItem
    from portolan_core.run_state import RunState


class FixAction(Enum):
    """Type of fix action performed."""

    CREATED = "created"
    UPDATED = "updated"
    SKIPPED = "skipped"


@dataclass
class FixResult:
    """Result from fixing a single file's metadata."""

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
    """Aggregate report of fix results."""

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
    state: RunState | None = None,
) -> FixReport:
    """Apply fixes for all issues in a MetadataReport."""
    fix_results: list[FixResult] = []
    fresh_skipped = 0
    to_finalize: dict[Path, list[tuple[FixResult, PreparedItem]]] = {}

    for check_result in report.results:
        if check_result.status == MetadataStatus.FRESH:
            fresh_skipped += 1
            continue

        if check_result.status == MetadataStatus.MISSING and not dry_run:
            result, prepared = _prepare_missing(check_result, directory, state=state)
            if prepared is not None:
                catalog_root, item = prepared
                to_finalize.setdefault(catalog_root, []).append((result, item))
        else:
            result = _fix_single_file(check_result, directory, dry_run=dry_run, state=state)
        fix_results.append(result)

    for catalog_root, batch in to_finalize.items():
        fix_results.extend(_finalize_created_items(catalog_root, batch, state))

    return FixReport(results=fix_results, fresh_skipped=fresh_skipped)


def _finalize_created_items(
    catalog_root: Path, batch: list[tuple[FixResult, PreparedItem]], state: RunState | None
) -> list[FixResult]:
    """Finalize the prepared MISSING items of one catalog, then write their mirrors."""
    from portolan_cli.finalization import finalize_items
    from portolan_cli.stac_parquet import generate_parquet_mirrors
    from portolan_cli.version_ops import owned_run

    with owned_run(catalog_root, state) as run:
        try:
            finalize_items(catalog_root, [item for _result, item in batch], state=run)
        except Exception as e:
            for result, _item in batch:
                result.success = False
                result.message = f"Failed to fix: {e}"
            return []

        collections = {item.collection_id for _result, item in batch}
        mirrors = generate_parquet_mirrors(
            catalog_root,
            collections,
            generate_parquet=False,
            versioned_collections=collections,
            state=run,
        )
    return [
        FixResult(
            file_path=catalog_root / collection_id / "collection.json",
            action=FixAction.SKIPPED,
            success=True,
            message=f"Cannot generate the mirror: {exc}",
        )
        for collection_id, exc in mirrors.failed.items()
    ]


def repair_titles_and_links(
    catalog_root: Path, *, dry_run: bool = False, state: RunState | None = None
) -> list[FixResult]:
    """Repair catalog titles and link titles."""
    from portolan_cli.catalog import apply_catalog_human_titles, ensure_link_titles
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
        results.extend(_repair_item_titles(catalog_root, stac_file, data, dry_run=dry_run))

    # A title from metadata.yaml wins over the derived one, so this sweep runs after the loop.
    results.extend(
        FixResult(
            file_path=catalog_path,
            action=FixAction.UPDATED,
            success=True,
            message="Applied the metadata.yaml title/description",
        )
        for catalog_path in apply_catalog_human_titles(catalog_root, dry_run=dry_run, state=state)
    )

    # Backfill child/item link titles from their (now-titled) targets.
    if not dry_run:
        ensure_link_titles(catalog_root)

    return results


def _every_stac_object(catalog_root: Path) -> Iterator[Path]:
    """Every published object in the tree: containers, then the items they own."""
    from portolan_core.stac_links import owned_item_hrefs

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
    """Carry ``portolan:managed: false`` over to the ``external`` role before it goes."""
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
    """Remove fields that STAC does not allow."""
    return [
        result
        for path in _every_stac_object(catalog_root)
        if (result := _strip_removed_fields_from_object(path, dry_run=dry_run)) is not None
    ]


def repair_pmtiles_links(catalog_root: Path, *, dry_run: bool = False) -> list[FixResult]:
    """Backfill the ``rel="pmtiles"`` web-map-links link on collections (rashid PTL-VIZ-003)."""
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
    # A hand-edited collection can lack the web-map-links declaration; repair it so check and --fix agree.
    missing_extension = WEB_MAP_LINKS_EXTENSION not in data.get("stac_extensions", [])
    if not missing and not missing_extension:
        return None

    if not dry_run:
        collection_dir = collection_json.parent
        for href in missing:
            # Best effort: a custom --layer name is not recorded, so the file stem is used.
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


def _repair_item_titles(
    catalog_root: Path,
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
        # The repair writes only inside the catalog, after symlinks resolve.
        item_file = contained_link_target(catalog_root, stac_file.parent, href)
        if item_file is None or not item_file.exists():
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
    state: RunState | None = None,
) -> FixResult:
    """Fix metadata for a single file based on its check result."""
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
        if status in (MetadataStatus.STALE, MetadataStatus.BREAKING):
            # Item JSON sits at {item_dir}/{item_id}.json; the flat sibling layout is reported as ORPHANED.
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

            update_item_metadata(item_path, file_path, state=state)

            versions_path = collection_dir / "versions.json"
            if versions_path.exists():
                with contextlib.suppress(KeyError, FileNotFoundError):
                    update_versions_tracking(file_path, versions_path, state=state)

            action_desc = "Updated STAC item"
            if status == MetadataStatus.BREAKING:
                action_desc = "Updated STAC item (breaking schema change)"

            return FixResult(
                file_path=file_path,
                action=FixAction.UPDATED,
                success=True,
                message=action_desc,
            )

        if status == MetadataStatus.ORPHANED:
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

        return FixResult(
            file_path=file_path,
            action=FixAction.SKIPPED,
            success=True,
            message=f"Unknown status: {status}",
        )

    except Exception as e:
        return FixResult(
            file_path=file_path,
            action=FixAction.UPDATED,
            success=False,
            message=f"Failed to fix: {e}",
        )


def _prepare_missing(
    check_result: MetadataCheckResult, directory: Path, *, state: RunState | None = None
) -> tuple[FixResult, tuple[Path, PreparedItem] | None]:
    """Prepare the item of a MISSING file, for one ``finalize_items`` call later."""
    file_path = check_result.file_path
    collection_dir = _resolve_collection_dir(file_path, directory)
    try:
        prepared = prepare_missing_item(file_path, collection_dir, state=state)
    except Exception as e:
        return FixResult(file_path, FixAction.CREATED, False, f"Failed to fix: {e}"), None
    return FixResult(file_path, FixAction.CREATED, True, "Created STAC item"), prepared


def _resolve_collection_dir(file_path: Path, fallback: Path) -> Path:
    """Find the nearest ancestor of `file_path` containing collection.json."""
    if (fallback / "collection.json").exists():
        return fallback
    for ancestor in file_path.resolve().parents:
        if (ancestor / "collection.json").exists():
            return ancestor
        if ancestor == fallback.resolve():
            break
    return fallback
