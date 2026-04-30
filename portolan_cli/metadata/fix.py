"""Metadata fix functions.

Provides the fix_metadata orchestration function that applies
fixes for all issues in a MetadataReport:
- Creates missing STAC items
- Updates stale items with fresh metadata
- Handles breaking schema changes
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

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

    Attributes:
        results: List of individual fix results.
        skipped_count: Number of files skipped (already FRESH).
    """

    results: list[FixResult] = field(default_factory=list)
    skipped_count: int = 0

    @property
    def total_count(self) -> int:
        """Total number of files that were fixed (not skipped)."""
        return len(self.results)

    @property
    def success_count(self) -> int:
        """Number of successful fixes."""
        return sum(1 for r in self.results if r.success)

    @property
    def failure_count(self) -> int:
        """Number of failed fixes."""
        return sum(1 for r in self.results if not r.success)

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
    skipped_count = 0

    for check_result in report.results:
        if check_result.status == MetadataStatus.FRESH:
            skipped_count += 1
            continue

        result = _fix_single_file(check_result, directory, dry_run=dry_run)
        fix_results.append(result)

    return FixReport(results=fix_results, skipped_count=skipped_count)


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
            # produced by `add` ({item_dir}/{item_id}.json). Per ADR-0041
            # only this layout is supported — the legacy flat sibling-JSON
            # layout is reported as ORPHANED upstream, never STALE.
            item_path = file_path.parent / f"{file_path.stem}.json"

            # Collection-level assets (ADR-0031) have no companion item.json
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
    correctly in both cases (and across nested-catalog hierarchies per
    ADR-0032).
    """
    if (fallback / "collection.json").exists():
        return fallback
    for ancestor in file_path.resolve().parents:
        if (ancestor / "collection.json").exists():
            return ancestor
        if ancestor == fallback.resolve():
            break
    return fallback
