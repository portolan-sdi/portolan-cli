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
        return FixResult(
            file_path=file_path,
            action=action,
            success=True,
            message=f"Would {action.value} item (dry run)",
        )

    try:
        if status == MetadataStatus.MISSING:
            # Create new STAC item
            create_missing_item(file_path, directory)
            return FixResult(
                file_path=file_path,
                action=FixAction.CREATED,
                success=True,
                message="Created STAC item",
            )

        elif status in (MetadataStatus.STALE, MetadataStatus.BREAKING):
            # Update existing item
            item_path = file_path.with_suffix(".json")
            update_item_metadata(item_path, file_path)

            # Also update versions.json tracking if it exists
            versions_path = directory / "versions.json"
            if versions_path.exists():
                try:
                    update_versions_tracking(file_path, versions_path)
                except (KeyError, FileNotFoundError):
                    # Asset not in versions.json yet - skip tracking update
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
