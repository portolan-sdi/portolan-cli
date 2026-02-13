"""Fix mode implementations for portolan scan.

This module provides fix operations for detected issues:
- Safe fixes (--fix): Rename files, no data loss possible
- Unsafe fixes (--fix --unsafe-fix): Move/restructure files
- Dry-run mode (--dry-run): Preview fixes without applying

Fix Categories:
- SAFE: Rename invalid characters, transliterate non-ASCII
- UNSAFE: Split directories, move files to resolve duplicates
- MANUAL: Cannot be auto-fixed, requires user action

Functions:
    apply_safe_fixes: Apply safe fixes to scan result.
    apply_unsafe_fixes: Apply unsafe fixes to scan result.
    preview_fix: Generate a fix proposal for an issue.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.scan import ScanIssue


class FixCategory(Enum):
    """Risk categories for automatic fixes."""

    SAFE = "safe"
    UNSAFE = "unsafe"
    MANUAL = "manual"


@dataclass(frozen=True)
class ProposedFix:
    """A proposed fix for a detected issue."""

    issue: ScanIssue
    category: FixCategory
    action: str  # "rename", "move", "split", "delete"
    details: dict[str, Any]
    preview: str  # Human-readable preview

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "issue_path": str(self.issue.path),
            "category": self.category.value,
            "action": self.action,
            "details": self.details,
            "preview": self.preview,
        }


def _compute_safe_rename(path: Path) -> tuple[Path, str] | None:
    """Compute safe rename for a file with invalid characters.

    Args:
        path: Path to the file with invalid characters.

    Returns:
        Tuple of (new_path, preview_message) or None if no rename needed.
    """
    # TODO: Implement safe rename computation
    raise NotImplementedError("_compute_safe_rename not yet implemented")


def apply_safe_fixes(
    issues: list[ScanIssue],
    dry_run: bool = False,
) -> tuple[list[ProposedFix], list[ProposedFix]]:
    """Apply safe fixes to detected issues.

    Args:
        issues: List of scan issues to fix.
        dry_run: If True, compute fixes but don't apply them.

    Returns:
        Tuple of (proposed_fixes, applied_fixes).
        If dry_run=True, applied_fixes is empty.
    """
    # TODO: Implement safe fix application
    raise NotImplementedError("apply_safe_fixes not yet implemented")


def _compute_unsafe_split(
    dir_path: Path,
    files: list[Path],
) -> tuple[dict[str, Any], str] | None:
    """Compute unsafe directory split for multiple primaries.

    Args:
        dir_path: Directory containing multiple primaries.
        files: List of primary files to split.

    Returns:
        Tuple of (details_dict, preview_message) or None if no split needed.
    """
    # TODO: Implement unsafe split computation
    raise NotImplementedError("_compute_unsafe_split not yet implemented")


def _compute_unsafe_rename(
    path: Path,
    _new_name: str,  # underscore prefix to mark as intentionally unused for now
) -> tuple[dict[str, Any], str]:
    """Compute unsafe rename for path conflicts.

    Args:
        path: Path to rename.
        _new_name: New filename (underscore prefix: unused in stub).

    Returns:
        Tuple of (details_dict, preview_message).
    """
    # TODO: Implement unsafe rename computation
    raise NotImplementedError("_compute_unsafe_rename not yet implemented")


def apply_unsafe_fixes(
    issues: list[ScanIssue],
    dry_run: bool = False,
) -> tuple[list[ProposedFix], list[ProposedFix]]:
    """Apply unsafe fixes to detected issues.

    Args:
        issues: List of scan issues to fix.
        dry_run: If True, compute fixes but don't apply them.

    Returns:
        Tuple of (proposed_fixes, applied_fixes).
        If dry_run=True, applied_fixes is empty.
    """
    # TODO: Implement unsafe fix application
    raise NotImplementedError("apply_unsafe_fixes not yet implemented")


def preview_fix(issue: ScanIssue) -> ProposedFix | None:
    """Generate a fix proposal for an issue.

    Args:
        issue: The scan issue to fix.

    Returns:
        ProposedFix if the issue is fixable, None if manual only.
    """
    # TODO: Implement fix preview
    raise NotImplementedError("preview_fix not yet implemented")
