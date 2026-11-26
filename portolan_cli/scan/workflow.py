"""The `scan` workflow: scan a directory, infer collections, and apply safe fixes.

`run_scan` takes the options of the `scan` command and returns a `ScanOutcome`.
It does not print. The CLI renders the outcome, and the JSON envelope `data`
of the command is `ScanOutcome.to_dict()`.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from portolan_cli.scan.core import ScanIssue, ScanOptions, ScanResult, Severity, scan_directory
from portolan_cli.scan.fix import apply_safe_fixes
from portolan_cli.scan.infer import infer_collections
from portolan_cli.scan.output import format_fix_commands_json

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


@dataclass(frozen=True)
class ScanOutcome:
    """The result of one `scan` run."""

    result: ScanResult
    strict: bool = False

    @property
    def strict_failure(self) -> bool:
        """True when `strict` is set and the scan found an error or a warning."""
        return self.strict and (self.result.has_errors or self.result.warning_count > 0)

    @property
    def failing_issues(self) -> list[ScanIssue]:
        """The issues that fail the command: errors, plus warnings in strict mode."""
        return [
            issue
            for issue in self.result.issues
            if issue.severity == Severity.ERROR
            or (self.strict and issue.severity == Severity.WARNING)
        ]

    def to_dict(self) -> dict[str, Any]:
        """Return the envelope `data` of the `scan` command."""
        result = self.result
        data: dict[str, Any] = dict(result.to_dict())
        # The command replaces the summary of ScanResult.to_dict with this shorter one.
        data["summary"] = {
            "ready_count": len(result.ready),
            "error_count": result.error_count,
            "warning_count": result.warning_count,
            "skipped_count": len(result.skipped),
        }
        data["fix_commands"] = format_fix_commands_json(result)
        return data


def run_scan(
    path: Path,
    *,
    recursive: bool = True,
    max_depth: int | None = None,
    include_hidden: bool = False,
    follow_symlinks: bool = False,
    show_all: bool = False,
    suggest_collections: bool = False,
    strict: bool = False,
    fix: bool = False,
    dry_run: bool = False,
    progress_callback: Callable[[], None] | None = None,
) -> ScanOutcome:
    """Scan `path`, infer collections, and apply safe fixes, as `portolan scan` does.

    Args:
        path: The directory to scan.
        recursive: Scan subdirectories.
        max_depth: The maximum recursion depth. 0 is the target directory only.
        include_hidden: Include files whose name starts with a dot.
        follow_symlinks: Follow symbolic links.
        show_all: Keep every issue without truncation.
        suggest_collections: Suggest collection groupings from filename patterns.
        strict: Treat warnings as errors.
        fix: Apply safe fixes (renames). The result records the proposed and applied fixes.
        dry_run: With `fix`, propose the fixes and apply none.
        progress_callback: Called once for each directory scanned.

    Returns:
        The outcome of the scan.

    Raises:
        PathNotFoundError: If `path` does not exist.
        NotADirectoryError: If `path` is not a directory.
    """
    from portolan_cli.errors import PathNotFoundError

    if not path.exists():
        raise PathNotFoundError(f"Directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    options = ScanOptions(
        recursive=recursive,
        max_depth=max_depth,
        include_hidden=include_hidden,
        follow_symlinks=follow_symlinks,
        show_all=show_all,
        suggest_collections=suggest_collections,
        strict=strict,
    )
    result = scan_directory(path, options, progress_callback=progress_callback)

    if suggest_collections and result.ready:
        result.collection_suggestions = infer_collections(result.ready)

    if fix:
        proposed, applied = apply_safe_fixes(result.issues, dry_run=dry_run)
        result.proposed_fixes = proposed
        result.applied_fixes = applied

    return ScanOutcome(result=result, strict=strict)
