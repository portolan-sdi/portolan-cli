"""Scan output formatting and presentation.

This module provides enhanced output formatting for the `portolan scan` command:

1. Structure Validation Checklist - Pass/fail checks for catalog structure
2. Fixability Labels - Categorize issues by how they can be fixed
3. Collection Inference Output - Format suggested collections
4. Skip Reason Categories - Better categorization of skipped files
5. Next Steps Summary - Actionable guidance for users
6. Tree View Output - Directory tree with file status markers

Example:
    >>> from portolan_cli.scan_output import format_scan_output
    >>> output = format_scan_output(scan_result)
    >>> print(output)
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.scan import IssueType, ScanIssue, ScanResult, Severity
from portolan_cli.scan_classify import FileCategory, SkippedFile

if TYPE_CHECKING:
    from portolan_cli.scan_infer import CollectionSuggestion


# =============================================================================
# Fixability Labels
# =============================================================================


class Fixability(Enum):
    """Categories for how issues can be fixed."""

    AUTO_FIX = "auto_fix"  # Will be generated automatically on import
    FIX_FLAG = "fix_flag"  # Fixable with `portolan scan --fix`
    MANUAL = "manual"  # User must decide/fix manually

    @property
    def label(self) -> str:
        """Human-readable label for output."""
        labels = {
            Fixability.AUTO_FIX: "[auto-fix]",
            Fixability.FIX_FLAG: "[--fix]",
            Fixability.MANUAL: "[manual]",
        }
        return labels[self]


# Mapping of issue types to their fixability
_FIXABILITY_MAP: dict[IssueType, Fixability] = {
    # Auto-fix: Generated on import
    IssueType.EXISTING_CATALOG: Fixability.AUTO_FIX,
    # --fix: Can be auto-renamed
    IssueType.INVALID_CHARACTERS: Fixability.FIX_FLAG,
    IssueType.WINDOWS_RESERVED_NAME: Fixability.FIX_FLAG,
    IssueType.LONG_PATH: Fixability.FIX_FLAG,
    # Manual: User decision required
    IssueType.MULTIPLE_PRIMARIES: Fixability.MANUAL,
    IssueType.MIXED_FLAT_MULTIITEM: Fixability.MANUAL,
    IssueType.MIXED_FORMATS: Fixability.MANUAL,
    IssueType.DUAL_FORMAT: Fixability.MANUAL,
    IssueType.FILEGDB_DETECTED: Fixability.MANUAL,
    IssueType.HIVE_PARTITION_DETECTED: Fixability.MANUAL,
    # Errors that need manual intervention
    IssueType.INCOMPLETE_SHAPEFILE: Fixability.MANUAL,
    IssueType.ZERO_BYTE_FILE: Fixability.MANUAL,
    IssueType.SYMLINK_LOOP: Fixability.MANUAL,
    IssueType.BROKEN_SYMLINK: Fixability.MANUAL,
    IssueType.PERMISSION_DENIED: Fixability.MANUAL,
    IssueType.DUPLICATE_BASENAME: Fixability.MANUAL,
    IssueType.PATH_TOO_LONG: Fixability.FIX_FLAG,
    IssueType.ORPHAN_SIDECAR: Fixability.MANUAL,
}


def get_fixability(issue_type: IssueType) -> Fixability:
    """Get the fixability category for an issue type.

    Args:
        issue_type: The type of issue.

    Returns:
        The fixability category (AUTO_FIX, FIX_FLAG, or MANUAL).
    """
    return _FIXABILITY_MAP.get(issue_type, Fixability.MANUAL)


# =============================================================================
# Structure Validation Checklist
# =============================================================================


@dataclass(frozen=True)
class ChecklistItem:
    """A single item in the structure validation checklist."""

    name: str
    description: str
    passed: bool
    message: str | None = None


def _check_naming_match(stem: str, parent: str) -> bool:
    """Check if a file stem matches its parent directory name (flexible)."""
    stem_l, parent_l = stem.lower(), parent.lower()
    return stem_l == parent_l or parent_l in stem_l or stem_l in parent_l


def _count_naming_issues(result: ScanResult) -> int:
    """Count geo-assets with names that don't match their parent directory."""
    return sum(
        1
        for f in result.ready
        if f.path.parent != result.root and not _check_naming_match(f.path.stem, f.path.parent.name)
    )


def _count_issues_by_type(result: ScanResult, issue_type: IssueType) -> int:
    """Count issues of a specific type."""
    return sum(1 for i in result.issues if i.issue_type == issue_type)


def generate_structure_checklist(result: ScanResult) -> list[ChecklistItem]:
    """Generate a structure validation checklist from scan result.

    Checks:
    - Root catalog.json exists (or will be generated)
    - Root README.md exists
    - Each collection dir has collection.json
    - Each leaf dir has exactly 1 geo-asset
    - Geo-asset names match dir/collection ID
    - All IDs are valid slugs
    - No geo-assets at root level
    - No mixed flat/multi-item collections

    Args:
        result: The scan result to analyze.

    Returns:
        List of checklist items with pass/fail status.
    """
    catalog_exists = (result.root / "catalog.json").exists()
    readme_exists = (result.root / "README.md").exists()
    root_asset_count = sum(1 for f in result.ready if f.path.parent == result.root)
    multiple_primaries = _count_issues_by_type(result, IssueType.MULTIPLE_PRIMARIES)
    mixed_structure = _count_issues_by_type(result, IssueType.MIXED_FLAT_MULTIITEM)
    naming_issues = _count_naming_issues(result)

    return [
        ChecklistItem(
            name="root_catalog",
            description="Root catalog.json",
            passed=True,
            message="exists" if catalog_exists else "will generate",
        ),
        ChecklistItem(
            name="root_readme",
            description="Root README.md",
            passed=True,
            message="exists" if readme_exists else "will generate",
        ),
        ChecklistItem(
            name="no_root_geo_assets",
            description="No geo-assets at root level",
            passed=root_asset_count == 0,
            message=f"{root_asset_count} found at root" if root_asset_count else None,
        ),
        ChecklistItem(
            name="single_geo_asset_per_dir",
            description="Each leaf dir has exactly 1 geo-asset",
            passed=multiple_primaries == 0,
            message=f"{multiple_primaries} dirs with multiple assets"
            if multiple_primaries
            else None,
        ),
        ChecklistItem(
            name="no_mixed_structure",
            description="No mixed flat/multi-item collections",
            passed=mixed_structure == 0,
            message="unclear structure detected" if mixed_structure else None,
        ),
        ChecklistItem(
            name="geo_asset_naming",
            description="Geo-asset names match dir/collection ID",
            passed=naming_issues <= len(result.ready) // 2,
            message=f"{naming_issues} may need renaming" if naming_issues else None,
        ),
    ]


# =============================================================================
# Skip Reason Categories
# =============================================================================


# Human-readable display names for file categories
_CATEGORY_DISPLAY_NAMES: dict[FileCategory, str] = {
    FileCategory.GEO_ASSET: "geo-asset",
    FileCategory.KNOWN_SIDECAR: "sidecar",
    FileCategory.TABULAR_DATA: "tabular",
    FileCategory.STAC_METADATA: "stac-metadata",
    FileCategory.DOCUMENTATION: "documentation",
    FileCategory.VISUALIZATION: "visualization",
    FileCategory.THUMBNAIL: "thumbnail",
    FileCategory.STYLE: "style",
    FileCategory.JUNK: "junk",
    FileCategory.UNKNOWN: "unknown",
}


def get_category_display_name(category: FileCategory) -> str:
    """Get human-readable display name for a file category.

    Args:
        category: The file category.

    Returns:
        Human-readable name for display.
    """
    return _CATEGORY_DISPLAY_NAMES.get(category, category.value)


def group_skipped_files(
    skipped: list[Path | SkippedFile],
) -> dict[FileCategory, list[SkippedFile]]:
    """Group skipped files by their category.

    Args:
        skipped: List of skipped files (may include legacy Path objects).

    Returns:
        Dictionary mapping FileCategory to list of SkippedFile objects.
    """
    grouped: dict[FileCategory, list[SkippedFile]] = defaultdict(list)

    for item in skipped:
        if isinstance(item, SkippedFile):
            grouped[item.category].append(item)
        # Legacy Path objects are ignored (no category info)

    return dict(grouped)


# =============================================================================
# Collection Inference Output
# =============================================================================


def format_collection_suggestion(
    suggestion: CollectionSuggestion,
    max_files: int = 5,
) -> str:
    """Format a collection suggestion for display.

    Args:
        suggestion: The collection suggestion to format.
        max_files: Maximum number of files to list before truncating.

    Returns:
        Formatted string for terminal output.
    """
    confidence_pct = int(suggestion.confidence * 100)
    file_count = len(suggestion.files)

    # Header line
    lines = [f"  {suggestion.suggested_name} ({file_count} files, {confidence_pct}% confidence)"]

    # List files (truncated if needed)
    files_to_show = suggestion.files[:max_files]
    for f in files_to_show:
        lines.append(f"    - {f.name}")

    # Show truncation message
    if file_count > max_files:
        remaining = file_count - max_files
        lines.append(f"    ... and {remaining} more")

    return "\n".join(lines)


# =============================================================================
# Next Steps Summary
# =============================================================================


def _count_by_fixability(result: ScanResult, fixability: Fixability) -> int:
    """Count issues with a specific fixability level."""
    return sum(1 for i in result.issues if get_fixability(i.issue_type) == fixability)


def _pluralize(count: int, singular: str, plural: str) -> str:
    """Return singular or plural form based on count."""
    return singular if count == 1 else plural


def generate_next_steps(result: ScanResult) -> list[str]:
    """Generate actionable next steps based on scan result.

    Args:
        result: The scan result to analyze.

    Returns:
        List of next step strings, ready for display.
    """
    if not result.ready:
        return [
            "No geo-assets found. Check if files have supported extensions "
            "(.geojson, .shp, .gpkg, .tif, .parquet)"
        ]

    fixable = _count_by_fixability(result, Fixability.FIX_FLAG)
    manual = _count_by_fixability(result, Fixability.MANUAL)
    steps: list[str] = []

    if fixable > 0:
        steps.append(
            f"Run `portolan scan --fix` to auto-rename {fixable} "
            f"file{_pluralize(fixable, '', 's')} with invalid characters"
        )
    if manual > 0:
        steps.append(
            f"{manual} file{_pluralize(manual, ' needs', 's need')} manual grouping decisions "
            "(multiple geo-assets in same dir)"
        )
    if result.error_count > 0:
        steps.append(
            f"Fix {result.error_count} error{_pluralize(result.error_count, '', 's')} before proceeding"
        )
    elif manual == 0:
        steps.append(
            "After fixes: ready to generate catalog"
            if fixable
            else "Structure valid: ready to generate catalog"
        )

    return steps


# =============================================================================
# Tree View Output
# =============================================================================


@dataclass
class TreeNode:
    """A node in the directory tree."""

    name: str
    is_dir: bool
    children: dict[str, TreeNode]
    status: str | None = None  # Status marker (e.g., "geo-asset", "sidecar", "missing")
    size_bytes: int | None = None
    issue: str | None = None  # Issue type if any


def build_tree_structure(result: ScanResult) -> dict[str, Any]:
    """Build a nested dictionary representing the directory tree.

    Args:
        result: The scan result to build tree from.

    Returns:
        Nested dictionary where keys are names and values are either
        dicts (directories) or tuples (files with metadata).
    """
    tree: dict[str, Any] = {}

    def add_path(
        path: Path,
        status: str,
        size: int | None = None,
        issue: str | None = None,
    ) -> None:
        """Add a path to the tree structure."""
        try:
            rel_path = path.relative_to(result.root)
        except ValueError:
            return

        parts = rel_path.parts
        current = tree

        # Navigate/create directories
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        # Add the file/final element
        if parts:
            current[parts[-1]] = {
                "_status": status,
                "_size": size,
                "_issue": issue,
            }

    # Add ready files
    for f in result.ready:
        add_path(f.path, "geo-asset", f.size_bytes)

    # Add skipped files
    for item in result.skipped:
        if isinstance(item, SkippedFile):
            add_path(item.path, get_category_display_name(item.category))
        else:
            add_path(item, "skipped")

    # Add files with issues
    for issue in result.issues:
        # Only add if it's a file-level issue
        if issue.path.is_file() or not issue.path.exists():
            add_path(issue.path, "issue", issue=issue.issue_type.value)

    return tree


def render_tree_view(
    result: ScanResult,
    show_missing: bool = False,
) -> str:
    """Render a tree view of the scan result.

    Args:
        result: The scan result to render.
        show_missing: If True, show expected but missing files.

    Returns:
        Formatted tree string with box-drawing characters.
    """
    lines: list[str] = []
    tree = build_tree_structure(result)

    # Add root directory name
    lines.append(f"{result.root.name}/")

    # Add expected missing files if requested
    if show_missing:
        if not (result.root / "catalog.json").exists():
            lines.append("    catalog.json                    [missing - will generate]")
        if not (result.root / "README.md").exists():
            lines.append("    README.md                       [missing - will generate]")

    def render_node(
        node: dict[str, Any],
        prefix: str = "",
    ) -> None:
        """Recursively render tree nodes."""
        items = sorted(node.items())
        for i, (name, value) in enumerate(items):
            is_last_item = i == len(items) - 1
            connector = "\u2514\u2500\u2500 " if is_last_item else "\u251c\u2500\u2500 "
            child_prefix = "    " if is_last_item else "\u2502   "

            if isinstance(value, dict):
                # Check if it's a file node (has _status) or directory
                if "_status" in value:
                    status = value.get("_status", "")
                    size = value.get("_size")
                    issue = value.get("_issue")

                    # Format the line
                    if status == "geo-asset":
                        size_str = f" ({_format_size(size)})" if size else ""
                        marker = "\u2713 geo-asset" + size_str
                    elif issue:
                        marker = f"\u26a0 {status} [{issue}]"
                    else:
                        marker = f"\u2192 {status}"

                    lines.append(f"{prefix}{connector}{name:<30} {marker}")
                else:
                    # It's a directory
                    lines.append(f"{prefix}{connector}{name}/")
                    render_node(value, prefix + child_prefix)

    render_node(tree)
    return "\n".join(lines)


def _format_size(size_bytes: int | None) -> str:
    """Format file size in human-readable form."""
    if size_bytes is None:
        return ""

    if size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


# =============================================================================
# Full Formatted Output
# =============================================================================


def _format_header(result: ScanResult) -> list[str]:
    """Format the summary header section."""
    lines: list[str] = []
    ready_count = len(result.ready)
    if ready_count == 0:
        lines.append(f"Scanned {result.directories_scanned} directories")
        lines.append("No geo-assets found")
    else:
        lines.append(f"{ready_count} geo-asset{'s' if ready_count != 1 else ''} found")
    return lines


def _format_breakdown(result: ScanResult) -> list[str]:
    """Format the format breakdown section."""
    if not result.ready:
        return []
    formats: dict[str, int] = {}
    for f in result.ready:
        formats[f.extension] = formats.get(f.extension, 0) + 1
    return [
        f"  {count} {ext} file{'s' if count != 1 else ''}" for ext, count in sorted(formats.items())
    ]


def _format_issues(result: ScanResult) -> list[str]:
    """Format issues by severity with fixability labels."""
    if not result.issues:
        return []
    lines: list[str] = [""]
    for severity in [Severity.ERROR, Severity.WARNING, Severity.INFO]:
        severity_issues = [i for i in result.issues if i.severity == severity]
        if not severity_issues:
            continue
        label = severity.value + "s" if len(severity_issues) != 1 else severity.value
        lines.append(f"{len(severity_issues)} {label}:")
        for issue in severity_issues[:10]:
            fix_label = get_fixability(issue.issue_type).label
            lines.append(f"  {fix_label} {issue.relative_path}: {issue.message}")
        if len(severity_issues) > 10:
            lines.append(f"  ... and {len(severity_issues) - 10} more")
    return lines


def _format_skipped(result: ScanResult) -> list[str]:
    """Format skipped files summary."""
    if not result.skipped:
        return []
    grouped = group_skipped_files(result.skipped)
    if not grouped:
        return []
    lines: list[str] = ["", "Skipped files by category:"]
    for category, files in sorted(grouped.items(), key=lambda x: len(x[1]), reverse=True):
        lines.append(f"  {len(files)} {get_category_display_name(category)}")
    return lines


def _get_severity_marker(severity: Severity) -> str:
    """Get the marker symbol for a severity level.

    Args:
        severity: The severity level.

    Returns:
        Unicode marker: ✗ for error, ⚠ for warning, ℹ for info.
    """
    markers = {
        Severity.ERROR: "\u2717",  # ✗
        Severity.WARNING: "\u26a0",  # ⚠
        Severity.INFO: "\u2139",  # ℹ
    }
    return markers.get(severity, "\u2022")  # • as fallback


def _parse_issue_location(relative_path: str) -> tuple[str, str]:
    """Parse an issue's relative path into (directory, filename).

    Args:
        relative_path: The relative path string from the issue.

    Returns:
        Tuple of (directory_path, filename) for tree grouping.
    """
    rel_path = Path(relative_path) if relative_path else Path(".")
    rel_str = str(rel_path)

    # Root directory issue
    if rel_str in (".", ""):
        return ".", "."
    # File directly in root
    if rel_path.parent == Path("."):
        return ".", rel_str
    # File in subdirectory
    return str(rel_path.parent), rel_path.name


def _build_errors_tree(
    manual_issues: list[ScanIssue],
) -> dict[str, list[tuple[str, Severity, str]]]:
    """Build a tree structure grouping issues by directory.

    Args:
        manual_issues: List of issues requiring manual resolution.

    Returns:
        Dict mapping directory paths to list of (filename, severity, message) tuples.
    """
    tree: dict[str, list[tuple[str, Severity, str]]] = defaultdict(list)

    for issue in manual_issues:
        dir_path, filename = _parse_issue_location(issue.relative_path)
        tree[dir_path].append((filename, issue.severity, _shorten_message(issue.message)))

    return dict(tree)


# Message shortening patterns: (substring_to_match, short_form)
_MESSAGE_PATTERNS: list[tuple[str, str]] = [
    ("File is empty", "empty file"),
    ("Sidecar files without primary", "orphan sidecar"),
    ("both raster and vector", "mixed formats"),
    ("Duplicate basenames", "duplicate basename"),
    ("files at root and in subdirectories", "unclear structure"),
]


def _shorten_message(message: str) -> str:
    """Shorten a message for inline tree display.

    Args:
        message: The full issue message.

    Returns:
        Shortened message suitable for inline display.
    """
    # Handle special case: shapefile sidecars (extract the sidecar list)
    if message.startswith("Shapefile missing required sidecars:"):
        return f"missing {message.split(':')[1].strip()}"

    # Handle special case: primary assets count (extract the number)
    if message.startswith("Directory has") and "primary assets" in message:
        parts = message.split()
        return f"{parts[2]} primary assets" if len(parts) >= 4 else message[:37] + "..."

    # Check simple substring patterns
    for pattern, short_form in _MESSAGE_PATTERNS:
        if pattern in message:
            return short_form

    # Default: truncate if too long
    return message[:37] + "..." if len(message) > 40 else message


def _render_errors_tree(tree: dict[str, list[tuple[str, Severity, str]]]) -> list[str]:
    """Render the error tree as formatted lines.

    Args:
        tree: Dict mapping directory paths to list of (filename, severity, message).

    Returns:
        List of formatted output lines.
    """
    lines: list[str] = []

    # Sort directories: root first, then alphabetically
    sorted_dirs = sorted(tree.keys(), key=lambda d: ("" if d == "." else d))

    for dir_path in sorted_dirs:
        issues = tree[dir_path]

        # Show directory header (skip for root if only one entry)
        if dir_path != "." or len(tree) > 1:
            if dir_path == ".":
                lines.append("./")
            else:
                lines.append(f"{dir_path}/")

        # Show issues under this directory
        indent = "  " if (dir_path != "." or len(tree) > 1) else ""
        for filename, severity, message in issues:
            marker = _get_severity_marker(severity)
            if filename == ".":
                # Directory-level issue
                lines.append(f"{indent}{marker} {message}")
            else:
                lines.append(f"{indent}{marker} {filename} ({message})")

    return lines


def _format_manual_only(result: ScanResult) -> str:
    """Format output showing only issues requiring manual resolution.

    Uses a tree structure grouped by directory for easier scanning.

    Args:
        result: The scan result to format.

    Returns:
        Formatted output showing only manual-resolution issues.
    """
    # Filter to only MANUAL fixability issues
    manual_issues = [
        issue for issue in result.issues if get_fixability(issue.issue_type) == Fixability.MANUAL
    ]

    if not manual_issues:
        return "\u2713 No files require manual resolution"

    lines: list[str] = []
    count = len(manual_issues)
    plural = "s" if count != 1 else ""
    verb = "require" if count != 1 else "requires"
    lines.append(f"\u2717 {count} file{plural} {verb} manual resolution")
    lines.append("")

    # Build and render the tree
    tree = _build_errors_tree(manual_issues)
    lines.extend(_render_errors_tree(tree))

    return "\n".join(lines)


# =============================================================================
# Compact Output (Default)
# =============================================================================


def _group_issues_by_type(
    issues: list[ScanIssue],
) -> dict[IssueType, list[ScanIssue]]:
    """Group issues by their type for aggregation."""
    grouped: dict[IssueType, list[ScanIssue]] = defaultdict(list)
    for issue in issues:
        grouped[issue.issue_type].append(issue)
    return dict(grouped)


def _group_by_parent_dir(issues: list[ScanIssue], root: Path) -> dict[str, int]:
    """Group issues by parent directory with counts.

    Args:
        issues: List of issues to group.
        root: Root path for relative path calculation.

    Returns:
        Dict mapping parent directory to count of issues.
    """
    parent_counts: dict[str, int] = defaultdict(int)
    for issue in issues:
        try:
            rel_path = issue.path.relative_to(root)
            # Get first directory component, or "." for root
            parent = str(rel_path.parts[0]) if rel_path.parts else "."
            parent_counts[parent] += 1
        except ValueError:
            parent_counts["."] += 1
    return dict(parent_counts)


def _extract_asset_count(message: str) -> int | None:
    """Extract asset count from 'Directory has N primary assets' message."""
    if "primary assets" in message:
        parts = message.split()
        for i, part in enumerate(parts):
            if part == "has" and i + 1 < len(parts):
                try:
                    return int(parts[i + 1])
                except ValueError:
                    pass
    return None


def format_compact_output(result: ScanResult) -> str | None:
    """Format ultra-compact output for check command.

    Design principles:
    - Silent on success (returns None)
    - Errors first, then warnings
    - Aggregate issues by type, not per-file
    - One hint per issue type, not repeated
    - No file listings (counts only)

    Args:
        result: The scan result to format.

    Returns:
        Formatted string, or None if no issues (silent success).
    """
    lines: list[str] = []

    # Handle errors (blocking issues)
    error_issues = [i for i in result.issues if i.severity == Severity.ERROR]
    if error_issues:
        error_grouped = _group_issues_by_type(error_issues)
        for issue_type, issues in error_grouped.items():
            lines.extend(
                _format_issue_group_compact(issue_type, issues, result.root, is_error=True)
            )

    # Handle warnings (non-blocking)
    warning_issues = [i for i in result.issues if i.severity == Severity.WARNING]
    if warning_issues:
        if lines:
            lines.append("")  # Separator between errors and warnings
        warning_grouped = _group_issues_by_type(warning_issues)
        for issue_type, issues in warning_grouped.items():
            lines.extend(
                _format_issue_group_compact(issue_type, issues, result.root, is_error=False)
            )

    # Success: minimal pass message
    if not lines:
        if not result.ready:
            return "\u2713 No geo-assets found"
        return "\u2713 Check passed with no warnings or errors"

    return "\n".join(lines)


def _format_simple_issue_list(
    marker: str,
    header: str,
    issues: list[ScanIssue],
    root: Path,
    max_items: int = 5,
) -> list[str]:
    """Format a simple list of issues with header and truncation.

    Helper to reduce complexity of _format_issue_group_compact.
    """
    lines: list[str] = [header]
    count = len(issues)

    for issue in issues[:max_items]:
        rel_path = _get_relative_path(issue.path, root)
        lines.append(f"  {rel_path}")

    if count > max_items:
        lines.append(f"  ... and {count - max_items} more")

    return lines


def _format_issue_group_compact(
    issue_type: IssueType,
    issues: list[ScanIssue],
    root: Path,
    *,
    is_error: bool,
) -> list[str]:
    """Format a group of same-type issues compactly.

    Args:
        issue_type: The type of issues in this group.
        issues: List of issues of this type.
        root: Root path for relative paths.
        is_error: Whether these are errors (✗) or warnings (⚠).

    Returns:
        List of formatted lines.
    """
    marker = "\u2717" if is_error else "\u26a0"  # ✗ or ⚠
    count = len(issues)

    # Dispatch to type-specific formatters
    if issue_type == IssueType.MULTIPLE_PRIMARIES:
        return _format_multiple_primaries_compact(marker, issues, root)

    if issue_type == IssueType.INCOMPLETE_SHAPEFILE:
        header = f"{marker} {count} incomplete shapefile{'s' if count != 1 else ''}"
        return _format_simple_issue_list(marker, header, issues, root, max_items=10)

    if issue_type == IssueType.ZERO_BYTE_FILE:
        header = f"{marker} {count} empty file{'s' if count != 1 else ''}"
        return _format_simple_issue_list(marker, header, issues, root)

    if issue_type == IssueType.BROKEN_SYMLINK:
        header = f"{marker} {count} broken symlink{'s' if count != 1 else ''}"
        return _format_simple_issue_list(marker, header, issues, root)

    if issue_type == IssueType.FILEGDB_DETECTED:
        header = (
            f"{marker} {count} FileGDB {'directory' if count == 1 else 'directories'} "
            "(will be converted during sync)"
        )
        return _format_simple_issue_list(marker, header, issues, root)

    # Generic fallback
    header = (
        f"{marker} {count} {issue_type.value.replace('_', ' ')} issue{'s' if count != 1 else ''}"
    )
    return _format_simple_issue_list(marker, header, issues, root, max_items=3)


def _format_multiple_primaries_compact(
    marker: str,
    issues: list[ScanIssue],
    root: Path,
) -> list[str]:
    """Format MULTIPLE_PRIMARIES issues with directory alignment."""
    lines: list[str] = []
    count = len(issues)

    lines.append(
        f"{marker} {count} {'directory' if count == 1 else 'directories'} exceed 1 geo-asset limit"
    )
    lines.append("")

    for issue in issues:
        rel_path = _get_relative_path(issue.path, root)
        asset_count = _extract_asset_count(issue.message)
        count_str = f"{asset_count} geo-assets" if asset_count else "multiple geo-assets"
        lines.append(f"  {rel_path + '/':<35} {count_str}")

    lines.append("")
    lines.append("\u2192 Manually reorganize so each directory has 1 geo-asset")

    return lines


def _get_relative_path(path: Path, root: Path) -> str:
    """Get relative path as string, or absolute if not relative to root."""
    try:
        return str(path.relative_to(root))
    except ValueError:
        return str(path)


def format_scan_output(
    result: ScanResult,
    show_tree: bool = False,
    show_missing: bool = True,
    manual_only: bool = False,
    compact: bool = False,
) -> str | None:
    """Format complete scan output for terminal display.

    Args:
        result: The scan result to format.
        show_tree: If True, include tree view.
        show_missing: If True, show expected but missing files in tree.
        manual_only: If True, show only issues requiring manual resolution.
        compact: If True, use ultra-compact format (silent on success).

    Returns:
        Complete formatted output string, or None if compact mode with no issues.
    """
    # Handle compact mode (new default for check command)
    if compact:
        return format_compact_output(result)

    # Handle manual-only mode
    if manual_only:
        return _format_manual_only(result)

    lines: list[str] = []

    # Header and format breakdown
    lines.extend(_format_header(result))
    lines.extend(_format_breakdown(result))

    # Tree view (if requested)
    if show_tree:
        lines.append("")
        lines.append(render_tree_view(result, show_missing=show_missing))

    # Structure checklist
    checklist = generate_structure_checklist(result)
    failed_checks = [c for c in checklist if not c.passed]
    if failed_checks:
        lines.append("")
        lines.append("Structure checks:")
        for check in checklist:
            mark = "\u2713" if check.passed else "\u2717"
            msg = f" ({check.message})" if check.message else ""
            lines.append(f"  [{mark}] {check.description}{msg}")

    # Issues, skipped, suggestions, next steps
    lines.extend(_format_issues(result))
    lines.extend(_format_skipped(result))

    if result.collection_suggestions:
        lines.append("")
        lines.append("Suggested collections:")
        for suggestion in result.collection_suggestions:
            lines.append(format_collection_suggestion(suggestion))

    steps = generate_next_steps(result)
    if steps:
        lines.append("")
        lines.append("Next steps:")
        for step in steps:
            lines.append(f"  \u2192 {step}")

    return "\n".join(lines)
