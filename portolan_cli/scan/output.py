"""Scan output formatting and presentation."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

from portolan_cli.scan.classify import FileCategory, SkippedFile
from portolan_cli.scan.core import (
    IssueType,
    ScanIssue,
    ScannedFile,
    ScanResult,
    Severity,
)
from portolan_core.format_families import RASTER, VECTOR

if TYPE_CHECKING:
    from portolan_cli.scan.infer import CollectionSuggestion

_KIB = 1024
_MAX_ISSUES_SHOWN = 10
_MESSAGE_PARTS_MIN = 4
_MESSAGE_MAX_LEN = 40
_RASTER_ITEM_MIN_DEPTH = 2
_MAX_COMMANDS_SHOWN = 5
_MAX_FILES_SHOWN = 3


# Fixability Labels


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
    IssueType.INVALID_COLLECTION_ID: Fixability.FIX_FLAG,
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
    """Get the fixability category for an issue type."""
    return _FIXABILITY_MAP.get(issue_type, Fixability.MANUAL)


# Structure Validation Checklist


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
    """Generate a structure validation checklist from scan result."""
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


# Skip Reason Categories


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
    """Get human-readable display name for a file category."""
    return _CATEGORY_DISPLAY_NAMES.get(category, category.value)


def group_skipped_files(
    skipped: list[Path | SkippedFile],
) -> dict[FileCategory, list[SkippedFile]]:
    """Group skipped files by their category."""
    grouped: dict[FileCategory, list[SkippedFile]] = defaultdict(list)

    for item in skipped:
        if isinstance(item, SkippedFile):
            grouped[item.category].append(item)
        # Legacy Path objects are ignored (no category info)

    return dict(grouped)


# Collection Inference Output


def format_collection_suggestion(
    suggestion: CollectionSuggestion,
    max_files: int = 5,
) -> str:
    """Format a collection suggestion for display."""
    confidence_pct = int(suggestion.confidence * 100)
    file_count = len(suggestion.files)

    # Header line
    lines = [f"  {suggestion.suggested_name} ({file_count} files, {confidence_pct}% confidence)"]

    # List files (truncated if needed)
    files_to_show = suggestion.files[:max_files]
    lines.extend(f"    - {f.name}" for f in files_to_show)

    # Show truncation message
    if file_count > max_files:
        remaining = file_count - max_files
        lines.append(f"    ... and {remaining} more")

    return "\n".join(lines)


# Next Steps Summary


def _count_by_fixability(result: ScanResult, fixability: Fixability) -> int:
    """Count issues with a specific fixability level."""
    return sum(1 for i in result.issues if get_fixability(i.issue_type) == fixability)


def _pluralize(count: int, singular: str, plural: str) -> str:
    """Return singular or plural form based on count."""
    return singular if count == 1 else plural


def generate_next_steps(result: ScanResult) -> list[str]:
    """Generate actionable next steps based on scan result."""
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


# Tree View Output


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
    """Build a nested dictionary representing the directory tree."""
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
    """Render a tree view of the scan result."""
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

    if size_bytes < _KIB:
        return f"{size_bytes} B"
    if size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    if size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"


# Full Formatted Output


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
        if len(severity_issues) > _MAX_ISSUES_SHOWN:
            lines.append(f"  ... and {len(severity_issues) - _MAX_ISSUES_SHOWN} more")
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
    """Get the marker symbol for a severity level."""
    markers = {
        Severity.ERROR: "\u2717",  # ✗
        Severity.WARNING: "\u26a0",  # ⚠
        Severity.INFO: "\u2139",  # ℹ
    }
    return markers.get(severity, "\u2022")  # • as fallback


def _parse_issue_location(relative_path: str) -> tuple[str, str]:
    """Parse an issue's relative path into (directory, filename)."""
    rel_path = Path(relative_path) if relative_path else Path()
    rel_str = str(rel_path)

    # Root directory issue
    if rel_str in (".", ""):
        return ".", "."
    # File directly in root
    if rel_path.parent == Path():
        return ".", rel_str
    # File in subdirectory
    return str(rel_path.parent), rel_path.name


def _build_errors_tree(
    manual_issues: list[ScanIssue],
) -> dict[str, list[tuple[str, Severity, str]]]:
    """Build a tree structure grouping issues by directory."""
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
    """Shorten a message for inline tree display."""
    # Handle special case: shapefile sidecars (extract the sidecar list)
    if message.startswith("Shapefile missing required sidecars:"):
        return f"missing {message.split(':')[1].strip()}"

    # Handle special case: primary assets count (extract the number)
    if message.startswith("Directory has") and "primary assets" in message:
        parts = message.split()
        return (
            f"{parts[2]} primary assets"
            if len(parts) >= _MESSAGE_PARTS_MIN
            else message[: _MESSAGE_MAX_LEN - 3] + "..."
        )

    # Check simple substring patterns
    for pattern, short_form in _MESSAGE_PATTERNS:
        if pattern in message:
            return short_form

    # Default: truncate if too long
    return message[: _MESSAGE_MAX_LEN - 3] + "..." if len(message) > _MESSAGE_MAX_LEN else message


def _render_errors_tree(tree: dict[str, list[tuple[str, Severity, str]]]) -> list[str]:
    """Render the error tree as formatted lines."""
    lines: list[str] = []

    # Sort directories: root first, then alphabetically
    sorted_dirs = sorted(tree.keys(), key=lambda d: "" if d == "." else d)

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
    """Format output showing only issues requiring manual resolution."""
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


def _format_special_formats(result: ScanResult) -> list[str]:
    """Format detected special formats (Hive partitions, etc.) for display."""
    if not result.special_formats:
        return []

    lines: list[str] = []
    lines.append("")
    lines.append("Detected special formats:")

    for sf in result.special_formats:
        if sf.format_type == "hive_partition":
            # Use scan-time snapshot from sf.details instead of re-scanning filesystem
            partition_meta = getattr(sf, "details", None) or {}

            # Check for new format (partition:keys) first, then legacy (partition_keys)
            keys_raw = partition_meta.get("partition:keys", [])
            if keys_raw and isinstance(keys_raw, list) and isinstance(keys_raw[0], dict):
                # New format: [{"name": "x", "type": "string"}, ...]
                key_names = [k.get("name", "unknown") for k in keys_raw]
                file_count = partition_meta.get("partition:file_count", 0)
                strategy = partition_meta.get("partition:strategy")

                strategy_str = f" ({strategy})" if strategy else ""
                keys_str = ", ".join(key_names) if key_names else "unknown"
                lines.append(f"  ✓ Hive-partitioned{strategy_str}: {sf.relative_path}")
                lines.append(f"      Keys: {keys_str}, {file_count} partitions")
            else:
                # Legacy format: {"partition_keys": ["x", "y"]}
                legacy_keys = partition_meta.get("partition_keys", [])
                keys_str = ", ".join(legacy_keys) if legacy_keys else "unknown"
                lines.append(f"  ✓ Hive-partitioned: {sf.relative_path}")
                lines.append(f"      Keys: {keys_str}")
        elif sf.format_type == "filegdb":
            gdbtable_count = sf.details.get("gdbtable_count", 0)
            lines.append(f"  ✓ FileGDB: {sf.relative_path} ({gdbtable_count} tables)")
        elif sf.format_type == "stac_catalog":
            lines.append(f"  ℹ Existing STAC catalog: {sf.relative_path}")
        elif sf.format_type == "stac_collection":
            lines.append(f"  ℹ Existing STAC collection: {sf.relative_path}")
        else:
            lines.append(f"  • {sf.format_type}: {sf.relative_path}")

    return lines


def format_scan_output(
    result: ScanResult,
    show_tree: bool = False,
    show_missing: bool = True,
    manual_only: bool = False,
) -> str:
    """Format complete scan output for terminal display."""
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

    # Special formats (Hive partitions, FileGDB, etc.)
    lines.extend(_format_special_formats(result))

    # Issues, skipped, suggestions, next steps
    lines.extend(_format_issues(result))
    lines.extend(_format_skipped(result))

    if result.collection_suggestions:
        lines.append("")
        lines.append("Suggested collections:")
        lines.extend(
            format_collection_suggestion(suggestion) for suggestion in result.collection_suggestions
        )

    steps = generate_next_steps(result)
    if steps:
        lines.append("")
        lines.append("Next steps:")
        lines.extend(f"  \u2192 {step}" for step in steps)

    return "\n".join(lines)


# Phase 3: Enhanced Output for Nested Catalogs


def _get_collection_id(file: ScannedFile) -> str | None:
    """Extract inferred_collection_id from ScannedFile."""
    # Primary: top-level field (nested catalog support)
    if file.inferred_collection_id:
        return file.inferred_collection_id
    # Fallback: metadata dict (backward compatibility)
    return file.metadata.get("inferred_collection_id") or None


def _get_format_status(file: ScannedFile) -> str:
    """Return the format status of a ScannedFile as a string."""
    return file.format_status.value


def _get_format_display_name(file: ScannedFile) -> str:
    """Extract format_display_name from ScannedFile."""
    # Primary: top-level field
    if file.format_display_name:
        return file.format_display_name
    # Fallback: metadata dict
    name = file.metadata.get("format_display_name")
    if isinstance(name, str):
        return name
    return file.extension.lstrip(".").upper()


def format_file_entry(file: ScannedFile) -> str:
    """Format a single file entry with collection ID and format status."""
    filename = file.path.name
    format_name = _get_format_display_name(file)
    collection_id = _get_collection_id(file)

    parts = [filename]

    # Add format info
    parts.append(f"({format_name})")

    # Add the collection ID when the file has one, nested or not.
    if collection_id:
        parts.append(f"→ {collection_id}")

    return " ".join(parts)


def group_files_by_collection(
    files: list[ScannedFile],
) -> dict[str, list[ScannedFile]]:
    """Group scanned files by their inferred collection ID."""
    grouped: dict[str, list[ScannedFile]] = defaultdict(list)

    for file in files:
        collection_id = _get_collection_id(file)
        key = collection_id or "(uncategorized)"
        grouped[key].append(file)

    return dict(grouped)


# Structure Pattern Detection


@dataclass(frozen=True)
class StructurePattern:
    """Detected structure pattern for a scan result."""

    pattern_type: str
    collections: list[str]
    is_nested: bool
    suggested_commands: list[str]


def detect_structure_pattern(result: ScanResult) -> StructurePattern:
    """Detect the structure pattern from scan result."""
    if not result.ready:
        return StructurePattern(
            pattern_type="empty",
            collections=[],
            is_nested=False,
            suggested_commands=[],
        )

    # Extract unique collection IDs
    collections: set[str] = set()
    for f in result.ready:
        cid = _get_collection_id(f)
        if cid:
            collections.add(cid)

    collection_list = sorted(collections)
    is_nested = any("/" in cid for cid in collection_list)

    # Check format types
    has_vector = any(f.format_type == VECTOR for f in result.ready)
    has_raster = any(f.format_type == RASTER for f in result.ready)

    # Calculate path depths for pattern detection
    depths = [f.relative_path.count("/") for f in result.ready]
    avg_depth = sum(depths) / len(depths) if depths else 0

    # Detect pattern - check format-specific patterns first
    if len(collection_list) == 0:
        pattern_type = "flat"
    elif has_raster and not has_vector:
        # Check if rasters are organized as items (subdirs per date/scene)
        # Raster items typically have deeper paths (collection/item/asset)
        pattern_type = (
            "raster_items" if avg_depth >= _RASTER_ITEM_MIN_DEPTH else "raster_collection"
        )
    elif has_vector and not has_raster:
        # Vector collections have files at collection level
        pattern_type = "vector_collection"
    elif has_raster and has_vector:
        pattern_type = "mixed"
    elif len(collection_list) == 1:
        pattern_type = "single_collection"
    else:
        pattern_type = "multiple_collections"

    # Generate suggested commands
    commands = [f"portolan add {cid}" for cid in collection_list]

    return StructurePattern(
        pattern_type=pattern_type,
        collections=collection_list,
        is_nested=is_nested,
        suggested_commands=commands,
    )


def generate_structure_recommendation(result: ScanResult) -> str:
    """Generate actionable structure recommendations."""
    pattern = detect_structure_pattern(result)

    if pattern.pattern_type == "empty":
        return ""

    lines: list[str] = []

    # Pattern description
    pattern_descriptions = {
        "vector_collection": "Vector data organized as collections",
        "raster_items": "Raster data organized as items (date/scene subdirectories)",
        "raster_collection": "Raster collection",
        "single_collection": "Single collection structure",
        "mixed": "Mixed vector and raster data",
        "flat": "Flat structure (files at root level)",
    }

    desc = pattern_descriptions.get(pattern.pattern_type, pattern.pattern_type)
    lines.append(f"Structure detected: {desc}")

    if pattern.is_nested:
        lines.append(" Nested catalog structure")

    # Commands to run
    if pattern.suggested_commands:
        lines.append("")
        lines.append("Suggested commands:")
        # Limit to 5
        lines.extend(f"  $ {cmd}" for cmd in pattern.suggested_commands[:_MAX_COMMANDS_SHOWN])
        if len(pattern.suggested_commands) > _MAX_COMMANDS_SHOWN:
            remaining = len(pattern.suggested_commands) - _MAX_COMMANDS_SHOWN
            lines.append(f"  ... and {remaining} more")

    return "\n".join(lines)


def generate_ascii_tree_recommendation(collections: list[str]) -> str:
    """Generate ASCII tree showing recommended catalog structure."""
    if not collections:
        return ""

    # Build tree structure
    tree: dict[str, Any] = {}

    for collection_id in collections:
        parts = collection_id.split("/")
        current = tree
        for part in parts:
            if part not in current:
                current[part] = {}
            current = current[part]

    # Render tree
    lines: list[str] = ["catalog/"]

    def render_level(node: dict[str, Any], prefix: str = "") -> None:
        items = sorted(node.items())
        for i, (name, children) in enumerate(items):
            is_last = i == len(items) - 1
            connector = "\u2514\u2500\u2500 " if is_last else "\u251c\u2500\u2500 "
            child_prefix = "    " if is_last else "\u2502   "

            if children:
                # It's a directory/intermediate catalog
                lines.append(f"{prefix}{connector}{name}/")
                render_level(children, prefix + child_prefix)
            else:
                # It's a leaf collection
                lines.append(f"{prefix}{connector}{name}/ (collection)")

    render_level(tree)
    return "\n".join(lines)


# Verbose Output Formatting


def format_issue_basic(issue: ScanIssue) -> str:
    """Format an issue with minimal info (for non-verbose mode)."""
    marker = _get_severity_marker(issue.severity)
    return f"{marker} {issue.relative_path}: {issue.message}"


def format_issue_verbose(issue: ScanIssue) -> str:
    """Format an issue with full details (for --verbose mode)."""
    marker = _get_severity_marker(issue.severity)
    lines = [f"{marker} {issue.relative_path}"]
    lines.append(f"  Issue: {issue.message}")

    if issue.suggestion:
        lines.append(f"  Fix: {issue.suggestion}")

    # Add spec references for specific issue types
    spec_refs = {
        IssueType.MULTIPLE_PRIMARIES: "One primary geo-asset per directory",
        IssueType.MIXED_FLAT_MULTIITEM: "Nested catalogs with flat collections",
    }

    if issue.issue_type in spec_refs:
        lines.append(f"  Reference: {spec_refs[issue.issue_type]}")

    return "\n".join(lines)


# JSON Output Formatting (Phase 3 Enhancements)


def format_ready_file_json(file: ScannedFile) -> dict[str, Any]:
    """Format a ready file for JSON output with enhanced fields."""
    return {
        "path": str(file.path),
        "relative_path": file.relative_path,
        "extension": file.extension,
        "format_type": file.format_type or "unknown",
        "size_bytes": file.size_bytes,
        "metadata": file.metadata,
        # Enhanced fields (extracted from metadata for convenience)
        "inferred_collection_id": _get_collection_id(file),
        "format_status": _get_format_status(file),
        "format_display_name": _get_format_display_name(file),
    }


def format_recommended_structure_json(result: ScanResult) -> dict[str, Any]:
    """Format recommended structure for JSON output."""
    pattern = detect_structure_pattern(result)

    return {
        "pattern_type": pattern.pattern_type,
        "collections": pattern.collections,
        "is_nested": pattern.is_nested,
        "suggested_commands": pattern.suggested_commands,
    }


def format_fix_commands_json(result: ScanResult) -> list[dict[str, Any]]:
    """Format fix commands for JSON output (agent consumption)."""
    # Add commands for each detected collection
    pattern = detect_structure_pattern(result)
    commands: list[dict[str, Any]] = [
        {
            "command": "add",
            "args": [collection_id],
            "options": {},
            "reason": "Collection detected, needs tracking",
        }
        for collection_id in pattern.collections
    ]

    # One scan --fix per unique directory, because scan --fix takes a directory.
    fix_reasons: dict[str, list[str]] = {}  # directory -> list of reasons

    for issue in result.issues:
        fix = get_fixability(issue.issue_type)
        if fix == Fixability.FIX_FLAG:
            # Get the parent directory of the file
            # issue.relative_path is a file path; we need its directory
            file_path = Path(issue.relative_path)
            dir_path = str(file_path.parent) if file_path.parent.parts else "."
            if dir_path not in fix_reasons:
                fix_reasons[dir_path] = []
            fix_reasons[dir_path].append(issue.message)

    # Emit one command per unique directory with aggregated reasons
    for dir_path in sorted(fix_reasons.keys()):
        reasons = fix_reasons[dir_path]
        # Use positional arg format: scan --fix <directory>
        commands.append(
            {
                "command": "scan",
                "args": ["--fix", dir_path],
                "reason": reasons[0] if len(reasons) == 1 else f"{len(reasons)} issues",
            }
        )

    return commands


# Enhanced Summary Format


def format_enhanced_summary(
    result: ScanResult,
    *,
    verbose: bool = False,
    show_tree: bool = False,
) -> str:
    """Format enhanced scan summary with nested collection support."""
    lines: list[str] = []

    # Header
    ready_count = len(result.ready)
    if ready_count == 0:
        lines.append(f"Scanned {result.directories_scanned} directories")
        lines.append("No geo-assets found")
        return "\n".join(lines)

    lines.append(
        f"{ready_count} geo-asset{'s' if ready_count != 1 else ''} found "
        f"in {result.directories_scanned} directories"
    )

    # Group files by collection
    grouped = group_files_by_collection(result.ready)

    lines.append("")
    lines.append("Collections:")
    for collection_id, files in sorted(grouped.items()):
        file_count = len(files)
        lines.append(f"  {collection_id} ({file_count} file{'s' if file_count != 1 else ''})")
        for f in files[:_MAX_FILES_SHOWN]:  # Show first 3 files
            format_name = _get_format_display_name(f)
            lines.append(f"    - {f.path.name} ({format_name})")
        if file_count > _MAX_FILES_SHOWN:
            lines.append(f"    ... and {file_count - _MAX_FILES_SHOWN} more")

    # Tree view (if requested)
    if show_tree:
        lines.append("")
        lines.append(render_tree_view(result, show_missing=True))

    # Structure recommendations
    recommendation = generate_structure_recommendation(result)
    if recommendation:
        lines.append("")
        lines.append(recommendation)

    # Issues
    if result.issues:
        lines.append("")
        error_count = result.error_count
        warn_count = result.warning_count
        if error_count > 0:
            lines.append(f"{error_count} error{'s' if error_count != 1 else ''}")
        if warn_count > 0:
            lines.append(f"{warn_count} warning{'s' if warn_count != 1 else ''}")

        for issue in result.issues[:10]:
            if verbose:
                lines.append(format_issue_verbose(issue))
            else:
                lines.append(f"  {format_issue_basic(issue)}")

        if len(result.issues) > _MAX_ISSUES_SHOWN:
            lines.append(f"  ... and {len(result.issues) - _MAX_ISSUES_SHOWN} more")

    # Next steps
    steps = generate_next_steps(result)
    if steps:
        lines.append("")
        lines.append("Next steps:")
        lines.extend(f"  → {step}" for step in steps)

    return "\n".join(lines)
