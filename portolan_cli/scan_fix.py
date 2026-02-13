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

import hashlib
import re
import shutil
import unicodedata
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.scan import ScanIssue

# Define constants locally to avoid circular imports with scan.py
# These must match the values in scan.py

# Pattern for invalid filename characters
# Matches: spaces, parentheses, brackets, braces, control chars (0x00-0x1F, 0x7F), and non-ASCII
# Also matches path separators for defense-in-depth against path traversal
INVALID_CHAR_PATTERN: re.Pattern[str] = re.compile(r"[\s()\[\]{}/\\\x00-\x1f\x7f]|[^\x00-\x7f]")

# Path length threshold for warnings
# Set to 200 as a conservative cross-platform value:
# - Windows MAX_PATH is 260 (leaves room for directory + extension)
# - Linux PATH_MAX is typically 4096 (not a concern)
# This threshold is for the FULL path, not just filename
LONG_PATH_THRESHOLD: int = 200

# Windows reserved device names (case-insensitive)
WINDOWS_RESERVED_NAMES: frozenset[str] = frozenset(
    {
        "con",
        "prn",
        "aux",
        "nul",
        "com1",
        "com2",
        "com3",
        "com4",
        "com5",
        "com6",
        "com7",
        "com8",
        "com9",
        "lpt1",
        "lpt2",
        "lpt3",
        "lpt4",
        "lpt5",
        "lpt6",
        "lpt7",
        "lpt8",
        "lpt9",
    }
)

# Shapefile sidecar extensions
# Comprehensive list including all known sidecar types
SHAPEFILE_REQUIRED_SIDECARS: frozenset[str] = frozenset({".dbf", ".shx"})
SHAPEFILE_OPTIONAL_SIDECARS: frozenset[str] = frozenset(
    {
        ".prj",  # Projection info
        ".cpg",  # Code page (encoding)
        ".sbn",  # Spatial index
        ".sbx",  # Spatial index
        ".qix",  # GDAL spatial index
        ".shp.xml",  # ESRI metadata (handled specially below)
        ".qmd",  # QGIS metadata
        ".atx",  # Attribute index
        ".ixs",  # Geocoding index
        ".mxs",  # Geocoding index
        ".fbn",  # Read-only spatial index
        ".fbx",  # Read-only spatial index
        ".ain",  # Attribute index
        ".aih",  # Attribute index
    }
)
SHAPEFILE_ALL_SIDECARS: frozenset[str] = SHAPEFILE_REQUIRED_SIDECARS | SHAPEFILE_OPTIONAL_SIDECARS

# FIX_FLAG issue types - these are safe to fix with --fix
# Must match scan_output.py _FIXABILITY_MAP entries with FIX_FLAG value
FIX_FLAG_ISSUE_TYPES: frozenset[str] = frozenset(
    {
        "invalid_characters",
        "windows_reserved_name",
        "long_path",
        "path_too_long",
    }
)


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


# =============================================================================
# Helper functions
# =============================================================================


def _transliterate_to_ascii(text: str) -> str:
    """Transliterate non-ASCII characters to ASCII equivalents.

    Uses Unicode NFKD normalization to decompose characters, then
    encodes to ASCII, ignoring characters that can't be represented.

    Example:
        >>> _transliterate_to_ascii("données")
        'donnees'
        >>> _transliterate_to_ascii("naïve")
        'naive'
    """
    # Normalize to decomposed form (é -> e + combining accent)
    normalized = unicodedata.normalize("NFKD", text)
    # Encode to ASCII, ignoring non-ASCII characters
    return normalized.encode("ascii", "ignore").decode("ascii")


def _sanitize_filename(name: str) -> str:
    """Sanitize a filename by replacing problematic characters.

    - Replaces spaces, parentheses, brackets, braces with underscores
    - Removes path separators (defense-in-depth against traversal)
    - Transliterates non-ASCII to ASCII
    - Collapses multiple consecutive underscores
    - Falls back to hash-based name if result would be empty

    Args:
        name: Original filename (without extension).

    Returns:
        Sanitized filename (never empty).
    """
    # FIRST: Sanitize path separators BEFORE using Path() to avoid misinterpretation
    # This is defense-in-depth against path traversal attacks
    name_safe = re.sub(r"[/\\]", "_", name)

    # Split stem and extension
    # Handle multiple extensions like .tar.gz properly
    stem = Path(name_safe).stem
    suffix = name_safe[len(stem) :]

    # Transliterate non-ASCII characters
    sanitized = _transliterate_to_ascii(stem)

    # Replace problematic characters with underscores
    # Matches: spaces, parentheses, brackets, braces, control chars
    sanitized = re.sub(r"[\s()\[\]{}\x00-\x1f\x7f]", "_", sanitized)

    # Collapse multiple consecutive underscores
    sanitized = re.sub(r"_+", "_", sanitized)

    # Remove leading/trailing underscores
    sanitized = sanitized.strip("_")

    # CRITICAL: Handle case where sanitization produces empty string
    # This can happen with filenames containing only non-ASCII chars (e.g., "日本語.shp")
    if not sanitized:
        sanitized = f"file_{_compute_short_hash(stem)}"

    return sanitized + suffix


def _is_windows_reserved(stem: str) -> bool:
    """Check if a filename stem is a Windows reserved name."""
    return stem.lower() in WINDOWS_RESERVED_NAMES


def _needs_rename(path: Path) -> bool:
    """Check if a file needs to be renamed.

    Returns True if:
    - Filename contains invalid characters (spaces, non-ASCII, etc.)
    - Filename is a Windows reserved name
    - Full path exceeds length threshold
    """
    name = path.name
    stem = path.stem

    # Check for invalid characters
    if INVALID_CHAR_PATTERN.search(name):
        return True

    # Check for Windows reserved names
    if _is_windows_reserved(stem):
        return True

    # Check for long path
    if len(str(path)) > LONG_PATH_THRESHOLD:
        return True

    return False


def _compute_short_hash(text: str, length: int = 8) -> str:
    """Compute a short hash of text for uniqueness.

    An 8-character hex hash provides 2^32 (~4 billion) unique values,
    which is sufficient for filename uniqueness in typical datasets.
    For extremely large datasets, collision probability follows the
    birthday paradox but remains negligible for practical use.

    Args:
        text: Text to hash.
        length: Number of characters in the hash (default: 8).

    Returns:
        Hexadecimal hash string.
    """
    return hashlib.sha256(text.encode()).hexdigest()[:length]


def _find_sidecars(shp_path: Path) -> list[Path]:
    """Find all sidecar files for a shapefile.

    Sidecars are files with the same stem but different extensions.
    Handles both simple extensions (.dbf) and compound extensions (.shp.xml).

    Args:
        shp_path: Path to the .shp file.

    Returns:
        List of paths to existing sidecar files.
    """
    sidecars: list[Path] = []
    parent = shp_path.parent
    stem = shp_path.stem

    for ext in SHAPEFILE_ALL_SIDECARS:
        # Handle compound extension .shp.xml specially
        if ext == ".shp.xml":
            sidecar = parent / f"{stem}.shp.xml"
        else:
            sidecar = parent / f"{stem}{ext}"
        if sidecar.exists():
            sidecars.append(sidecar)

    return sidecars


# =============================================================================
# Core fix computation
# =============================================================================


def _compute_safe_rename(path: Path) -> tuple[Path, str] | None:
    """Compute safe rename for a file with invalid characters.

    Handles three types of issues:
    1. INVALID_CHARACTERS: spaces, parentheses, non-ASCII → sanitized
    2. WINDOWS_RESERVED_NAME: CON, PRN, etc. → _CON, _PRN
    3. LONG_PATH: truncate filename with hash suffix

    Args:
        path: Path to the file with invalid characters.

    Returns:
        Tuple of (new_path, preview_message) or None if no rename needed.
    """
    if not _needs_rename(path):
        return None

    name = path.name
    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    # Determine what kind of fix is needed
    is_reserved = _is_windows_reserved(stem)
    is_long = len(str(path)) > LONG_PATH_THRESHOLD
    has_invalid_chars = bool(INVALID_CHAR_PATTERN.search(name))

    # Start with the original name
    new_stem = stem

    # Fix invalid characters first
    if has_invalid_chars:
        new_stem = _sanitize_filename(stem)

    # Fix Windows reserved names (add underscore prefix)
    if is_reserved:
        new_stem = f"_{new_stem}"

    # Truncate for long paths
    if is_long:
        # Calculate how much we need to shorten
        target_path_len = LONG_PATH_THRESHOLD

        # Calculate minimum possible path length:
        # parent + "/" + "x" (1-char stem) + hash_suffix + suffix
        hash_suffix = f"_{_compute_short_hash(name)}"
        min_filename = f"x{hash_suffix}{suffix}"
        min_possible_len = len(str(parent / min_filename))

        # CRITICAL: If directory path alone is too long, we cannot fix this
        # Return None to indicate this needs manual intervention
        if min_possible_len > target_path_len:
            return None

        current_len = len(str(parent / f"{new_stem}{suffix}"))
        excess = current_len - target_path_len

        if excess > 0:
            # Calculate max stem length
            max_stem_len = len(new_stem) - excess - len(hash_suffix)
            if max_stem_len < 1:
                max_stem_len = 1
            new_stem = new_stem[:max_stem_len] + hash_suffix

    new_name = f"{new_stem}{suffix}"
    new_path = parent / new_name

    # Build preview message
    preview_parts = [f"Rename: {name} → {new_name}"]

    # Check for sidecars if it's a shapefile
    if suffix.lower() == ".shp":
        sidecars = _find_sidecars(path)
        if sidecars:
            sidecar_exts = ", ".join(s.suffix for s in sidecars)
            preview_parts.append(f" (+ sidecars: {sidecar_exts})")

    preview = "".join(preview_parts)

    return new_path, preview


def _apply_rename(
    old_path: Path,
    new_path: Path,
) -> bool:
    """Apply a file rename operation.

    Uses atomic rename when possible to avoid TOCTOU race conditions.

    Args:
        old_path: Current file path.
        new_path: Target file path.

    Returns:
        True if rename succeeded, False if collision or error.
    """
    try:
        # Use os.rename for atomic operation on same filesystem
        # This avoids TOCTOU race condition between exists() check and move
        import os

        os.rename(str(old_path), str(new_path))
        return True
    except FileExistsError:
        # Target already exists (collision)
        return False
    except OSError:
        # Cross-filesystem or other error, fall back to shutil.move
        # but check for collision first
        if new_path.exists():
            return False
        try:
            shutil.move(str(old_path), str(new_path))
            return True
        except (OSError, shutil.Error):
            return False


def _apply_shapefile_rename(
    shp_path: Path,
    new_shp_path: Path,
) -> bool:
    """Apply rename for shapefile and all its sidecars.

    Implements rollback on failure: if any sidecar rename fails,
    all previously renamed files are restored to original names.

    Args:
        shp_path: Current .shp file path.
        new_shp_path: Target .shp file path.

    Returns:
        True if all renames succeeded, False if any collision or error.
    """
    import os

    new_stem = new_shp_path.stem
    new_parent = new_shp_path.parent

    # Find all sidecars
    sidecars = _find_sidecars(shp_path)

    # Build list of all renames to perform
    renames: list[tuple[Path, Path]] = [(shp_path, new_shp_path)]
    for sidecar in sidecars:
        # Handle .shp.xml compound extension specially
        if str(sidecar).endswith(".shp.xml"):
            new_sidecar = new_parent / f"{new_stem}.shp.xml"
        else:
            new_sidecar = new_parent / f"{new_stem}{sidecar.suffix}"
        renames.append((sidecar, new_sidecar))

    # Check for collisions first (before any rename)
    for _, target in renames:
        if target.exists():
            return False

    # Perform renames with rollback on failure
    completed: list[tuple[Path, Path]] = []

    for old_path, new_path in renames:
        try:
            os.rename(str(old_path), str(new_path))
            completed.append((old_path, new_path))
        except FileExistsError:
            # Collision detected - rollback
            _rollback_renames(completed)
            return False
        except OSError:
            # Try shutil.move as fallback
            if new_path.exists():
                _rollback_renames(completed)
                return False
            try:
                shutil.move(str(old_path), str(new_path))
                completed.append((old_path, new_path))
            except (OSError, shutil.Error):
                # Rename failed - rollback
                _rollback_renames(completed)
                return False

    return True


def _rollback_renames(completed: list[tuple[Path, Path]]) -> None:
    """Rollback completed renames by restoring original names.

    Best-effort rollback: logs errors but continues attempting
    to restore all files.

    Args:
        completed: List of (original_path, new_path) tuples to rollback.
    """
    import os

    for old_path, new_path in reversed(completed):
        try:
            os.rename(str(new_path), str(old_path))
        except OSError:
            # Best effort - can't do much if rollback fails
            # In production, this should log the error
            pass


# =============================================================================
# Public API
# =============================================================================


def _is_fix_flag_issue(issue: ScanIssue) -> bool:
    """Check if an issue is a FIX_FLAG issue (safe to auto-fix).

    Args:
        issue: The scan issue to check.

    Returns:
        True if the issue can be safely auto-fixed.
    """
    return issue.issue_type.value in FIX_FLAG_ISSUE_TYPES


def apply_safe_fixes(
    issues: list[ScanIssue],
    dry_run: bool = False,
) -> tuple[list[ProposedFix], list[ProposedFix]]:
    """Apply safe fixes to detected issues.

    Safe fixes include:
    - Renaming files with invalid characters
    - Renaming Windows reserved names
    - Truncating long paths

    Args:
        issues: List of scan issues to fix.
        dry_run: If True, compute fixes but don't apply them.

    Returns:
        Tuple of (proposed_fixes, applied_fixes).
        If dry_run=True, applied_fixes is empty.
    """
    proposed: list[ProposedFix] = []
    applied: list[ProposedFix] = []

    for issue in issues:
        # Filter to only FIX_FLAG issues (safe fixes)
        if not _is_fix_flag_issue(issue):
            continue

        # Skip if file doesn't exist
        if not issue.path.exists():
            continue

        # Compute the rename
        result = _compute_safe_rename(issue.path)
        if result is None:
            continue

        new_path, preview = result

        # Check for collision before creating ProposedFix
        collision = new_path.exists()
        if collision:
            preview = f"{preview} [COLLISION: target exists]"

        # Create the proposed fix
        fix = ProposedFix(
            issue=issue,
            category=FixCategory.SAFE,
            action="rename",
            details={
                "old_path": str(issue.path),
                "new_path": str(new_path),
                "collision": collision,
            },
            preview=preview,
        )
        proposed.append(fix)

        # Apply if not dry run and no collision
        if not dry_run and not collision:
            # Handle shapefiles specially (rename sidecars too)
            if issue.path.suffix.lower() == ".shp":
                success = _apply_shapefile_rename(issue.path, new_path)
            else:
                success = _apply_rename(issue.path, new_path)

            if success:
                applied.append(fix)

    return proposed, applied


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
    # Only handle FIX_FLAG issues (safe fixes)
    if not _is_fix_flag_issue(issue):
        return None

    # FIX_FLAG issues - compute the safe rename
    if not issue.path.exists():
        return None

    result = _compute_safe_rename(issue.path)
    if result is None:
        return None

    new_path, preview = result

    return ProposedFix(
        issue=issue,
        category=FixCategory.SAFE,
        action="rename",
        details={
            "old_path": str(issue.path),
            "new_path": str(new_path),
        },
        preview=preview,
    )
