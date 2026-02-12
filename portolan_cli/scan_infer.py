"""Collection inference for portolan scan.

This module infers collection groupings from filename patterns:
- Common prefix extraction
- Numeric suffix detection (flood_rp10, flood_rp100)
- Level/tier patterns (admin_L1, admin_L2)
- Temporal patterns (census_2010, census_2020)

Functions:
    find_common_prefix: Find longest common prefix among names.
    extract_numeric_groups: Group files by common base with numeric suffix.
    detect_pattern_marker: Detect known pattern markers in filenames.
    infer_collections: Main function to infer collection groupings.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from portolan_cli.scan import ScannedFile

# Numeric suffix pattern: base_123 or base-123 or baseL123
NUMERIC_SUFFIX_PATTERN = re.compile(r"^(.+?)[-_]?(\d+)$")

# Pattern for known markers: base_marker123 (e.g., flood_rp10, admin_L1)
MARKER_PATTERNS = {
    "rp": ("return_period", re.compile(r"^(.+?)_?rp(\d+)$")),
    "L": ("level", re.compile(r"^(.+?)_?L(\d+)$")),
    "v": ("version", re.compile(r"^(.+?)_?v(\d+)$")),
}

# Minimum prefix length for suggestions
MIN_PREFIX_LENGTH = 3


@dataclass(frozen=True)
class CollectionSuggestion:
    """Suggested collection grouping from filename patterns."""

    suggested_name: str
    files: tuple[Path, ...]
    pattern_type: str  # "prefix", "numeric", "marker", "level", "temporal"
    confidence: float  # 0.0 to 1.0
    reason: str

    def __post_init__(self) -> None:
        """Validate the suggestion."""
        if not 0.0 <= self.confidence <= 1.0:
            msg = f"confidence must be 0.0-1.0, got {self.confidence}"
            raise ValueError(msg)
        if len(self.files) < 2:
            msg = f"files must have >= 2 items, got {len(self.files)}"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "suggested_name": self.suggested_name,
            "files": [str(p) for p in self.files],
            "pattern_type": self.pattern_type,
            "confidence": self.confidence,
            "reason": self.reason,
        }


def _get_stem(name: str) -> str:
    """Get filename stem (without extension)."""
    return Path(name).stem


def find_common_prefix(names: list[str]) -> str | None:
    """Find longest common prefix among names.

    Args:
        names: List of filenames to analyze.

    Returns:
        Longest common prefix (>= 3 chars), or None if too short.
    """
    if len(names) < 2:
        return None

    # Get stems (without extensions)
    stems = [_get_stem(name) for name in names]

    # Find common prefix
    if not stems:
        return None

    prefix = stems[0]
    for stem in stems[1:]:
        # Shrink prefix until it matches
        while prefix and not stem.startswith(prefix):
            prefix = prefix[:-1]
        if not prefix:
            return None

    # Strip trailing non-alphanumeric characters (separators)
    prefix = prefix.rstrip("_-")

    # Check minimum length
    if len(prefix) < MIN_PREFIX_LENGTH:
        return None

    return prefix


def extract_numeric_groups(names: list[str]) -> dict[str, list[str]]:
    """Group files by common base with numeric suffix.

    Args:
        names: List of filenames to analyze.

    Returns:
        Dictionary mapping base name to list of matching filenames.
        Only groups with 2+ members are included.
    """
    groups: dict[str, list[str]] = defaultdict(list)

    for name in names:
        stem = _get_stem(name)
        match = NUMERIC_SUFFIX_PATTERN.match(stem)
        if match:
            base = match.group(1)
            groups[base].append(name)

    # Filter to groups with 2+ members
    return {k: v for k, v in groups.items() if len(v) >= 2}


def detect_pattern_marker(names: list[str]) -> tuple[str, str] | None:
    """Detect known pattern markers in filenames.

    Args:
        names: List of filenames to analyze.

    Returns:
        Tuple of (base_name, pattern_type) if pattern found, None otherwise.
    """
    if len(names) < 2:
        return None

    stems = [_get_stem(name) for name in names]

    # Check each marker pattern
    for _marker, (pattern_type, pattern) in MARKER_PATTERNS.items():
        bases: set[str] = set()
        match_count = 0

        for stem in stems:
            match = pattern.match(stem)
            if match:
                bases.add(match.group(1))
                match_count += 1

        # If we found matches with consistent base
        if match_count >= 2 and len(bases) == 1:
            return (bases.pop(), pattern_type)

    return None


def _infer_from_markers(
    names: list[str],
    path_by_name: dict[str, Path],
) -> CollectionSuggestion | None:
    """Infer collection from known pattern markers (highest confidence)."""
    marker_result = detect_pattern_marker(names)
    if not marker_result:
        return None

    base_name, pattern_type = marker_result

    # Find the matching pattern
    pattern_info = None
    for _marker, (ptype, pattern) in MARKER_PATTERNS.items():
        if ptype == pattern_type:
            pattern_info = pattern
            break

    if not pattern_info:
        return None

    matching_names = [n for n in names if pattern_info.match(_get_stem(n))]
    if len(matching_names) < 2:
        return None

    matching_paths = tuple(path_by_name[n] for n in matching_names)
    return CollectionSuggestion(
        suggested_name=base_name,
        files=matching_paths,
        pattern_type=pattern_type,
        confidence=0.9,
        reason=f"Detected {pattern_type} pattern in filenames",
    )


def _infer_from_numeric(
    names: list[str],
    path_by_name: dict[str, Path],
    existing_names: set[str],
) -> list[CollectionSuggestion]:
    """Infer collections from numeric suffix patterns."""
    suggestions: list[CollectionSuggestion] = []
    numeric_groups = extract_numeric_groups(names)

    for base, group_names in numeric_groups.items():
        # Skip if already covered by marker detection
        if base in existing_names:
            continue

        matching_paths = tuple(path_by_name[n] for n in group_names)
        # Confidence based on group size
        confidence = min(0.8, 0.5 + 0.1 * len(group_names))
        suggestions.append(
            CollectionSuggestion(
                suggested_name=base,
                files=matching_paths,
                pattern_type="numeric",
                confidence=confidence,
                reason=f"Found {len(group_names)} files with numeric suffix pattern",
            )
        )

    return suggestions


def _infer_from_prefix(
    names: list[str],
    path_by_name: dict[str, Path],
    existing_names: set[str],
) -> CollectionSuggestion | None:
    """Infer collection from common prefix (lower confidence)."""
    prefix = find_common_prefix(names)
    if not prefix or prefix in existing_names:
        return None

    matching_paths = tuple(path_by_name[n] for n in names if _get_stem(n).startswith(prefix))
    if len(matching_paths) < 2:
        return None

    return CollectionSuggestion(
        suggested_name=prefix,
        files=matching_paths,
        pattern_type="prefix",
        confidence=0.6,
        reason=f"Found common prefix '{prefix}' in filenames",
    )


def infer_collections(
    files: list[ScannedFile],
    min_confidence: float = 0.5,
) -> list[CollectionSuggestion]:
    """Infer collection groupings from filename patterns.

    Uses multiple heuristics:
    - Common prefix extraction
    - Numeric suffix detection
    - Pattern marker detection (rp, L, v, year)

    Args:
        files: List of geo-asset files to analyze.
        min_confidence: Minimum confidence threshold (0.0-1.0).

    Returns:
        List of CollectionSuggestion objects, sorted by confidence descending.
    """
    if len(files) < 2:
        return []

    names = [f.path.name for f in files]
    path_by_name = {f.path.name: f.path for f in files}
    suggestions: list[CollectionSuggestion] = []
    existing_names: set[str] = set()

    # 1. Pattern markers (highest confidence)
    marker_suggestion = _infer_from_markers(names, path_by_name)
    if marker_suggestion:
        suggestions.append(marker_suggestion)
        existing_names.add(marker_suggestion.suggested_name)

    # 2. Numeric suffix groups
    numeric_suggestions = _infer_from_numeric(names, path_by_name, existing_names)
    for s in numeric_suggestions:
        suggestions.append(s)
        existing_names.add(s.suggested_name)

    # 3. Common prefix (lower confidence)
    prefix_suggestion = _infer_from_prefix(names, path_by_name, existing_names)
    if prefix_suggestion:
        suggestions.append(prefix_suggestion)

    # Filter and sort
    suggestions = [s for s in suggestions if s.confidence >= min_confidence]
    suggestions.sort(key=lambda s: s.confidence, reverse=True)

    return suggestions
