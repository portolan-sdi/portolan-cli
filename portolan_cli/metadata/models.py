"""Metadata check data structures.

This module provides dataclasses for tracking metadata state and validation results:
- MetadataStatus: Enum for file metadata states (MISSING, FRESH, STALE, BREAKING)
- FileMetadataState: Holds current vs stored metadata for comparison
- MetadataCheckResult: Per-file validation result with status and fix hints
- MetadataReport: Aggregate report with counts and issue lists

These structures follow the pattern established in validation/results.py.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

# Tolerance for floating-point bbox comparisons (approx 0.1mm at equator)
BBOX_TOLERANCE = 1e-9


def _bboxes_equal(
    bbox1: list[float] | None,
    bbox2: list[float] | None,
    tolerance: float = BBOX_TOLERANCE,
) -> bool:
    """Compare two bounding boxes with floating-point tolerance.

    Args:
        bbox1: First bounding box [west, south, east, north], or None.
        bbox2: Second bounding box [west, south, east, north], or None.
        tolerance: Maximum allowed difference for each coordinate.

    Returns:
        True if bboxes are equal within tolerance, False otherwise.
        Returns False if either bbox is None or lengths differ.
    """
    if bbox1 is None or bbox2 is None:
        return False
    if len(bbox1) != len(bbox2):
        return False
    return all(math.isclose(a, b, abs_tol=tolerance) for a, b in zip(bbox1, bbox2, strict=True))


class MetadataStatus(Enum):
    """Status of file metadata relative to stored state.

    Attributes:
        MISSING: No STAC metadata exists for this file.
        FRESH: Metadata is up to date with file contents.
        STALE: File has changed; metadata needs regeneration.
        BREAKING: Schema has breaking changes (column removed, type changed, etc.).

    The severity property allows sorting issues by importance:
        BREAKING > MISSING > STALE > FRESH
    """

    MISSING = "missing"
    FRESH = "fresh"
    STALE = "stale"
    BREAKING = "breaking"

    @property
    def severity(self) -> int:
        """Return numeric severity for ordering.

        Higher values are more severe.

        Returns:
            int: Severity level (0-3).
        """
        severity_map = {
            MetadataStatus.FRESH: 0,
            MetadataStatus.STALE: 1,
            MetadataStatus.MISSING: 2,
            MetadataStatus.BREAKING: 3,
        }
        return severity_map[self]


@dataclass
class FileMetadataState:
    """Comparison state for a single file's metadata.

    Holds both current (extracted from file) and stored (from versions.json/STAC)
    metadata values for change detection.

    Attributes:
        file_path: Path to the geo-asset file.
        current_mtime: Current file modification time (Unix timestamp).
        stored_mtime: Stored modification time from versions.json, or None if new.
        current_bbox: Current bounding box [west, south, east, north].
        stored_bbox: Stored bounding box, or None if new.
        current_feature_count: Current feature/row count (or pixel count for raster).
        stored_feature_count: Stored count, or None if new.
        current_schema_fingerprint: Hash/fingerprint of current schema.
        stored_schema_fingerprint: Stored schema fingerprint, or None if new.
    """

    file_path: Path
    current_mtime: float
    stored_mtime: float | None
    current_bbox: list[float] | None
    stored_bbox: list[float] | None
    current_feature_count: int | None
    stored_feature_count: int | None
    current_schema_fingerprint: str | None
    stored_schema_fingerprint: str | None

    @property
    def mtime_changed(self) -> bool:
        """Check if file modification time has changed.

        Returns:
            True if mtime differs or no stored mtime exists.
        """
        if self.stored_mtime is None:
            return True
        return self.current_mtime != self.stored_mtime

    @property
    def heuristics_changed(self) -> bool:
        """Check if quick heuristics (bbox, feature count) have changed.

        This is a fast check that catches most real data changes without
        full content hashing. Uses tolerance-based comparison for floats
        to avoid false positives from floating-point precision issues.

        Returns:
            True if bbox or feature count differs, or if no stored values exist.
        """
        # If no stored values, consider it changed (new file)
        if self.stored_bbox is None or self.stored_feature_count is None:
            return True

        # Compare bbox with tolerance for floating-point precision
        if not _bboxes_equal(self.current_bbox, self.stored_bbox):
            return True

        # Compare feature count
        if self.current_feature_count != self.stored_feature_count:
            return True

        return False

    @property
    def schema_changed(self) -> bool:
        """Check if schema fingerprint has changed.

        Returns:
            True if schema fingerprint differs or no stored fingerprint exists.
        """
        if self.stored_schema_fingerprint is None:
            return True
        return self.current_schema_fingerprint != self.stored_schema_fingerprint

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns:
            Dictionary with all state values.
        """
        return {
            "file_path": str(self.file_path),
            "current_mtime": self.current_mtime,
            "stored_mtime": self.stored_mtime,
            "current_bbox": self.current_bbox,
            "stored_bbox": self.stored_bbox,
            "current_feature_count": self.current_feature_count,
            "stored_feature_count": self.stored_feature_count,
            "current_schema_fingerprint": self.current_schema_fingerprint,
            "stored_schema_fingerprint": self.stored_schema_fingerprint,
            "mtime_changed": self.mtime_changed,
            "heuristics_changed": self.heuristics_changed,
            "schema_changed": self.schema_changed,
        }


@dataclass
class MetadataCheckResult:
    """Result from checking a single file's metadata.

    Attributes:
        file_path: Path to the checked file.
        status: Metadata status (FRESH, STALE, MISSING, BREAKING).
        message: Human-readable description of the result.
        changes: List of what changed (e.g., ["mtime", "bbox", "schema"]).
        fix_hint: Optional suggestion for fixing the issue.
    """

    file_path: Path
    status: MetadataStatus
    message: str
    changes: list[str] = field(default_factory=list)
    fix_hint: str | None = None

    @property
    def is_ok(self) -> bool:
        """Check if metadata is up to date.

        Returns:
            True if status is FRESH.
        """
        return self.status == MetadataStatus.FRESH

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict.

        Returns:
            Dictionary with result data. Omits fix_hint if None.
        """
        result: dict[str, Any] = {
            "file_path": str(self.file_path),
            "status": self.status.value,
            "message": self.message,
            "changes": self.changes,
        }
        if self.fix_hint is not None:
            result["fix_hint"] = self.fix_hint
        return result


@dataclass
class MetadataReport:
    """Aggregate report of metadata check results.

    Collects results from checking multiple files and provides
    summary statistics and filtering.

    Attributes:
        results: List of individual check results.
    """

    results: list[MetadataCheckResult] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        """Total number of files checked.

        Returns:
            Number of results.
        """
        return len(self.results)

    @property
    def fresh_count(self) -> int:
        """Count of files with FRESH status.

        Returns:
            Number of up-to-date files.
        """
        return sum(1 for r in self.results if r.status == MetadataStatus.FRESH)

    @property
    def stale_count(self) -> int:
        """Count of files with STALE status.

        Returns:
            Number of files needing metadata regeneration.
        """
        return sum(1 for r in self.results if r.status == MetadataStatus.STALE)

    @property
    def missing_count(self) -> int:
        """Count of files with MISSING status.

        Returns:
            Number of files without STAC metadata.
        """
        return sum(1 for r in self.results if r.status == MetadataStatus.MISSING)

    @property
    def breaking_count(self) -> int:
        """Count of files with BREAKING status.

        Returns:
            Number of files with breaking schema changes.
        """
        return sum(1 for r in self.results if r.status == MetadataStatus.BREAKING)

    @property
    def passed(self) -> bool:
        """Check if all files have fresh metadata.

        Returns:
            True if all results are FRESH (or no results exist).
        """
        return all(r.status == MetadataStatus.FRESH for r in self.results)

    @property
    def issues(self) -> list[MetadataCheckResult]:
        """Get all non-FRESH results.

        Returns:
            List of results that need attention.
        """
        return [r for r in self.results if r.status != MetadataStatus.FRESH]

    def filter_by_status(self, status: MetadataStatus) -> list[MetadataCheckResult]:
        """Filter results by status.

        Args:
            status: Status to filter by.

        Returns:
            List of results with the specified status.
        """
        return [r for r in self.results if r.status == status]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict for --json output.

        Returns:
            Dictionary with summary statistics and all results.
        """
        return {
            "passed": self.passed,
            "total_count": self.total_count,
            "fresh_count": self.fresh_count,
            "stale_count": self.stale_count,
            "missing_count": self.missing_count,
            "breaking_count": self.breaking_count,
            "results": [r.to_dict() for r in self.results],
        }
