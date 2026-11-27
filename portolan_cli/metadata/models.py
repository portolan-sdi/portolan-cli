"""Metadata check data structures."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any

from portolan_core.run_state import file_mtime_matches

if TYPE_CHECKING:
    from pathlib import Path

# Tolerance for floating-point bbox comparisons (approx 0.1mm at equator)
BBOX_TOLERANCE = 1e-9


def _bboxes_equal(
    bbox1: list[float] | None,
    bbox2: list[float] | None,
    tolerance: float = BBOX_TOLERANCE,
) -> bool:
    """Compare two bounding boxes with floating-point tolerance."""
    if bbox1 is None or bbox2 is None:
        return False
    if len(bbox1) != len(bbox2):
        return False
    return all(math.isclose(a, b, abs_tol=tolerance) for a, b in zip(bbox1, bbox2, strict=True))


class MetadataStatus(Enum):
    """Status of file metadata relative to stored state."""

    MISSING = "missing"
    FRESH = "fresh"
    STALE = "stale"
    BREAKING = "breaking"
    ORPHANED = "orphaned"

    @property
    def severity(self) -> int:
        """Return numeric severity for ordering."""
        severity_map = {
            MetadataStatus.FRESH: 0,
            MetadataStatus.ORPHANED: 1,
            MetadataStatus.STALE: 2,
            MetadataStatus.MISSING: 3,
            MetadataStatus.BREAKING: 4,
        }
        return severity_map[self]


@dataclass
class FileMetadataState:
    """Comparison state for a single file's metadata."""

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
        """Check if file modification time has changed."""
        if self.stored_mtime is None:
            return True
        return not file_mtime_matches(self.current_mtime, self.stored_mtime)

    @property
    def heuristics_changed(self) -> bool:
        """Check if quick heuristics (bbox, feature count) have changed."""
        # Collection-level assets pass None bboxes, which means no bbox signal.
        bbox_unavailable = self.stored_bbox is None and self.current_bbox is None
        if not bbox_unavailable:
            if self.stored_bbox is None:
                # New file: stored values absent but a current bbox exists.
                return True
            if self.current_bbox is not None and not _bboxes_equal(
                self.current_bbox, self.stored_bbox
            ):
                return True

        # Feature count is the next signal. Same new-file logic applies.
        if self.stored_feature_count is None:
            return self.current_feature_count is not None
        return bool(
            self.current_feature_count is not None
            and self.current_feature_count != self.stored_feature_count
        )

    @property
    def schema_changed(self) -> bool:
        """Check if schema fingerprint has changed."""
        if self.stored_schema_fingerprint is None:
            return True
        return self.current_schema_fingerprint != self.stored_schema_fingerprint

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
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
    """Result from checking a single file's metadata."""

    file_path: Path
    status: MetadataStatus
    message: str
    changes: list[str] = field(default_factory=list)
    fix_hint: str | None = None

    @property
    def is_ok(self) -> bool:
        """Check if metadata is up to date."""
        return self.status == MetadataStatus.FRESH

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict."""
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
    """Aggregate report of metadata check results."""

    results: list[MetadataCheckResult] = field(default_factory=list)

    @property
    def total_count(self) -> int:
        """Total number of files checked."""
        return len(self.results)

    @property
    def fresh_count(self) -> int:
        """Count of files with FRESH status."""
        return sum(1 for r in self.results if r.status == MetadataStatus.FRESH)

    @property
    def stale_count(self) -> int:
        """Count of files with STALE status."""
        return sum(1 for r in self.results if r.status == MetadataStatus.STALE)

    @property
    def missing_count(self) -> int:
        """Count of files with MISSING status."""
        return sum(1 for r in self.results if r.status == MetadataStatus.MISSING)

    @property
    def breaking_count(self) -> int:
        """Count of files with BREAKING status."""
        return sum(1 for r in self.results if r.status == MetadataStatus.BREAKING)

    @property
    def orphaned_count(self) -> int:
        """Count of files with ORPHANED status."""
        return sum(1 for r in self.results if r.status == MetadataStatus.ORPHANED)

    @property
    def passed(self) -> bool:
        """Check if all files have fresh metadata."""
        return all(r.status == MetadataStatus.FRESH for r in self.results)

    @property
    def issues(self) -> list[MetadataCheckResult]:
        """Get all non-FRESH results."""
        return [r for r in self.results if r.status != MetadataStatus.FRESH]

    def filter_by_status(self, status: MetadataStatus) -> list[MetadataCheckResult]:
        """Filter results by status."""
        return [r for r in self.results if r.status == status]

    def to_dict(self) -> dict[str, Any]:
        """Convert to JSON-serializable dict for --json output."""
        return {
            "passed": self.passed,
            "total_count": self.total_count,
            "fresh_count": self.fresh_count,
            "stale_count": self.stale_count,
            "missing_count": self.missing_count,
            "breaking_count": self.breaking_count,
            "orphaned_count": self.orphaned_count,
            "results": [r.to_dict() for r in self.results],
        }
