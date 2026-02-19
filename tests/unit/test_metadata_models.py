"""Tests for metadata check data structures.

TDD: These tests are written FIRST, before implementation.
"""

from __future__ import annotations

from pathlib import Path

import pytest


class TestMetadataStatus:
    """Tests for MetadataStatus enum."""

    @pytest.mark.unit
    def test_status_values_exist(self) -> None:
        """MetadataStatus should have all expected status values."""
        from portolan_cli.metadata.models import MetadataStatus

        # All required statuses exist
        assert hasattr(MetadataStatus, "MISSING")
        assert hasattr(MetadataStatus, "FRESH")
        assert hasattr(MetadataStatus, "STALE")
        assert hasattr(MetadataStatus, "BREAKING")

    @pytest.mark.unit
    def test_status_string_values(self) -> None:
        """MetadataStatus enum values should be lowercase strings."""
        from portolan_cli.metadata.models import MetadataStatus

        assert MetadataStatus.MISSING.value == "missing"
        assert MetadataStatus.FRESH.value == "fresh"
        assert MetadataStatus.STALE.value == "stale"
        assert MetadataStatus.BREAKING.value == "breaking"

    @pytest.mark.unit
    def test_status_is_enum(self) -> None:
        """MetadataStatus should be a proper Enum."""
        from enum import Enum

        from portolan_cli.metadata.models import MetadataStatus

        assert issubclass(MetadataStatus, Enum)

    @pytest.mark.unit
    def test_status_has_exactly_four_values(self) -> None:
        """MetadataStatus should have exactly 4 values (no more, no less)."""
        from portolan_cli.metadata.models import MetadataStatus

        assert len(MetadataStatus) == 4


class TestFileMetadataState:
    """Tests for FileMetadataState dataclass."""

    @pytest.mark.unit
    def test_create_with_all_fields(self) -> None:
        """FileMetadataState should be creatable with all fields."""
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/path/to/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567880.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        assert state.file_path == Path("/path/to/data.parquet")
        assert state.current_mtime == 1234567890.0
        assert state.stored_mtime == 1234567880.0
        assert state.current_bbox == [-122.5, 37.7, -122.3, 37.9]

    @pytest.mark.unit
    def test_create_with_none_values(self) -> None:
        """FileMetadataState should accept None for optional stored values."""
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/path/to/new_file.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=None,  # No stored version yet
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=None,
            current_feature_count=1000,
            stored_feature_count=None,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint=None,
        )

        assert state.stored_mtime is None
        assert state.stored_bbox is None

    @pytest.mark.unit
    def test_is_dataclass(self) -> None:
        """FileMetadataState should be a dataclass."""
        from dataclasses import is_dataclass

        from portolan_cli.metadata.models import FileMetadataState

        assert is_dataclass(FileMetadataState)

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        """FileMetadataState should serialize to dict."""
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/path/to/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567880.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        d = state.to_dict()

        assert d["file_path"] == "/path/to/data.parquet"
        assert d["current_mtime"] == 1234567890.0
        assert d["stored_mtime"] == 1234567880.0
        assert d["current_feature_count"] == 1000

    @pytest.mark.unit
    def test_mtime_changed_property(self) -> None:
        """FileMetadataState should report if mtime changed."""
        from portolan_cli.metadata.models import FileMetadataState

        # mtime unchanged
        state_unchanged = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert not state_unchanged.mtime_changed

        # mtime changed
        state_changed = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567880.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert state_changed.mtime_changed

    @pytest.mark.unit
    def test_heuristics_changed_property(self) -> None:
        """FileMetadataState should report if heuristics (bbox/count) changed."""
        from portolan_cli.metadata.models import FileMetadataState

        # bbox changed
        state_bbox_changed = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.8],  # Different
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert state_bbox_changed.heuristics_changed

        # feature count changed
        state_count_changed = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1001,  # Different
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert state_count_changed.heuristics_changed

        # neither changed
        state_unchanged = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert not state_unchanged.heuristics_changed

    @pytest.mark.unit
    def test_schema_changed_property(self) -> None:
        """FileMetadataState should report if schema fingerprint changed."""
        from portolan_cli.metadata.models import FileMetadataState

        state_changed = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="xyz789",  # Different
        )
        assert state_changed.schema_changed

        state_unchanged = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )
        assert not state_unchanged.schema_changed


class TestMetadataCheckResult:
    """Tests for MetadataCheckResult dataclass."""

    @pytest.mark.unit
    def test_create_with_all_fields(self) -> None:
        """MetadataCheckResult should be creatable with all fields."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        result = MetadataCheckResult(
            file_path=Path("/path/to/data.parquet"),
            status=MetadataStatus.STALE,
            message="File has been modified since last check",
            changes=["mtime", "bbox"],
            fix_hint="Run 'portolan check --fix' to regenerate metadata",
        )

        assert result.file_path == Path("/path/to/data.parquet")
        assert result.status == MetadataStatus.STALE
        assert result.message == "File has been modified since last check"
        assert result.changes == ["mtime", "bbox"]
        assert result.fix_hint == "Run 'portolan check --fix' to regenerate metadata"

    @pytest.mark.unit
    def test_create_minimal(self) -> None:
        """MetadataCheckResult should work with minimal required fields."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        result = MetadataCheckResult(
            file_path=Path("/path/to/data.parquet"),
            status=MetadataStatus.FRESH,
            message="Metadata is up to date",
        )

        assert result.file_path == Path("/path/to/data.parquet")
        assert result.status == MetadataStatus.FRESH
        assert result.changes == []  # Should default to empty list
        assert result.fix_hint is None  # Should default to None

    @pytest.mark.unit
    def test_is_dataclass(self) -> None:
        """MetadataCheckResult should be a dataclass."""
        from dataclasses import is_dataclass

        from portolan_cli.metadata.models import MetadataCheckResult

        assert is_dataclass(MetadataCheckResult)

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        """MetadataCheckResult should serialize to dict."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        result = MetadataCheckResult(
            file_path=Path("/path/to/data.parquet"),
            status=MetadataStatus.STALE,
            message="File has been modified",
            changes=["mtime", "bbox"],
            fix_hint="Run check --fix",
        )

        d = result.to_dict()

        assert d["file_path"] == "/path/to/data.parquet"
        assert d["status"] == "stale"  # Enum value as string
        assert d["message"] == "File has been modified"
        assert d["changes"] == ["mtime", "bbox"]
        assert d["fix_hint"] == "Run check --fix"

    @pytest.mark.unit
    def test_to_dict_without_optional_fields(self) -> None:
        """to_dict should omit None fix_hint and empty changes."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        result = MetadataCheckResult(
            file_path=Path("/path/to/data.parquet"),
            status=MetadataStatus.FRESH,
            message="Metadata is up to date",
        )

        d = result.to_dict()

        # fix_hint should not be present if None
        assert "fix_hint" not in d
        # changes should still be present (empty list is valid)
        assert d["changes"] == []

    @pytest.mark.unit
    def test_is_ok_property(self) -> None:
        """MetadataCheckResult.is_ok should return True only for FRESH status."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        fresh = MetadataCheckResult(
            file_path=Path("/data.parquet"),
            status=MetadataStatus.FRESH,
            message="OK",
        )
        assert fresh.is_ok

        stale = MetadataCheckResult(
            file_path=Path("/data.parquet"),
            status=MetadataStatus.STALE,
            message="Stale",
        )
        assert not stale.is_ok

        missing = MetadataCheckResult(
            file_path=Path("/data.parquet"),
            status=MetadataStatus.MISSING,
            message="Missing",
        )
        assert not missing.is_ok

        breaking = MetadataCheckResult(
            file_path=Path("/data.parquet"),
            status=MetadataStatus.BREAKING,
            message="Breaking",
        )
        assert not breaking.is_ok


class TestMetadataReport:
    """Tests for MetadataReport dataclass."""

    @pytest.mark.unit
    def test_create_empty(self) -> None:
        """MetadataReport should be creatable with no results."""
        from portolan_cli.metadata.models import MetadataReport

        report = MetadataReport(results=[])

        assert report.results == []

    @pytest.mark.unit
    def test_create_with_results(self) -> None:
        """MetadataReport should hold multiple results."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(
                file_path=Path("/data1.parquet"),
                status=MetadataStatus.FRESH,
                message="OK",
            ),
            MetadataCheckResult(
                file_path=Path("/data2.parquet"),
                status=MetadataStatus.STALE,
                message="Stale",
            ),
        ]

        report = MetadataReport(results=results)

        assert len(report.results) == 2
        assert report.results[0].file_path == Path("/data1.parquet")
        assert report.results[1].file_path == Path("/data2.parquet")

    @pytest.mark.unit
    def test_is_dataclass(self) -> None:
        """MetadataReport should be a dataclass."""
        from dataclasses import is_dataclass

        from portolan_cli.metadata.models import MetadataReport

        assert is_dataclass(MetadataReport)

    @pytest.mark.unit
    def test_count_properties(self) -> None:
        """MetadataReport should provide counts by status."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f3.parquet"), MetadataStatus.STALE, "Stale"),
            MetadataCheckResult(Path("/f4.parquet"), MetadataStatus.MISSING, "Missing"),
            MetadataCheckResult(Path("/f5.parquet"), MetadataStatus.BREAKING, "Break"),
        ]

        report = MetadataReport(results=results)

        assert report.fresh_count == 2
        assert report.stale_count == 1
        assert report.missing_count == 1
        assert report.breaking_count == 1
        assert report.total_count == 5

    @pytest.mark.unit
    def test_passed_property(self) -> None:
        """MetadataReport.passed should be True only if all results are FRESH."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        # All fresh = passed
        all_fresh = MetadataReport(
            results=[
                MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
                MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.FRESH, "OK"),
            ]
        )
        assert all_fresh.passed

        # Empty = passed (vacuously true)
        empty = MetadataReport(results=[])
        assert empty.passed

        # Any non-fresh = not passed
        with_stale = MetadataReport(
            results=[
                MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
                MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.STALE, "Stale"),
            ]
        )
        assert not with_stale.passed

    @pytest.mark.unit
    def test_issues_property(self) -> None:
        """MetadataReport.issues should return non-FRESH results."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.STALE, "Stale"),
            MetadataCheckResult(Path("/f3.parquet"), MetadataStatus.MISSING, "Missing"),
        ]

        report = MetadataReport(results=results)
        issues = report.issues

        assert len(issues) == 2
        assert all(r.status != MetadataStatus.FRESH for r in issues)

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        """MetadataReport should serialize to dict for JSON output."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.STALE, "Stale"),
        ]

        report = MetadataReport(results=results)
        d = report.to_dict()

        assert d["passed"] is False
        assert d["total_count"] == 2
        assert d["fresh_count"] == 1
        assert d["stale_count"] == 1
        assert d["missing_count"] == 0
        assert d["breaking_count"] == 0
        assert len(d["results"]) == 2
        assert d["results"][0]["status"] == "fresh"
        assert d["results"][1]["status"] == "stale"

    @pytest.mark.unit
    def test_filter_by_status(self) -> None:
        """MetadataReport should allow filtering results by status."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.STALE, "Stale"),
            MetadataCheckResult(Path("/f3.parquet"), MetadataStatus.STALE, "Stale2"),
            MetadataCheckResult(Path("/f4.parquet"), MetadataStatus.MISSING, "Missing"),
        ]

        report = MetadataReport(results=results)

        stale_only = report.filter_by_status(MetadataStatus.STALE)
        assert len(stale_only) == 2
        assert all(r.status == MetadataStatus.STALE for r in stale_only)

        missing_only = report.filter_by_status(MetadataStatus.MISSING)
        assert len(missing_only) == 1

        fresh_only = report.filter_by_status(MetadataStatus.FRESH)
        assert len(fresh_only) == 1


class TestMetadataStatusSeverity:
    """Tests for metadata status severity ordering."""

    @pytest.mark.unit
    def test_status_severity_ordering(self) -> None:
        """MetadataStatus should have a sensible severity ordering.

        BREAKING > MISSING > STALE > FRESH
        This allows sorting issues by severity.
        """
        from portolan_cli.metadata.models import MetadataStatus

        # Verify severity can be compared (relies on enum ordering)
        assert MetadataStatus.BREAKING.severity > MetadataStatus.MISSING.severity
        assert MetadataStatus.MISSING.severity > MetadataStatus.STALE.severity
        assert MetadataStatus.STALE.severity > MetadataStatus.FRESH.severity


class TestEdgeCases:
    """Edge case tests for metadata models."""

    @pytest.mark.unit
    def test_file_metadata_state_with_new_file(self) -> None:
        """FileMetadataState should handle new files (no stored values)."""
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/new_file.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=None,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=None,
            current_feature_count=1000,
            stored_feature_count=None,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint=None,
        )

        # New file should show as changed since there's no stored version
        assert state.mtime_changed  # No stored mtime = changed
        assert state.heuristics_changed  # No stored values = changed
        assert state.schema_changed  # No stored fingerprint = changed

    @pytest.mark.unit
    def test_metadata_check_result_with_empty_changes_list(self) -> None:
        """MetadataCheckResult with FRESH status should have empty changes."""
        from portolan_cli.metadata.models import MetadataCheckResult, MetadataStatus

        result = MetadataCheckResult(
            file_path=Path("/data.parquet"),
            status=MetadataStatus.FRESH,
            message="Metadata is up to date",
            changes=[],
        )

        assert result.changes == []
        assert result.is_ok

    @pytest.mark.unit
    def test_metadata_report_with_only_fresh(self) -> None:
        """MetadataReport with all FRESH should pass and have no issues."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.FRESH, "OK"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.FRESH, "OK"),
        ]

        report = MetadataReport(results=results)

        assert report.passed
        assert report.issues == []
        assert report.fresh_count == 2
        assert report.stale_count == 0

    @pytest.mark.unit
    def test_metadata_report_with_all_breaking(self) -> None:
        """MetadataReport with all BREAKING should fail."""
        from portolan_cli.metadata.models import (
            MetadataCheckResult,
            MetadataReport,
            MetadataStatus,
        )

        results = [
            MetadataCheckResult(Path("/f1.parquet"), MetadataStatus.BREAKING, "Breaking"),
            MetadataCheckResult(Path("/f2.parquet"), MetadataStatus.BREAKING, "Breaking"),
        ]

        report = MetadataReport(results=results)

        assert not report.passed
        assert len(report.issues) == 2
        assert report.breaking_count == 2
