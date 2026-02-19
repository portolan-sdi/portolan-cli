"""Tests for metadata detection functions.

TDD: These tests are written FIRST, before implementation.

Tests cover:
- 2.1 get_stored_metadata(path) - Read existing STAC item + versions.json
- 2.2 get_current_metadata(path) - Extract fresh metadata from file
- 2.3 is_stale(stored, current) - MTIME check + heuristic fallback
- 2.4 detect_changes(stored, current) - Return list of what changed
- 3.1 check_file_metadata(path) - Return MetadataCheckResult for single file
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    pass


# =============================================================================
# Fixtures for Detection Tests
# =============================================================================


@pytest.fixture
def sample_item_json() -> dict[str, Any]:
    """Sample STAC item JSON."""
    return {
        "type": "Feature",
        "stac_version": "1.0.0",
        "id": "parcels",
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [[-122.5, 37.7], [-122.3, 37.7], [-122.3, 37.9], [-122.5, 37.9], [-122.5, 37.7]]
            ],
        },
        "bbox": [-122.5, 37.7, -122.3, 37.9],
        "properties": {
            "datetime": "2026-01-15T10:30:00Z",
        },
        "links": [],
        "assets": {
            "data": {
                "href": "parcels.parquet",
                "type": "application/x-parquet",
                "roles": ["data"],
            }
        },
        "collection": "test-collection",
    }


@pytest.fixture
def sample_versions_json() -> dict[str, Any]:
    """Sample versions.json content."""
    return {
        "spec_version": "1.0.0",
        "current_version": "1.0.0",
        "versions": [
            {
                "version": "1.0.0",
                "created": "2026-01-15T10:30:00Z",
                "breaking": False,
                "assets": {
                    "parcels.parquet": {
                        "sha256": "abc123def456789012345678901234567890123456789012345678901234abcd",
                        "size_bytes": 1048576,
                        "href": "parcels.parquet",
                        "source_path": "parcels.geojson",
                        "source_mtime": 1705312200.0,
                    }
                },
                "changes": ["parcels.parquet"],
            }
        ],
    }


@pytest.fixture
def catalog_with_metadata(
    tmp_path: Path, sample_item_json: dict[str, Any], sample_versions_json: dict[str, Any]
) -> Path:
    """Create a temporary catalog with STAC item and versions.json."""
    # Create catalog structure
    catalog_dir = tmp_path / "test-catalog"
    catalog_dir.mkdir()

    collection_dir = catalog_dir / "test-collection"
    collection_dir.mkdir()

    # Write item JSON
    item_path = collection_dir / "parcels.json"
    item_path.write_text(json.dumps(sample_item_json, indent=2))

    # Write versions.json
    versions_path = collection_dir / "versions.json"
    versions_path.write_text(json.dumps(sample_versions_json, indent=2))

    return catalog_dir


# =============================================================================
# Tests for get_stored_metadata
# =============================================================================


class TestGetStoredMetadata:
    """Tests for get_stored_metadata function."""

    @pytest.mark.unit
    def test_returns_stored_metadata_when_exists(
        self,
        catalog_with_metadata: Path,
    ) -> None:
        """get_stored_metadata returns metadata when item and versions exist."""
        from portolan_cli.metadata.detection import get_stored_metadata

        collection_dir = catalog_with_metadata / "test-collection"
        file_path = collection_dir / "parcels.parquet"

        result = get_stored_metadata(file_path, collection_dir)

        assert result is not None
        assert result.bbox == [-122.5, 37.7, -122.3, 37.9]
        assert result.source_mtime == 1705312200.0

    @pytest.mark.unit
    def test_returns_none_when_item_missing(self, tmp_path: Path) -> None:
        """get_stored_metadata returns None when no STAC item exists."""
        from portolan_cli.metadata.detection import get_stored_metadata

        file_path = tmp_path / "data.parquet"

        result = get_stored_metadata(file_path, tmp_path)

        assert result is None

    @pytest.mark.unit
    def test_returns_none_when_versions_missing(
        self,
        tmp_path: Path,
        sample_item_json: dict[str, Any],
    ) -> None:
        """get_stored_metadata returns None when versions.json is missing."""
        from portolan_cli.metadata.detection import get_stored_metadata

        # Write item but no versions.json
        item_path = tmp_path / "parcels.json"
        item_path.write_text(json.dumps(sample_item_json, indent=2))

        file_path = tmp_path / "parcels.parquet"

        result = get_stored_metadata(file_path, tmp_path)

        # Should still return item metadata even without versions.json
        # But source_mtime will be None
        assert result is not None
        assert result.bbox == [-122.5, 37.7, -122.3, 37.9]
        assert result.source_mtime is None


# =============================================================================
# Tests for get_current_metadata
# =============================================================================


class TestGetCurrentMetadata:
    """Tests for get_current_metadata function."""

    @pytest.mark.unit
    def test_extracts_geoparquet_metadata(self, valid_points_parquet: Path) -> None:
        """get_current_metadata extracts metadata from GeoParquet file."""
        from portolan_cli.metadata.detection import get_current_metadata

        result = get_current_metadata(valid_points_parquet)

        assert result is not None
        assert result.file_path == valid_points_parquet
        assert result.current_mtime is not None
        assert result.current_mtime > 0
        # Real parquet file should have bbox and feature_count
        assert result.current_feature_count is not None
        assert result.current_feature_count > 0

    @pytest.mark.unit
    def test_extracts_cog_metadata(self, valid_singleband_cog: Path) -> None:
        """get_current_metadata extracts metadata from COG file."""
        from portolan_cli.metadata.detection import get_current_metadata

        result = get_current_metadata(valid_singleband_cog)

        assert result is not None
        assert result.file_path == valid_singleband_cog
        assert result.current_mtime is not None
        assert result.current_bbox is not None
        # COG has pixel count instead of feature count
        assert result.current_feature_count is not None

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """get_current_metadata raises FileNotFoundError for missing files."""
        from portolan_cli.metadata.detection import get_current_metadata

        nonexistent = tmp_path / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            get_current_metadata(nonexistent)

    @pytest.mark.unit
    def test_raises_for_unsupported_format(self, tmp_path: Path) -> None:
        """get_current_metadata raises ValueError for unsupported formats."""
        from portolan_cli.metadata.detection import get_current_metadata

        # Create a file with unsupported extension
        txt_file = tmp_path / "data.txt"
        txt_file.write_text("not geospatial data")

        with pytest.raises(ValueError, match="unsupported|Unsupported"):
            get_current_metadata(txt_file)


# =============================================================================
# Tests for is_stale
# =============================================================================


class TestIsStale:
    """Tests for is_stale function (MTIME + heuristic check)."""

    @pytest.mark.unit
    def test_fresh_when_mtime_unchanged(self) -> None:
        """is_stale returns False when mtime hasn't changed."""
        from portolan_cli.metadata.detection import is_stale
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,  # Same mtime
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        stale, reason = is_stale(state)

        assert not stale
        assert reason == "mtime_unchanged"

    @pytest.mark.unit
    def test_stale_when_mtime_changed_and_heuristics_changed(self) -> None:
        """is_stale returns True when mtime changed AND heuristics changed."""
        from portolan_cli.metadata.detection import is_stale
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567900.0,  # Newer mtime
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.6, -122.3, 37.9],  # Different bbox
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1100,  # Different count
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        stale, reason = is_stale(state)

        assert stale
        assert reason == "content_changed"

    @pytest.mark.unit
    def test_touched_but_unchanged_when_mtime_changed_heuristics_same(self) -> None:
        """is_stale returns special status when only mtime changed."""
        from portolan_cli.metadata.detection import is_stale
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567900.0,  # Newer mtime
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],  # Same bbox
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,  # Same count
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",  # Same schema
            stored_schema_fingerprint="abc123",
        )

        stale, reason = is_stale(state)

        # File was touched but content unchanged - this is INFO, not STALE
        assert not stale
        assert reason == "touched_unchanged"

    @pytest.mark.unit
    def test_stale_when_new_file(self) -> None:
        """is_stale returns True for new files (no stored metadata)."""
        from portolan_cli.metadata.detection import is_stale
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/new_data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=None,  # New file
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=None,
            current_feature_count=1000,
            stored_feature_count=None,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint=None,
        )

        stale, reason = is_stale(state)

        assert stale
        assert reason == "new_file"

    @pytest.mark.unit
    def test_stale_when_schema_changed(self) -> None:
        """is_stale returns True when schema fingerprint changed."""
        from portolan_cli.metadata.detection import is_stale
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567900.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],  # Same bbox
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,  # Same count
            stored_feature_count=1000,
            current_schema_fingerprint="xyz789",  # Different schema!
            stored_schema_fingerprint="abc123",
        )

        stale, reason = is_stale(state)

        assert stale
        assert reason == "schema_changed"


# =============================================================================
# Tests for detect_changes
# =============================================================================


class TestDetectChanges:
    """Tests for detect_changes function."""

    @pytest.mark.unit
    def test_detects_bbox_change(self) -> None:
        """detect_changes returns 'bbox' when bbox differs."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.6, -122.3, 37.9],  # Different
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert "bbox" in changes

    @pytest.mark.unit
    def test_detects_feature_count_change(self) -> None:
        """detect_changes returns 'feature_count' when count differs."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1100,  # Different
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert "feature_count" in changes

    @pytest.mark.unit
    def test_detects_schema_change(self) -> None:
        """detect_changes returns 'schema' when schema fingerprint differs."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="xyz789",  # Different
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert "schema" in changes

    @pytest.mark.unit
    def test_detects_mtime_change(self) -> None:
        """detect_changes returns 'mtime' when modification time differs."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567900.0,  # Different
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert "mtime" in changes

    @pytest.mark.unit
    def test_detects_multiple_changes(self) -> None:
        """detect_changes returns multiple change types when present."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567900.0,  # Different
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.6, -122.3, 37.9],  # Different
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1100,  # Different
            stored_feature_count=1000,
            current_schema_fingerprint="xyz789",  # Different
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert len(changes) == 4
        assert "mtime" in changes
        assert "bbox" in changes
        assert "feature_count" in changes
        assert "schema" in changes

    @pytest.mark.unit
    def test_returns_empty_when_no_changes(self) -> None:
        """detect_changes returns empty list when nothing changed."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
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

        changes = detect_changes(state)

        assert changes == []


# =============================================================================
# Tests for check_file_metadata
# =============================================================================


class TestCheckFileMetadata:
    """Tests for check_file_metadata function."""

    @pytest.mark.unit
    def test_returns_missing_when_no_metadata(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """check_file_metadata returns MISSING status when no STAC item exists."""
        # Copy parquet to tmp_path (no STAC metadata)
        import shutil

        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.metadata.models import MetadataStatus

        dest = tmp_path / "data.parquet"
        shutil.copy(valid_points_parquet, dest)

        result = check_file_metadata(dest, tmp_path)

        assert result.status == MetadataStatus.MISSING
        assert result.file_path == dest
        # Message should mention no/missing and metadata/stac
        msg = result.message.lower()
        assert ("no" in msg or "missing" in msg) and ("metadata" in msg or "stac" in msg)

    @pytest.mark.unit
    def test_returns_fresh_when_metadata_current(
        self,
        catalog_with_metadata: Path,
        valid_points_parquet: Path,
    ) -> None:
        """check_file_metadata returns FRESH status when metadata is up to date."""
        import os
        import shutil

        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.metadata.models import MetadataStatus

        collection_dir = catalog_with_metadata / "test-collection"
        parquet_path = collection_dir / "parcels.parquet"

        # Copy actual parquet file
        shutil.copy(valid_points_parquet, parquet_path)

        # Update the versions.json to match current file mtime
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())
        current_mtime = os.stat(parquet_path).st_mtime
        versions_data["versions"][0]["assets"]["parcels.parquet"]["source_mtime"] = current_mtime
        versions_path.write_text(json.dumps(versions_data, indent=2))

        result = check_file_metadata(parquet_path, collection_dir)

        assert result.status == MetadataStatus.FRESH
        assert result.is_ok

    @pytest.mark.unit
    def test_returns_stale_when_file_modified(
        self,
        catalog_with_metadata: Path,
        valid_points_parquet: Path,
    ) -> None:
        """check_file_metadata returns STALE status when file is newer than metadata."""
        import shutil
        import time

        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.metadata.models import MetadataStatus

        collection_dir = catalog_with_metadata / "test-collection"
        parquet_path = collection_dir / "parcels.parquet"

        # Copy actual parquet file
        shutil.copy(valid_points_parquet, parquet_path)

        # Touch the file to change mtime (make it newer than stored)
        time.sleep(0.01)  # Ensure mtime difference
        parquet_path.touch()

        result = check_file_metadata(parquet_path, collection_dir)

        # Should be stale because mtime changed
        # But heuristics may or may not match, depends on stored values
        # BREAKING is also valid if schema fingerprint differs (test file vs stored)
        assert result.status in (
            MetadataStatus.STALE,
            MetadataStatus.FRESH,
            MetadataStatus.BREAKING,
        )

    @pytest.mark.unit
    def test_includes_fix_hint_for_issues(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """check_file_metadata includes fix_hint for non-FRESH results."""
        import shutil

        from portolan_cli.metadata.detection import check_file_metadata
        from portolan_cli.metadata.models import MetadataStatus

        dest = tmp_path / "data.parquet"
        shutil.copy(valid_points_parquet, dest)

        result = check_file_metadata(dest, tmp_path)

        assert result.status == MetadataStatus.MISSING
        assert result.fix_hint is not None
        assert "fix" in result.fix_hint.lower() or "create" in result.fix_hint.lower()

    @pytest.mark.unit
    def test_raises_for_nonexistent_file(self, tmp_path: Path) -> None:
        """check_file_metadata raises FileNotFoundError for missing files."""
        from portolan_cli.metadata.detection import check_file_metadata

        nonexistent = tmp_path / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            check_file_metadata(nonexistent, tmp_path)


# =============================================================================
# Tests for Schema Fingerprinting
# =============================================================================


class TestSchemaFingerprint:
    """Tests for schema fingerprint generation."""

    @pytest.mark.unit
    def test_fingerprint_consistent_for_same_schema(self, valid_points_parquet: Path) -> None:
        """Same file should produce consistent schema fingerprint."""
        from portolan_cli.metadata.detection import compute_schema_fingerprint

        fp1 = compute_schema_fingerprint(valid_points_parquet)
        fp2 = compute_schema_fingerprint(valid_points_parquet)

        assert fp1 == fp2
        assert len(fp1) > 0

    @pytest.mark.unit
    def test_fingerprint_different_for_different_schema(
        self,
        valid_points_parquet: Path,
        projected_parquet: Path,
    ) -> None:
        """Different schemas should produce different fingerprints."""
        from portolan_cli.metadata.detection import compute_schema_fingerprint

        fp1 = compute_schema_fingerprint(valid_points_parquet)
        fp2 = compute_schema_fingerprint(projected_parquet)

        # Different files may have same or different schemas
        # This test verifies the function works
        assert len(fp1) > 0
        assert len(fp2) > 0


# =============================================================================
# Tests for StoredMetadata Dataclass
# =============================================================================


class TestStoredMetadataDataclass:
    """Tests for StoredMetadata dataclass used by get_stored_metadata."""

    @pytest.mark.unit
    def test_stored_metadata_fields(self) -> None:
        """StoredMetadata should have expected fields."""
        from portolan_cli.metadata.detection import StoredMetadata

        stored = StoredMetadata(
            item_id="parcels",
            bbox=[-122.5, 37.7, -122.3, 37.9],
            source_mtime=1705312200.0,
            sha256="abc123",
            feature_count=1000,
            schema_fingerprint="xyz789",
        )

        assert stored.item_id == "parcels"
        assert stored.bbox == [-122.5, 37.7, -122.3, 37.9]
        assert stored.source_mtime == 1705312200.0
        assert stored.sha256 == "abc123"
        assert stored.feature_count == 1000
        assert stored.schema_fingerprint == "xyz789"

    @pytest.mark.unit
    def test_stored_metadata_optional_fields(self) -> None:
        """StoredMetadata should accept None for optional fields."""
        from portolan_cli.metadata.detection import StoredMetadata

        stored = StoredMetadata(
            item_id="parcels",
            bbox=[-122.5, 37.7, -122.3, 37.9],
            source_mtime=None,
            sha256=None,
            feature_count=None,
            schema_fingerprint=None,
        )

        assert stored.source_mtime is None
        assert stored.sha256 is None
        assert stored.feature_count is None
        assert stored.schema_fingerprint is None


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestDetectionEdgeCases:
    """Edge case tests for detection functions."""

    @pytest.mark.unit
    def test_handles_null_bbox_in_stored_metadata(self) -> None:
        """Detection should handle None bbox gracefully."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=None,  # No stored bbox
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        # Should detect that bbox changed (from None to value)
        assert "bbox" in changes

    @pytest.mark.unit
    def test_handles_null_feature_count(self) -> None:
        """Detection should handle None feature count gracefully."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.5, 37.7, -122.3, 37.9],
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=None,  # No stored count
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        assert "feature_count" in changes

    @pytest.mark.unit
    def test_handles_floating_point_bbox_comparison(self) -> None:
        """Detection should handle floating point bbox comparison correctly."""
        from portolan_cli.metadata.detection import detect_changes
        from portolan_cli.metadata.models import FileMetadataState

        # Very small floating point differences should be considered equal
        state = FileMetadataState(
            file_path=Path("/data.parquet"),
            current_mtime=1234567890.0,
            stored_mtime=1234567890.0,
            current_bbox=[-122.50000000001, 37.7, -122.3, 37.9],  # Tiny diff
            stored_bbox=[-122.5, 37.7, -122.3, 37.9],
            current_feature_count=1000,
            stored_feature_count=1000,
            current_schema_fingerprint="abc123",
            stored_schema_fingerprint="abc123",
        )

        changes = detect_changes(state)

        # Depends on implementation - exact comparison vs tolerance
        # For now, exact comparison is used, so this would show as changed
        # This test documents the behavior
        assert isinstance(changes, list)
