"""Tests for metadata fix functions."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.metadata.models import (
    MetadataCheckResult,
    MetadataReport,
    MetadataStatus,
)


def _create_collection_json(collection_dir: Path) -> None:
    """Create a minimal collection.json for testing."""
    collection_data = {
        "type": "Collection",
        "stac_version": "1.0.0",
        "id": collection_dir.name,
        "description": "Test collection",
        "license": "CC-BY-4.0",
        "extent": {
            "spatial": {"bbox": [[-180, -90, 180, 90]]},
            "temporal": {"interval": [["2020-01-01T00:00:00Z", None]]},
        },
        "links": [],
    }
    (collection_dir / "collection.json").write_text(json.dumps(collection_data))


class TestFixMetadataFunction:
    """Tests for fix_metadata function."""

    @pytest.mark.unit
    def test_fix_creates_missing_items(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata creates STAC items for files with MISSING status."""
        import shutil

        from portolan_cli.metadata.fix import fix_metadata

        # Set up collection directory with parquet file but no metadata
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        _create_collection_json(collection_dir)
        parquet_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, parquet_path)

        # Create report with MISSING status
        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet_path,
                    status=MetadataStatus.MISSING,
                    message="No STAC metadata found",
                    fix_hint="Run portolan fix",
                )
            ]
        )

        # Run fix
        fix_report = fix_metadata(collection_dir, report)

        # Check item was created
        item_path = collection_dir / "data.json"
        assert item_path.exists()
        assert fix_report.total_count == 1
        assert fix_report.success_count == 1

    @pytest.mark.unit
    def test_fix_skips_fresh_files(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata skips files with FRESH status."""
        import shutil

        from portolan_cli.metadata.fix import fix_metadata

        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        parquet_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, parquet_path)

        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet_path,
                    status=MetadataStatus.FRESH,
                    message="Metadata is up to date",
                )
            ]
        )

        fix_report = fix_metadata(collection_dir, report)
        assert fix_report.total_count == 0
        assert fix_report.skipped_count == 1


class TestFixReport:
    """Tests for FixReport dataclass."""

    @pytest.mark.unit
    def test_fix_report_counts(self) -> None:
        """FixReport correctly counts successes and failures."""
        from portolan_cli.metadata.fix import FixAction, FixReport, FixResult

        report = FixReport(
            results=[
                FixResult(
                    file_path=Path("a.parquet"),
                    action=FixAction.CREATED,
                    success=True,
                    message="Created item",
                ),
                FixResult(
                    file_path=Path("b.parquet"),
                    action=FixAction.CREATED,
                    success=False,
                    message="Failed",
                ),
            ],
            skipped_count=2,
        )

        assert report.total_count == 2
        assert report.success_count == 1
        assert report.failure_count == 1
        assert report.skipped_count == 2

    @pytest.mark.unit
    def test_fix_report_to_dict(self) -> None:
        """FixReport.to_dict() returns JSON-serializable output."""
        from portolan_cli.metadata.fix import FixAction, FixReport, FixResult

        report = FixReport(
            results=[
                FixResult(
                    file_path=Path("test.parquet"),
                    action=FixAction.CREATED,
                    success=True,
                    message="Created item",
                ),
            ],
            skipped_count=1,
        )

        result = report.to_dict()
        assert "total_count" in result
        assert "success_count" in result
        assert "results" in result


class TestFixStaleAndBreaking:
    """Tests for fixing STALE and BREAKING metadata."""

    @pytest.mark.unit
    def test_fix_updates_stale_items(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata updates existing STAC items for files with STALE status."""
        import shutil

        from portolan_cli.metadata.fix import FixAction, fix_metadata

        # Set up collection with existing item
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        _create_collection_json(collection_dir)
        parquet_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, parquet_path)

        # Create existing STAC item that we'll update
        existing_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "data",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {},
            "links": [],
            "collection": "test-collection",
        }
        (collection_dir / "data.json").write_text(json.dumps(existing_item))

        # Create report with STALE status
        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet_path,
                    status=MetadataStatus.STALE,
                    message="Metadata is stale: mtime, bbox",
                    changes=["mtime", "bbox"],
                    fix_hint="Run portolan fix",
                )
            ]
        )

        # Run fix
        fix_report = fix_metadata(collection_dir, report)

        # Verify update succeeded
        assert fix_report.total_count == 1
        assert fix_report.success_count == 1
        assert fix_report.results[0].action == FixAction.UPDATED
        assert "Updated STAC item" in fix_report.results[0].message

        # Verify item was actually updated (bbox should have changed)
        updated_item = json.loads((collection_dir / "data.json").read_text())
        assert updated_item["bbox"] != [0, 0, 0, 0]

    @pytest.mark.unit
    def test_fix_updates_breaking_items(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata updates items with BREAKING status (schema change)."""
        import shutil

        from portolan_cli.metadata.fix import FixAction, fix_metadata

        # Set up collection with existing item
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        _create_collection_json(collection_dir)
        parquet_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, parquet_path)

        # Create existing STAC item
        existing_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "data",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {},
            "links": [],
            "collection": "test-collection",
        }
        (collection_dir / "data.json").write_text(json.dumps(existing_item))

        # Create report with BREAKING status
        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet_path,
                    status=MetadataStatus.BREAKING,
                    message="Schema has breaking changes",
                    changes=["schema"],
                    fix_hint="Run 'portolan fix --breaking'",
                )
            ]
        )

        # Run fix
        fix_report = fix_metadata(collection_dir, report)

        # Verify update succeeded with breaking message
        assert fix_report.total_count == 1
        assert fix_report.success_count == 1
        assert fix_report.results[0].action == FixAction.UPDATED
        assert "breaking" in fix_report.results[0].message.lower()

    @pytest.mark.unit
    def test_fix_dry_run_does_not_modify_files(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata with dry_run=True reports actions without making changes."""
        import shutil

        from portolan_cli.metadata.fix import FixAction, fix_metadata

        # Set up collection without item
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        _create_collection_json(collection_dir)
        parquet_path = collection_dir / "data.parquet"
        shutil.copy(valid_points_parquet, parquet_path)

        # Create report with MISSING status
        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet_path,
                    status=MetadataStatus.MISSING,
                    message="No STAC metadata found",
                )
            ]
        )

        # Run fix with dry_run=True
        fix_report = fix_metadata(collection_dir, report, dry_run=True)

        # Verify dry run reports but doesn't create file
        assert fix_report.total_count == 1
        assert fix_report.results[0].action == FixAction.CREATED
        assert "dry run" in fix_report.results[0].message.lower()
        assert not (collection_dir / "data.json").exists()

    @pytest.mark.unit
    def test_fix_handles_mixed_statuses(
        self,
        valid_points_parquet: Path,
        tmp_path: Path,
    ) -> None:
        """fix_metadata correctly handles a mix of FRESH, MISSING, and STALE."""
        import shutil

        from portolan_cli.metadata.fix import FixAction, fix_metadata

        # Set up collection
        collection_dir = tmp_path / "test-collection"
        collection_dir.mkdir()
        _create_collection_json(collection_dir)

        # Create two parquet files
        parquet1 = collection_dir / "fresh.parquet"
        parquet2 = collection_dir / "missing.parquet"
        parquet3 = collection_dir / "stale.parquet"
        shutil.copy(valid_points_parquet, parquet1)
        shutil.copy(valid_points_parquet, parquet2)
        shutil.copy(valid_points_parquet, parquet3)

        # Create existing item for stale file
        existing_item = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": "stale",
            "geometry": {"type": "Point", "coordinates": [0, 0]},
            "bbox": [0, 0, 0, 0],
            "properties": {"datetime": "2020-01-01T00:00:00Z"},
            "assets": {},
            "links": [],
            "collection": "test-collection",
        }
        (collection_dir / "stale.json").write_text(json.dumps(existing_item))

        # Create report with mixed statuses
        report = MetadataReport(
            results=[
                MetadataCheckResult(
                    file_path=parquet1,
                    status=MetadataStatus.FRESH,
                    message="Up to date",
                ),
                MetadataCheckResult(
                    file_path=parquet2,
                    status=MetadataStatus.MISSING,
                    message="No metadata",
                ),
                MetadataCheckResult(
                    file_path=parquet3,
                    status=MetadataStatus.STALE,
                    message="Outdated",
                    changes=["mtime"],
                ),
            ]
        )

        # Run fix
        fix_report = fix_metadata(collection_dir, report)

        # Verify: 1 skipped (FRESH), 1 created (MISSING), 1 updated (STALE)
        assert fix_report.skipped_count == 1
        assert fix_report.total_count == 2
        assert fix_report.success_count == 2

        # Check actions
        actions = {r.file_path.name: r.action for r in fix_report.results}
        assert actions["missing.parquet"] == FixAction.CREATED
        assert actions["stale.parquet"] == FixAction.UPDATED
