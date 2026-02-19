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
