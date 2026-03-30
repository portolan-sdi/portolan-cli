"""Unit tests for ArcGIS extraction report models.

Tests the dataclasses for extraction reporting:
- LayerResult: Individual layer extraction status
- ExtractionSummary: Aggregate statistics
- MetadataExtracted: Harvested ArcGIS metadata
- ExtractionReport: Full extraction report

Following TDD: tests written before implementation.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from portolan_cli.extract.arcgis.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)

if TYPE_CHECKING:
    pass


class TestLayerResult:
    """Tests for LayerResult dataclass."""

    def test_success_layer_creation(self) -> None:
        """Create a successful layer result with all fields."""
        layer = LayerResult(
            id=0,
            name="Census_Block_Groups",
            status="success",
            features=1336,
            size_bytes=1949696,
            duration_seconds=12.4,
            output_path="census_block_groups/census_block_groups.parquet",
            warnings=[],
            error=None,
            attempts=1,
        )

        assert layer.id == 0
        assert layer.name == "Census_Block_Groups"
        assert layer.status == "success"
        assert layer.features == 1336
        assert layer.size_bytes == 1949696
        assert layer.duration_seconds == 12.4
        assert layer.output_path == "census_block_groups/census_block_groups.parquet"
        assert layer.warnings == []
        assert layer.error is None
        assert layer.attempts == 1

    def test_failed_layer_creation(self) -> None:
        """Create a failed layer result with error details."""
        layer = LayerResult(
            id=5,
            name="Problematic_Layer",
            status="failed",
            features=None,
            size_bytes=None,
            duration_seconds=None,
            output_path=None,
            warnings=[],
            error="Timeout after 3 retries",
            attempts=3,
        )

        assert layer.id == 5
        assert layer.status == "failed"
        assert layer.error == "Timeout after 3 retries"
        assert layer.attempts == 3
        assert layer.features is None
        assert layer.size_bytes is None

    def test_skipped_layer_creation(self) -> None:
        """Create a skipped layer result (e.g., from resume)."""
        layer = LayerResult(
            id=2,
            name="Previously_Succeeded",
            status="skipped",
            features=500,
            size_bytes=123456,
            duration_seconds=5.0,
            output_path="path/to/data.parquet",
            warnings=[],
            error=None,
            attempts=0,
        )

        assert layer.status == "skipped"
        assert layer.attempts == 0

    def test_layer_with_warnings(self) -> None:
        """Layer with warnings still succeeds."""
        layer = LayerResult(
            id=3,
            name="Layer_With_Warnings",
            status="success",
            features=100,
            size_bytes=50000,
            duration_seconds=3.2,
            output_path="layer/data.parquet",
            warnings=["Empty geometry in 5 features", "Truncated field values"],
            error=None,
            attempts=2,
        )

        assert layer.status == "success"
        assert len(layer.warnings) == 2
        assert "Empty geometry" in layer.warnings[0]

    def test_to_dict_success(self) -> None:
        """Serialize successful layer to dict."""
        layer = LayerResult(
            id=0,
            name="Test_Layer",
            status="success",
            features=100,
            size_bytes=50000,
            duration_seconds=2.5,
            output_path="test/data.parquet",
            warnings=["warning1"],
            error=None,
            attempts=1,
        )

        result = layer.to_dict()

        assert result["id"] == 0
        assert result["name"] == "Test_Layer"
        assert result["status"] == "success"
        assert result["features"] == 100
        assert result["size_bytes"] == 50000
        assert result["duration_seconds"] == 2.5
        assert result["output_path"] == "test/data.parquet"
        assert result["warnings"] == ["warning1"]
        assert "error" not in result  # None values should be excluded
        assert result["attempts"] == 1

    def test_to_dict_failed(self) -> None:
        """Serialize failed layer to dict includes error."""
        layer = LayerResult(
            id=1,
            name="Failed_Layer",
            status="failed",
            features=None,
            size_bytes=None,
            duration_seconds=None,
            output_path=None,
            warnings=[],
            error="Connection refused",
            attempts=3,
        )

        result = layer.to_dict()

        assert result["status"] == "failed"
        assert result["error"] == "Connection refused"
        assert "features" not in result  # None values excluded
        assert "size_bytes" not in result
        assert "duration_seconds" not in result
        assert "output_path" not in result

    def test_from_dict_success(self) -> None:
        """Deserialize successful layer from dict."""
        data = {
            "id": 0,
            "name": "Test_Layer",
            "status": "success",
            "features": 200,
            "size_bytes": 100000,
            "duration_seconds": 5.0,
            "output_path": "test/output.parquet",
            "warnings": [],
            "attempts": 1,
        }

        layer = LayerResult.from_dict(data)

        assert layer.id == 0
        assert layer.name == "Test_Layer"
        assert layer.status == "success"
        assert layer.features == 200
        assert layer.error is None

    def test_from_dict_failed(self) -> None:
        """Deserialize failed layer from dict."""
        data = {
            "id": 5,
            "name": "Failed_Layer",
            "status": "failed",
            "error": "Timeout",
            "warnings": [],
            "attempts": 3,
        }

        layer = LayerResult.from_dict(data)

        assert layer.id == 5
        assert layer.status == "failed"
        assert layer.error == "Timeout"
        assert layer.features is None
        assert layer.output_path is None

    def test_roundtrip_serialization(self) -> None:
        """Layer survives JSON roundtrip."""
        original = LayerResult(
            id=7,
            name="Roundtrip_Test",
            status="success",
            features=1000,
            size_bytes=500000,
            duration_seconds=15.7,
            output_path="roundtrip/test.parquet",
            warnings=["warning"],
            error=None,
            attempts=1,
        )

        json_str = json.dumps(original.to_dict())
        restored = LayerResult.from_dict(json.loads(json_str))

        assert restored.id == original.id
        assert restored.name == original.name
        assert restored.status == original.status
        assert restored.features == original.features
        assert restored.size_bytes == original.size_bytes
        assert restored.duration_seconds == original.duration_seconds
        assert restored.output_path == original.output_path
        assert restored.warnings == original.warnings
        assert restored.error == original.error
        assert restored.attempts == original.attempts


class TestExtractionSummary:
    """Tests for ExtractionSummary dataclass."""

    def test_summary_creation(self) -> None:
        """Create extraction summary with all fields."""
        summary = ExtractionSummary(
            total_layers=10,
            succeeded=8,
            failed=1,
            skipped=1,
            total_features=45000,
            total_size_bytes=52428800,
            total_duration_seconds=180.5,
        )

        assert summary.total_layers == 10
        assert summary.succeeded == 8
        assert summary.failed == 1
        assert summary.skipped == 1
        assert summary.total_features == 45000
        assert summary.total_size_bytes == 52428800
        assert summary.total_duration_seconds == 180.5

    def test_all_succeeded(self) -> None:
        """Summary where all layers succeeded."""
        summary = ExtractionSummary(
            total_layers=5,
            succeeded=5,
            failed=0,
            skipped=0,
            total_features=10000,
            total_size_bytes=5000000,
            total_duration_seconds=60.0,
        )

        assert summary.succeeded == summary.total_layers
        assert summary.failed == 0

    def test_to_dict(self) -> None:
        """Serialize summary to dict."""
        summary = ExtractionSummary(
            total_layers=3,
            succeeded=2,
            failed=1,
            skipped=0,
            total_features=500,
            total_size_bytes=100000,
            total_duration_seconds=10.0,
        )

        result = summary.to_dict()

        assert result["total_layers"] == 3
        assert result["succeeded"] == 2
        assert result["failed"] == 1
        assert result["skipped"] == 0
        assert result["total_features"] == 500
        assert result["total_size_bytes"] == 100000
        assert result["total_duration_seconds"] == 10.0

    def test_from_dict(self) -> None:
        """Deserialize summary from dict."""
        data = {
            "total_layers": 10,
            "succeeded": 9,
            "failed": 1,
            "skipped": 0,
            "total_features": 50000,
            "total_size_bytes": 10000000,
            "total_duration_seconds": 300.0,
        }

        summary = ExtractionSummary.from_dict(data)

        assert summary.total_layers == 10
        assert summary.succeeded == 9
        assert summary.failed == 1

    def test_roundtrip_serialization(self) -> None:
        """Summary survives JSON roundtrip."""
        original = ExtractionSummary(
            total_layers=7,
            succeeded=5,
            failed=2,
            skipped=0,
            total_features=25000,
            total_size_bytes=12500000,
            total_duration_seconds=120.0,
        )

        json_str = json.dumps(original.to_dict())
        restored = ExtractionSummary.from_dict(json.loads(json_str))

        assert restored.total_layers == original.total_layers
        assert restored.succeeded == original.succeeded
        assert restored.failed == original.failed
        assert restored.total_features == original.total_features


class TestMetadataExtracted:
    """Tests for MetadataExtracted dataclass."""

    def test_full_metadata_creation(self) -> None:
        """Create metadata with all fields populated."""
        metadata = MetadataExtracted(
            source_url="https://services.arcgis.com/test/FeatureServer",
            attribution="City of Philadelphia",
            keywords=["census", "demographics"],
            contact_name="GIS Department",
            processing_notes="Exported from ArcGIS Server",
            known_issues="Some features may be outdated",
            license_info_raw="Public domain - no restrictions",
        )

        assert metadata.source_url == "https://services.arcgis.com/test/FeatureServer"
        assert metadata.attribution == "City of Philadelphia"
        assert metadata.keywords == ["census", "demographics"]
        assert metadata.contact_name == "GIS Department"
        assert metadata.processing_notes == "Exported from ArcGIS Server"
        assert metadata.known_issues == "Some features may be outdated"
        assert metadata.license_info_raw == "Public domain - no restrictions"

    def test_minimal_metadata(self) -> None:
        """Create metadata with only required field."""
        metadata = MetadataExtracted(
            source_url="https://services.arcgis.com/test/FeatureServer",
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        )

        assert metadata.source_url == "https://services.arcgis.com/test/FeatureServer"
        assert metadata.attribution is None
        assert metadata.keywords is None

    def test_to_dict_full(self) -> None:
        """Serialize full metadata to dict."""
        metadata = MetadataExtracted(
            source_url="https://test.com/FeatureServer",
            attribution="Test Org",
            keywords=["test", "data"],
            contact_name="Test User",
            processing_notes="Notes",
            known_issues="Issues",
            license_info_raw="MIT",
        )

        result = metadata.to_dict()

        assert result["source_url"] == "https://test.com/FeatureServer"
        assert result["attribution"] == "Test Org"
        assert result["keywords"] == ["test", "data"]
        assert result["contact_name"] == "Test User"
        assert result["processing_notes"] == "Notes"
        assert result["known_issues"] == "Issues"
        assert result["license_info_raw"] == "MIT"

    def test_to_dict_with_nulls(self) -> None:
        """Serialize metadata with null values."""
        metadata = MetadataExtracted(
            source_url="https://test.com/FeatureServer",
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        )

        result = metadata.to_dict()

        # Null values should still be included (for documentation of what was checked)
        assert result["source_url"] == "https://test.com/FeatureServer"
        assert result["attribution"] is None
        assert result["keywords"] is None

    def test_from_dict(self) -> None:
        """Deserialize metadata from dict."""
        data = {
            "source_url": "https://example.com/FeatureServer",
            "attribution": "Example Org",
            "keywords": ["key1", "key2"],
            "contact_name": None,
            "processing_notes": None,
            "known_issues": None,
            "license_info_raw": "CC-BY-4.0",
        }

        metadata = MetadataExtracted.from_dict(data)

        assert metadata.source_url == "https://example.com/FeatureServer"
        assert metadata.attribution == "Example Org"
        assert metadata.keywords == ["key1", "key2"]
        assert metadata.contact_name is None
        assert metadata.license_info_raw == "CC-BY-4.0"

    def test_roundtrip_serialization(self) -> None:
        """Metadata survives JSON roundtrip."""
        original = MetadataExtracted(
            source_url="https://roundtrip.com/FeatureServer",
            attribution="Roundtrip Org",
            keywords=["round", "trip"],
            contact_name="Tester",
            processing_notes="Test notes",
            known_issues=None,
            license_info_raw="Public domain",
        )

        json_str = json.dumps(original.to_dict())
        restored = MetadataExtracted.from_dict(json.loads(json_str))

        assert restored.source_url == original.source_url
        assert restored.attribution == original.attribution
        assert restored.keywords == original.keywords
        assert restored.contact_name == original.contact_name


class TestExtractionReport:
    """Tests for ExtractionReport dataclass."""

    def test_full_report_creation(self) -> None:
        """Create a complete extraction report."""
        metadata = MetadataExtracted(
            source_url="https://services.arcgis.com/test/FeatureServer",
            attribution="City of Philadelphia",
            keywords=["census"],
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw="Public domain",
        )

        layers = [
            LayerResult(
                id=0,
                name="Layer_0",
                status="success",
                features=1000,
                size_bytes=500000,
                duration_seconds=10.0,
                output_path="layer_0/data.parquet",
                warnings=[],
                error=None,
                attempts=1,
            ),
            LayerResult(
                id=1,
                name="Layer_1",
                status="failed",
                features=None,
                size_bytes=None,
                duration_seconds=None,
                output_path=None,
                warnings=[],
                error="Timeout",
                attempts=3,
            ),
        ]

        summary = ExtractionSummary(
            total_layers=2,
            succeeded=1,
            failed=1,
            skipped=0,
            total_features=1000,
            total_size_bytes=500000,
            total_duration_seconds=30.0,
        )

        report = ExtractionReport(
            extraction_date="2026-03-30T14:30:00Z",
            source_url="https://services.arcgis.com/test/FeatureServer",
            portolan_version="0.4.0",
            gpio_version="0.2.0",
            metadata_extracted=metadata,
            layers=layers,
            summary=summary,
        )

        assert report.extraction_date == "2026-03-30T14:30:00Z"
        assert report.source_url == "https://services.arcgis.com/test/FeatureServer"
        assert report.portolan_version == "0.4.0"
        assert report.gpio_version == "0.2.0"
        assert report.metadata_extracted == metadata
        assert len(report.layers) == 2
        assert report.summary == summary

    def test_to_dict(self) -> None:
        """Serialize report to dict."""
        report = _create_sample_report()

        result = report.to_dict()

        assert result["extraction_date"] == "2026-03-30T14:30:00Z"
        assert result["source_url"] == "https://test.com/FeatureServer"
        assert result["portolan_version"] == "0.4.0"
        assert result["gpio_version"] == "0.2.0"
        assert "metadata_extracted" in result
        assert "layers" in result
        assert "summary" in result
        assert len(result["layers"]) == 1

    def test_from_dict(self) -> None:
        """Deserialize report from dict."""
        data = {
            "extraction_date": "2026-03-30T14:30:00Z",
            "source_url": "https://example.com/FeatureServer",
            "portolan_version": "0.4.0",
            "gpio_version": "0.2.0",
            "metadata_extracted": {
                "source_url": "https://example.com/FeatureServer",
                "attribution": "Test",
                "keywords": None,
                "contact_name": None,
                "processing_notes": None,
                "known_issues": None,
                "license_info_raw": None,
            },
            "layers": [
                {
                    "id": 0,
                    "name": "Test_Layer",
                    "status": "success",
                    "features": 100,
                    "size_bytes": 50000,
                    "duration_seconds": 5.0,
                    "output_path": "test/data.parquet",
                    "warnings": [],
                    "attempts": 1,
                }
            ],
            "summary": {
                "total_layers": 1,
                "succeeded": 1,
                "failed": 0,
                "skipped": 0,
                "total_features": 100,
                "total_size_bytes": 50000,
                "total_duration_seconds": 5.0,
            },
        }

        report = ExtractionReport.from_dict(data)

        assert report.extraction_date == "2026-03-30T14:30:00Z"
        assert report.source_url == "https://example.com/FeatureServer"
        assert len(report.layers) == 1
        assert report.layers[0].name == "Test_Layer"
        assert report.summary.total_layers == 1

    def test_roundtrip_serialization(self) -> None:
        """Report survives JSON roundtrip."""
        original = _create_sample_report()

        json_str = json.dumps(original.to_dict())
        restored = ExtractionReport.from_dict(json.loads(json_str))

        assert restored.extraction_date == original.extraction_date
        assert restored.source_url == original.source_url
        assert restored.portolan_version == original.portolan_version
        assert len(restored.layers) == len(original.layers)
        assert restored.summary.total_layers == original.summary.total_layers


class TestReportIO:
    """Tests for save_report and load_report functions."""

    def test_save_report(self, tmp_path: Path) -> None:
        """Save report to JSON file."""
        report = _create_sample_report()
        report_path = tmp_path / ".portolan" / "extraction-report.json"
        report_path.parent.mkdir(parents=True)

        save_report(report, report_path)

        assert report_path.exists()
        content = json.loads(report_path.read_text())
        assert content["extraction_date"] == "2026-03-30T14:30:00Z"
        assert content["source_url"] == "https://test.com/FeatureServer"

    def test_load_report(self, tmp_path: Path) -> None:
        """Load report from JSON file."""
        report_path = tmp_path / "extraction-report.json"
        data = {
            "extraction_date": "2026-03-30T14:30:00Z",
            "source_url": "https://load.com/FeatureServer",
            "portolan_version": "0.4.0",
            "gpio_version": "0.2.0",
            "metadata_extracted": {
                "source_url": "https://load.com/FeatureServer",
                "attribution": None,
                "keywords": None,
                "contact_name": None,
                "processing_notes": None,
                "known_issues": None,
                "license_info_raw": None,
            },
            "layers": [],
            "summary": {
                "total_layers": 0,
                "succeeded": 0,
                "failed": 0,
                "skipped": 0,
                "total_features": 0,
                "total_size_bytes": 0,
                "total_duration_seconds": 0.0,
            },
        }
        report_path.write_text(json.dumps(data))

        report = load_report(report_path)

        assert report.source_url == "https://load.com/FeatureServer"
        assert report.extraction_date == "2026-03-30T14:30:00Z"

    def test_save_load_roundtrip(self, tmp_path: Path) -> None:
        """Report survives save/load roundtrip."""
        original = _create_sample_report()
        report_path = tmp_path / "extraction-report.json"

        save_report(original, report_path)
        restored = load_report(report_path)

        assert restored.extraction_date == original.extraction_date
        assert restored.source_url == original.source_url
        assert restored.portolan_version == original.portolan_version
        assert len(restored.layers) == len(original.layers)

    def test_load_nonexistent_raises(self, tmp_path: Path) -> None:
        """Loading nonexistent file raises FileNotFoundError."""
        report_path = tmp_path / "nonexistent.json"

        with pytest.raises(FileNotFoundError):
            load_report(report_path)

    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Save creates parent directories if needed."""
        report = _create_sample_report()
        report_path = tmp_path / "deep" / "nested" / "extraction-report.json"

        save_report(report, report_path)

        assert report_path.exists()


def _create_sample_report() -> ExtractionReport:
    """Create a sample report for testing."""
    metadata = MetadataExtracted(
        source_url="https://test.com/FeatureServer",
        attribution="Test Org",
        keywords=["test"],
        contact_name=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )

    layers = [
        LayerResult(
            id=0,
            name="Test_Layer",
            status="success",
            features=100,
            size_bytes=50000,
            duration_seconds=5.0,
            output_path="test/data.parquet",
            warnings=[],
            error=None,
            attempts=1,
        )
    ]

    summary = ExtractionSummary(
        total_layers=1,
        succeeded=1,
        failed=0,
        skipped=0,
        total_features=100,
        total_size_bytes=50000,
        total_duration_seconds=5.0,
    )

    return ExtractionReport(
        extraction_date="2026-03-30T14:30:00Z",
        source_url="https://test.com/FeatureServer",
        portolan_version="0.4.0",
        gpio_version="0.2.0",
        metadata_extracted=metadata,
        layers=layers,
        summary=summary,
    )
