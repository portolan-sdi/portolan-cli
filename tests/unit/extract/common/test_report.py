from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    FolderCoverage,
    LayerResult,
    MetadataExtracted,
    load_report,
    save_report,
)


@pytest.mark.unit
def test_folder_coverage_roundtrip() -> None:
    cov = FolderCoverage(
        folders_visited=["A", "B"],
        folders_skipped=[("Locked", "499 Token Required")],
        services_found=5,
    )
    d = cov.to_dict()
    assert d["folders_visited"] == ["A", "B"]
    assert d["folders_skipped"] == [{"folder": "Locked", "reason": "499 Token Required"}]
    assert d["services_found"] == 5
    back = FolderCoverage.from_dict(d)
    assert back == cov


@pytest.mark.unit
def test_extraction_report_folder_coverage_save_load_roundtrip(tmp_path: Path) -> None:
    """ExtractionReport with folder_coverage serialises and deserialises exactly."""
    coverage = FolderCoverage(
        folders_visited=["NationalDatasets", "Boundaries"],
        folders_skipped=[("Restricted", "499 Token Required")],
        services_found=7,
    )
    report = ExtractionReport(
        extraction_date="2024-01-15T12:00:00Z",
        source_url="https://gis.example.com/arcgis/rest/services",
        portolan_version="1.0.0",
        gpio_version="0.5.0",
        metadata_extracted=MetadataExtracted(
            source_url="https://gis.example.com/arcgis/rest/services",
            description=None,
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        ),
        layers=[
            LayerResult(
                id=0,
                name="parcels",
                status="success",
                features=100,
                size_bytes=4096,
                duration_seconds=1.5,
                output_path="parcels/parcels.parquet",
                warnings=[],
                error=None,
                attempts=1,
            )
        ],
        summary=ExtractionSummary(
            total_layers=1,
            succeeded=1,
            failed=0,
            skipped=0,
            empty=0,
            total_features=100,
            total_size_bytes=4096,
            total_duration_seconds=1.5,
        ),
        folder_coverage=coverage,
    )

    report_path = tmp_path / ".portolan" / "extraction-report.json"
    save_report(report, report_path)
    loaded = load_report(report_path)

    assert loaded.folder_coverage is not None
    assert loaded.folder_coverage == coverage
    assert loaded.folder_coverage.folders_visited == ["NationalDatasets", "Boundaries"]
    assert loaded.folder_coverage.folders_skipped == [("Restricted", "499 Token Required")]
    assert loaded.folder_coverage.services_found == 7
