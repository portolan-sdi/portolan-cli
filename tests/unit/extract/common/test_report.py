from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.extract.carto.orchestrator import ExtractionOptions as CartoOptions
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    FolderCoverage,
    LayerResult,
    MetadataExtracted,
    load_report,
    run_extract_command,
    save_report,
)
from portolan_cli.extract.common.source import Discovery, build_report
from portolan_cli.input_hardening import InputValidationError

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


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
            producer=None,
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


def _summary(wall_duration_seconds: float | None = None) -> ExtractionSummary:
    return ExtractionSummary(
        total_layers=2,
        succeeded=2,
        failed=0,
        skipped=0,
        empty=0,
        total_features=10,
        total_size_bytes=100,
        total_duration_seconds=3.0,
        wall_duration_seconds=wall_duration_seconds,
    )


@pytest.mark.unit
def test_summary_wall_duration_roundtrip() -> None:
    summary = _summary(wall_duration_seconds=1.75)
    data = summary.to_dict()
    assert data["wall_duration_seconds"] == 1.75
    assert ExtractionSummary.from_dict(data) == summary


@pytest.mark.unit
def test_summary_without_wall_duration_keeps_the_old_shape() -> None:
    data = _summary().to_dict()
    assert "wall_duration_seconds" not in data
    assert ExtractionSummary.from_dict(data).wall_duration_seconds is None


def _recording_extract(seen: list[str | None]) -> Callable[..., ExtractionReport]:
    """Build a fake extract_*_catalog that records the catalog id it receives."""

    def extract(*, url: str, output_dir: Path, options: CartoOptions) -> ExtractionReport:
        seen.append(options.catalog_id)
        return build_report(Discovery(source_url=url, layers=[]), [])

    return extract


@pytest.mark.unit
def test_run_extract_command_forwards_the_id_it_checks(tmp_path: Path) -> None:
    seen: list[str | None] = []

    run_extract_command(
        _recording_extract(seen),
        "https://example.invalid/api/v2/sql",
        tmp_path,
        CartoOptions(catalog_id="requested-id"),
    )

    assert seen == ["requested-id"]


@pytest.mark.unit
def test_run_extract_command_rejects_a_bad_id_in_the_options(tmp_path: Path) -> None:
    """A bad id in the options fails before the extraction runs."""
    seen: list[str | None] = []

    with pytest.raises(InputValidationError, match="Invalid catalog ID"):
        run_extract_command(
            _recording_extract(seen),
            "https://example.invalid/api/v2/sql",
            tmp_path,
            CartoOptions(catalog_id="bad id!"),
        )

    assert seen == []
