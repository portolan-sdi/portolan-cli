"""Integration tests for Carto extraction auto-init.

Exercises the real catalog auto-init pipeline (init_catalog → add_files →
via-links → metadata seeding) using a real GeoParquet fixture, with only the
network extraction mocked out.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from portolan_cli.extract.carto.discovery import CartoDiscoveryResult, CartoTableInfo
from portolan_cli.extract.carto.orchestrator import _auto_init_catalog
from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
)

pytestmark = [pytest.mark.integration]

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures"
SQL_API_URL = "https://phl.carto.com/api/v2/sql"


def _report(output_path: str, name: str) -> ExtractionReport:
    layer = LayerResult(
        id=0,
        name=name,
        status="success",
        features=3,
        size_bytes=1202,
        duration_seconds=0.5,
        output_path=output_path,
        warnings=[],
        error=None,
        attempts=1,
    )
    return ExtractionReport(
        extraction_date="2026-06-17T12:00:00Z",
        source_url=SQL_API_URL,
        portolan_version="test",
        gpio_version="test",
        metadata_extracted=MetadataExtracted(
            source_url=SQL_API_URL,
            description=None,
            attribution="Carto account: phl",
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        ),
        layers=[layer],
        summary=ExtractionSummary(
            total_layers=1,
            succeeded=1,
            failed=0,
            skipped=0,
            empty=0,
            total_features=3,
            total_size_bytes=1202,
            total_duration_seconds=0.5,
        ),
    )


def _discovery() -> CartoDiscoveryResult:
    return CartoDiscoveryResult(
        service_url=SQL_API_URL,
        tables=[CartoTableInfo("vacant_land", id=0, has_geometry=True)],
        account_name="phl",
    )


def test_auto_init_builds_catalog_with_via_link_and_metadata(tmp_path: Path) -> None:
    collection_dir = tmp_path / "vacant_land"
    collection_dir.mkdir()
    shutil.copy(FIXTURES_DIR / "simple.parquet", collection_dir / "vacant_land.parquet")

    report = _report("vacant_land/vacant_land.parquet", "vacant_land")
    _auto_init_catalog(tmp_path, report, _discovery())

    # Catalog + collection STAC created
    assert (tmp_path / "catalog.json").exists()
    collection_json = tmp_path / "vacant_land" / "collection.json"
    assert collection_json.exists()

    # Provenance via-link points at the Carto SQL query for the table
    links = json.loads(collection_json.read_text()).get("links", [])
    via = [link for link in links if link.get("rel") == "via"]
    assert via, "expected a via provenance link"
    assert "phl.carto.com/api/v2/sql" in via[0]["href"]
    assert "vacant_land" in via[0]["href"]

    # metadata.yaml seeded at catalog and collection level
    assert (tmp_path / ".portolan" / "metadata.yaml").exists()
    assert (collection_dir / ".portolan" / "metadata.yaml").exists()


def test_auto_init_skipped_when_no_successful_tables(tmp_path: Path) -> None:
    report = _report("vacant_land/vacant_land.parquet", "vacant_land")
    report.layers[0].status = "failed"
    report.layers[0].output_path = None

    _auto_init_catalog(tmp_path, report, _discovery())
    assert not (tmp_path / "catalog.json").exists()
