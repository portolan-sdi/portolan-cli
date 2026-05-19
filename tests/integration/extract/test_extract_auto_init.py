"""Integration tests for extract arcgis auto-init feature.

Tests that extraction automatically initializes a Portolan catalog
and adds extracted files, unless --raw flag is specified.

Uses REAL fixtures from tests/fixtures/ rather than mocks.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from portolan_cli.extract.arcgis.orchestrator import _auto_init_catalog
from portolan_cli.extract.arcgis.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
)

pytestmark = [pytest.mark.integration]


# Path to real test fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


def make_layer_result(
    *,
    layer_id: int = 0,
    name: str = "Test Layer",
    status: str = "success",
    output_path: str | None = "test_layer/data.parquet",
    features: int | None = 100,
    size_bytes: int | None = 1000,
    error: str | None = None,
) -> LayerResult:
    """Create a LayerResult with sensible defaults."""
    return LayerResult(
        id=layer_id,
        name=name,
        status=status,
        output_path=output_path,
        features=features if status == "success" else None,
        size_bytes=size_bytes if status == "success" else None,
        duration_seconds=1.0 if status == "success" else None,
        warnings=[],
        error=error,
        attempts=1 if status != "failed" else 3,
    )


def make_report(
    layers: list[LayerResult],
    source_url: str = "https://example.com/arcgis/rest/services/Test/FeatureServer",
) -> ExtractionReport:
    """Create an ExtractionReport with sensible defaults."""
    succeeded = sum(1 for layer in layers if layer.status == "success")
    failed = sum(1 for layer in layers if layer.status == "failed")
    skipped = sum(1 for layer in layers if layer.status == "skipped")
    total_features = sum(layer.features or 0 for layer in layers)
    total_bytes = sum(layer.size_bytes or 0 for layer in layers)

    return ExtractionReport(
        extraction_date="2026-03-30T12:00:00Z",
        source_url=source_url,
        portolan_version="0.1.0",
        gpio_version="0.5.0",
        metadata_extracted=MetadataExtracted(
            source_url=source_url,
            description=None,
            attribution=None,
            keywords=None,
            contact_name=None,
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        ),
        layers=layers,
        summary=ExtractionSummary(
            total_layers=len(layers),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            total_features=total_features,
            total_size_bytes=total_bytes,
            total_duration_seconds=float(len(layers)),
        ),
    )


class TestAutoInitCatalog:
    """Tests for automatic catalog initialization after extraction."""

    def test_auto_init_creates_catalog_json(self, tmp_path: Path) -> None:
        """_auto_init_catalog creates catalog.json at output root."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture to simulate extracted file
        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        # Create report pointing to real file
        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        # Should have catalog.json at root
        assert (output_dir / "catalog.json").exists(), "catalog.json should be created"

    def test_auto_init_creates_config_yaml(self, tmp_path: Path) -> None:
        """_auto_init_catalog creates .portolan/config.yaml sentinel."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture
        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        # Should have .portolan/config.yaml (per ADR-0027)
        assert (output_dir / ".portolan" / "config.yaml").exists(), "config.yaml should exist"

    def test_auto_init_creates_collection_per_layer(self, tmp_path: Path) -> None:
        """Each extracted layer becomes a STAC collection."""
        output_dir = tmp_path / "multi_layer"
        output_dir.mkdir()

        # Copy fixtures to simulate multiple extracted layers
        # Uses simple.parquet which has no CRS issues (no CRS = assumed WGS84)
        layers = []
        for i in range(2):
            layer_name = f"layer_{i}"
            layer_dir = output_dir / layer_name
            layer_dir.mkdir()

            fixture_path = FIXTURES_DIR / "simple.parquet"
            output_parquet = layer_dir / "data.parquet"
            shutil.copy(fixture_path, output_parquet)

            layers.append(
                make_layer_result(
                    layer_id=i,
                    name=layer_name.replace("_", " ").title(),
                    output_path=f"{layer_name}/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            )

        report = make_report(layers=layers)

        _auto_init_catalog(output_dir, report)

        # Each layer should have a collection.json
        assert (output_dir / "layer_0" / "collection.json").exists()
        assert (output_dir / "layer_1" / "collection.json").exists()

    def test_auto_init_skipped_when_no_successful_layers(self, tmp_path: Path) -> None:
        """No catalog created when all layers failed."""
        output_dir = tmp_path / "failed_extraction"
        output_dir.mkdir()

        report = make_report(
            layers=[
                make_layer_result(
                    status="failed",
                    output_path=None,
                    error="Network error",
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        # Should NOT create catalog (no successful extractions)
        assert not (output_dir / "catalog.json").exists()
        assert not (output_dir / ".portolan").exists()

    def test_auto_init_uses_service_name_as_title(self, tmp_path: Path) -> None:
        """Catalog title derived from service URL."""
        import json

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture
        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            source_url="https://services.arcgis.com/abc/arcgis/rest/services/DenHaagHousing/FeatureServer",
        )

        _auto_init_catalog(output_dir, report)

        # Check catalog title
        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        assert catalog_json.get("title") == "DenHaagHousing"


class TestExtractWithRawFlag:
    """Tests that raw=True skips catalog initialization."""

    def test_raw_extraction_skips_auto_init(self, tmp_path: Path) -> None:
        """ExtractionOptions(raw=True) prevents catalog creation.

        This test verifies the orchestrator respects the raw flag.
        """
        from unittest.mock import patch

        from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "raw_output"

        # Copy fixture to use as "extracted" file
        fixture_src = FIXTURES_DIR / "simple.parquet"

        def mock_extract_side_effect(
            service_url: str, layer: object, output_path: Path, options: object
        ) -> tuple[int, int, float]:
            """Copy real fixture to output path."""
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 1.0)

        with patch(
            "portolan_cli.extract.arcgis.orchestrator._extract_single_layer",
            side_effect=mock_extract_side_effect,
        ):
            with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
                mock_discover.return_value = ServiceDiscoveryResult(
                    layers=[LayerInfo(id=0, name="TestLayer", layer_type="Feature Layer")],
                )

                extract_arcgis_catalog(
                    url="https://example.com/arcgis/rest/services/Test/FeatureServer",
                    output_dir=output_dir,
                    options=ExtractionOptions(dry_run=False, raw=True),
                )

        # Should NOT have catalog.json (raw mode)
        assert not (output_dir / "catalog.json").exists(), "raw mode should not create catalog"

        # But should have extraction report
        assert (output_dir / ".portolan" / "extraction-report.json").exists()

    def test_default_extraction_creates_catalog(self, tmp_path: Path) -> None:
        """ExtractionOptions(raw=False, the default) creates catalog."""
        from unittest.mock import patch

        from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult
        from portolan_cli.extract.arcgis.orchestrator import (
            ExtractionOptions,
            extract_arcgis_catalog,
        )

        output_dir = tmp_path / "catalog_output"

        # Copy fixture to use as "extracted" file
        fixture_src = FIXTURES_DIR / "simple.parquet"

        def mock_extract_side_effect(
            service_url: str, layer: object, output_path: Path, options: object
        ) -> tuple[int, int, float]:
            """Copy real fixture to output path."""
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 1.0)

        with patch(
            "portolan_cli.extract.arcgis.orchestrator._extract_single_layer",
            side_effect=mock_extract_side_effect,
        ):
            with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
                mock_discover.return_value = ServiceDiscoveryResult(
                    layers=[LayerInfo(id=0, name="TestLayer", layer_type="Feature Layer")],
                )

                extract_arcgis_catalog(
                    url="https://example.com/arcgis/rest/services/Test/FeatureServer",
                    output_dir=output_dir,
                    options=ExtractionOptions(dry_run=False, raw=False),
                )

        # Should have catalog.json (default behavior)
        assert (output_dir / "catalog.json").exists(), "default extraction should create catalog"
        assert (output_dir / ".portolan" / "config.yaml").exists()


class TestMetadataPropagation:
    """Tests for Issue #369: Propagate rich metadata to STAC files.

    Verifies that extraction --auto mode populates catalog.json and
    collection.json with meaningful metadata from services, not generic
    placeholders like 'Collection: layer_name_abc123'.
    """

    def test_arcgis_service_description_propagates_to_catalog(self, tmp_path: Path) -> None:
        """ArcGIS service description populates catalog.json description field."""
        import json

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture
        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        # Create report with rich service description
        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            source_url="https://services.arcgis.com/abc/arcgis/rest/services/Housing/FeatureServer",
        )
        # Add description to metadata
        report.metadata_extracted = MetadataExtracted(
            source_url=report.source_url,
            description="This dataset contains housing information for the municipality including parcels, buildings, and addresses.",
            attribution="Municipal GIS Department",
            keywords=["housing", "parcels", "buildings"],
            contact_name="John Doe",
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        )

        _auto_init_catalog(output_dir, report)

        # Check catalog.json has the description
        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        assert "housing information" in catalog_json.get("description", "").lower(), (
            "Catalog description should contain service description"
        )

    def test_arcgis_layer_description_propagates_to_collection(self, tmp_path: Path) -> None:
        """ArcGIS layer description populates collection.json description field."""
        import json
        from unittest.mock import patch

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        # Copy a real fixture
        layer_dir = output_dir / "buildings"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    layer_id=0,
                    name="Buildings",
                    output_path="buildings/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        # Mock fetch_layer_details to return rich description
        # Note: imported inside _seed_collection_metadata_arcgis, so patch at source
        with patch("portolan_cli.extract.arcgis.discovery.fetch_layer_details") as mock_fetch:
            mock_fetch.return_value = {
                "name": "Buildings",
                "description": "Building footprints with construction year and height attributes",
            }
            _auto_init_catalog(output_dir, report)

        # Check collection.json has the layer description
        collection_json = json.loads((layer_dir / "collection.json").read_text())
        assert "building footprints" in collection_json.get("description", "").lower(), (
            "Collection description should contain layer description"
        )

    def test_technical_names_not_used_as_catalog_title(self, tmp_path: Path) -> None:
        """Technical names like 'bu_building_v2' should not become catalog title."""
        import json

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        # URL with technical service name
        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            source_url="https://example.com/arcgis/rest/services/bu_building_emprise_v2/FeatureServer",
        )

        _auto_init_catalog(output_dir, report)

        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        # Catalog should be created
        assert catalog_json.get("id") is not None
        # Technical name should NOT appear in title (Issue #369)
        title = catalog_json.get("title")
        assert title is None or "bu_building_emprise_v2" not in title, (
            f"Technical name leaked into catalog title: {title}"
        )
        # Description should use default, not the technical name
        description = catalog_json.get("description", "")
        assert "bu_building_emprise_v2" not in description, (
            f"Technical name leaked into description: {description}"
        )

    def test_collection_gets_title_from_layer_metadata(self, tmp_path: Path) -> None:
        """Collection.json title populated from layer metadata."""
        import json
        from unittest.mock import patch

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "parcels"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "data.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    layer_id=0,
                    name="Parcels",
                    output_path="parcels/data.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        # Mock fetch_layer_details to return title
        # Note: imported inside _seed_collection_metadata_arcgis, so patch at source
        with patch("portolan_cli.extract.arcgis.discovery.fetch_layer_details") as mock_fetch:
            mock_fetch.return_value = {
                "name": "Land Parcels",
                "description": "Municipal land parcel boundaries",
            }
            _auto_init_catalog(output_dir, report)

        collection_json = json.loads((layer_dir / "collection.json").read_text())
        # Title should be present (either from layer name or as set)
        assert collection_json.get("title") == "Land Parcels", (
            "Collection title should be set from layer metadata"
        )
