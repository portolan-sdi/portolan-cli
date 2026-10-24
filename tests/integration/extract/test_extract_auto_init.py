"""Integration tests for extract arcgis auto-init feature.

Tests that extraction automatically initializes a Portolan catalog
and adds extracted files, unless --raw flag is specified.

Uses REAL fixtures from tests/fixtures/ rather than mocks.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import pytest

from portolan_cli.extract.arcgis.orchestrator import ExtractionOptions
from portolan_cli.extract.common.report import (
    ExtractionReport,
    LayerResult,
    MetadataExtracted,
)
from portolan_cli.licensing import ResolvedLicense
from tests.support.extract import build_arcgis_catalog
from tests.support.extract import make_layer_result as shared_layer_result
from tests.support.extract import make_report as shared_make_report


def make_layer_result(
    *,
    name: str = "Test Layer",
    output_path: str | None = "test_layer/test_layer.parquet",
    **fields: Any,
) -> LayerResult:
    """Create a LayerResult with this file's default name and path."""
    return shared_layer_result(name=name, output_path=output_path, **fields)


pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("no_extract_side_requests")]


# Path to real test fixtures
FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


def make_report(
    layers: list[LayerResult],
    source_url: str = "https://example.com/arcgis/rest/services/Test/FeatureServer",
) -> ExtractionReport:
    """Create an ExtractionReport for ``layers``."""
    return shared_make_report(layers, source_url)


# Building the catalog directly skips the license resolution the driver does
# before its download, so the license is passed in here (issue #686).
TEST_LICENSE = ResolvedLicense(license_id="CC-BY-4.0", license_url=None)


def _layer_report(
    output_dir: Path,
    directory: str = "test_layer",
    *,
    source_url: str | None = None,
    **fields: Any,
) -> ExtractionReport:
    """Copy the fixture to ``directory/directory.parquet`` and report it as extracted."""
    layer_dir = output_dir / directory
    layer_dir.mkdir()
    output_parquet = layer_dir / f"{directory}.parquet"
    shutil.copy(FIXTURES_DIR / "simple.parquet", output_parquet)
    layer = make_layer_result(
        output_path=f"{directory}/{directory}.parquet",
        size_bytes=output_parquet.stat().st_size,
        **fields,
    )
    return make_report([layer], source_url) if source_url else make_report([layer])


class TestAutoInitCatalog:
    """Tests for automatic catalog initialization after extraction."""

    def test_auto_init_creates_catalog_json(self, tmp_path: Path) -> None:
        """build_arcgis_catalog creates catalog.json at output root."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        report = _layer_report(output_dir)

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Should have catalog.json at root
        assert (output_dir / "catalog.json").exists(), "catalog.json should be created"

    def test_auto_init_uses_supplied_catalog_id(self, tmp_path: Path) -> None:
        """extract --id names the catalog instead of the directory (issue #821)."""
        import json

        # A scratch directory name is exactly what produced the "publish" catalog
        # in issue #821, so the derived id here differs from the supplied one.
        output_dir = tmp_path / "publish"
        output_dir.mkdir()

        layer_dir = output_dir / "test_layer"
        layer_dir.mkdir()
        output_parquet = layer_dir / f"{layer_dir.name}.parquet"
        shutil.copy(FIXTURES_DIR / "simple.parquet", output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    output_path="test_layer/test_layer.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        build_arcgis_catalog(output_dir, report, TEST_LICENSE, "phl-housing")

        catalog = json.loads((output_dir / "catalog.json").read_text(encoding="utf-8"))
        versions = json.loads((output_dir / "versions.json").read_text(encoding="utf-8"))
        assert catalog["id"] == "phl-housing"
        assert versions["catalog_id"] == "phl-housing"

    def test_auto_init_creates_config_yaml(self, tmp_path: Path) -> None:
        """build_arcgis_catalog creates .portolan/config.yaml sentinel."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        report = _layer_report(output_dir)

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Should have .portolan/config.yaml
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
            output_parquet = layer_dir / f"{layer_dir.name}.parquet"
            shutil.copy(fixture_path, output_parquet)

            layers.append(
                make_layer_result(
                    layer_id=i,
                    name=layer_name.replace("_", " ").title(),
                    output_path=f"{layer_name}/{layer_name}.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            )

        report = make_report(layers=layers)

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Each layer should have a collection.json
        assert (output_dir / "layer_0" / "collection.json").exists()
        assert (output_dir / "layer_1" / "collection.json").exists()

    def test_second_extraction_adds_collection_to_existing_catalog(self, tmp_path: Path) -> None:
        """A second extraction into an existing catalog adds a collection (issue #767).

        Before the fix, the second catalog build raised
        CatalogAlreadyExistsError after the data was already on disk, so the
        second layer's collection.json was never written.
        """
        import json

        output_dir = tmp_path / "shared_catalog"
        output_dir.mkdir()

        def _extract_layer(name: str, layer_id: int) -> ExtractionReport:
            report = _layer_report(output_dir, name, layer_id=layer_id, name=name)
            build_arcgis_catalog(output_dir, report, TEST_LICENSE)
            return report

        # First layer creates the catalog.
        _extract_layer("layer_a", 0)
        assert (output_dir / "catalog.json").exists()
        assert (output_dir / "layer_a" / "collection.json").exists()

        # Second layer must be added, not refused.
        _extract_layer("layer_b", 1)
        assert (output_dir / "layer_b" / "collection.json").exists(), (
            "second layer's collection.json should be written into the existing catalog"
        )

        # The root catalog links to both collections.
        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        child_hrefs = {
            link.get("href") for link in catalog_json.get("links", []) if link.get("rel") == "child"
        }
        assert "./layer_a/collection.json" in child_hrefs
        assert "./layer_b/collection.json" in child_hrefs

    def test_second_extraction_keeps_root_catalog_title_and_description(
        self, tmp_path: Path
    ) -> None:
        """A second extraction from a different service keeps the root metadata (issue #832).

        The #767 fix lets a second extraction add a collection to an existing
        catalog. Before this fix, that second run also rewrote the root
        catalog.json title and description with the new service's values. The
        root metadata must survive the second extraction.
        """
        import json

        output_dir = tmp_path / "shared_catalog"
        output_dir.mkdir()

        def _extract(name: str, layer_id: int, source_url: str, description: str) -> None:
            report = _layer_report(
                output_dir, name, layer_id=layer_id, name=name, source_url=source_url
            )
            report.metadata_extracted = MetadataExtracted(
                source_url=source_url,
                description=description,
                attribution=None,
                keywords=None,
                producer=None,
                processing_notes=None,
                known_issues=None,
                license_info_raw=None,
            )
            build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # First extraction seeds the root title and description.
        _extract(
            "housing",
            0,
            "https://services.arcgis.com/abc/arcgis/rest/services/HousingData/FeatureServer",
            "Housing information for the municipality.",
        )
        catalog_before = json.loads((output_dir / "catalog.json").read_text())
        assert catalog_before.get("title") == "HousingData"
        assert "housing information" in catalog_before.get("description", "").lower()

        # Second extraction from a different service must not replace the root metadata.
        _extract(
            "roads",
            1,
            "https://services.arcgis.com/abc/arcgis/rest/services/RoadNetwork/FeatureServer",
            "Road centerlines for the region.",
        )
        catalog_after = json.loads((output_dir / "catalog.json").read_text())
        assert catalog_after.get("title") == "HousingData", (
            "second extraction must not overwrite the root catalog title"
        )
        assert catalog_after.get("description") == catalog_before.get("description"), (
            "second extraction must not overwrite the root catalog description"
        )

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

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Should NOT create catalog (no successful extractions)
        assert not (output_dir / "catalog.json").exists()
        assert not (output_dir / ".portolan").exists()

    def test_auto_init_uses_service_name_as_title(self, tmp_path: Path) -> None:
        """Catalog title derived from service URL."""
        import json

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        report = _layer_report(
            output_dir,
            source_url="https://services.arcgis.com/abc/arcgis/rest/services/DenHaagHousing/FeatureServer",
        )

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Check catalog title
        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        assert catalog_json.get("title") == "DenHaagHousing"


def _extract_test_layer(output_dir: Path, options: ExtractionOptions) -> None:
    """Extract one fake layer, TestLayer, with the fixture as its data."""
    from unittest.mock import patch

    from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult
    from portolan_cli.extract.arcgis.orchestrator import ArcGISLayerSource, extract_arcgis_catalog

    def copy_fixture(self: object, service_url: str, layer: object, output_path: Path) -> int:
        shutil.copy(FIXTURES_DIR / "simple.parquet", output_path)
        return 100

    discovery = ServiceDiscoveryResult(
        layers=[LayerInfo(id=0, name="TestLayer", layer_type="Feature Layer")],
    )
    with (
        patch.object(ArcGISLayerSource, "fetch", autospec=True, side_effect=copy_fixture),
        patch("portolan_cli.extract.arcgis.orchestrator.discover_layers", return_value=discovery),
    ):
        extract_arcgis_catalog(
            url="https://example.com/arcgis/rest/services/Test/FeatureServer",
            output_dir=output_dir,
            options=options,
        )


class TestExtractWithRawFlag:
    """Tests that raw=True skips catalog initialization."""

    def test_raw_extraction_skips_auto_init(self, tmp_path: Path) -> None:
        """ExtractionOptions(raw=True) prevents catalog creation.

        This test verifies the orchestrator respects the raw flag.
        """
        output_dir = tmp_path / "raw_output"
        _extract_test_layer(output_dir, ExtractionOptions(dry_run=False, raw=True))

        # Should NOT have catalog.json (raw mode)
        assert not (output_dir / "catalog.json").exists(), "raw mode should not create catalog"

        # But should have extraction report
        assert (output_dir / ".portolan" / "extraction-report.json").exists()

    def test_default_extraction_creates_catalog(self, tmp_path: Path) -> None:
        """ExtractionOptions(raw=False, the default, license="CC-BY-4.0") creates catalog."""
        output_dir = tmp_path / "catalog_output"
        _extract_test_layer(
            output_dir, ExtractionOptions(dry_run=False, raw=False, license="CC-BY-4.0")
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

        report = _layer_report(
            output_dir,
            source_url="https://services.arcgis.com/abc/arcgis/rest/services/Housing/FeatureServer",
        )
        # Add description to metadata
        report.metadata_extracted = MetadataExtracted(
            source_url=report.source_url,
            description="This dataset contains housing information for the municipality including parcels, buildings, and addresses.",
            attribution="Municipal GIS Department",
            keywords=["housing", "parcels", "buildings"],
            producer="John Doe",
            processing_notes=None,
            known_issues=None,
            license_info_raw=None,
        )

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

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

        report = _layer_report(output_dir, "buildings", layer_id=0, name="Buildings")

        # Mock fetch_layer_details to return rich description
        # Note: imported inside ArcGISLayerSource.layer_metadata, so patch at source
        with patch("portolan_cli.extract.arcgis.discovery.fetch_layer_details") as mock_fetch:
            mock_fetch.return_value = {
                "name": "Buildings",
                "description": "Building footprints with construction year and height attributes",
            }
            build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        # Check collection.json has the layer description
        collection_json = json.loads((output_dir / "buildings" / "collection.json").read_text())
        assert "building footprints" in collection_json.get("description", "").lower(), (
            "Collection description should contain layer description"
        )

    def test_technical_names_not_used_as_catalog_title(self, tmp_path: Path) -> None:
        """Technical names like 'bu_building_v2' should not become catalog title."""
        import json

        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        report = _layer_report(
            output_dir,
            source_url="https://example.com/arcgis/rest/services/bu_building_emprise_v2/FeatureServer",
        )

        build_arcgis_catalog(output_dir, report, TEST_LICENSE)

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

        report = _layer_report(output_dir, "parcels", layer_id=0, name="Parcels")

        # Mock fetch_layer_details to return title
        # Note: imported inside ArcGISLayerSource.layer_metadata, so patch at source
        with patch("portolan_cli.extract.arcgis.discovery.fetch_layer_details") as mock_fetch:
            mock_fetch.return_value = {
                "name": "Land Parcels",
                "description": "Municipal land parcel boundaries",
            }
            build_arcgis_catalog(output_dir, report, TEST_LICENSE)

        collection_json = json.loads((output_dir / "parcels" / "collection.json").read_text())
        # Title should be present (either from layer name or as set)
        assert collection_json.get("title") == "Land Parcels", (
            "Collection title should be set from layer metadata"
        )
