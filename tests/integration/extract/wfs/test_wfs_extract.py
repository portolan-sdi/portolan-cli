"""Integration tests for WFS extraction.

Tests the full extraction workflow including:
- Auto-init catalog creation
- Via link provenance
- Resume functionality
- Raw mode (skip catalog)
- Parallel workers
- Version negotiation

Uses REAL fixtures from tests/fixtures/ rather than mocks.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import patch

import pytest

from portolan_cli.extract.common.report import (
    ExtractionReport,
    ExtractionSummary,
    LayerResult,
    MetadataExtracted,
)
from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult
from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    _auto_init_catalog,
    extract_wfs_catalog,
)

pytestmark = [pytest.mark.integration]

FIXTURES_DIR = Path(__file__).parent.parent.parent.parent / "fixtures"


def make_layer_info(name: str, layer_id: int = 0) -> LayerInfo:
    """Create a LayerInfo for testing."""
    return LayerInfo(
        name=name,
        typename=name,
        title=name.title(),
        abstract=f"Description of {name}",
        bbox=None,
        id=layer_id,
    )


def make_discovery_result(
    layers: list[LayerInfo],
    service_url: str = "https://example.com/wfs",
) -> WFSDiscoveryResult:
    """Create a WFSDiscoveryResult for testing."""
    return WFSDiscoveryResult(
        service_url=service_url,
        layers=layers,
        service_title="Test WFS Service",
        service_abstract="A test WFS service",
        provider="Test Provider",
        keywords=["test", "wfs"],
        contact_name="Test Contact",
        access_constraints=None,
        fees=None,
    )


def make_layer_result(
    *,
    layer_id: int = 0,
    name: str = "test_layer",
    status: str = "success",
    output_path: str | None = "test_layer/test_layer.parquet",
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
    source_url: str = "https://example.com/wfs",
) -> ExtractionReport:
    """Create an ExtractionReport with sensible defaults."""
    succeeded = sum(1 for layer in layers if layer.status == "success")
    failed = sum(1 for layer in layers if layer.status == "failed")
    skipped = sum(1 for layer in layers if layer.status == "skipped")
    total_features = sum(layer.features or 0 for layer in layers)
    total_bytes = sum(layer.size_bytes or 0 for layer in layers)

    return ExtractionReport(
        extraction_date="2026-04-23T12:00:00Z",
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


class TestWFSAutoInitCatalog:
    """Tests for automatic catalog initialization after WFS extraction."""

    def test_auto_init_creates_catalog_json(self, tmp_path: Path) -> None:
        """_auto_init_catalog creates catalog.json at output root."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "buildings"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "buildings.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="buildings",
                    output_path="buildings/buildings.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        assert (output_dir / "catalog.json").exists(), "catalog.json should be created"

    def test_auto_init_creates_config_yaml(self, tmp_path: Path) -> None:
        """_auto_init_catalog creates .portolan/config.yaml sentinel."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "roads"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "roads.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="roads",
                    output_path="roads/roads.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        assert (output_dir / ".portolan" / "config.yaml").exists(), "config.yaml should exist"

    def test_auto_init_creates_collection_per_layer(self, tmp_path: Path) -> None:
        """Each extracted WFS layer becomes a STAC collection."""
        output_dir = tmp_path / "multi_layer"
        output_dir.mkdir()

        layers = []
        for layer_name in ["buildings", "roads"]:
            layer_dir = output_dir / layer_name
            layer_dir.mkdir()

            fixture_path = FIXTURES_DIR / "simple.parquet"
            output_parquet = layer_dir / f"{layer_name}.parquet"
            shutil.copy(fixture_path, output_parquet)

            layers.append(
                make_layer_result(
                    layer_id=len(layers),
                    name=layer_name,
                    output_path=f"{layer_name}/{layer_name}.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            )

        report = make_report(layers=layers)

        _auto_init_catalog(output_dir, report)

        assert (output_dir / "buildings" / "collection.json").exists()
        assert (output_dir / "roads" / "collection.json").exists()

    def test_auto_init_skipped_when_no_successful_layers(self, tmp_path: Path) -> None:
        """No catalog created when all layers failed."""
        output_dir = tmp_path / "failed_extraction"
        output_dir.mkdir()

        report = make_report(
            layers=[
                make_layer_result(
                    status="failed",
                    output_path=None,
                    error="WFS service unavailable",
                )
            ],
        )

        _auto_init_catalog(output_dir, report)

        assert not (output_dir / "catalog.json").exists()
        assert not (output_dir / ".portolan").exists()


class TestWFSViaLinks:
    """Tests for WFS provenance via links."""

    def test_via_link_uses_getfeature_url(self, tmp_path: Path) -> None:
        """Via links use GetFeature-style WFS URLs."""
        output_dir = tmp_path / "via_test"
        output_dir.mkdir()

        layer_dir = output_dir / "parcels"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "parcels.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="ns:parcels",
                    output_path="parcels/parcels.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
            source_url="https://geoserver.example.com/wfs",
        )

        _auto_init_catalog(output_dir, report)

        collection_path = output_dir / "parcels" / "collection.json"
        assert collection_path.exists()

        collection = json.loads(collection_path.read_text())
        via_links = [link for link in collection.get("links", []) if link.get("rel") == "via"]

        assert len(via_links) == 1
        via_link = via_links[0]
        assert "service=WFS" in via_link["href"]
        assert "request=GetFeature" in via_link["href"]
        # URL-encoded colon: ns:parcels → ns%3Aparcels
        assert "typename=ns%3Aparcels" in via_link["href"]


class TestWFSExtractOrchestrator:
    """Integration tests for extract_wfs_catalog orchestrator."""

    def test_dry_run_does_not_create_files(self, tmp_path: Path) -> None:
        """Dry run lists layers without creating output."""
        output_dir = tmp_path / "dry_run_output"

        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=output_dir,
                options=ExtractionOptions(dry_run=True),
            )

        assert len(report.layers) == 2
        assert all(layer.status == "pending" for layer in report.layers)
        assert not output_dir.exists(), "dry run should not create output directory"

    def test_raw_mode_skips_catalog_init(self, tmp_path: Path) -> None:
        """raw=True creates files but skips STAC catalog."""
        output_dir = tmp_path / "raw_output"
        fixture_src = FIXTURES_DIR / "simple.parquet"

        layers = [make_layer_info("test", 0)]
        discovery = make_discovery_result(layers)

        def mock_extract_side_effect(
            service_url: str,
            layer: object,
            output_path: Path,
            options: object,
            negotiated_version: str,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 1.0)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            with patch(
                "portolan_cli.extract.wfs.orchestrator._extract_single_layer",
                side_effect=mock_extract_side_effect,
            ):
                extract_wfs_catalog(
                    url="https://example.com/wfs",
                    output_dir=output_dir,
                    options=ExtractionOptions(raw=True),
                )

        assert not (output_dir / "catalog.json").exists(), "raw mode should not create catalog"
        assert (output_dir / ".portolan" / "extraction-report.json").exists()

    def test_default_mode_creates_catalog(self, tmp_path: Path) -> None:
        """Default extraction creates STAC catalog."""
        output_dir = tmp_path / "catalog_output"
        fixture_src = FIXTURES_DIR / "simple.parquet"

        layers = [make_layer_info("test", 0)]
        discovery = make_discovery_result(layers)

        def mock_extract_side_effect(
            service_url: str,
            layer: object,
            output_path: Path,
            options: object,
            negotiated_version: str,
        ) -> tuple[int, int, float]:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 1.0)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            with patch(
                "portolan_cli.extract.wfs.orchestrator._extract_single_layer",
                side_effect=mock_extract_side_effect,
            ):
                extract_wfs_catalog(
                    url="https://example.com/wfs",
                    output_dir=output_dir,
                    options=ExtractionOptions(raw=False),
                )

        assert (output_dir / "catalog.json").exists(), "default extraction should create catalog"
        assert (output_dir / ".portolan" / "config.yaml").exists()

    def test_layer_filter_reduces_extraction(self, tmp_path: Path) -> None:
        """Layer filter limits which layers are extracted."""
        output_dir = tmp_path / "filtered_output"

        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
            make_layer_info("water", 2),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=output_dir,
                layer_filter=["buildings"],
                options=ExtractionOptions(dry_run=True),
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"

    def test_layer_exclude_removes_matching(self, tmp_path: Path) -> None:
        """Layer exclude removes matching layers."""
        output_dir = tmp_path / "excluded_output"

        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("test_data", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=output_dir,
                layer_exclude=["test_*"],
                options=ExtractionOptions(dry_run=True),
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"


class TestParallelExtraction:
    """Tests for parallel layer extraction with workers > 1."""

    def test_parallel_workers_extract_concurrently(self, tmp_path: Path) -> None:
        """Multiple workers extract layers in parallel."""
        output_dir = tmp_path / "parallel_output"
        fixture_src = FIXTURES_DIR / "simple.parquet"

        layers = [
            make_layer_info("layer_a", 0),
            make_layer_info("layer_b", 1),
            make_layer_info("layer_c", 2),
            make_layer_info("layer_d", 3),
        ]
        discovery = make_discovery_result(layers)

        extracted_layers: list[str] = []

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: object,
            negotiated_version: str,
        ) -> tuple[int, int, float]:
            extracted_layers.append(layer.name)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 0.1)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            with patch(
                "portolan_cli.extract.wfs.orchestrator._extract_single_layer",
                side_effect=mock_extract_side_effect,
            ):
                report = extract_wfs_catalog(
                    url="https://example.com/wfs",
                    output_dir=output_dir,
                    options=ExtractionOptions(raw=True, workers=2),
                )

        # All 4 layers should be extracted
        assert len(extracted_layers) == 4
        assert report.summary.succeeded == 4
        assert set(extracted_layers) == {"layer_a", "layer_b", "layer_c", "layer_d"}

    def test_single_worker_extracts_sequentially(self, tmp_path: Path) -> None:
        """Single worker extracts layers in order."""
        output_dir = tmp_path / "sequential_output"
        fixture_src = FIXTURES_DIR / "simple.parquet"

        layers = [
            make_layer_info("layer_a", 0),
            make_layer_info("layer_b", 1),
        ]
        discovery = make_discovery_result(layers)

        extraction_order: list[str] = []

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: object,
            negotiated_version: str,
        ) -> tuple[int, int, float]:
            extraction_order.append(layer.name)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (100, output_path.stat().st_size, 0.1)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            with patch(
                "portolan_cli.extract.wfs.orchestrator._extract_single_layer",
                side_effect=mock_extract_side_effect,
            ):
                extract_wfs_catalog(
                    url="https://example.com/wfs",
                    output_dir=output_dir,
                    options=ExtractionOptions(raw=True, workers=1),
                )

        # Sequential extraction should preserve order
        assert extraction_order == ["layer_a", "layer_b"]


class TestWFSResume:
    """Tests for WFS extraction resume functionality."""

    def test_resume_skips_completed_layers(self, tmp_path: Path) -> None:
        """Resume mode skips layers that already succeeded."""
        output_dir = tmp_path / "resume_test"
        output_dir.mkdir()
        (output_dir / ".portolan").mkdir()

        fixture_src = FIXTURES_DIR / "simple.parquet"
        layer_dir = output_dir / "buildings"
        layer_dir.mkdir()
        shutil.copy(fixture_src, layer_dir / "buildings.parquet")

        existing_report = make_report(
            layers=[
                make_layer_result(
                    layer_id=0,
                    name="buildings",
                    status="success",
                    output_path="buildings/buildings.parquet",
                ),
                make_layer_result(
                    layer_id=1,
                    name="roads",
                    status="failed",
                    output_path=None,
                    error="Network error",
                ),
            ],
        )
        report_path = output_dir / ".portolan" / "extraction-report.json"
        report_path.write_text(json.dumps(existing_report.to_dict()))

        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
        ]
        discovery = make_discovery_result(layers)

        extract_calls: list[LayerInfo] = []

        def mock_extract_side_effect(
            service_url: str,
            layer: LayerInfo,
            output_path: Path,
            options: object,
            negotiated_version: str,
        ) -> tuple[int, int, float]:
            extract_calls.append(layer)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(fixture_src, output_path)
            return (50, output_path.stat().st_size, 0.5)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            with patch(
                "portolan_cli.extract.wfs.orchestrator._extract_single_layer",
                side_effect=mock_extract_side_effect,
            ):
                report = extract_wfs_catalog(
                    url="https://example.com/wfs",
                    output_dir=output_dir,
                    options=ExtractionOptions(resume=True, raw=True),
                )

        assert len(extract_calls) == 1, "Should only extract failed layer"
        assert extract_calls[0].name == "roads"
        assert report.summary.succeeded == 2
        assert report.summary.failed == 0


class TestWFSMetadataPropagation:
    """Tests for Issue #369: Propagate rich WFS metadata to STAC files.

    Verifies that WFS extraction --auto mode populates catalog.json and
    collection.json with meaningful metadata from GetCapabilities and ISO 19139,
    not generic placeholders like 'Collection: layer_name_abc123'.
    """

    def test_service_title_propagates_to_catalog(self, tmp_path: Path) -> None:
        """WFS service title from GetCapabilities populates catalog.json title."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "buildings"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "buildings.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="buildings",
                    output_path="buildings/buildings.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        # Discovery result with rich service metadata
        discovery = WFSDiscoveryResult(
            service_url="https://example.com/wfs",
            layers=[make_layer_info("buildings", 0)],
            service_title="INSPIRE Buildings Service",
            service_abstract="This service provides building footprints compliant with INSPIRE directive",
            provider="National Mapping Agency",
            keywords=["inspire", "buildings", "cadastre"],
            contact_name="GIS Support Team",
            access_constraints=None,
            fees=None,
        )

        _auto_init_catalog(output_dir, report, discovery)

        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        assert catalog_json.get("title") == "INSPIRE Buildings Service", (
            "Catalog title should be set from WFS service title"
        )
        assert "building footprints" in catalog_json.get("description", "").lower(), (
            "Catalog description should contain service abstract"
        )

    def test_service_abstract_propagates_to_catalog_description(self, tmp_path: Path) -> None:
        """WFS service abstract from GetCapabilities populates catalog.json description."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "roads"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "roads.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="roads",
                    output_path="roads/roads.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        discovery = WFSDiscoveryResult(
            service_url="https://example.com/wfs",
            layers=[make_layer_info("roads", 0)],
            service_title="Transport Network Service",
            service_abstract="Comprehensive road network data including highways, local roads, and paths",
            provider=None,
            keywords=None,
            contact_name=None,
            access_constraints=None,
            fees=None,
        )

        _auto_init_catalog(output_dir, report, discovery)

        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        assert "road network" in catalog_json.get("description", "").lower()

    def test_layer_metadata_propagates_to_collection(self, tmp_path: Path) -> None:
        """WFS layer title/abstract from GetCapabilities populates collection.json."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "parcels"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "parcels.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="parcels",
                    output_path="parcels/parcels.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        # Create layer with rich metadata
        layer_info = LayerInfo(
            name="parcels",
            typename="ns:Parcels",
            title="Land Parcels",
            abstract="Cadastral parcel boundaries with ownership and land use information",
            keywords=["cadastre", "parcels", "land use"],
            bbox=None,
            id=0,
        )

        discovery = WFSDiscoveryResult(
            service_url="https://example.com/wfs",
            layers=[layer_info],
            service_title="Cadastre Service",
            service_abstract="Cadastral data",
            provider=None,
            keywords=None,
            contact_name=None,
            access_constraints=None,
            fees=None,
        )

        _auto_init_catalog(output_dir, report, discovery)

        collection_json = json.loads((layer_dir / "collection.json").read_text())
        assert collection_json.get("title") == "Land Parcels", (
            "Collection title should be set from layer title"
        )
        assert "parcel boundaries" in collection_json.get("description", "").lower(), (
            "Collection description should contain layer abstract"
        )

    def test_technical_service_title_filtered(self, tmp_path: Path) -> None:
        """Technical service titles like 'wfs_service_v2' are filtered out."""
        output_dir = tmp_path / "test_catalog"
        output_dir.mkdir()

        layer_dir = output_dir / "test"
        layer_dir.mkdir()
        fixture_path = FIXTURES_DIR / "simple.parquet"
        output_parquet = layer_dir / "test.parquet"
        shutil.copy(fixture_path, output_parquet)

        report = make_report(
            layers=[
                make_layer_result(
                    name="test",
                    output_path="test/test.parquet",
                    size_bytes=output_parquet.stat().st_size,
                )
            ],
        )

        # Technical/identifier-like service title should be filtered
        discovery = WFSDiscoveryResult(
            service_url="https://example.com/wfs",
            layers=[make_layer_info("test", 0)],
            service_title="wfs_service_internal_v2",  # Technical name
            service_abstract="A meaningful service description with proper explanation",
            provider=None,
            keywords=None,
            contact_name=None,
            access_constraints=None,
            fees=None,
        )

        _auto_init_catalog(output_dir, report, discovery)

        catalog_json = json.loads((output_dir / "catalog.json").read_text())
        # Technical title should be filtered (Issue #369)
        title = catalog_json.get("title")
        assert title is None or "wfs_service_internal_v2" not in title, (
            f"Technical service title leaked into catalog: {title}"
        )
        # Description should be set from the meaningful abstract
        assert "meaningful service description" in catalog_json.get("description", "").lower()


class TestProjectedCRSBBoxRegression:
    """Regression tests for Issue #377: bbox coordinate mixing.

    When extracting WFS data in a projected CRS (e.g., EPSG:3035),
    the collection.json extent.bbox must contain only WGS84 coordinates,
    not a mix of WGS84 and projected values.

    The actual fix is in geoparquet-io v1.1.1's WFS extraction code.
    These tests verify portolan's CRS transformation handles projected data correctly.
    """

    def test_portolan_transforms_projected_bbox_to_wgs84(self, tmp_path: Path) -> None:
        """Portolan's CRS module transforms projected bbox to WGS84.

        Regression test for https://github.com/portolan-sdi/portolan-cli/issues/377

        The bug was: minx/miny were WGS84 but maxx/maxy stayed in projected coords.
        Example bad bbox: [2.84, 49.50, 4065342.57, 3100515.54]

        This test verifies portolan's transform_bbox_to_wgs84 correctly handles
        projected CRS coordinates.
        """
        from portolan_cli.crs import transform_bbox_to_wgs84

        # Bbox in EPSG:3035 (European projected CRS) - roughly Belgium
        projected_bbox = (3800000.0, 3000000.0, 4100000.0, 3200000.0)

        # Transform to WGS84
        wgs84_bbox = transform_bbox_to_wgs84(projected_bbox, "EPSG:3035")

        minx, miny, maxx, maxy = wgs84_bbox

        # All values must be in WGS84 range
        assert -180 <= minx <= 180, f"minx {minx} not in WGS84 longitude range"
        assert -90 <= miny <= 90, f"miny {miny} not in WGS84 latitude range"
        assert -180 <= maxx <= 180, f"maxx {maxx} not in WGS84 longitude range"
        assert -90 <= maxy <= 90, f"maxy {maxy} not in WGS84 latitude range"

        # Sanity check: bbox should be roughly Belgium/Netherlands region
        # EPSG:3035 coords (3.8M-4.1M, 3.0M-3.2M) map to roughly lon 2-10, lat 48-54
        assert 0 < minx < 15, f"minx {minx} not in expected region"
        assert 45 < miny < 60, f"miny {miny} not in expected region"
        assert 0 < maxx < 15, f"maxx {maxx} not in expected region"
        assert 45 < maxy < 60, f"maxy {maxy} not in expected region"

    def test_wgs84_passthrough_unchanged(self, tmp_path: Path) -> None:
        """Data already in WGS84 passes through unchanged."""
        from portolan_cli.crs import transform_bbox_to_wgs84

        # Bbox already in WGS84 (Belgium approx)
        wgs84_bbox = (2.5, 49.5, 6.5, 51.5)

        # Transform should return unchanged
        result = transform_bbox_to_wgs84(wgs84_bbox, "EPSG:4326")

        assert abs(result[0] - 2.5) < 0.01, f"minx {result[0]} drifted from 2.5"
        assert abs(result[1] - 49.5) < 0.01, f"miny {result[1]} drifted from 49.5"
        assert abs(result[2] - 6.5) < 0.01, f"maxx {result[2]} drifted from 6.5"
        assert abs(result[3] - 51.5) < 0.01, f"maxy {result[3]} drifted from 51.5"

    def test_mixed_coordinates_obviously_invalid(self) -> None:
        """Mixed WGS84/projected coordinates are obviously invalid.

        This is the exact bug pattern from issue #377:
        [2.84, 49.50, 4065342.57, 3100515.54]

        minx/miny are WGS84 but maxx/maxy are still in EPSG:3035.
        The fix in geoparquet-io v1.1.1 ensures this never happens.
        """
        # The bug produced this mixed bbox
        mixed_bbox = (2.84, 49.50, 4065342.57, 3100515.54)

        # These coordinates are obviously invalid for WGS84
        # WGS84 longitude range: -180 to 180
        # WGS84 latitude range: -90 to 90
        assert mixed_bbox[2] > 180, "maxx exceeds valid WGS84 longitude range"
        assert mixed_bbox[3] > 90, "maxy exceeds valid WGS84 latitude range"

        # The first two coordinates look like valid WGS84
        assert -180 <= mixed_bbox[0] <= 180, "minx in WGS84 range"
        assert -90 <= mixed_bbox[1] <= 90, "miny in WGS84 range"
