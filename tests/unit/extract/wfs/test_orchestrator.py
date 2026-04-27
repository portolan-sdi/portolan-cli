"""Tests for WFS extraction orchestrator.

These tests verify the main orchestration logic for WFS extraction,
including layer filtering, extraction, and catalog initialization.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from portolan_cli.extract.wfs.discovery import LayerInfo, WFSDiscoveryResult
from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    _assign_slugs,
    _slugify,
    extract_wfs_catalog,
)

pytestmark = pytest.mark.unit


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


class TestExtractionOptions:
    """Tests for ExtractionOptions dataclass."""

    def test_default_values(self) -> None:
        """Default options have sensible values."""
        options = ExtractionOptions()
        assert options.workers == 1
        assert options.retries == 3
        assert options.timeout == 300.0  # 5 minutes per layer
        assert options.resume is False
        assert options.raw is False
        assert options.dry_run is False
        assert options.wfs_version == "auto"
        assert options.page_size == 10000


class TestExtractWfsCatalog:
    """Tests for extract_wfs_catalog function."""

    def test_dry_run_returns_pending_layers(self, tmp_path: Path) -> None:
        """Dry run returns report with pending status."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 2
        assert all(layer.status == "pending" for layer in report.layers)

    def test_dry_run_includes_service_metadata(self, tmp_path: Path) -> None:
        """Dry run report includes service metadata from discovery."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        # Service metadata should be in the report
        assert report.metadata_extracted.description == "A test WFS service"
        assert report.metadata_extracted.attribution == "Test Provider"
        assert "test" in (report.metadata_extracted.keywords or "")

    def test_layer_filter_applied(self, tmp_path: Path) -> None:
        """Layer filter reduces extracted layers."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("roads", 1),
            make_layer_info("water", 2),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                layer_filter=["buildings"],
                options=options,
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"

    def test_layer_exclude_applied(self, tmp_path: Path) -> None:
        """Layer exclude removes matching layers."""
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("test_layer", 1),
        ]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                layer_exclude=["test_*"],
                options=options,
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"


class TestVersionNegotiation:
    """Tests for WFS version negotiation."""

    def test_auto_version_negotiates(self, tmp_path: Path) -> None:
        """Auto version triggers negotiation."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version") as mock_negotiate,
        ):
            mock_discover.return_value = discovery
            mock_negotiate.return_value = "2.0.0"

            options = ExtractionOptions(dry_run=True, wfs_version="auto")
            extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        mock_negotiate.assert_called_once_with("https://example.com/wfs", "auto")
        # discover_layers should be called with the negotiated version
        mock_discover.assert_called_once()
        call_args = mock_discover.call_args
        assert call_args[1]["version"] == "2.0.0"

    def test_explicit_version_skips_negotiation(self, tmp_path: Path) -> None:
        """Explicit version skips negotiation."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with (
            patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.wfs.orchestrator._negotiate_version") as mock_negotiate,
        ):
            mock_discover.return_value = discovery
            mock_negotiate.return_value = "1.1.0"

            options = ExtractionOptions(dry_run=True, wfs_version="1.1.0")
            extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        # negotiate_version is called but returns the explicit version
        mock_negotiate.assert_called_once_with("https://example.com/wfs", "1.1.0")


class TestExtractWfsCatalogListMode:
    """Tests for list mode (no layer specified, no --all)."""

    def test_no_layer_filter_extracts_all(self, tmp_path: Path) -> None:
        """Without filter, extract all layers (matching ArcGIS behavior)."""
        layers = [make_layer_info("buildings", 0)]
        discovery = make_discovery_result(layers)

        with patch("portolan_cli.extract.wfs.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = discovery

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 1


class TestWFSMetadataSeeding:
    """Tests for WFS metadata seeding functions."""

    def test_report_metadata_to_wfs_metadata_conversion(self) -> None:
        """Converts MetadataExtracted to WFSMetadata correctly."""
        from portolan_cli.extract.common.report import MetadataExtracted
        from portolan_cli.extract.wfs.orchestrator import _report_metadata_to_wfs_metadata

        report_metadata = MetadataExtracted(
            source_url="https://example.com/wfs",
            description="A test WFS service",
            attribution="Test Provider",
            keywords=["test", "wfs"],
            contact_name="Test Contact",
            processing_notes="WFS service: Test Service",
            known_issues=None,
            license_info_raw="CC-BY-4.0",
        )

        wfs_metadata = _report_metadata_to_wfs_metadata(report_metadata)

        assert wfs_metadata.source_url == "https://example.com/wfs"
        assert wfs_metadata.service_abstract == "A test WFS service"
        assert wfs_metadata.provider_name == "Test Provider"
        assert wfs_metadata.keywords == ["test", "wfs"]
        assert wfs_metadata.access_constraints == "CC-BY-4.0"

    def test_seed_metadata_from_extraction_creates_file(self, tmp_path: Path) -> None:
        """Service-level metadata seeding creates catalog metadata.yaml."""
        from portolan_cli.extract.common.report import (
            ExtractionReport,
            ExtractionSummary,
            MetadataExtracted,
        )
        from portolan_cli.extract.wfs.orchestrator import _seed_metadata_from_extraction

        # Create .portolan directory (normally created by extract_wfs_catalog)
        (tmp_path / ".portolan").mkdir()

        report = ExtractionReport(
            extraction_date="2024-01-01T00:00:00Z",
            source_url="https://example.com/wfs",
            portolan_version="0.1.0",
            gpio_version="1.0.0",
            metadata_extracted=MetadataExtracted(
                source_url="https://example.com/wfs",
                description="Test WFS Service",
                attribution="Test Provider",
                keywords=["test"],
                contact_name=None,
                processing_notes=None,
                known_issues=None,
                license_info_raw=None,
            ),
            layers=[],
            summary=ExtractionSummary(
                total_layers=0,
                succeeded=0,
                failed=0,
                skipped=0,
                total_features=0,
                total_size_bytes=0,
                total_duration_seconds=0.0,
            ),
        )

        _seed_metadata_from_extraction(tmp_path, report)

        metadata_path = tmp_path / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()
        content = metadata_path.read_text()
        assert "Test Provider" in content

    def test_seed_collection_metadata_wfs_creates_files(self, tmp_path: Path) -> None:
        """Collection-level metadata seeding creates per-collection metadata.yaml."""
        from portolan_cli.extract.common.report import (
            ExtractionReport,
            ExtractionSummary,
            LayerResult,
            MetadataExtracted,
        )
        from portolan_cli.extract.wfs.orchestrator import _seed_collection_metadata_wfs

        # Create collection directory structure
        collection_dir = tmp_path / "buildings_abc123"
        collection_dir.mkdir()
        (collection_dir / ".portolan").mkdir()

        discovery = make_discovery_result(
            [
                LayerInfo(
                    name="ns:buildings",
                    typename="ns:buildings",
                    title="Buildings Layer",
                    abstract="All buildings in the city",
                    bbox=None,
                    id=0,
                ),
            ]
        )

        report = ExtractionReport(
            extraction_date="2024-01-01T00:00:00Z",
            source_url="https://example.com/wfs",
            portolan_version="0.1.0",
            gpio_version="1.0.0",
            metadata_extracted=MetadataExtracted(
                source_url="https://example.com/wfs",
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
                    name="ns:buildings",
                    status="success",
                    features=100,
                    size_bytes=1000,
                    duration_seconds=1.0,
                    output_path="buildings_abc123/buildings_abc123.parquet",
                    warnings=[],
                    error=None,
                    attempts=1,
                ),
            ],
            summary=ExtractionSummary(
                total_layers=1,
                succeeded=1,
                failed=0,
                skipped=0,
                total_features=100,
                total_size_bytes=1000,
                total_duration_seconds=1.0,
            ),
        )

        _seed_collection_metadata_wfs(tmp_path, report, discovery)

        metadata_path = collection_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()
        content = metadata_path.read_text()
        assert "All buildings in the city" in content
        assert "Buildings Layer" in content

    def test_seed_collection_metadata_wfs_skips_failed_layers(self, tmp_path: Path) -> None:
        """Collection seeding skips failed layer results."""
        from portolan_cli.extract.common.report import (
            ExtractionReport,
            ExtractionSummary,
            LayerResult,
            MetadataExtracted,
        )
        from portolan_cli.extract.wfs.orchestrator import _seed_collection_metadata_wfs

        discovery = make_discovery_result([make_layer_info("failed_layer", 0)])

        report = ExtractionReport(
            extraction_date="2024-01-01T00:00:00Z",
            source_url="https://example.com/wfs",
            portolan_version="0.1.0",
            gpio_version="1.0.0",
            metadata_extracted=MetadataExtracted(
                source_url="https://example.com/wfs",
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
                    name="failed_layer",
                    status="failed",
                    features=0,
                    size_bytes=0,
                    duration_seconds=0.0,
                    output_path="",
                    warnings=[],
                    error="Connection timeout",
                    attempts=3,
                ),
            ],
            summary=ExtractionSummary(
                total_layers=1,
                succeeded=0,
                failed=1,
                skipped=0,
                total_features=0,
                total_size_bytes=0,
                total_duration_seconds=0.0,
            ),
        )

        # Should not raise, should just skip
        _seed_collection_metadata_wfs(tmp_path, report, discovery)

        # No collection directory created since layer failed
        assert not (tmp_path / "failed_layer" / ".portolan" / "metadata.yaml").exists()

    def test_seed_collection_metadata_wfs_uses_iso_metadata(self, tmp_path: Path) -> None:
        """ISO metadata from CSW takes precedence over WFS GetCapabilities metadata."""

        from portolan_cli.extract.common.report import (
            ExtractionReport,
            ExtractionSummary,
            LayerResult,
            MetadataExtracted,
        )
        from portolan_cli.extract.csw.models import ISOMetadata
        from portolan_cli.extract.wfs.orchestrator import _seed_collection_metadata_wfs

        # Create collection directory structure
        collection_dir = tmp_path / "buildings_abc123"
        collection_dir.mkdir()
        (collection_dir / ".portolan").mkdir()

        # Layer with metadata_urls (triggers CSW fetch)
        discovery = make_discovery_result(
            [
                LayerInfo(
                    name="ns:buildings",
                    typename="ns:buildings",
                    title="WFS Title",
                    abstract="WFS Abstract (should be overridden)",
                    bbox=None,
                    id=0,
                    metadata_urls=[{"url": "https://csw.example.com/record"}],
                ),
            ]
        )

        report = ExtractionReport(
            extraction_date="2024-01-01T00:00:00Z",
            source_url="https://example.com/wfs",
            portolan_version="0.1.0",
            gpio_version="1.0.0",
            metadata_extracted=MetadataExtracted(
                source_url="https://example.com/wfs",
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
                    name="ns:buildings",
                    status="success",
                    features=100,
                    size_bytes=1000,
                    duration_seconds=1.0,
                    output_path="buildings_abc123/buildings_abc123.parquet",
                    warnings=[],
                    error=None,
                    attempts=1,
                ),
            ],
            summary=ExtractionSummary(
                total_layers=1,
                succeeded=1,
                failed=0,
                skipped=0,
                total_features=100,
                total_size_bytes=1000,
                total_duration_seconds=1.0,
            ),
        )

        # Mock the CSW fetch to return rich ISO metadata
        mock_iso = ISOMetadata(
            file_identifier="test-iso-id",
            title="ISO Title",
            abstract="Rich ISO description from CSW",
            contact_organization="ISO Contact Org",
            contact_email="iso@example.com",
        )

        with patch(
            "portolan_cli.extract.wfs.orchestrator._try_fetch_iso_metadata",
            return_value=mock_iso,
        ):
            _seed_collection_metadata_wfs(tmp_path, report, discovery)

        metadata_path = collection_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()
        content = metadata_path.read_text()
        # ISO metadata should be used, not WFS metadata
        assert "Rich ISO description from CSW" in content
        assert "WFS Abstract" not in content

    def test_seed_collection_metadata_wfs_nested_output_path(self, tmp_path: Path) -> None:
        """Collection seeding handles nested output paths correctly."""
        from portolan_cli.extract.common.report import (
            ExtractionReport,
            ExtractionSummary,
            LayerResult,
            MetadataExtracted,
        )
        from portolan_cli.extract.wfs.orchestrator import _seed_collection_metadata_wfs

        # Create nested collection directory structure (like multi-layer extract)
        nested_dir = tmp_path / "service" / "layer_abc123"
        nested_dir.mkdir(parents=True)
        (nested_dir / ".portolan").mkdir()

        discovery = make_discovery_result(
            [
                LayerInfo(
                    name="ns:layer",
                    typename="ns:layer",
                    title="Nested Layer",
                    abstract="Layer in nested structure",
                    bbox=None,
                    id=0,
                ),
            ]
        )

        report = ExtractionReport(
            extraction_date="2024-01-01T00:00:00Z",
            source_url="https://example.com/wfs",
            portolan_version="0.1.0",
            gpio_version="1.0.0",
            metadata_extracted=MetadataExtracted(
                source_url="https://example.com/wfs",
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
                    name="ns:layer",
                    status="success",
                    features=50,
                    size_bytes=500,
                    duration_seconds=0.5,
                    # Nested output path
                    output_path="service/layer_abc123/layer_abc123.parquet",
                    warnings=[],
                    error=None,
                    attempts=1,
                ),
            ],
            summary=ExtractionSummary(
                total_layers=1,
                succeeded=1,
                failed=0,
                skipped=0,
                total_features=50,
                total_size_bytes=500,
                total_duration_seconds=0.5,
            ),
        )

        _seed_collection_metadata_wfs(tmp_path, report, discovery)

        # Metadata should be in the nested collection directory
        metadata_path = nested_dir / ".portolan" / "metadata.yaml"
        assert metadata_path.exists()
        content = metadata_path.read_text()
        assert "Layer in nested structure" in content


class TestSlugify:
    """Tests for _slugify function (Issue #379)."""

    def test_basic_slug(self) -> None:
        """Basic name is slugified without hash."""
        result = _slugify("my_layer", disambiguate=False)
        assert result == "my_layer"
        assert len(result.split("_")) == 2  # No hash appended

    def test_slug_with_disambiguation(self) -> None:
        """Hash appended when disambiguate=True."""
        result = _slugify("my_layer", disambiguate=True)
        parts = result.split("_")
        assert len(parts) == 3  # my, layer, hash
        assert len(parts[-1]) == 6  # 6-char hash

    def test_slug_normalizes_special_chars(self) -> None:
        """Special characters converted to underscores."""
        result = _slugify("ns:Feature-Type.Name", disambiguate=False)
        assert result == "ns_feature_type_name"

    def test_slug_lowercase(self) -> None:
        """Names are lowercased."""
        result = _slugify("MyLayer", disambiguate=False)
        assert result == "mylayer"

    def test_slug_unique_id_differentiates(self) -> None:
        """Same name with different unique_ids produces different slugs."""
        slug1 = _slugify("same_name", disambiguate=True, unique_id=0)
        slug2 = _slugify("same_name", disambiguate=True, unique_id=1)
        assert slug1 != slug2


class TestAssignSlugs:
    """Tests for _assign_slugs function (Issue #379)."""

    def test_unique_names_no_hash(self) -> None:
        """Unique layer names get slugs without hash suffixes."""
        layers = [
            make_layer_info("layer_a", 0),
            make_layer_info("layer_b", 1),
            make_layer_info("layer_c", 2),
        ]
        slugs = _assign_slugs(layers)

        assert slugs[0] == "layer_a"
        assert slugs[1] == "layer_b"
        assert slugs[2] == "layer_c"

    def test_collision_gets_hash(self) -> None:
        """Colliding slugs get hash suffix."""
        # "ns:layer" and "ns_layer" both slugify to "ns_layer"
        layers = [
            make_layer_info("ns:layer", 0),
            make_layer_info("ns_layer", 1),
        ]
        slugs = _assign_slugs(layers)

        # Both should have hash suffixes due to collision
        assert "_" in slugs[0] and len(slugs[0].split("_")[-1]) == 6
        assert "_" in slugs[1] and len(slugs[1].split("_")[-1]) == 6
        # But they should be different
        assert slugs[0] != slugs[1]

    def test_partial_collision(self) -> None:
        """Only colliding names get hash, others stay clean."""
        # layer:a and layer_a both slugify to "layer_a" (collision)
        # layer_b and layer_c are unique
        layers = [
            make_layer_info("layer:a", 0),  # Slugifies to layer_a
            make_layer_info("layer_a", 1),  # Slugifies to layer_a (collision!)
            make_layer_info("layer_b", 2),  # Unique
            make_layer_info("layer_c", 3),  # Unique
        ]
        slugs = _assign_slugs(layers)

        # layer_b and layer_c: no collision, no hash
        assert slugs[2] == "layer_b"
        assert slugs[3] == "layer_c"

        # layer:a and layer_a: collision, both get hash
        assert len(slugs[0].split("_")[-1]) == 6
        assert len(slugs[1].split("_")[-1]) == 6
        assert slugs[0] != slugs[1]

    def test_identical_names_different_ids(self) -> None:
        """Identical names with different IDs produce different slugs."""
        # Edge case: exact same name but different layer IDs
        # (could happen with bad WFS data or duplicate entries)
        layers = [
            make_layer_info("buildings", 0),
            make_layer_info("buildings", 1),
        ]
        slugs = _assign_slugs(layers)

        # Both should have hash suffixes
        assert len(slugs[0].split("_")[-1]) == 6
        assert len(slugs[1].split("_")[-1]) == 6
        # Critical: they must be DIFFERENT despite identical names
        assert slugs[0] != slugs[1]
