"""Tests for ArcGIS extraction orchestrator.

The orchestrator ties together all extraction components:
URL parsing → Discovery → Filtering → Extraction → Report generation.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.extract.arcgis.discovery import (
    LayerInfo,
    ServiceDiscoveryResult,
    ServiceInfo,
)
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    ServicesRootDiscoveryResult,
    _slugify,
    extract_arcgis_catalog,
    list_services,
)

pytestmark = pytest.mark.unit

# Valid test URL that passes URL parser validation
TEST_FEATURE_SERVER_URL = (
    "https://services.arcgis.com/abc123/ArcGIS/rest/services/Census/FeatureServer"
)
TEST_SERVICES_ROOT_URL = "https://services.arcgis.com/abc123/ArcGIS/rest/services"


# =============================================================================
# _slugify tests
# =============================================================================


class TestSlugify:
    """Tests for _slugify helper function."""

    def test_converts_spaces_to_underscores(self) -> None:
        """Should convert spaces to underscores."""
        assert _slugify("Census Block Groups") == "census_block_groups"

    def test_lowercases_text(self) -> None:
        """Should convert to lowercase."""
        assert _slugify("CENSUS_TRACTS") == "census_tracts"

    def test_replaces_special_chars(self) -> None:
        """Should replace special characters with underscores."""
        assert _slugify("Layer (2024)") == "layer_2024"
        assert _slugify("Data-Set/Version.1") == "data_set_version_1"

    def test_strips_leading_trailing_underscores(self) -> None:
        """Should strip underscores from ends."""
        assert _slugify("  Layer  ") == "layer"
        assert _slugify("__test__") == "test"

    def test_returns_unnamed_for_empty_string(self) -> None:
        """Should return 'unnamed' for empty input."""
        assert _slugify("") == "unnamed"
        assert _slugify("___") == "unnamed"

    def test_handles_complex_names(self) -> None:
        """Should handle complex layer names from real services."""
        assert _slugify("2020 Census - Block Groups (PA)") == "2020_census_block_groups_pa"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_discovery_result() -> ServiceDiscoveryResult:
    """Create a mock discovery result with test layers."""
    return ServiceDiscoveryResult(
        layers=[
            LayerInfo(id=0, name="Census_Block_Groups", layer_type="Feature Layer"),
            LayerInfo(id=1, name="Census_Tracts", layer_type="Feature Layer"),
            LayerInfo(id=2, name="School_Districts", layer_type="Feature Layer"),
        ],
        service_description="Test Census Service",
        description="Test description",
        copyright_text="Test copyright",
        author="Test Author",
        keywords="test, census",
    )


@pytest.fixture
def mock_gpio() -> MagicMock:
    """Create a mock gpio module."""
    mock = MagicMock()
    mock_table = MagicMock()
    mock_table.__len__ = MagicMock(return_value=100)
    mock_table.sort_hilbert.return_value = mock_table
    mock.extract_arcgis.return_value = mock_table
    return mock


# =============================================================================
# extract_arcgis_catalog tests
# =============================================================================


class TestExtractArcgisCatalog:
    """Tests for extract_arcgis_catalog function."""

    def test_dry_run_returns_pending_layers(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Dry run should return layers without extracting."""
        with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = mock_discovery_result

            options = ExtractionOptions(dry_run=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
            )

            assert len(result.layers) == 3
            assert all(r.status == "pending" for r in result.layers)
            assert result.summary.total_layers == 3
            # Should not create output files
            assert not (tmp_path / ".portolan").exists()

    def test_dry_run_with_filter_applies_filter(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Dry run with filter should only include matching layers."""
        with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = mock_discovery_result

            options = ExtractionOptions(dry_run=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                layer_filter=["Census*"],
                options=options,
            )

            # Only Census_Block_Groups and Census_Tracts should match
            assert len(result.layers) == 2
            layer_names = [r.name for r in result.layers]
            assert "Census_Block_Groups" in layer_names
            assert "Census_Tracts" in layer_names
            assert "School_Districts" not in layer_names

    def test_dry_run_with_exclude_filter(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Dry run with exclude filter should exclude matching layers."""
        with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = mock_discovery_result

            options = ExtractionOptions(dry_run=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                layer_exclude=["*Tracts*"],
                options=options,
            )

            # Census_Tracts should be excluded
            assert len(result.layers) == 2
            layer_names = [r.name for r in result.layers]
            assert "Census_Tracts" not in layer_names

    def test_progress_callback_called(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Progress callback should be called for each layer."""
        progress_events: list[ExtractionProgress] = []

        def on_progress(progress: ExtractionProgress) -> None:
            progress_events.append(progress)

        with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = mock_discovery_result

            options = ExtractionOptions(dry_run=True)
            extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
                on_progress=on_progress,
            )

            # Dry run doesn't call progress callbacks - only real extraction does
            assert len(progress_events) == 0

    def test_extraction_creates_output_structure(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path, mock_gpio: MagicMock
    ) -> None:
        """Extraction should create proper output directory structure."""
        # Create a parquet file for the mock to simulate
        test_parquet = tmp_path / "test.parquet"
        test_parquet.write_bytes(b"test content")

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator.geoparquet_io", mock_gpio, create=True),
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            # Return (feature_count, file_size, duration) for each extraction
            mock_extract.return_value = (100, 1024, 1.5)

            # Use raw=True to skip auto-init (tested separately in integration tests)
            options = ExtractionOptions(raw=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
            )

            # Should create .portolan directory with report
            assert (tmp_path / ".portolan").exists()
            assert (tmp_path / ".portolan" / "extraction-report.json").exists()

            # Check report summary
            assert result.summary.total_layers == 3
            assert result.summary.succeeded == 3

    def test_services_root_dry_run_lists_services_and_layers(self, tmp_path: Path) -> None:
        """Services root dry run should list all services and their layers."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Transportation", service_type="MapServer"),
        ]
        mock_census_layers = ServiceDiscoveryResult(
            layers=[
                LayerInfo(id=0, name="Block_Groups", layer_type="Feature Layer"),
                LayerInfo(id=1, name="Tracts", layer_type="Feature Layer"),
            ],
        )
        mock_transport_layers = ServiceDiscoveryResult(
            layers=[
                LayerInfo(id=0, name="Roads", layer_type="Feature Layer"),
            ],
        )

        with (
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_services"
            ) as mock_discover_services,
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_layers"
            ) as mock_discover_layers,
        ):
            mock_discover_services.return_value = (mock_services, [])
            # Return different layers for each service
            mock_discover_layers.side_effect = [mock_census_layers, mock_transport_layers]

            options = ExtractionOptions(dry_run=True)
            result = extract_arcgis_catalog(
                url=TEST_SERVICES_ROOT_URL,
                output_dir=tmp_path,
                options=options,
            )

            # Should discover services first
            mock_discover_services.assert_called_once()

            # Should probe each service for layers
            assert mock_discover_layers.call_count == 2

            # Result should contain all layers from all services
            assert result.summary.total_layers == 3
            layer_names = [r.name for r in result.layers]
            assert "Block_Groups" in layer_names
            assert "Tracts" in layer_names
            assert "Roads" in layer_names

    def test_report_contains_metadata(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Report should contain extracted metadata."""
        with patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover:
            mock_discover.return_value = mock_discovery_result

            options = ExtractionOptions(dry_run=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
            )

            assert result.source_url == TEST_FEATURE_SERVER_URL
            assert result.metadata_extracted is not None
            # Keywords come from service metadata
            assert result.metadata_extracted.keywords == ["test", "census"]


class TestExtractionOptions:
    """Tests for ExtractionOptions dataclass."""

    def test_default_values(self) -> None:
        """Should have sensible defaults."""
        options = ExtractionOptions()

        assert options.workers == 3
        assert options.retries == 3
        assert options.timeout == 60.0
        assert options.resume is False
        assert options.dry_run is False
        assert options.sort_hilbert is True

    def test_custom_values(self) -> None:
        """Should accept custom values."""
        options = ExtractionOptions(
            workers=5,
            retries=5,
            timeout=120.0,
            resume=True,
            dry_run=True,
            sort_hilbert=False,
        )

        assert options.workers == 5
        assert options.retries == 5
        assert options.timeout == 120.0
        assert options.resume is True
        assert options.dry_run is True
        assert options.sort_hilbert is False


class TestExtractionProgress:
    """Tests for ExtractionProgress dataclass."""

    def test_creates_progress_event(self) -> None:
        """Should create progress event with all fields."""
        progress = ExtractionProgress(
            layer_index=0,
            total_layers=5,
            layer_name="Test_Layer",
            status="extracting",
        )

        assert progress.layer_index == 0
        assert progress.total_layers == 5
        assert progress.layer_name == "Test_Layer"
        assert progress.status == "extracting"


# =============================================================================
# Integration tests (with mocked gpio)
# =============================================================================


class TestOrchestratorIntegration:
    """Integration tests for orchestrator with mocked external dependencies."""

    def test_full_extraction_flow(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Test full extraction flow with mocked gpio."""
        progress_events: list[ExtractionProgress] = []

        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover.return_value = mock_discovery_result
            mock_extract.return_value = (100, 2048, 2.5)

            # Use raw=True to skip auto-init (tested separately in integration tests)
            options = ExtractionOptions(raw=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
                on_progress=lambda p: progress_events.append(p),
            )

            # Verify extraction was called for each layer
            assert mock_extract.call_count == 3

            # Verify report structure
            assert result.summary.total_layers == 3
            assert result.summary.succeeded == 3
            assert result.summary.failed == 0
            assert result.summary.total_features == 300  # 100 * 3 layers
            assert result.summary.total_size_bytes == 6144  # 2048 * 3 layers

            # Verify progress events
            assert len(progress_events) == 9  # 3 events per layer (starting, extracting, success)
            starting_events = [e for e in progress_events if e.status == "starting"]
            assert len(starting_events) == 3

    def test_extraction_failure_recorded(
        self, mock_discovery_result: ServiceDiscoveryResult, tmp_path: Path
    ) -> None:
        """Failed extractions should be recorded in report."""
        with (
            patch("portolan_cli.extract.arcgis.orchestrator.discover_layers") as mock_discover,
            patch("portolan_cli.extract.arcgis.orchestrator.retry_with_backoff") as mock_retry,
        ):
            mock_discover.return_value = mock_discovery_result

            # Mock retry to return failure
            from portolan_cli.extract.arcgis.retry import RetryResult

            mock_retry.return_value = RetryResult(
                success=False,
                value=None,
                attempts=3,
                error=Exception("Connection timeout"),
            )

            # Use raw=True to skip auto-init (tested separately in integration tests)
            options = ExtractionOptions(raw=True)
            result = extract_arcgis_catalog(
                url=TEST_FEATURE_SERVER_URL,
                output_dir=tmp_path,
                options=options,
            )

            # All layers should fail
            assert result.summary.succeeded == 0
            assert result.summary.failed == 3
            assert all(r.status == "failed" for r in result.layers)
            assert all("Connection timeout" in (r.error or "") for r in result.layers)


# =============================================================================
# Services root support tests
# =============================================================================


class TestListServices:
    """Tests for list_services function (--list-services mode)."""

    def test_list_services_returns_services_only(self) -> None:
        """list_services should return services without probing for layers."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Transportation", service_type="MapServer"),
            ServiceInfo(name="Basemap", service_type="MapServer"),
        ]

        with patch("portolan_cli.extract.arcgis.orchestrator.discover_services") as mock_discover:
            mock_discover.return_value = (mock_services, ["Archived", "Internal"])

            result = list_services(TEST_SERVICES_ROOT_URL)

            # Should call discover_services with return_folders=True
            mock_discover.assert_called_once()
            call_kwargs = mock_discover.call_args.kwargs
            assert call_kwargs.get("return_folders") is True

            # Should return all services
            assert len(result.services) == 3
            assert result.services[0].name == "Census_2020"
            assert result.services[1].name == "Transportation"

            # Should include folders
            assert result.folders == ["Archived", "Internal"]

    def test_list_services_filters_by_type(self) -> None:
        """list_services should filter by service type."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Transportation", service_type="MapServer"),
        ]

        with patch("portolan_cli.extract.arcgis.orchestrator.discover_services") as mock_discover:
            mock_discover.return_value = (mock_services, [])

            result = list_services(
                TEST_SERVICES_ROOT_URL,
                service_types=["FeatureServer"],
            )

            # discover_services should be called with service_types filter
            mock_discover.assert_called_once()
            call_kwargs = mock_discover.call_args.kwargs
            assert call_kwargs.get("service_types") == ["FeatureServer"]

            # Result should contain the services
            assert len(result.services) == 2

    def test_list_services_applies_glob_filter(self) -> None:
        """list_services should apply glob pattern filters."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Census_2010", service_type="FeatureServer"),
            ServiceInfo(name="Transportation", service_type="MapServer"),
        ]

        with patch("portolan_cli.extract.arcgis.orchestrator.discover_services") as mock_discover:
            mock_discover.return_value = (mock_services, [])

            result = list_services(
                TEST_SERVICES_ROOT_URL,
                service_filter=["Census*"],
            )

            # Should only return Census services
            assert len(result.services) == 2
            assert all("Census" in s.name for s in result.services)

    def test_list_services_raises_for_non_services_root(self) -> None:
        """list_services should raise error for non-services-root URLs."""
        with pytest.raises(ValueError, match="not a services root URL"):
            list_services(TEST_FEATURE_SERVER_URL)


class TestServicesRootDiscoveryResult:
    """Tests for ServicesRootDiscoveryResult dataclass."""

    def test_creates_with_services_and_folders(self) -> None:
        """Should create result with services and folders."""
        services = [
            ServiceInfo(name="Test", service_type="FeatureServer"),
        ]
        result = ServicesRootDiscoveryResult(
            services=services,
            folders=["Archived"],
            base_url=TEST_SERVICES_ROOT_URL,
        )

        assert len(result.services) == 1
        assert result.folders == ["Archived"]
        assert result.base_url == TEST_SERVICES_ROOT_URL

    def test_to_dict_for_json_output(self) -> None:
        """Should convert to dict for JSON serialization."""
        services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Roads", service_type="MapServer"),
        ]
        result = ServicesRootDiscoveryResult(
            services=services,
            folders=["Archived"],
            base_url=TEST_SERVICES_ROOT_URL,
        )

        d = result.to_dict()

        assert d["base_url"] == TEST_SERVICES_ROOT_URL
        assert len(d["services"]) == 2
        assert d["services"][0]["name"] == "Census_2020"
        assert d["services"][0]["type"] == "FeatureServer"
        assert d["services"][0]["url"].endswith("Census_2020/FeatureServer")
        assert d["folders"] == ["Archived"]
        assert d["total_services"] == 2


class TestServicesRootExtraction:
    """Tests for services root extraction (full extraction mode)."""

    def test_services_root_extracts_all_services(self, tmp_path: Path) -> None:
        """Services root extraction should extract all services and layers."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
        ]
        mock_census_layers = ServiceDiscoveryResult(
            layers=[
                LayerInfo(id=0, name="Block_Groups", layer_type="Feature Layer"),
            ],
        )

        with (
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_services"
            ) as mock_discover_services,
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_layers"
            ) as mock_discover_layers,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover_services.return_value = (mock_services, [])
            mock_discover_layers.return_value = mock_census_layers
            mock_extract.return_value = (100, 2048, 2.5)

            # Use raw=True to skip auto-init (tested separately in integration tests)
            options = ExtractionOptions(raw=True)
            result = extract_arcgis_catalog(
                url=TEST_SERVICES_ROOT_URL,
                output_dir=tmp_path,
                options=options,
            )

            # Should extract layers
            assert mock_extract.call_count == 1
            assert result.summary.succeeded == 1

            # Should create .portolan directory with report
            assert (tmp_path / ".portolan").exists()
            assert (tmp_path / ".portolan" / "extraction-report.json").exists()

            # Verify _extract_single_layer was called with correct path structure
            # (service as subcatalog: census_2020/block_groups/block_groups.parquet)
            call_args = mock_extract.call_args
            output_path = call_args[0][2]  # Third positional arg is output_path
            assert "census_2020" in str(output_path)
            assert "block_groups" in str(output_path)

    def test_services_root_applies_service_filter(self, tmp_path: Path) -> None:
        """Services root should apply service filter."""
        mock_services = [
            ServiceInfo(name="Census_2020", service_type="FeatureServer"),
            ServiceInfo(name="Transportation", service_type="MapServer"),
        ]
        mock_census_layers = ServiceDiscoveryResult(
            layers=[
                LayerInfo(id=0, name="Block_Groups", layer_type="Feature Layer"),
            ],
        )

        with (
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_services"
            ) as mock_discover_services,
            patch(
                "portolan_cli.extract.arcgis.orchestrator.discover_layers"
            ) as mock_discover_layers,
            patch("portolan_cli.extract.arcgis.orchestrator._extract_single_layer") as mock_extract,
        ):
            mock_discover_services.return_value = (mock_services, [])
            mock_discover_layers.return_value = mock_census_layers
            mock_extract.return_value = (100, 2048, 2.5)

            # Use raw=True to skip auto-init (tested separately in integration tests)
            options = ExtractionOptions(raw=True)
            result = extract_arcgis_catalog(
                url=TEST_SERVICES_ROOT_URL,
                output_dir=tmp_path,
                service_filter=["Census*"],
                options=options,
            )

            # Should only probe Census service for layers
            assert mock_discover_layers.call_count == 1

            # Should only extract Census layers
            assert result.summary.total_layers == 1
