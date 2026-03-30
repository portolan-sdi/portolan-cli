"""Tests for ArcGIS extraction orchestrator.

The orchestrator ties together all extraction components:
URL parsing → Discovery → Filtering → Extraction → Report generation.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.extract.arcgis.discovery import LayerInfo, ServiceDiscoveryResult
from portolan_cli.extract.arcgis.orchestrator import (
    ExtractionOptions,
    ExtractionProgress,
    _slugify,
    extract_arcgis_catalog,
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

            options = ExtractionOptions()
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

    def test_services_root_not_supported(self, tmp_path: Path) -> None:
        """Should raise NotImplementedError for services root URLs."""
        with pytest.raises(NotImplementedError, match="Services root URLs not yet supported"):
            extract_arcgis_catalog(
                url=TEST_SERVICES_ROOT_URL,
                output_dir=tmp_path,
            )

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

            options = ExtractionOptions()
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

            options = ExtractionOptions()
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
