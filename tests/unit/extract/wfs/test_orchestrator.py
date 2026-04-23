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
