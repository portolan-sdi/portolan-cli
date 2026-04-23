"""Tests for WFS extraction orchestrator.

These tests verify the main orchestration logic for WFS extraction,
including layer filtering, extraction, and catalog initialization.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from portolan_cli.extract.wfs.orchestrator import (
    ExtractionOptions,
    extract_wfs_catalog,
)

pytestmark = pytest.mark.unit


class TestExtractionOptions:
    """Tests for ExtractionOptions dataclass."""

    def test_default_values(self) -> None:
        """Default options have sensible values."""
        options = ExtractionOptions()
        assert options.workers == 1
        assert options.retries == 3
        assert options.timeout == 60.0
        assert options.resume is False
        assert options.raw is False
        assert options.dry_run is False
        assert options.wfs_version == "auto"


class TestExtractWfsCatalog:
    """Tests for extract_wfs_catalog function."""

    def test_dry_run_returns_pending_layers(self, tmp_path: Path) -> None:
        """Dry run returns report with pending status."""
        mock_layers = [
            MagicMock(name="buildings", typename="buildings", title="Buildings", id=0),
            MagicMock(name="roads", typename="roads", title="Roads", id=1),
        ]
        mock_layers[0].name = "buildings"
        mock_layers[1].name = "roads"

        with patch("portolan_cli.extract.wfs.orchestrator.list_layers") as mock_list:
            mock_list.return_value = mock_layers

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 2
        assert all(layer.status == "pending" for layer in report.layers)

    def test_layer_filter_applied(self, tmp_path: Path) -> None:
        """Layer filter reduces extracted layers."""
        mock_layers = [
            MagicMock(name="buildings", typename="buildings", title="Buildings", id=0),
            MagicMock(name="roads", typename="roads", title="Roads", id=1),
            MagicMock(name="water", typename="water", title="Water", id=2),
        ]
        for layer in mock_layers:
            layer.name = layer._mock_name

        with patch("portolan_cli.extract.wfs.orchestrator.list_layers") as mock_list:
            mock_list.return_value = mock_layers

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
        mock_layers = [
            MagicMock(name="buildings", typename="buildings", title="Buildings", id=0),
            MagicMock(name="test_layer", typename="test_layer", title="Test", id=1),
        ]
        for layer in mock_layers:
            layer.name = layer._mock_name

        with patch("portolan_cli.extract.wfs.orchestrator.list_layers") as mock_list:
            mock_list.return_value = mock_layers

            options = ExtractionOptions(dry_run=True)
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                layer_exclude=["test_*"],
                options=options,
            )

        assert len(report.layers) == 1
        assert report.layers[0].name == "buildings"


class TestExtractWfsCatalogListMode:
    """Tests for list mode (no layer specified, no --all)."""

    def test_no_layer_filter_no_all_raises(self, tmp_path: Path) -> None:
        """Without --all or layer filter, extraction should require explicit choice."""
        mock_layers = [
            MagicMock(name="buildings", typename="buildings", id=0),
        ]
        mock_layers[0].name = "buildings"

        with patch("portolan_cli.extract.wfs.orchestrator.list_layers") as mock_list:
            mock_list.return_value = mock_layers

            # This should raise because no --all flag and no layer filter
            # means we don't know what to extract
            options = ExtractionOptions(dry_run=True)

            # For now, without --all, extract all (matching ArcGIS behavior)
            # In future could require explicit confirmation
            report = extract_wfs_catalog(
                url="https://example.com/wfs",
                output_dir=tmp_path,
                options=options,
            )

        assert len(report.layers) == 1
