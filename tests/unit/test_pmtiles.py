"""Tests for PMTiles generation module.

Tests the core PMTiles generation functionality. Integration tests that
actually generate PMTiles require tippecanoe and are marked accordingly.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


class TestPMTilesErrors:
    """Tests for PMTiles error classes."""

    @pytest.mark.unit
    def test_pmtiles_not_available_error_message(self) -> None:
        """Error message includes installation instructions."""
        from portolan_cli.pmtiles import PMTilesNotAvailableError

        error = PMTilesNotAvailableError()
        assert "gpio-pmtiles" in str(error)
        assert "pip install" in str(error)

    @pytest.mark.unit
    def test_tippecanoe_not_found_error_message(self) -> None:
        """Error message includes installation instructions."""
        from portolan_cli.pmtiles import TippecanoeNotFoundError

        error = TippecanoeNotFoundError()
        assert "tippecanoe" in str(error)
        assert "brew" in str(error) or "apt" in str(error)

    @pytest.mark.unit
    def test_pmtiles_generation_error_includes_source(self) -> None:
        """Error includes source path and original error."""
        from portolan_cli.pmtiles import PMTilesGenerationError

        original = ValueError("test error")
        error = PMTilesGenerationError("/path/to/file.parquet", original)

        assert "/path/to/file.parquet" in str(error)
        assert "test error" in str(error)


class TestPMTilesResult:
    """Tests for PMTilesResult dataclass."""

    @pytest.mark.unit
    def test_total_counts_all_results(self) -> None:
        """Total property counts generated, skipped, and failed."""
        from portolan_cli.pmtiles import PMTilesResult

        result = PMTilesResult(
            generated=[Path("a.pmtiles"), Path("b.pmtiles")],
            skipped=[Path("c.pmtiles")],
            failed=[(Path("d.parquet"), "error")],
        )

        assert result.total == 4

    @pytest.mark.unit
    def test_success_true_when_no_failures(self) -> None:
        """Success is True when failed list is empty."""
        from portolan_cli.pmtiles import PMTilesResult

        result = PMTilesResult(
            generated=[Path("a.pmtiles")],
            skipped=[],
            failed=[],
        )

        assert result.success is True

    @pytest.mark.unit
    def test_success_false_when_failures_exist(self) -> None:
        """Success is False when failed list has items."""
        from portolan_cli.pmtiles import PMTilesResult

        result = PMTilesResult(
            generated=[],
            skipped=[],
            failed=[(Path("a.parquet"), "error")],
        )

        assert result.success is False


class TestCheckPMTilesAvailable:
    """Tests for dependency checking."""

    @pytest.mark.unit
    def test_raises_when_gpio_pmtiles_not_installed(self) -> None:
        """Raises PMTilesNotAvailableError when gpio-pmtiles missing."""
        from portolan_cli.pmtiles import (
            PMTilesNotAvailableError,
            check_pmtiles_available,
        )

        with patch.dict("sys.modules", {"gpio_pmtiles": None}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                # Mock the import to raise ImportError
                with patch(
                    "builtins.__import__",
                    side_effect=ImportError("No module named 'gpio_pmtiles'"),
                ):
                    with pytest.raises(PMTilesNotAvailableError):
                        check_pmtiles_available()

    @pytest.mark.unit
    def test_raises_when_tippecanoe_not_in_path(self) -> None:
        """Raises TippecanoeNotFoundError when tippecanoe not in PATH."""
        from portolan_cli.pmtiles import (
            TippecanoeNotFoundError,
            check_pmtiles_available,
        )

        # Mock gpio_pmtiles as available but tippecanoe missing
        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value=None):
                with pytest.raises(TippecanoeNotFoundError):
                    check_pmtiles_available()


class TestFindGeoparquetAssets:
    """Tests for _find_geoparquet_assets function."""

    @pytest.mark.unit
    def test_finds_parquet_assets_by_media_type(self, tmp_path: Path) -> None:
        """Finds assets with application/vnd.apache.parquet type."""
        from portolan_cli.pmtiles import _find_geoparquet_assets

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        assets = _find_geoparquet_assets(collection_dir)

        assert len(assets) == 1
        assert assets[0][0] == "data"
        assert assets[0][1].name == "data.parquet"

    @pytest.mark.unit
    def test_ignores_stac_items_parquet(self, tmp_path: Path) -> None:
        """Ignores parquet files with stac-items role."""
        from portolan_cli.pmtiles import _find_geoparquet_assets

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "geoparquet-items": {
                    "href": "./items.parquet",
                    "type": "application/x-parquet",
                    "roles": ["stac-items"],
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "items.parquet").write_bytes(b"PAR1")

        assets = _find_geoparquet_assets(collection_dir)

        assert len(assets) == 0

    @pytest.mark.unit
    def test_returns_empty_for_missing_collection_json(self, tmp_path: Path) -> None:
        """Returns empty list when collection.json doesn't exist."""
        from portolan_cli.pmtiles import _find_geoparquet_assets

        assets = _find_geoparquet_assets(tmp_path)

        assert assets == []


class TestShouldGenerate:
    """Tests for _should_generate function."""

    @pytest.mark.unit
    def test_returns_true_when_force(self, tmp_path: Path) -> None:
        """Returns True when force=True regardless of file state."""
        from portolan_cli.pmtiles import _should_generate

        parquet = tmp_path / "data.parquet"
        pmtiles = tmp_path / "data.pmtiles"
        parquet.write_bytes(b"PAR1")
        pmtiles.write_bytes(b"PMT")

        assert _should_generate(parquet, pmtiles, force=True) is True

    @pytest.mark.unit
    def test_returns_true_when_pmtiles_missing(self, tmp_path: Path) -> None:
        """Returns True when PMTiles file doesn't exist."""
        from portolan_cli.pmtiles import _should_generate

        parquet = tmp_path / "data.parquet"
        pmtiles = tmp_path / "data.pmtiles"
        parquet.write_bytes(b"PAR1")

        assert _should_generate(parquet, pmtiles, force=False) is True

    @pytest.mark.unit
    def test_returns_false_when_pmtiles_newer(self, tmp_path: Path) -> None:
        """Returns False when PMTiles is newer than parquet."""
        import time

        from portolan_cli.pmtiles import _should_generate

        parquet = tmp_path / "data.parquet"
        pmtiles = tmp_path / "data.pmtiles"

        parquet.write_bytes(b"PAR1")
        time.sleep(0.01)  # Ensure different mtime
        pmtiles.write_bytes(b"PMT")

        assert _should_generate(parquet, pmtiles, force=False) is False


class TestAddPMTilesAssetToCollection:
    """Tests for add_pmtiles_asset_to_collection function."""

    @pytest.mark.unit
    def test_adds_pmtiles_asset_with_correct_role(self, tmp_path: Path) -> None:
        """Adds PMTiles asset with role=['overview']."""
        from portolan_cli.pmtiles import add_pmtiles_asset_to_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                    "title": "Main data",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        add_pmtiles_asset_to_collection(collection_dir, "data", "./data.pmtiles")

        updated = json.loads((collection_dir / "collection.json").read_text())

        assert "data-tiles" in updated["assets"]
        assert updated["assets"]["data-tiles"]["roles"] == ["overview"]
        assert updated["assets"]["data-tiles"]["type"] == "application/vnd.pmtiles"

    @pytest.mark.unit
    def test_idempotent_does_not_duplicate(self, tmp_path: Path) -> None:
        """Calling twice doesn't create duplicate assets."""
        from portolan_cli.pmtiles import add_pmtiles_asset_to_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {"href": "./data.parquet"},
                "data-tiles": {"href": "./data.pmtiles"},
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        add_pmtiles_asset_to_collection(collection_dir, "data", "./data.pmtiles")

        updated = json.loads((collection_dir / "collection.json").read_text())

        # Should still have exactly 2 assets
        assert len(updated["assets"]) == 2


class TestGeneratePMTilesForCollection:
    """Tests for generate_pmtiles_for_collection function."""

    @pytest.mark.unit
    def test_returns_empty_result_for_no_geoparquet(self, tmp_path: Path) -> None:
        """Returns empty result when collection has no GeoParquet assets."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {"type": "Collection", "assets": {}}
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        # Mock dependencies as available
        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert result.total == 0
        assert result.success is True


# Integration tests that require tippecanoe
@pytest.mark.skipif(
    shutil.which("tippecanoe") is None,
    reason="tippecanoe not installed",
)
class TestPMTilesIntegration:
    """Integration tests that actually generate PMTiles.

    These tests require tippecanoe to be installed and are skipped
    if tippecanoe is not available.
    """

    @pytest.mark.integration
    def test_generates_pmtiles_from_geoparquet(self, tmp_path: Path) -> None:
        """Actually generates PMTiles from a real GeoParquet file."""
        # This test would use a real fixture and verify the output
        # Skipped for now as it requires a real GeoParquet file
        pytest.skip("Requires real GeoParquet fixture")
