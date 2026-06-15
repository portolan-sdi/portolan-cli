"""Tests for PMTiles generation module.

Tests the core PMTiles generation functionality. Integration tests that
actually generate PMTiles require tippecanoe and are marked accordingly.
"""

from __future__ import annotations

import json
import os
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
                    "type": "application/vnd.apache.parquet",
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
        assert updated["assets"]["data-tiles"]["roles"] == ["visual"]
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


class TestTrackGeneratedAssetsInVersions:
    """Tests for _track_generated_assets_in_versions (Issue #519).

    Generated side-step artifacts (PMTiles, thumbnail) must be tracked in
    versions.json with a checksum, size, and mtime so sync can verify integrity
    and skip unchanged files. PMTiles and its thumbnail share ONE version
    snapshot, not two.
    """

    @staticmethod
    def _write_versions(collection_dir: Path) -> None:
        versions_json = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-01T00:00:00+00:00",
                    "breaking": False,
                    "assets": {
                        "data.parquet": {
                            "sha256": "deadbeef",
                            "size_bytes": 100,
                            "href": "demographics/data.parquet",
                        }
                    },
                    "changes": ["data.parquet"],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_json))

    @pytest.mark.unit
    def test_asset_tracked_with_checksum_size_mtime(self, tmp_path: Path) -> None:
        """A generated thumbnail is added to versions.json as a full asset."""
        from portolan_cli.pmtiles import _track_generated_assets_in_versions
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        self._write_versions(collection_dir)

        thumb = collection_dir / "data.thumb.png"
        thumb.write_bytes(b"\x89PNG fake-thumbnail-bytes")

        _track_generated_assets_in_versions(
            collection_dir, [thumb], tmp_path, message="Generated thumbnail: data.thumb.png"
        )

        versions = read_versions(collection_dir / "versions.json")
        assert versions.current_version == "1.0.1"

        latest = versions.versions[-1]
        # Full snapshot: prior data asset carried forward, thumbnail added.
        assert "data.parquet" in latest.assets
        assert "data.thumb.png" in latest.assets

        thumb_asset = latest.assets["data.thumb.png"]
        assert thumb_asset.sha256
        assert thumb_asset.size_bytes == thumb.stat().st_size
        assert thumb_asset.mtime is not None
        # Href is catalog-root-relative (includes the collection dir).
        assert thumb_asset.href == "demographics/data.thumb.png"
        # A regenerated thumbnail shows up in the version's changes array.
        assert "data.thumb.png" in latest.changes

    @pytest.mark.unit
    def test_pmtiles_and_thumbnail_share_one_version(self, tmp_path: Path) -> None:
        """PMTiles + thumbnail tracked together land in a SINGLE version (Issue #519)."""
        from portolan_cli.pmtiles import _track_generated_assets_in_versions
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        self._write_versions(collection_dir)

        pmtiles = collection_dir / "data.pmtiles"
        pmtiles.write_bytes(b"PMTILES")
        thumb = collection_dir / "data.thumb.jpg"
        thumb.write_bytes(b"\xff\xd8\xff jpeg")

        _track_generated_assets_in_versions(
            collection_dir, [pmtiles, thumb], tmp_path, message="Generated PMTiles and thumbnail"
        )

        versions = read_versions(collection_dir / "versions.json")
        # One bump (1.0.0 -> 1.0.1), not two.
        assert len(versions.versions) == 2
        assert versions.current_version == "1.0.1"
        latest = versions.versions[-1].assets
        assert "data.pmtiles" in latest
        assert "data.thumb.jpg" in latest

    @pytest.mark.unit
    def test_only_if_missing_skips_already_tracked(self, tmp_path: Path) -> None:
        """only_if_missing creates no version when every asset is already tracked."""
        from portolan_cli.pmtiles import _track_generated_assets_in_versions
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        self._write_versions(collection_dir)

        parquet = collection_dir / "data.parquet"  # already tracked in fixture
        parquet.write_bytes(b"PAR1")

        _track_generated_assets_in_versions(
            collection_dir, [parquet], tmp_path, message="Backfill", only_if_missing=True
        )

        versions = read_versions(collection_dir / "versions.json")
        # No new version: data.parquet is already in the latest snapshot.
        assert len(versions.versions) == 1
        assert versions.current_version == "1.0.0"

    @pytest.mark.unit
    def test_creates_versions_file_when_missing(self, tmp_path: Path) -> None:
        """First-ever version is created at 1.0.0 if versions.json is absent."""
        from portolan_cli.pmtiles import _track_generated_assets_in_versions
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()

        thumb = collection_dir / "data.thumb.png"
        thumb.write_bytes(b"\x89PNG fake-thumbnail-bytes")

        _track_generated_assets_in_versions(
            collection_dir, [thumb], tmp_path, message="Generated thumbnail: data.thumb.png"
        )

        versions = read_versions(collection_dir / "versions.json")
        assert versions.current_version == "1.0.0"
        assert "data.thumb.png" in versions.versions[-1].assets

    @pytest.mark.unit
    def test_raises_when_asset_missing(self, tmp_path: Path) -> None:
        """A missing asset file is a hard error (no phantom asset)."""
        from portolan_cli.pmtiles import _track_generated_assets_in_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()
        self._write_versions(collection_dir)

        with pytest.raises(FileNotFoundError):
            _track_generated_assets_in_versions(
                collection_dir, [collection_dir / "data.thumb.png"], tmp_path, message="x"
            )


class TestGeneratePMTiles:
    """Tests for generate_pmtiles function parameter passthrough."""

    @pytest.mark.unit
    def test_passes_all_parameters_to_gpio_pmtiles(self, tmp_path: Path) -> None:
        """All parameters are passed through to create_pmtiles_from_geoparquet."""
        from portolan_cli.pmtiles import generate_pmtiles

        parquet = tmp_path / "data.parquet"
        pmtiles = tmp_path / "data.pmtiles"
        parquet.write_bytes(b"PAR1")

        mock_create = MagicMock()
        mock_module = MagicMock()
        mock_module.create_pmtiles_from_geoparquet = mock_create

        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                generate_pmtiles(
                    parquet,
                    pmtiles,
                    min_zoom=2,
                    max_zoom=12,
                    layer="test-layer",
                    bbox="-122.5,37.5,-122.0,38.0",
                    where="population > 1000",
                    include_cols="name,geometry",
                    precision=5,
                    attribution="© Test",
                    src_crs="EPSG:3857",
                )

        mock_create.assert_called_once_with(
            input_path=str(parquet),
            output_path=str(pmtiles),
            min_zoom=2,
            max_zoom=12,
            layer="test-layer",
            bbox="-122.5,37.5,-122.0,38.0",
            where="population > 1000",
            include_cols="name,geometry",
            precision=5,
            attribution="© Test",
            src_crs="EPSG:3857",
        )

    @pytest.mark.unit
    def test_default_precision_is_six(self, tmp_path: Path) -> None:
        """Default precision value is 6."""
        from portolan_cli.pmtiles import generate_pmtiles

        parquet = tmp_path / "data.parquet"
        pmtiles = tmp_path / "data.pmtiles"
        parquet.write_bytes(b"PAR1")

        mock_create = MagicMock()
        mock_module = MagicMock()
        mock_module.create_pmtiles_from_geoparquet = mock_create

        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                generate_pmtiles(parquet, pmtiles)

        call_kwargs = mock_create.call_args[1]
        assert call_kwargs["precision"] == 6


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

    @pytest.mark.unit
    def test_passes_all_parameters_to_generate_pmtiles(self, tmp_path: Path) -> None:
        """All parameters are forwarded to generate_pmtiles."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        # Create collection with parquet asset
        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        # Create versions.json
        versions_json = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_json))

        mock_generate = MagicMock()
        mock_module = MagicMock()

        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate):
                    generate_pmtiles_for_collection(
                        collection_dir,
                        tmp_path,
                        min_zoom=2,
                        max_zoom=12,
                        layer="test",
                        bbox="-122,37,-121,38",
                        where="pop > 100",
                        include_cols="name",
                        precision=4,
                        attribution="© Me",
                        src_crs="EPSG:4326",
                    )

        # Verify generate_pmtiles was called with all parameters
        mock_generate.assert_called_once()
        call_kwargs = mock_generate.call_args[1]
        assert call_kwargs["min_zoom"] == 2
        assert call_kwargs["max_zoom"] == 12
        assert call_kwargs["layer"] == "test"
        assert call_kwargs["bbox"] == "-122,37,-121,38"
        assert call_kwargs["where"] == "pop > 100"
        assert call_kwargs["include_cols"] == "name"
        assert call_kwargs["precision"] == 4
        assert call_kwargs["attribution"] == "© Me"
        assert call_kwargs["src_crs"] == "EPSG:4326"

    @pytest.mark.unit
    def test_cleans_up_partial_file_on_failure(self, tmp_path: Path) -> None:
        """Partial PMTiles file is deleted when generation fails (Issue #385).

        When gpio-pmtiles/tippecanoe fails mid-generation, it may leave a
        partial output file. This test verifies that such files are cleaned
        up to prevent phantom assets in versions.json on subsequent add runs.
        """
        from portolan_cli.pmtiles import (
            PMTilesGenerationError,
            generate_pmtiles_for_collection,
        )

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        # Create collection with parquet asset
        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        pmtiles_path = collection_dir / "data.pmtiles"

        # Mock generate_pmtiles to create partial file then raise
        def mock_generate_raises(*args: object, **kwargs: object) -> None:
            # Simulate: gpio-pmtiles creates file, then fails mid-processing
            pmtiles_path.write_bytes(b"partial content")
            raise PMTilesGenerationError("data.parquet", ValueError("Non-geospatial"))

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate_raises):
                    result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        # Verify failure was recorded
        assert len(result.failed) == 1
        assert "data.parquet" in str(result.failed[0][0])

        # KEY ASSERTION: Partial file should be cleaned up (Issue #385)
        assert not pmtiles_path.exists(), (
            "Partial PMTiles file should be deleted on generation failure. "
            "Leaving it causes phantom assets in versions.json on next add."
        )

    @pytest.mark.unit
    def test_cleans_up_partial_file_on_unexpected_error(self, tmp_path: Path) -> None:
        """Partial PMTiles file is deleted on unexpected errors too (Issue #385)."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        pmtiles_path = collection_dir / "data.pmtiles"

        # Mock to create partial file then raise unexpected exception
        def mock_generate_unexpected(*args: object, **kwargs: object) -> None:
            pmtiles_path.write_bytes(b"partial")
            raise RuntimeError("Unexpected tippecanoe crash")

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate_unexpected):
                    result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert len(result.failed) == 1
        assert not pmtiles_path.exists(), "Partial file should be cleaned up on any error"

    @pytest.mark.unit
    def test_cleans_up_partial_file_on_keyboard_interrupt(self, tmp_path: Path) -> None:
        """Partial PMTiles file is deleted even on KeyboardInterrupt (Issue #385).

        KeyboardInterrupt inherits from BaseException, not Exception.
        The finally block ensures cleanup even when user hits Ctrl+C.
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection

        collection_dir = tmp_path / "collection"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        pmtiles_path = collection_dir / "data.pmtiles"

        def mock_generate_interrupted(*args: object, **kwargs: object) -> None:
            pmtiles_path.write_bytes(b"partial")
            raise KeyboardInterrupt()

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate_interrupted):
                    with pytest.raises(KeyboardInterrupt):
                        generate_pmtiles_for_collection(collection_dir, tmp_path)

        # KEY ASSERTION: Partial file cleaned up even on KeyboardInterrupt
        assert not pmtiles_path.exists(), (
            "Partial file must be cleaned up on KeyboardInterrupt. "
            "finally block handles BaseException subclasses."
        )

    @pytest.mark.unit
    def test_generated_thumbnail_tracked_in_versions(self, tmp_path: Path) -> None:
        """PMTiles and thumbnail land in ONE versions.json snapshot (Issue #519).

        Regression: the side-step registered the thumbnail in collection.json
        but never tracked it in versions.json, so it shipped without a
        checksum/size and sync could not verify it. A second regression had the
        PMTiles and thumbnail each bump their own version (two snapshots for one
        side-step); they must now share a single version snapshot.
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))
        (collection_dir / "data.parquet").write_bytes(b"PAR1")

        versions_json = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_json))

        pmtiles_path = collection_dir / "data.pmtiles"
        thumb_path = collection_dir / "data.thumb.png"

        def mock_generate(*args: object, **kwargs: object) -> None:
            pmtiles_path.write_bytes(b"PMTILES")

        def mock_thumbnail(*args: object, **kwargs: object) -> Path:
            thumb_path.write_bytes(b"\x89PNG fake-thumbnail-bytes")
            return thumb_path

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate):
                    with patch(
                        "portolan_cli.pmtiles.generate_vector_thumbnail",
                        mock_thumbnail,
                    ):
                        result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert pmtiles_path in result.generated

        versions = read_versions(collection_dir / "versions.json")
        # One side-step == one new version snapshot, not two (Issue #519).
        # The old double-bump behavior produced 3 versions here.
        assert len(versions.versions) == 2, (
            "PMTiles + thumbnail must share a single version snapshot, not bump twice"
        )
        all_tracked = versions.versions[-1].assets
        assert "data.pmtiles" in all_tracked, "PMTiles should still be tracked"
        assert "data.thumb.png" in all_tracked, (
            "Generated thumbnail must be tracked in versions.json (Issue #519)"
        )
        thumb_asset = all_tracked["data.thumb.png"]
        assert thumb_asset.sha256
        assert thumb_asset.size_bytes == thumb_path.stat().st_size
        assert thumb_asset.href == "demographics/data.thumb.png"

    @pytest.mark.unit
    def test_skip_path_backfills_untracked_thumbnail(self, tmp_path: Path) -> None:
        """Skip path backfills artifacts left untracked by the old code (Issue #519).

        When PMTiles is up-to-date, generation is skipped. A thumbnail generated
        before tracking existed (present on disk, in collection.json, but absent
        from versions.json) must be backfilled — without forcing regeneration —
        and in exactly one version bump.
        """
        from portolan_cli.pmtiles import generate_pmtiles_for_collection
        from portolan_cli.versions import read_versions

        collection_dir = tmp_path / "demographics"
        collection_dir.mkdir()

        collection_json = {
            "type": "Collection",
            "assets": {
                "data": {
                    "href": "./data.parquet",
                    "type": "application/vnd.apache.parquet",
                }
            },
        }
        (collection_dir / "collection.json").write_text(json.dumps(collection_json))

        # Source parquet OLDER than pmtiles -> _should_generate returns False.
        parquet = collection_dir / "data.parquet"
        parquet.write_bytes(b"PAR1")
        pmtiles = collection_dir / "data.pmtiles"
        pmtiles.write_bytes(b"PMTILES")
        # Thumbnail uses the .thumb.jpg convention (thumbnail_path_for).
        thumb = collection_dir / "data.thumb.jpg"
        thumb.write_bytes(b"\xff\xd8\xff fake-jpeg-bytes")

        old = parquet.stat().st_mtime - 100
        os.utime(parquet, (old, old))

        # versions.json tracks neither the pmtiles nor the thumbnail yet.
        versions_json = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-01T00:00:00Z",
                    "breaking": False,
                    "assets": {},
                    "changes": [],
                }
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(versions_json))

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert pmtiles in result.skipped, "Up-to-date PMTiles should be skipped, not regenerated"

        versions = read_versions(collection_dir / "versions.json")
        # Exactly one backfill version bump.
        assert len(versions.versions) == 2
        latest = versions.versions[-1].assets
        assert "data.pmtiles" in latest, "Untracked PMTiles should be backfilled on skip"
        assert "data.thumb.jpg" in latest, "Untracked thumbnail should be backfilled on skip"

        # Idempotent: a second run with everything tracked creates NO new version.
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                generate_pmtiles_for_collection(collection_dir, tmp_path)
        versions_after = read_versions(collection_dir / "versions.json")
        assert len(versions_after.versions) == 2, (
            "Backfill must not bump a version when everything is already tracked"
        )

    @staticmethod
    def _fresh_collection(collection_dir: Path) -> Path:
        """A collection with one parquet asset and an empty 1.0.0 baseline."""
        collection_dir.mkdir()
        (collection_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "assets": {
                        "data": {
                            "href": "./data.parquet",
                            "type": "application/vnd.apache.parquet",
                        }
                    },
                }
            )
        )
        (collection_dir / "data.parquet").write_bytes(b"PAR1")
        (collection_dir / "versions.json").write_text(
            json.dumps(
                {
                    "spec_version": "1.0.0",
                    "current_version": "1.0.0",
                    "versions": [
                        {
                            "version": "1.0.0",
                            "created": "2026-01-01T00:00:00Z",
                            "breaking": False,
                            "assets": {},
                            "changes": [],
                        }
                    ],
                }
            )
        )
        return collection_dir

    @pytest.mark.unit
    def test_thumbnail_disabled_tracks_only_pmtiles(self, tmp_path: Path) -> None:
        """When thumbnails are disabled, only the PMTiles is tracked (one version)."""
        from unittest.mock import patch as _patch

        from portolan_cli.pmtiles import generate_pmtiles_for_collection
        from portolan_cli.thumbnail import ThumbnailConfig
        from portolan_cli.versions import read_versions

        collection_dir = self._fresh_collection(tmp_path / "demographics")
        pmtiles_path = collection_dir / "data.pmtiles"

        def mock_generate(*args: object, **kwargs: object) -> None:
            pmtiles_path.write_bytes(b"PMTILES")

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate):
                    with _patch(
                        "portolan_cli.pmtiles.get_thumbnail_config",
                        return_value=ThumbnailConfig(enabled=False),
                    ):
                        result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert pmtiles_path in result.generated
        versions = read_versions(collection_dir / "versions.json")
        assert len(versions.versions) == 2, "PMTiles-only generation is still one version"
        latest = versions.versions[-1].assets
        assert "data.pmtiles" in latest
        assert not any(k.endswith(".thumb.jpg") or k.endswith(".thumb.png") for k in latest), (
            "No thumbnail should be tracked when thumbnails are disabled"
        )

    @pytest.mark.unit
    def test_thumbnail_render_failure_tracks_only_pmtiles(self, tmp_path: Path) -> None:
        """A failed thumbnail render must not block PMTiles tracking (Issue #13)."""
        from portolan_cli.pmtiles import generate_pmtiles_for_collection
        from portolan_cli.versions import read_versions

        collection_dir = self._fresh_collection(tmp_path / "demographics")
        pmtiles_path = collection_dir / "data.pmtiles"

        def mock_generate(*args: object, **kwargs: object) -> None:
            pmtiles_path.write_bytes(b"PMTILES")

        def boom(*args: object, **kwargs: object) -> None:
            raise RuntimeError("render exploded")

        mock_module = MagicMock()
        with patch.dict("sys.modules", {"gpio_pmtiles": mock_module}):
            with patch("portolan_cli.pmtiles.shutil.which", return_value="/usr/bin/tippecanoe"):
                with patch("portolan_cli.pmtiles.generate_pmtiles", mock_generate):
                    with patch("portolan_cli.pmtiles.generate_vector_thumbnail", boom):
                        result = generate_pmtiles_for_collection(collection_dir, tmp_path)

        assert pmtiles_path in result.generated, "PMTiles must succeed despite thumbnail failure"
        versions = read_versions(collection_dir / "versions.json")
        # PMTiles still tracked, in exactly one version; no thumbnail.
        assert len(versions.versions) == 2
        latest = versions.versions[-1].assets
        assert "data.pmtiles" in latest
        assert not any(".thumb." in k for k in latest)


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
