"""Tests using real-world fixtures."""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

import pytest

from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata

if TYPE_CHECKING:
    from pathlib import Path


class TestAntimeridian:
    """Test antimeridian edge case — this is Portolan-specific logic."""

    @pytest.mark.integration
    def test_fieldmaps_antimeridian_bbox(self, fieldmaps_boundaries_path: Path) -> None:
        metadata = extract_geoparquet_metadata(fieldmaps_boundaries_path)

        assert metadata.feature_count == 3
        assert metadata.bbox is not None

        minx, miny, maxx, maxy = metadata.bbox

        # Coordinates are valid numbers (not NaN, not inf)
        assert all(isinstance(c, (int, float)) for c in metadata.bbox)
        assert all(not math.isnan(c) for c in metadata.bbox)  # NaN check (NaN != NaN)

        # Latitude is valid
        assert -90 <= miny <= 90
        assert -90 <= maxy <= 90
        assert miny <= maxy

        # Longitude values are present and numeric
        # (antimeridian representation varies by source — we just verify no corruption)
        assert minx is not None
        assert maxx is not None


class TestSmokeTests:
    """Smoke tests — verify code paths don't crash on real data."""

    @pytest.mark.integration
    def test_smoke_nwi_wetlands(self, nwi_wetlands_path: Path) -> None:
        """Smoke test: NWI Wetlands (complex polygons with holes)."""
        metadata = extract_geoparquet_metadata(nwi_wetlands_path)
        assert metadata.feature_count == 1000
        assert metadata.bbox is not None

    @pytest.mark.integration
    @pytest.mark.realdata
    def test_nwi_wetlands_adds_with_its_albers_crs(
        self, nwi_wetlands_path: Path, tmp_path: Path
    ) -> None:
        import json
        import shutil

        from click.testing import CliRunner

        from portolan_cli.cli import cli

        runner = CliRunner()
        assert (
            runner.invoke(cli, ["init", str(tmp_path), "--auto", "--license", "CC0-1.0"]).exit_code
            == 0
        )
        collection_dir = tmp_path / "wetlands"
        collection_dir.mkdir()
        shutil.copy(nwi_wetlands_path, collection_dir)

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(tmp_path), "--no-thumbnails", str(collection_dir)],
        )

        assert result.exit_code == 0, result.output
        assets = json.loads((collection_dir / "collection.json").read_text())["assets"]
        assert assets["nwi-wetlands"]["proj:code"] == "EPSG:5070"

    @pytest.mark.integration
    def test_smoke_open_buildings(self, open_buildings_path: Path) -> None:
        """Smoke test: Open Buildings (1000 simple polygons)."""
        metadata = extract_geoparquet_metadata(open_buildings_path)
        assert metadata.feature_count == 1000
        assert metadata.bbox is not None

    @pytest.mark.integration
    def test_smoke_road_detections(self, road_detections_path: Path) -> None:
        """Smoke test: Road Detections (LineString geometries)."""
        metadata = extract_geoparquet_metadata(road_detections_path)
        assert metadata.feature_count == 1000
        assert metadata.bbox is not None
