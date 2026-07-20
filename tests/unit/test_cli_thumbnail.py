"""Tests for the `portolan thumbnail` CLI command."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner


@pytest.fixture
def cli_runner() -> CliRunner:
    return CliRunner()


class TestThumbnailCommand:
    @pytest.mark.unit
    def test_vector_parquet_writes_thumbnail(
        self, cli_runner: CliRunner, tmp_path: Path, valid_points_parquet: Path
    ) -> None:
        pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")
        from portolan_cli.cli import cli

        src = tmp_path / "roads.parquet"
        shutil.copy2(valid_points_parquet, src)
        result = cli_runner.invoke(cli, ["thumbnail", str(src), "--basemap", "none"])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "roads.thumb.jpg").is_file()

    @pytest.mark.unit
    def test_raster_tif_writes_thumbnail(
        self, cli_runner: CliRunner, tmp_path: Path, valid_rgb_cog: Path
    ) -> None:
        pytest.importorskip("rasterio")
        pytest.importorskip("numpy")
        from portolan_cli.cli import cli

        src = tmp_path / "dem.tif"
        shutil.copy2(valid_rgb_cog, src)
        result = cli_runner.invoke(cli, ["thumbnail", str(src)])
        assert result.exit_code == 0, result.output
        assert (tmp_path / "dem.thumb.jpg").is_file()

    @pytest.mark.unit
    def test_unsupported_suffix_errors(self, cli_runner: CliRunner, tmp_path: Path) -> None:
        from portolan_cli.cli import cli

        src = tmp_path / "data.txt"
        src.write_text("nope")
        result = cli_runner.invoke(cli, ["thumbnail", str(src)])
        assert result.exit_code == 1
        assert "unsupported" in result.output.lower()

    @pytest.mark.unit
    def test_json_output_envelope(
        self, cli_runner: CliRunner, tmp_path: Path, valid_points_parquet: Path
    ) -> None:
        pytest.importorskip("geopandas")
        pytest.importorskip("matplotlib")
        from portolan_cli.cli import cli

        src = tmp_path / "roads.parquet"
        shutil.copy2(valid_points_parquet, src)
        result = cli_runner.invoke(
            cli, ["--format", "json", "thumbnail", str(src), "--basemap", "none"]
        )
        assert result.exit_code == 0, result.output
        data = json.loads(result.output)
        assert data["success"] is True
        assert data["command"] == "thumbnail"
        assert data["data"]["thumbnail"].endswith("roads.thumb.jpg")
