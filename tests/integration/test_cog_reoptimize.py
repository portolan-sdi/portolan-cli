"""Tests for COG re-optimization (--force) and parallel conversion (issue #530).

Covers:
- convert_file(force=True): re-encodes already-valid COGs (raster only), adding
  overviews via current CogSettings; leaves vectors skipped.
- convert_directory(workers=N): parallel conversion via ProcessPoolExecutor with
  per-file on_progress callbacks, matching the serial result set.
- check_directory(force=, workers=): unions CLOUD_NATIVE rasters into the fix set
  and threads force/workers through; dry-run preview lists forced COGs.

Per ADR-0010, conversion itself is delegated to rio-cogeo; these tests verify
Portolan's orchestration (the force bypass, parallelism, and check threading),
not raster math.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds


def _make_cog_without_overviews(path: Path, size: int = 1500) -> Path:
    """Write a valid COG that has NO internal overviews.

    rio-cogeo's ``cog_translate`` with ``overview_level=0`` produces a COG that
    passes ``cog_validate`` (overviews are warnings, not errors) but lacks the
    pyramid that makes web rendering fast. This reproduces the JRC GloFAS source
    tiles from issue #530. ``size`` is larger than the 512px default tile size so
    that a forced re-encode auto-generates at least one overview level.
    """
    from rio_cogeo.cogeo import cog_translate
    from rio_cogeo.profiles import cog_profiles

    plain = path.parent / f".{path.stem}.plain.tif"
    transform = from_bounds(-122.5, 37.7, -122.3, 37.9, size, size)
    with rasterio.open(
        plain,
        "w",
        driver="GTiff",
        height=size,
        width=size,
        count=1,
        dtype="uint8",
        crs="EPSG:4326",
        transform=transform,
    ) as dst:
        dst.write((np.indices((size, size)).sum(axis=0) % 256).astype("uint8"), 1)

    profile = cog_profiles.get("deflate")
    cog_translate(str(plain), str(path), profile, quiet=True, overview_level=0)
    plain.unlink()
    return path


def _overview_count(path: Path) -> int:
    """Number of overview levels on band 1 of a raster."""
    with rasterio.open(path) as src:
        return len(src.overviews(1))


class TestConvertFileForce:
    """convert_file(force=True) re-optimizes valid COGs (rasters only)."""

    @pytest.mark.integration
    def test_force_reencodes_cog_and_adds_overviews(self, tmp_path: Path) -> None:
        """A valid COG without overviews is re-encoded in place WITH overviews."""
        from portolan_cli.convert import ConversionStatus, convert_file
        from portolan_cli.formats import is_cloud_optimized_geotiff

        cog = _make_cog_without_overviews(tmp_path / "tile.tif")
        # Precondition: it is already a valid COG (so --fix would normally skip it)
        # and it has no overviews.
        assert is_cloud_optimized_geotiff(cog) is True
        assert _overview_count(cog) == 0

        result = convert_file(cog, force=True)

        assert result.status == ConversionStatus.SUCCESS
        assert result.output is not None
        # In-place re-encode (ADR-0020: rasters convert in place).
        assert result.output.resolve() == cog.resolve()
        assert _overview_count(cog) >= 1

    @pytest.mark.integration
    def test_force_false_still_skips_valid_cog(self, tmp_path: Path) -> None:
        """Without force, a valid COG is still SKIPPED (unchanged behavior)."""
        from portolan_cli.convert import ConversionStatus, convert_file

        cog = _make_cog_without_overviews(tmp_path / "tile.tif")

        result = convert_file(cog, force=False)

        assert result.status == ConversionStatus.SKIPPED
        assert result.output is None
        assert _overview_count(cog) == 0

    @pytest.mark.integration
    def test_force_does_not_touch_geoparquet(self, valid_points_parquet: Path) -> None:
        """force is raster-scoped: a valid GeoParquet stays SKIPPED."""
        from portolan_cli.convert import ConversionStatus, convert_file

        result = convert_file(valid_points_parquet, force=True)

        assert result.status == ConversionStatus.SKIPPED
        assert result.output is None
        assert result.format_from == "GeoParquet"


class TestConvertDirectoryParallel:
    """convert_directory(workers=N) parallelizes via ProcessPoolExecutor."""

    @pytest.mark.integration
    def test_parallel_converts_all_files_with_progress(self, tmp_path: Path) -> None:
        """workers>1 converts every file and fires on_progress once per file."""
        from portolan_cli.convert import (
            ConversionResult,
            ConversionStatus,
            convert_directory,
        )

        input_dir = tmp_path / "tiles"
        input_dir.mkdir()
        cogs = [_make_cog_without_overviews(input_dir / f"tile_{i}.tif") for i in range(4)]

        seen: list[ConversionResult] = []

        def on_progress(result: ConversionResult) -> None:
            seen.append(result)

        report = convert_directory(
            input_dir,
            file_paths=cogs,
            on_progress=on_progress,
            workers=4,
            force=True,
        )

        assert report.total == 4
        assert all(r.status == ConversionStatus.SUCCESS for r in report.results)
        # Callback fired exactly once per file (in the parent process).
        assert len(seen) == 4
        assert {r.source.name for r in seen} == {f"tile_{i}.tif" for i in range(4)}
        # Every COG now has overviews.
        assert all(_overview_count(c) >= 1 for c in cogs)

    @pytest.mark.integration
    def test_parallel_matches_serial_result_set(self, tmp_path: Path) -> None:
        """Parallel and serial runs produce the same set of outcomes."""
        from portolan_cli.convert import convert_directory

        def build(dir_name: str) -> tuple[Path, list[Path]]:
            d = tmp_path / dir_name
            d.mkdir()
            files = [_make_cog_without_overviews(d / f"t_{i}.tif") for i in range(3)]
            return d, files

        serial_dir, serial_files = build("serial")
        parallel_dir, parallel_files = build("parallel")

        serial = convert_directory(serial_dir, file_paths=serial_files, workers=1, force=True)
        parallel = convert_directory(parallel_dir, file_paths=parallel_files, workers=3, force=True)

        assert parallel.total == serial.total == 3
        serial_statuses = sorted(r.status.value for r in serial.results)
        parallel_statuses = sorted(r.status.value for r in parallel.results)
        assert parallel_statuses == serial_statuses

    @pytest.mark.integration
    def test_parallel_one_bad_file_does_not_abort_batch(self, tmp_path: Path) -> None:
        """A failing file yields a FAILED result; the rest still convert."""
        from portolan_cli.convert import ConversionStatus, convert_directory

        input_dir = tmp_path / "tiles"
        input_dir.mkdir()
        good = [_make_cog_without_overviews(input_dir / f"good_{i}.tif") for i in range(2)]
        # A .tif that is not a real raster -> conversion fails for this one only.
        bad = input_dir / "bad.tif"
        bad.write_bytes(b"not a real geotiff")

        report = convert_directory(
            input_dir,
            file_paths=[*good, bad],
            workers=3,
            force=True,
        )

        assert report.total == 3
        statuses = {r.source.name: r.status for r in report.results}
        assert statuses["bad.tif"] == ConversionStatus.FAILED
        assert statuses["good_0.tif"] == ConversionStatus.SUCCESS
        assert statuses["good_1.tif"] == ConversionStatus.SUCCESS


class TestCheckDirectoryForce:
    """check_directory(force=, workers=) re-optimizes valid COGs."""

    @pytest.mark.integration
    def test_force_fix_reoptimizes_cog_leaves_geoparquet(
        self, tmp_path: Path, valid_points_parquet: Path
    ) -> None:
        """--fix --force re-encodes valid COGs but leaves valid GeoParquet alone."""
        from portolan_cli.check import check_directory

        cog = _make_cog_without_overviews(tmp_path / "tile.tif")
        gpq = tmp_path / "vector.parquet"
        shutil.copy(valid_points_parquet, gpq)
        assert _overview_count(cog) == 0

        report = check_directory(tmp_path, fix=True, force=True, workers=2)

        assert report.conversion_report is not None
        results = {r.source.name: r for r in report.conversion_report.results}
        # COG was re-optimized.
        assert results["tile.tif"].status.value == "success"
        assert _overview_count(cog) >= 1
        # GeoParquet was never re-processed (not in the convert set).
        assert "vector.parquet" not in results

    @pytest.mark.integration
    def test_force_requires_fix(self, tmp_path: Path) -> None:
        """force without fix is a programmer error (mirrors remove_legacy guard)."""
        from portolan_cli.check import check_directory

        with pytest.raises(ValueError):
            check_directory(tmp_path, fix=False, force=True)

    @pytest.mark.integration
    def test_force_dry_run_previews_cog_reoptimization(self, tmp_path: Path) -> None:
        """--fix --force --dry-run lists the valid COG as a would-re-optimize COG."""
        from portolan_cli.check import check_directory

        cog = _make_cog_without_overviews(tmp_path / "tile.tif")

        report = check_directory(tmp_path, fix=True, force=True, dry_run=True)

        assert report.conversion_report is not None
        previews = {r.source.name: r for r in report.conversion_report.results}
        assert "tile.tif" in previews
        assert previews["tile.tif"].format_to == "COG"
        # Dry run must not modify the file.
        assert _overview_count(cog) == 0
