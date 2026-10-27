"""Regression tests for the COG writer's source metadata and overview handling."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import numpy as np
import pytest
import rasterio
from rasterio.enums import Resampling
from rasterio.transform import from_origin

from portolan_cli import trace
from portolan_cli.conversion_config import CogSettings
from portolan_cli.convert import validate_cog, write_cog

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.integration


@pytest.mark.parametrize(("compression", "expected_opens"), [("DEFLATE", 4), ("PACKBITS", 7)])
@pytest.mark.parametrize("tile_size", [128, 512])
@pytest.mark.parametrize("masked", [False, True])
def test_gdal_reads_are_counted(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
    compression: str,
    expected_opens: int,
    tile_size: int,
    masked: bool,
) -> None:
    """The VRT and the source it opens must appear in the read counter."""
    source = _source(tmp_path / "src" / "scene.tif")
    if masked:
        with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), rasterio.open(source, "r+") as src:
            mask = np.full((512, 512), 255, dtype="uint8")
            mask[:3] = 0
            src.write_mask(mask)
    with (
        caplog.at_level(logging.DEBUG, logger="rasterio._env"),
        rasterio.Env(CPL_DEBUG=True),
        trace.tracing(),
    ):
        write_cog(source, tmp_path, CogSettings(compression=compression, tile_size=tile_size))
        counted = trace.counters()["raster.opens"]
    opened = sum("GDALOpen(" in r.getMessage() for r in caplog.records)
    assert counted == opened
    extra_overview_opens = (
        (4 if masked else 2) if compression == "DEFLATE" and tile_size == 128 else 0
    )
    assert opened == expected_opens + extra_overview_opens


def _source(path: Path, *, overviews: bool = False) -> Path:
    """Write a checkerboard whose nearest and average overviews differ."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pixels = np.tile(np.array([[0, 100], [100, 0]], dtype="uint16"), (256, 256))
    with rasterio.open(
        path, "w", driver="GTiff", width=512, height=512, count=1, dtype="uint16",
        crs="EPSG:32618", transform=from_origin(100, 200, 3, 4),
    ) as dst:  # fmt: skip
        dst.write(pixels, 1)
        if overviews:
            dst.build_overviews([2, 4], Resampling.nearest)
    return path


@pytest.mark.parametrize("compression", ["DEFLATE", "LZW", "ZSTD", "RAW", "PACKBITS"])
def test_requested_resampling_rebuilds_existing_overviews(tmp_path: Path, compression: str) -> None:
    """A requested average pyramid must not retain a source's nearest pixels."""
    source = _source(tmp_path / "src" / "scene.tif", overviews=True)
    before = source.read_bytes()

    output = write_cog(
        source, tmp_path, CogSettings(compression=compression, tile_size=128, resampling="average")
    )

    with rasterio.open(output) as cog:
        assert cog.overviews(1) == [2, 4]
        assert cog.tags()["OVERVIEW_RESAMPLING"] == "AVERAGE"
        overview = cog.read(1, out_shape=(256, 256))
        assert np.all(overview == 50), f"overview values: {np.unique(overview)}"
    assert validate_cog(output) is None
    assert source.read_bytes() == before


def test_statistics_recomputed_after_source_pixels_change(tmp_path: Path) -> None:
    """GDAL's cached statistics can outlive an external pixel edit."""
    source = _source(tmp_path / "src" / "scene.tif")
    with rasterio.open(source, "r+") as dst:
        dst.update_tags(
            1,
            STATISTICS_MINIMUM="0",
            STATISTICS_MAXIMUM="100",
            STATISTICS_MEAN="50",
            STATISTICS_STDDEV="50",
            STATISTICS_VALID_PERCENT="100",
        )
        dst.write(np.full((512, 512), 7, dtype="uint16"), 1)
    before = source.read_bytes()

    output = write_cog(source, tmp_path)

    with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(output) as cog:
        assert cog.tags(1)["STATISTICS_MEAN"] == "7"
        assert cog.tags(1)["STATISTICS_MINIMUM"] == "7"
        assert cog.tags(1)["STATISTICS_MAXIMUM"] == "7"
        assert cog.tags(1)["STATISTICS_STDDEV"] == "0"
    assert source.read_bytes() == before
    assert list(tmp_path.rglob("*.aux.xml")) == []


def test_statistics_rewrite_preserves_absent_overviews(tmp_path: Path) -> None:
    """Adding statistics must not add a pyramid to an otherwise unchanged COG."""
    from rasterio.shutil import copy as gdal_copy

    from portolan_cli.preparation import convert_raster

    source = _source(tmp_path / "src" / "scene.tif")
    cog = tmp_path / "scene.tif"
    gdal_copy(source, cog, driver="COG", blocksize=128, overviews="NONE")
    with rasterio.open(cog) as src:
        assert src.overviews(1) == []
        assert "STATISTICS_MEAN" not in src.tags(1)

    convert_raster(cog, tmp_path)

    with rasterio.open(cog) as src:
        assert src.overviews(1) == []
        assert src.tags(1)["STATISTICS_MEAN"] == "50"


@pytest.mark.parametrize("dtype", ["int16", "uint16", "float64"])
@pytest.mark.parametrize("shape", [(1, 1), (7, 17), (271, 600)])
@pytest.mark.parametrize("compression", ["DEFLATE", "LZW", "ZSTD", "RAW", "PACKBITS"])
@pytest.mark.parametrize("in_place", [False, True])
def test_lossless_pixels_and_band_metadata_survive(
    tmp_path: Path, dtype: str, shape: tuple[int, int], compression: str, in_place: bool
) -> None:
    """Lossless conversion keeps independent band validity and publisher metadata."""
    height, width = shape
    pixels = (np.arange(3 * height * width).reshape(3, height, width) % 251 + 1).astype(dtype)
    pixels[0, : max(1, height // 2)] = 0
    source = tmp_path / "src" / "scene.tif"
    source.parent.mkdir()
    transform = from_origin(100, 200, 3, 4)
    with rasterio.open(
        source, "w", driver="GTiff", width=width, height=height, count=3, dtype=dtype,
        crs="EPSG:32618", transform=transform, nodata=0,
    ) as dst:  # fmt: skip
        dst.write(pixels)
        dst.scales = (0.1, 0.2, 0.3)
        dst.offsets = (1.0, 2.0, 3.0)
        dst.update_tags(publisher="example")
        for band in dst.indexes:
            dst.set_band_description(band, f"measurement {band}")
            dst.set_band_unit(band, "m")
            dst.update_tags(band, sensor=f"sensor {band}")
    before = source.read_bytes()
    output_dir = source.parent if in_place else tmp_path

    output = write_cog(source, output_dir, CogSettings(compression=compression))

    with rasterio.open(output) as cog:
        np.testing.assert_array_equal(cog.read(), pixels)
        assert cog.crs == rasterio.crs.CRS.from_epsg(32618)
        assert cog.transform == transform
        assert cog.nodata == 0
        assert cog.scales == (0.1, 0.2, 0.3)
        assert cog.offsets == (1.0, 2.0, 3.0)
        assert cog.descriptions == ("measurement 1", "measurement 2", "measurement 3")
        assert cog.units == ("m", "m", "m")
        assert cog.tags()["publisher"] == "example"
        for band in cog.indexes:
            assert cog.tags(band)["sensor"] == f"sensor {band}"
            valid = pixels[band - 1][pixels[band - 1] != 0]
            if valid.size:
                assert float(cog.tags(band)["STATISTICS_MEAN"]) == pytest.approx(valid.mean())
            else:
                assert cog.tags(band)["STATISTICS_VALID_PERCENT"] == "0"
    assert validate_cog(output) is None
    if not in_place:
        assert source.read_bytes() == before
    assert list(tmp_path.rglob("*.aux.xml")) == []


@pytest.mark.parametrize("compression", ["DEFLATE", "PACKBITS"])
@pytest.mark.parametrize("nan_nodata", [False, True])
def test_float_nan_and_internal_mask_survive(
    tmp_path: Path, nan_nodata: bool, compression: str
) -> None:
    """Both NaN nodata and an explicit internal mask survive the VRT copy."""
    source = tmp_path / "src" / "scene.tif"
    source.parent.mkdir()
    pixels = np.arange(7 * 17, dtype="float64").reshape(1, 7, 17)
    pixels[0, :2] = np.nan
    mask = np.full((7, 17), 255, dtype="uint8")
    mask[:2] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), rasterio.open(
        source, "w", driver="GTiff", width=17, height=7, count=1, dtype="float64",
        crs="EPSG:4326", transform=from_origin(0, 1, 0.01, 0.01),
        nodata=np.nan if nan_nodata else None,
    ) as dst:  # fmt: skip
        dst.write(pixels)
        if not nan_nodata:
            dst.write_mask(mask)
    before = source.read_bytes()

    output = write_cog(source, tmp_path, CogSettings(compression=compression))

    with rasterio.open(output) as cog:
        np.testing.assert_array_equal(cog.read(), pixels)
        np.testing.assert_array_equal(cog.read_masks(1), mask)
        assert float(cog.tags(1)["STATISTICS_MEAN"]) == pytest.approx(pixels[0, 2:].mean())
    assert validate_cog(output) is None
    assert source.read_bytes() == before


@pytest.mark.parametrize("height", [4096, 4097])
def test_statistics_precision_threshold(tmp_path: Path, height: int) -> None:
    """Exact statistics include every pixel through 4096 squared, then use sampling."""
    source = tmp_path / "src" / "scene.tif"
    source.parent.mkdir()
    pixels = np.full((height, 4096), 7, dtype="uint8")
    pixels[-1, -1] = 9
    with rasterio.open(
        source, "w", driver="GTiff", width=4096, height=height, count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 1, 0.00001, 0.00001),
        tiled=True, blockxsize=128, blockysize=128,
    ) as dst:  # fmt: skip
        dst.write(pixels, 1)

    output = write_cog(source, tmp_path)

    with rasterio.open(output) as cog:
        tags = cog.tags(1)
        if height == 4096:
            assert tags.get("STATISTICS_APPROXIMATE") != "YES"
            assert tags["STATISTICS_MAXIMUM"] == "9"
        else:
            assert tags["STATISTICS_APPROXIMATE"] == "YES"
    assert list(tmp_path.rglob("*.aux.xml")) == []


@pytest.mark.parametrize("separate_output", [False, True])
def test_cog_with_statistics_is_kept_byte_for_byte(tmp_path: Path, separate_output: bool) -> None:
    """An already conformant COG is retained or copied without another rewrite."""
    from portolan_cli.preparation import convert_raster

    source = _source(tmp_path / "src" / "scene.tif")
    cog = write_cog(source, source.parent)
    before = cog.read_bytes()
    output_dir = tmp_path if separate_output else cog.parent

    output = convert_raster(cog, output_dir)

    assert cog.read_bytes() == before
    assert output.read_bytes() == before


@pytest.mark.parametrize("compression", ["DEFLATE", "PACKBITS"])
def test_palette_survives_conversion(tmp_path: Path, compression: str) -> None:
    """A categorical raster retains both its indices and its color table."""
    from rasterio.enums import ColorInterp

    source = tmp_path / "src" / "scene.tif"
    source.parent.mkdir()
    pixels = (np.arange(7 * 17).reshape(7, 17) % 3).astype("uint8")
    with rasterio.open(
        source, "w", driver="GTiff", width=17, height=7, count=1, dtype="uint8",
        crs="EPSG:4326", transform=from_origin(0, 1, 0.01, 0.01),
    ) as dst:  # fmt: skip
        dst.write(pixels, 1)
        dst.write_colormap(1, {0: (0, 0, 0, 255), 1: (200, 100, 0, 255), 2: (0, 100, 200, 255)})
    with rasterio.open(source) as src:
        expected_palette = src.colormap(1)
    before = source.read_bytes()

    output = write_cog(source, tmp_path, CogSettings(compression=compression))

    with rasterio.open(output) as cog:
        assert cog.colorinterp == (ColorInterp.palette,)
        assert cog.colormap(1) == expected_palette
        np.testing.assert_array_equal(cog.read(1), pixels)
    assert source.read_bytes() == before


def test_packbits_tiling_overviews_and_mask(tmp_path: Path) -> None:
    """The PACKBITS fallback builds a valid COG pyramid without changing masked pixels."""
    source = tmp_path / "src" / "scene.tif"
    source.parent.mkdir()
    pixels = (np.arange(3 * 271 * 600).reshape(3, 271, 600) % 251 + 1).astype("uint16")
    pixels[0, :3] = 0
    mask = np.full((271, 600), 255, dtype="uint8")
    mask[:3] = 0
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), rasterio.open(
        source, "w", driver="GTiff", width=600, height=271, count=3, dtype="uint16",
        crs="EPSG:32618", transform=from_origin(100, 200, 3, 4), nodata=0,
    ) as dst:  # fmt: skip
        dst.write(pixels)
        dst.write_mask(mask)
    before = source.read_bytes()

    output = write_cog(source, tmp_path, CogSettings(compression="PACKBITS", tile_size=128))

    with rasterio.open(output) as cog:
        np.testing.assert_array_equal(cog.read(), pixels)
        for band in cog.indexes:
            np.testing.assert_array_equal(cog.read_masks(band), mask)
            assert cog.overviews(band) == [2, 4]
        assert cog.block_shapes == [(128, 128)] * 3
        assert cog.tags(ns="IMAGE_STRUCTURE")["COMPRESSION"] == "PACKBITS"
    assert validate_cog(output) is None
    assert source.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["scene.tif", "src"]


@pytest.mark.parametrize("fail_at", ["tiles", "final"])
def test_packbits_failed_copy_keeps_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fail_at: str
) -> None:
    """A failed intermediate or final copy leaves the in-place source and no scratch files."""
    from typing import Any

    import rasterio.shutil

    source = _source(tmp_path / "scene.tif")
    before = source.read_bytes()
    real_copy = rasterio.shutil.copy

    def fail_copy(src: Any, dst: Any, **kwargs: Any) -> None:
        real_copy(src, dst, **kwargs)
        if kwargs.get("driver") == "GTiff" and (
            bool(kwargs.get("copy_src_overviews")) == (fail_at == "final")
        ):
            raise OSError("injected copy failure")

    monkeypatch.setattr(rasterio.shutil, "copy", fail_copy)

    with pytest.raises(OSError, match="injected copy failure"):
        write_cog(source, tmp_path, CogSettings(compression="PACKBITS"))

    assert source.read_bytes() == before
    assert sorted(p.name for p in tmp_path.iterdir()) == ["scene.tif"]
