"""Tests for the ArcGIS ImageServer tile cache reader.

A hosted tiled imagery layer reports ``capabilities: "Image,TilesOnly"``. It
rejects ``exportImage`` with HTTP 400 and serves only the pre-rendered cache at
``/tile/{level}/{row}/{col}`` (issue #870). These tests cover the cache reader.

TDD: These tests are written FIRST, before implementation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import numpy as np
import pytest
import rasterio

from portolan_cli.extract.arcgis.imageserver.tilecache import (
    CacheTileRef,
    LevelOfDetail,
    TileCacheError,
    TileCacheInfo,
    cache_tile_refs,
    compute_cache_tile_grid,
    decode_tile_bytes,
    export_image_supported,
    fetch_cache_tile,
    parse_tile_info,
    probe_block_empty,
    select_probe_lod,
)
from portolan_cli.extract.arcgis.imageserver.tiling import TileSpec

pytestmark = pytest.mark.unit

FIXTURE_DIR = Path(__file__).parents[4] / "fixtures" / "imageserver" / "tilecache"
LERC_TILE = FIXTURE_DIR / "lerc2d_level0_0_0.bin"
LERC_EMPTY = FIXTURE_DIR / "lerc2d_empty.bin"

ORIGIN_X = -12060495.1357351
ORIGIN_Y = 5110175.25118694
SERVICE_URL = "https://tiledimageservices.arcgis.com/QVEN/arcgis/rest/services/x/ImageServer"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def tile_info() -> dict[str, Any]:
    """tileInfo block copied from a live hosted tiled imagery layer."""
    return {
        "rows": 256,
        "cols": 256,
        "dpi": 96,
        "format": "LERC2D",
        "origin": {"x": ORIGIN_X, "y": ORIGIN_Y},
        "spatialReference": {"wkid": 102100, "latestWkid": 3857},
        "lods": [
            {"level": level, "resolution": 15360.0 / (2**level), "scale": 1.0}
            for level in range(10)
        ],
    }


@pytest.fixture
def cache(tile_info: dict[str, Any]) -> TileCacheInfo:
    """Parsed tile cache info for the live service."""
    parsed = parse_tile_info({"tileInfo": tile_info})
    assert parsed is not None
    return parsed


@pytest.fixture
def lod_native() -> LevelOfDetail:
    """Level 9, which matches the 30 m native pixel size."""
    return LevelOfDetail(level=9, resolution=30.0)


def _client(handler: Any) -> httpx.AsyncClient:
    """Build an AsyncClient backed by a mock transport."""
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# =============================================================================
# parse_tile_info
# =============================================================================


class TestParseTileInfo:
    """Reading the tileInfo block from an ImageServer response."""

    def test_returns_none_when_service_has_no_cache(self) -> None:
        assert parse_tile_info({"name": "x", "bandCount": 1}) is None

    def test_returns_none_when_lods_are_missing(self, tile_info: dict[str, Any]) -> None:
        tile_info["lods"] = []
        assert parse_tile_info({"tileInfo": tile_info}) is None

    def test_returns_none_when_origin_is_missing(self, tile_info: dict[str, Any]) -> None:
        del tile_info["origin"]
        assert parse_tile_info({"tileInfo": tile_info}) is None

    def test_reads_grid_origin_and_format(self, cache: TileCacheInfo) -> None:
        assert cache.tile_width == 256
        assert cache.tile_height == 256
        assert cache.tile_format == "LERC2D"
        assert cache.origin_x == ORIGIN_X
        assert cache.origin_y == ORIGIN_Y

    def test_reads_every_level_of_detail(self, cache: TileCacheInfo) -> None:
        assert len(cache.lods) == 10
        assert cache.lods[0] == LevelOfDetail(level=0, resolution=15360.0)
        assert cache.lods[9] == LevelOfDetail(level=9, resolution=30.0)

    def test_uppercases_the_format(self, tile_info: dict[str, Any]) -> None:
        tile_info["format"] = "lerc2d"
        parsed = parse_tile_info({"tileInfo": tile_info})
        assert parsed is not None
        assert parsed.tile_format == "LERC2D"

    def test_reads_the_cache_crs(self, cache: TileCacheInfo) -> None:
        assert cache.crs_string() == "EPSG:3857"

    def test_falls_back_to_the_service_crs(self, tile_info: dict[str, Any]) -> None:
        del tile_info["spatialReference"]
        parsed = parse_tile_info(
            {"tileInfo": tile_info, "spatialReference": {"wkid": 26915}},
        )
        assert parsed is not None
        assert parsed.crs_string() == "EPSG:26915"


# =============================================================================
# export_image_supported
# =============================================================================


class TestExportImageSupported:
    """TilesOnly services reject exportImage, so discovery must detect them."""

    def test_false_when_capabilities_say_tiles_only(self) -> None:
        assert export_image_supported(["Image", "TilesOnly"]) is False

    def test_detection_ignores_case(self) -> None:
        assert export_image_supported(["image", "tilesonly"]) is False

    def test_true_for_a_normal_image_service(self) -> None:
        assert export_image_supported(["Image", "Metadata", "Catalog"]) is True

    def test_true_when_capabilities_are_empty(self) -> None:
        assert export_image_supported([]) is True


# =============================================================================
# Level selection
# =============================================================================


class TestSelectLod:
    """The reader must read the level that matches the native pixel size."""

    def test_picks_the_level_that_matches_the_native_pixel_size(self, cache: TileCacheInfo) -> None:
        assert cache.select_lod(30.0).level == 9

    def test_picks_a_coarser_level_for_a_coarser_pixel_size(self, cache: TileCacheInfo) -> None:
        assert cache.select_lod(240.0).level == 6

    def test_picks_the_finest_level_when_the_cache_stops_short(self, cache: TileCacheInfo) -> None:
        assert cache.select_lod(1.0).level == 9

    def test_picks_the_finest_level_when_the_pixel_size_is_unknown(
        self, cache: TileCacheInfo
    ) -> None:
        assert cache.select_lod(0.0).level == 9

    def test_raises_when_the_cache_has_no_levels(self) -> None:
        empty = TileCacheInfo(
            tile_width=256,
            tile_height=256,
            tile_format="LERC2D",
            origin_x=0.0,
            origin_y=0.0,
            lods=(),
            spatial_reference={"wkid": 3857},
        )
        with pytest.raises(TileCacheError, match="no levels"):
            empty.select_lod(30.0)


# =============================================================================
# Grid computation
# =============================================================================


class TestComputeCacheTileGrid:
    """Output tiles must align with the cache grid."""

    def test_groups_cache_tiles_into_blocks_of_the_requested_size(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # 512 px blocks over a 1024 x 512 px extent: 2 columns, 1 row.
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 512 * 30.0,
            "xmax": ORIGIN_X + 1024 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))
        assert [(t.x, t.y) for t in tiles] == [(0, 0), (1, 0)]
        assert [(t.width_px, t.height_px) for t in tiles] == [(512, 512), (512, 512)]

    def test_first_tile_starts_at_the_cache_origin(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 512 * 30.0,
            "xmax": ORIGIN_X + 512 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))
        assert tiles[0].bbox == pytest.approx(
            (ORIGIN_X, ORIGIN_Y - 512 * 30.0, ORIGIN_X + 512 * 30.0, ORIGIN_Y)
        )

    def test_clips_the_last_tile_to_the_extent(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # 600 px wide extent with 512 px blocks: the second column is 88 px.
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 512 * 30.0,
            "xmax": ORIGIN_X + 600 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))
        assert [t.width_px for t in tiles] == [512, 88]

    def test_snaps_a_misaligned_extent_back_to_the_cache_grid(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # Start 100 px in. The first tile is 412 px, so the next one starts on a
        # 512 px boundary and every request covers whole cache tiles.
        extent = {
            "xmin": ORIGIN_X + 100 * 30.0,
            "ymin": ORIGIN_Y - 512 * 30.0,
            "xmax": ORIGIN_X + 1024 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))
        assert [t.width_px for t in tiles] == [412, 512]
        assert tiles[1].bbox[0] == pytest.approx(ORIGIN_X + 512 * 30.0)

    def test_float_noise_in_the_extent_makes_no_sliver_row(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # A live service reports an extent whose ymax sits 1.86e-09 above the
        # cache origin. Without a snap the grid starts one pixel high, which
        # adds a row of 1 pixel tall tiles and shifts every block index by one.
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 1024 * 30.0,
            "xmax": ORIGIN_X + 1024 * 30.0,
            "ymax": ORIGIN_Y + 1.862645149230957e-09,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))

        assert {t.height_px for t in tiles} == {512}
        assert {t.y for t in tiles} == {0, 1}
        assert tiles[0].bbox[3] == pytest.approx(ORIGIN_Y)

    def test_float_noise_on_the_left_edge_makes_no_sliver_column(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        extent = {
            "xmin": ORIGIN_X - 1.862645149230957e-09,
            "ymin": ORIGIN_Y - 512 * 30.0,
            "xmax": ORIGIN_X + 1024 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))

        assert {t.width_px for t in tiles} == {512}
        assert {t.x for t in tiles} == {0, 1}

    def test_a_real_partial_tile_still_survives_the_snap(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # The snap must not swallow a genuine partial pixel row. A 600 px
        # extent still yields a 512 px tile and an 88 px tile.
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 600 * 30.0,
            "xmax": ORIGIN_X + 512 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=512))

        assert [t.height_px for t in tiles] == [512, 88]

    def test_tile_size_below_one_cache_tile_still_reads_one_cache_tile(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        extent = {
            "xmin": ORIGIN_X,
            "ymin": ORIGIN_Y - 256 * 30.0,
            "xmax": ORIGIN_X + 256 * 30.0,
            "ymax": ORIGIN_Y,
        }
        tiles = list(compute_cache_tile_grid(extent, cache, lod_native, tile_size=64))
        assert len(tiles) == 1
        assert (tiles[0].width_px, tiles[0].height_px) == (256, 256)


# =============================================================================
# Cache tile references
# =============================================================================


class TestCacheTileRefs:
    """Each output tile maps onto a rectangle of cache tiles."""

    def test_maps_a_block_onto_four_cache_tiles(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        spec = TileSpec(
            x=0,
            y=0,
            bbox=(ORIGIN_X, ORIGIN_Y - 512 * 30.0, ORIGIN_X + 512 * 30.0, ORIGIN_Y),
            width_px=512,
            height_px=512,
        )
        refs = cache_tile_refs(spec, cache, lod_native)
        assert [(r.row, r.col) for r in refs] == [(0, 0), (0, 1), (1, 0), (1, 1)]
        assert [(r.dst_row, r.dst_col) for r in refs] == [
            (0, 0),
            (0, 256),
            (256, 0),
            (256, 256),
        ]

    def test_reports_the_selected_level(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        spec = TileSpec(
            x=0,
            y=0,
            bbox=(ORIGIN_X, ORIGIN_Y - 256 * 30.0, ORIGIN_X + 256 * 30.0, ORIGIN_Y),
            width_px=256,
            height_px=256,
        )
        assert [r.level for r in cache_tile_refs(spec, cache, lod_native)] == [9]

    def test_offsets_are_negative_when_the_tile_starts_mid_cache_tile(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # Start 100 px into cache tile (0, 0). The cache tile lands 100 px above
        # and left of the output tile, so both offsets are -100.
        spec = TileSpec(
            x=0,
            y=0,
            bbox=(
                ORIGIN_X + 100 * 30.0,
                ORIGIN_Y - 200 * 30.0,
                ORIGIN_X + 200 * 30.0,
                ORIGIN_Y - 100 * 30.0,
            ),
            width_px=100,
            height_px=100,
        )
        refs = cache_tile_refs(spec, cache, lod_native)
        assert [(r.row, r.col, r.dst_row, r.dst_col) for r in refs] == [(0, 0, -100, -100)]

    def test_skips_cache_tiles_left_of_the_cache_origin(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        spec = TileSpec(
            x=0,
            y=0,
            bbox=(ORIGIN_X - 256 * 30.0, ORIGIN_Y - 256 * 30.0, ORIGIN_X, ORIGIN_Y),
            width_px=256,
            height_px=256,
        )
        assert cache_tile_refs(spec, cache, lod_native) == []


# =============================================================================
# Decoding
# =============================================================================


class TestDecodeTileBytes:
    """LERC decoding, which GDAL cannot do for LERC2 version 6."""

    def test_decodes_a_lerc2d_tile(self) -> None:
        data, mask = decode_tile_bytes(LERC_TILE.read_bytes(), "LERC2D")
        assert data.shape == (1, 256, 256)
        assert data.dtype == np.uint8
        assert mask.shape == (256, 256)
        assert int(mask.sum()) == 2851
        assert sorted(np.unique(data[0][mask]).tolist()) == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    def test_reports_an_empty_tile_as_fully_masked(self) -> None:
        _, mask = decode_tile_bytes(LERC_EMPTY.read_bytes(), "LERC2D")
        assert not mask.any()

    def test_decodes_a_png_tile(self) -> None:
        png = _png_bytes()
        data, mask = decode_tile_bytes(png, "PNG")
        assert data.shape == (1, 4, 4)
        assert data[0, 0, 0] == 7
        assert mask.all()

    def test_rejects_an_unknown_format(self) -> None:
        with pytest.raises(TileCacheError, match="PDF"):
            decode_tile_bytes(b"whatever", "PDF")

    def test_reports_the_format_when_a_lerc_blob_is_corrupt(self) -> None:
        with pytest.raises(TileCacheError, match="LERC"):
            decode_tile_bytes(b"Lerc2 " + b"\x00" * 40, "LERC2D")


def _png_bytes() -> bytes:
    """Build a 4x4 single-band PNG in memory."""
    from rasterio.io import MemoryFile

    with MemoryFile() as memfile:
        with memfile.open(driver="PNG", width=4, height=4, count=1, dtype="uint8") as dataset:
            dataset.write(np.full((1, 4, 4), 7, dtype=np.uint8))
        return bytes(memfile.read())


# =============================================================================
# Fetching
# =============================================================================


class TestFetchCacheTile:
    """Downloading, mosaicking, and writing a georeferenced tile."""

    @pytest.fixture
    def spec(self) -> TileSpec:
        return TileSpec(
            x=0,
            y=0,
            bbox=(ORIGIN_X, ORIGIN_Y - 512 * 30.0, ORIGIN_X + 512 * 30.0, ORIGIN_Y),
            width_px=512,
            height_px=512,
        )

    @pytest.mark.asyncio
    async def test_writes_a_georeferenced_mosaic(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        body = LERC_TILE.read_bytes()

        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=body)

        out = tmp_path / "tile.tif"
        async with _client(handler) as client:
            result = await fetch_cache_tile(
                SERVICE_URL, spec, out, client, cache, lod_native, "EPSG:3857"
            )

        assert result.empty is False
        assert result.bytes_downloaded == len(body) * 4
        with rasterio.open(out) as src:
            assert (src.width, src.height) == (512, 512)
            assert src.crs.to_string() == "EPSG:3857"
            assert src.transform.c == pytest.approx(ORIGIN_X)
            assert src.transform.f == pytest.approx(ORIGIN_Y)
            assert src.transform.a == pytest.approx(30.0)
            assert src.dtypes == ("uint8",)
            # Each of the four cache tiles carries 2851 valid pixels.
            assert int((src.dataset_mask() > 0).sum()) == 2851 * 4

    @pytest.mark.asyncio
    async def test_requests_the_cache_endpoint(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(200, content=LERC_TILE.read_bytes())

        async with _client(handler) as client:
            await fetch_cache_tile(
                SERVICE_URL, spec, tmp_path / "t.tif", client, cache, lod_native, "EPSG:3857"
            )

        assert sorted(seen) == [
            "/QVEN/arcgis/rest/services/x/ImageServer/tile/9/0/0",
            "/QVEN/arcgis/rest/services/x/ImageServer/tile/9/0/1",
            "/QVEN/arcgis/rest/services/x/ImageServer/tile/9/1/0",
            "/QVEN/arcgis/rest/services/x/ImageServer/tile/9/1/1",
        ]

    @pytest.mark.asyncio
    async def test_reports_empty_when_every_cache_tile_is_absent(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="<html>not found</html>")

        out = tmp_path / "tile.tif"
        async with _client(handler) as client:
            result = await fetch_cache_tile(
                SERVICE_URL, spec, out, client, cache, lod_native, "EPSG:3857"
            )

        assert result.empty is True
        assert not out.exists()

    @pytest.mark.asyncio
    async def test_reports_empty_when_every_pixel_is_masked(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=LERC_EMPTY.read_bytes())

        out = tmp_path / "tile.tif"
        async with _client(handler) as client:
            result = await fetch_cache_tile(
                SERVICE_URL, spec, out, client, cache, lod_native, "EPSG:3857"
            )

        assert result.empty is True
        assert not out.exists()

    @pytest.mark.asyncio
    async def test_writes_the_tile_when_only_one_cache_tile_is_present(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            if request.url.path.endswith("/tile/9/0/0"):
                return httpx.Response(200, content=LERC_TILE.read_bytes())
            return httpx.Response(404, text="missing")

        out = tmp_path / "tile.tif"
        async with _client(handler) as client:
            result = await fetch_cache_tile(
                SERVICE_URL, spec, out, client, cache, lod_native, "EPSG:3857"
            )

        assert result.empty is False
        with rasterio.open(out) as src:
            assert int((src.dataset_mask() > 0).sum()) == 2851

    @pytest.mark.asyncio
    async def test_raises_on_a_server_error(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        async with _client(handler) as client:
            with pytest.raises(TileCacheError, match="HTTP 500"):
                await fetch_cache_tile(
                    SERVICE_URL,
                    spec,
                    tmp_path / "t.tif",
                    client,
                    cache,
                    lod_native,
                    "EPSG:3857",
                )

    @pytest.mark.asyncio
    async def test_error_names_the_failed_cache_tile(
        self,
        spec: TileSpec,
        cache: TileCacheInfo,
        lod_native: LevelOfDetail,
        tmp_path: Path,
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(403, text="token required")

        async with _client(handler) as client:
            with pytest.raises(TileCacheError, match=r"tile/9/"):
                await fetch_cache_tile(
                    SERVICE_URL,
                    spec,
                    tmp_path / "t.tif",
                    client,
                    cache,
                    lod_native,
                    "EPSG:3857",
                )


class TestCacheTileRefUrl:
    """The cache endpoint URL."""

    def test_builds_the_tile_url(self) -> None:
        ref = CacheTileRef(level=9, row=3, col=4, dst_row=0, dst_col=0)
        assert ref.url("https://host/x/ImageServer/") == "https://host/x/ImageServer/tile/9/3/4"


# =============================================================================
# Coarse scan (issue #870)
# =============================================================================


class TestSelectProbeLod:
    """Finding a coarse level where one cache tile covers a whole output block."""

    def test_finds_the_level_where_one_tile_covers_the_block(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        # A 4096 px block at 30 m spans 122,880 m. At level 5 the resolution is
        # 480 m, so 256 px covers exactly that span.
        probe = select_probe_lod(cache, lod_native, 4096)
        assert probe is not None
        assert probe.level == 5

    def test_returns_none_when_the_block_is_one_cache_tile(
        self, cache: TileCacheInfo, lod_native: LevelOfDetail
    ) -> None:
        assert select_probe_lod(cache, lod_native, 256) is None

    def test_returns_none_when_the_cache_has_no_coarser_level(
        self, tile_info: dict[str, Any]
    ) -> None:
        tile_info["lods"] = [{"level": 9, "resolution": 30.0}]
        single = parse_tile_info({"tileInfo": tile_info})
        assert single is not None
        assert select_probe_lod(single, LevelOfDetail(9, 30.0), 4096) is None


class TestProbeBlockEmpty:
    """The coarse probe must never drop data when it cannot tell."""

    @pytest.fixture
    def block(self) -> TileSpec:
        return TileSpec(
            x=0,
            y=0,
            bbox=(ORIGIN_X, ORIGIN_Y - 4096 * 30.0, ORIGIN_X + 4096 * 30.0, ORIGIN_Y),
            width_px=4096,
            height_px=4096,
        )

    @pytest.mark.asyncio
    async def test_reports_empty_when_the_coarse_tile_has_no_valid_pixel(
        self, block: TileSpec, cache: TileCacheInfo
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=LERC_EMPTY.read_bytes())

        async with _client(handler) as client:
            assert await probe_block_empty(
                SERVICE_URL, block, client, cache, LevelOfDetail(5, 480.0)
            )

    @pytest.mark.asyncio
    async def test_reports_not_empty_when_the_coarse_tile_holds_data(
        self, block: TileSpec, cache: TileCacheInfo
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, content=LERC_TILE.read_bytes())

        async with _client(handler) as client:
            assert not await probe_block_empty(
                SERVICE_URL, block, client, cache, LevelOfDetail(5, 480.0)
            )

    @pytest.mark.asyncio
    async def test_reports_empty_when_the_coarse_tile_is_absent(
        self, block: TileSpec, cache: TileCacheInfo
    ) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, text="missing")

        async with _client(handler) as client:
            assert await probe_block_empty(
                SERVICE_URL, block, client, cache, LevelOfDetail(5, 480.0)
            )

    @pytest.mark.asyncio
    async def test_reads_the_block_when_the_probe_fails(
        self, block: TileSpec, cache: TileCacheInfo
    ) -> None:
        # A failed probe must never skip a block. The reader falls back to a
        # full read, which costs time but never drops data.
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, text="boom")

        async with _client(handler) as client:
            assert not await probe_block_empty(
                SERVICE_URL, block, client, cache, LevelOfDetail(5, 480.0)
            )

    @pytest.mark.asyncio
    async def test_requests_one_coarse_tile_for_the_whole_block(
        self, block: TileSpec, cache: TileCacheInfo
    ) -> None:
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(200, content=LERC_EMPTY.read_bytes())

        async with _client(handler) as client:
            await probe_block_empty(SERVICE_URL, block, client, cache, LevelOfDetail(5, 480.0))

        assert seen == ["/QVEN/arcgis/rest/services/x/ImageServer/tile/5/0/0"]


class TestLercLoadFailure:
    """The advice must match the failure (issue #870)."""

    def test_an_absent_package_says_to_install_it(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import builtins

        real_import = builtins.__import__

        def fake(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "lerc":
                raise ImportError("No module named 'lerc'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake)
        with pytest.raises(TileCacheError, match="pip install lerc"):
            decode_tile_bytes(LERC_TILE.read_bytes(), "LERC2D")

    def test_an_unloadable_binary_does_not_say_to_install_it(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # On Linux aarch64 the package is installed and its binary is absent.
        # Telling the user to install it again would waste their time.
        import builtins

        real_import = builtins.__import__

        def fake(name: str, *args: Any, **kwargs: Any) -> Any:
            if name == "lerc":
                raise OSError("libLerc.so.4: cannot open shared object file")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake)
        with pytest.raises(TileCacheError) as caught:
            decode_tile_bytes(LERC_TILE.read_bytes(), "LERC2D")

        message = str(caught.value)
        assert "pip install lerc" not in message
        assert "The package is installed" in message
        assert "Linux x86-64" in message
