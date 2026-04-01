"""Tests for ImageServer tile grid calculation.

These tests cover:
- Basic tile grid computation
- Edge cases: tiny extents, uneven divisions, negative coordinates
- Tile count calculation
- TileSpec properties
"""

from __future__ import annotations

import pytest

from portolan_cli.extract.arcgis.imageserver.tiling import (
    TileSpec,
    compute_tile_grid,
    tile_count,
)


class TestTileSpec:
    """Tests for TileSpec dataclass."""

    @pytest.mark.unit
    def test_get_id_format(self) -> None:
        """TileSpec.get_id() returns expected format."""
        tile = TileSpec(x=3, y=7, bbox=(0, 0, 100, 100), width_px=256, height_px=256)
        assert tile.get_id() == "tile_3_7"

    @pytest.mark.unit
    def test_get_id_zero_indices(self) -> None:
        """TileSpec.get_id() handles zero indices."""
        tile = TileSpec(x=0, y=0, bbox=(0, 0, 100, 100), width_px=256, height_px=256)
        assert tile.get_id() == "tile_0_0"

    @pytest.mark.unit
    def test_frozen_dataclass(self) -> None:
        """TileSpec is immutable (frozen)."""
        tile = TileSpec(x=0, y=0, bbox=(0, 0, 100, 100), width_px=256, height_px=256)
        with pytest.raises(AttributeError):
            tile.x = 5  # type: ignore[misc]

    @pytest.mark.unit
    def test_hashable(self) -> None:
        """TileSpec is hashable (can be used in sets/dicts)."""
        tile1 = TileSpec(x=0, y=0, bbox=(0, 0, 100, 100), width_px=256, height_px=256)
        tile2 = TileSpec(x=0, y=1, bbox=(0, 100, 100, 200), width_px=256, height_px=256)

        tile_set = {tile1, tile2}
        assert len(tile_set) == 2


class TestTileCount:
    """Tests for tile_count function."""

    @pytest.mark.unit
    def test_single_tile(self) -> None:
        """Small extent produces single tile."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 100, "ymax": 100}
        # 100 map units / (10 pixels * 10 units/pixel) = 1 tile
        count = tile_count(extent, pixel_size_x=10, pixel_size_y=10, tile_size=10)
        assert count == 1

    @pytest.mark.unit
    def test_exact_division(self) -> None:
        """Extent that divides evenly into tiles."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 1000, "ymax": 500}
        # 1000 / (100 * 1) = 10 cols, 500 / (100 * 1) = 5 rows = 50 tiles
        count = tile_count(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100)
        assert count == 50

    @pytest.mark.unit
    def test_uneven_division(self) -> None:
        """Extent that doesn't divide evenly produces extra partial tiles."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 150, "ymax": 100}
        # 150 / 100 = 1.5 -> ceil = 2 cols, 100 / 100 = 1 row = 2 tiles
        count = tile_count(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100)
        assert count == 2

    @pytest.mark.unit
    def test_default_tile_size(self) -> None:
        """Default tile size is 4096."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 40960, "ymax": 40960}
        # 40960 / (4096 * 1) = 10 tiles per axis
        count = tile_count(extent, pixel_size_x=1, pixel_size_y=1)
        assert count == 100

    @pytest.mark.unit
    def test_tiny_extent(self) -> None:
        """Very small extent still produces at least one tile."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 0.001, "ymax": 0.001}
        count = tile_count(extent, pixel_size_x=1, pixel_size_y=1, tile_size=4096)
        assert count >= 1

    @pytest.mark.unit
    def test_negative_coordinates(self) -> None:
        """Extent with negative coordinates works correctly."""
        extent = {"xmin": -100, "ymin": -50, "xmax": 100, "ymax": 50}
        # 200 width, 100 height = 200 / 100 = 2 cols, 100 / 100 = 1 row
        count = tile_count(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100)
        assert count == 2


class TestComputeTileGrid:
    """Tests for compute_tile_grid function."""

    @pytest.mark.unit
    def test_single_tile_grid(self) -> None:
        """Small extent produces single tile with correct bbox."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 100, "ymax": 100}
        tiles = list(compute_tile_grid(extent, pixel_size_x=10, pixel_size_y=10, tile_size=10))

        assert len(tiles) == 1
        tile = tiles[0]
        assert tile.x == 0
        assert tile.y == 0
        assert tile.bbox == (0, 0, 100, 100)
        assert tile.width_px == 10
        assert tile.height_px == 10

    @pytest.mark.unit
    def test_grid_order_row_major(self) -> None:
        """Tiles are yielded in row-major order (top to bottom, left to right)."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 200, "ymax": 200}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        # 2x2 grid
        assert len(tiles) == 4

        # Row 0
        assert tiles[0].x == 0 and tiles[0].y == 0
        assert tiles[1].x == 1 and tiles[1].y == 0

        # Row 1
        assert tiles[2].x == 0 and tiles[2].y == 1
        assert tiles[3].x == 1 and tiles[3].y == 1

    @pytest.mark.unit
    def test_tile_bbox_values(self) -> None:
        """Tile bboxes cover the extent correctly."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 200, "ymax": 100}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        assert len(tiles) == 2

        # First tile: (0,0) to (100,100)
        assert tiles[0].bbox == (0, 0, 100, 100)

        # Second tile: (100,0) to (200,100)
        assert tiles[1].bbox == (100, 0, 200, 100)

    @pytest.mark.unit
    def test_edge_tile_clipping(self) -> None:
        """Edge tiles are clipped to extent boundary."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 150, "ymax": 100}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        assert len(tiles) == 2

        # First tile: full size
        assert tiles[0].bbox == (0, 0, 100, 100)
        assert tiles[0].width_px == 100

        # Edge tile: clipped
        assert tiles[1].bbox == (100, 0, 150, 100)
        assert tiles[1].width_px == 50  # Only 50 pixels wide

    @pytest.mark.unit
    def test_edge_tile_height_clipping(self) -> None:
        """Bottom edge tiles have correct height."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 100, "ymax": 150}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        assert len(tiles) == 2

        # First tile: full size
        assert tiles[0].bbox == (0, 0, 100, 100)
        assert tiles[0].height_px == 100

        # Bottom tile: clipped
        assert tiles[1].bbox == (0, 100, 100, 150)
        assert tiles[1].height_px == 50

    @pytest.mark.unit
    def test_negative_extent(self) -> None:
        """Handles extent with negative coordinates."""
        extent = {"xmin": -100, "ymin": -50, "xmax": 100, "ymax": 50}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        assert len(tiles) == 2

        assert tiles[0].bbox == (-100, -50, 0, 50)
        assert tiles[1].bbox == (0, -50, 100, 50)

    @pytest.mark.unit
    def test_non_integer_pixel_size(self) -> None:
        """Handles non-integer pixel sizes (common in real data)."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 100.5, "ymax": 100.5}
        tiles = list(compute_tile_grid(extent, pixel_size_x=0.5, pixel_size_y=0.5, tile_size=100))

        # 100.5 / (0.5 * 100) = 2.01 -> ceil = 3 tiles per axis? No, let's verify
        # tile_width_map = 100 * 0.5 = 50 map units
        # num_cols = ceil(100.5 / 50) = ceil(2.01) = 3
        assert len(tiles) == 9  # 3x3 grid

    @pytest.mark.unit
    def test_generator_is_lazy(self) -> None:
        """compute_tile_grid returns a generator, not a list."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 1000000, "ymax": 1000000}
        result = compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100)

        # Should be a generator, not a list
        import types

        assert isinstance(result, types.GeneratorType)

    @pytest.mark.unit
    def test_pixel_dimensions_match_bbox(self) -> None:
        """Tile pixel dimensions are consistent with bbox and pixel size."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 100, "ymax": 100}
        pixel_size = 2.0
        tile_size = 25

        tiles = list(
            compute_tile_grid(
                extent, pixel_size_x=pixel_size, pixel_size_y=pixel_size, tile_size=tile_size
            )
        )

        for tile in tiles:
            minx, miny, maxx, maxy = tile.bbox
            expected_width_px = round((maxx - minx) / pixel_size)
            expected_height_px = round((maxy - miny) / pixel_size)

            assert tile.width_px == expected_width_px
            assert tile.height_px == expected_height_px

    @pytest.mark.unit
    def test_tiles_cover_full_extent(self) -> None:
        """All tiles together cover the full extent."""
        extent = {"xmin": 10, "ymin": 20, "xmax": 310, "ymax": 220}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        # Find the actual covered extent
        all_minx = min(t.bbox[0] for t in tiles)
        all_miny = min(t.bbox[1] for t in tiles)
        all_maxx = max(t.bbox[2] for t in tiles)
        all_maxy = max(t.bbox[3] for t in tiles)

        assert all_minx == extent["xmin"]
        assert all_miny == extent["ymin"]
        assert all_maxx == extent["xmax"]
        assert all_maxy == extent["ymax"]

    @pytest.mark.unit
    def test_no_tile_gaps(self) -> None:
        """Adjacent tiles share edges with no gaps."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 300, "ymax": 200}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        # Group tiles by row
        tiles_by_row: dict[int, list[TileSpec]] = {}
        for tile in tiles:
            tiles_by_row.setdefault(tile.y, []).append(tile)

        # Check horizontal adjacency
        for row_tiles in tiles_by_row.values():
            sorted_tiles = sorted(row_tiles, key=lambda t: t.x)
            for i in range(len(sorted_tiles) - 1):
                current = sorted_tiles[i]
                next_tile = sorted_tiles[i + 1]
                # Current tile's right edge should equal next tile's left edge
                assert current.bbox[2] == next_tile.bbox[0]

    @pytest.mark.unit
    def test_asymmetric_pixel_sizes(self) -> None:
        """Handles different X and Y pixel sizes."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 200, "ymax": 100}
        # X pixels are 2x larger than Y pixels
        tiles = list(compute_tile_grid(extent, pixel_size_x=2, pixel_size_y=1, tile_size=50))

        # tile_width_map = 50 * 2 = 100
        # tile_height_map = 50 * 1 = 50
        # cols = ceil(200 / 100) = 2
        # rows = ceil(100 / 50) = 2
        assert len(tiles) == 4


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.unit
    def test_zero_dimension_extent_x(self) -> None:
        """Extent with zero width still produces at least one tile."""
        extent = {"xmin": 100, "ymin": 0, "xmax": 100, "ymax": 100}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        # Should produce at least 1 tile (degenerate case)
        assert len(tiles) >= 1

    @pytest.mark.unit
    def test_zero_dimension_extent_y(self) -> None:
        """Extent with zero height still produces at least one tile."""
        extent = {"xmin": 0, "ymin": 50, "xmax": 100, "ymax": 50}
        tiles = list(compute_tile_grid(extent, pixel_size_x=1, pixel_size_y=1, tile_size=100))

        # Should produce at least 1 tile (degenerate case)
        assert len(tiles) >= 1

    @pytest.mark.unit
    def test_very_small_pixel_size(self) -> None:
        """Very small pixel size produces many tiles."""
        extent = {"xmin": 0, "ymin": 0, "xmax": 10, "ymax": 10}
        # This would create a 1000x1000 tile grid if unchecked
        count = tile_count(extent, pixel_size_x=0.01, pixel_size_y=0.01, tile_size=10)

        # 10 / (0.01 * 10) = 100 tiles per axis = 10000 total
        assert count == 10000

    @pytest.mark.unit
    def test_floating_point_precision(self) -> None:
        """Handles floating point precision correctly."""
        # Use values that can cause floating point issues
        extent = {"xmin": 0.1, "ymin": 0.2, "xmax": 0.3, "ymax": 0.4}
        tiles = list(compute_tile_grid(extent, pixel_size_x=0.1, pixel_size_y=0.1, tile_size=2))

        # Should handle this without errors
        assert len(tiles) >= 1

        # Tiles should cover the extent
        for tile in tiles:
            assert tile.bbox[0] >= extent["xmin"] - 1e-10
            assert tile.bbox[1] >= extent["ymin"] - 1e-10
            assert tile.bbox[2] <= extent["xmax"] + 1e-10
            assert tile.bbox[3] <= extent["ymax"] + 1e-10

    @pytest.mark.unit
    def test_large_coordinates(self) -> None:
        """Handles large coordinate values (e.g., State Plane feet)."""
        # State Plane coordinates can be in millions of feet
        extent = {
            "xmin": 1420000.0,
            "ymin": 460000.0,
            "xmax": 1435000.0,
            "ymax": 475000.0,
        }
        tiles = list(compute_tile_grid(extent, pixel_size_x=10, pixel_size_y=10, tile_size=4096))

        # 15000 / (10 * 4096) = 0.37 -> 1 tile per axis? No:
        # tile_width = 4096 * 10 = 40960 > 15000, so 1 col
        assert len(tiles) == 1
        assert tiles[0].bbox[0] == extent["xmin"]
        assert tiles[0].bbox[1] == extent["ymin"]
