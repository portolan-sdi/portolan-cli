"""Tile grid calculation for ImageServer extraction.

This module computes a grid of tiles covering a service extent. Each tile
represents a portion of the raster that will be requested independently,
enabling efficient parallel downloads and handling of large datasets.

Tile grid math:
- Tile width (map units) = tile_size * pixel_size_x
- Number of columns = ceil((xmax - xmin) / tile_width)
- Edge tiles may be smaller than tile_size when the extent doesn't
  align perfectly with tile boundaries

Typical usage:
    from portolan_cli.extract.arcgis.imageserver.tiling import (
        compute_tile_grid,
        tile_count,
    )

    extent = {"xmin": 0, "ymin": 0, "xmax": 100000, "ymax": 50000}

    # Get total count for progress display
    total = tile_count(extent, pixel_size_x=10, pixel_size_y=10)
    print(f"Downloading {total} tiles...")

    # Iterate tiles (memory-efficient generator)
    for tile in compute_tile_grid(extent, pixel_size_x=10, pixel_size_y=10):
        download_tile(tile.bbox, tile.width_px, tile.height_px)
"""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass


@dataclass(frozen=True)
class TileSpec:
    """Specification for a single tile in the grid.

    Attributes:
        x: Tile column index (0-indexed, from left)
        y: Tile row index (0-indexed, from top)
        bbox: Bounding box in map units (minx, miny, maxx, maxy)
        width_px: Tile width in pixels
        height_px: Tile height in pixels

    Note:
        Edge tiles (right and bottom edges) may have smaller pixel
        dimensions than interior tiles when the extent doesn't align
        perfectly with tile boundaries.
    """

    x: int
    y: int
    bbox: tuple[float, float, float, float]
    width_px: int
    height_px: int

    def get_id(self) -> str:
        """Generate tile ID string for STAC Item.

        Returns:
            Tile ID in format 'tile_{x}_{y}'
        """
        return f"tile_{self.x}_{self.y}"


def _compute_grid_dimensions(
    extent: dict[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int,
) -> tuple[int, int, float, float]:
    """Compute grid dimensions and tile sizes in map units.

    Args:
        extent: Bounding box with xmin, ymin, xmax, ymax keys
        pixel_size_x: Pixel size in X direction (map units)
        pixel_size_y: Pixel size in Y direction (map units)
        tile_size: Tile size in pixels

    Returns:
        Tuple of (num_cols, num_rows, tile_width_map, tile_height_map)
    """
    extent_width = extent["xmax"] - extent["xmin"]
    extent_height = extent["ymax"] - extent["ymin"]

    # Tile dimensions in map units
    tile_width_map = tile_size * pixel_size_x
    tile_height_map = tile_size * pixel_size_y

    # Number of tiles needed (ceil to cover entire extent)
    num_cols = math.ceil(extent_width / tile_width_map)
    num_rows = math.ceil(extent_height / tile_height_map)

    # Ensure at least 1 tile even for tiny extents
    num_cols = max(1, num_cols)
    num_rows = max(1, num_rows)

    return num_cols, num_rows, tile_width_map, tile_height_map


def tile_count(
    extent: dict[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int = 4096,
) -> int:
    """Return total number of tiles covering the extent.

    This is useful for progress display before iterating tiles.

    Args:
        extent: Bounding box with xmin, ymin, xmax, ymax keys
        pixel_size_x: Pixel size in X direction (map units per pixel)
        pixel_size_y: Pixel size in Y direction (map units per pixel)
        tile_size: Tile size in pixels (default: 4096)

    Returns:
        Total number of tiles (columns * rows)

    Example:
        total = tile_count(extent, pixel_size_x=10, pixel_size_y=10)
        progress_bar = tqdm(total=total)
    """
    num_cols, num_rows, _, _ = _compute_grid_dimensions(
        extent, pixel_size_x, pixel_size_y, tile_size
    )
    return num_cols * num_rows


def compute_tile_grid(
    extent: dict[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int = 4096,
) -> Iterator[TileSpec]:
    """Generate tile specs covering the full extent.

    Tiles are yielded in row-major order (top-to-bottom, left-to-right).
    Edge tiles are clipped to the extent boundary and may have smaller
    pixel dimensions than interior tiles.

    This is a generator function for memory efficiency - tiles are not
    pre-computed, allowing efficient handling of large extents with
    many tiles.

    Args:
        extent: Bounding box with xmin, ymin, xmax, ymax keys
        pixel_size_x: Pixel size in X direction (map units per pixel)
        pixel_size_y: Pixel size in Y direction (map units per pixel)
        tile_size: Tile size in pixels (default: 4096)

    Yields:
        TileSpec objects for each tile in the grid

    Example:
        for tile in compute_tile_grid(extent, pixel_size_x=10, pixel_size_y=10):
            print(f"Tile ({tile.x}, {tile.y}): {tile.bbox}")
            print(f"  Size: {tile.width_px}x{tile.height_px} pixels")
    """
    num_cols, num_rows, tile_width_map, tile_height_map = _compute_grid_dimensions(
        extent, pixel_size_x, pixel_size_y, tile_size
    )

    xmin = extent["xmin"]
    ymin = extent["ymin"]
    xmax = extent["xmax"]
    ymax = extent["ymax"]

    # Iterate in row-major order
    for row in range(num_rows):
        for col in range(num_cols):
            # Calculate tile bounding box
            tile_xmin = xmin + col * tile_width_map
            tile_ymin = ymin + row * tile_height_map
            tile_xmax = min(tile_xmin + tile_width_map, xmax)
            tile_ymax = min(tile_ymin + tile_height_map, ymax)

            # Calculate pixel dimensions for this tile
            # (may be smaller for edge tiles)
            tile_width_actual = tile_xmax - tile_xmin
            tile_height_actual = tile_ymax - tile_ymin

            width_px = round(tile_width_actual / pixel_size_x)
            height_px = round(tile_height_actual / pixel_size_y)

            yield TileSpec(
                x=col,
                y=row,
                bbox=(tile_xmin, tile_ymin, tile_xmax, tile_ymax),
                width_px=width_px,
                height_px=height_px,
            )
