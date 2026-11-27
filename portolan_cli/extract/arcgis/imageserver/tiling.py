"""Tile grid calculation for ImageServer extraction."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping


@dataclass(frozen=True)
class TileSpec:
    """Specification for a single tile in the grid."""

    x: int
    y: int
    bbox: tuple[float, float, float, float]
    width_px: int
    height_px: int

    def get_id(self) -> str:
        """Generate tile ID string for STAC Item."""
        return f"tile_{self.x}_{self.y}"


def _compute_grid_dimensions(
    extent: Mapping[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int,
) -> tuple[int, int, float, float]:
    """Compute grid dimensions and tile sizes in map units."""
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
    extent: Mapping[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int = 4096,
) -> int:
    """Return total number of tiles covering the extent."""
    num_cols, num_rows, _, _ = _compute_grid_dimensions(
        extent, pixel_size_x, pixel_size_y, tile_size
    )
    return num_cols * num_rows


def compute_tile_grid(
    extent: Mapping[str, float],
    pixel_size_x: float,
    pixel_size_y: float,
    tile_size: int = 4096,
) -> Iterator[TileSpec]:
    """Generate tile specs covering the full extent."""
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
