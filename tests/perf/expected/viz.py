"""Scale-gate constants of viz: thumbnail draws and basemap tiles. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-many-collections-thumbs", "thumbnail.draws"): (2, 0),  # viz.thumbnail:_count_draw x4
    ("add-many-collections-thumbs", "tiles.fetched"): (27, -9),  # viz.thumbnail:_fetch_basemap x4
}
# fmt: on

#: The tracemalloc growth of ``add.thumbnails`` for one file of 100,000 polygons
#: with the stub basemap, in MB: 45.58 measured plus 15%. The growth is the same
#: when the add stack loads at startup (45.57) or inside the command (45.58).
THUMBNAIL_TRACEMALLOC_MB = 52.4
#: Full figure draws for that thumbnail. The render draws the features and
#: the basemap image on one axes, and Figure.savefig draws the figure once.
THUMBNAIL_DRAWS = 1
