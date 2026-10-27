"""Scale-gate constants: thumbnail draws and basemap tiles (#992).

Each entry maps (scenario, counter) to (slope, intercept). The comment names
the functions that made the counts at size 2n, with their number of calls.
A pair that is absent is (0, 0). ``tests/perf/gates.py`` explains the values.
"""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-many-collections-thumbs", "thumbnail.draws"): (10, 0),  # viz.thumbnail:_count_draw x20
    ("add-many-collections-thumbs", "tiles.fetched"): (27, -9),  # viz.thumbnail:add_basemap x45
}
# fmt: on

#: The tracemalloc peak of ``add.thumbnails`` for one file of 100,000 polygons
#: with the stub basemap, in MB: 354.0 measured plus 15%.
THUMBNAIL_TRACEMALLOC_MB = 407.1
#: Full figure draws for that thumbnail. On the Agg backend each draw_idle
#: draws. geopandas plot_series 1, contextily add_basemap 1, savefig with
#: bbox_inches="tight" 2 (print_figure and print_jpg), pyplot.savefig 1.
THUMBNAIL_DRAWS = 5
