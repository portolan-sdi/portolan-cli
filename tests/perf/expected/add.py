"""Scale-gate constants: collection.json writes (#992).

Each entry maps (scenario, counter) to (slope, intercept). The comment names
the functions that made the counts at size 2n, with their number of calls.
A pair that is absent is (0, 0). ``tests/perf/gates.py`` explains the values.
"""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "collection_json.writes"): (0, 5),  # finalization:_save_collection_with_links x1, finalization:_fix_collection_links x1
    ("add-many-collections", "collection_json.writes"): (15, 0),  # finalization:_save_collection_with_links x6, finalization:_fix_collection_links x6
    ("add-many-collections-thumbs", "collection_json.writes"): (12, 0),  # finalization:_save_collection_with_links x4, finalization:_fix_collection_links x4
    ("add-one-collection", "collection_json.writes"): (0, 5),  # finalization:_save_collection_with_links x1, finalization:_fix_collection_links x1
    ("add-rasters", "collection_json.writes"): (0, 7),  # finalization:_save_collection_with_links x1, finalization:_fix_collection_links x1
    ("add-with-companions", "collection_json.writes"): (3, 6),  # add:_update_collection_with_asset x6, finalization:_save_collection_with_links x1
}
# fmt: on
