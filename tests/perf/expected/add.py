"""Scale-gate constants of add: collection.json writes. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "collection_json.writes"): (0, 1),  # finalization:_save_collection_with_links x1
    ("add-many-collections", "collection_json.writes"): (3, 0),  # finalization:_save_collection_with_links x6
    ("add-many-collections-thumbs", "collection_json.writes"): (4, 0),  # finalization:_save_collection_with_links x4, collection_thumbnail:register_collection_thumbnail x4
    ("add-one-collection", "collection_json.writes"): (0, 1),  # finalization:_save_collection_with_links x1
    ("add-rasters", "collection_json.writes"): (0, 3),  # finalization:_save_collection_with_links x1, catalog:ensure_link_titles x1
    ("add-with-companions", "collection_json.writes"): (0, 1),  # finalization:_save_collection_with_links x1
}
# fmt: on
