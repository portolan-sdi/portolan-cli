"""Scale-gate constants of versions: versions.json reads and writes, published versions, checksums, items.parquet. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "checksum.files"): (0, 1),  # sync.checksums:compute_checksum x1
    ("add-large-vector", "versions.published"): (0, 1),  # versions:add_version x1
    ("add-large-vector", "versions_json.reads"): (0, 3),  # catalog:_read_catalog_index x2, versions:read_versions x1
    ("add-large-vector", "versions_json.writes"): (0, 2),  # versions:write_versions x1, catalog:update_catalog_index x1
    ("add-many-collections", "checksum.files"): (3, 0),  # sync.checksums:compute_checksum x6
    ("add-many-collections", "versions.published"): (3, 0),  # versions:add_version x6
    ("add-many-collections", "versions_json.reads"): (6, 1),  # catalog:_read_catalog_index x7, versions:read_versions x6
    ("add-many-collections", "versions_json.writes"): (6, 0),  # versions:write_versions x6, catalog:update_catalog_index x6
    ("add-many-collections-thumbs", "checksum.files"): (6, 0),  # sync.checksums:compute_checksum x12
    ("add-many-collections-thumbs", "versions.published"): (2, 0),  # versions:add_version x4
    ("add-many-collections-thumbs", "versions_json.reads"): (6, 1),  # versions:read_versions x8, catalog:_read_catalog_index x5
    ("add-many-collections-thumbs", "versions_json.writes"): (6, 1),  # versions:write_versions x8, catalog:update_catalog_index x5
    ("add-one-collection", "checksum.files"): (3, 0),  # sync.checksums:compute_checksum x6
    ("add-one-collection", "versions.published"): (0, 1),  # versions:add_version x1
    ("add-one-collection", "versions_json.reads"): (0, 3),  # catalog:_read_catalog_index x2, versions:read_versions x1
    ("add-one-collection", "versions_json.writes"): (0, 2),  # versions:write_versions x1, catalog:update_catalog_index x1
    ("add-rasters", "checksum.files"): (6, 1),  # sync.checksums:compute_checksum x13
    ("add-rasters", "items_parquet.writes"): (0, 1),  # stac_parquet:generate_items_parquet x1
    ("add-rasters", "versions.published"): (0, 1),  # versions:add_version x1
    ("add-rasters", "versions_json.reads"): (0, 5),  # catalog:_read_catalog_index x3, versions:read_versions x2
    ("add-rasters", "versions_json.writes"): (0, 4),  # versions:write_versions x2, catalog:update_catalog_index x2
    ("add-with-companions", "checksum.files"): (12, 0),  # sync.checksums:compute_checksum x24
    ("add-with-companions", "versions.published"): (3, 1),  # versions:add_version x7
    ("add-with-companions", "versions_json.reads"): (6, 3),  # catalog:_read_catalog_index x8, versions:read_versions x7
    ("add-with-companions", "versions_json.writes"): (6, 2),  # versions:write_versions x7, catalog:update_catalog_index x7
    ("check-metadata", "versions_json.reads"): (0, 1),  # versions:read_versions x1
    ("re-add-unchanged", "checksum.files"): (0, 1),  # sync.checksums:compute_checksum x1
    ("re-add-unchanged", "versions_json.reads"): (3, 3),  # versions:read_versions x8, catalog:_read_catalog_index x1
    ("status", "versions_json.reads"): (0, 1),  # versions:read_versions x1
}
# fmt: on
