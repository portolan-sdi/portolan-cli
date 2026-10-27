"""Scale-gate constants of config: YAML parses and merged-config builds. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "config.merges"): (0, 4),  # config:merge_config_files x2, config:merge_metadata_files x2
    ("add-large-vector", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("add-many-collections", "config.merges"): (6, 2),  # config:merge_config_files x7, config:merge_metadata_files x7
    ("add-many-collections", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("add-many-collections-thumbs", "config.merges"): (4, 2),  # config:merge_config_files x5, config:merge_metadata_files x5
    ("add-many-collections-thumbs", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("add-one-collection", "config.merges"): (0, 4),  # config:merge_config_files x2, config:merge_metadata_files x2
    ("add-one-collection", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("add-rasters", "config.merges"): (0, 4),  # config:merge_config_files x2, config:merge_metadata_files x2
    ("add-rasters", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("add-with-companions", "config.merges"): (0, 4),  # config:merge_config_files x2, config:merge_metadata_files x2
    ("add-with-companions", "config.yaml_parses"): (0, 2),  # config:parse_yaml x2
    ("check-metadata", "config.merges"): (0, 1),  # config:merge_config_files x1
    ("check-metadata", "config.yaml_parses"): (0, 1),  # config:parse_yaml x1
    ("re-add-unchanged", "config.merges"): (0, 2),  # config:merge_config_files x2
    ("re-add-unchanged", "config.yaml_parses"): (0, 1),  # config:parse_yaml x1
    ("status", "config.yaml_parses"): (0, 1),  # config:parse_yaml x1
}
# fmt: on
