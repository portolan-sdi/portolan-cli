"""Scale-gate constants of config: YAML parses and merged-config builds. See gates.py."""

from __future__ import annotations

# fmt: off
EXPECTED: dict[tuple[str, str], tuple[int, int]] = {
    ("add-large-vector", "config.merges"): (0, 18),  # config:load_merged_config x13, config:load_merged_yaml x5
    ("add-large-vector", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("add-many-collections", "config.merges"): (51, 1),  # config:load_merged_config x78, config:load_merged_yaml x25
    ("add-many-collections", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("add-many-collections-thumbs", "config.merges"): (34, 1),  # config:load_merged_config x52, config:load_merged_yaml x17
    ("add-many-collections-thumbs", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("add-one-collection", "config.merges"): (9, 15),  # config:load_merged_config x23, config:load_merged_yaml x10
    ("add-one-collection", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("add-rasters", "config.merges"): (12, 15),  # config:load_merged_config x29, config:load_merged_yaml x10
    ("add-rasters", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("add-with-companions", "config.merges"): (15, 15),  # config:load_merged_config x23, config:load_merged_yaml x22
    ("add-with-companions", "config.yaml_parses"): (0, 2),  # config:_parse_yaml x2
    ("check-metadata", "config.yaml_parses"): (0, 1),  # config:_parse_yaml x1
    ("re-add-unchanged", "config.merges"): (0, 11),  # config:load_merged_config x11
    ("re-add-unchanged", "config.yaml_parses"): (0, 1),  # config:_parse_yaml x1
    ("status", "config.yaml_parses"): (0, 1),  # config:_parse_yaml x1
}
# fmt: on
