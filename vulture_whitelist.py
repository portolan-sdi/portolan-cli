"""Vulture whitelist.

Vulture reads this file as source. Each name below counts as used. See
https://github.com/jendrikseipp/vulture#whitelisting.

``[tool.vulture]`` runs at ``min_confidence = 60``. The names below are
vulture's own ``--make-whitelist`` output at 60 on 2026-10-23, deduplicated by
name and grouped by package. Each comment keeps the first place vulture
reported and the count of further places.

``tests/docs/test_quality_config.py`` pins the name count. The list only
shrinks. Delete a name when its code goes away or gains a caller, and lower
the pinned count in the same commit. Do not add a name to hide new dead code.
"""

from __future__ import annotations

# --- portolan_cli ---
_.__class__  # unused attribute (portolan_cli/__init__.py:63). The module swaps its own class.
add_cmd  # unused function (portolan_cli/cli.py:3140)
add_external_cmd  # unused function (portolan_cli/cli.py:3486)
bump  # unused function (portolan_cli/cli.py:6651)
config_get  # unused function (portolan_cli/cli.py:4942)
config_list  # unused function (portolan_cli/cli.py:5013)
config_set  # unused function (portolan_cli/cli.py:4868)
config_unset  # unused function (portolan_cli/cli.py:5077)
DEFAULT_RESOLUTIONS  # unused variable (portolan_cli/conversion_config.py:578)
document_path  # unused variable (portolan_cli/hookspecs.py:59)
HUMAN_ENRICHABLE_ASSET_FIELDS  # unused variable (portolan_cli/stac.py:77)
info_cmd  # unused function (portolan_cli/cli.py:1109)
is_geospatial  # unused variable (portolan_cli/extension_registry.py:65)
is_tabular  # unused variable (portolan_cli/extension_registry.py:66)
list_cmd  # unused function (portolan_cli/cli.py:829)
logo_cmd  # unused function (portolan_cli/cli.py:6224)
metadata_init  # unused function (portolan_cli/cli.py:5286)
metadata_validate  # unused function (portolan_cli/cli.py:5397)
pull_command  # unused function (portolan_cli/cli.py:4326)
REQUIRED_FIELDS  # unused variable (portolan_cli/metadata_yaml.py:59)
ResamplingMethod  # unused variable (portolan_cli/convert.py:56)
rm_cmd  # unused function (portolan_cli/cli.py:3699)
routes_as  # unused variable (portolan_cli/extension_registry.py:56)
scan_category  # unused variable (portolan_cli/extension_registry.py:58)
skills_list_cmd  # unused function (portolan_cli/cli.py:7091)
skills_show_cmd  # unused function (portolan_cli/cli.py:7104)
status_cmd  # unused function (portolan_cli/cli.py:964)
version_list_cmd  # unused function (portolan_cli/cli.py:6539)

# --- portolan_cli/backends ---
__getattr__  # unused function (portolan_cli/backends/iceberg/__init__.py:34)
add_spatial_columns  # unused function (portolan_cli/backends/iceberg/spatial.py:84)
portolan_backends  # unused function (portolan_cli/backends/iceberg/hooks.py:46, 5 more)

# --- portolan_cli/extract ---
extract_arcgis_cmd  # unused function (portolan_cli/extract/arcgis/cli.py:259)
extract_carto_cmd  # unused function (portolan_cli/extract/carto/cli.py:18)
extract_wfs_cmd  # unused function (portolan_cli/extract/wfs/cli.py:18)

# --- portolan_cli/plugins ---
_.format_commands  # unused method (portolan_cli/plugins/commands.py:165)
group_class  # unused variable (portolan_cli/plugins/commands.py:57)
portolan_formats  # unused function (portolan_cli/plugins/builtin/formats.py:26, 2 more)

# --- portolan_cli/scan ---
FILEGDB_INTERNAL_EXTENSIONS  # unused variable (portolan_cli/scan/detect.py:35)
ProgressCallback  # unused variable (portolan_cli/scan/core.py:42)
SPECIAL_DIRECTORY  # unused variable (portolan_cli/scan/classify.py:134)

# --- portolan_cli/viz ---

# --- tests ---
_.exitstatus  # pytest calls it by name (tests/conftest.py:230)
_mutmut_stats_children  # unused function (tests/conftest.py:263)
catalog_with_versions_for_dry_run  # unused function (tests/conftest.py:511)
isolate_aws_config  # unused function (tests/conftest.py:790)
isolate_installed_plugins  # unused function (tests/conftest.py:928)
isolate_trace_and_logging  # unused function (tests/conftest.py:903)
no_basemap_tiles  # unused function (tests/conftest.py:854)
no_extract_side_requests  # unused function (tests/conftest.py:872)
pytest_collection_modifyitems  # unused function (tests/conftest.py:192)
pytest_configure  # unused function (tests/conftest.py:160)
pytest_runtest_call  # unused function (tests/conftest.py:250)
pytest_runtest_setup  # unused function (tests/conftest.py:245)
pytest_sessionfinish  # pytest calls it by name (tests/conftest.py:219)
temp_catalog_dir  # unused function (tests/conftest.py:494)

# --- tests/benchmark ---
pytestmark  # unused variable (tests/benchmark/test_entry_points_scan.py:25, 141 more)

# --- tests/docs ---
_.log_message  # unused method (tests/replay_server.py:141)
catalog_with_geojson  # unused function (tests/docs/conftest.py:77)
daemon_threads  # unused variable (tests/replay_server.py:131)

# --- tests/fixtures ---
convert_to_geoparquet  # unused function (tests/fixtures/generate.py:444)

# --- tests/iceberg ---
_.__str__  # unused attribute (tests/iceberg/unit/test_stac_generator.py:622)
_exported_catalog  # unused function (tests/iceberg/unit/test_config_isolation.py:16)
_isolate_pyiceberg_config  # unused function (tests/iceberg/conftest.py:19)

# --- tests/integration ---
_.side_effect  # unused attribute (tests/integration/test_clone_ergonomics_integration.py:217, 101 more)
_aws_env  # unused function (tests/integration/test_s3_moto.py:241)
_create_extraction_mock  # unused function (tests/integration/extract/test_featureserver_metadata_seeding.py:38)
broken_profile_environ  # unused function (tests/integration/test_push_integration.py:1852)
catalog_with_vectors  # unused function (tests/integration/test_add_rm_integration.py:38)
make_imageserver_report  # unused function (tests/integration/extract/test_imageserver_metadata_seeding.py:64)

# --- tests/plugins ---
fresh_manager  # unused function (tests/plugins/test_example_plugin.py:31)
portolan_enrich_collection  # unused function (tests/plugins/portolan_example/src/portolan_example/hooks.py:36, 1 more)

# --- tests/unit ---
_.__iter__  # unused attribute (tests/unit/test_download.py:173, 17 more)
_.__len__  # unused attribute (tests/unit/extract/arcgis/test_orchestrator.py:205, 1 more)
_.portolan_backends  # unused method (tests/unit/backends/test_backends.py:171, 6 more)
_.portolan_enrich_collection  # unused method (tests/unit/plugins/test_enrich.py:51, 1 more)
_.portolan_formats  # unused method (tests/unit/plugins/test_formats.py:26, 1 more)
_.testinspect_cog_includes_version_when_tracked  # unused method (tests/unit/test_inspect.py:307)
_.testinspect_cog_returns_metadata  # unused method (tests/unit/test_inspect.py:292)
_.testinspect_geoparquet_includes_version_when_tracked  # unused method (tests/unit/test_inspect.py:252)
_.testinspect_geoparquet_no_version_when_not_tracked  # unused method (tests/unit/test_inspect.py:264)
_.testinspect_geoparquet_returns_metadata  # unused method (tests/unit/test_inspect.py:237)
_.testvalidate_geoparquet_handles_directory  # unused method (tests/unit/test_convert.py:1939)
_add_scripts_to_path  # unused function (tests/unit/test_scripts.py:87)
_create_versions_file  # unused function (tests/unit/test_status.py:27)
_no_rashid  # unused function (tests/unit/validation/test_workflow_notice.py:36)
converted_parquet  # unused function (tests/unit/test_check_remove_legacy.py:108)
csv_safe_values  # unused variable (tests/unit/test_csv_skip.py:429)
DEFAULT_ESRI_COLOR  # unused variable (tests/unit/extract/common/converters/test_esri.py:343)
empty_remote  # unused function (tests/unit/test_issue_252_catalog_json_roundtrip.py:35)
default_root_level  # unused function (tests/unit/test_logging_setup.py:28)
fake_echo  # unused function (tests/unit/test_scan_output_batching.py:69)
hello  # unused function (tests/unit/plugin_fixtures/commands.py:13)
lazy  # unused function (tests/unit/plugin_fixtures/lazy_commands.py:8)
mock_check_report  # unused function (tests/unit/test_cli_check.py:43)
mock_cog_file  # unused function (tests/unit/test_add_no_duplicate_nesting.py:29)
non_geo_column_names  # unused variable (tests/unit/test_csv_skip.py:410)
not_a_command  # unused variable (tests/unit/plugin_fixtures/commands.py:31)
portolan_enrich_itme  # unused function (tests/unit/plugin_fixtures/misspelled.py:10)
remote_versions_diverged  # unused function (tests/unit/test_push.py:151)
remote_versions_same  # unused function (tests/unit/test_push.py:126)
stub_add_files  # unused function (tests/unit/extract/common/test_orchestrator_base.py:32)
