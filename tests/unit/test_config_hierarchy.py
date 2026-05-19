"""Tests for hierarchical .portolan/ folder support (ADR-0039).

Tests the directory tree walking and config/metadata merging:
- find_portolan_files(): Find all .portolan/{filename} from start to root
- load_merged_yaml(): Deep merge YAML files with child-overrides-parent
- load_merged_config(): Config-specific merge with backwards compatibility
- load_merged_metadata(): Metadata-specific merge for README generation
"""

from __future__ import annotations

from pathlib import Path

import pytest


class TestFindPortolanFiles:
    """Tests for find_portolan_files function."""

    @pytest.mark.unit
    def test_returns_empty_list_when_no_portolan_folders(self, tmp_path: Path) -> None:
        """find_portolan_files returns [] when no .portolan/ folders exist."""
        from portolan_cli.config import find_portolan_files

        # Create catalog structure without any .portolan folders
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()

        result = find_portolan_files(
            start_path=collection_dir,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == []

    @pytest.mark.unit
    def test_returns_catalog_root_file_only(self, tmp_path: Path) -> None:
        """find_portolan_files returns only catalog root file when that's all that exists."""
        from portolan_cli.config import find_portolan_files

        # Create catalog with .portolan at root only
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://bucket/")

        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()

        result = find_portolan_files(
            start_path=collection_dir,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == [catalog_root / ".portolan" / "config.yaml"]

    @pytest.mark.unit
    def test_returns_files_from_root_to_start(self, tmp_path: Path) -> None:
        """find_portolan_files returns files in order: catalog root -> start_path."""
        from portolan_cli.config import find_portolan_files

        # Create catalog with .portolan at both root and collection
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://catalog/")

        collection_dir = catalog_root / "demographics"
        collection_dir.mkdir()
        (collection_dir / ".portolan").mkdir()
        (collection_dir / ".portolan" / "config.yaml").write_text("remote: s3://collection/")

        result = find_portolan_files(
            start_path=collection_dir,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        # Order: root first, then start_path (for merging: parent, then child)
        assert result == [
            catalog_root / ".portolan" / "config.yaml",
            collection_dir / ".portolan" / "config.yaml",
        ]

    @pytest.mark.unit
    def test_includes_intermediate_subcatalog_folders(self, tmp_path: Path) -> None:
        """find_portolan_files includes .portolan from intermediate subcatalogs."""
        from portolan_cli.config import find_portolan_files

        # Create: catalog/ -> historical/ (subcatalog) -> census-1990/ (collection)
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("aws_profile: root")

        subcatalog = catalog_root / "historical"
        subcatalog.mkdir()
        (subcatalog / ".portolan").mkdir()
        (subcatalog / ".portolan" / "config.yaml").write_text("aws_profile: subcatalog")

        collection = subcatalog / "census-1990"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text("aws_profile: collection")

        result = find_portolan_files(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == [
            catalog_root / ".portolan" / "config.yaml",
            subcatalog / ".portolan" / "config.yaml",
            collection / ".portolan" / "config.yaml",
        ]

    @pytest.mark.unit
    def test_skips_levels_without_portolan_folder(self, tmp_path: Path) -> None:
        """find_portolan_files skips levels that don't have .portolan/ folders."""
        from portolan_cli.config import find_portolan_files

        # Create: catalog/ -> subcatalog/ (no .portolan) -> collection/
        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://root/")

        subcatalog = catalog_root / "subcatalog"
        subcatalog.mkdir()
        # No .portolan here!

        collection = subcatalog / "collection"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text("remote: s3://collection/")

        result = find_portolan_files(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        # Should skip subcatalog level
        assert result == [
            catalog_root / ".portolan" / "config.yaml",
            collection / ".portolan" / "config.yaml",
        ]

    @pytest.mark.unit
    def test_skips_levels_without_requested_file(self, tmp_path: Path) -> None:
        """find_portolan_files skips .portolan folders that don't have the requested file."""
        from portolan_cli.config import find_portolan_files

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://root/")
        # No metadata.yaml at root

        collection = catalog_root / "collection"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "metadata.yaml").write_text("title: My Collection")
        # No config.yaml at collection

        # Looking for config.yaml
        config_result = find_portolan_files(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )
        assert config_result == [catalog_root / ".portolan" / "config.yaml"]

        # Looking for metadata.yaml
        metadata_result = find_portolan_files(
            start_path=collection,
            filename="metadata.yaml",
            catalog_root=catalog_root,
        )
        assert metadata_result == [collection / ".portolan" / "metadata.yaml"]

    @pytest.mark.unit
    def test_start_path_equals_catalog_root(self, tmp_path: Path) -> None:
        """find_portolan_files works when start_path is the catalog root."""
        from portolan_cli.config import find_portolan_files

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://bucket/")

        result = find_portolan_files(
            start_path=catalog_root,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == [catalog_root / ".portolan" / "config.yaml"]

    @pytest.mark.unit
    def test_does_not_traverse_above_catalog_root(self, tmp_path: Path) -> None:
        """find_portolan_files stops at catalog_root, doesn't go higher."""
        from portolan_cli.config import find_portolan_files

        # Create .portolan above catalog root (should be ignored)
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("remote: s3://above-root/")

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("remote: s3://catalog/")

        collection = catalog_root / "collection"
        collection.mkdir()

        result = find_portolan_files(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        # Should only include catalog root, not tmp_path
        assert result == [catalog_root / ".portolan" / "config.yaml"]
        assert tmp_path / ".portolan" / "config.yaml" not in result


class TestLoadMergedYaml:
    """Tests for load_merged_yaml function (generic deep merge)."""

    @pytest.mark.unit
    def test_returns_empty_dict_when_no_files(self, tmp_path: Path) -> None:
        """load_merged_yaml returns {} when no .portolan/ folders exist."""
        from portolan_cli.config import load_merged_yaml

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        collection = catalog_root / "collection"
        collection.mkdir()

        result = load_merged_yaml(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == {}

    @pytest.mark.unit
    def test_loads_single_file(self, tmp_path: Path) -> None:
        """load_merged_yaml loads a single file correctly."""
        from portolan_cli.config import load_merged_yaml

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "remote: s3://bucket/\naws_profile: prod\n"
        )

        result = load_merged_yaml(
            start_path=catalog_root,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result == {"remote": "s3://bucket/", "aws_profile": "prod"}

    @pytest.mark.unit
    def test_child_overrides_parent_for_scalar_values(self, tmp_path: Path) -> None:
        """load_merged_yaml: child values override parent values for scalars."""
        from portolan_cli.config import load_merged_yaml

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "remote: s3://parent/\naws_profile: parent-profile\n"
        )

        collection = catalog_root / "collection"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text(
            "remote: s3://child/\n"  # Override remote, but not aws_profile
        )

        result = load_merged_yaml(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result["remote"] == "s3://child/"  # Overridden
        assert result["aws_profile"] == "parent-profile"  # Inherited

    @pytest.mark.unit
    def test_deep_merge_nested_dicts(self, tmp_path: Path) -> None:
        """load_merged_yaml deep-merges nested dictionaries."""
        from portolan_cli.config import load_merged_yaml

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Data Team\n  email: data@org.com\n"
            "columns:\n  geoid:\n    description: Census GEOID\n"
        )

        collection = catalog_root / "collection"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  email: demographics@org.com\n"  # Override email only
            "columns:\n  total_pop:\n    description: Total population\n"  # Add column
        )

        result = load_merged_yaml(
            start_path=collection,
            filename="metadata.yaml",
            catalog_root=catalog_root,
        )

        # Deep merge contact
        assert result["contact"]["name"] == "Data Team"  # Inherited
        assert result["contact"]["email"] == "demographics@org.com"  # Overridden

        # Deep merge columns
        assert result["columns"]["geoid"]["description"] == "Census GEOID"  # Inherited
        assert result["columns"]["total_pop"]["description"] == "Total population"  # Added

    @pytest.mark.unit
    def test_three_level_merge(self, tmp_path: Path) -> None:
        """load_merged_yaml correctly merges three levels of hierarchy."""
        from portolan_cli.config import load_merged_yaml

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "aws_profile: root\nremote: s3://root/\nworkers: 4\n"
        )

        subcatalog = catalog_root / "historical"
        subcatalog.mkdir()
        (subcatalog / ".portolan").mkdir()
        (subcatalog / ".portolan" / "config.yaml").write_text(
            "aws_profile: subcatalog\n"  # Override
        )

        collection = subcatalog / "census-1990"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text(
            "remote: s3://collection/\n"  # Override
        )

        result = load_merged_yaml(
            start_path=collection,
            filename="config.yaml",
            catalog_root=catalog_root,
        )

        assert result["aws_profile"] == "subcatalog"  # From subcatalog
        assert result["remote"] == "s3://collection/"  # From collection
        assert result["workers"] == 4  # From root


class TestLoadMergedConfig:
    """Tests for load_merged_config (config.yaml specific)."""

    @pytest.mark.unit
    def test_backwards_compatible_with_collections_section(self, tmp_path: Path) -> None:
        """load_merged_config supports legacy collections: section in root config."""
        from portolan_cli.config import load_merged_config

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "remote: s3://catalog/\n"
            "collections:\n"
            "  demographics:\n"
            "    remote: s3://demographics-legacy/\n"
        )

        # No collection-level .portolan
        collection = catalog_root / "demographics"
        collection.mkdir()

        result = load_merged_config(
            path=collection,
            catalog_root=catalog_root,
        )

        # Should get config from root collections: section
        assert result["remote"] == "s3://demographics-legacy/"

    @pytest.mark.unit
    def test_collection_folder_overrides_collections_section(self, tmp_path: Path) -> None:
        """Collection .portolan/config.yaml takes precedence over root collections: section."""
        from portolan_cli.config import load_merged_config

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "remote: s3://catalog/\n"
            "aws_profile: catalog-profile\n"
            "collections:\n"
            "  demographics:\n"
            "    remote: s3://legacy-override/\n"
            "    aws_profile: legacy-profile\n"
        )

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text(
            "remote: s3://folder-override/\n"  # This should win
        )

        result = load_merged_config(
            path=collection,
            catalog_root=catalog_root,
        )

        # Folder config should override collections: section
        assert result["remote"] == "s3://folder-override/"
        # But inherit from collections: for unset keys
        assert result["aws_profile"] == "legacy-profile"


class TestLoadMergedMetadata:
    """Tests for load_merged_metadata (metadata.yaml specific)."""

    @pytest.mark.unit
    def test_returns_empty_dict_when_no_metadata(self, tmp_path: Path) -> None:
        """load_merged_metadata returns {} when no metadata.yaml exists."""
        from portolan_cli.config import load_merged_metadata

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        collection = catalog_root / "collection"
        collection.mkdir()

        result = load_merged_metadata(
            path=collection,
            catalog_root=catalog_root,
        )

        assert result == {}

    @pytest.mark.unit
    def test_merges_metadata_hierarchy(self, tmp_path: Path) -> None:
        """load_merged_metadata merges metadata.yaml files from hierarchy."""
        from portolan_cli.config import load_merged_metadata

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Default Contact\n  email: default@org.com\nlicense: CC-BY-4.0\n"
        )

        collection = catalog_root / "collection"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "metadata.yaml").write_text(
            "title: My Collection\n"
            "description: A great collection\n"
            "contact:\n  email: collection@org.com\n"
        )

        result = load_merged_metadata(
            path=collection,
            catalog_root=catalog_root,
        )

        assert result["title"] == "My Collection"  # From collection
        assert result["description"] == "A great collection"  # From collection
        assert result["license"] == "CC-BY-4.0"  # Inherited from root
        assert result["contact"]["name"] == "Default Contact"  # Inherited
        assert result["contact"]["email"] == "collection@org.com"  # Overridden


class TestGetSettingWithHierarchy:
    """Tests for get_setting() using hierarchical .portolan/ folders.

    Note: Tests use non-sensitive keys (workers, backend) because sensitive keys
    (remote, profile, region) cannot be read from config.yaml per Issue #356.
    """

    @pytest.mark.unit
    def test_collection_folder_config_overrides_catalog(self, tmp_path: Path) -> None:
        """get_setting uses collection .portolan/config.yaml over catalog config."""
        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("workers: 4\n")

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text("workers: 8\n")

        result = get_setting(
            "workers",
            catalog_path=catalog_root,
            collection="demographics",
            collection_path=collection,  # New parameter for hierarchy lookup
        )

        assert result == 8

    @pytest.mark.unit
    def test_collection_folder_inherits_from_catalog(self, tmp_path: Path) -> None:
        """get_setting inherits unset keys from catalog when using folder config."""
        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("workers: 4\nbackend: file\n")

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text(
            "backend: iceberg\n"  # Only override backend
        )

        # workers should inherit from catalog
        result = get_setting(
            "workers",
            catalog_path=catalog_root,
            collection="demographics",
            collection_path=collection,
        )

        assert result == 4

    @pytest.mark.unit
    def test_env_var_overrides_collection_folder_config(self, tmp_path: Path) -> None:
        """Environment variable overrides collection .portolan/config.yaml."""
        import os
        from unittest import mock

        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("aws_profile: catalog\n")

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text("aws_profile: collection\n")

        with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
            result = get_setting(
                "aws_profile",
                catalog_path=catalog_root,
                collection="demographics",
                collection_path=collection,
            )

        assert result == "env-profile"

    @pytest.mark.unit
    def test_cli_value_overrides_all_sources(self, tmp_path: Path) -> None:
        """CLI value overrides all other sources including folder config."""
        import os
        from unittest import mock

        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("aws_profile: catalog\n")

        collection = catalog_root / "demographics"
        collection.mkdir()
        (collection / ".portolan").mkdir()
        (collection / ".portolan" / "config.yaml").write_text("aws_profile: collection\n")

        with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
            result = get_setting(
                "aws_profile",
                cli_value="cli-profile",
                catalog_path=catalog_root,
                collection="demographics",
                collection_path=collection,
            )

        assert result == "cli-profile"

    @pytest.mark.unit
    def test_subcatalog_folder_config_used(self, tmp_path: Path) -> None:
        """get_setting uses subcatalog .portolan/config.yaml in hierarchy."""
        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text("workers: 2\n")

        subcatalog = catalog_root / "historical"
        subcatalog.mkdir()
        (subcatalog / ".portolan").mkdir()
        (subcatalog / ".portolan" / "config.yaml").write_text("workers: 4\n")

        collection = subcatalog / "census-1990"
        collection.mkdir()
        # No .portolan here - should inherit from subcatalog

        result = get_setting(
            "workers",
            catalog_path=catalog_root,
            collection="historical/census-1990",
            collection_path=collection,
        )

        assert result == 4

    @pytest.mark.unit
    def test_backwards_compatible_without_collection_path(self, tmp_path: Path) -> None:
        """get_setting works without collection_path (legacy behavior)."""
        from portolan_cli.config import get_setting

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        (catalog_root / ".portolan").mkdir()
        (catalog_root / ".portolan" / "config.yaml").write_text(
            "workers: 2\ncollections:\n  demographics:\n    workers: 8\n"
        )

        # Without collection_path, uses legacy collections: section
        result = get_setting(
            "workers",
            catalog_path=catalog_root,
            collection="demographics",
        )

        assert result == 8
