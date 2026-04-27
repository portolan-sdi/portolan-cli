"""Tests for portolan_cli.config module.

Tests the configuration system including:
- Loading/saving config from .portolan/config.yaml
- Setting precedence (CLI > env > collection config > catalog config > default)
- Collection-level config overrides
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock

import pytest

if TYPE_CHECKING:
    pass


class TestLoadConfig:
    """Tests for load_config function."""

    @pytest.mark.unit
    def test_load_config_returns_empty_dict_when_file_missing(self, tmp_path: Path) -> None:
        """load_config should return empty dict if config file doesn't exist."""
        from portolan_cli.config import load_config

        result = load_config(tmp_path)

        assert result == {}

    @pytest.mark.unit
    def test_load_config_reads_yaml_file(self, tmp_path: Path) -> None:
        """load_config should parse .portolan/config.yaml."""
        from portolan_cli.config import load_config

        # Create config file
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("remote: s3://my-bucket/catalog\naws_profile: dev\n")

        result = load_config(tmp_path)

        assert result == {"remote": "s3://my-bucket/catalog", "aws_profile": "dev"}

    @pytest.mark.unit
    def test_load_config_handles_empty_file(self, tmp_path: Path) -> None:
        """load_config should return empty dict for empty config file."""
        from portolan_cli.config import load_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text("")

        result = load_config(tmp_path)

        assert result == {}

    @pytest.mark.unit
    def test_load_config_includes_collections_section(self, tmp_path: Path) -> None:
        """load_config should include collections config."""
        from portolan_cli.config import load_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        config_file.write_text(
            "remote: s3://bucket/catalog\n"
            "collections:\n"
            "  demographics:\n"
            "    remote: s3://public/demographics\n"
        )

        result = load_config(tmp_path)

        assert result["remote"] == "s3://bucket/catalog"
        assert result["collections"]["demographics"]["remote"] == "s3://public/demographics"

    @pytest.mark.unit
    def test_load_config_raises_on_malformed_yaml(self, tmp_path: Path) -> None:
        """load_config should raise ConfigParseError for malformed YAML."""
        from portolan_cli.config import load_config
        from portolan_cli.errors import ConfigParseError

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # Invalid YAML: unquoted colon in value
        config_file.write_text("remote: s3://bucket\n  invalid: indentation: here\n")

        with pytest.raises(ConfigParseError) as exc_info:
            load_config(tmp_path)

        assert "PRTLN-CFG001" in str(exc_info.value)

    @pytest.mark.unit
    def test_load_config_raises_on_non_dict_content(self, tmp_path: Path) -> None:
        """load_config should raise ConfigInvalidStructureError for non-dict YAML."""
        from portolan_cli.config import load_config
        from portolan_cli.errors import ConfigInvalidStructureError

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # YAML list instead of dict
        config_file.write_text("- item1\n- item2\n")

        with pytest.raises(ConfigInvalidStructureError) as exc_info:
            load_config(tmp_path)

        assert "PRTLN-CFG002" in str(exc_info.value)
        assert "mapping" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_load_config_raises_on_invalid_collections_structure(self, tmp_path: Path) -> None:
        """load_config should raise ConfigInvalidStructureError if collections is not a dict."""
        from portolan_cli.config import load_config
        from portolan_cli.errors import ConfigInvalidStructureError

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        config_file = portolan_dir / "config.yaml"
        # collections is a string, not a dict
        config_file.write_text("remote: s3://bucket/\ncollections: not_a_dict\n")

        with pytest.raises(ConfigInvalidStructureError) as exc_info:
            load_config(tmp_path)

        assert "collections" in str(exc_info.value).lower()


class TestSaveConfig:
    """Tests for save_config function."""

    @pytest.mark.unit
    def test_save_config_creates_yaml_file(self, tmp_path: Path) -> None:
        """save_config should create .portolan/config.yaml."""
        from portolan_cli.config import save_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        save_config(tmp_path, {"remote": "s3://bucket/", "aws_profile": "prod"})

        config_file = portolan_dir / "config.yaml"
        assert config_file.exists()
        content = config_file.read_text()
        assert "remote:" in content
        assert "s3://bucket/" in content
        assert "aws_profile:" in content
        assert "prod" in content

    @pytest.mark.unit
    def test_save_config_creates_portolan_dir_if_missing(self, tmp_path: Path) -> None:
        """save_config should create .portolan directory if it doesn't exist."""
        from portolan_cli.config import save_config

        save_config(tmp_path, {"remote": "s3://bucket/"})

        assert (tmp_path / ".portolan").exists()
        assert (tmp_path / ".portolan" / "config.yaml").exists()

    @pytest.mark.unit
    def test_save_config_preserves_collections_section(self, tmp_path: Path) -> None:
        """save_config should preserve collections config."""
        from portolan_cli.config import save_config

        config = {
            "remote": "s3://bucket/",
            "collections": {
                "demographics": {"remote": "s3://other/"},
            },
        }

        save_config(tmp_path, config)

        content = (tmp_path / ".portolan" / "config.yaml").read_text()
        assert "collections:" in content
        assert "demographics:" in content


class TestGetSetting:
    """Tests for get_setting function with precedence."""

    @pytest.mark.unit
    def test_cli_value_takes_highest_precedence(self, tmp_path: Path) -> None:
        """CLI value should override all other sources."""
        from portolan_cli.config import get_setting, save_config

        # Set up catalog config
        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://config-value/"})

        # Set up env var
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-value/"}):
            result = get_setting(
                "remote",
                cli_value="s3://cli-value/",
                catalog_path=tmp_path,
            )

        assert result == "s3://cli-value/"

    @pytest.mark.unit
    def test_env_var_overrides_config_file(self, tmp_path: Path) -> None:
        """Environment variable should override catalog config."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://config-value/"})

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-value/"}):
            result = get_setting("remote", catalog_path=tmp_path)

        assert result == "s3://env-value/"

    @pytest.mark.unit
    def test_collection_config_overrides_catalog_config(self, tmp_path: Path) -> None:
        """Collection-level config should override catalog-level."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive key to test precedence without triggering security check
        save_config(
            tmp_path,
            {
                "backend": "stac",
                "collections": {
                    "demographics": {"backend": "iceberg"},
                },
            },
        )

        result = get_setting("backend", catalog_path=tmp_path, collection="demographics")

        assert result == "iceberg"

    @pytest.mark.unit
    def test_env_var_overrides_collection_config(self, tmp_path: Path) -> None:
        """Environment variable should override collection config."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog/",
                "collections": {
                    "demographics": {"remote": "s3://collection/"},
                },
            },
        )

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env/"}):
            result = get_setting("remote", catalog_path=tmp_path, collection="demographics")

        assert result == "s3://env/"

    @pytest.mark.unit
    def test_catalog_config_used_when_no_override(self, tmp_path: Path) -> None:
        """Catalog config should be used when no CLI or env override."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive key to test precedence without triggering security check
        save_config(tmp_path, {"backend": "iceberg"})

        result = get_setting("backend", catalog_path=tmp_path)

        assert result == "iceberg"

    @pytest.mark.unit
    def test_returns_none_when_setting_not_found(self, tmp_path: Path) -> None:
        """get_setting should return None when setting not found anywhere."""
        from portolan_cli.config import get_setting

        (tmp_path / ".portolan").mkdir()

        result = get_setting("nonexistent_key", catalog_path=tmp_path)

        assert result is None

    @pytest.mark.unit
    def test_env_var_naming_convention(self, tmp_path: Path) -> None:
        """Environment variables should use PORTOLAN_<UPPER_KEY> naming."""
        from portolan_cli.config import get_setting

        # aws_profile -> PORTOLAN_AWS_PROFILE
        with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "test-profile"}):
            result = get_setting("aws_profile", catalog_path=tmp_path)

        assert result == "test-profile"

    @pytest.mark.unit
    def test_env_var_handles_hyphenated_keys(self, tmp_path: Path) -> None:
        """Environment variables should normalize hyphens to underscores."""
        from portolan_cli.config import get_setting

        # max-depth -> PORTOLAN_MAX_DEPTH
        with mock.patch.dict(os.environ, {"PORTOLAN_MAX_DEPTH": "10"}):
            result = get_setting("max-depth", catalog_path=tmp_path)

        assert result == "10"

    @pytest.mark.unit
    def test_empty_env_var_does_not_override(self, tmp_path: Path) -> None:
        """Empty environment variable should not override config file value."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive key to test empty env var behavior
        save_config(tmp_path, {"backend": "iceberg"})

        # Empty env var should be ignored
        with mock.patch.dict(os.environ, {"PORTOLAN_BACKEND": ""}):
            result = get_setting("backend", catalog_path=tmp_path)

        assert result == "iceberg"

    @pytest.mark.unit
    def test_works_without_catalog_path(self) -> None:
        """get_setting should work with just env vars if no catalog_path."""
        from portolan_cli.config import get_setting

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-only/"}):
            result = get_setting("remote")

        assert result == "s3://env-only/"

    @pytest.mark.unit
    def test_returns_none_without_catalog_path_or_env(self) -> None:
        """get_setting should return None when no catalog_path and no env var."""
        from portolan_cli.config import get_setting

        # Ensure no relevant env vars are set
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": ""}, clear=False):
            # Clear the env var if it exists
            if "PORTOLAN_REMOTE" in os.environ:
                del os.environ["PORTOLAN_REMOTE"]
            result = get_setting("remote")

        assert result is None

    @pytest.mark.unit
    def test_collection_falls_back_to_catalog_config(self, tmp_path: Path) -> None:
        """Collection should inherit catalog config for unset keys."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive keys to test fallback behavior
        save_config(
            tmp_path,
            {
                "backend": "stac",
                "statistics.enabled": True,
                "collections": {
                    "demographics": {"backend": "iceberg"},
                    # statistics.enabled not set for collection
                },
            },
        )

        result = get_setting("statistics.enabled", catalog_path=tmp_path, collection="demographics")

        # Should fall back to catalog-level statistics.enabled
        assert result is True

    @pytest.mark.unit
    def test_dotted_key_resolves_nested_yaml(self, tmp_path: Path) -> None:
        """Dotted keys like 'pmtiles.src_crs' should traverse nested YAML."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"src_crs": "EPSG:3035"}})

        coll_path = tmp_path / "my_collection"
        coll_path.mkdir()

        result = get_setting(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="my_collection",
            collection_path=coll_path,
        )

        assert result == "EPSG:3035"

    @pytest.mark.unit
    def test_dotted_key_resolves_deeply_nested_yaml(self, tmp_path: Path) -> None:
        """Dotted keys should work at arbitrary depth."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"a": {"b": {"c": "deep_value"}}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = get_setting(
            "a.b.c",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result == "deep_value"

    @pytest.mark.unit
    def test_dotted_key_returns_none_for_missing_path(self, tmp_path: Path) -> None:
        """Dotted key should return None if any part of path is missing."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"min_zoom": 4}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = get_setting(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is None

    @pytest.mark.unit
    def test_dotted_key_returns_none_for_nonexistent_parent(self, tmp_path: Path) -> None:
        """Dotted key should return None if parent key doesn't exist."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://bucket/"})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = get_setting(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is None

    @pytest.mark.unit
    def test_dotted_key_returns_false_boolean(self, tmp_path: Path) -> None:
        """Dotted key should correctly return False (not None) for disabled settings."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"enabled": False}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = get_setting(
            "pmtiles.enabled",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is False

    @pytest.mark.unit
    def test_dotted_key_returns_zero_integer(self, tmp_path: Path) -> None:
        """Dotted key should correctly return 0 (not None) for zero values."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"min_zoom": 0}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = get_setting(
            "pmtiles.min_zoom",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result == 0


class TestSetSetting:
    """Tests for set_setting function."""

    @pytest.mark.unit
    def test_set_catalog_level_setting(self, tmp_path: Path) -> None:
        """set_setting should update catalog-level config."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "backend", "iceberg")

        config = load_config(tmp_path)
        assert config["backend"] == "iceberg"

    @pytest.mark.unit
    def test_set_collection_level_setting(self, tmp_path: Path) -> None:
        """set_setting with collection should update collection config."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "backend", "stac", collection="demographics")

        config = load_config(tmp_path)
        assert config["collections"]["demographics"]["backend"] == "stac"

    @pytest.mark.unit
    def test_set_preserves_existing_config(self, tmp_path: Path) -> None:
        """set_setting should preserve other existing settings."""
        from portolan_cli.config import load_config, save_config, set_setting

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"backend": "stac", "custom_key": "old"})

        set_setting(tmp_path, "custom_key", "new")

        config = load_config(tmp_path)
        assert config["backend"] == "stac"
        assert config["custom_key"] == "new"

    @pytest.mark.unit
    def test_set_creates_collections_section_if_missing(self, tmp_path: Path) -> None:
        """set_setting should create collections section if needed."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "backend", "stac", collection="newcollection")

        config = load_config(tmp_path)
        assert "collections" in config
        assert "newcollection" in config["collections"]


class TestUnsetSetting:
    """Tests for unset_setting function."""

    @pytest.mark.unit
    def test_unset_removes_catalog_setting(self, tmp_path: Path) -> None:
        """unset_setting should remove setting from catalog config."""
        from portolan_cli.config import load_config, save_config, unset_setting

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://bucket/", "aws_profile": "prod"})

        result = unset_setting(tmp_path, "aws_profile")

        assert result is True
        config = load_config(tmp_path)
        assert "aws_profile" not in config
        assert "remote" in config  # Other settings preserved

    @pytest.mark.unit
    def test_unset_returns_false_for_missing_key(self, tmp_path: Path) -> None:
        """unset_setting should return False if key doesn't exist."""
        from portolan_cli.config import save_config, unset_setting

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://bucket/"})

        result = unset_setting(tmp_path, "nonexistent")

        assert result is False

    @pytest.mark.unit
    def test_unset_collection_setting(self, tmp_path: Path) -> None:
        """unset_setting should remove collection-level setting."""
        from portolan_cli.config import load_config, save_config, unset_setting

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog/",
                "collections": {
                    "demographics": {"remote": "s3://collection/", "aws_profile": "col"},
                },
            },
        )

        result = unset_setting(tmp_path, "aws_profile", collection="demographics")

        assert result is True
        config = load_config(tmp_path)
        # remote should still exist in collection
        assert config["collections"]["demographics"]["remote"] == "s3://collection/"
        # aws_profile should be removed
        assert "aws_profile" not in config["collections"]["demographics"]

    @pytest.mark.unit
    def test_unset_nonexistent_collection_key(self, tmp_path: Path) -> None:
        """unset_setting should return False for nonexistent key in collection."""
        from portolan_cli.config import save_config, unset_setting

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog/",
                "collections": {
                    "demographics": {"remote": "s3://collection/"},
                },
            },
        )

        # Try to unset a key that doesn't exist in the collection
        result = unset_setting(tmp_path, "aws_profile", collection="demographics")

        assert result is False


class TestConfigConstants:
    """Tests for config-related constants and defaults."""

    @pytest.mark.unit
    def test_sensitive_settings_includes_remote(self) -> None:
        """SENSITIVE_SETTINGS should include 'remote' (Issue #356)."""
        from portolan_cli.config import SENSITIVE_SETTINGS

        assert "remote" in SENSITIVE_SETTINGS

    @pytest.mark.unit
    def test_sensitive_settings_includes_profile(self) -> None:
        """SENSITIVE_SETTINGS should include 'profile' and 'aws_profile' (Issue #356)."""
        from portolan_cli.config import SENSITIVE_SETTINGS

        assert "profile" in SENSITIVE_SETTINGS
        assert "aws_profile" in SENSITIVE_SETTINGS

    @pytest.mark.unit
    def test_sensitive_settings_includes_region(self) -> None:
        """SENSITIVE_SETTINGS should include 'region' (Issue #356)."""
        from portolan_cli.config import SENSITIVE_SETTINGS

        assert "region" in SENSITIVE_SETTINGS

    @pytest.mark.unit
    def test_known_settings_includes_ignored_files(self) -> None:
        """KNOWN_SETTINGS should include 'ignored_files'."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "ignored_files" in KNOWN_SETTINGS

    @pytest.mark.unit
    def test_sensitive_settings_not_in_known_settings(self) -> None:
        """Sensitive settings should not be in KNOWN_SETTINGS (Issue #356)."""
        from portolan_cli.config import KNOWN_SETTINGS, SENSITIVE_SETTINGS

        for setting in SENSITIVE_SETTINGS:
            assert setting not in KNOWN_SETTINGS, f"{setting} should not be in KNOWN_SETTINGS"


class TestGetIgnoredFiles:
    """Tests for get_ignored_files function."""

    @pytest.mark.unit
    def test_returns_default_patterns_when_no_config(self, tmp_path: Path) -> None:
        """get_ignored_files should return DEFAULT_IGNORED_FILES when no config exists."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES, get_ignored_files

        result = get_ignored_files(tmp_path)

        assert result == DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_default_patterns_include_ds_store(self) -> None:
        """DEFAULT_IGNORED_FILES should include .DS_Store."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES

        assert ".DS_Store" in DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_default_patterns_include_thumbs_db(self) -> None:
        """DEFAULT_IGNORED_FILES should include Thumbs.db."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES

        assert "Thumbs.db" in DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_default_patterns_include_tmp_glob(self) -> None:
        """DEFAULT_IGNORED_FILES should include *.tmp glob pattern."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES

        assert "*.tmp" in DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_default_patterns_include_git_glob(self) -> None:
        """DEFAULT_IGNORED_FILES should include .git* glob pattern."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES

        assert ".git*" in DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_returns_list_type(self, tmp_path: Path) -> None:
        """get_ignored_files should always return a list."""
        from portolan_cli.config import get_ignored_files

        result = get_ignored_files(tmp_path)

        assert isinstance(result, list)

    @pytest.mark.unit
    def test_loads_ignored_files_from_config(self, tmp_path: Path) -> None:
        """get_ignored_files should load patterns from .portolan/config.yaml."""
        from portolan_cli.config import get_ignored_files

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("ignored_files:\n  - '*.bak'\n  - '*.swp'\n")

        result = get_ignored_files(tmp_path)

        assert result == ["*.bak", "*.swp"]

    @pytest.mark.unit
    def test_config_ignored_files_overrides_defaults(self, tmp_path: Path) -> None:
        """Config ignored_files replaces (not merges with) defaults."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES, get_ignored_files

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("ignored_files:\n  - '*.custom'\n")

        result = get_ignored_files(tmp_path)

        # Should be exactly the config value, not merged with defaults
        assert result == ["*.custom"]
        assert result != DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_empty_ignored_files_list_is_valid(self, tmp_path: Path) -> None:
        """An explicit empty list in config means no files are ignored."""
        from portolan_cli.config import get_ignored_files

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("ignored_files: []\n")

        result = get_ignored_files(tmp_path)

        assert result == []

    @pytest.mark.unit
    def test_raises_on_non_list_ignored_files(self, tmp_path: Path) -> None:
        """get_ignored_files should raise ConfigInvalidStructureError if ignored_files is not a list."""
        from portolan_cli.config import get_ignored_files
        from portolan_cli.errors import ConfigInvalidStructureError

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("ignored_files: not_a_list\n")

        with pytest.raises(ConfigInvalidStructureError) as exc_info:
            get_ignored_files(tmp_path)

        assert "ignored_files" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_raises_on_non_string_items_in_ignored_files(self, tmp_path: Path) -> None:
        """get_ignored_files should raise ConfigInvalidStructureError if items are not strings."""
        from portolan_cli.config import get_ignored_files
        from portolan_cli.errors import ConfigInvalidStructureError

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("ignored_files:\n  - 123\n  - valid_pattern\n")

        with pytest.raises(ConfigInvalidStructureError) as exc_info:
            get_ignored_files(tmp_path)

        assert "ignored_files" in str(exc_info.value).lower()

    @pytest.mark.unit
    def test_works_without_catalog_path(self) -> None:
        """get_ignored_files returns defaults when no catalog_path provided."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES, get_ignored_files

        result = get_ignored_files(None)

        assert result == DEFAULT_IGNORED_FILES

    @pytest.mark.unit
    def test_load_config_accepts_ignored_files_list(self, tmp_path: Path) -> None:
        """load_config should parse ignored_files as a list without error."""
        from portolan_cli.config import load_config

        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(
            "remote: s3://bucket/\nignored_files:\n  - '.DS_Store'\n  - '*.tmp'\n"
        )

        result = load_config(tmp_path)

        assert result["ignored_files"] == [".DS_Store", "*.tmp"]
        assert result["remote"] == "s3://bucket/"


class TestConfigFilePath:
    """Tests for config file path handling."""

    @pytest.mark.unit
    def test_config_file_location(self, tmp_path: Path) -> None:
        """Config file should be at .portolan/config.yaml."""
        from portolan_cli.config import get_config_path

        path = get_config_path(tmp_path)

        assert path == tmp_path / ".portolan" / "config.yaml"


class TestSensitiveSettings:
    """Tests for sensitive settings protection (Issue #356)."""

    @pytest.mark.unit
    def test_set_setting_rejects_remote(self, tmp_path: Path) -> None:
        """set_setting should reject 'remote' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ValueError) as exc_info:
            set_setting(tmp_path, "remote", "s3://bucket/")

        assert "remote" in str(exc_info.value)
        assert "PORTOLAN_REMOTE" in str(exc_info.value)
        assert ".env" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_profile(self, tmp_path: Path) -> None:
        """set_setting should reject 'profile' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ValueError) as exc_info:
            set_setting(tmp_path, "profile", "myprofile")

        assert "profile" in str(exc_info.value)
        assert "PORTOLAN_PROFILE" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_aws_profile(self, tmp_path: Path) -> None:
        """set_setting should reject 'aws_profile' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ValueError) as exc_info:
            set_setting(tmp_path, "aws_profile", "myprofile")

        assert "aws_profile" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_region(self, tmp_path: Path) -> None:
        """set_setting should reject 'region' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ValueError) as exc_info:
            set_setting(tmp_path, "region", "us-west-2")

        assert "region" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_allows_non_sensitive_keys(self, tmp_path: Path) -> None:
        """set_setting should allow non-sensitive keys like 'ignored_files'."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "ignored_files", ["*.bak"])

        config = load_config(tmp_path)
        assert config["ignored_files"] == ["*.bak"]


class TestDotenvLoading:
    """Tests for .env file loading (Issue #356)."""

    @pytest.mark.unit
    def test_load_dotenv_from_catalog_loads_env_file(self, tmp_path: Path) -> None:
        """load_dotenv_from_catalog should load .env file from catalog root."""
        from portolan_cli.config import load_dotenv_from_catalog

        # Create .env file
        env_file = tmp_path / ".env"
        env_file.write_text("PORTOLAN_REMOTE=s3://from-dotenv/\n")

        # Ensure PORTOLAN_REMOTE is not set before test
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PORTOLAN_REMOTE", None)
            result = load_dotenv_from_catalog(tmp_path)

            assert result is True
            assert os.environ.get("PORTOLAN_REMOTE") == "s3://from-dotenv/"

    @pytest.mark.unit
    def test_load_dotenv_returns_false_when_no_env_file(self, tmp_path: Path) -> None:
        """load_dotenv_from_catalog should return False when .env doesn't exist."""
        from portolan_cli.config import load_dotenv_from_catalog

        result = load_dotenv_from_catalog(tmp_path)

        assert result is False

    @pytest.mark.unit
    def test_load_dotenv_returns_false_for_none_path(self) -> None:
        """load_dotenv_from_catalog should return False when path is None."""
        from portolan_cli.config import load_dotenv_from_catalog

        result = load_dotenv_from_catalog(None)

        assert result is False

    @pytest.mark.unit
    def test_load_dotenv_does_not_override_existing_env_vars(self, tmp_path: Path) -> None:
        """load_dotenv_from_catalog should not override existing env vars."""
        from portolan_cli.config import load_dotenv_from_catalog

        env_file = tmp_path / ".env"
        env_file.write_text("PORTOLAN_REMOTE=s3://from-dotenv/\n")

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://existing/"}):
            load_dotenv_from_catalog(tmp_path)

            # Should keep existing value, not override
            assert os.environ.get("PORTOLAN_REMOTE") == "s3://existing/"

    @pytest.mark.unit
    def test_get_setting_reads_from_dotenv_via_env_var(self, tmp_path: Path) -> None:
        """get_setting should read values loaded from .env via env vars."""
        from portolan_cli.config import get_setting, load_dotenv_from_catalog

        (tmp_path / ".portolan").mkdir()
        env_file = tmp_path / ".env"
        env_file.write_text("PORTOLAN_REMOTE=s3://from-dotenv/\n")

        with mock.patch.dict(os.environ, {}, clear=True):
            load_dotenv_from_catalog(tmp_path)
            result = get_setting("remote", catalog_path=tmp_path)

            assert result == "s3://from-dotenv/"


class TestEnvFileIgnored:
    """Tests that .env files are excluded from tracking (Issue #356)."""

    @pytest.mark.unit
    def test_default_ignored_files_includes_env(self) -> None:
        """DEFAULT_IGNORED_FILES should include .env patterns."""
        from portolan_cli.config import DEFAULT_IGNORED_FILES

        assert ".env" in DEFAULT_IGNORED_FILES
        assert ".env.*" in DEFAULT_IGNORED_FILES
        assert ".env.local" in DEFAULT_IGNORED_FILES


class TestSensitiveSettingsMigration:
    """Tests for check_sensitive_settings_in_config (Issue #356 migration)."""

    @pytest.mark.unit
    def test_detects_sensitive_settings_in_config(self, tmp_path: Path) -> None:
        """check_sensitive_settings_in_config should detect sensitive settings."""
        from portolan_cli.config import check_sensitive_settings_in_config, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://bucket/", "profile": "prod"})

        result = check_sensitive_settings_in_config(tmp_path)

        assert "remote" in result
        assert "profile" in result

    @pytest.mark.unit
    def test_returns_empty_when_no_sensitive_settings(self, tmp_path: Path) -> None:
        """check_sensitive_settings_in_config should return empty list when clean."""
        from portolan_cli.config import check_sensitive_settings_in_config, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"backend": "stac", "statistics.enabled": True})

        result = check_sensitive_settings_in_config(tmp_path)

        assert result == []

    @pytest.mark.unit
    def test_returns_empty_for_empty_config(self, tmp_path: Path) -> None:
        """check_sensitive_settings_in_config should handle empty config."""
        from portolan_cli.config import check_sensitive_settings_in_config

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("")

        result = check_sensitive_settings_in_config(tmp_path)

        assert result == []

    @pytest.mark.unit
    def test_detects_nested_collection_sensitive_settings(self, tmp_path: Path) -> None:
        """check_sensitive_settings_in_config should detect collection-level sensitive settings."""
        from portolan_cli.config import check_sensitive_settings_in_config, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "backend": "stac",
                "collections": {
                    "demo": {"remote": "s3://demo-bucket/", "aws_profile": "demo-profile"},
                    "prod": {"profile": "prod-profile"},
                },
            },
        )

        result = check_sensitive_settings_in_config(tmp_path)

        assert "collections.demo.remote" in result
        assert "collections.demo.aws_profile" in result
        assert "collections.prod.profile" in result


class TestListSettings:
    """Tests for list_settings function."""

    @pytest.mark.unit
    def test_list_settings_basic(self, tmp_path: Path) -> None:
        """list_settings should return all configured settings."""
        from portolan_cli.config import list_settings, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive keys to test list functionality
        save_config(tmp_path, {"backend": "iceberg", "statistics.enabled": True})

        result = list_settings(catalog_path=tmp_path)

        assert "backend" in result
        assert result["backend"]["value"] == "iceberg"
        assert result["backend"]["source"] == "catalog"

    @pytest.mark.unit
    def test_list_settings_with_collection(self, tmp_path: Path) -> None:
        """list_settings should include collection-level settings."""
        from portolan_cli.config import list_settings, save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive keys to test collection config
        save_config(
            tmp_path,
            {
                "backend": "stac",
                "collections": {
                    "demographics": {"backend": "iceberg", "statistics.enabled": False},
                },
            },
        )

        result = list_settings(catalog_path=tmp_path, collection="demographics")

        assert "backend" in result
        assert result["backend"]["value"] == "iceberg"
        assert result["backend"]["source"] == "collection"
        assert "statistics.enabled" in result
        assert result["statistics.enabled"]["value"] is False
        assert result["statistics.enabled"]["source"] == "collection"

    @pytest.mark.unit
    def test_list_settings_without_catalog_path(self) -> None:
        """list_settings should work with just env vars if no catalog_path."""
        from portolan_cli.config import list_settings

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env/"}):
            result = list_settings()

        assert "remote" in result
        assert result["remote"]["value"] == "s3://env/"
        assert result["remote"]["source"] == "env"


class TestGetSettingSource:
    """Tests for get_setting_source function."""

    @pytest.mark.unit
    def test_source_returns_default_without_catalog_path(self) -> None:
        """get_setting_source should return 'default' when no catalog_path."""
        from portolan_cli.config import get_setting_source

        result = get_setting_source("remote", catalog_path=None, collection=None)

        assert result == "default"

    @pytest.mark.unit
    def test_source_returns_env_when_env_var_set(self) -> None:
        """get_setting_source should return 'env' when env var is set."""
        from portolan_cli.config import get_setting_source

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env/"}):
            result = get_setting_source("remote", catalog_path=None, collection=None)

        assert result == "env"

    @pytest.mark.unit
    def test_source_returns_collection_for_collection_setting(self, tmp_path: Path) -> None:
        """get_setting_source should return 'collection' for collection-level."""
        from portolan_cli.config import get_setting_source, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog/",
                "collections": {"demo": {"remote": "s3://collection/"}},
            },
        )

        result = get_setting_source("remote", catalog_path=tmp_path, collection="demo")

        assert result == "collection"


class TestPMTilesConfigSettings:
    """Tests for PMTiles configuration settings."""

    @pytest.mark.unit
    def test_pmtiles_settings_are_known(self) -> None:
        """All PMTiles config keys are in KNOWN_SETTINGS."""
        from portolan_cli.config import KNOWN_SETTINGS

        pmtiles_keys = [
            "pmtiles.enabled",
            "pmtiles.min_zoom",
            "pmtiles.max_zoom",
            "pmtiles.layer",
            "pmtiles.bbox",
            "pmtiles.where",
            "pmtiles.include_cols",
            "pmtiles.precision",
            "pmtiles.attribution",
            "pmtiles.src_crs",
        ]

        for key in pmtiles_keys:
            assert key in KNOWN_SETTINGS, f"Missing config key: {key}"

    @pytest.mark.unit
    def test_pmtiles_default_values(self) -> None:
        """PMTiles settings have correct defaults."""
        from portolan_cli.config import DEFAULT_SETTINGS

        assert DEFAULT_SETTINGS["pmtiles.enabled"] is False
        assert DEFAULT_SETTINGS["pmtiles.min_zoom"] is None
        assert DEFAULT_SETTINGS["pmtiles.max_zoom"] is None
        assert DEFAULT_SETTINGS["pmtiles.precision"] == 6
        assert DEFAULT_SETTINGS["pmtiles.layer"] is None
        assert DEFAULT_SETTINGS["pmtiles.bbox"] is None
        assert DEFAULT_SETTINGS["pmtiles.where"] is None
        assert DEFAULT_SETTINGS["pmtiles.include_cols"] is None
        assert DEFAULT_SETTINGS["pmtiles.attribution"] is None
        assert DEFAULT_SETTINGS["pmtiles.src_crs"] is None

    @pytest.mark.unit
    def test_pmtiles_config_loads_from_yaml(self, tmp_path: Path) -> None:
        """PMTiles settings load correctly from config.yaml."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "pmtiles.enabled": True,
                "pmtiles.min_zoom": 2,
                "pmtiles.max_zoom": 12,
                "pmtiles.precision": 5,
                "pmtiles.layer": "boundaries",
                "pmtiles.bbox": "-122.5,37.5,-122.0,38.0",
                "pmtiles.where": "population > 1000",
                "pmtiles.include_cols": "name,geometry",
                "pmtiles.attribution": "© Test",
                "pmtiles.src_crs": "EPSG:3857",
            },
        )

        assert get_setting("pmtiles.enabled", catalog_path=tmp_path) is True
        assert get_setting("pmtiles.min_zoom", catalog_path=tmp_path) == 2
        assert get_setting("pmtiles.max_zoom", catalog_path=tmp_path) == 12
        assert get_setting("pmtiles.precision", catalog_path=tmp_path) == 5
        assert get_setting("pmtiles.layer", catalog_path=tmp_path) == "boundaries"
        assert get_setting("pmtiles.bbox", catalog_path=tmp_path) == "-122.5,37.5,-122.0,38.0"
        assert get_setting("pmtiles.where", catalog_path=tmp_path) == "population > 1000"
        assert get_setting("pmtiles.include_cols", catalog_path=tmp_path) == "name,geometry"
        assert get_setting("pmtiles.attribution", catalog_path=tmp_path) == "© Test"
        assert get_setting("pmtiles.src_crs", catalog_path=tmp_path) == "EPSG:3857"

    @pytest.mark.unit
    def test_pmtiles_nested_yaml_structure(self, tmp_path: Path) -> None:
        """PMTiles settings work with nested YAML (how users actually write config)."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        # This is how users write config.yaml (nested, not flat keys)
        save_config(
            tmp_path,
            {
                "pmtiles": {
                    "enabled": True,
                    "src_crs": "EPSG:3035",
                    "min_zoom": 4,
                    "max_zoom": 14,
                }
            },
        )

        assert get_setting("pmtiles.enabled", catalog_path=tmp_path) is True
        assert get_setting("pmtiles.src_crs", catalog_path=tmp_path) == "EPSG:3035"
        assert get_setting("pmtiles.min_zoom", catalog_path=tmp_path) == 4
        assert get_setting("pmtiles.max_zoom", catalog_path=tmp_path) == 14
