"""Tests for portolan_cli.config module.

Tests the configuration system including:
- Loading/saving config from .portolan/config.yaml
- Setting precedence (CLI > env > collection config > catalog config > default)
- Collection-level config overrides
"""

from __future__ import annotations

import os
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from portolan_cli import trace
from portolan_cli.errors import ConfigError
from tests.conftest import cleared_environ


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


Resolver = Callable[..., Any]


def _setting_through_state(
    key: str,
    cli_value: Any | None = None,
    catalog_path: Path | None = None,
    collection: str | None = None,
    collection_path: Path | None = None,
) -> Any | None:
    """Call ``RunState.setting`` with the arguments of ``get_setting``.

    A legacy ``collection`` name becomes the directory of that collection. With
    no catalog, the state opens on an empty directory, which holds no config.
    """
    from portolan_cli.run_state import RunState

    with tempfile.TemporaryDirectory() as empty:
        root = catalog_path if catalog_path is not None else Path(empty)
        if collection_path is None and collection is not None:
            collection_path = root / collection
        return RunState.open(root).setting(key, collection_dir=collection_path, cli_value=cli_value)


class TestGetSetting:
    """Tests for get_setting function with precedence.

    Each test runs twice: through ``config.get_setting`` and through
    ``RunState.setting``, which must keep the same precedence.
    """

    @pytest.fixture(params=["get_setting", "run_state"])
    def resolve(self, request: pytest.FixtureRequest) -> Resolver:
        from portolan_cli.config import get_setting

        return get_setting if request.param == "get_setting" else _setting_through_state

    @pytest.mark.unit
    def test_cli_value_takes_highest_precedence(self, tmp_path: Path, resolve: Resolver) -> None:
        """CLI value should override all other sources."""
        from portolan_cli.config import save_config

        # Set up catalog config
        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://config-value/"})

        # Set up env var
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-value/"}):
            result = resolve(
                "remote",
                cli_value="s3://cli-value/",
                catalog_path=tmp_path,
            )

        assert result == "s3://cli-value/"

    @pytest.mark.unit
    def test_env_var_overrides_config_file(self, tmp_path: Path, resolve: Resolver) -> None:
        """Environment variable should override catalog config."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://config-value/"})

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-value/"}):
            result = resolve("remote", catalog_path=tmp_path)

        assert result == "s3://env-value/"

    @pytest.mark.unit
    def test_collection_config_overrides_catalog_config(
        self, tmp_path: Path, resolve: Resolver
    ) -> None:
        """Collection-level config should override catalog-level."""
        from portolan_cli.config import save_config

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

        result = resolve("backend", catalog_path=tmp_path, collection="demographics")

        assert result == "iceberg"

    @pytest.mark.unit
    def test_env_var_overrides_collection_config(self, tmp_path: Path, resolve: Resolver) -> None:
        """Environment variable should override collection config."""
        from portolan_cli.config import save_config

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
            result = resolve("remote", catalog_path=tmp_path, collection="demographics")

        assert result == "s3://env/"

    @pytest.mark.unit
    def test_catalog_config_used_when_no_override(self, tmp_path: Path, resolve: Resolver) -> None:
        """Catalog config should be used when no CLI or env override."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive key to test precedence without triggering security check
        save_config(tmp_path, {"backend": "iceberg"})

        result = resolve("backend", catalog_path=tmp_path)

        assert result == "iceberg"

    @pytest.mark.unit
    def test_returns_none_when_setting_not_found(self, tmp_path: Path, resolve: Resolver) -> None:
        """get_setting should return None when setting not found anywhere."""
        (tmp_path / ".portolan").mkdir()

        result = resolve("nonexistent_key", catalog_path=tmp_path)

        assert result is None

    @pytest.mark.unit
    def test_env_var_naming_convention(self, tmp_path: Path, resolve: Resolver) -> None:
        """Environment variables should use PORTOLAN_<UPPER_KEY> naming."""
        # aws_profile -> PORTOLAN_AWS_PROFILE
        with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "test-profile"}):
            result = resolve("aws_profile", catalog_path=tmp_path)

        assert result == "test-profile"

    @pytest.mark.unit
    def test_env_var_handles_hyphenated_keys(self, tmp_path: Path, resolve: Resolver) -> None:
        """Environment variables should normalize hyphens to underscores."""
        # max-depth -> PORTOLAN_MAX_DEPTH
        with mock.patch.dict(os.environ, {"PORTOLAN_MAX_DEPTH": "10"}):
            result = resolve("max-depth", catalog_path=tmp_path)

        assert result == "10"

    @pytest.mark.unit
    def test_empty_env_var_does_not_override(self, tmp_path: Path, resolve: Resolver) -> None:
        """Empty environment variable should not override config file value."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        # Use non-sensitive key to test empty env var behavior
        save_config(tmp_path, {"backend": "iceberg"})

        # Empty env var should be ignored
        with mock.patch.dict(os.environ, {"PORTOLAN_BACKEND": ""}):
            result = resolve("backend", catalog_path=tmp_path)

        assert result == "iceberg"

    @pytest.mark.unit
    def test_works_without_catalog_path(self, resolve: Resolver) -> None:
        """get_setting should work with just env vars if no catalog_path."""
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-only/"}):
            result = resolve("remote")

        assert result == "s3://env-only/"

    @pytest.mark.unit
    def test_returns_none_without_catalog_path_or_env(self, resolve: Resolver) -> None:
        """get_setting should return None when no catalog_path and no env var."""
        # Ensure no relevant env vars are set
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": ""}, clear=False):
            # Clear the env var if it exists
            if "PORTOLAN_REMOTE" in os.environ:
                del os.environ["PORTOLAN_REMOTE"]
            result = resolve("remote")

        assert result is None

    @pytest.mark.unit
    def test_collection_falls_back_to_catalog_config(
        self, tmp_path: Path, resolve: Resolver
    ) -> None:
        """Collection should inherit catalog config for unset keys."""
        from portolan_cli.config import save_config

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

        result = resolve("statistics.enabled", catalog_path=tmp_path, collection="demographics")

        # Should fall back to catalog-level statistics.enabled
        assert result is True

    @pytest.mark.unit
    def test_dotted_key_resolves_nested_yaml(self, tmp_path: Path, resolve: Resolver) -> None:
        """Dotted keys like 'pmtiles.src_crs' should traverse nested YAML."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"src_crs": "EPSG:3035"}})

        coll_path = tmp_path / "my_collection"
        coll_path.mkdir()

        result = resolve(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="my_collection",
            collection_path=coll_path,
        )

        assert result == "EPSG:3035"

    @pytest.mark.unit
    def test_dotted_key_resolves_deeply_nested_yaml(
        self, tmp_path: Path, resolve: Resolver
    ) -> None:
        """Dotted keys should work at arbitrary depth."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"a": {"b": {"c": "deep_value"}}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = resolve(
            "a.b.c",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result == "deep_value"

    @pytest.mark.unit
    def test_dotted_key_returns_none_for_missing_path(
        self, tmp_path: Path, resolve: Resolver
    ) -> None:
        """Dotted key should return None if any part of path is missing."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"min_zoom": 4}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = resolve(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is None

    @pytest.mark.unit
    def test_dotted_key_returns_none_for_nonexistent_parent(
        self, tmp_path: Path, resolve: Resolver
    ) -> None:
        """Dotted key should return None if parent key doesn't exist."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://bucket/"})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = resolve(
            "pmtiles.src_crs",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is None

    @pytest.mark.unit
    def test_dotted_key_returns_false_boolean(self, tmp_path: Path, resolve: Resolver) -> None:
        """Dotted key should correctly return False (not None) for disabled settings."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"enabled": False}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = resolve(
            "pmtiles.enabled",
            catalog_path=tmp_path,
            collection="coll",
            collection_path=coll_path,
        )

        assert result is False

    @pytest.mark.unit
    def test_dotted_key_returns_zero_integer(self, tmp_path: Path, resolve: Resolver) -> None:
        """Dotted key should correctly return 0 (not None) for zero values."""
        from portolan_cli.config import save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"pmtiles": {"min_zoom": 0}})

        coll_path = tmp_path / "coll"
        coll_path.mkdir()

        result = resolve(
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
        save_config(tmp_path, {"backend": "stac", "statistics": {"enabled": True}})

        set_setting(tmp_path, "statistics.enabled", False)

        config = load_config(tmp_path)
        assert config["backend"] == "stac"
        assert config["statistics"] == {"enabled": False}

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

        with pytest.raises(ConfigError) as exc_info:
            set_setting(tmp_path, "remote", "s3://bucket/")

        assert "remote" in str(exc_info.value)
        assert "PORTOLAN_REMOTE" in str(exc_info.value)
        assert ".env" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_profile(self, tmp_path: Path) -> None:
        """set_setting should reject 'profile' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ConfigError) as exc_info:
            set_setting(tmp_path, "profile", "myprofile")

        assert "profile" in str(exc_info.value)
        assert "PORTOLAN_PROFILE" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_aws_profile(self, tmp_path: Path) -> None:
        """set_setting should reject 'aws_profile' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ConfigError) as exc_info:
            set_setting(tmp_path, "aws_profile", "myprofile")

        assert "aws_profile" in str(exc_info.value)

    @pytest.mark.unit
    def test_set_setting_rejects_region(self, tmp_path: Path) -> None:
        """set_setting should reject 'region' as a sensitive setting."""
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()

        with pytest.raises(ConfigError) as exc_info:
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

        with cleared_environ():
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


class TestCoerceHelpers:
    """Tests for coerce_bool / coerce_int (env values arrive as strings, #620)."""

    @pytest.mark.unit
    def test_coerce_bool_truthy_strings(self) -> None:
        from portolan_cli.config import coerce_bool

        for truthy in ("true", "True", "1", "yes", "on"):
            assert coerce_bool(truthy) is True

    @pytest.mark.unit
    def test_coerce_bool_falsey_and_default(self) -> None:
        from portolan_cli.config import coerce_bool

        assert coerce_bool("false") is False
        assert coerce_bool("no") is False
        assert coerce_bool(None) is False
        assert coerce_bool(None, default=True) is True
        assert coerce_bool(True) is True

    @pytest.mark.unit
    def test_coerce_int_parses_and_falls_back(self) -> None:
        from portolan_cli.config import coerce_int

        assert coerce_int("42", default=0) == 42
        assert coerce_int(7, default=0) == 7
        assert coerce_int("not-an-int", default=100) == 100
        assert coerce_int(None, default=5) == 5


class TestResolvePushSettings:
    """Tests for resolve_push_settings precedence (CLI > env > config, #620)."""

    def _init_catalog(self, path: Path, config: str = "") -> None:
        portolan_dir = path / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text(config)
        (path / "catalog.json").write_text("{}")

    @pytest.mark.unit
    def test_cli_value_wins_and_profile_defaults(self, tmp_path: Path) -> None:
        from portolan_cli.config import resolve_push_settings

        self._init_catalog(tmp_path)
        with mock.patch.dict(os.environ, {}, clear=False):
            for key in ("PORTOLAN_REMOTE", "PORTOLAN_AWS_PROFILE", "PORTOLAN_REGION"):
                os.environ.pop(key, None)
            destination, profile, region = resolve_push_settings(
                "s3://cli-bucket/", None, tmp_path, None
            )

        assert destination == "s3://cli-bucket/"
        assert profile is None  # nothing configured → the AWS tools choose
        assert region is None

    @pytest.mark.unit
    def test_env_remote_used_when_no_cli(self, tmp_path: Path) -> None:
        from portolan_cli.config import resolve_push_settings

        self._init_catalog(tmp_path)
        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-bucket/"}):
            destination, profile, _ = resolve_push_settings(None, None, tmp_path, None)

        assert destination == "s3://env-bucket/"

    @pytest.mark.unit
    def test_cli_profile_overrides_env(self, tmp_path: Path) -> None:
        from portolan_cli.config import resolve_push_settings

        self._init_catalog(tmp_path)
        with mock.patch.dict(os.environ, {"PORTOLAN_AWS_PROFILE": "env-profile"}):
            _, profile, _ = resolve_push_settings(None, "cli-profile", tmp_path, None)

        assert profile == "cli-profile"

    @pytest.mark.unit
    def test_stale_sensitive_config_raises(self, tmp_path: Path) -> None:
        """A sensitive setting in config.yaml is rejected (must live in .env)."""
        from portolan_cli.config import resolve_push_settings

        self._init_catalog(tmp_path, "remote: s3://stale-from-config/\n")
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PORTOLAN_REMOTE", None)
            with pytest.raises(ValueError):
                resolve_push_settings(None, None, tmp_path, None)


def _read_yaml(catalog: Path) -> object:
    """Parse ``config.yaml`` directly, without the code under test."""
    import yaml

    return yaml.safe_load((catalog / ".portolan" / "config.yaml").read_text(encoding="utf-8"))


class TestDottedKeysNest:
    """``set_setting`` writes a dotted key as a nested path (issue #1020)."""

    @pytest.mark.unit
    def test_dotted_key_writes_nested_yaml(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        set_setting(tmp_path, "conversion.vector.sort", "none")

        assert _read_yaml(tmp_path) == {"conversion": {"vector": {"sort": "none"}}}

    @pytest.mark.unit
    def test_second_dotted_key_merges_into_existing_block(self, tmp_path: Path) -> None:
        from portolan_cli.config import save_config, set_setting

        save_config(tmp_path, {"backend": "file", "conversion": {"cog": {"tile_size": 256}}})

        set_setting(tmp_path, "conversion.vector.sort", "none")
        set_setting(tmp_path, "conversion.vector.add_bbox", "false")

        assert _read_yaml(tmp_path) == {
            "backend": "file",
            "conversion": {
                "cog": {"tile_size": 256},
                "vector": {"sort": "none", "add_bbox": False},
            },
        }

    @pytest.mark.unit
    def test_cli_strings_are_stored_as_the_schema_type(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        set_setting(tmp_path, "statistics.enabled", "false")
        set_setting(tmp_path, "partitioning.target_rows", "5000")
        set_setting(tmp_path, "partitioning.threshold_gb", "0.5")
        set_setting(tmp_path, "push.exclude", "*.tmp, scratch/")
        set_setting(tmp_path, "pmtiles.src_crs", "EPSG:2263")

        assert _read_yaml(tmp_path) == {
            "statistics": {"enabled": False},
            "partitioning": {"target_rows": 5000, "threshold_gb": 0.5},
            "push": {"exclude": ["*.tmp", "scratch/"]},
            "pmtiles": {"src_crs": "EPSG:2263"},
        }

    @pytest.mark.unit
    def test_collection_level_dotted_key_nests_under_the_collection(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting, set_setting

        set_setting(tmp_path, "pmtiles.enabled", "true", collection="demo")

        assert _read_yaml(tmp_path) == {"collections": {"demo": {"pmtiles": {"enabled": True}}}}
        assert get_setting("pmtiles.enabled", catalog_path=tmp_path, collection="demo") is True

    @pytest.mark.unit
    def test_unset_removes_the_nested_leaf_and_empty_parents(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting, unset_setting

        set_setting(tmp_path, "backend", "file")
        set_setting(tmp_path, "conversion.vector.sort", "none")
        assert _read_yaml(tmp_path) == {
            "backend": "file",
            "conversion": {"vector": {"sort": "none"}},
        }

        assert unset_setting(tmp_path, "conversion.vector.sort") is True
        assert _read_yaml(tmp_path) == {"backend": "file"}
        assert unset_setting(tmp_path, "conversion.vector.sort") is False


class TestUnknownKeysRejected:
    """``set_setting`` rejects a key that no reader reads (issue #1020)."""

    @pytest.mark.unit
    def test_unknown_key_raises_with_the_known_keys(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(ConfigError, match=r"Unknown setting 'conversion\.vector\.compression'"):
            set_setting(tmp_path, "conversion.vector.compression", "zstd")

        with pytest.raises(ConfigError) as excinfo:
            set_setting(tmp_path, "workers", "8")
        message = str(excinfo.value)
        assert "conversion.vector.sort" in message
        assert "statistics.enabled" in message
        assert not (tmp_path / ".portolan" / "config.yaml").exists()

    @pytest.mark.unit
    def test_value_of_the_wrong_type_raises(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(
            ConfigError, match=r"'conversion\.vector\.add_bbox' takes true or false"
        ):
            set_setting(tmp_path, "conversion.vector.add_bbox", "maybe")
        with pytest.raises(ConfigError, match=r"'pmtiles\.min_zoom' takes an integer"):
            set_setting(tmp_path, "pmtiles.min_zoom", "low")

    @pytest.mark.unit
    def test_handler_pin_takes_any_extension(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.conversion_config import get_handler_pins

        set_setting(tmp_path, "conversion.handlers..tif", "cog")

        assert _read_yaml(tmp_path) == {"conversion": {"handlers": {".tif": "cog"}}}
        assert get_handler_pins(tmp_path) == {".tif": "cog"}


class TestConversionReadersSeeConfigSet:
    """Each conversion reader returns what ``set_setting`` wrote (issue #1020)."""

    @pytest.mark.unit
    def test_vector_settings(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.conversion_config import get_vector_settings

        set_setting(tmp_path, "conversion.vector.sort", "none")
        set_setting(tmp_path, "conversion.vector.add_bbox", "false")

        settings = get_vector_settings(tmp_path)
        assert (settings.sort, settings.add_bbox) == ("none", False)

    @pytest.mark.unit
    def test_cog_settings(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.conversion_config import get_cog_settings

        set_setting(tmp_path, "conversion.cog.compression", "zstd")
        set_setting(tmp_path, "conversion.cog.tile_size", "256")

        settings = get_cog_settings(tmp_path)
        assert (settings.compression, settings.tile_size) == ("ZSTD", 256)

    @pytest.mark.unit
    def test_conversion_overrides(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.conversion_config import get_conversion_overrides

        set_setting(tmp_path, "conversion.extensions.convert", "fgb")
        set_setting(tmp_path, "conversion.paths.preserve", "legacy/**")

        overrides = get_conversion_overrides(tmp_path)
        assert overrides.extensions_convert == frozenset({".fgb"})
        assert overrides.paths_preserve == ("legacy/**",)


class TestLegacyFlatKeys:
    """A flat dotted key from an older ``config set`` keeps its effect (issue #1020)."""

    @pytest.mark.unit
    def test_flat_key_reads_as_its_nested_equivalent(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting
        from portolan_cli.conversion_config import get_vector_settings

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "conversion.vector.sort: none\n"
            "conversion.vector.add_bbox: 'false'\n"
            "statistics.enabled: 'false'\n"
        )

        settings = get_vector_settings(tmp_path)
        assert (settings.sort, settings.add_bbox) == ("none", False)
        assert get_setting("statistics.enabled", catalog_path=tmp_path) is False

    @pytest.mark.unit
    def test_next_set_rewrites_flat_keys_as_nested(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "conversion.vector.sort: none\nconversion:\n  vector:\n    sort: hilbert\n"
        )

        set_setting(tmp_path, "backend", "file")

        # The flat key wins: get_setting read it first before this change.
        assert _read_yaml(tmp_path) == {
            "conversion": {"vector": {"sort": "none"}},
            "backend": "file",
        }


def _parses() -> int:
    return trace.counters().get("config.yaml_parses", 0)


def _merges() -> int:
    return trace.counters().get("config.merges", 0)


def _hierarchy(root: Path) -> Path:
    """Write a catalog with a config and a metadata file at the root and in ``demo``."""
    _write_config(root, "backend: file\npmtiles:\n  min_zoom: 1\n")
    _write_config(root / "demo", "pmtiles:\n  min_zoom: 3\n")
    (root / ".portolan" / "metadata.yaml").write_text("license: CC-BY-4.0\n", encoding="utf-8")
    (root / "demo" / ".portolan" / "metadata.yaml").write_text(
        "title: Demo\ncontact:\n  name: Ana\n", encoding="utf-8"
    )
    return root / "demo"


def _rewrite_in_place(path: Path, text: str, mtime_ns: int) -> None:
    """Write ``text`` into the same inode and pin its mtime to ``mtime_ns``."""
    with path.open("r+", encoding="utf-8") as handle:
        handle.write(text)
        handle.truncate()
    os.utime(path, ns=(mtime_ns, mtime_ns))


class TestOneParsePerFile:
    """One run parses each YAML file once while it stays the same (issues #1006, #994)."""

    @pytest.mark.unit
    def test_threads_that_miss_together_merge_once(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import threading
        import time

        from portolan_cli import config
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        state = RunState.open(tmp_path)
        real_parse, real_merge = config.parse_yaml, config.merge_config_files

        def slow_parse(path: Path) -> Any:
            time.sleep(0.02)  # Keeps the other threads inside the miss.
            return real_parse(path)

        def slow_merge(*args: Any) -> dict[str, Any]:
            time.sleep(0.02)
            return real_merge(*args)

        monkeypatch.setattr(config, "parse_yaml", slow_parse)
        monkeypatch.setattr(config, "merge_config_files", slow_merge)
        start = threading.Barrier(4)

        def read() -> None:
            start.wait()
            state.config(collection)

        with trace.tracing():
            threads = [threading.Thread(target=read) for _ in range(4)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

            # The config of demo and of the root.
            assert (_parses(), _merges()) == (2, 1)

    @pytest.mark.unit
    def test_a_run_parses_each_distinct_file_once(self, tmp_path: Path) -> None:
        from portolan_cli.conversion_config import (
            get_cog_settings,
            get_conversion_overrides,
            get_handler_pins,
            get_vector_settings,
        )
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        state = RunState.open(tmp_path)

        with trace.tracing():
            for _ in range(5):
                state.setting("statistics.enabled")
                state.setting("pmtiles.min_zoom", collection_dir=collection)
                state.config(collection)
                state.config(tmp_path)
                state.metadata(collection)
                state.metadata(collection, inherit=False)
                get_vector_settings(tmp_path, state=state)
                get_cog_settings(tmp_path, state=state)
                get_conversion_overrides(tmp_path, state=state)
                get_handler_pins(tmp_path, state=state)

            # Two config.yaml files and two metadata.yaml files.
            assert _parses() == 4
            # The config of demo and of the root, and the metadata of demo.
            assert _merges() == 3

    @pytest.mark.unit
    def test_a_call_without_a_state_parses_again(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting, set_setting

        set_setting(tmp_path, "backend", "file")

        with trace.tracing():
            get_setting("backend", catalog_path=tmp_path)
            get_setting("backend", catalog_path=tmp_path)

            assert _parses() == 2

    @pytest.mark.unit
    def test_a_file_rewritten_mid_run_is_parsed_again(self, tmp_path: Path) -> None:
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        config_file = collection / ".portolan" / "config.yaml"
        state = RunState.open(tmp_path)

        with trace.tracing():
            assert state.setting("pmtiles.min_zoom", collection_dir=collection) == 3
            config_file.write_text("pmtiles:\n  min_zoom: 12\n", encoding="utf-8")

            assert state.setting("pmtiles.min_zoom", collection_dir=collection) == 12
            assert state.config(collection)["pmtiles"] == {"min_zoom": 12}
            assert _parses() == 3

    @pytest.mark.unit
    def test_a_same_size_rewrite_with_a_new_mtime_ns_is_parsed_again(self, tmp_path: Path) -> None:
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        config_file = collection / ".portolan" / "config.yaml"
        before = config_file.stat()
        state = RunState.open(tmp_path)

        with trace.tracing():
            assert state.setting("pmtiles.min_zoom", collection_dir=collection) == 3
            # The same inode and the same size. Only st_mtime_ns moves, by 1 ns.
            _rewrite_in_place(config_file, "pmtiles:\n  min_zoom: 4\n", before.st_mtime_ns + 1)
            after = config_file.stat()
            assert (after.st_ino, after.st_size) == (before.st_ino, before.st_size)

            assert state.setting("pmtiles.min_zoom", collection_dir=collection) == 4
            assert _parses() == 3

    @pytest.mark.unit
    def test_invalidate_drops_a_file_whose_stat_did_not_change(self, tmp_path: Path) -> None:
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        metadata_file = collection / ".portolan" / "metadata.yaml"
        before = metadata_file.stat()
        state = RunState.open(tmp_path)
        assert state.metadata(collection)["title"] == "Demo"

        # A coarse clock leaves the stat equal after an in-place write.
        _rewrite_in_place(metadata_file, "title: Ommo\ncontact:\n  name: Ana\n", before.st_mtime_ns)
        assert state.metadata(collection)["title"] == "Demo"
        state.invalidate(metadata_file)

        assert state.metadata(collection)["title"] == "Ommo"
        assert state.metadata(collection, inherit=False)["title"] == "Ommo"

    @pytest.mark.unit
    def test_caller_edits_do_not_reach_the_memo(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        set_setting(tmp_path, "push.exclude", "*.tmp")
        state = RunState.open(tmp_path)

        state.config(collection)["push"]["exclude"].append("mutated")
        state.config(tmp_path)["pmtiles"]["min_zoom"] = 99
        state.metadata(collection)["contact"]["name"] = "edited"
        state.metadata(collection, inherit=False)["contact"]["name"] = "edited"
        patterns = state.setting("push.exclude")
        assert isinstance(patterns, list)
        patterns.append("mutated")
        merged_patterns = state.setting("push.exclude", collection_dir=collection)
        assert isinstance(merged_patterns, list)
        merged_patterns.append("mutated")

        assert state.setting("push.exclude") == ["*.tmp"]
        assert state.config(collection)["push"]["exclude"] == ["*.tmp"]
        assert state.config(tmp_path)["pmtiles"]["min_zoom"] == 1
        assert state.metadata(collection)["contact"] == {"name": "Ana"}
        assert state.metadata(collection, inherit=False)["contact"] == {"name": "Ana"}

    @pytest.mark.unit
    def test_the_environment_applies_on_each_call(self, tmp_path: Path) -> None:
        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        state = RunState.open(tmp_path)
        assert state.config(collection)["pmtiles"]["min_zoom"] == 3

        with mock.patch.dict(os.environ, {"PORTOLAN_PMTILES_MIN_ZOOM": "7"}):
            assert state.config(collection)["pmtiles"]["min_zoom"] == 7
            assert state.setting("pmtiles.min_zoom", collection_dir=collection) == 7

        assert state.config(collection)["pmtiles"]["min_zoom"] == 3

    @pytest.mark.unit
    def test_the_root_config_raises_as_load_catalog_config_does(self, tmp_path: Path) -> None:
        from portolan_cli.errors import ConfigParseError
        from portolan_cli.run_state import RunState

        _write_config(tmp_path, "backend: [unclosed\n")

        with pytest.raises(ConfigParseError):
            RunState.open(tmp_path).config(tmp_path)


def _read_through_state(name: str, root: Path, collection: Path, state: Any) -> object:
    """Call one config reader of a per-file or per-collection path with ``state``."""
    from types import SimpleNamespace

    from portolan_cli import (
        add,
        catalog_list,
        config,
        conversion_config,
        finalization,
        formats,
        metadata_yaml,
        partitioning,
        readme,
    )
    from portolan_cli.plugins.formats import handler_for
    from portolan_cli.stac import MergeStrategy
    from portolan_cli.validation import config as validation_config
    from portolan_cli.viz import pmtiles, style

    data = collection / "data.parquet"
    calls: dict[str, Callable[[], object]] = {
        "get_ignored_files": lambda: config.get_ignored_files(root, state=state),
        "load_catalog_config": lambda: config.load_catalog_config(root, state=state),
        "get_vector_settings": lambda: conversion_config.get_vector_settings(root, state=state),
        "handler_for": lambda: handler_for(data, root, state=state),
        "get_cloud_native_status": lambda: formats.get_cloud_native_status(data, root, state=state),
        "partition_strategy": lambda: partitioning.partition_strategy(data, root, state=state),
        "find_large_partition_candidates": lambda: partitioning.find_large_partition_candidates(
            [], root, state=state
        ),
        "get_pmtiles_settings": lambda: pmtiles.get_pmtiles_settings(
            root, "demo", collection, state=state
        ),
        "get_vector_style_config": lambda: style.get_vector_style_config(root, state=state),
        "enrich_cog_assets": lambda: style.enrich_cog_assets(
            {"cog": SimpleNamespace(media_type="image/tiff", extra_fields={})}, root, state=state
        ),
        "load_and_validate_metadata": lambda: metadata_yaml.load_and_validate_metadata(
            collection, root, state=state
        ),
        "generate_readme_for_collection": lambda: readme.generate_readme_for_collection(
            collection, root, state=state
        ),
        "generate_catalog_readme": lambda: readme.generate_catalog_readme(root, state=state),
        "list_catalog_contents": lambda: catalog_list.list_catalog_contents(root, state=state),
        "write_companion_collection": lambda: add._write_companion_collection(
            root,
            collection.relative_to(root).as_posix(),
            [finalization.CompanionAsset(data)],
            MergeStrategy.SMART,
            False,
            state,
        ),
        "load_rules_config": lambda: validation_config.load_rules_config(root, state=state),
        "load_public_url": lambda: validation_config.load_public_url(root, state=state),
        "plan_tabular": lambda: add._plan_tabular(
            add._DeferredPlan(), data, collection.relative_to(root).as_posix(), root, [], state
        ),
    }
    return calls[name]()


class TestReadersTakeTheState:
    """Each reader on a per-file or per-collection path reads through the state (#994)."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "name",
        [
            "get_ignored_files",
            "load_catalog_config",
            "get_vector_settings",
            "handler_for",
            "get_cloud_native_status",
            "partition_strategy",
            "find_large_partition_candidates",
            "get_pmtiles_settings",
            "get_vector_style_config",
            "enrich_cog_assets",
            "load_and_validate_metadata",
            "generate_readme_for_collection",
            "generate_catalog_readme",
            "list_catalog_contents",
            "write_companion_collection",
            "plan_tabular",
            "load_rules_config",
            "load_public_url",
        ],
    )
    def test_a_repeated_call_parses_nothing(self, tmp_path: Path, name: str) -> None:
        import json

        from portolan_cli.run_state import RunState

        collection = _hierarchy(tmp_path)
        (tmp_path / "catalog.json").write_text(json.dumps({"id": "root"}), encoding="utf-8")
        (collection / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "stac_version": "1.1.0",
                    "id": "demo",
                    "description": "Demo",
                    "license": "other",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                }
            ),
            encoding="utf-8",
        )
        (collection / "data.parquet").write_bytes(b"PAR1")
        state = RunState.open(tmp_path)

        with trace.tracing():
            _read_through_state(name, tmp_path, collection, state)
            first = _parses()
            _read_through_state(name, tmp_path, collection, state)
            _read_through_state(name, tmp_path, collection, state)

            assert first > 0, "the reader must read a YAML file"
            assert _parses() == first


class TestCheckReadsTheConfigOnce:
    """check reads the catalog config.yaml through one run state (issue #994)."""

    @pytest.mark.integration
    def test_run_check_parses_the_catalog_config_once(self, tmp_path: Path) -> None:
        from portolan_cli.validation.runner import run_check

        _write_config(tmp_path, "publish:\n  public_url: https://data.example.org/\n")

        with trace.tracing():
            outcome = run_check(tmp_path, data=False, structural=False, geo_assets=False)

            assert outcome.live_hint is not None
            assert _parses() == 1


class TestHandlersReadTheState:
    """A format handler takes no state. add hands it the settings (issue #994)."""

    @pytest.mark.integration
    @pytest.mark.parametrize("workers", [1, 4])
    def test_partition_settings_reach_the_handler_in_each_worker(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, workers: int
    ) -> None:
        import shutil
        import threading

        from portolan_cli import conversion_config
        from portolan_cli.add import add_files
        from tests.conftest import _init_catalog

        root = _init_catalog(tmp_path / "catalog")
        source = (
            Path(__file__).parent.parent
            / "fixtures"
            / "realdata"
            / "philadelphia-housing"
            / "council_districts_2024.parquet"
        )
        for name in ("a", "b", "c", "d"):
            (root / name).mkdir()
            shutil.copy(source, root / name / "districts.parquet")
        seen: list[tuple[str, bool]] = []
        real = conversion_config.partition_override

        def spy(directory: Path) -> conversion_config.PartitionSettings | None:
            found = real(directory)
            seen.append((threading.current_thread().name, found is not None))
            return found

        monkeypatch.setattr(conversion_config, "partition_override", spy)

        with trace.tracing():
            added, _skipped, failures = add_files(paths=[root], catalog_root=root, workers=workers)

        assert failures == []
        assert len(added) == 4
        # partition_strategy reads partitioning.enabled and threshold_gb per file.
        assert len(seen) == 8
        assert all(found for _thread, found in seen)
        workers_used = {thread for thread, _found in seen}
        assert (workers_used == {"MainThread"}) is (workers == 1)


def _geo_asset_catalog(root: Path, n: int) -> Path:
    """A catalog with ``n`` valid COGs and ``n`` FlatGeobuf files, one per directory."""
    import shutil

    from tests.conftest import _init_catalog

    fixtures = Path(__file__).parent.parent / "fixtures"
    catalog = _init_catalog(root)
    for i in range(n):
        (catalog / f"r{i}").mkdir()
        shutil.copy(fixtures / "raster" / "valid" / "rgb.tif", catalog / f"r{i}" / "rgb.tif")
        (catalog / f"v{i}").mkdir()
        shutil.copy(fixtures / "cloud_native" / "sample.fgb", catalog / f"v{i}" / "sample.fgb")
    return catalog


class TestCheckFixReadsTheState:
    """The geo-asset pass of ``check --fix`` reads config.yaml once per run (#994)."""

    @staticmethod
    def _parses_at(root: Path, n: int, *, dry_run: bool) -> int:
        from portolan_cli.run_state import RunState
        from portolan_cli.scan.check import check_directory

        catalog = _geo_asset_catalog(root, n)
        with trace.tracing():
            check_directory(
                catalog,
                fix=True,
                dry_run=dry_run,
                force=True,
                catalog_path=catalog,
                workers=1,
                state=RunState.open(catalog),
            )
            return _parses()

    @pytest.mark.integration
    @pytest.mark.parametrize("dry_run", [True, False], ids=["dry-run", "fix"])
    def test_force_parses_the_config_once(self, tmp_path: Path, dry_run: bool) -> None:
        one = self._parses_at(tmp_path / "one", 1, dry_run=dry_run)
        two = self._parses_at(tmp_path / "two", 2, dry_run=dry_run)

        assert (one, two) == (1, 1)

    @pytest.mark.unit
    def test_a_pool_worker_parses_the_config_once(self, tmp_path: Path) -> None:
        from portolan_cli import convert

        catalog = _geo_asset_catalog(tmp_path / "catalog", 2)
        convert._init_worker_state(catalog)
        try:
            with trace.tracing():
                for i in range(2):
                    convert._convert_in_worker(catalog / f"v{i}" / "sample.fgb", catalog_path=catalog)

                assert _parses() == 1
        finally:
            convert._init_worker_state(None)


class TestYamlLoader:
    """Every YAML load uses libyaml when PyYAML has it (issue #994)."""

    @pytest.mark.unit
    @pytest.mark.parametrize("with_libyaml", [True, False])
    def test_both_loaders_read_the_same_config(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, with_libyaml: bool
    ) -> None:
        import yaml

        from portolan_cli import config

        if with_libyaml:
            assert hasattr(yaml, "CSafeLoader"), "this test needs PyYAML built with libyaml"
        else:
            monkeypatch.delattr(yaml, "CSafeLoader")
        loaders: list[type] = []
        real_load = yaml.load

        def spy(stream: str, Loader: type) -> object:  # noqa: N803 (the PyYAML name)
            loaders.append(Loader)
            return real_load(stream, Loader=Loader)

        monkeypatch.setattr(yaml, "load", spy)
        _write_config(tmp_path, "pmtiles:\n  min_zoom: 2\nconversion.vector.sort: none\n")

        assert config.load_config(tmp_path) == {
            "pmtiles": {"min_zoom": 2},
            "conversion": {"vector": {"sort": "none"}},
        }
        assert config.safe_load("[1, 2]") == [1, 2]
        expected = yaml.CSafeLoader if with_libyaml else yaml.SafeLoader
        assert loaders == [expected, expected]

    @pytest.mark.unit
    def test_the_loader_refuses_python_tags(self) -> None:
        import yaml

        from portolan_cli.config import safe_load

        with pytest.raises(yaml.YAMLError):
            safe_load("!!python/object/apply:os.system ['true']\n")


def _write_config(directory: Path, text: str) -> None:
    (directory / ".portolan").mkdir(parents=True, exist_ok=True)
    (directory / ".portolan" / "config.yaml").write_text(text, encoding="utf-8")


class TestTypedValues:
    """Each schema type parses the text of a ``config set`` VALUE (issue #1020)."""

    @pytest.mark.unit
    @pytest.mark.parametrize(("text", "stored"), [("auto", "auto"), ("7", 7)])
    def test_int_or_auto_takes_an_integer_or_auto(
        self, tmp_path: Path, text: str, stored: object
    ) -> None:
        from portolan_cli.config import set_setting

        set_setting(tmp_path, "conversion.vector.resolution", text)

        assert _read_yaml(tmp_path) == {"conversion": {"vector": {"resolution": stored}}}

    @pytest.mark.unit
    @pytest.mark.parametrize("value", ["high", True])
    def test_int_or_auto_rejects_other_values(self, tmp_path: Path, value: object) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(ConfigError, match="takes an integer or 'auto'"):
            set_setting(tmp_path, "conversion.vector.resolution", value)

    @pytest.mark.unit
    def test_a_string_setting_keeps_yaml_like_text_as_text(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        set_setting(tmp_path, "pmtiles.layer", "true")
        set_setting(tmp_path, "backend", "123")

        assert _read_yaml(tmp_path) == {"pmtiles": {"layer": "true"}, "backend": "123"}


class TestPatternKeys:
    """A key below a ``.*`` pattern reads and unsets as one name (issue #1020)."""

    @pytest.mark.unit
    def test_get_and_unset_a_handler_pin(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting, set_setting, unset_setting

        set_setting(tmp_path, "backend", "file")
        set_setting(tmp_path, "conversion.handlers..tif", "cog")

        assert get_setting("conversion.handlers..tif", catalog_path=tmp_path) == "cog"
        assert unset_setting(tmp_path, "conversion.handlers..tif") is True
        assert _read_yaml(tmp_path) == {"backend": "file"}


class TestLegacyFlatKeysInCollections:
    """The fold also reads the flat keys under ``collections:`` (issue #1020)."""

    @pytest.mark.unit
    def test_flat_key_under_a_collection_reads_as_its_typed_nested_value(
        self, tmp_path: Path
    ) -> None:
        from portolan_cli.config import get_setting

        _write_config(tmp_path, "collections:\n  demo:\n    pmtiles.enabled: 'true'\n")

        assert get_setting("pmtiles.enabled", catalog_path=tmp_path, collection="demo") is True

    @pytest.mark.unit
    def test_flat_key_with_a_bad_value_keeps_the_value_for_the_reader(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_config

        _write_config(tmp_path, "statistics.enabled: banana\n")

        assert load_config(tmp_path) == {"statistics": {"enabled": "banana"}}


class TestLegacyCollectionsMerge:
    """``load_merged_config`` merges root, legacy ``collections:``, then the hierarchy."""

    @pytest.mark.unit
    def test_legacy_section_overrides_the_root_and_the_folder_overrides_both(
        self, tmp_path: Path
    ) -> None:
        from portolan_cli.config import load_merged_config

        root = tmp_path / "catalog"
        _write_config(
            root,
            "backend: file\npmtiles:\n  enabled: false\n  min_zoom: 1\n"
            "collections:\n  demo:\n    pmtiles:\n      enabled: true\n      min_zoom: 2\n",
        )
        _write_config(root / "demo", "pmtiles:\n  min_zoom: 3\n")
        # An unresolved root once merged the root file twice, over the legacy values.
        (tmp_path / "other").mkdir()
        unresolved_root = tmp_path / "other" / ".." / "catalog"

        merged = load_merged_config(root / "demo", unresolved_root)

        assert merged == {"backend": "file", "pmtiles": {"enabled": True, "min_zoom": 3}}


class TestLoadersReturnDeepCopies:
    """An edit of a nested value in a result does not reach the next read (issue #1006)."""

    @pytest.mark.unit
    def test_get_setting_and_load_merged_config(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting, load_merged_config

        _write_config(tmp_path, "check:\n  severity:\n    PTL-PRV-001: warning\n")

        check = get_setting("check", catalog_path=tmp_path)
        assert isinstance(check, dict)
        check["severity"]["PTL-PRV-001"] = "edited"
        load_merged_config(tmp_path, tmp_path)["check"]["severity"]["PTL-PRV-001"] = "edited"

        assert get_setting("check", catalog_path=tmp_path) == {
            "severity": {"PTL-PRV-001": "warning"}
        }

    @pytest.mark.unit
    def test_metadata_loaders(self, tmp_path: Path) -> None:
        from portolan_cli.config import load_merged_metadata, load_own_metadata

        collection = tmp_path / "demo"
        (collection / ".portolan").mkdir(parents=True)
        (collection / ".portolan" / "metadata.yaml").write_text(
            "title: Demo\ncontact:\n  name: Ana\n", encoding="utf-8"
        )

        load_own_metadata(collection)["contact"]["name"] = "edited"
        load_merged_metadata(collection, tmp_path)["contact"]["name"] = "edited"

        expected = {"title": "Demo", "contact": {"name": "Ana"}}
        assert load_own_metadata(collection) == expected
        assert load_merged_metadata(collection, tmp_path) == expected
        assert load_own_metadata(tmp_path / "absent") == {}


class TestListNestedSettings:
    """``list_settings`` lists nested leaves with their sources (issue #1020)."""

    @pytest.mark.unit
    def test_catalog_listing_holds_leaves_but_not_the_collections_section(
        self, tmp_path: Path
    ) -> None:
        from portolan_cli.config import list_settings

        _write_config(
            tmp_path,
            "backend: file\ncollections:\n  demo:\n    plugin_key: on\n",
        )

        with cleared_environ():
            settings = list_settings(tmp_path)

        assert settings["backend"] == {"value": "file", "source": "catalog"}
        assert settings["statistics.enabled"] == {"value": True, "source": "default"}
        assert [key for key in settings if key.startswith("collections")] == []
        assert "conversion.handlers.*" not in settings

    @pytest.mark.unit
    def test_collection_listing_holds_a_key_outside_the_schema(self, tmp_path: Path) -> None:
        from portolan_cli.config import list_settings

        _write_config(tmp_path, "collections:\n  demo:\n    plugin:\n      key: on\n")

        with cleared_environ():
            settings = list_settings(tmp_path, collection="demo")

        assert settings["plugin.key"] == {"value": True, "source": "collection"}

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("text", "collection"),
        [
            ("remote: s3://stale/\n", None),
            ("collections:\n  demo:\n    remote: s3://stale/\n", "demo"),
        ],
    )
    def test_a_sensitive_key_in_the_file_lists_as_insecure(
        self, tmp_path: Path, text: str, collection: str | None
    ) -> None:
        from portolan_cli.config import list_settings

        _write_config(tmp_path, text)

        with cleared_environ():
            settings = list_settings(tmp_path, collection=collection)

        assert settings["remote"] == {"value": "s3://stale/", "source": "config (INSECURE)"}

    @pytest.mark.unit
    def test_an_unset_key_has_the_default_source(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting_source

        _write_config(tmp_path, "backend: file\n")

        with cleared_environ():
            assert get_setting_source("pmtiles.min_zoom", tmp_path, None) == "default"
            assert get_setting_source("backend", tmp_path, None) == "catalog"

    @pytest.mark.unit
    def test_the_unknown_key_message_lists_pattern_keys_by_name(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(ConfigError) as excinfo:
            set_setting(tmp_path, "workers", "8")

        assert "backend, check.disabled, check.severity.<name>, conversion.cog.compression" in str(
            excinfo.value
        )


class TestCollectionScope:
    """``set_setting`` writes a collection value only where a reader reads it."""

    @pytest.mark.unit
    def test_a_catalog_only_setting_rejects_a_collection(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(
            ConfigError,
            match=r"^\[PRTLN-CFG000\] Setting 'conversion\.vector\.sort' applies to the "
            r"whole catalog\. Run the command again without --collection\.$",
        ):
            set_setting(tmp_path, "conversion.vector.sort", "none", collection="demo")
        assert not (tmp_path / ".portolan" / "config.yaml").exists()

    @pytest.mark.unit
    def test_a_nested_collection_reads_its_own_section(self, tmp_path: Path) -> None:
        from portolan_cli.config import get_setting, set_setting

        set_setting(tmp_path, "pmtiles.enabled", "true", collection="parent/child")

        def read(*parts: str) -> object:
            return get_setting(
                "pmtiles.enabled", catalog_path=tmp_path, collection_path=tmp_path.joinpath(*parts)
            )

        assert read("parent", "child") is True
        assert read("parent", "child", "item") is True
        assert read("parent") is False


class TestEnvironmentValuesTakeTheSchemaType:
    """A ``PORTOLAN_<KEY>`` value reads as the type of its setting."""

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("key", "text", "expected"),
        [
            ("statistics.enabled", "false", False),
            ("push.exclude", "*.md, scratch/", ["*.md", "scratch/"]),
            ("pmtiles.min_zoom", "3", 3),
            ("backend", "123", "123"),
            ("statistics.enabled", "banana", "banana"),
        ],
    )
    def test_env_value(self, tmp_path: Path, key: str, text: str, expected: object) -> None:
        from portolan_cli.config import _get_env_var_name, get_setting

        with cleared_environ(), mock.patch.dict(os.environ, {_get_env_var_name(key): text}):
            value = get_setting(key, catalog_path=tmp_path)

        assert (value, type(value)) == (expected, type(expected))

    @pytest.mark.unit
    def test_config_set_keeps_the_environment_out_of_the_file(self, tmp_path: Path) -> None:
        """The readers see PORTOLAN_<KEY>, but a save writes the file content only."""
        from portolan_cli.config import get_config_path, load_catalog_config, set_setting

        with cleared_environ(PORTOLAN_THUMBNAILS_QUALITY="20"):
            set_setting(tmp_path, "thumbnails.enabled", "false")
            seen = load_catalog_config(tmp_path).section("thumbnails")

        assert seen == {"enabled": False, "quality": 20}
        saved = get_config_path(tmp_path).read_text(encoding="utf-8")
        assert saved == "thumbnails:\n  enabled: false\n"


class TestSchemaEdges:
    """``set_setting`` rejects the values that no reader can use."""

    @pytest.mark.unit
    def test_a_pattern_is_not_a_key(self, tmp_path: Path) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(ConfigError, match=r"Unknown setting 'conversion\.handlers\.\*'"):
            set_setting(tmp_path, "conversion.handlers.*", "cog")

    @pytest.mark.unit
    @pytest.mark.parametrize("text", ["[2024-01-01]", "[[a]]", "[{a: 1}]"])
    def test_a_list_holds_only_scalars(self, tmp_path: Path, text: str) -> None:
        from portolan_cli.config import set_setting

        with pytest.raises(ConfigError, match=r"'push\.exclude' takes a comma-separated list"):
            set_setting(tmp_path, "push.exclude", text)
