"""Tests for portolan_cli.config module.

Tests the configuration system including:
- Loading/saving config from .portolan/config.yaml
- Setting precedence (CLI > env > collection config > catalog config > default)
- Collection-level config overrides
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest

if TYPE_CHECKING:
    from collections.abc import Iterator


class TestLoadConfig:
    """Tests for load_config function."""

    @pytest.mark.unit
    def test_load_config_returns_empty_dict_when_file_missing(
        self, tmp_path: Path
    ) -> None:
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
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog-bucket/",
                "collections": {
                    "demographics": {"remote": "s3://demographics-bucket/"},
                },
            },
        )

        result = get_setting("remote", catalog_path=tmp_path, collection="demographics")

        assert result == "s3://demographics-bucket/"

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
            result = get_setting(
                "remote", catalog_path=tmp_path, collection="demographics"
            )

        assert result == "s3://env/"

    @pytest.mark.unit
    def test_catalog_config_used_when_no_override(self, tmp_path: Path) -> None:
        """Catalog config should be used when no CLI or env override."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://config-value/"})

        result = get_setting("remote", catalog_path=tmp_path)

        assert result == "s3://config-value/"

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
    def test_works_without_catalog_path(self) -> None:
        """get_setting should work with just env vars if no catalog_path."""
        from portolan_cli.config import get_setting

        with mock.patch.dict(os.environ, {"PORTOLAN_REMOTE": "s3://env-only/"}):
            result = get_setting("remote")

        assert result == "s3://env-only/"

    @pytest.mark.unit
    def test_collection_falls_back_to_catalog_config(self, tmp_path: Path) -> None:
        """Collection should inherit catalog config for unset keys."""
        from portolan_cli.config import get_setting, save_config

        (tmp_path / ".portolan").mkdir()
        save_config(
            tmp_path,
            {
                "remote": "s3://catalog/",
                "aws_profile": "catalog-profile",
                "collections": {
                    "demographics": {"remote": "s3://override/"},
                    # aws_profile not set for collection
                },
            },
        )

        result = get_setting(
            "aws_profile", catalog_path=tmp_path, collection="demographics"
        )

        # Should fall back to catalog-level aws_profile
        assert result == "catalog-profile"


class TestSetSetting:
    """Tests for set_setting function."""

    @pytest.mark.unit
    def test_set_catalog_level_setting(self, tmp_path: Path) -> None:
        """set_setting should update catalog-level config."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "remote", "s3://new-bucket/")

        config = load_config(tmp_path)
        assert config["remote"] == "s3://new-bucket/"

    @pytest.mark.unit
    def test_set_collection_level_setting(self, tmp_path: Path) -> None:
        """set_setting with collection should update collection config."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "remote", "s3://collection-bucket/", collection="demographics")

        config = load_config(tmp_path)
        assert config["collections"]["demographics"]["remote"] == "s3://collection-bucket/"

    @pytest.mark.unit
    def test_set_preserves_existing_config(self, tmp_path: Path) -> None:
        """set_setting should preserve other existing settings."""
        from portolan_cli.config import load_config, save_config, set_setting

        (tmp_path / ".portolan").mkdir()
        save_config(tmp_path, {"remote": "s3://existing/", "aws_profile": "prod"})

        set_setting(tmp_path, "aws_profile", "dev")

        config = load_config(tmp_path)
        assert config["remote"] == "s3://existing/"
        assert config["aws_profile"] == "dev"

    @pytest.mark.unit
    def test_set_creates_collections_section_if_missing(self, tmp_path: Path) -> None:
        """set_setting should create collections section if needed."""
        from portolan_cli.config import load_config, set_setting

        (tmp_path / ".portolan").mkdir()

        set_setting(tmp_path, "remote", "s3://new/", collection="newcollection")

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


class TestConfigConstants:
    """Tests for config-related constants and defaults."""

    @pytest.mark.unit
    def test_known_settings_includes_remote(self) -> None:
        """KNOWN_SETTINGS should include 'remote'."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "remote" in KNOWN_SETTINGS

    @pytest.mark.unit
    def test_known_settings_includes_aws_profile(self) -> None:
        """KNOWN_SETTINGS should include 'aws_profile'."""
        from portolan_cli.config import KNOWN_SETTINGS

        assert "aws_profile" in KNOWN_SETTINGS


class TestConfigFilePath:
    """Tests for config file path handling."""

    @pytest.mark.unit
    def test_config_file_location(self, tmp_path: Path) -> None:
        """Config file should be at .portolan/config.yaml."""
        from portolan_cli.config import get_config_path

        path = get_config_path(tmp_path)

        assert path == tmp_path / ".portolan" / "config.yaml"
