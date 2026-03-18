"""Tests for input validation and hardening against agent hallucinations.

These tests verify that the input validation module correctly rejects
malicious or malformed inputs that agents might hallucinate.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.validation import (
    InputValidationError,
    validate_collection_id,
    validate_config_key,
    validate_config_value,
    validate_item_id,
    validate_remote_url,
    validate_safe_path,
)


@pytest.mark.unit
class TestValidateSafePath:
    """Tests for path traversal protection."""

    def test_valid_relative_path(self, tmp_path: Path) -> None:
        """Valid relative paths within base dir are accepted."""
        # Create the directory structure first
        (tmp_path / "data").mkdir()
        (tmp_path / "data" / "file.parquet").touch()

        # Use the tmp_path as the base and validate a relative path
        safe_path = validate_safe_path(tmp_path / "data" / "file.parquet", tmp_path)
        assert safe_path.is_absolute()
        assert safe_path.is_relative_to(tmp_path)

    def test_valid_absolute_path(self, tmp_path: Path) -> None:
        """Valid absolute paths within base dir are accepted."""
        target = tmp_path / "data" / "file.parquet"
        safe_path = validate_safe_path(target, tmp_path)
        assert safe_path == target

    def test_rejects_path_traversal_relative(self, tmp_path: Path) -> None:
        """Path traversal with ../ is rejected."""
        with pytest.raises(InputValidationError, match="Path traversal"):
            validate_safe_path(Path("../../.ssh/id_rsa"), tmp_path)

    def test_rejects_path_traversal_absolute(self, tmp_path: Path) -> None:
        """Absolute path outside base dir is rejected."""
        with pytest.raises(InputValidationError, match="Path traversal"):
            validate_safe_path(Path("/etc/passwd"), tmp_path)

    def test_rejects_symlink_escape(self, tmp_path: Path) -> None:
        """Symlink that escapes base dir is rejected."""
        # Create a symlink pointing outside base dir
        link_path = tmp_path / "escape_link"
        link_path.symlink_to("/etc")

        with pytest.raises(InputValidationError, match="Path traversal"):
            validate_safe_path(link_path / "passwd", tmp_path)

    def test_none_base_dir_uses_cwd(self) -> None:
        """When base_dir is None, CWD is used."""
        # This just verifies it doesn't crash
        safe_path = validate_safe_path(Path("relative.txt"))
        assert safe_path.is_absolute()

    def test_rejects_path_with_excessive_nesting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test handling of OSError from path.resolve() for deeply nested paths."""

        class PathWithOSError(type(tmp_path)):
            """Path subclass that raises OSError on resolve()."""

            def resolve(self, strict: bool = False) -> Path:
                """Simulate OSError during path resolution."""
                if "trigger_error" in str(self):
                    raise OSError("Path too deep or filesystem error")
                return super().resolve(strict=strict)

        # Create a path that will trigger the error
        error_path = PathWithOSError(tmp_path / "trigger_error" / "file.txt")

        with pytest.raises(InputValidationError, match="Cannot resolve path"):
            validate_safe_path(error_path, tmp_path)


@pytest.mark.unit
class TestValidateCollectionId:
    """Tests for collection ID validation."""

    def test_valid_collection_id(self) -> None:
        """Valid collection IDs are accepted."""
        assert validate_collection_id("census-2020") == "census-2020"
        assert validate_collection_id("imagery_v2") == "imagery_v2"
        assert validate_collection_id("buildings") == "buildings"

    def test_rejects_empty_id(self) -> None:
        """Empty collection ID is rejected."""
        with pytest.raises(InputValidationError, match="cannot be empty"):
            validate_collection_id("")

    def test_rejects_control_characters(self) -> None:
        """Control characters are rejected."""
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_collection_id("census\x00data")
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_collection_id("data\nwith\nnewlines")

    def test_rejects_query_parameters(self) -> None:
        """Query parameters (agent hallucination) are rejected."""
        with pytest.raises(InputValidationError, match="Query parameters"):
            validate_collection_id("census?fields=name")

    def test_rejects_url_fragments(self) -> None:
        """URL fragments are rejected."""
        with pytest.raises(InputValidationError, match="fragments"):
            validate_collection_id("census#section")

    def test_rejects_url_encoding(self) -> None:
        """Pre-encoded strings (agent hallucination) are rejected."""
        with pytest.raises(InputValidationError, match="URL-encoded"):
            validate_collection_id("census%20data")
        with pytest.raises(InputValidationError, match="URL-encoded"):
            validate_collection_id("%2e%2e")  # Encoded ../

    def test_allows_forward_slashes_rejects_backslashes(self) -> None:
        """Forward slashes allowed for nested catalogs (ADR-0032), backslashes rejected."""
        # Forward slashes are allowed (nested catalog paths)
        validate_collection_id("environment/air-quality")  # Should not raise

        # Backslashes are still rejected
        with pytest.raises(InputValidationError, match="Backslashes"):
            validate_collection_id("parent\\child")

    def test_rejects_path_segments_starting_with_numbers(self) -> None:
        """Path segments starting with numbers are rejected (ADR-0032)."""
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("environment/2024")
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("2024/january")

    def test_rejects_path_segments_starting_with_hyphen(self) -> None:
        """Path segments starting with hyphens are rejected (ADR-0032)."""
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("environment/-air")
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("-environment/air")

    def test_rejects_path_segments_starting_with_underscore(self) -> None:
        """Path segments starting with underscores are rejected (ADR-0032)."""
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("environment/_quality")
        with pytest.raises(InputValidationError, match="must start with"):
            validate_collection_id("_environment/quality")

    def test_rejects_uppercase(self) -> None:
        """Uppercase characters violate STAC best practice."""
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_collection_id("Census-2020")

    def test_rejects_special_characters(self) -> None:
        """Special characters are rejected."""
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_collection_id("census@data")
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_collection_id("census data")  # Space


@pytest.mark.unit
class TestValidateItemId:
    """Tests for item ID validation."""

    def test_valid_item_id(self) -> None:
        """Valid item IDs are accepted."""
        assert validate_item_id("block-123") == "block-123"
        assert validate_item_id("2020-Q1") == "2020-Q1"
        assert validate_item_id("v1.2.3") == "v1.2.3"

    def test_rejects_empty_id(self) -> None:
        """Empty item ID is rejected."""
        with pytest.raises(InputValidationError, match="cannot be empty"):
            validate_item_id("")

    def test_rejects_control_characters(self) -> None:
        """Control characters are rejected."""
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_item_id("item\x00data")

    def test_rejects_query_parameters(self) -> None:
        """Query parameters are rejected."""
        with pytest.raises(InputValidationError, match="Query parameters"):
            validate_item_id("item?fields=name")

    def test_rejects_url_encoding(self) -> None:
        """Pre-encoded strings are rejected."""
        with pytest.raises(InputValidationError, match="URL-encoded"):
            validate_item_id("item%20name")

    def test_rejects_path_traversals(self) -> None:
        """Path traversals are rejected."""
        with pytest.raises(InputValidationError, match="Path separators"):
            validate_item_id("../item")
        with pytest.raises(InputValidationError, match="Path separators"):
            validate_item_id("parent/child")


@pytest.mark.unit
class TestValidateRemoteUrl:
    """Tests for remote URL validation."""

    def test_valid_s3_url(self) -> None:
        """Valid S3 URLs are accepted."""
        url = "s3://my-bucket/catalog/"
        assert validate_remote_url(url) == url

    def test_valid_gs_url(self) -> None:
        """Valid GCS URLs are accepted."""
        url = "gs://my-bucket/catalog/"
        assert validate_remote_url(url) == url

    def test_valid_azure_url(self) -> None:
        """Valid Azure URLs are accepted."""
        url = "az://container/catalog/"
        assert validate_remote_url(url) == url

    def test_valid_http_url(self) -> None:
        """Valid HTTP URLs are accepted."""
        url = "https://storage.example.com/catalog/"
        assert validate_remote_url(url) == url

    def test_rejects_empty_url(self) -> None:
        """Empty URL is rejected."""
        with pytest.raises(InputValidationError, match="cannot be empty"):
            validate_remote_url("")

    def test_rejects_unsupported_scheme(self) -> None:
        """Unsupported URL schemes are rejected."""
        with pytest.raises(InputValidationError, match="Unsupported URL scheme"):
            validate_remote_url("ftp://bucket/catalog")
        with pytest.raises(InputValidationError, match="Unsupported URL scheme"):
            validate_remote_url("file:///local/path")

    def test_rejects_path_traversal(self) -> None:
        """Path traversals in URL paths are rejected."""
        with pytest.raises(InputValidationError, match="Path traversal"):
            validate_remote_url("s3://bucket/../etc/passwd")
        with pytest.raises(InputValidationError, match="Path traversal"):
            validate_remote_url("gs://bucket/data/../../secrets")

    def test_rejects_control_characters(self) -> None:
        """Control characters in any component are rejected."""
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_remote_url("s3://bucket/data\x00file")

    def test_malformed_url_rejected(self) -> None:
        """Malformed URLs are rejected."""
        # Test empty netloc (no bucket/host)
        with pytest.raises(InputValidationError, match="missing host/bucket"):
            validate_remote_url("s3://")
        with pytest.raises(InputValidationError, match="missing host/bucket"):
            validate_remote_url("https:///path")
        with pytest.raises(InputValidationError, match="missing host/bucket"):
            validate_remote_url("gs://")

    def test_handles_urlparse_value_error(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test handling of ValueError from urlparse (rare edge case)."""
        from urllib import parse as urllib_parse

        original_urlparse = urllib_parse.urlparse

        def mock_urlparse(url: str, *args, **kwargs):
            """Mock urlparse that raises ValueError for specific input."""
            if "trigger_value_error" in url:
                raise ValueError("Invalid URL characters")
            return original_urlparse(url, *args, **kwargs)

        monkeypatch.setattr("portolan_cli.validation.input_hardening.urlparse", mock_urlparse)

        with pytest.raises(InputValidationError, match="Malformed URL"):
            validate_remote_url("s3://trigger_value_error/path")


@pytest.mark.unit
class TestValidateConfigKey:
    """Tests for config key validation."""

    def test_valid_config_keys(self) -> None:
        """Valid config keys are accepted."""
        assert validate_config_key("remote") == "remote"
        assert validate_config_key("aws_profile") == "aws_profile"
        assert validate_config_key("max_workers") == "max_workers"

    def test_rejects_empty_key(self) -> None:
        """Empty config key is rejected."""
        with pytest.raises(InputValidationError, match="cannot be empty"):
            validate_config_key("")

    def test_rejects_control_characters(self) -> None:
        """Control characters are rejected."""
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_config_key("key\x00name")

    def test_rejects_uppercase(self) -> None:
        """Uppercase keys are rejected."""
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_config_key("Remote")

    def test_rejects_special_characters(self) -> None:
        """Special characters are rejected."""
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_config_key("aws-profile")  # Hyphen not allowed
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_config_key("remote.url")  # Dot not allowed

    def test_rejects_starting_with_number(self) -> None:
        """Keys starting with numbers are rejected."""
        with pytest.raises(InputValidationError, match="lowercase"):
            validate_config_key("3d_mode")


@pytest.mark.unit
class TestValidateConfigValue:
    """Tests for config value validation."""

    def test_valid_config_values(self) -> None:
        """Valid config values are accepted."""
        assert validate_config_value("s3://bucket/", "remote") == "s3://bucket/"
        assert validate_config_value("production", "aws_profile") == "production"
        assert validate_config_value("10", "max_workers") == "10"

    def test_rejects_empty_value(self) -> None:
        """Empty config value is rejected."""
        with pytest.raises(InputValidationError, match="cannot be empty"):
            validate_config_value("", "remote")

    def test_rejects_control_characters(self) -> None:
        """Control characters (except newline) are rejected."""
        with pytest.raises(InputValidationError, match="Control characters"):
            validate_config_value("value\x00here", "key")

    def test_allows_newlines(self) -> None:
        """Newlines are allowed (for multiline values)."""
        value = "line1\nline2\nline3"
        assert validate_config_value(value, "description") == value

    def test_validates_remote_urls(self) -> None:
        """Remote values are validated as URLs."""
        with pytest.raises(InputValidationError):
            validate_config_value("s3://bucket/../escape", "remote")


@pytest.mark.unit
class TestAgentHallucinationScenarios:
    """Integration tests for realistic agent hallucination scenarios."""

    def test_agent_confuses_path_segments(self, tmp_path: Path) -> None:
        """Agent generates ../../.ssh by confusing context."""
        with pytest.raises(InputValidationError):
            validate_safe_path(Path("../../.ssh/id_rsa"), tmp_path)

    def test_agent_embeds_query_params_in_id(self) -> None:
        """Agent embeds ?fields=name in resource ID."""
        with pytest.raises(InputValidationError):
            validate_collection_id("census?fields=name,geometry")

    def test_agent_double_encodes_string(self) -> None:
        """Agent pre-encodes string that gets double-encoded."""
        with pytest.raises(InputValidationError):
            validate_collection_id("%2e%2e")  # Would become %252e%252e

    def test_agent_generates_invisible_characters(self) -> None:
        """Agent generates control characters in string output."""
        with pytest.raises(InputValidationError):
            validate_item_id("census\x00\x01\x02data")

    def test_agent_constructs_traversal_url(self) -> None:
        """Agent constructs URL with path traversal."""
        with pytest.raises(InputValidationError):
            validate_remote_url("s3://bucket/../../root")
