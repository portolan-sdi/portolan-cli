"""Tests for catalog creation using the new CatalogModel.

User Story 1: Initialize Catalog with Auto-Extracted Metadata

Tests cover:
- Auto-extraction of id from directory name
- Auto-generation of timestamps
- --auto flag behavior
- PRTLN-CAT001 error for existing catalog
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from portolan_cli.catalog import _sanitize_id, create_catalog
from portolan_cli.errors import CatalogAlreadyExistsError


class TestSanitizeIdEdgeCases:
    """Edge case tests for _sanitize_id function."""

    @pytest.mark.unit
    def test_empty_string_returns_default(self) -> None:
        """Empty string input should return 'catalog' default."""
        assert _sanitize_id("") == "catalog"

    @pytest.mark.unit
    def test_only_invalid_chars_returns_default(self) -> None:
        """String with only invalid characters should return 'catalog' default."""
        assert _sanitize_id("!!!@@@###") == "catalog"
        assert _sanitize_id("...") == "catalog"
        assert _sanitize_id("   ") == "catalog"

    @pytest.mark.unit
    def test_unicode_characters_sanitized(self) -> None:
        """Unicode characters should be converted to hyphens."""
        result = _sanitize_id("données")
        import re

        assert re.match(r"^[a-zA-Z0-9_-]+$", result)
        # The accented characters should be replaced
        assert "é" not in result
        assert "ée" not in result

    @pytest.mark.unit
    def test_leading_trailing_hyphens_removed(self) -> None:
        """Leading and trailing hyphens should be stripped."""
        assert _sanitize_id("-test-") == "test"
        assert _sanitize_id("---test---") == "test"

    @pytest.mark.unit
    def test_multiple_hyphens_collapsed(self) -> None:
        """Multiple consecutive hyphens should be collapsed to one."""
        result = _sanitize_id("my---data---catalog")
        assert "---" not in result
        assert result == "my-data-catalog"

    @pytest.mark.unit
    def test_spaces_converted_to_hyphens(self) -> None:
        """Spaces should be converted to hyphens."""
        result = _sanitize_id("my data catalog")
        assert " " not in result
        assert result == "my-data-catalog"

    @pytest.mark.unit
    def test_valid_characters_preserved(self) -> None:
        """Valid alphanumeric, hyphens, underscores should be preserved."""
        assert _sanitize_id("valid_id-123") == "valid_id-123"
        assert _sanitize_id("Test_Catalog-2024") == "Test_Catalog-2024"


class TestCreateCatalogAutoExtraction:
    """Tests for auto-extracted fields in catalog creation."""

    @pytest.mark.unit
    def test_id_extracted_from_directory_name(self, tmp_path: Path) -> None:
        """Catalog id should be auto-extracted from the directory name."""
        # Create a directory with a specific name
        catalog_dir = tmp_path / "my-data-catalog"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.id == "my-data-catalog"

    @pytest.mark.unit
    def test_id_sanitized_for_invalid_characters(self, tmp_path: Path) -> None:
        """Directory names with invalid characters should be sanitized."""
        # Directories with spaces/special chars should be converted
        catalog_dir = tmp_path / "My Data Catalog 2024"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        # ID should only contain alphanumeric, hyphens, underscores
        assert " " not in catalog.id
        # The exact sanitization logic depends on implementation
        # At minimum, the ID should be valid
        import re

        assert re.match(r"^[a-zA-Z0-9_-]+$", catalog.id)

    @pytest.mark.unit
    def test_id_sanitized_for_unicode_characters(self, tmp_path: Path) -> None:
        """Directory names with unicode characters should be sanitized."""
        # Unicode characters should be converted to hyphens
        catalog_dir = tmp_path / "données-géographiques"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        import re

        assert re.match(r"^[a-zA-Z0-9_-]+$", catalog.id)
        assert "é" not in catalog.id

    @pytest.mark.unit
    def test_id_sanitized_for_very_long_directory_name(self, tmp_path: Path) -> None:
        """Very long directory names should be sanitized gracefully."""
        # Long names should still produce a valid ID
        long_name = "a" * 200
        catalog_dir = tmp_path / long_name
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        import re

        assert re.match(r"^[a-zA-Z0-9_-]+$", catalog.id)
        assert len(catalog.id) > 0

    @pytest.mark.unit
    def test_created_timestamp_auto_generated(self, tmp_path: Path) -> None:
        """created timestamp should be auto-generated."""
        catalog_dir = tmp_path / "test-catalog"
        catalog_dir.mkdir()

        before = datetime.now(timezone.utc)
        catalog = create_catalog(catalog_dir)
        after = datetime.now(timezone.utc)

        assert catalog.created is not None
        assert before <= catalog.created <= after

    @pytest.mark.unit
    def test_updated_timestamp_auto_generated(self, tmp_path: Path) -> None:
        """updated timestamp should be auto-generated and equal to created."""
        catalog_dir = tmp_path / "test-catalog"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.updated is not None
        assert catalog.created == catalog.updated

    @pytest.mark.unit
    def test_type_set_to_catalog(self, tmp_path: Path) -> None:
        """type should always be 'Catalog'."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.type == "Catalog"

    @pytest.mark.unit
    def test_stac_version_set_to_1_0_0(self, tmp_path: Path) -> None:
        """stac_version should be '1.0.0'."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.stac_version == "1.0.0"

    @pytest.mark.unit
    def test_links_default_to_empty(self, tmp_path: Path) -> None:
        """links should default to empty list."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.links == []


class TestCreateCatalogOptionalFields:
    """Tests for user-provided optional fields."""

    @pytest.mark.unit
    def test_title_can_be_provided(self, tmp_path: Path) -> None:
        """title can be provided as parameter."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir, title="My Awesome Catalog")

        assert catalog.title == "My Awesome Catalog"

    @pytest.mark.unit
    def test_description_can_be_provided(self, tmp_path: Path) -> None:
        """description can be provided as parameter."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir, description="A catalog of geospatial data")

        assert catalog.description == "A catalog of geospatial data"

    @pytest.mark.unit
    def test_description_has_default(self, tmp_path: Path) -> None:
        """description should have a default value."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.description is not None
        assert len(catalog.description) > 0


class TestWriteCatalogJson:
    """Tests for writing catalog.json file."""

    @pytest.mark.unit
    def test_writes_catalog_json_file(self, tmp_path: Path) -> None:
        """write_catalog_json should create catalog.json in .portolan/."""
        from portolan_cli.catalog import write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)
        write_catalog_json(catalog, catalog_dir)

        catalog_file = catalog_dir / ".portolan" / "catalog.json"
        assert catalog_file.exists()

    @pytest.mark.unit
    def test_catalog_json_is_valid_json(self, tmp_path: Path) -> None:
        """catalog.json should be valid JSON."""
        from portolan_cli.catalog import write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)
        write_catalog_json(catalog, catalog_dir)

        catalog_file = catalog_dir / ".portolan" / "catalog.json"
        data = json.loads(catalog_file.read_text())

        assert isinstance(data, dict)

    @pytest.mark.unit
    def test_catalog_json_has_stac_fields(self, tmp_path: Path) -> None:
        """catalog.json should have all required STAC fields."""
        from portolan_cli.catalog import write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)
        write_catalog_json(catalog, catalog_dir)

        catalog_file = catalog_dir / ".portolan" / "catalog.json"
        data = json.loads(catalog_file.read_text())

        assert data["type"] == "Catalog"
        assert data["stac_version"] == "1.0.0"
        assert "id" in data
        assert "description" in data
        assert "links" in data

    @pytest.mark.unit
    def test_catalog_json_includes_timestamps(self, tmp_path: Path) -> None:
        """catalog.json should include created and updated timestamps."""
        from portolan_cli.catalog import write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)
        write_catalog_json(catalog, catalog_dir)

        catalog_file = catalog_dir / ".portolan" / "catalog.json"
        data = json.loads(catalog_file.read_text())

        assert "created" in data
        assert "updated" in data
        # Verify ISO format
        datetime.fromisoformat(data["created"])
        datetime.fromisoformat(data["updated"])


class TestCatalogExistsError:
    """Tests for PRTLN-CAT001 error when catalog exists."""

    @pytest.mark.unit
    def test_create_catalog_raises_if_exists(self, tmp_path: Path) -> None:
        """create_catalog should raise CatalogAlreadyExistsError if .portolan exists."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()
        (catalog_dir / ".portolan").mkdir()

        with pytest.raises(CatalogAlreadyExistsError) as exc_info:
            create_catalog(catalog_dir)

        assert exc_info.value.code == "PRTLN-CAT001"

    @pytest.mark.unit
    def test_error_includes_path(self, tmp_path: Path) -> None:
        """Error should include the path in the message."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()
        (catalog_dir / ".portolan").mkdir()

        with pytest.raises(CatalogAlreadyExistsError) as exc_info:
            create_catalog(catalog_dir)

        assert str(catalog_dir) in str(exc_info.value) or "test" in str(exc_info.value)


class TestAutoFlag:
    """Tests for --auto flag behavior."""

    @pytest.mark.unit
    def test_create_catalog_without_prompts(self, tmp_path: Path) -> None:
        """create_catalog should work without user input (library function)."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog is not None
        assert catalog.id == "test"

    @pytest.mark.unit
    def test_create_catalog_returns_warnings(self, tmp_path: Path) -> None:
        """create_catalog should return warnings for missing best-practice fields."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog, warnings = create_catalog(catalog_dir, return_warnings=True)

        # Should warn about missing title
        assert any("title" in w.lower() for w in warnings)

    @pytest.mark.unit
    def test_create_catalog_sets_default_description(self, tmp_path: Path) -> None:
        """create_catalog should set a default description when not provided."""
        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        catalog = create_catalog(catalog_dir)

        assert catalog.description is not None
        assert len(catalog.description) > 0


class TestInitCatalogFilesystemErrors:
    """Tests for CatalogInitError when filesystem operations fail."""

    @pytest.mark.unit
    def test_init_catalog_raises_on_root_mkdir_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when root directory creation fails."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        # Patch Path.mkdir to fail
        original_mkdir = Path.mkdir

        def failing_mkdir(self, *args, **kwargs):
            if "nonexistent-parent" in str(self):
                raise OSError("Permission denied")
            return original_mkdir(self, *args, **kwargs)

        with patch.object(Path, "mkdir", failing_mkdir):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(tmp_path / "nonexistent-parent" / "catalog")
            assert "Cannot create directory" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_portolan_mkdir_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when .portolan directory creation fails."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_mkdir = Path.mkdir

        def failing_mkdir(self, *args, **kwargs):
            if ".portolan" in str(self):
                raise OSError("Disk full")
            return original_mkdir(self, *args, **kwargs)

        with patch.object(Path, "mkdir", failing_mkdir):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot create .portolan directory" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_config_write_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when config.json write fails."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_write_text = Path.write_text

        def failing_write_text(self, *args, **kwargs):
            if "config.json" in str(self):
                raise OSError("Disk full")
            return original_write_text(self, *args, **kwargs)

        with patch.object(Path, "write_text", failing_write_text):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot write config.json" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_state_write_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when state.json write fails."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_write_text = Path.write_text

        def failing_write_text(self, *args, **kwargs):
            if "state.json" in str(self):
                raise OSError("Disk full")
            return original_write_text(self, *args, **kwargs)

        with patch.object(Path, "write_text", failing_write_text):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot write state.json" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_versions_write_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when versions.json write fails."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_write_text = Path.write_text

        def failing_write_text(self, *args, **kwargs):
            if "versions.json" in str(self):
                raise OSError("Disk full")
            return original_write_text(self, *args, **kwargs)

        with patch.object(Path, "write_text", failing_write_text):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot write versions.json" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_catalog_save_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when pystac catalog save fails."""
        from unittest.mock import patch

        import pystac

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        def failing_save(self, *args, **kwargs):
            raise OSError("Cannot write catalog")

        with patch.object(pystac.Catalog, "save", failing_save):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot write catalog.json" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_self_link_update_failure(self, tmp_path: Path) -> None:
        """CatalogInitError raised when adding self link fails."""
        import json
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_write_text = Path.write_text

        def failing_write_text(self, content, *args, **kwargs):
            # Only fail on the self-link update (when content contains "self" link)
            if "catalog.json" in str(self):
                try:
                    data = json.loads(content)
                    if any(link.get("rel") == "self" for link in data.get("links", [])):
                        raise OSError("Cannot update catalog")
                except json.JSONDecodeError:
                    pass
            return original_write_text(self, content, *args, **kwargs)

        with patch.object(Path, "write_text", failing_write_text):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot update catalog.json with self link" in exc_info.value.message

    @pytest.mark.unit
    def test_init_catalog_raises_on_catalog_json_decode_error(self, tmp_path: Path) -> None:
        """CatalogInitError raised when catalog.json is corrupted."""
        from unittest.mock import patch

        from portolan_cli.catalog import CatalogInitError, init_catalog

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original_read_text = Path.read_text

        def corrupted_read_text(self, *args, **kwargs):
            if "catalog.json" in str(self):
                return "not valid json {"
            return original_read_text(self, *args, **kwargs)

        with patch.object(Path, "read_text", corrupted_read_text):
            with pytest.raises(CatalogInitError) as exc_info:
                init_catalog(catalog_dir)
            assert "Cannot parse catalog.json" in exc_info.value.message


class TestCatalogRoundtrip:
    """Tests for saving and loading catalog."""

    @pytest.mark.unit
    def test_saved_catalog_can_be_loaded(self, tmp_path: Path) -> None:
        """A saved catalog can be loaded back as CatalogModel."""
        from portolan_cli.catalog import read_catalog_json, write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original = create_catalog(catalog_dir, title="Test Catalog")
        write_catalog_json(original, catalog_dir)

        loaded = read_catalog_json(catalog_dir)

        assert loaded.id == original.id
        assert loaded.title == original.title
        assert loaded.description == original.description

    @pytest.mark.unit
    def test_loaded_catalog_preserves_timestamps(self, tmp_path: Path) -> None:
        """Loaded catalog should have the same timestamps as saved."""
        from portolan_cli.catalog import read_catalog_json, write_catalog_json

        catalog_dir = tmp_path / "test"
        catalog_dir.mkdir()

        original = create_catalog(catalog_dir)
        write_catalog_json(original, catalog_dir)

        loaded = read_catalog_json(catalog_dir)

        # Timestamps should match (within ISO format precision)
        assert loaded.created.isoformat() == original.created.isoformat()
