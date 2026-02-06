"""Tests for Catalog.init() - the Python API for initializing a catalog."""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.catalog import Catalog, CatalogExistsError


class TestCatalogInit:
    """Tests for Catalog.init() method."""

    @pytest.mark.unit
    def test_init_creates_portolan_directory(self, tmp_path: Path) -> None:
        """init() should create a .portolan directory."""
        Catalog.init(tmp_path)

        portolan_dir = tmp_path / ".portolan"
        assert portolan_dir.exists()
        assert portolan_dir.is_dir()

    @pytest.mark.unit
    def test_init_creates_catalog_json(self, tmp_path: Path) -> None:
        """init() should create a catalog.json file with valid STAC structure."""
        Catalog.init(tmp_path)

        catalog_file = tmp_path / ".portolan" / "catalog.json"
        assert catalog_file.exists()

    @pytest.mark.unit
    def test_init_catalog_json_has_required_stac_fields(self, tmp_path: Path) -> None:
        """catalog.json must have required STAC Catalog fields."""
        import json

        Catalog.init(tmp_path)

        catalog_file = tmp_path / ".portolan" / "catalog.json"
        catalog = json.loads(catalog_file.read_text())

        # Required STAC Catalog fields per spec
        assert catalog["type"] == "Catalog"
        assert catalog["stac_version"] == "1.0.0"
        assert "id" in catalog
        assert "description" in catalog
        assert "links" in catalog

    @pytest.mark.unit
    def test_init_raises_error_if_catalog_exists(self, tmp_path: Path) -> None:
        """init() should raise CatalogExistsError if .portolan already exists."""
        # Create existing catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        with pytest.raises(CatalogExistsError):
            Catalog.init(tmp_path)

    @pytest.mark.unit
    def test_catalog_exists_error_includes_path_in_message(self, tmp_path: Path) -> None:
        """CatalogExistsError message should include the path for debugging."""
        # Create existing catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        with pytest.raises(CatalogExistsError) as exc_info:
            Catalog.init(tmp_path)

        # Error message must include the path so user knows which catalog exists
        assert str(tmp_path) in str(exc_info.value), (
            f"Expected path '{tmp_path}' in error message, got: {exc_info.value}"
        )

    @pytest.mark.unit
    def test_catalog_exists_error_stores_path_attribute(self, tmp_path: Path) -> None:
        """CatalogExistsError should store the path as an attribute for programmatic access."""
        # Create existing catalog
        portolan_dir = tmp_path / ".portolan"
        portolan_dir.mkdir()

        with pytest.raises(CatalogExistsError) as exc_info:
            Catalog.init(tmp_path)

        # The .path attribute stores the actual .portolan directory path
        assert exc_info.value.path == portolan_dir
        assert exc_info.value.path is not None

    @pytest.mark.unit
    def test_init_returns_catalog_instance(self, tmp_path: Path) -> None:
        """init() should return a Catalog instance for chaining."""
        result = Catalog.init(tmp_path)

        assert isinstance(result, Catalog)
        assert result.root == tmp_path
