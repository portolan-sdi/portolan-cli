"""Tests for nested catalog inference (ADR-0032).

These tests verify that Portolan correctly:
1. Infers nested collection IDs (e.g., climate/hittekaart, not just climate)
2. Creates intermediate catalogs (catalog.json at theme directories)
3. Creates leaf collections (collection.json at data directories)
"""

from pathlib import Path

import pytest


class TestInferNestedCollectionId:
    """Test infer_nested_collection_id() function."""

    def test_single_level_nesting(self, tmp_path: Path) -> None:
        """Single level: demographics/data.parquet -> demographics."""
        from portolan_cli.dataset import infer_nested_collection_id

        catalog_root = tmp_path
        file_path = tmp_path / "demographics" / "data.parquet"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        result = infer_nested_collection_id(file_path, catalog_root)
        assert result == "demographics"

    def test_two_level_nesting(self, tmp_path: Path) -> None:
        """Two levels: climate/hittekaart/data.parquet -> climate/hittekaart."""
        from portolan_cli.dataset import infer_nested_collection_id

        catalog_root = tmp_path
        file_path = tmp_path / "climate" / "hittekaart" / "hittekaart.parquet"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        result = infer_nested_collection_id(file_path, catalog_root)
        assert result == "climate/hittekaart"

    def test_three_level_nesting(self, tmp_path: Path) -> None:
        """Three levels: env/air/quality/data.parquet -> env/air/quality."""
        from portolan_cli.dataset import infer_nested_collection_id

        catalog_root = tmp_path
        file_path = tmp_path / "env" / "air" / "quality" / "pm25.parquet"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        result = infer_nested_collection_id(file_path, catalog_root)
        assert result == "env/air/quality"

    def test_file_at_root_raises_error(self, tmp_path: Path) -> None:
        """File directly at catalog root should raise ValueError."""
        from portolan_cli.dataset import infer_nested_collection_id

        catalog_root = tmp_path
        file_path = tmp_path / "data.parquet"
        file_path.touch()

        with pytest.raises(ValueError, match="must be in a subdirectory"):
            infer_nested_collection_id(file_path, catalog_root)

    def test_file_outside_catalog_raises_error(self, tmp_path: Path) -> None:
        """File outside catalog root should raise ValueError."""
        from portolan_cli.dataset import infer_nested_collection_id

        catalog_root = tmp_path / "catalog"
        catalog_root.mkdir()
        file_path = tmp_path / "outside" / "data.parquet"
        file_path.parent.mkdir(parents=True)
        file_path.touch()

        with pytest.raises(ValueError, match="outside catalog root"):
            infer_nested_collection_id(file_path, catalog_root)


class TestCreateIntermediateCatalogs:
    """Test create_intermediate_catalogs() function."""

    def test_creates_catalog_at_theme_level(self, tmp_path: Path) -> None:
        """For climate/hittekaart, creates climate/catalog.json."""
        from portolan_cli.catalog import create_intermediate_catalogs

        catalog_root = tmp_path
        # Create root catalog first
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text('{"type": "Catalog", "id": "test", "links": []}')

        collection_id = "climate/hittekaart"
        create_intermediate_catalogs(collection_id, catalog_root)

        # Should create climate/catalog.json
        intermediate = catalog_root / "climate" / "catalog.json"
        assert intermediate.exists()

        import json

        content = json.loads(intermediate.read_text())
        assert content["type"] == "Catalog"
        assert content["id"] == "climate"

    def test_creates_multiple_intermediate_levels(self, tmp_path: Path) -> None:
        """For env/air/quality, creates env/catalog.json and env/air/catalog.json."""
        from portolan_cli.catalog import create_intermediate_catalogs

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text('{"type": "Catalog", "id": "test", "links": []}')

        collection_id = "env/air/quality"
        create_intermediate_catalogs(collection_id, catalog_root)

        # Should create env/catalog.json
        env_catalog = catalog_root / "env" / "catalog.json"
        assert env_catalog.exists()

        # Should create env/air/catalog.json
        air_catalog = catalog_root / "env" / "air" / "catalog.json"
        assert air_catalog.exists()

        import json

        env_content = json.loads(env_catalog.read_text())
        assert env_content["type"] == "Catalog"
        assert env_content["id"] == "env"

        air_content = json.loads(air_catalog.read_text())
        assert air_content["type"] == "Catalog"
        assert air_content["id"] == "env/air"

    def test_skips_existing_catalogs(self, tmp_path: Path) -> None:
        """Should not overwrite existing intermediate catalogs."""
        from portolan_cli.catalog import create_intermediate_catalogs

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text('{"type": "Catalog", "id": "test", "links": []}')

        # Pre-create climate/catalog.json with custom description
        climate_dir = catalog_root / "climate"
        climate_dir.mkdir()
        existing = climate_dir / "catalog.json"
        existing.write_text('{"type": "Catalog", "id": "climate", "description": "Custom"}')

        collection_id = "climate/hittekaart"
        create_intermediate_catalogs(collection_id, catalog_root)

        # Should preserve existing catalog
        import json

        content = json.loads(existing.read_text())
        assert content["description"] == "Custom"

    def test_single_level_collection_creates_no_intermediates(self, tmp_path: Path) -> None:
        """For single-level 'demographics', no intermediate catalogs needed."""
        from portolan_cli.catalog import create_intermediate_catalogs

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text('{"type": "Catalog", "id": "test", "links": []}')

        collection_id = "demographics"
        create_intermediate_catalogs(collection_id, catalog_root)

        # Should NOT create demographics/catalog.json (it will have collection.json)
        intermediate = catalog_root / "demographics" / "catalog.json"
        assert not intermediate.exists()

    def test_intermediate_catalog_links_to_parent(self, tmp_path: Path) -> None:
        """Intermediate catalogs should have correct parent/root links."""
        from portolan_cli.catalog import create_intermediate_catalogs

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text(
            '{"type": "Catalog", "id": "test", "stac_version": "1.1.0", "links": []}'
        )

        collection_id = "climate/hittekaart"
        create_intermediate_catalogs(collection_id, catalog_root)

        import json

        content = json.loads((catalog_root / "climate" / "catalog.json").read_text())

        # Check links
        links_by_rel = {link["rel"]: link for link in content.get("links", [])}
        assert "root" in links_by_rel
        assert links_by_rel["root"]["href"] == "../catalog.json"
        assert "parent" in links_by_rel
        assert links_by_rel["parent"]["href"] == "../catalog.json"


class TestUpdateCatalogLinksNested:
    """Test that catalog links correctly reference intermediate catalogs."""

    def test_root_links_to_intermediate_catalog(self, tmp_path: Path) -> None:
        """Root catalog should link to climate/catalog.json, not climate/collection.json."""
        from portolan_cli.catalog import (
            create_intermediate_catalogs,
            update_catalog_links_for_nested,
        )

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text(
            '{"type": "Catalog", "id": "test", "stac_version": "1.1.0", '
            '"description": "Test", "links": [{"rel": "root", "href": "./catalog.json"}]}'
        )

        collection_id = "climate/hittekaart"
        create_intermediate_catalogs(collection_id, catalog_root)
        update_catalog_links_for_nested(catalog_root, collection_id)

        import json

        content = json.loads(root_catalog.read_text())
        child_links = [link for link in content["links"] if link["rel"] == "child"]

        # Root should link to climate/catalog.json
        hrefs = [link["href"] for link in child_links]
        assert "./climate/catalog.json" in hrefs
        # Root should NOT link directly to climate/hittekaart/collection.json
        assert "./climate/hittekaart/collection.json" not in hrefs

    def test_intermediate_links_to_leaf_collection(self, tmp_path: Path) -> None:
        """Intermediate catalog should link to leaf collection."""
        from portolan_cli.catalog import (
            create_intermediate_catalogs,
            update_catalog_links_for_nested,
        )

        catalog_root = tmp_path
        root_catalog = catalog_root / "catalog.json"
        root_catalog.write_text(
            '{"type": "Catalog", "id": "test", "stac_version": "1.1.0", '
            '"description": "Test", "links": [{"rel": "root", "href": "./catalog.json"}]}'
        )

        collection_id = "climate/hittekaart"
        create_intermediate_catalogs(collection_id, catalog_root)
        update_catalog_links_for_nested(catalog_root, collection_id)

        import json

        intermediate = catalog_root / "climate" / "catalog.json"
        content = json.loads(intermediate.read_text())
        child_links = [link for link in content["links"] if link["rel"] == "child"]

        # Intermediate should link to hittekaart/collection.json
        hrefs = [link["href"] for link in child_links]
        assert "./hittekaart/collection.json" in hrefs
