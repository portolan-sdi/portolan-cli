"""Unit tests for STAC metadata update functions.

Tests update_stac_metadata which patches title/description in
catalog.json and collection.json files after initial creation.

Per Issue #369: Extraction --auto mode should propagate rich metadata
from WFS/ArcGIS services to STAC files, not leave generic placeholders.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


class TestUpdateStacMetadata:
    """Tests for update_stac_metadata function."""

    @pytest.mark.unit
    def test_update_catalog_title_and_description(self, tmp_path: Path) -> None:
        """update_stac_metadata updates both title and description in catalog.json."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "description": "A Portolan-managed STAC catalog",
                    "stac_version": "1.1.0",
                    "links": [],
                }
            )
        )

        result = update_stac_metadata(
            catalog_path,
            title="Bâtiments INSPIRE",
            description="Couches de données spatiales des bâtiments",
        )

        assert result is True
        data = json.loads(catalog_path.read_text())
        assert data["title"] == "Bâtiments INSPIRE"
        assert data["description"] == "Couches de données spatiales des bâtiments"

    @pytest.mark.unit
    def test_update_collection_title_and_description(self, tmp_path: Path) -> None:
        """update_stac_metadata updates both title and description in collection.json."""
        from portolan_cli.stac import update_stac_metadata

        collection_path = tmp_path / "collection.json"
        collection_path.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "inspire_bu_bu_building_building_emprise_865a73",
                    "description": "Collection: inspire_bu_bu_building_building_emprise_865a73",
                    "stac_version": "1.1.0",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "license": "proprietary",
                }
            )
        )

        result = update_stac_metadata(
            collection_path,
            title="Building - building_emprise",
            description="Cette série de couches de données spatiales compile les informations du thème Bâtiments",
        )

        assert result is True
        data = json.loads(collection_path.read_text())
        assert data["title"] == "Building - building_emprise"
        assert (
            data["description"]
            == "Cette série de couches de données spatiales compile les informations du thème Bâtiments"
        )

    @pytest.mark.unit
    def test_update_title_only(self, tmp_path: Path) -> None:
        """update_stac_metadata can update just title, leaving description unchanged."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "description": "Original description",
                    "stac_version": "1.1.0",
                    "links": [],
                }
            )
        )

        result = update_stac_metadata(catalog_path, title="New Title")

        assert result is True
        data = json.loads(catalog_path.read_text())
        assert data["title"] == "New Title"
        assert data["description"] == "Original description"

    @pytest.mark.unit
    def test_update_description_only(self, tmp_path: Path) -> None:
        """update_stac_metadata can update just description, leaving title unchanged."""
        from portolan_cli.stac import update_stac_metadata

        collection_path = tmp_path / "collection.json"
        collection_path.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test",
                    "title": "Original Title",
                    "description": "Generic description",
                    "stac_version": "1.1.0",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "license": "proprietary",
                }
            )
        )

        result = update_stac_metadata(
            collection_path, description="Rich description from ISO 19139"
        )

        assert result is True
        data = json.loads(collection_path.read_text())
        assert data["title"] == "Original Title"
        assert data["description"] == "Rich description from ISO 19139"

    @pytest.mark.unit
    def test_no_update_when_both_none(self, tmp_path: Path) -> None:
        """update_stac_metadata returns False when nothing to update."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        original = {
            "type": "Catalog",
            "id": "test",
            "description": "Original",
            "stac_version": "1.1.0",
            "links": [],
        }
        catalog_path.write_text(json.dumps(original))

        result = update_stac_metadata(catalog_path, title=None, description=None)

        assert result is False
        # File should be unchanged
        data = json.loads(catalog_path.read_text())
        assert data == original

    @pytest.mark.unit
    def test_returns_false_for_missing_file(self, tmp_path: Path) -> None:
        """update_stac_metadata returns False when file doesn't exist."""
        from portolan_cli.stac import update_stac_metadata

        nonexistent = tmp_path / "nonexistent.json"

        result = update_stac_metadata(nonexistent, title="Title")

        assert result is False

    @pytest.mark.unit
    def test_preserves_other_fields(self, tmp_path: Path) -> None:
        """update_stac_metadata preserves all other JSON fields."""
        from portolan_cli.stac import update_stac_metadata

        collection_path = tmp_path / "collection.json"
        collection_path.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test-collection",
                    "description": "Old description",
                    "stac_version": "1.1.0",
                    "extent": {
                        "spatial": {"bbox": [[-122.5, 37.5, -122.0, 38.0]]},
                        "temporal": {"interval": [["2024-01-01T00:00:00Z", None]]},
                    },
                    "links": [{"rel": "self", "href": "./collection.json"}],
                    "license": "CC-BY-4.0",
                    "summaries": {"proj:code": ["EPSG:4326"]},
                    "keywords": ["buildings", "inspire"],
                }
            )
        )

        update_stac_metadata(collection_path, title="New Title", description="New Description")

        data = json.loads(collection_path.read_text())
        # Updated fields
        assert data["title"] == "New Title"
        assert data["description"] == "New Description"
        # Preserved fields
        assert data["id"] == "test-collection"
        assert data["stac_version"] == "1.1.0"
        assert data["extent"]["spatial"]["bbox"] == [[-122.5, 37.5, -122.0, 38.0]]
        assert data["links"] == [{"rel": "self", "href": "./collection.json"}]
        assert data["license"] == "CC-BY-4.0"
        assert data["summaries"] == {"proj:code": ["EPSG:4326"]}
        assert data["keywords"] == ["buildings", "inspire"]

    @pytest.mark.unit
    def test_skips_technical_names_for_title(self, tmp_path: Path) -> None:
        """update_stac_metadata skips technical-looking titles."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "title": "Human Readable Title",
                    "description": "Original",
                    "stac_version": "1.1.0",
                    "links": [],
                }
            )
        )

        # Technical name should be skipped
        result = update_stac_metadata(catalog_path, title="bu_building_emprise")

        assert result is False
        data = json.loads(catalog_path.read_text())
        assert data["title"] == "Human Readable Title"

    @pytest.mark.unit
    def test_skips_technical_names_for_description(self, tmp_path: Path) -> None:
        """update_stac_metadata skips technical-looking descriptions."""
        from portolan_cli.stac import update_stac_metadata

        collection_path = tmp_path / "collection.json"
        collection_path.write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "test",
                    "description": "Good existing description",
                    "stac_version": "1.1.0",
                    "extent": {
                        "spatial": {"bbox": [[-180, -90, 180, 90]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "license": "proprietary",
                }
            )
        )

        # Technical name should be skipped
        result = update_stac_metadata(collection_path, description="ns:layer_name_v2")

        assert result is False
        data = json.loads(collection_path.read_text())
        assert data["description"] == "Good existing description"

    @pytest.mark.unit
    def test_accepts_good_title_with_technical_description(self, tmp_path: Path) -> None:
        """update_stac_metadata can update title even if description is technical."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "description": "Original",
                    "stac_version": "1.1.0",
                    "links": [],
                }
            )
        )

        # Good title, technical description (description skipped, title applied)
        result = update_stac_metadata(
            catalog_path,
            title="Buildings Dataset",
            description="bu_building_v2",
        )

        assert result is True
        data = json.loads(catalog_path.read_text())
        assert data["title"] == "Buildings Dataset"
        assert data["description"] == "Original"  # Technical description skipped

    @pytest.mark.unit
    def test_unicode_metadata(self, tmp_path: Path) -> None:
        """update_stac_metadata handles Unicode characters correctly."""
        from portolan_cli.stac import update_stac_metadata

        catalog_path = tmp_path / "catalog.json"
        catalog_path.write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test",
                    "description": "Original",
                    "stac_version": "1.1.0",
                    "links": [],
                }
            )
        )

        result = update_stac_metadata(
            catalog_path,
            title="Données géographiques françaises",
            description="Cette couche contient les données des bâtiments avec caractères spéciaux: éàüö",
        )

        assert result is True
        data = json.loads(catalog_path.read_text())
        assert data["title"] == "Données géographiques françaises"
        assert "caractères spéciaux: éàüö" in data["description"]
