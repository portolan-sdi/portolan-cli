"""Unit tests for CatalogModel dataclass.

Tests cover:
- Dataclass creation with required and optional fields
- JSON serialization (to_dict/from_dict)
- Validation rules (id pattern, timestamps)
- STAC compatibility
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

import pytest

# These will be implemented - tests first!
from portolan_cli.models.catalog import CatalogModel, Link


class TestCatalogModelCreation:
    """Tests for creating CatalogModel instances."""

    @pytest.mark.unit
    def test_create_catalog_with_required_fields(self) -> None:
        """CatalogModel can be created with only required fields."""
        catalog = CatalogModel(
            id="test-catalog",
            description="Test catalog",
        )

        assert catalog.id == "test-catalog"
        assert catalog.description == "Test catalog"
        assert catalog.type == "Catalog"
        assert catalog.stac_version == "1.0.0"

    @pytest.mark.unit
    def test_create_catalog_with_all_fields(self) -> None:
        """CatalogModel can be created with all fields including optional."""
        now = datetime.now(timezone.utc)
        catalog = CatalogModel(
            id="full-catalog",
            description="Full test catalog",
            title="Full Catalog",
            created=now,
            updated=now,
            links=[Link(rel="self", href="./catalog.json")],
        )

        assert catalog.title == "Full Catalog"
        assert catalog.created == now
        assert catalog.updated == now
        assert len(catalog.links) == 1

    @pytest.mark.unit
    def test_type_defaults_to_catalog(self) -> None:
        """type field should always be 'Catalog'."""
        catalog = CatalogModel(id="test", description="Test")
        assert catalog.type == "Catalog"

    @pytest.mark.unit
    def test_stac_version_defaults_to_1_0_0(self) -> None:
        """stac_version should default to '1.0.0'."""
        catalog = CatalogModel(id="test", description="Test")
        assert catalog.stac_version == "1.0.0"

    @pytest.mark.unit
    def test_links_defaults_to_empty_list(self) -> None:
        """links should default to empty list."""
        catalog = CatalogModel(id="test", description="Test")
        assert catalog.links == []


class TestCatalogModelValidation:
    """Tests for CatalogModel validation rules."""

    @pytest.mark.unit
    def test_id_must_match_pattern(self) -> None:
        """id must match pattern ^[a-zA-Z0-9_-]+$."""
        # Valid IDs
        valid_ids = ["test", "test-catalog", "test_catalog", "Test123", "a"]
        for valid_id in valid_ids:
            catalog = CatalogModel(id=valid_id, description="Test")
            assert catalog.id == valid_id

    @pytest.mark.unit
    def test_invalid_id_raises_error(self) -> None:
        """Invalid IDs should raise ValueError."""
        invalid_ids = ["test catalog", "test/catalog", "test.catalog", "", " "]
        for invalid_id in invalid_ids:
            with pytest.raises(ValueError, match="Invalid catalog id"):
                CatalogModel(id=invalid_id, description="Test")

    @pytest.mark.unit
    def test_id_pattern_regex(self) -> None:
        """Verify the ID pattern regex works correctly."""
        pattern = re.compile(r"^[a-zA-Z0-9_-]+$")
        assert pattern.match("valid-id")
        assert pattern.match("valid_id")
        assert pattern.match("ValidId123")
        assert not pattern.match("invalid id")
        assert not pattern.match("")


class TestCatalogModelSerialization:
    """Tests for CatalogModel JSON serialization."""

    @pytest.mark.unit
    def test_to_dict_includes_required_fields(self) -> None:
        """to_dict() must include all STAC-required fields."""
        catalog = CatalogModel(id="test", description="Test catalog")
        data = catalog.to_dict()

        assert data["type"] == "Catalog"
        assert data["stac_version"] == "1.0.0"
        assert data["id"] == "test"
        assert data["description"] == "Test catalog"
        assert "links" in data

    @pytest.mark.unit
    def test_to_dict_excludes_none_optional_fields(self) -> None:
        """to_dict() should exclude optional fields that are None."""
        catalog = CatalogModel(id="test", description="Test")
        data = catalog.to_dict()

        # title should not be in output if None
        assert "title" not in data or data.get("title") is None

    @pytest.mark.unit
    def test_to_dict_includes_timestamps_when_set(self) -> None:
        """to_dict() should include timestamps in ISO format."""
        now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        catalog = CatalogModel(id="test", description="Test", created=now, updated=now)
        data = catalog.to_dict()

        assert data["created"] == "2024-01-15T12:00:00+00:00"
        assert data["updated"] == "2024-01-15T12:00:00+00:00"

    @pytest.mark.unit
    def test_from_dict_creates_catalog(self) -> None:
        """from_dict() should create CatalogModel from dict."""
        data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test-catalog",
            "description": "Test",
            "links": [],
        }
        catalog = CatalogModel.from_dict(data)

        assert catalog.id == "test-catalog"
        assert catalog.description == "Test"
        assert catalog.type == "Catalog"

    @pytest.mark.unit
    def test_from_dict_with_timestamps(self) -> None:
        """from_dict() should parse ISO timestamps."""
        data = {
            "type": "Catalog",
            "stac_version": "1.0.0",
            "id": "test",
            "description": "Test",
            "links": [],
            "created": "2024-01-15T12:00:00+00:00",
            "updated": "2024-01-15T12:00:00+00:00",
        }
        catalog = CatalogModel.from_dict(data)

        assert catalog.created is not None
        assert catalog.created.year == 2024

    @pytest.mark.unit
    def test_roundtrip_serialization(self) -> None:
        """to_dict -> from_dict should preserve all data."""
        now = datetime.now(timezone.utc)
        original = CatalogModel(
            id="roundtrip-test",
            description="Test roundtrip",
            title="Roundtrip",
            created=now,
            updated=now,
            links=[Link(rel="self", href="./catalog.json")],
        )

        data = original.to_dict()
        restored = CatalogModel.from_dict(data)

        assert restored.id == original.id
        assert restored.description == original.description
        assert restored.title == original.title
        assert len(restored.links) == len(original.links)


class TestLink:
    """Tests for Link dataclass."""

    @pytest.mark.unit
    def test_create_link_with_required_fields(self) -> None:
        """Link can be created with only required fields."""
        link = Link(rel="self", href="./catalog.json")

        assert link.rel == "self"
        assert link.href == "./catalog.json"

    @pytest.mark.unit
    def test_create_link_with_all_fields(self) -> None:
        """Link can be created with all fields."""
        link = Link(
            rel="child",
            href="./collection/collection.json",
            type="application/json",
            title="My Collection",
        )

        assert link.type == "application/json"
        assert link.title == "My Collection"

    @pytest.mark.unit
    def test_link_to_dict(self) -> None:
        """Link.to_dict() returns correct dict."""
        link = Link(rel="self", href="./catalog.json", type="application/json")
        data = link.to_dict()

        assert data["rel"] == "self"
        assert data["href"] == "./catalog.json"
        assert data["type"] == "application/json"

    @pytest.mark.unit
    def test_link_from_dict(self) -> None:
        """Link.from_dict() creates Link from dict."""
        data = {"rel": "self", "href": "./catalog.json"}
        link = Link.from_dict(data)

        assert link.rel == "self"
        assert link.href == "./catalog.json"
