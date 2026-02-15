"""Unit tests for Portolan error classes.

Tests cover:
- Base PortolanError behavior
- Error codes format (PRTLN-{category}{number})
- Error to_dict serialization
- Specific error types for each category
"""

from __future__ import annotations

import pytest

from portolan_cli.errors import (
    CatalogAlreadyExistsError,
    # Catalog errors
    CatalogError,
    CatalogNotFoundError,
    CollectionAlreadyExistsError,
    # Collection errors
    CollectionError,
    CollectionNotFoundError,
    InvalidBboxError,
    InvalidVersionError,
    ItemAlreadyExistsError,
    # Item errors
    ItemError,
    ItemNotFoundError,
    MissingGeometryError,
    PortolanError,
    SchemaColumnNotFoundError,
    # Schema errors
    SchemaError,
    SchemaExtractionError,
    SchemaTypeConflictError,
    # Validation errors
    ValidationError,
    # Version errors
    VersionError,
    VersionNotFoundError,
)


class TestPortolanError:
    """Tests for base PortolanError class."""

    @pytest.mark.unit
    def test_error_has_code_and_message(self) -> None:
        """PortolanError must have code and message attributes."""
        error = PortolanError("Test error message")

        assert hasattr(error, "code")
        assert hasattr(error, "message")
        assert error.message == "Test error message"

    @pytest.mark.unit
    def test_error_str_includes_code(self) -> None:
        """Error string representation should include code."""
        error = PortolanError("Test message")

        assert error.code in str(error)
        assert "Test message" in str(error)

    @pytest.mark.unit
    def test_error_to_dict(self) -> None:
        """PortolanError.to_dict() returns structured error data."""
        error = PortolanError("Test message", extra="value")
        data = error.to_dict()

        assert data["code"] == error.code
        assert data["message"] == "Test message"
        assert data["context"]["extra"] == "value"

    @pytest.mark.unit
    def test_error_stores_context(self) -> None:
        """PortolanError stores additional context as attributes."""
        error = PortolanError("Test", foo="bar", count=42)

        assert error.foo == "bar"
        assert error.count == 42


class TestCatalogErrors:
    """Tests for catalog-related error classes."""

    @pytest.mark.unit
    def test_catalog_error_base_code(self) -> None:
        """CatalogError has PRTLN-CAT prefix."""
        error = CatalogError("Generic catalog error")
        assert error.code.startswith("PRTLN-CAT")

    @pytest.mark.unit
    def test_catalog_already_exists_error(self) -> None:
        """CatalogAlreadyExistsError has correct code and stores path."""
        error = CatalogAlreadyExistsError("/path/to/catalog")

        assert error.code == "PRTLN-CAT001"
        assert error.path == "/path/to/catalog"
        assert "/path/to/catalog" in str(error)

    @pytest.mark.unit
    def test_catalog_not_found_error(self) -> None:
        """CatalogNotFoundError has correct code and stores path."""
        error = CatalogNotFoundError("/path/to/missing")

        assert error.code == "PRTLN-CAT002"
        assert error.path == "/path/to/missing"


class TestCollectionErrors:
    """Tests for collection-related error classes."""

    @pytest.mark.unit
    def test_collection_error_base_code(self) -> None:
        """CollectionError has PRTLN-COL prefix."""
        error = CollectionError("Generic collection error")
        assert error.code.startswith("PRTLN-COL")

    @pytest.mark.unit
    def test_collection_already_exists_error(self) -> None:
        """CollectionAlreadyExistsError has correct code."""
        error = CollectionAlreadyExistsError("my-collection")

        assert error.code == "PRTLN-COL001"
        assert error.collection_id == "my-collection"
        assert "my-collection" in str(error)

    @pytest.mark.unit
    def test_collection_not_found_error(self) -> None:
        """CollectionNotFoundError has correct code."""
        error = CollectionNotFoundError("missing-collection")

        assert error.code == "PRTLN-COL002"
        assert error.collection_id == "missing-collection"


class TestSchemaErrors:
    """Tests for schema-related error classes."""

    @pytest.mark.unit
    def test_schema_error_base_code(self) -> None:
        """SchemaError has PRTLN-SCH prefix."""
        error = SchemaError("Generic schema error")
        assert error.code.startswith("PRTLN-SCH")

    @pytest.mark.unit
    def test_schema_extraction_error(self) -> None:
        """SchemaExtractionError has correct code and context."""
        error = SchemaExtractionError("/path/to/file.parquet", "No geometry column")

        assert error.code == "PRTLN-SCH001"
        assert error.path == "/path/to/file.parquet"
        assert error.reason == "No geometry column"

    @pytest.mark.unit
    def test_schema_type_conflict_error(self) -> None:
        """SchemaTypeConflictError has correct code and context."""
        error = SchemaTypeConflictError("population", "int64", "geometry")

        assert error.code == "PRTLN-SCH002"
        assert error.column == "population"
        assert error.current_type == "int64"
        assert error.new_type == "geometry"
        assert "int64" in str(error)
        assert "geometry" in str(error)

    @pytest.mark.unit
    def test_schema_column_not_found_error(self) -> None:
        """SchemaColumnNotFoundError has correct code."""
        error = SchemaColumnNotFoundError("missing_column")

        assert error.code == "PRTLN-SCH003"
        assert error.column == "missing_column"


class TestItemErrors:
    """Tests for item-related error classes."""

    @pytest.mark.unit
    def test_item_error_base_code(self) -> None:
        """ItemError has PRTLN-ITM prefix."""
        error = ItemError("Generic item error")
        assert error.code.startswith("PRTLN-ITM")

    @pytest.mark.unit
    def test_item_not_found_error(self) -> None:
        """ItemNotFoundError has correct code and context."""
        error = ItemNotFoundError("item-001", "my-collection")

        assert error.code == "PRTLN-ITM001"
        assert error.item_id == "item-001"
        assert error.collection_id == "my-collection"

    @pytest.mark.unit
    def test_item_already_exists_error(self) -> None:
        """ItemAlreadyExistsError has correct code."""
        error = ItemAlreadyExistsError("item-002", "my-collection")

        assert error.code == "PRTLN-ITM002"
        assert error.item_id == "item-002"


class TestVersionErrors:
    """Tests for version-related error classes."""

    @pytest.mark.unit
    def test_version_error_base_code(self) -> None:
        """VersionError has PRTLN-VER prefix."""
        error = VersionError("Generic version error")
        assert error.code.startswith("PRTLN-VER")

    @pytest.mark.unit
    def test_version_not_found_error(self) -> None:
        """VersionNotFoundError has correct code and context."""
        error = VersionNotFoundError("1.0.0", "my-collection")

        assert error.code == "PRTLN-VER001"
        assert error.version == "1.0.0"
        assert error.collection_id == "my-collection"

    @pytest.mark.unit
    def test_invalid_version_error(self) -> None:
        """InvalidVersionError has correct code."""
        error = InvalidVersionError("not-a-version")

        assert error.code == "PRTLN-VER002"
        assert error.version == "not-a-version"


class TestValidationErrors:
    """Tests for validation-related error classes."""

    @pytest.mark.unit
    def test_validation_error_base_code(self) -> None:
        """ValidationError has PRTLN-VAL prefix."""
        error = ValidationError("Generic validation error")
        assert error.code.startswith("PRTLN-VAL")

    @pytest.mark.unit
    def test_missing_geometry_error(self) -> None:
        """MissingGeometryError has correct code."""
        error = MissingGeometryError("/path/to/data.parquet")

        assert error.code == "PRTLN-VAL001"
        assert error.path == "/path/to/data.parquet"

    @pytest.mark.unit
    def test_invalid_bbox_error(self) -> None:
        """InvalidBboxError has correct code."""
        error = InvalidBboxError("min_lon > 180")

        assert error.code == "PRTLN-VAL002"
        assert error.reason == "min_lon > 180"


class TestErrorCodeFormat:
    """Tests for error code format consistency."""

    @pytest.mark.unit
    def test_all_error_codes_match_pattern(self) -> None:
        """All error codes must match PRTLN-{CAT|COL|SCH|ITM|VER|VAL}NNN pattern."""
        import re

        pattern = re.compile(r"^PRTLN-(CAT|COL|SCH|ITM|VER|VAL)\d{3}$")

        errors = [
            CatalogAlreadyExistsError("/test"),
            CatalogNotFoundError("/test"),
            CollectionAlreadyExistsError("test"),
            CollectionNotFoundError("test"),
            SchemaExtractionError("/test", "reason"),
            SchemaTypeConflictError("col", "int", "str"),
            SchemaColumnNotFoundError("col"),
            ItemNotFoundError("item", "col"),
            ItemAlreadyExistsError("item", "col"),
            VersionNotFoundError("1.0.0", "col"),
            InvalidVersionError("bad"),
            MissingGeometryError("/test"),
            InvalidBboxError("reason"),
        ]

        for error in errors:
            assert pattern.match(error.code), f"Error code '{error.code}' doesn't match pattern"

    @pytest.mark.unit
    def test_error_codes_are_unique(self) -> None:
        """Each error type should have a unique code."""
        errors = [
            CatalogAlreadyExistsError("/test"),
            CatalogNotFoundError("/test"),
            CollectionAlreadyExistsError("test"),
            CollectionNotFoundError("test"),
            SchemaExtractionError("/test", "reason"),
            SchemaTypeConflictError("col", "int", "str"),
            SchemaColumnNotFoundError("col"),
            ItemNotFoundError("item", "col"),
            ItemAlreadyExistsError("item", "col"),
            VersionNotFoundError("1.0.0", "col"),
            InvalidVersionError("bad"),
            MissingGeometryError("/test"),
            InvalidBboxError("reason"),
        ]

        codes = [e.code for e in errors]
        assert len(codes) == len(set(codes)), "Error codes are not unique"
