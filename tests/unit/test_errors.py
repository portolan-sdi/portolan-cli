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
    # Conversion errors
    ConversionError,
    ConversionFailedError,
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
    UnsupportedFormatError,
    # Validation errors
    ValidationError,
    ValidationFailedError,
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

    @pytest.mark.unit
    def test_error_context_cannot_clobber_reserved_attrs(self) -> None:
        """Context keys cannot overwrite reserved attributes like code."""
        # Try to pass reserved names as context - they should be ignored
        # Note: 'message' is a positional arg, so we can't pass it as kwarg
        error = PortolanError(
            "Original message",
            code="EVIL-CODE",  # Should be ignored (reserved)
            context={"evil": True},  # Should be ignored (reserved)
            safe_key="this should work",  # Should work
        )

        # Reserved attributes should be preserved
        assert error.code == "PRTLN-000"  # Default, not overwritten
        assert error.message == "Original message"  # Not overwritten

        # Context dict contains what was passed, but reserved keys weren't set as attrs
        assert "code" in error.context
        assert error.context["code"] == "EVIL-CODE"

        # Safe keys should work
        assert error.safe_key == "this should work"


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
        """All error codes must match PRTLN-{CAT|COL|SCH|ITM|VER|VAL|CNV}NNN pattern."""
        import re

        pattern = re.compile(r"^PRTLN-(CAT|COL|SCH|ITM|VER|VAL|CNV)\d{3}$")

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
            # Conversion errors
            UnsupportedFormatError("/test", "netcdf"),
            ConversionFailedError("/test", ValueError("test")),
            ValidationFailedError("/test", ["error1"]),
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
            # Conversion errors
            UnsupportedFormatError("/test", "netcdf"),
            ConversionFailedError("/test", ValueError("test")),
            ValidationFailedError("/test", ["error1"]),
        ]

        codes = [e.code for e in errors]
        assert len(codes) == len(set(codes)), "Error codes are not unique"


class TestConversionErrors:
    """Tests for conversion-related error classes.

    Conversion errors use the PRTLN-CNV prefix:
    - PRTLN-CNV000: ConversionError (base)
    - PRTLN-CNV001: UnsupportedFormatError
    - PRTLN-CNV002: ConversionFailedError
    - PRTLN-CNV003: ValidationFailedError
    """

    @pytest.mark.unit
    def test_conversion_error_base_code(self) -> None:
        """ConversionError has PRTLN-CNV prefix."""
        error = ConversionError("Generic conversion error")
        assert error.code.startswith("PRTLN-CNV")

    @pytest.mark.unit
    def test_conversion_error_is_portolan_error(self) -> None:
        """ConversionError inherits from PortolanError."""
        error = ConversionError("Test error")
        assert isinstance(error, PortolanError)

    @pytest.mark.unit
    def test_conversion_error_base_has_default_code(self) -> None:
        """ConversionError base class has PRTLN-CNV000 code."""
        error = ConversionError("Generic conversion error")
        assert error.code == "PRTLN-CNV000"

    @pytest.mark.unit
    def test_unsupported_format_error_code(self) -> None:
        """UnsupportedFormatError has PRTLN-CNV001 code."""
        error = UnsupportedFormatError("/path/to/file.netcdf", "netcdf")

        assert error.code == "PRTLN-CNV001"

    @pytest.mark.unit
    def test_unsupported_format_error_has_path(self) -> None:
        """UnsupportedFormatError stores path in context."""
        error = UnsupportedFormatError("/path/to/file.hdf5", "hdf5")

        assert error.path == "/path/to/file.hdf5"

    @pytest.mark.unit
    def test_unsupported_format_error_has_format_type(self) -> None:
        """UnsupportedFormatError stores format_type in context."""
        error = UnsupportedFormatError("/path/to/file.las", "las")

        assert error.format_type == "las"

    @pytest.mark.unit
    def test_unsupported_format_error_message(self) -> None:
        """UnsupportedFormatError has descriptive message."""
        error = UnsupportedFormatError("/path/to/file.netcdf", "netcdf")

        assert "/path/to/file.netcdf" in str(error)
        assert "netcdf" in str(error)

    @pytest.mark.unit
    def test_conversion_failed_error_code(self) -> None:
        """ConversionFailedError has PRTLN-CNV002 code."""
        original_error = ValueError("Something went wrong")
        error = ConversionFailedError("/path/to/file.shp", original_error)

        assert error.code == "PRTLN-CNV002"

    @pytest.mark.unit
    def test_conversion_failed_error_has_path(self) -> None:
        """ConversionFailedError stores path in context."""
        original_error = ValueError("Something went wrong")
        error = ConversionFailedError("/path/to/file.shp", original_error)

        assert error.path == "/path/to/file.shp"

    @pytest.mark.unit
    def test_conversion_failed_error_has_original_error(self) -> None:
        """ConversionFailedError stores original_error in context."""
        original_error = ValueError("Something went wrong")
        error = ConversionFailedError("/path/to/file.shp", original_error)

        assert error.original_error == original_error

    @pytest.mark.unit
    def test_conversion_failed_error_message(self) -> None:
        """ConversionFailedError has descriptive message."""
        original_error = ValueError("Something went wrong")
        error = ConversionFailedError("/path/to/file.shp", original_error)

        assert "/path/to/file.shp" in str(error)
        assert "Something went wrong" in str(error)

    @pytest.mark.unit
    def test_validation_failed_error_code(self) -> None:
        """ValidationFailedError has PRTLN-CNV003 code."""
        validation_errors = ["Missing bbox metadata", "Invalid geometry type"]
        error = ValidationFailedError("/path/to/output.parquet", validation_errors)

        assert error.code == "PRTLN-CNV003"

    @pytest.mark.unit
    def test_validation_failed_error_has_path(self) -> None:
        """ValidationFailedError stores path in context."""
        validation_errors = ["Missing bbox metadata"]
        error = ValidationFailedError("/path/to/output.parquet", validation_errors)

        assert error.path == "/path/to/output.parquet"

    @pytest.mark.unit
    def test_validation_failed_error_has_validation_errors(self) -> None:
        """ValidationFailedError stores validation_errors in context."""
        validation_errors = ["Missing bbox metadata", "Invalid geometry type"]
        error = ValidationFailedError("/path/to/output.parquet", validation_errors)

        assert error.validation_errors == validation_errors

    @pytest.mark.unit
    def test_validation_failed_error_message(self) -> None:
        """ValidationFailedError has descriptive message."""
        validation_errors = ["Missing bbox metadata", "Invalid geometry type"]
        error = ValidationFailedError("/path/to/output.parquet", validation_errors)

        assert "/path/to/output.parquet" in str(error)
        # Message should mention validation failure
        assert "validation" in str(error).lower() or "failed" in str(error).lower()
