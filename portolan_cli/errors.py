"""Structured error codes for Portolan.

All errors follow the format PRTLN-{category}{number}:
- PRTLN-CAT*: Catalog errors
- PRTLN-COL*: Collection errors
- PRTLN-SCH*: Schema errors
- PRTLN-ITM*: Item errors
- PRTLN-VER*: Version errors
- PRTLN-VAL*: Validation errors
- PRTLN-CNV*: Conversion errors
- PRTLN-CFG*: Configuration errors
"""

from __future__ import annotations

from typing import Any


class PortolanError(Exception):
    """Base class for all Portolan errors.

    All errors have:
    - code: Structured error code (e.g., PRTLN-CAT001)
    - message: Human-readable error message
    """

    code: str = "PRTLN-000"

    # Reserved attribute names that cannot be overwritten by context
    _RESERVED_ATTRS = frozenset({"code", "message", "context", "args"})

    def __init__(self, message: str, **context: Any) -> None:
        """Initialize a Portolan error.

        Args:
            message: Human-readable error message.
            **context: Additional context stored as error attributes.
                Reserved keys (code, message, context, args) are ignored.
        """
        self.message = message
        self.context = context
        for key, value in context.items():
            # Skip reserved attributes to prevent clobbering
            if key not in self._RESERVED_ATTRS:
                setattr(self, key, value)
        super().__init__(f"[{self.code}] {message}")

    def to_dict(self) -> dict[str, Any]:
        """Convert error to JSON-serializable dict."""
        return {
            "code": self.code,
            "message": self.message,
            "context": self.context,
        }


# Catalog Errors (PRTLN-CAT*)
class CatalogError(PortolanError):
    """Base class for catalog-related errors."""

    code = "PRTLN-CAT000"


class CatalogAlreadyExistsError(CatalogError):
    """Raised when attempting to initialize a catalog that already exists.

    Error code: PRTLN-CAT001
    """

    code = "PRTLN-CAT001"

    def __init__(self, path: str) -> None:
        super().__init__(f"Catalog already exists at {path}", path=path)


class CatalogNotFoundError(CatalogError):
    """Raised when a catalog is required but not found.

    Error code: PRTLN-CAT002
    """

    code = "PRTLN-CAT002"

    def __init__(self, path: str) -> None:
        super().__init__(f"No catalog found at {path}", path=path)


class UnmanagedStacCatalogError(CatalogError):
    """Raised when an existing STAC catalog is found but not managed by Portolan.

    Error code: PRTLN-CAT003

    This occurs when a directory has a catalog.json file but is not managed
    by Portolan (missing .portolan/config.json and .portolan/state.json).
    Use `portolan adopt` to bring an existing STAC catalog under management.
    """

    code = "PRTLN-CAT003"

    def __init__(self, path: str) -> None:
        super().__init__(
            f"Existing STAC catalog found at {path}, use 'portolan adopt' to manage it",
            path=path,
        )


# Collection Errors (PRTLN-COL*)
class CollectionError(PortolanError):
    """Base class for collection-related errors."""

    code = "PRTLN-COL000"


class CollectionAlreadyExistsError(CollectionError):
    """Raised when attempting to create a collection that already exists.

    Error code: PRTLN-COL001
    """

    code = "PRTLN-COL001"

    def __init__(self, collection_id: str) -> None:
        super().__init__(
            f"Collection '{collection_id}' already exists", collection_id=collection_id
        )


class CollectionNotFoundError(CollectionError):
    """Raised when a collection is required but not found.

    Error code: PRTLN-COL002
    """

    code = "PRTLN-COL002"

    def __init__(self, collection_id: str) -> None:
        super().__init__(f"Collection '{collection_id}' not found", collection_id=collection_id)


# Schema Errors (PRTLN-SCH*)
class SchemaError(PortolanError):
    """Base class for schema-related errors."""

    code = "PRTLN-SCH000"


class SchemaExtractionError(SchemaError):
    """Raised when schema cannot be extracted from a file.

    Error code: PRTLN-SCH001
    """

    code = "PRTLN-SCH001"

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"Cannot extract schema from {path}: {reason}", path=path, reason=reason)


class SchemaTypeConflictError(SchemaError):
    """Raised when imported schema has incompatible type changes.

    Error code: PRTLN-SCH002
    """

    code = "PRTLN-SCH002"

    def __init__(self, column: str, current_type: str, new_type: str) -> None:
        super().__init__(
            f"Cannot change column '{column}' type from '{current_type}' to '{new_type}'",
            column=column,
            current_type=current_type,
            new_type=new_type,
        )


class SchemaColumnNotFoundError(SchemaError):
    """Raised when a referenced column doesn't exist.

    Error code: PRTLN-SCH003
    """

    code = "PRTLN-SCH003"

    def __init__(self, column: str) -> None:
        super().__init__(f"Column '{column}' not found in schema", column=column)


# Item Errors (PRTLN-ITM*)
class ItemError(PortolanError):
    """Base class for item-related errors."""

    code = "PRTLN-ITM000"


class ItemNotFoundError(ItemError):
    """Raised when an item is required but not found.

    Error code: PRTLN-ITM001
    """

    code = "PRTLN-ITM001"

    def __init__(self, item_id: str, collection_id: str) -> None:
        super().__init__(
            f"Item '{item_id}' not found in collection '{collection_id}'",
            item_id=item_id,
            collection_id=collection_id,
        )


class ItemAlreadyExistsError(ItemError):
    """Raised when attempting to create an item that already exists.

    Error code: PRTLN-ITM002
    """

    code = "PRTLN-ITM002"

    def __init__(self, item_id: str, collection_id: str) -> None:
        super().__init__(
            f"Item '{item_id}' already exists in collection '{collection_id}'",
            item_id=item_id,
            collection_id=collection_id,
        )


# Version Errors (PRTLN-VER*)
class VersionError(PortolanError):
    """Base class for version-related errors."""

    code = "PRTLN-VER000"


class VersionNotFoundError(VersionError):
    """Raised when a version is required but not found.

    Error code: PRTLN-VER001
    """

    code = "PRTLN-VER001"

    def __init__(self, version: str, collection_id: str) -> None:
        super().__init__(
            f"Version '{version}' not found in collection '{collection_id}'",
            version=version,
            collection_id=collection_id,
        )


class InvalidVersionError(VersionError):
    """Raised when a version string is not valid semantic version.

    Error code: PRTLN-VER002
    """

    code = "PRTLN-VER002"

    def __init__(self, version: str) -> None:
        super().__init__(f"Invalid semantic version: '{version}'", version=version)


# Validation Errors (PRTLN-VAL*)
class ValidationError(PortolanError):
    """Base class for validation-related errors."""

    code = "PRTLN-VAL000"


class MissingGeometryError(ValidationError):
    """Raised when a GeoParquet file has no geometry column.

    Error code: PRTLN-VAL001
    """

    code = "PRTLN-VAL001"

    def __init__(self, path: str) -> None:
        super().__init__(f"No geometry column found in {path}", path=path)


class InvalidBboxError(ValidationError):
    """Raised when a bounding box is invalid.

    Error code: PRTLN-VAL002
    """

    code = "PRTLN-VAL002"

    def __init__(self, reason: str) -> None:
        super().__init__(f"Invalid bounding box: {reason}", reason=reason)


# Conversion Errors (PRTLN-CNV*)
class ConversionError(PortolanError):
    """Base class for conversion-related errors."""

    code = "PRTLN-CNV000"


class UnsupportedFormatError(ConversionError):
    """Raised when a file format is not supported for conversion.

    Error code: PRTLN-CNV001
    """

    code = "PRTLN-CNV001"

    def __init__(self, path: str, format_type: str) -> None:
        super().__init__(
            f"Unsupported format '{format_type}' for file {path}",
            path=path,
            format_type=format_type,
        )


class ConversionFailedError(ConversionError):
    """Raised when file conversion fails due to an error.

    Error code: PRTLN-CNV002
    """

    code = "PRTLN-CNV002"

    def __init__(self, path: str, original_error: Exception) -> None:
        # Store serializable representations for to_dict()/JSON
        super().__init__(
            f"Conversion failed for {path}: {original_error}",
            path=path,
            original_error_type=type(original_error).__name__,
            original_error_message=str(original_error),
        )
        # Keep original exception for programmatic access (not serialized)
        self.original_exception = original_error


class ValidationFailedError(ConversionError):
    """Raised when converted output fails validation.

    Error code: PRTLN-CNV003
    """

    code = "PRTLN-CNV003"

    def __init__(self, path: str, validation_errors: list[str]) -> None:
        error_count = len(validation_errors)
        errors_summary = "; ".join(validation_errors[:3])
        if error_count > 3:
            errors_summary += f"... and {error_count - 3} more"
        super().__init__(
            f"Validation failed for {path}: {errors_summary}",
            path=path,
            validation_errors=validation_errors,
        )


# Configuration Errors (PRTLN-CFG*)
class ConfigError(PortolanError):
    """Base class for configuration-related errors."""

    code = "PRTLN-CFG000"


class ConfigParseError(ConfigError):
    """Raised when a configuration file cannot be parsed.

    Error code: PRTLN-CFG001
    """

    code = "PRTLN-CFG001"

    def __init__(self, path: str, parse_error: str) -> None:
        super().__init__(
            f"Failed to parse config file {path}: {parse_error}",
            path=path,
            parse_error=parse_error,
        )


class ConfigInvalidStructureError(ConfigError):
    """Raised when a configuration file has an invalid structure.

    Error code: PRTLN-CFG002
    """

    code = "PRTLN-CFG002"

    def __init__(self, path: str, detail: str) -> None:
        super().__init__(
            f"Invalid config structure in {path}: {detail}",
            path=path,
            detail=detail,
        )
