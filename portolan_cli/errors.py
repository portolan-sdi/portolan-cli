"""Structured error codes for Portolan.

All errors follow the format PRTLN-{category}{number}:
- PRTLN-CAT*: Catalog errors
- PRTLN-COL*: Collection errors
- PRTLN-SCH*: Schema errors
- PRTLN-ITM*: Item errors
- PRTLN-VER*: Version errors
- PRTLN-VAL*: Validation errors
- PRTLN-CNV*: Conversion errors
- PRTLN-LOG*: Catalog logo errors
- PRTLN-CFG*: Configuration errors
- PRTLN-EXT*: Extract / harvest errors
- PRTLN-SYN*: Sync errors (push, pull)
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
    by Portolan (missing .portolan/config.yaml).
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


class AssetKeyCollisionError(CollectionError):
    """Raised when two different files claim one collection asset key.

    Error code: PRTLN-COL003
    """

    code = "PRTLN-COL003"

    def __init__(
        self, collection_id: str, asset_key: str, existing_href: str, new_href: str
    ) -> None:
        super().__init__(
            f"Collection '{collection_id}': asset key '{asset_key}' already holds "
            f"'{existing_href}'. Adding '{new_href}' under the same key would remove "
            "that asset. Remove one of the two files from the collection, or rename it.",
            collection_id=collection_id,
            asset_key=asset_key,
            existing_href=existing_href,
            new_href=new_href,
        )


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


class NoGeometryError(ValueError):
    """Raised when a file lacks geometry needed to create a STAC item.

    This is a ValueError subclass so it integrates naturally with existing
    exception handling (callers catching ValueError will still catch this).
    It replaces fragile string-pattern matching with a typed check:
        ``isinstance(err, NoGeometryError)``

    Used by ``_pre_validate_geometry`` and ``add`` when a file has
    no geometry metadata (e.g., tabular parquet without ``geo`` key, GeoJSON
    with no features, etc.).
    """

    def __init__(self, path: str, reason: str) -> None:
        self.path = path
        self.reason = reason
        super().__init__(f"Cannot create STAC item for '{path}': missing bounding box. {reason}")


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


class InvalidProvidersError(ValidationError):
    """Raised when metadata.yaml declares a providers array Portolan cannot use.

    Generation tolerates a missing providers block — the host is seeded from
    ``contact`` and PTL-PRV-001 tells the human to name a producer. It cannot
    tolerate input where no conformant array exists, such as two entries both
    claiming the ``host`` role: nothing can choose between them.

    Error code: PRTLN-VAL003
    """

    code = "PRTLN-VAL003"

    def __init__(self, reason: str) -> None:
        super().__init__(f"Invalid providers in metadata.yaml: {reason}", reason=reason)


class MissingLicenseError(ValidationError):
    """Raised when Portolan is about to write a collection with no usable license.

    A license is a legal fact Portolan cannot invent, and a collection without
    one fails PTL-LIC-001 or PTL-LIC-002 the moment ``portolan check`` reads it.
    Rather than write a catalog it knows to be non-conformant, generation stops
    and names what the human has to supply (issue #686).

    Error code: PRTLN-VAL004
    """

    code = "PRTLN-VAL004"

    def __init__(self, target: str, reason: str) -> None:
        super().__init__(f"No usable license for {target}: {reason}", target=target, reason=reason)


class InvalidLiveUrlError(ValidationError):
    """The live pass requires an HTTPS base URL with a host and a valid port.

    Error code: PRTLN-VAL005
    """

    code = "PRTLN-VAL005"

    def __init__(self, source: str) -> None:
        super().__init__(
            f"--live needs a valid https base URL. Set {source} to a valid https URL.",
            source=source,
        )


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


class CRSMismatchError(ConversionError):
    """Raised when bbox coordinates don't match the declared CRS.

    Error code: PRTLN-CNV004

    This indicates a common data quality issue where a file declares a projected
    CRS (e.g., EPSG:28992) but actually contains WGS84 coordinates. This can
    happen when ArcGIS services return WGS84 data but declare the original
    projection.
    """

    code = "PRTLN-CNV004"

    def __init__(
        self,
        source_crs: str,
        bbox: tuple[float, float, float, float],
        likely_actual_crs: str = "EPSG:4326",
    ) -> None:
        super().__init__(
            f"CRS mismatch: declares {source_crs} but coordinates "
            f"({bbox[0]:.2f}, {bbox[1]:.2f}, {bbox[2]:.2f}, {bbox[3]:.2f}) "
            f"appear to be {likely_actual_crs}. The data may have been "
            "reprojected but the CRS metadata was not updated.",
            source_crs=source_crs,
            bbox=bbox,
            likely_actual_crs=likely_actual_crs,
        )


class RewriteFidelityError(ConversionError):
    """Raised when an in-place rewrite would lose part of the source file.

    Error code: PRTLN-CNV005

    ``add`` rewrites a GeoParquet in place to give it a bbox covering column
    (issue #805). That replaces the operator's own file, so the rewrite proves
    it kept every row, every column, and the declared CRS before it swaps.
    geoparquet-io writes no CRS, so a projected GeoParquet trips this check.

    Portolan keeps the operator's file when this fires. It does not repair the
    rewritten copy. The file stays non-conformant, and ``check`` reports it.
    """

    code = "PRTLN-CNV005"

    def __init__(self, path: str, lost: str) -> None:
        super().__init__(
            f"the rewrite of {path} lost {lost}",
            path=path,
            lost=lost,
        )
        self.lost = lost


class InvalidOutputError(ConversionError):
    """Raised when a format handler writes a file that fails its own validation.

    Error code: PRTLN-CNV008
    """

    code = "PRTLN-CNV008"

    def __init__(self, path: str, reason: str) -> None:
        super().__init__(f"{path} failed validation: {reason}", path=path, reason=reason)
        self.reason = reason


class PartitionCollisionError(ConversionError):
    """Raised when a size partition would write into a directory that holds partitions.

    Error code: PRTLN-CNV009

    geoparquet-io names each Hive file after its cell and does not write over a
    cell that exists. A second write into the same directory would lose the
    rows in each shared cell, so Portolan keeps the converted file and stops.
    """

    code = "PRTLN-CNV009"

    def __init__(self, path: str, directory: str, existing: list[str]) -> None:
        shown = ", ".join(existing[:3]) + (", ..." if len(existing) > 3 else "")
        super().__init__(
            f"cannot partition {path}: {directory} already holds partitions ({shown}). "
            "Remove them, or put each large file in its own collection.",
            path=path,
            directory=directory,
            existing=existing,
        )


class OutputExistsError(ConversionError):
    """Raised when a conversion would write over a file that its source did not make.

    Error code: PRTLN-CNV010

    A converter names its output after the source stem, so ``b1.csv`` and
    ``b1.geojson`` both convert to ``b1.parquet``. When that name holds a file
    and no earlier add tracked the source, the file belongs to the operator.
    Portolan keeps it and does not convert.
    """

    code = "PRTLN-CNV010"

    def __init__(self, source: str, output: str) -> None:
        super().__init__(
            f"converting {source} would replace {output}, which no add of {source} wrote. "
            "Rename one of the two files.",
            source=source,
            output=output,
        )


class AmbiguousFormatError(ConversionError):
    """Raised when two plugin format handlers claim one file and no pin decides.

    Error code: PRTLN-CNV006
    """

    code = "PRTLN-CNV006"

    def __init__(self, path: str, extension: str, handlers: list[str]) -> None:
        names = ", ".join(f"'{name}'" for name in handlers)
        super().__init__(
            f"{names} all claim {path}. Pin one in .portolan/config.yaml under "
            f"conversion.handlers, for example '{extension}: {handlers[0]}'.",
            path=path,
            extension=extension,
            handlers=handlers,
        )


class UnknownFormatHandlerError(ConversionError):
    """Raised when conversion.handlers pins a handler that no plugin provides.

    Error code: PRTLN-CNV007
    """

    code = "PRTLN-CNV007"

    def __init__(self, extension: str, handler: str, available: list[str]) -> None:
        super().__init__(
            f"conversion.handlers pins '{handler}' for {extension}, but no installed "
            f"format handler has that name. Available: {', '.join(available)}.",
            extension=extension,
            handler=handler,
            available=available,
        )


# Logo Errors (PRTLN-LOG*)
class LogoError(PortolanError):
    """Base class for catalog-logo errors."""

    code = "PRTLN-LOG000"


class UnsupportedLogoFormatError(LogoError):
    """Raised when a logo's media type is outside the seven the spec permits.

    Error code: PRTLN-LOG001

    PORTO-CORE-075 fixes the enum because a client drops an icon whose media
    type it does not recognize, so an unlisted type renders nowhere.
    """

    code = "PRTLN-LOG001"

    def __init__(self, path: str, allowed: list[str]) -> None:
        super().__init__(
            f"Unsupported logo format for {path}. Permitted media types: {', '.join(allowed)}",
            path=path,
            allowed=allowed,
        )


class LogoSourceNotFoundError(LogoError):
    """Raised when the logo source path is not an existing file.

    Error code: PRTLN-LOG002
    """

    code = "PRTLN-LOG002"

    def __init__(self, path: str) -> None:
        super().__init__(f"Logo source is not a file: {path}", path=path)


class RemoteLogoSourceError(LogoError):
    """Raised when the logo source is a URL rather than a local path.

    Error code: PRTLN-LOG003

    Portolan copies the image into the catalog so the published href stays
    relative. Fetching a remote image is the caller's job: download it first,
    then point at the downloaded file.
    """

    code = "PRTLN-LOG003"

    def __init__(self, source: str) -> None:
        super().__init__(
            f"Logo source must be a local file, got a URL: {source}. "
            "Download the image first, then pass the local path.",
            source=source,
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


class InsecureS3EndpointError(ConfigError):
    """Raised when an S3 endpoint would send credentials over plaintext HTTP.

    Error code: PRTLN-CFG003
    """

    code = "PRTLN-CFG003"

    def __init__(self, endpoint: str) -> None:
        super().__init__(
            "Refusing to send S3 credentials over plaintext HTTP. "
            "Use HTTPS or remove the credentials for anonymous access.",
            endpoint=endpoint,
        )


class ProfileCredentialsError(ConfigError):
    """Raised when an AWS profile fails to produce credentials.

    Error code: PRTLN-CFG004
    """

    code = "PRTLN-CFG004"

    def __init__(self, profile: str, reason: str) -> None:
        super().__init__(
            f"AWS profile '{profile}' failed to supply credentials: {reason}",
            profile=profile,
            reason=reason,
        )


# Sync Errors (PRTLN-SYN*)
class SyncError(PortolanError):
    """Base class for push and pull errors."""

    code = "PRTLN-SYN000"


class PushVerifyError(SyncError):
    """Raised when ``push --verify`` cannot safely check or repair an object.

    A failed HEAD, missing ETag, or local checksum mismatch prevents a safe
    repair. Push stops to keep the bytes consistent with the manifest.

    Error code: PRTLN-SYN001
    """

    code = "PRTLN-SYN001"

    def __init__(self, failures: list[str]) -> None:
        shown = "; ".join(failures[:5])
        more = f" (and {len(failures) - 5} more)" if len(failures) > 5 else ""
        super().__init__(
            f"Could not verify {len(failures)} remote object(s): {shown}{more}",
            failures=failures,
        )


# Extract Errors (PRTLN-EXT*)
class ArcGISAuthError(PortolanError):
    """Raised when ArcGIS token resolution or authentication fails.

    Error code: PRTLN-EXT002
    """

    code = "PRTLN-EXT002"
