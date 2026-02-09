# Feature Specification: Cloud-Native Dataset Warnings

**Feature Branch**: `002-cloud-native-warnings`
**Created**: 2025-02-09
**Status**: Draft
**Input**: User description: "Non-cloud-native dataset handling: When a user adds a dataset, check if it's cloud-native (GeoParquet, COG, FlatGeobuf, etc.), convertible (Shapefile, GeoJSON, GeoPackage, etc.), or unsupported (NetCDF, HDF5, etc.). Cloud-native files pass through silently. Convertible files emit a single-line warning then convert. Unsupported files are rejected with a helpful 'coming soon' error."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Add Cloud-Native Dataset (Priority: P1)

A user adds a cloud-native file (GeoParquet, COG, FlatGeobuf, PMTiles, COPC, Parquet, Zarr, Raquet) to their catalog. The system accepts it silently without any warnings or conversion steps.

**Why this priority**: This is the happy path for users already using best practices. Cloud-native formats are the target state, so accepting them seamlessly is the core behavior.

**Independent Test**: Can be fully tested by adding a GeoParquet file and verifying no warnings are emitted and the file passes through unchanged.

**Acceptance Scenarios**:

1. **Given** a user has a valid GeoParquet file, **When** they add it to the catalog, **Then** the file is accepted silently with no warning messages
2. **Given** a user has a valid COG file, **When** they add it to the catalog, **Then** the file is accepted silently with no warning or re-conversion
3. **Given** a user has a valid FlatGeobuf file, **When** they add it to the catalog, **Then** the file is accepted silently

---

### User Story 2 - Add Convertible Non-Cloud-Native Dataset (Priority: P1)

A user adds a convertible non-cloud-native file (Shapefile, GeoJSON, GeoPackage, CSV with geometry, JP2, non-COG TIFF). The system emits a single-line warning indicating the format is not cloud-native, then automatically converts it to the appropriate cloud-native format.

**Why this priority**: This is the primary use case for the feature. Most users will have legacy formats that need conversion, so clear feedback during this transition is essential.

**Independent Test**: Can be fully tested by adding a Shapefile and verifying (1) the warning message appears in the expected format and (2) the file is converted to GeoParquet.

**Acceptance Scenarios**:

1. **Given** a user has a Shapefile, **When** they add it to the catalog, **Then** the system emits "⚠ SHP is not cloud-native. Converting to GeoParquet." and converts the file
2. **Given** a user has a GeoJSON file, **When** they add it to the catalog, **Then** the system emits "⚠ GeoJSON is not cloud-native. Converting to GeoParquet." and converts the file
3. **Given** a user has a GeoPackage file, **When** they add it to the catalog, **Then** the system emits "⚠ GPKG is not cloud-native. Converting to GeoParquet." and converts the file
4. **Given** a user has a non-COG TIFF file, **When** they add it to the catalog, **Then** the system emits "⚠ TIFF is not cloud-native. Converting to COG." and converts the file
5. **Given** a user has a JP2 file, **When** they add it to the catalog, **Then** the system emits "⚠ JP2 is not cloud-native. Converting to COG." and converts the file

---

### User Story 3 - Reject Unsupported Dataset (Priority: P2)

A user attempts to add an unsupported file format (NetCDF, HDF5, LAS/LAZ non-COPC). The system rejects the file with a clear, helpful error message indicating the format is not yet supported and suggesting it may be available in the future.

**Why this priority**: This provides a good user experience for edge cases, preventing silent failures and setting expectations about future support.

**Independent Test**: Can be fully tested by attempting to add a NetCDF file and verifying the rejection message appears with helpful guidance.

**Acceptance Scenarios**:

1. **Given** a user has a NetCDF file, **When** they attempt to add it to the catalog, **Then** the system rejects it with an error message indicating NetCDF is not yet supported
2. **Given** a user has an HDF5 file, **When** they attempt to add it to the catalog, **Then** the system rejects it with an error message indicating HDF5 is not yet supported
3. **Given** a user has a LAS file (non-COPC), **When** they attempt to add it to the catalog, **Then** the system rejects it with an error message indicating LAS is not yet supported

---

### Edge Cases

- What happens when a file has an ambiguous extension (e.g., `.tif` that could be COG or non-COG)? → System must detect actual format, not rely solely on extension
- How does system handle a file with no extension? → System should attempt format detection or reject with clear error
- What happens when conversion fails mid-process? → System should emit error and leave original file unchanged
- How does system handle a file that claims to be GeoParquet but is invalid? → Defer to geoparquet-io validation; emit appropriate error

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: System MUST classify input files into one of three categories: cloud-native, convertible, or unsupported
- **FR-002**: System MUST accept cloud-native files (GeoParquet, Parquet, COG, FlatGeobuf, COPC, PMTiles, Zarr, Raquet) without emitting warnings
- **FR-003**: System MUST emit a single-line warning for convertible files before converting them
- **FR-004**: Warning message MUST follow the format: "⚠ {FORMAT} is not cloud-native. Converting to {TARGET}."
- **FR-005**: System MUST convert vector formats (Shapefile, GeoJSON, GeoPackage, CSV) to GeoParquet
- **FR-006**: System MUST convert raster formats (non-COG TIFF, JP2) to COG
- **FR-007**: System MUST reject unsupported files (NetCDF, HDF5, LAS/LAZ non-COPC) with helpful error messages
- **FR-008**: Rejection error messages MUST indicate the format is "not yet supported" (implying future support)
- **FR-009**: System MUST detect actual file format, not rely solely on file extension
- **FR-010**: System MUST NOT re-convert files that are already in cloud-native format (e.g., a COG should not be re-converted)

### Key Entities

- **CloudNativeStatus**: Represents the classification of a file format (CLOUD_NATIVE, CONVERTIBLE, UNSUPPORTED)
- **FormatMapping**: Maps source formats to their target cloud-native formats and display abbreviations

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Cloud-native files are processed without any warning output to the user
- **SC-002**: Convertible files show exactly one warning line before conversion begins
- **SC-003**: Unsupported files are rejected immediately with a clear error message (no partial processing)
- **SC-004**: Users can distinguish between "needs conversion" and "not supported" based on message type (warning vs error)
- **SC-005**: All format detection operates on file content/headers, not solely on file extensions

## Assumptions

- The existing format conversion logic (geoparquet-io, rio-cogeo) remains unchanged; this feature adds classification and user feedback layer
- The `output.py` module's `warn()` and `error()` functions will be used for consistent terminal output
- Format detection will leverage existing libraries (e.g., fiona for vectors, rasterio for rasters) to identify actual format
- COPC is treated as cloud-native; non-COPC LAS/LAZ is treated as unsupported
- Parquet (non-geo) and Raquet are included in cloud-native category per the design specification

## Out of Scope

- Adding support for currently unsupported formats (NetCDF, HDF5, LAS/LAZ)
- Batch processing or progress indicators for multiple file conversions
- User preferences to skip conversion or override warnings
- Plugin architecture for adding new format handlers (covered by ADR-0003 separately)
