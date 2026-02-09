# Implementation Plan: Cloud-Native Dataset Warnings

**Branch**: `002-cloud-native-warnings` | **Date**: 2025-02-09 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/002-cloud-native-warnings/spec.md`

## Summary

Add cloud-native status classification to the format detection layer. Files are classified as CLOUD_NATIVE (pass through silently), CONVERTIBLE (emit warning then convert), or UNSUPPORTED (reject with helpful error). This feature adds a user feedback layer above the existing format conversion pipeline without changing the conversion logic itself.

## Technical Context

**Language/Version**: Python 3.10+
**Primary Dependencies**: click (CLI), geoparquet-io (vector conversion), rio-cogeo (raster conversion), rasterio (COG detection)
**Storage**: N/A (file-based, no database)
**Testing**: pytest with markers (unit, integration), hypothesis for property-based tests
**Target Platform**: Linux/macOS/Windows CLI
**Project Type**: Single project (CLI wraps library)
**Performance Goals**: Format detection < 100ms per file (current behavior maintained)
**Constraints**: No changes to upstream library integration; use existing output.py for terminal messages
**Scale/Scope**: Single-file format detection; no batch processing in this feature

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Status | Notes |
|-----------|--------|-------|
| I. TDD (Non-Negotiable) | ✅ PASS | Tests will be written first for each classification case |
| II. Geospatial Edge Cases | ✅ PASS | Format detection edge cases identified (ambiguous extensions, COG vs non-COG TIFF) |
| III. Real-World Fixtures | ✅ PASS | Will use minimal but real GeoParquet, COG, Shapefile test files |
| IV. Stay Current Upstream | ✅ PASS | Delegates to rio-cogeo for COG detection; no reimplementation |
| V. Scope Before Build | ✅ PASS | Spec complete with acceptance scenarios and edge cases |
| VI. CLI Wraps API | ✅ PASS | Logic in formats.py; CLI in cli.py is thin wrapper |

**Gate Status**: PASS - All principles satisfied.

## Project Structure

### Documentation (this feature)

```text
specs/002-cloud-native-warnings/
├── plan.md              # This file
├── research.md          # Phase 0 output (COG detection patterns)
├── data-model.md        # Phase 1 output (CloudNativeStatus enum)
├── quickstart.md        # Phase 1 output (usage examples)
├── contracts/           # N/A (no API contracts for this feature)
├── checklists/          # Quality checklists
│   └── requirements.md  # Spec quality validation
└── tasks.md             # Phase 2 output (/speckit.tasks command)
```

### Source Code (repository root)

```text
portolan_cli/
├── formats.py           # Add CloudNativeStatus enum, get_cloud_native_status()
├── dataset.py           # Check status before conversion, emit warning if CONVERTIBLE
├── output.py            # Existing warn()/error() functions (no changes needed)
└── validation/          # No changes expected

tests/
├── unit/
│   └── test_formats.py  # Unit tests for CloudNativeStatus classification
├── integration/
│   └── test_dataset.py  # Integration tests for warning output during add
└── fixtures/
    └── formats/         # Minimal test files for each format category
```

**Structure Decision**: Follows existing single-project structure. Changes are localized to `formats.py` (classification logic) and `dataset.py` (warning emission). Uses existing `output.py` for terminal messages.

## Complexity Tracking

> No violations requiring justification. Implementation is minimal and follows existing patterns.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| (none) | N/A | N/A |
