# Tasks: Cloud-Native Dataset Warnings

**Input**: Design documents from `/specs/002-cloud-native-warnings/`
**Prerequisites**: plan.md, spec.md, research.md, data-model.md, quickstart.md

**Tests**: Required per Constitution Principle I (TDD is non-negotiable)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (e.g., US1, US2, US3)
- Include exact file paths in descriptions

## Path Conventions

- **Single project**: `portolan_cli/`, `tests/` at repository root
- Paths follow existing project structure per plan.md

---

## Phase 1: Setup

**Purpose**: Prepare test fixtures and foundational infrastructure

- [ ] T001 Create test fixtures directory at tests/fixtures/formats/
- [ ] T002 [P] Create minimal GeoParquet test fixture at tests/fixtures/formats/cloud_native.parquet
- [ ] T003 [P] Create minimal COG test fixture at tests/fixtures/formats/cloud_native.tif
- [ ] T004 [P] Create minimal Shapefile test fixture at tests/fixtures/formats/convertible.shp
- [ ] T005 [P] Create minimal non-COG TIFF test fixture at tests/fixtures/formats/convertible.tif
- [ ] T006 [P] Create README.md documenting test fixtures at tests/fixtures/formats/README.md

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core enums and data structures that ALL user stories depend on

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Tests for Foundational

- [ ] T007 [P] Unit test for CloudNativeStatus enum in tests/unit/test_formats_status.py
- [ ] T008 [P] Unit test for FormatInfo dataclass in tests/unit/test_formats_status.py

### Implementation for Foundational

- [ ] T009 Add CloudNativeStatus enum (CLOUD_NATIVE, CONVERTIBLE, UNSUPPORTED) in portolan_cli/formats.py
- [ ] T010 Add FormatInfo dataclass with status, display_name, target_format, error_message in portolan_cli/formats.py
- [ ] T011 Add UnsupportedFormatError exception in portolan_cli/formats.py

**Checkpoint**: Foundation ready - CloudNativeStatus and FormatInfo are available for all user stories

---

## Phase 3: User Story 1 - Add Cloud-Native Dataset (Priority: P1) 🎯 MVP

**Goal**: Accept cloud-native files silently without warnings

**Independent Test**: Add a GeoParquet file and verify no warnings are emitted

### Tests for User Story 1

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T012 [P] [US1] Unit test: GeoParquet returns CLOUD_NATIVE status in tests/unit/test_formats_cloud_native.py
- [ ] T013 [P] [US1] Unit test: COG returns CLOUD_NATIVE status in tests/unit/test_formats_cloud_native.py
- [ ] T014 [P] [US1] Unit test: FlatGeobuf returns CLOUD_NATIVE status in tests/unit/test_formats_cloud_native.py
- [ ] T015 [P] [US1] Integration test: add_dataset with GeoParquet emits no warnings in tests/integration/test_dataset_warnings.py

### Implementation for User Story 1

- [ ] T016 [P] [US1] Add is_geoparquet() helper function in portolan_cli/formats.py
- [ ] T017 [P] [US1] Add is_cloud_optimized_geotiff() helper function in portolan_cli/formats.py
- [ ] T018 [US1] Add CLOUD_NATIVE_EXTENSIONS frozenset (.fgb, .copc.laz, .pmtiles, .zarr, .raquet) in portolan_cli/formats.py
- [ ] T019 [US1] Implement get_cloud_native_status() for cloud-native formats in portolan_cli/formats.py
- [ ] T020 [US1] Verify no output emitted for CLOUD_NATIVE status in portolan_cli/dataset.py (no changes needed if status check returns early)

**Checkpoint**: Cloud-native files pass through silently - User Story 1 complete

---

## Phase 4: User Story 2 - Add Convertible Non-Cloud-Native Dataset (Priority: P1)

**Goal**: Emit warning for convertible files, then proceed with conversion

**Independent Test**: Add a Shapefile and verify warning message appears in expected format

### Tests for User Story 2

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T021 [P] [US2] Unit test: Shapefile returns CONVERTIBLE status with "SHP" display_name in tests/unit/test_formats_convertible.py
- [ ] T022 [P] [US2] Unit test: GeoJSON returns CONVERTIBLE status with "GeoJSON" display_name in tests/unit/test_formats_convertible.py
- [ ] T023 [P] [US2] Unit test: non-COG TIFF returns CONVERTIBLE status with "TIFF" display_name in tests/unit/test_formats_convertible.py
- [ ] T024 [P] [US2] Integration test: add_dataset with Shapefile emits warning message in tests/integration/test_dataset_warnings.py
- [ ] T025 [P] [US2] Integration test: warning message matches format "⚠ {FORMAT} is not cloud-native. Converting to {TARGET}." in tests/integration/test_dataset_warnings.py

### Implementation for User Story 2

- [ ] T026 [US2] Add CONVERTIBLE_VECTOR_EXTENSIONS frozenset (.shp, .geojson, .gpkg, .csv) in portolan_cli/formats.py
- [ ] T027 [US2] Add CONVERTIBLE_RASTER_EXTENSIONS frozenset (.jp2) in portolan_cli/formats.py
- [ ] T028 [US2] Add FORMAT_DISPLAY_NAMES mapping (extension → display name) in portolan_cli/formats.py
- [ ] T029 [US2] Extend get_cloud_native_status() for convertible vector formats in portolan_cli/formats.py
- [ ] T030 [US2] Extend get_cloud_native_status() for convertible raster formats in portolan_cli/formats.py
- [ ] T031 [US2] Extend get_cloud_native_status() to detect non-COG TIFF (use is_cloud_optimized_geotiff) in portolan_cli/formats.py
- [ ] T032 [US2] Add warning emission for CONVERTIBLE status in add_dataset() in portolan_cli/dataset.py

**Checkpoint**: Convertible files emit warning then convert - User Story 2 complete

---

## Phase 5: User Story 3 - Reject Unsupported Dataset (Priority: P2)

**Goal**: Reject unsupported files with helpful error message

**Independent Test**: Attempt to add a NetCDF file and verify rejection message

### Tests for User Story 3

> **NOTE: Write these tests FIRST, ensure they FAIL before implementation**

- [ ] T033 [P] [US3] Unit test: NetCDF returns UNSUPPORTED status with error message in tests/unit/test_formats_unsupported.py
- [ ] T034 [P] [US3] Unit test: HDF5 returns UNSUPPORTED status with error message in tests/unit/test_formats_unsupported.py
- [ ] T035 [P] [US3] Unit test: LAS/LAZ (non-COPC) returns UNSUPPORTED status with COPC guidance in tests/unit/test_formats_unsupported.py
- [ ] T036 [P] [US3] Integration test: add_dataset with NetCDF raises UnsupportedFormatError in tests/integration/test_dataset_warnings.py

### Implementation for User Story 3

- [ ] T037 [US3] Add UNSUPPORTED_EXTENSIONS frozenset (.nc, .netcdf, .h5, .hdf5, .las, .laz) in portolan_cli/formats.py
- [ ] T038 [US3] Add UNSUPPORTED_ERROR_MESSAGES mapping (extension → error message) in portolan_cli/formats.py
- [ ] T039 [US3] Extend get_cloud_native_status() for unsupported formats in portolan_cli/formats.py
- [ ] T040 [US3] Add error emission and exception raising for UNSUPPORTED status in add_dataset() in portolan_cli/dataset.py

**Checkpoint**: Unsupported files rejected with helpful error - User Story 3 complete

---

## Phase 6: Polish & Cross-Cutting Concerns

**Purpose**: Edge cases, documentation, and cleanup

- [ ] T041 [P] Unit test: ambiguous .tif extension correctly detects COG vs non-COG in tests/unit/test_formats_edge_cases.py
- [ ] T042 [P] Unit test: .json file detected as GeoJSON vs plain JSON in tests/unit/test_formats_edge_cases.py
- [ ] T043 [P] Unit test: unknown extension returns appropriate status in tests/unit/test_formats_edge_cases.py
- [ ] T044 Property-based test: all known extensions map to valid FormatInfo in tests/unit/test_formats_property.py
- [ ] T045 Update docstrings for all new functions in portolan_cli/formats.py
- [ ] T046 Run quickstart.md validation scenarios manually
- [ ] T047 Ensure all new code passes mypy --strict
- [ ] T048 Run full test suite and verify coverage

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)**: No dependencies - can start immediately
- **Foundational (Phase 2)**: Depends on Setup (fixtures needed for tests) - BLOCKS all user stories
- **User Story 1 (Phase 3)**: Depends on Foundational
- **User Story 2 (Phase 4)**: Depends on Foundational (can run parallel with US1)
- **User Story 3 (Phase 5)**: Depends on Foundational (can run parallel with US1/US2)
- **Polish (Phase 6)**: Depends on all user stories complete

### User Story Dependencies

- **User Story 1 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 2 (P1)**: Can start after Foundational - No dependencies on other stories
- **User Story 3 (P2)**: Can start after Foundational - No dependencies on other stories

All three user stories are independent and can be implemented in parallel after Foundational phase.

### Within Each User Story

- Tests MUST be written and FAIL before implementation
- Helpers/constants before main function
- Main function before integration point (dataset.py)

### Parallel Opportunities

- All fixture creation tasks (T002-T005) can run in parallel
- All test tasks within a phase marked [P] can run in parallel
- All three user stories can be worked on in parallel after Foundational

---

## Parallel Example: Phase 3 (User Story 1)

```bash
# Launch all tests for User Story 1 together:
Task: "Unit test: GeoParquet returns CLOUD_NATIVE status"
Task: "Unit test: COG returns CLOUD_NATIVE status"
Task: "Unit test: FlatGeobuf returns CLOUD_NATIVE status"
Task: "Integration test: add_dataset with GeoParquet emits no warnings"

# Launch helper implementations in parallel:
Task: "Add is_geoparquet() helper function"
Task: "Add is_cloud_optimized_geotiff() helper function"
```

---

## Implementation Strategy

### MVP First (User Story 1 + 2)

1. Complete Phase 1: Setup (fixtures)
2. Complete Phase 2: Foundational (enums, dataclass)
3. Complete Phase 3: User Story 1 (cloud-native passthrough)
4. Complete Phase 4: User Story 2 (convertible warnings)
5. **STOP and VALIDATE**: Test both stories independently
6. This covers the most common use cases

### Incremental Delivery

1. Setup + Foundational → Foundation ready
2. Add User Story 1 → Cloud-native files work → Demo
3. Add User Story 2 → Warnings work → Demo
4. Add User Story 3 → Rejections work → Demo
5. Polish → Edge cases covered → Release

### Single Developer Strategy

1. Complete phases sequentially (1 → 2 → 3 → 4 → 5 → 6)
2. Verify tests fail before implementation at each phase
3. Commit after each user story checkpoint

---

## Notes

- [P] tasks = different files, no dependencies
- [Story] label maps task to specific user story for traceability
- Each user story is independently completable and testable
- TDD is mandatory per Constitution Principle I
- All tests must fail before implementation begins
- Commit after each task or logical group
- Stop at any checkpoint to validate story independently
