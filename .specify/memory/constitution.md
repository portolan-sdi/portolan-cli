<!--
SYNC IMPACT REPORT
==================
Version change: N/A → 1.0.0 (initial adoption)
Modified principles: N/A (initial version)
Added sections: Core Principles (6), Geospatial Domain Constraints, Development Workflow, Governance
Removed sections: N/A
Templates requiring updates:
  - .specify/templates/plan-template.md: ✅ Compatible (Constitution Check section exists)
  - .specify/templates/spec-template.md: ✅ Compatible (edge cases section aligns with Principle III)
  - .specify/templates/tasks-template.md: ✅ Compatible (TDD workflow preserved)
  - .specify/templates/commands/*.md: N/A (no commands directory found)
Follow-up TODOs: None

Core Principles:
  I.   Test-Driven Development (NON-NEGOTIABLE)
  II.  Geospatial Edge Cases Are First-Class Citizens
  III. Real-World Test Fixtures Over Synthetic Data
  IV.  Stay Current With Upstream Libraries
  V.   Specification-Driven Development
  VI.  CLI Wraps API (Thin CLI Layer)
-->

# Portolan CLI Constitution

## Core Principles

### I. Test-Driven Development (NON-NEGOTIABLE)

TDD is mandatory for all development. No exceptions unless the user explicitly says "skip tests."

- Tests MUST be written BEFORE implementation code
- Tests MUST fail before implementation begins (Red phase)
- Implementation MUST be minimal code to pass tests (Green phase)
- Refactoring MUST preserve passing tests (Refactor phase)
- Edge cases MUST be added after happy path passes

**Rationale**: AI agents write most code. Human review cannot scale. Automated test
suites are the only reliable quality gate. Tautological tests are defended against
via mutation testing (mutmut), property-based testing (hypothesis), and human-written
test specifications in `tests/specs/`.

### II. Geospatial Edge Cases Are First-Class Citizens

Portolan orchestrates conversion and sync—it does NOT implement geometry operations.
However, geospatial metadata and catalog management have unique edge cases that MUST
be tested.

**Catalog & Metadata Edge Cases**:
- Bounding boxes crossing the antimeridian (lon_min > lon_max)
- Polar coordinates and global extents
- CRS metadata preservation through sync operations
- Empty datasets (zero features, valid schema)
- Datasets with null/missing extent metadata

**Sync & Versioning Edge Cases**:
- versions.json corruption or missing entries
- Checksum mismatches between local and remote
- Remote drift detection (external bucket edits)
- Partial sync failures (some files uploaded, others failed)
- Concurrent access conflicts (when multi-user lands)

**Format Detection Edge Cases**:
- Ambiguous file extensions (.json could be GeoJSON or plain JSON)
- Files with wrong extensions (Shapefile named .parquet)
- Upstream library failures (geoparquet-io, rio-cogeo errors)

**Rationale**: Portolan delegates conversion to upstream libraries, but orchestration
has its own failure modes. A corrupted versions.json, an inverted bounding box in
STAC metadata, or a silent sync failure can break entire catalogs. Test the
orchestration layer explicitly.

### III. Real-World Test Fixtures Over Synthetic Data

Test fixtures MUST represent real-world data edge cases, not idealized synthetic
examples.

- Fixtures MUST be small but representative (few rows/pixels, enough to test behavior)
- Every valid fixture MUST have a corresponding invalid variant
- Fixtures MUST cover known upstream library quirks (see `context/shared/known-issues/`)
- Fixtures MUST be committed to git with documentation in `tests/fixtures/README.md`
- Property-based tests (hypothesis) MUST be used for invariant verification

**Rationale**: Synthetic "happy path" test data misses the edge cases that cause
production failures. Real-world geospatial data is messy—mixed encodings, invalid
geometries, unexpected null values. Tests must reflect this reality.

### IV. Stay Current With Upstream Libraries

Portolan orchestrates upstream libraries (geoparquet-io, rio-cogeo, gpio-pmtiles).
Their APIs and behaviors change. We MUST stay current.

- Use Context7 MCP for official API documentation BEFORE generating code
- Use Gitingest for source code exploration when investigating edge cases
- Pin dependencies with explicit version constraints in pyproject.toml
- Document upstream quirks in `context/shared/known-issues/`
- Re-verify integration tests when bumping dependency versions
- Never reimplement functionality that upstream libraries provide

**Rationale**: Stale assumptions about library behavior cause subtle bugs. A function
signature that changed in v2.0, a default parameter that flipped, or a new
validation that rejects previously-valid input—these break silently without
current documentation.

### V. Scope Before You Build

Implementation MUST NOT begin until scope is well-defined. The scoping process
matters more than the specific tooling used.

**Scoping Requirements**:
- User stories MUST be defined with acceptance criteria (Given/When/Then)
- Edge cases MUST be identified BEFORE implementation
- Success criteria MUST be measurable and testable
- Scope MUST fit in a document readable in one sitting
- YAGNI: If a capability is not in scope, it MUST NOT be implemented

**Scoping Workflow** (flexible):
1. **Discuss with Claude** — Clarify requirements, explore edge cases, refine scope
2. **Document the scope** — Spec, ticket, or design doc (format flexible)
3. **Hand off to implementation** — New agent session or continue in same session
4. **Implement with TDD** — Tests first, then minimal implementation

**Speckit Integration** (optional):
- `/speckit.specify` → `/speckit.plan` → `/speckit.tasks` provides structured workflow
- Use when formal documentation is needed or scope is complex
- Skip for small, well-understood changes

**Rationale**: Heavy upfront scoping prevents scope creep, catches edge cases early,
and enables clean handoff between scoping and implementation sessions. The goal is
clarity of intent, not ceremony.

### VI. CLI Wraps API (Thin CLI Layer)

All logic lives in the library layer (`portolan_cli/`). The CLI is a thin Click
wrapper that parses arguments and calls library functions.

- CLI commands MUST NOT contain business logic
- Library functions MUST be independently callable without CLI
- CLI MUST use `portolan_cli/output.py` for all terminal messages
- Every CLI command MUST have `--auto` fallback for automation
- JSON output MUST be available for programmatic consumption

**Rationale**: Testability and reusability. Library functions can be unit-tested
without subprocess spawning. Other tools can import the library directly. CLI
remains focused on argument parsing and output formatting.

## Geospatial Domain Constraints

These constraints address the unique challenges of orchestrating cloud-native
geospatial data catalogs.

**Separation of Concerns**:
- Portolan orchestrates; upstream libraries convert (geoparquet-io, rio-cogeo)
- Format validation MUST be delegated to upstream libraries, not reimplemented
- Geometry operations MUST NOT be implemented in Portolan

**Validation Strategy**:
- GeoParquet validation → delegate to geoparquet-io
- COG validation → delegate to rio-cogeo's `cog_validate`
- STAC metadata → validate against STAC specification
- Catalog structure → validate against Portolan spec

**Metadata Preservation**:
- CRS metadata MUST be preserved through sync operations
- Extent/bounding box MUST be extracted from upstream library output
- STAC metadata MUST reflect the actual data, not assumptions

**Data Integrity**:
- versions.json is the single source of truth for version history and sync state
- Checksums MUST be computed and verified for all synced files
- Portolan owns bucket contents—no support for external edits (ADR-0006)

## Development Workflow

**Quality Gates (All Strict)**:

| Tier | When | What |
|------|------|------|
| Tier 1 | Pre-commit | ruff, vulture, xenon, mypy, fast tests |
| Tier 2 | Every PR | lint, mypy, security, full tests, docs build |
| Tier 3 | Nightly | mutation testing, benchmarks, live network tests |

**Code Standards**:
- ALL code MUST have type annotations (mypy --strict)
- ALL public functions MUST have docstrings
- ALL non-obvious decisions MUST have an ADR in `context/shared/adr/`
- NO new dependencies without ADR justification

**Test Organization**:

| Marker | Purpose | Performance |
|--------|---------|-------------|
| @pytest.mark.unit | Fast, isolated, no I/O | < 100ms |
| @pytest.mark.integration | Multi-component, may touch filesystem | < 5s |
| @pytest.mark.network | Requires network (mocked locally) | Varies |
| @pytest.mark.benchmark | Performance measurement | Varies |
| @pytest.mark.slow | Takes > 5 seconds | > 5s |

## Governance

This constitution supersedes all other development practices. Amendments require:

1. Written proposal documenting the change and rationale
2. Review of impact on existing tests and documentation
3. Migration plan for any breaking changes
4. Version bump following semantic versioning:
   - MAJOR: Principle removed or fundamentally redefined
   - MINOR: New principle or section added
   - PATCH: Clarification or wording improvement

**Compliance**:
- All PRs MUST verify compliance with these principles
- Pre-commit hooks MUST enforce automatable constraints
- Complexity MUST be justified in the Complexity Tracking section of plans
- Use CLAUDE.md for runtime development guidance

**Version**: 1.0.0 | **Ratified**: 2026-02-09 | **Last Amended**: 2026-02-09
