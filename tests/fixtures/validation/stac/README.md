# STAC Validation Test Fixtures

Test fixtures for `StacSchemaRule` and `StacLintRule` validation.

## Fixtures

| Directory | Purpose | Expected Result |
|-----------|---------|-----------------|
| `valid/` | Minimal valid STAC catalog | Pass both schema and lint |
| `missing-id/` | Catalog missing required `id` field | Fail schema validation |
| `bad-id/` | Catalog with `:` and `/` in ID | Pass schema, fail lint (percent_encoded) |
| `invalid-json/` | Malformed JSON syntax | Fail with parse error |
| `recursive/` | Catalog → Collection → Item hierarchy | Pass both (tests recursive traversal) |
| `recursive-invalid/` | Nested item missing required `id` | Fail schema (tests recursive error detection) |
| `many-violations/` | Catalog with 4+ lint violations | Fail lint with truncated message |
| `self-contained-valid/` | SELF_CONTAINED 1.1.0 catalog (relative hrefs) → collection → item, all valid | Pass schema and fields (relative-href IRI errors must not be treated as failures) |
| `self-contained-invalid-collection/` | SELF_CONTAINED 1.1.0 catalog whose collection is missing `extent` + `stac_version` | Fail schema and fields (issue #543 regression) |
| `self-contained-invalid-item/` | SELF_CONTAINED 1.1.0 catalog whose nested item is missing `id` | Fail schema (issue #543 regression) |

### Issue #543 regression fixtures (STAC 1.1.0, relative hrefs)

The `self-contained-*` fixtures use `stac_version: 1.1.0` and relative `self`/`root`
hrefs — the exact shape of real Portolan output. Under STAC 1.1.0 the root
`catalog.json`'s own relative `self` href triggers an acceptable `must be iri`
error; before #543 that error short-circuited `StacSchemaRule` into a pass
*before any linked collection or item was inspected*, so schema/field violations
below the root went undetected. These fixtures pin that behavior: the valid one
must pass (relative hrefs are fine), the invalid ones must fail on the real
violation, not the acceptable IRI error.

## Usage

These fixtures are used by `tests/unit/validation/test_stac_rules.py`.
