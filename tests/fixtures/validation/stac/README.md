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

## Usage

These fixtures are used by `tests/unit/validation/test_stac_rules.py`.
