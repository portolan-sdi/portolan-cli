# STAC Validation Test Fixtures

Test fixtures for `StacSchemaRule` and `StacLintRule` validation.

## Fixtures

| Directory | Purpose | Expected Result |
|-----------|---------|-----------------|
| `valid/` | Minimal valid STAC catalog | Pass both schema and lint |
| `missing-id/` | Catalog missing required `id` field | Fail schema validation |
| `bad-id/` | Catalog with `:` and `/` in ID | Pass schema, fail lint (percent_encoded) |
| `invalid-json/` | Malformed JSON syntax | Fail with parse error |

## Usage

These fixtures are used by `tests/unit/validation/test_stac_rules.py`.
