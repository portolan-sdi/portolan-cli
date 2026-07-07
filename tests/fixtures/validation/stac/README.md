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
| `lint-below-root/` | SELF_CONTAINED 1.1.0 catalog with a CLEAN root, a collection missing `summaries` + a `rel='self'` link, and an item with a non-searchable id | Fail lint on below-root violations, each attributed to its object path (issue #604 regression) |

### Issue #604 regression fixture (below-root best-practice linting)

`lint-below-root/` pins the `StacLintRule` blind spot fixed in #604. stac-check's
`create_best_practices_dict()` only lints the object its `Linter` was built from,
so a recursive `Linter(catalog.json)` run reported best-practice violations for
the ROOT catalog only. The root here is clean while the linked collection and
item each carry violations — pre-fix `stac_lint` reported "All best practice
checks passed"; post-fix it fails with `searchable_identifiers` (ERROR) plus
`check_summaries` and `check_links_self` (WARNING), each prefixed with the
offending object's relative path.

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
