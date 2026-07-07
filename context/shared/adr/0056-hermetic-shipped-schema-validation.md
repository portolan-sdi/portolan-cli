# ADR-0056: Hermetic validation against the shipped STAC schemas

## Status
Accepted

## Context

`tests/spec_compliance/` is meant to prove that CLI output conforms to the
schemas Portolan actually ships (`spec/schema/catalog.schema.json`,
`collection.schema.json`). It did not. The compliance tests validated documents
against hand-copied inline schema stubs in `conftest.py`
(`portolan_catalog_schema` / `portolan_collection_schema`) that covered only the
Portolan *extension* fields, never the STAC base. The shipped schemas were
loaded by no validation test, so they could drift from CLI output silently
(issue #557, pre-sprint audit finding C12; the stubs were already looser than
the shipped schemas — obs 5023).

The shipped schemas extend upstream STAC v1.1.0 via `allOf` + absolute-URL
`$ref` (`https://schemas.stacspec.org/v1.1.0/...`). Those references cannot be
resolved offline, which is why the stubs existed. Validating against the real
schemas therefore requires resolving the STAC closure without network access,
because the test suite is hermetic and this logic is the seed of `reis`, the
validator being extracted from this repo (issue #563).

Two forces complicate a naive "just load the schema":

- **Relative hrefs.** Portolan emits relative hrefs by design (SELF_CONTAINED,
  ADR-0051). The STAC schema marks href fields `format: iri`, which relative
  paths fail. The href/"published" policy is unresolved (discussion #573).
- **Mixed dialects and malformed upstream `$id`.** The Portolan wrappers are
  JSON Schema 2020-12; the STAC base schemas are draft-07. Some STAC v1.1.0
  schemas declare a broken `$id` (e.g. `common.json` → `.../commonjson`).

## Decision

1. **Vendor the STAC v1.1.0 `$ref` closure** (11 files) as package data under
   `portolan_cli/validation/_vendored/stac/1.1.0/`, fetched by
   `scripts/refresh_stac_schemas.py`. Files stay byte-identical to upstream.
2. **Add `portolan_cli/validation/schema_registry.py`** (in the reis extraction
   seam): builds a `referencing.Registry` from the vendored closure and exposes
   `validate_document(instance, schema)`. It keys each resource by its canonical
   retrieval URL and normalizes `$id` at load time, side-stepping the malformed
   upstream ids. `jsonschema` resolves each referenced resource under its own
   `$schema`, so a 2020-12 validator follows `$ref`s into draft-07 resources.
3. **Run with `format` assertions OFF.** JSON Schema `format` is opt-in in
   `jsonschema`, so `format: iri` is simply not enforced. This makes relative
   hrefs pass without any special-casing and does **not** pre-judge #573. When
   #573 settles the href policy, format enforcement is turned on in this one
   module, consistently for tests and any runtime consumer.
4. **Delete the inline stubs**; compliance tests validate against the shipped
   schemas via the registry. `jsonschema` + `referencing` become explicit
   runtime dependencies (previously transitive via `stac-check`).

## Consequences

- Compliance tests now exercise the STAC base plus Portolan extensions from the
  real shipped files. A backward-incompatible edit to a shipped schema breaks
  the suite (the missing guard #557 was about). Real `init`/`add` output already
  conformed, so no CLI output changes were needed.
- Validation is hermetic and works from an installed wheel (the vendored closure
  ships as package data). Note: `.gitignore` had a bare `catalog.json` rule for
  test artifacts that silently dropped the vendored STAC catalog base from the
  wheel; the vendored tree is now explicitly un-ignored.
- `format`-off means iri/datetime formats are not checked here. That is a
  deliberate, documented gap owned by #573, not an oversight.
- Vendored schemas can lag a future STAC version. STAC v1.1.0 is immutable, so
  the copy does not drift; a version bump is a manual `refresh_stac_schemas.py`
  run (with `--check` available for CI). No automated network-diff test, by
  choice.

## Alternatives considered

- **Keep the inline stubs.** Rejected: that is the bug. They validate a fiction.
- **Enforce `format: iri` and tolerate relative hrefs via an acceptable-error
  filter** (mirroring `stac_rules.py`). Rejected for now: it re-introduces href
  special-casing and pre-judges #573. `format`-off is simpler and neutral.
- **Fetch STAC schemas over the network at test time.** Rejected: non-hermetic,
  flaky, and unusable for the offline `reis` extraction.
- **Depend on a package that ships STAC schemas.** None does cleanly; `pystac`
  ships no JSON Schemas. Vendoring the closure is explicit and auditable.
