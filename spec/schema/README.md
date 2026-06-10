# Portolan Spec Schemas (Vendored)

This directory contains vendored copies of the machine-readable schemas from
[portolan-spec](https://github.com/portolan-sdi/portolan-spec).

## Source

These schemas were created as part of [Issue #23](https://github.com/portolan-sdi/portolan-spec/issues/23)
and merged in [PR #24](https://github.com/portolan-sdi/portolan-spec/pull/24).

Original location: `https://github.com/portolan-sdi/portolan-spec/tree/main/schema`

## Files

| File | Description |
|------|-------------|
| `versions.schema.json` | JSON Schema for `versions.json` manifest |
| `collection.schema.json` | JSON Schema for STAC Collections with Portolan extensions |
| `catalog.schema.json` | JSON Schema for STAC Catalogs with Portolan extensions |
| `rules.yaml` | Semantic validation rules (cannot be expressed in JSON Schema) |

## Why Vendor?

Vendoring provides:

1. **Reproducibility** - Tests work without network access
2. **Stability** - Schema changes don't break CI unexpectedly
3. **Speed** - No HTTP fetching during test runs

## Updating

To update these schemas from upstream:

```bash
cd tests/spec_compliance/schemas

# Fetch latest schemas
for f in versions.schema.json collection.schema.json catalog.schema.json rules.yaml; do
  gh api repos/portolan-sdi/portolan-spec/contents/schema/$f \
    --jq '.content' | base64 -d > $f
done
```

Then run the compliance tests to verify the CLI still conforms:

```bash
uv run pytest tests/spec_compliance/ -v
```

## Schema Notes

The collection and catalog schemas reference external STAC schemas:
- `https://schemas.stacspec.org/v1.0.0/collection-spec/json-schema/collection.json`
- `https://schemas.stacspec.org/v1.0.0/catalog-spec/json-schema/catalog.json`

For compliance testing, we use "Portolan-only" schemas (defined in `conftest.py`)
that validate Portolan-specific requirements without requiring external $ref resolution.
