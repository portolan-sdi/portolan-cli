# Vendored STAC Extension Schemas

Published JSON Schemas, copied verbatim so the conformance gate can validate
generated STAC against them without touching the network.

| File | Source URI |
|------|------------|
| `raster-v2.0.0.schema.json` | https://stac-extensions.github.io/raster/v2.0.0/schema.json |

## Why These Are Vendored

rashid validates against the Portolan profile schema and STAC core. It carries
no rule for STAC extension schemas, so a stale or wrong extension version
passes `portolan check` untouched. That gap is how the CLI declared raster
v1.1.0 for months while every gate stayed green (issue #654). The gate now
validates emitted raster items against the real v2.0.0 schema, which needs the
schema bytes on disk.

Every `$ref` in `raster-v2.0.0.schema.json` is internal (`#/definitions/...`),
so validation resolves with no remote lookups.

## Keeping Them Fresh

A copy on disk can drift from the published document. Two tests guard it:

- `tests/unit/test_vendored_schemas.py` pins the file's `$id` and its structure,
  and runs offline.
- The same file carries a `@pytest.mark.network` test that refetches the URI
  and asserts the bytes still match. It runs in the network tier, so drift
  surfaces there rather than silently.

When a schema version moves, update `EXTENSION_URLS` in `portolan_cli/stac.py`,
the registry table in portolan-spec `stac/README.md`, and the file here in the
same change.
