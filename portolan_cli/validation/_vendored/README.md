# Vendored upstream schemas

These files are **fetched from upstream, not hand-authored.** Do not edit them by
hand.

## `stac/1.1.0/`

The transitive `$ref` closure of the STAC v1.1.0 catalog and collection JSON
Schemas, fetched from `https://schemas.stacspec.org/v1.1.0/`. Portolan's shipped
`spec/schema/catalog.schema.json` and `collection.schema.json` extend these via
`allOf` + absolute-URL `$ref`. Vendoring the closure lets
`portolan_cli.validation.schema_registry` resolve those references offline, so
spec-compliance validation is hermetic (no network in tests or at runtime).

Layout mirrors the upstream URL paths so relative `$ref`s line up. Each file is
kept byte-identical to upstream; the registry normalizes the (occasionally
malformed) `$id` at load time rather than editing the files, so the refresh
check below stays a faithful diff against upstream.

### Refreshing

Run only when bumping the pinned STAC version (also update the `$ref`s in
`spec/schema/*.json` and `STAC_VERSION` in `schema_registry.py`):

```bash
uv run python scripts/refresh_stac_schemas.py          # re-fetch + write
uv run python scripts/refresh_stac_schemas.py --check   # verify current
```

STAC v1.1.0 is an immutable published version, so the vendored copy does not
drift on its own. There is deliberately no automated network-diff test.
