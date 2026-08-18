# CLI-Owned versions.json Manifest Schemas

JSON Schemas for the `versions.json` manifest the CLI writes — its single
source of truth for version history, checksums, and sync state (see
`docs/reference/versions-manifest.md`). These are CLI-owned, not part of the
published Portolan specification, and not vendored from anywhere: this
directory is their canonical home.

| File | Describes |
|------|-----------|
| `versions.schema.json` | Collection-level `versions.json` (version history) |
| `catalog-versions.schema.json` | Root-level `versions.json` (collection index) |

`tests/integration/test_versions_manifest_schema.py` validates CLI output
against both schemas and enforces the semantic invariants a JSON Schema cannot
express (current-version consistency, change references, version uniqueness).
A schema change here is a compatibility decision: catalogs written by older
CLI versions must stay readable.
