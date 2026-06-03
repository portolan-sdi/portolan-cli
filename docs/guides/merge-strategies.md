# Merge Strategies

When running `portolan add`, Portolan auto-detects metadata from your data files (row counts, column types, MIME types). But what happens when your collection already has hand-authored metadata?

The `--merge-strategy` flag controls how auto-detected values are merged with existing metadata.

## Strategies

### `smart` (default)

The smart strategy preserves human-authored fields while updating machine-derivable fields:

**Preserved (human-enrichable):**
- Asset `title`
- Asset `description`
- Column `description` in `table:columns`

**Updated (machine-derivable):**
- Asset `href`, `media_type`, `roles`
- `table:row_count`, `table:primary_geometry`
- Column `name` and `type` in `table:columns`
- Extension fields (`file:size`, `proj:epsg`, `pmtiles:*`, etc.)

```bash
# Default behavior - preserves your titles and descriptions
portolan add data.parquet
```

### `keep`

Preserve all existing metadata. Only add fields that are missing.

Use this when importing a legacy catalog where you trust the existing metadata completely.

```bash
# Don't overwrite anything
portolan add data.parquet --merge-strategy=keep
```

### `overwrite`

Replace everything with auto-detected values. Use when you want to regenerate metadata from scratch.

```bash
# Start fresh
portolan add data.parquet --merge-strategy=overwrite
```

## Use Cases

### AI-Generated Metadata

When Claude Code or another AI agent generates your `collection.json` with rich descriptions, use the default `smart` strategy. Portolan preserves the agent's prose while ensuring machine-derived values (row counts, types) are accurate.

### Legacy Catalog Migration

When importing an existing STAC catalog into Portolan, use `keep` to preserve all existing metadata:

```bash
portolan add . --merge-strategy=keep
```

### Regenerating Metadata

If metadata has become stale or corrupted, use `overwrite` to regenerate everything from the data files:

```bash
portolan add . --merge-strategy=overwrite --force
```

## Field Classification

| Category | Fields | Default Behavior |
|----------|--------|------------------|
| Human-enrichable | `title`, `description`, column descriptions | Preserved |
| Machine-derivable | `href`, `media_type`, `roles`, row counts, types, extension fields | Updated |

The full classification is defined in `portolan_cli/stac.py` as `HUMAN_ENRICHABLE_ASSET_FIELDS` and `MACHINE_DERIVABLE_EXTRA_FIELD_PREFIXES`.
