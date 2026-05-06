"""Agent quickstart content for Portolan CLI.

This module contains the kata-style instructions for AI coding agents.
The content is designed to be dense, actionable, and parseable.

Reference: https://github.com/wesm/kata
"""

QUICKSTART = """# Portolan Agent Quickstart

Dense reference for AI coding agents. Also available via CLI:

    portolan quickstart
    portolan agent-instructions   # alias

## Session Setup

- Run from catalog root, or pass --catalog-root <path>
- Use --format=json (global) or --json (per-command) for machine parsing
- Set PORTOLAN_REMOTE and PORTOLAN_PROFILE in .env for push/pull

## Per-Task Guidelines

- Never add files without initializing first (portolan init)
- Use --pmtiles with add to generate vector tiles, styles, and thumbnails
- Scan extracts metadata; check validates STAC structure
- Use --fix to auto-remediate, --dry-run to preview
- Prefer --auto for non-interactive workflows

## STAC Terminology

| Term | Meaning |
|------|---------|
| Catalog | Root container (catalog.json) |
| Collection | Group of related items |
| Item | Single spatiotemporal entity |
| Asset | Actual data file |

Do NOT use "dataset" — use STAC terms.

## Full Workflow Example

```bash
# 1. Extract from WFS (or start with local GeoParquet)
portolan extract wfs "https://example.com/wfs" output_dir \\
  --layers "layer_name" --auto --json

# 2. Initialize catalog (if not using extract)
portolan init --auto --json

# 3. Add files with PMTiles generation (creates tiles + style + thumbnail)
portolan add data/*.parquet --pmtiles --json

# 4. Scan for metadata extraction
portolan scan --json

# 5. Validate and auto-fix issues
portolan check --fix --json

# 6. Generate documentation
portolan readme --recursive --json

# 7. Configure remote
echo "PORTOLAN_REMOTE=s3://bucket/path/" >> .env
echo "PORTOLAN_PROFILE=aws-profile-name" >> .env

# 8. Push to cloud storage
portolan push --json
```

## Key Commands

| Command | Purpose |
|---------|---------|
| init | Create catalog in current directory |
| add | Track files (--pmtiles generates vector tiles + style + thumbnail) |
| scan | Extract metadata from assets |
| check | Validate STAC structure (--fix auto-remediates) |
| push | Upload to remote storage |
| pull | Download from remote storage |
| list | Show catalog contents |
| status | Show sync status |
| info | Show detailed item/collection info |
| readme | Generate README.md (--recursive for all collections) |
| extract wfs | Pull data from WFS service |
| partition | Split large GeoParquet (>2GB) into Hive-partitioned structure |

## PMTiles Generation

Use `--pmtiles` with `add` for vector data:

```bash
portolan add buildings.parquet --pmtiles --json
```

This generates:
- PMTiles vector tiles (web-optimized)
- Mapbox GL style.json (auto-generated color scheme)
- PNG thumbnail (preview image)

All three are registered as STAC assets automatically.

## JSON Output

Both forms work:
```bash
portolan --format=json list      # Global flag
portolan list --json             # Per-command flag
```

Envelope structure:
```json
{
  "success": true,
  "command": "list",
  "data": { ... },
  "errors": []
}
```

On failure, check `errors` array:
```json
{
  "success": false,
  "errors": [{"type": "ErrorType", "message": "description"}]
}
```

## Partitioning

Large vector files (>2GB) trigger partitioning prompts during add/scan.
Use `--auto` to accept defaults, or pre-partition:

```bash
portolan partition large_file.parquet --column region --json
```
"""
