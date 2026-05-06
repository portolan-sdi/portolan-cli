# End-to-End Tutorial + Quickstart Command Plan

**Status:** Ready for implementation
**Created:** 2026-05-06
**Context:** PRs #380 and #367 were too WFS-focused. This replaces them with a single clean end-to-end example.

## Goal

Two outputs from one effort:
1. **Human-facing docs** (`docs/tutorials/end-to-end.md`) — narrative walkthrough in mkdocs
2. **Agent quickstart command** (`portolan quickstart`) — dense, actionable reference for AI coding tools (kata-style)

## Data Source

**Belgium Buildings WFS:**
- Endpoint: `https://geoservices.wallonie.be/geoserver/inspire_bu/ows`
- Layer: `inspire_bu:BU.Building_building_emprise`
- Extract without limit to measure actual size
- Push to existing bucket: `s3://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/`

**Note:** Partitioning triggers at 2GB. If data < 2GB, document the threshold but don't force artificial data.

## Workflow Scope

Full MVP demonstration:
- `portolan extract wfs` (pull without limit)
- `portolan init`
- `portolan add`
- `portolan scan`
- `portolan check --fix`
- Partitioning (auto-detected at 2GB+)
- Style generation
- Thumbnail generation
- `portolan readme --recursive`
- Configure remote (`.env`)
- Clear existing bucket
- `portolan push`

**Excluded:** COG conversion (separate tutorial, same mechanics).

## Implementation Steps

### Step 1: Workspace Setup

```bash
cd ~/Documents/dev/portolan/portolan-test-data
mkdir belgium-buildings-mvp && cd belgium-buildings-mvp
showboat init WORKFLOW.md "Belgium Buildings MVP Demo"
```

### Step 2: Extract Without Limit

```bash
showboat exec WORKFLOW.md bash \
  "uv run portolan extract wfs \
    https://geoservices.wallonie.be/geoserver/inspire_bu/ows \
    buildings \
    --layers 'inspire_bu:BU.Building_building_emprise' \
    --auto"
```

Measure resulting GeoParquet size.

### Step 3: Full Workflow (showboat captures each)

Execute and capture with showboat:
1. `portolan init`
2. `portolan add`
3. `portolan scan`
4. `portolan check --fix`
5. Observe partitioning behavior
6. Style generation
7. Thumbnail generation
8. `portolan readme --recursive`
9. Configure `.env` with:
   ```
   PORTOLAN_REMOTE=s3://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/
   PORTOLAN_PROFILE=source-coop
   ```
10. Clear bucket: `aws s3 rm s3://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/ --recursive`
11. `portolan push`

### Step 4: Verify Reproducibility

```bash
showboat verify WORKFLOW.md
```

### Step 5: Port to portolan-cli Docs

- Copy WORKFLOW.md content → `docs/tutorials/end-to-end.md`
- Add human narrative between code blocks
- Add to mkdocs.yml nav

### Step 6: Create Quickstart Command

**File:** `portolan_cli/quickstart_content.py`

Follow kata's pattern (https://github.com/wesm/kata):

```python
QUICKSTART = """
# Portolan Agent Quickstart

This is the short version to give any coding agent. Also available via CLI:

    portolan quickstart
    portolan agent-instructions   # alias

## Session Setup

- Run from catalog root, or pass --catalog-root <path>
- Use --format=json for all reads when parsing output
- Set PORTOLAN_REMOTE and PORTOLAN_PROFILE for push/pull

## Per-Task Guidelines

- Never add files without initializing first (portolan init)
- Scan before check: scan detects, check validates
- Use --fix to auto-remediate, --dry-run to preview
- Prefer --auto for non-interactive workflows
- Search/list before creating; avoid duplicates

## STAC Terminology

| Term | Meaning |
|------|---------|
| Catalog | Root container (catalog.json) |
| Collection | Group of related items |
| Item | Single spatiotemporal entity |
| Asset | Actual data file |

## Example Session

```bash
# Initialize catalog
portolan init --auto --format=json

# Add data files
portolan add data/*.parquet --format=json

# Scan for metadata
portolan scan --format=json

# Validate and fix issues
portolan check --fix --format=json

# Generate documentation
portolan readme --recursive --format=json

# Push to remote
portolan push --format=json
```

## Command Patterns

For agent parsing, always use --format=json:

```bash
portolan --format=json list
portolan --format=json status
portolan --format=json info <path>
```

JSON envelope structure:
```json
{
  "success": true,
  "command": "command_name",
  "data": { ... },
  "errors": [ ... ]
}
```
"""
```

**CLI command:** Add to `cli.py`:

```python
@cli.command()
def quickstart():
    """Print agent quickstart instructions."""
    from portolan_cli.quickstart_content import QUICKSTART
    click.echo(QUICKSTART)

@cli.command("agent-instructions")
def agent_instructions():
    """Alias for quickstart."""
    from portolan_cli.quickstart_content import QUICKSTART
    click.echo(QUICKSTART)
```

### Step 7: DRY Integration

**Script:** `scripts/generate_quickstart.py`

- Validates quickstart content syntax
- Could inject into docs via markers if needed
- Three modes: `--dry-run`, `--update`, `--check`

**Pre-commit hook in prek.toml:**

```toml
{
  id = "validate-quickstart",
  entry = "uv run python scripts/generate_quickstart.py --check",
  files = { glob = ["portolan_cli/quickstart_content.py"] },
  pass_filenames = false,
  language = "system",
}
```

### Step 8: CI Integration

- mkdocs build validates tutorial renders
- `portolan quickstart > /dev/null` syntax check
- Optional: extract and run code blocks from tutorial

## Deliverables

| Deliverable | Location |
|-------------|----------|
| Showboat workflow (one-time) | `portolan-test-data/belgium-buildings-mvp/WORKFLOW.md` |
| Human tutorial | `portolan-cli/docs/tutorials/end-to-end.md` |
| Agent quickstart content | `portolan-cli/portolan_cli/quickstart_content.py` |
| CLI commands | `portolan quickstart`, `portolan agent-instructions` |
| Validation script | `portolan-cli/scripts/generate_quickstart.py` |

## Research Findings

### JSON Output Coverage

97% coverage (30/31 commands). Only `partition` missing `--json` flag. Global `--format=json` available.

**Gap to fix:** Add `--json` to `partition` command.

### Showboat CLI

- Installed: `~/.local/bin/showboat` v0.6.1
- Commands: `init`, `note`, `exec`, `image`, `pop`, `verify`, `extract`
- `showboat verify` re-runs code blocks, compares outputs
- Existing tutorials in portolan-test-data have showboat IDs

### DRY Patterns in Repo

Marker-based replacement: `<!-- BEGIN GENERATED: section -->` / `<!-- END GENERATED: section -->`

Scripts:
- `scripts/generate_skill_md.py` — Click CLI introspection
- `scripts/generate_claude_md_sections.py` — Multi-source generation
- `scripts/validate_claude_md.py` — Validation
- `scripts/update_freshness.py` — Freshness tracking

Three-mode pattern: `--dry-run`, `--update`, `--check`

Pre-commit integration via prek.toml.

### Belgium Buildings Tutorial (Existing)

- WFS: `https://geoservices.wallonie.be/geoserver/inspire_bu/ows`
- Layer: `inspire_bu:BU.Building_building_emprise`
- Bucket: `s3://us-west-2.opendata.source.coop/nlebovits/belgium-buildings/`
- Profile: `source-coop`
- Current examples use 100k features

## Notes for Implementer

1. **Start with showboat workflow** — run actual commands, capture outputs
2. **Measure data size** — if < 2GB, partitioning won't trigger; document threshold
3. **Clear bucket before push** — `aws s3 rm ... --recursive`
4. **Port showboat output to docs** — add narrative, don't just copy
5. **Quickstart follows kata pattern** — session setup, guidelines, example, terminology
6. **Test both outputs** — `portolan quickstart` prints cleanly, mkdocs builds
