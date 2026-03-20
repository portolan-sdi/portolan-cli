# ADR-0022: Git-Style Tracking and Command Model

## Status
Accepted

## Context

Portolan needs a clear command model where each command addresses a distinct concern. The model should feel familiar to users who know git.

## Decision

### Concerns and Commands

| Concern | Command | Description |
|---------|---------|-------------|
| Tracking setup | `init` | Initialize catalog |
| Structure validation | `scan` | "Is my directory organized correctly?" |
| Metadata/format validation | `check` | "Is metadata valid? Are files cloud-native?" |
| Tracking state | `status` | "What's tracked, untracked, modified?" |
| Add to tracking | `add` | Track files (validates, converts, generates metadata) |
| Remove from tracking | `rm` | Untrack files |
| Browse structure | `list` | "What's in my catalog?" |
| Browse metadata | `info` | "What are the details of this item?" |
| Upload | `push` | Send local to remote |
| Download | `pull` | Get remote to local |
| Orchestrate | `sync` | Full pipeline: scan → check → push/pull |

### Command Details

**`check` flags:**
- `check` — validate all (metadata + geo-assets)
- `check --fix` — fix all (generate metadata + convert files)
- `check --metadata` — validate metadata only
- `check --geo-assets` — validate geo-assets only (cloud-native status)
- `check --metadata --fix` — generate/repair metadata only
- `check --geo-assets --fix` — convert files only (GeoJSON→GeoParquet, TIFF→COG)

**`add` — tracks files already in the catalog directory:**
```bash
# Add a new collection
cp -r ~/my-data/ ./catalog/demographics/
portolan add demographics/

# Add a single file to existing collection
cp ~/new-file.parquet ./catalog/demographics/
portolan add demographics/new-file.parquet

# Add everything (initial setup)
portolan init
portolan add .
```

**`rm` — removes from tracking:**
```bash
# Remove file (deletes file + removes metadata)
portolan rm demographics/census.parquet

# Remove entire collection
portolan rm demographics/

# Untrack but keep file
portolan rm --keep demographics/old-file.parquet
```

**`list` — browse catalog structure:**
```bash
portolan list
# demographics/
#   census.parquet (GeoParquet, 4.2MB)
#   boundaries.parquet (GeoParquet, 1.1MB)
# imagery/
#   satellite.tif (COG, 120MB)
```

**`info` — browse item metadata:**
```bash
portolan info demographics/census.parquet
# Format: GeoParquet
# CRS: EPSG:4326
# Bbox: [-122.5, 37.7, -122.3, 37.9]
# Features: 4,231
# Version: v1.2.0
```

### Typical Workflow

1. User has uninitialized directory of geo files
2. `scan` — tells user how to restructure (user fixes manually)
3. `check --fix` — validates, converts, generates metadata
4. `push` — uploads to remote
5. Later: `add`/`rm` for incremental changes
6. `sync` — orchestrates the full pipeline

### Key Principles

- **Subdirectory = collection** (directory name = collection ID)
- **Files tracked when `add` is run** (not implicit on copy)
- **`rm` deletes files by default** (use `--keep` to preserve)
- **`add` is idempotent** — running again on tracked files repairs metadata

## Consequences

- Clear separation of concerns across commands
- Familiar git-like mental model
- `dataset add/remove` renamed to top-level `add/rm`
- `check --fix` handles both conversion and metadata generation
