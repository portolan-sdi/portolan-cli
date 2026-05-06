# Version Management

Portolan tracks data versions in `versions.json`, enabling git-like workflows for your geospatial data.

## Quick Start

```bash
# Check what's changed since the last version
portolan status

# Create a new version after modifying files
portolan version bump my-collection 1.1.0 -m "Updated source data"

# View version history
portolan version list my-collection
```

## Understanding Status

The `portolan status` command shows the state of your collections:

```bash
$ portolan status
→ Collection: demographics
→   Local version: 1.0.0
    Remote version: (offline or not configured)
→
→   Modified files:
⚠     census-data.parquet (checksum changed)
→
→   Deleted files:
✗     old-data.parquet (missing from disk)
```

### Status Categories

| Category | Meaning |
|----------|---------|
| **Modified** | File checksum differs from `versions.json` |
| **Deleted** | File in `versions.json` but missing from disk |
| **Untracked** | Data file on disk but not in `versions.json` |

!!! note "Managed Files"
    STAC metadata files (`collection.json`, `*.json` items), `README.md`, and `metadata.yaml` are **not** shown as untracked. These are managed by Portolan separately from data versioning.

### Sync State

When connected to a remote, status shows whether you're in sync:

- **in_sync**: Local and remote versions match
- **ahead**: Local has unpushed versions
- **behind**: Remote has versions you haven't pulled
- **unknown**: Remote not configured or offline

Use `--offline` to skip the remote check:

```bash
portolan status --offline
```

## Creating Versions

Use `portolan version bump` to create a new version:

```bash
portolan version bump <collection> <version> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `-m, --notes` | Version message (like a commit message) |
| `--breaking` | Mark as a breaking change |
| `-y, --yes` | Skip confirmation prompt |
| `--json` | JSON output for automation |

### Examples

```bash
# Simple version bump with message
portolan version bump demographics 1.2.0 -m "Added 2024 census data"

# Breaking change (schema modified)
portolan version bump demographics 2.0.0 --breaking -m "Changed column names"

# Non-interactive (for CI/CD)
portolan version bump demographics 1.2.0 -y --json
```

### Version Format

Versions must be valid [semver](https://semver.org/) strings:

- ✅ `1.0.0`, `2.1.3`, `1.0.0-beta.1`, `1.0.0+build.123`
- ❌ `1.0`, `v1.0.0`, `latest`, `bad-version`

### Validation

The bump command validates:

1. **Semver format** - Rejects invalid version strings
2. **Uniqueness** - Rejects versions that already exist

## Viewing History

### Current Version

```bash
$ portolan version current demographics
→ demographics: 1.2.0  2026-05-06 10:30:00 — Added 2024 census data
    3 asset(s)
```

### All Versions

```bash
$ portolan version list demographics
→ Versions for 'demographics' (3 total):

→   1.0.0  2026-01-15 08:00:00 — Initial release
      census-data.parquet
→   1.1.0  2026-03-20 14:30:00 — Updated boundaries
      census-data.parquet
→   1.2.0  2026-05-06 10:30:00 — Added 2024 census data
      census-data.parquet
      demographics-2024.parquet
```

## JSON Output

All commands support `--json` for machine-readable output:

```bash
$ portolan status --json
{
  "success": true,
  "command": "status",
  "data": {
    "collections": [
      {
        "collection": "demographics",
        "local_version": "1.2.0",
        "remote_version": "1.2.0",
        "sync_state": "in_sync",
        "modified_files": [],
        "untracked_files": [],
        "deleted_files": []
      }
    ]
  }
}
```

## Workflow Example

A typical workflow:

```bash
# 1. Check current state
portolan status

# 2. Make changes to your data files
# ... edit parquet files, add new files, etc.

# 3. Review what changed
portolan status

# 4. Create a new version
portolan version bump my-collection 1.3.0 -m "Monthly data update"

# 5. Push to remote
portolan push
```

## What Gets Versioned?

### Tracked in versions.json

- Data files (`.parquet`, `.tif`, `.csv`, etc.)
- Checksums (SHA256) for change detection
- File sizes and modification times

### NOT Tracked (Managed Separately)

- `collection.json` - STAC collection metadata
- `*.json` item files - STAC item metadata
- `README.md` - Generated documentation
- `metadata.yaml` - Human enrichment layer
- `versions.json` - The version file itself

These files are synced with `portolan push` but aren't versioned—they're derived from or supplement the data.

## See Also

- [Configuration](../reference/configuration.md) - Configure remote settings
- [CLI Reference](../reference/cli.md) - Full command reference
