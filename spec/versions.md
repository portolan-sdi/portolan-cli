# Version Manifest (`versions.json`)

Each collection **MUST** include a `versions.json` file that tracks version history, asset checksums, and sync state.

## Location

```
{collection_id}/versions.json
```

Versioning is per-collection, not per-item. When any item in a collection changes, the collection version increments.

## Schema

```json
{
  "spec_version": "1.0.0",
  "current_version": "2.1.0",
  "versions": [
    {
      "version": "2.1.0",
      "created": "2024-01-15T10:30:00Z",
      "breaking": false,
      "assets": {
        "districts/districts.parquet": {
          "sha256": "abc123def456...",
          "size_bytes": 1048576,
          "href": "boundaries/districts/districts.parquet"
        }
      },
      "changes": ["districts/districts.parquet"]
    }
  ]
}
```

### Path Resolution

Asset keys and hrefs follow strict conventions to ensure `push` and `pull` can resolve files unambiguously.

**Asset keys** are scoped relative to the collection directory:
- For single-file collections (no items): the filename alone (e.g., `tunnels.parquet`)
- For collections with items: `{item_id}/{filename}` (e.g., `districts/districts.parquet`)

**Asset hrefs** are catalog-root-relative paths, enabling tools to resolve files via `catalog_root / href`:
- For single-file collections: `{collection_id}/{filename}` (e.g., `tunnels/tunnels.parquet`)
- For collections with items: `{collection_id}/{item_id}/{filename}` (e.g., `boundaries/districts/districts.parquet`)

The collection directory name **MUST NOT** appear in the asset key. The asset key is resolved relative to the collection directory, not the catalog root.

```
# Correct — asset key is collection-relative
"tunnels.parquet": {
  "href": "tunnels/tunnels.parquet"
}

# Incorrect — asset key duplicates the collection directory
"tunnels/tunnels.parquet": {
  "href": "tunnels/tunnels/tunnels.parquet"
}
```

## Fields

### Root Level

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `spec_version` | string | **MUST** | Schema version for the versions.json format (currently `"1.0.0"`) |
| `current_version` | string \| null | **MUST** | The latest version string, or `null` if no versions exist |
| `versions` | array | **MUST** | List of version entries, oldest first |

### Version Entry

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `version` | string | **MUST** | Semantic version string (e.g., `"1.0.0"`) |
| `created` | string | **MUST** | ISO 8601 timestamp in UTC (e.g., `"2024-01-15T10:30:00Z"`) |
| `breaking` | boolean | **MUST** | `true` if this version has breaking changes |
| `assets` | object | **MUST** | Map of item-scoped asset keys to asset metadata |
| `changes` | array | **MUST** | List of asset keys that changed in this version |

### Asset Entry

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `sha256` | string | **MUST** | SHA-256 checksum of the file content |
| `size_bytes` | integer | **MUST** | File size in bytes |
| `href` | string | **MUST** | Catalog-root-relative path to the asset (e.g., `"boundaries/districts/districts.parquet"`) |

## Versioning Rules

### Version Numbering

Versions **SHOULD** follow [Semantic Versioning](https://semver.org/):
- **Major** (X.0.0): Breaking changes (schema changes, column removals)
- **Minor** (0.X.0): New features (new columns, new items)
- **Patch** (0.0.X): Data updates (same schema, new data)

### Breaking Changes

A change is considered **breaking** if consumers depending on the previous schema would fail:
- Column removed or renamed
- Column type changed
- Geometry type changed
- CRS changed

Adding new columns is **not** breaking.

### Change Detection

A file is listed in `changes` if:
- It's new (not in the previous version)
- Its SHA-256 checksum differs from the previous version

## Sync State

The `versions.json` file serves as the sync manifest:
1. Compare local `versions.json` against remote
2. Push files where local checksum differs from remote
3. Update remote `versions.json` after successful push

## Example: Single-File Collection

A collection at `tunnels/` with one GeoParquet file. The asset key is the filename alone, and the href is catalog-root-relative:

```json
{
  "spec_version": "1.0.0",
  "current_version": "1.0.0",
  "versions": [
    {
      "version": "1.0.0",
      "created": "2024-01-15T10:30:00Z",
      "breaking": false,
      "assets": {
        "tunnels.parquet": {
          "sha256": "abc123...",
          "size_bytes": 1048576,
          "href": "tunnels/tunnels.parquet"
        }
      },
      "changes": ["tunnels.parquet"]
    }
  ]
}
```

## Example: Collection with Items

A collection at `boundaries/` with item subdirectories. The asset key includes the item ID, and the href includes both collection and item:

```json
{
  "spec_version": "1.0.0",
  "current_version": "1.1.0",
  "versions": [
    {
      "version": "1.0.0",
      "created": "2024-01-01T00:00:00Z",
      "breaking": false,
      "assets": {
        "districts/districts.parquet": {
          "sha256": "abc123...",
          "size_bytes": 524288,
          "href": "boundaries/districts/districts.parquet"
        }
      },
      "changes": ["districts/districts.parquet"]
    },
    {
      "version": "1.1.0",
      "created": "2024-06-15T12:00:00Z",
      "breaking": false,
      "assets": {
        "districts/districts.parquet": {
          "sha256": "def456...",
          "size_bytes": 786432,
          "href": "boundaries/districts/districts.parquet"
        },
        "districts/districts.pmtiles": {
          "sha256": "ghi789...",
          "size_bytes": 262144,
          "href": "boundaries/districts/districts.pmtiles"
        }
      },
      "changes": ["districts/districts.parquet", "districts/districts.pmtiles"]
    }
  ]
}
```

In this example:
- v1.0.0: Initial import with one parquet file
- v1.1.0: Updated parquet data and added PMTiles derivative
