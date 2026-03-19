# Feature: Source Tracking in versions.json

Adds `source_path` and `source_mtime` optional fields to the Asset dataclass to
track the original source file that was converted to produce a cloud-native asset.

## Purpose

When a non-cloud-native file (e.g., GeoJSON, Shapefile) is converted to
GeoParquet, we need to track:
1. The original source file path (relative to catalog root)
2. The modification time of the source when conversion occurred

This enables:
- Detecting when source files have changed and need re-conversion
- Auditing the provenance of converted files
- Warning users when source files have been modified since conversion

## Asset Dataclass Changes

```python
@dataclass(frozen=True)
class Asset:
    sha256: str
    size_bytes: int
    href: str
    source_path: str | None = None   # NEW: Relative path to original source
    source_mtime: float | None = None  # NEW: Source mtime when converted
```

## Serialization Format

```json
{
  "data.parquet": {
    "sha256": "abc123...",
    "size_bytes": 1048576,
    "href": "data.parquet",
    "source_path": "data.geojson",
    "source_mtime": 1708372800.0
  }
}
```

## Test Scenarios

### Asset Dataclass (Task 5.2)

- [ ] Asset accepts optional `source_path` field (default: None)
- [ ] Asset accepts optional `source_mtime` field (default: None)
- [ ] Asset without source fields remains valid (backward compatible)
- [ ] Asset fields are immutable (frozen dataclass)

### Serialization (Task 5.4)

- [ ] `_serialize_versions_file` includes `source_path` when present
- [ ] `_serialize_versions_file` includes `source_mtime` when present
- [ ] `_serialize_versions_file` omits `source_path` when None
- [ ] `_serialize_versions_file` omits `source_mtime` when None
- [ ] JSON output is valid and can be parsed

### Parsing (Task 5.5)

- [ ] `_parse_versions_file` reads `source_path` from JSON
- [ ] `_parse_versions_file` reads `source_mtime` from JSON
- [ ] `_parse_versions_file` defaults to None when fields missing
- [ ] Backward compatible: old versions.json without source fields still parses
- [ ] Round-trip: serialize -> parse produces identical Asset

### Integration (Task 5.6)

- [ ] `add_version()` accepts Assets with source tracking
- [ ] `write_versions()` then `read_versions()` preserves source tracking
- [ ] Conversion workflow can record source metadata

## Invariants

- [ ] source_path and source_mtime are always set together or both None
- [ ] source_mtime is a Unix timestamp (float) when present
- [ ] source_path is relative to catalog root when present
- [ ] Adding source tracking does not break existing versions.json files
