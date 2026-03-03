# ADR-0026: Conversion Config Design

## Status
Accepted

## Context

Portolan classifies files into three statuses (CLOUD_NATIVE, CONVERTIBLE, UNSUPPORTED) and
automatically converts CONVERTIBLE formats to cloud-native equivalents. However, users need
flexibility to:

1. **Force-convert cloud-native formats**: Some cloud-native formats (e.g., FlatGeobuf) are
   excellent for streaming but suboptimal for analytics. Users may want to convert them to
   GeoParquet for columnar query performance.

2. **Preserve convertible formats**: Regulatory or archival requirements may mandate keeping
   original formats (e.g., Shapefiles) unchanged.

3. **Apply rules by path**: Different directories may have different requirements (e.g.,
   "archive/" should preserve everything, "incoming/" should convert everything).

This relates to:
- GitHub Issue #75: FlatGeobuf cloud-native status
- GitHub Issue #103: Config for non-cloud-native file handling
- ADR-0014: Accept non-cloud-native formats
- ADR-0024: Hierarchical config system

## Decision

### 1. Two-tier override system: extensions and paths

Configuration supports both extension-based and path-based overrides:

```yaml
conversion:
  extensions:
    convert: [fgb]        # Force-convert these cloud-native formats
    preserve: [shp, gpkg] # Keep these convertible formats as-is
  paths:
    preserve:             # Glob patterns for files to preserve
      - "archive/**"
      - "regulatory/*.shp"
```

### 2. Path-based rules take precedence over extension-based rules

When both rules could apply, path patterns win. This allows:
- Global extension rules (e.g., "preserve all Shapefiles")
- Directory-specific exceptions (e.g., "except in incoming/, convert everything")

Resolution order:
1. Path-based rules (highest precedence)
2. Extension-based rules
3. Default format classification

### 3. Force-convert is opt-in, only for CLOUD_NATIVE formats

The `extensions.convert` list only affects formats that are already classified as
CLOUD_NATIVE. This prevents accidental double-conversion and makes the config semantics
clear: "force-convert" means "treat this cloud-native format as if it were convertible."

Attempting to add a CONVERTIBLE format (e.g., `.shp`) to `extensions.convert` has no
effect — the file would already be converted by default.

### 4. UNSUPPORTED formats cannot be overridden

Formats classified as UNSUPPORTED (e.g., `.xyz`, unknown extensions) are genuinely
unsupported by Portolan's conversion pipeline. Config overrides cannot change this status
because:
- No converter exists for these formats
- Silently ignoring them could lead to data loss

Users must handle unsupported formats manually or contribute a converter.

### 5. Config is loaded from catalog root

Overrides are loaded from `.portolan/config.yaml` at the catalog root, consistent with
ADR-0024's hierarchical config system. This ensures:
- Config travels with the catalog
- CI/CD can set config via environment or local overrides
- Collection-level config is possible in future extensions

## Consequences

### Easier
- Users can customize conversion behavior without modifying code
- Regulatory workflows can preserve required formats
- Analytics workflows can standardize on GeoParquet

### Harder
- Users must understand the precedence rules
- Debugging "why wasn't my file converted?" requires checking config
- Testing matrix expands to cover config interactions

### Trade-offs
- **Simplicity vs flexibility**: We chose a two-tier system (extensions + paths) rather
  than a single unified rule language. This is less powerful but easier to understand.
- **Silent vs noisy**: Invalid config items (e.g., non-string extensions) are silently
  ignored rather than raising errors. This prevents crashes but may hide typos.

## Alternatives considered

### Single rule language (e.g., glob patterns only)
Rejected: Extension-based rules are the common case and should be simple. Requiring glob
patterns for everything (`**/*.shp`) adds noise for simple cases.

### Raise errors on invalid config
Rejected: YAML configs often have minor typos. Crashing the entire workflow for a typo
in an optional config seems too harsh. Instead, we log warnings and continue with
valid items.

### Allow overriding UNSUPPORTED to CONVERTIBLE
Rejected: Without a converter, this would be misleading. Users would expect conversion
to happen, but it wouldn't. Better to be explicit about what's supported.
