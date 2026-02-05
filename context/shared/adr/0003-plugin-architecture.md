# ADR-0003: Plugin Architecture for Format Support

## Status
Accepted

## Context

Portolan needs to support multiple geospatial formats: GeoParquet, COG, PMTiles, COPC (point clouds), 3D Tiles, and potentially Iceberg. Each format has different:

- Conversion tooling (geoparquet-io, rio-cogeo, etc.)
- Dependency footprint (some are heavy)
- Maturity level (some are production-ready, others experimental)
- User demand (some are universal, others niche)

Bundling everything into core creates bloat, installation friction, and ties Portolan's release cycle to all dependencies.

## Decision

We use a **plugin architecture** with Python entry points:

### Core formats (always installed)
- **GeoParquet** via geoparquet-io
- **COG** via rio-cogeo

### Plugin formats (separate packages)
- PMTiles → `portolan-pmtiles`
- COPC → `portolan-copc`
- 3D Tiles → `portolan-3dtiles`
- Iceberg → `portolan-iceberg`

### Entry point registration

```toml
# Example: portolan-pmtiles/pyproject.toml
[project.entry-points."portolan.formats"]
pmtiles = "portolan_pmtiles:PMTilesFormat"
```

### Runtime discovery

```python
from importlib.metadata import entry_points

def get_formats():
    discovered = entry_points(group="portolan.formats")
    return {ep.name: ep.load() for ep in discovered}
```

### Promotion path

Plugins that prove essential can be promoted to core dependencies. This is a one-way door — once core, always core (for backward compatibility).

### Plugin interface

Internal format handlers follow a consistent interface from day one, even before external plugins exist. This ensures the plugin boundary is well-defined when someone needs it.

## Consequences

### What becomes easier
- **Lean core install** — Users who only need GeoParquet/COG don't pull in PMTiles dependencies
- **Independent release cycles** — Plugin bugs don't block core releases
- **Experimentation** — New formats can incubate as plugins before (maybe) becoming core
- **Community contribution** — Third parties can create plugins without core maintainer involvement

### What becomes harder
- **Discoverability** — Users must know to install `portolan-pmtiles` separately
- **Testing matrix** — Need to test core + each plugin combination
- **Interface stability** — Plugin interface becomes a compatibility contract

### Trade-offs
- We accept discoverability friction for installation simplicity
- We accept interface maintenance burden for ecosystem flexibility

## Alternatives Considered

### 1. Bundle everything in core
**Rejected:** Heavy install, slow releases, forces users to pull dependencies they don't need.

### 2. Optional extras (`pip install portolan[pmtiles]`)
**Considered:** Simpler than separate packages, but still couples release cycles. May revisit if plugin ecosystem stays small.

### 3. No plugin system, add formats as needed
**Rejected:** Doesn't scale. Each format addition requires core changes and full release cycle.
