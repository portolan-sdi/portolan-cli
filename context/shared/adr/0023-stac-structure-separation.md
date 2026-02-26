# ADR-0023: STAC Structure Separation

## Status
Accepted

## Context

ADR-0012 placed all catalog structure inside `.portolan/`:

```
.portolan/
├── catalog.json
├── collections/{name}/
│   ├── collection.json
│   ├── versions.json
│   └── {item}/item.json
```

ADR-0021 moved `catalog.json` to root but didn't address collections/items. The result: **`portolan init` and `portolan dataset add` are incompatible.**

This violates STAC conventions and makes catalogs unusable with standard STAC tooling (STAC Browser, PySTAC, stac-validator).

## Decision

**Principle: STAC takes precedent.** Only Portolan-internal tooling state lives in `.portolan/`.

### Directory Structure (Local AND Remote)

```
./catalog.json                          # STAC catalog
./versions.json                         # Catalog-level versioning (discoverable)
./.portolan/
│   ├── config.yaml                     # Internal: catalog configuration (see ADR-0024)
│   └── state.json                      # Internal: local sync state
./demographics/
│   ├── collection.json                 # STAC collection
│   ├── versions.json                   # Collection-level versioning (discoverable)
│   └── census-2020/
│       ├── item.json                   # STAC item
│       └── data.parquet                # Asset file
```

### What Goes Where

| File | Location | Rationale |
|------|----------|-----------|
| `catalog.json` | `./` | STAC standard |
| `collection.json` | `./{collection}/` | STAC standard |
| `item.json` | `./{collection}/{item}/` | STAC standard |
| `versions.json` | Alongside STAC files | Consumer-visible metadata (version history, checksums) |
| `config.json` | `.portolan/` | Internal tooling configuration |
| `state.json` | `.portolan/` | Internal local state (not synced? TBD) |

### Local = Remote

Remote is an exact mirror of local. This enables:
- Simple push/pull (no path translation)
- `portolan clone` recreates exact local structure
- STAC tooling works on both local and remote

## Consequences

### Supersedes
- **ADR-0012** structure section (flat hierarchy principle remains)
- **ADR-0021** (subsumed into this ADR)

### Code Changes Required
- `dataset.py`: STAC paths at root, not `.portolan/collections/`
- `push.py`: Read/write collection structure from root
- `pull.py`: Read/write collection structure from root
- `validation/rules.py`: Validate root-level structure
- Tests: Update path expectations

### Benefits
- Compatible with STAC Browser, PySTAC, stac-validator
- `versions.json` discoverable alongside the data it describes
- Clear separation: STAC + versioning = user-visible; config + state = internal

### Migration
Pre-1.0; existing catalogs with `.portolan/collections/` structure need manual migration or re-init.

## Alternatives Considered

### Keep versions.json in .portolan/
**Rejected:** Version history and checksums are metadata consumers want to discover. Hiding them makes the catalog less useful.

### Nested .portolan/ per collection
**Rejected:** Unnecessarily complex. Single root `.portolan/` for internal state is sufficient.
