# ADR-0021: catalog.json at Root Level

## Status
Accepted

## Context

Issue #94: `init_catalog()` writes `./catalog.json`, but `dataset.py` reads `.portolan/catalog.json`. This inconsistency blocks dataset operations.

## Decision

**catalog.json lives at root level** (`./catalog.json`), following STAC best practices.

All STAC metadata is user-visible:
- `./catalog.json` — root catalog
- `./collection-name/collection.json` — collection metadata
- `./collection-name/item.json` — item metadata

`.portolan/` contains only Portolan internals (versions.json, config).

## Consequences

- Compatible with PySTAC, STAC Browser, and other STAC tooling
- Requires fixing `dataset.py` functions to read from root (see Issue #94)
- Users can inspect STAC metadata directly

## Alternatives Considered

**`.portolan/catalog.json`**: Rejected—non-standard layout breaks STAC tooling.
