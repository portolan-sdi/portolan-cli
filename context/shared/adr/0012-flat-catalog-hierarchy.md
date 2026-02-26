# ADR-0012: Flat Catalog Hierarchy

## Status
Accepted
Superseded-By: ADR-0023

> Note: Directory structure superseded by ADR-0023. Flat hierarchy principle remains.

## Context

Portolan needs a catalog structure for organizing datasets. STAC supports arbitrary nesting of catalogs and collections, but this flexibility creates ambiguity:

- When a user runs `portolan dataset add ./data/`, how do we interpret nested directories?
- Do we support subcollections (e.g., `landsat/collection-2/level-2`)?
- How deep can the hierarchy go?

Looking at production STAC catalogs (Planetary Computer, AWS Open Data, Element 84), they consistently use flat structures with descriptive collection names rather than deep nesting.

## Decision

Portolan uses a **flat three-level hierarchy**:

```text
.portolan/                    ← catalog (one per init)
├── catalog.json
├── {collection}/             ← collection (first-level subdirectory)
│   ├── collection.json
│   ├── versions.json
│   └── {item}/               ← item (dataset within collection)
│       ├── data.parquet
│       └── ...
└── {collection}/
    └── ...
```

**Rules:**
1. `portolan init` creates the catalog at the current directory
2. First-level subdirectories are collections
3. Everything within a collection is an item (no subcollections)
4. Use descriptive collection names instead of nesting: `landsat-8-c2-l2` not `landsat/c2/l2`

## Consequences

### What becomes easier
- Clear mental model: catalog → collections → items
- Simple directory traversal for `dataset list`
- Matches how real-world STAC catalogs are organized
- No ambiguity when adding datasets

### What becomes harder
- Users with deeply nested source data must flatten or specify collection explicitly
- Can't represent hierarchical relationships (e.g., "Landsat" parent of "Landsat-8")

### Trade-offs
- We trade structural expressiveness for simplicity
- Users who need hierarchy can encode it in naming conventions

## Alternatives Considered

### 1. Support arbitrary nesting
**Rejected:** Creates ambiguity about what level is a "collection" vs "subcollection." Every directory operation becomes a question: "Is this a collection or an item?"

### 2. Interactive hierarchy detection
**Rejected:** "Found 3 levels deep. Which level is collections?" adds friction and makes the CLI less automatable.

### 3. Subcollections with explicit depth limit
**Rejected:** Adds complexity without clear benefit. Real-world catalogs don't use subcollections.
