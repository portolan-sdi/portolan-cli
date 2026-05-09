# ADR-0045: Styles as STAC Assets

## Status
Accepted (supersedes ADR-0043 style storage section)

## Context

ADR-0043 stored styles inline on PMTiles assets as `pmtiles:style` — a single Mapbox GL style snippet embedded in the STAC asset's extra fields. This approach:

1. Only supports a single style per asset (no "buildings by age" vs "by use" alternatives)
2. Requires parsing STAC to extract the style — it's not independently addressable
3. Uses a partial Mapbox GL spec (layers only, no sources) requiring consumers to assemble the full style

We want to support multiple named styles per collection, each independently loadable as a URL, with human-readable titles and descriptions for style picker UIs.

## Decision

### Style files as standalone assets

Each style is a complete Mapbox GL v8 JSON file stored in `{collection}/styles/`:

```
collection/
├── collection.json
├── data.pmtiles
└── styles/
    ├── default.json
    ├── by-age.json
    └── by-use.json
```

Each style file is self-contained with a relative source path to the PMTiles:

```json
{
  "version": 8,
  "name": "Buildings by Construction Year",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../data.pmtiles"
    }
  },
  "layers": [...]
}
```

### STAC registration

Style files are registered as collection-level assets with:
- Key: `styles/{name}` (e.g., `styles/default`, `styles/by-age`)
- Type: `application/json`
- Roles: `["style"]`
- Title: Short label for picker UIs
- Description: What the style shows (colors, data mapping)

### Collection manifest

A `portolan:styles` array on the collection lists asset keys in display order. First entry is the default style.

```json
{
  "portolan:styles": ["styles/default", "styles/by-age"],
  "assets": {
    "styles/default": {
      "href": "./styles/default.json",
      "type": "application/json",
      "title": "Default",
      "roles": ["style"]
    }
  }
}
```

This starts as a Portolan convention (`portolan:` prefix) that may evolve into a standalone STAC extension.

### Default style generation

`portolan` auto-generates `styles/default.json` during PMTiles generation (not during scan — per ADR-0016). User-created style files are never overwritten.

### Style discovery

During scan, `styles/*.json` files are discovered and registered as STAC assets. `styles/default` sorts first; remaining styles are alphabetical.

## Consequences

**Easier:**
- Multiple styles per collection (data-driven, thematic, labeled)
- Styles are independently addressable by URL — no STAC parsing needed
- Complete Mapbox GL specs work directly with Mapbox GL JS; MapLibre GL requires registering a `pmtiles://` protocol handler (see [protomaps docs](https://docs.protomaps.com/pmtiles/maplibre))
- Browser-based style picker reads `portolan:styles` for discovery

**Harder:**
- Style files must be kept in sync with PMTiles source paths (mitigated by relative paths)
- Slightly more files on disk per collection

## Alternatives Considered

### Keep inline pmtiles:style with multiple styles
Rejected: Would require a non-standard array-of-styles property, still not independently addressable, and bloats STAC JSON for multi-style collections.

### Separate style.json without STAC registration
Rejected: No discoverability — consumers wouldn't know styles exist without convention-based scanning.
