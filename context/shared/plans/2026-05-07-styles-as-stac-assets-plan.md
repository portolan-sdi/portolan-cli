# Styles as STAC Assets — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace inline `pmtiles:style` with standalone Mapbox GL v8 style files stored as STAC assets, with a `portolan:styles` manifest on collections.

**Architecture:** Style files live in `{collection}/styles/*.json` as complete Mapbox GL v8 specs with relative PMTiles source paths. During PMTiles generation, a default style file is auto-created. During scan, all style files are discovered and registered as STAC assets with a `portolan:styles` array on the collection (first = default). The existing `VectorStyleConfig` dataclass and config loading are preserved — only the output format changes.

**Tech Stack:** Python 3.10+, Click CLI, pystac, JSON

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `portolan_cli/style.py` | Modify | Remove `build_pmtiles_style`, add `build_full_style`, `write_style_file`, `write_default_style`, `discover_styles`, `build_styles_manifest` |
| `portolan_cli/pmtiles.py` | Modify | Remove `pmtiles:style` from `add_pmtiles_asset_to_collection` and `_build_style_for_geoparquet`; call `write_default_style` instead |
| `portolan_cli/metadata/pmtiles.py` | Modify | Remove `style` field from `PMTilesMetadata` and `pmtiles:style` from `to_stac_properties` |
| `portolan_cli/scan_classify.py` | Modify | Update `STYLE_FILENAMES` to recognize files inside `styles/` directories |
| `portolan_cli/dataset.py` | Modify | Call style discovery + registration during collection building |
| `tests/unit/test_style.py` | Modify | Replace `build_pmtiles_style` tests with `build_full_style`/`write_style_file`/`discover_styles` tests |
| `tests/fixtures/metadata/style/valid/` | Modify | Update fixtures to full Mapbox GL style format (with `sources`) |
| `context/shared/adr/0044-styles-as-stac-assets.md` | Create | New ADR superseding 0043's style storage decision |
| `context/shared/adr/0043-style-and-thumbnail-architecture.md` | Modify | Add "Superseded by ADR-0044" note to style storage section |
| `portolan_cli/skills/sourcecoop.md` | Modify | Add styles section (canonical skill location in this repo) |
| `CLAUDE.md` | Modify | Add ADR-0044 to the ADR index |

---

### Task 1: Write ADR-0044

**Files:**
- Create: `context/shared/adr/0044-styles-as-stac-assets.md`
- Modify: `context/shared/adr/0043-style-and-thumbnail-architecture.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Create ADR-0044**

```markdown
# ADR-0044: Styles as STAC Assets

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
- Complete Mapbox GL specs work directly with MapLibre/Mapbox GL
- Browser-based style picker reads `portolan:styles` for discovery

**Harder:**
- Style files must be kept in sync with PMTiles source paths (mitigated by relative paths)
- Slightly more files on disk per collection

## Alternatives Considered

### Keep inline pmtiles:style with multiple styles
Rejected: Would require a non-standard array-of-styles property, still not independently addressable, and bloats STAC JSON for multi-style collections.

### Separate style.json without STAC registration
Rejected: No discoverability — consumers wouldn't know styles exist without convention-based scanning.
```

- [ ] **Step 2: Update ADR-0043 status**

In `context/shared/adr/0043-style-and-thumbnail-architecture.md`, change the Status section:

```markdown
## Status
Accepted (style storage section superseded by ADR-0044; thumbnail and basemap decisions remain active)
```

- [ ] **Step 3: Add ADR-0044 to CLAUDE.md index**

Add this row to the ADR index table in `CLAUDE.md`, after ADR-0043:

```markdown
| [0044](context/shared/adr/0044-styles-as-stac-assets.md) | Styles as standalone STAC assets (supersedes ADR-0043 style storage) |
```

- [ ] **Step 4: Commit**

```bash
git add context/shared/adr/0044-styles-as-stac-assets.md context/shared/adr/0043-style-and-thumbnail-architecture.md CLAUDE.md
git commit -m "docs(adr): add ADR-0044 styles as STAC assets

Supersedes ADR-0043's inline pmtiles:style approach with standalone
Mapbox GL v8 style files registered as STAC assets."
```

---

### Task 2: Update style fixtures to full Mapbox GL format

**Files:**
- Modify: `tests/fixtures/metadata/style/valid/style_polygon.json`
- Modify: `tests/fixtures/metadata/style/valid/style_line.json`
- Modify: `tests/fixtures/metadata/style/valid/style_point.json`
- Modify: `tests/fixtures/metadata/style/valid/style_categorical.json`
- Modify: `tests/fixtures/metadata/style/valid/style_graduated.json`

- [ ] **Step 1: Update polygon style fixture**

Replace `tests/fixtures/metadata/style/valid/style_polygon.json` with a complete Mapbox GL style:

```json
{
  "version": 8,
  "name": "Default",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../parcels.pmtiles"
    }
  },
  "layers": [
    {
      "id": "parcels-fill",
      "type": "fill",
      "source": "data",
      "source-layer": "parcels",
      "paint": {
        "fill-color": "#3388ff",
        "fill-opacity": 0.6,
        "fill-outline-color": "#2266cc"
      }
    }
  ]
}
```

- [ ] **Step 2: Update line style fixture**

Replace `tests/fixtures/metadata/style/valid/style_line.json`:

```json
{
  "version": 8,
  "name": "Default",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../roads.pmtiles"
    }
  },
  "layers": [
    {
      "id": "roads-line",
      "type": "line",
      "source": "data",
      "source-layer": "roads",
      "paint": {
        "line-color": "#3388ff",
        "line-width": 2,
        "line-opacity": 0.8
      }
    }
  ]
}
```

- [ ] **Step 3: Update point style fixture**

Replace `tests/fixtures/metadata/style/valid/style_point.json`:

```json
{
  "version": 8,
  "name": "Default",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../cities.pmtiles"
    }
  },
  "layers": [
    {
      "id": "cities-circle",
      "type": "circle",
      "source": "data",
      "source-layer": "cities",
      "paint": {
        "circle-color": "#3388ff",
        "circle-radius": 4,
        "circle-opacity": 0.8
      }
    }
  ]
}
```

- [ ] **Step 4: Update categorical style fixture**

Replace `tests/fixtures/metadata/style/valid/style_categorical.json`:

```json
{
  "version": 8,
  "name": "Land Use Categories",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../parcels.pmtiles"
    }
  },
  "layers": [
    {
      "id": "parcels-categorical",
      "type": "fill",
      "source": "data",
      "source-layer": "parcels",
      "paint": {
        "fill-color": [
          "match", ["get", "land_use"],
          "residential", "#ff6b6b",
          "commercial", "#4ecdc4",
          "industrial", "#45b7d1",
          "#95afc0"
        ],
        "fill-opacity": 0.7
      }
    }
  ]
}
```

- [ ] **Step 5: Update graduated style fixture**

Replace `tests/fixtures/metadata/style/valid/style_graduated.json`:

```json
{
  "version": 8,
  "name": "Population Density",
  "sources": {
    "data": {
      "type": "vector",
      "url": "../parcels.pmtiles"
    }
  },
  "layers": [
    {
      "id": "parcels-graduated",
      "type": "fill",
      "source": "data",
      "source-layer": "parcels",
      "paint": {
        "fill-color": [
          "interpolate", ["linear"], ["get", "population"],
          0, "#f7fbff",
          100, "#6baed6",
          1000, "#08306b"
        ],
        "fill-opacity": 0.7
      }
    }
  ]
}
```

- [ ] **Step 6: Commit**

```bash
git add tests/fixtures/metadata/style/valid/
git commit -m "test(fixtures): update style fixtures to full Mapbox GL format

Add sources block and name field to match the new standalone style
file format from ADR-0044."
```

---

### Task 3: Replace `build_pmtiles_style` with `build_full_style` and `write_style_file`

**Files:**
- Modify: `portolan_cli/style.py`
- Test: `tests/unit/test_style.py`

- [ ] **Step 1: Write failing tests for `build_full_style`**

Replace the `TestBuildPmtilesStyle` class in `tests/unit/test_style.py` with:

```python
class TestBuildFullStyle:
    """Tests for build_full_style function."""

    @pytest.mark.unit
    def test_polygon_full_style(self) -> None:
        """Builds complete Mapbox GL style for polygon geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Default",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        assert style["version"] == 8
        assert style["name"] == "Default"
        assert style["sources"]["data"]["type"] == "vector"
        assert style["sources"]["data"]["url"] == "../data.pmtiles"
        assert len(style["layers"]) == 1

        layer = style["layers"][0]
        assert layer["type"] == "fill"
        assert layer["source"] == "data"
        assert layer["source-layer"] == "parcels"
        assert layer["paint"]["fill-color"] == "#3388ff"
        assert layer["paint"]["fill-opacity"] == 0.6
        assert layer["paint"]["fill-outline-color"] == "#2266cc"

    @pytest.mark.unit
    def test_linestring_full_style(self) -> None:
        """Builds complete Mapbox GL style for line geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Roads",
            geometry_type="LineString",
            source_layer="roads",
            pmtiles_relative_path="../roads.pmtiles",
            config=config,
        )

        assert style["name"] == "Roads"
        assert style["sources"]["data"]["url"] == "../roads.pmtiles"
        layer = style["layers"][0]
        assert layer["type"] == "line"
        assert layer["paint"]["line-color"] == "#3388ff"

    @pytest.mark.unit
    def test_point_full_style(self) -> None:
        """Builds complete Mapbox GL style for point geometry."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Cities",
            geometry_type="Point",
            source_layer="cities",
            pmtiles_relative_path="../cities.pmtiles",
            config=config,
        )

        layer = style["layers"][0]
        assert layer["type"] == "circle"
        assert layer["paint"]["circle-radius"] == 4

    @pytest.mark.unit
    def test_custom_config_applied(self) -> None:
        """Custom VectorStyleConfig values are applied to full style."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig(
            polygon_fill_color="#ff0000",
            polygon_fill_opacity=0.9,
            polygon_outline_color="#000000",
        )
        style = build_full_style(
            name="Custom",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        layer = style["layers"][0]
        assert layer["paint"]["fill-color"] == "#ff0000"
        assert layer["paint"]["fill-opacity"] == 0.9
        assert layer["paint"]["fill-outline-color"] == "#000000"

    @pytest.mark.unit
    def test_full_style_is_json_serializable(self) -> None:
        """Full style dict round-trips through JSON."""
        import json
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Test",
            geometry_type="Polygon",
            source_layer="layer",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        serialized = json.dumps(style)
        deserialized = json.loads(serialized)
        assert deserialized == style
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestBuildFullStyle -v`
Expected: FAIL — `build_full_style` does not exist yet.

- [ ] **Step 3: Implement `build_full_style` in `style.py`**

Replace the `build_pmtiles_style` function in `portolan_cli/style.py` with:

```python
def build_full_style(
    name: str,
    geometry_type: str,
    source_layer: str,
    pmtiles_relative_path: str,
    config: VectorStyleConfig,
) -> dict[str, Any]:
    """Build a complete Mapbox GL v8 style spec for a PMTiles source.

    Generates a self-contained style with sources, layers, and metadata.
    The style can be loaded directly by MapLibre GL without assembly.

    Args:
        name: Human-readable style name (used in pickers).
        geometry_type: OGC geometry type (Point, LineString, Polygon, etc.).
        source_layer: Name of the source layer in PMTiles.
        pmtiles_relative_path: Relative path from styles/ dir to the PMTiles file.
        config: Style configuration for colors/sizes/opacities.

    Returns:
        Complete Mapbox GL v8 style dict.
    """
    geom_lower = geometry_type.lower()

    if "point" in geom_lower:
        layer_type = "circle"
        paint: dict[str, Any] = {
            "circle-color": config.point_color,
            "circle-radius": config.point_radius,
            "circle-opacity": config.point_opacity,
        }
        suffix = "circle"
    elif "line" in geom_lower:
        layer_type = "line"
        paint = {
            "line-color": config.line_color,
            "line-width": config.line_width,
            "line-opacity": config.line_opacity,
        }
        suffix = "line"
    else:
        layer_type = "fill"
        paint = {
            "fill-color": config.polygon_fill_color,
            "fill-opacity": config.polygon_fill_opacity,
            "fill-outline-color": config.polygon_outline_color,
        }
        suffix = "fill"

    return {
        "version": 8,
        "name": name,
        "sources": {
            "data": {
                "type": "vector",
                "url": pmtiles_relative_path,
            }
        },
        "layers": [
            {
                "id": f"{source_layer}-{suffix}",
                "type": layer_type,
                "source": "data",
                "source-layer": source_layer,
                "paint": paint,
            }
        ],
    }
```

Also update the module docstring at the top of `style.py` to replace `build_pmtiles_style` with `build_full_style` in the public API list, and add `write_style_file`, `write_default_style`, `discover_styles`, `build_styles_manifest`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestBuildFullStyle -v`
Expected: All 5 tests PASS.

- [ ] **Step 5: Write failing tests for `write_style_file`**

Add to `tests/unit/test_style.py`:

```python
class TestWriteStyleFile:
    """Tests for write_style_file function."""

    @pytest.mark.unit
    def test_writes_style_to_disk(self, tmp_path: Path) -> None:
        """Writes style JSON to the specified directory."""
        import json
        from portolan_cli.style import write_style_file

        style_dict = {"version": 8, "name": "Test", "sources": {}, "layers": []}
        style_dir = tmp_path / "styles"

        write_style_file(style_dir, "default", style_dict)

        output = style_dir / "default.json"
        assert output.exists()
        loaded = json.loads(output.read_text())
        assert loaded == style_dict

    @pytest.mark.unit
    def test_creates_styles_directory(self, tmp_path: Path) -> None:
        """Creates the styles/ directory if it doesn't exist."""
        from portolan_cli.style import write_style_file

        style_dir = tmp_path / "styles"
        assert not style_dir.exists()

        write_style_file(style_dir, "default", {"version": 8, "name": "T", "sources": {}, "layers": []})

        assert style_dir.exists()
        assert (style_dir / "default.json").exists()

    @pytest.mark.unit
    def test_overwrites_existing_file(self, tmp_path: Path) -> None:
        """Overwrites an existing style file."""
        import json
        from portolan_cli.style import write_style_file

        style_dir = tmp_path / "styles"
        style_dir.mkdir()
        (style_dir / "default.json").write_text('{"old": true}')

        new_style = {"version": 8, "name": "New", "sources": {}, "layers": []}
        write_style_file(style_dir, "default", new_style)

        loaded = json.loads((style_dir / "default.json").read_text())
        assert loaded == new_style
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestWriteStyleFile -v`
Expected: FAIL — `write_style_file` does not exist yet.

- [ ] **Step 7: Implement `write_style_file`**

Add to `portolan_cli/style.py`:

```python
def write_style_file(
    style_dir: Path,
    name: str,
    style_dict: dict[str, Any],
) -> Path:
    """Write a style dict to a JSON file.

    Args:
        style_dir: Directory to write to (created if needed).
        name: Filename stem (e.g., "default" -> "default.json").
        style_dict: Complete Mapbox GL style dict.

    Returns:
        Path to the written file.
    """
    style_dir.mkdir(parents=True, exist_ok=True)
    path = style_dir / f"{name}.json"
    path.write_text(json.dumps(style_dict, indent=2))
    return path
```

Add `import json` to the imports at the top of `style.py` (it's not there yet).

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestWriteStyleFile -v`
Expected: All 3 tests PASS.

- [ ] **Step 9: Write failing tests for `write_default_style`**

Add to `tests/unit/test_style.py`:

```python
class TestWriteDefaultStyle:
    """Tests for write_default_style convenience function."""

    @pytest.mark.unit
    def test_writes_default_style_file(self, tmp_path: Path) -> None:
        """Creates styles/default.json with correct content."""
        import json
        from portolan_cli.style import write_default_style

        path = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="buildings",
            pmtiles_filename="buildings.pmtiles",
        )

        assert path == tmp_path / "styles" / "default.json"
        assert path.exists()

        style = json.loads(path.read_text())
        assert style["version"] == 8
        assert style["name"] == "Default"
        assert style["sources"]["data"]["url"] == "../buildings.pmtiles"
        assert style["layers"][0]["source-layer"] == "buildings"
        assert style["layers"][0]["type"] == "fill"

    @pytest.mark.unit
    def test_uses_custom_config(self, tmp_path: Path) -> None:
        """Respects VectorStyleConfig when generating default style."""
        import json
        from portolan_cli.style import VectorStyleConfig, write_default_style

        config = VectorStyleConfig(polygon_fill_color="#ff0000")
        path = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="data",
            pmtiles_filename="data.pmtiles",
            config=config,
        )

        style = json.loads(path.read_text())
        assert style["layers"][0]["paint"]["fill-color"] == "#ff0000"

    @pytest.mark.unit
    def test_does_not_overwrite_existing(self, tmp_path: Path) -> None:
        """Does not overwrite an existing default.json (preserves user edits)."""
        from portolan_cli.style import write_default_style

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        existing = styles_dir / "default.json"
        existing.write_text('{"version": 8, "name": "Custom", "sources": {}, "layers": []}')

        path = write_default_style(
            collection_path=tmp_path,
            geometry_type="Polygon",
            source_layer="data",
            pmtiles_filename="data.pmtiles",
        )

        assert path is None
        assert '"Custom"' in existing.read_text()
```

- [ ] **Step 10: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestWriteDefaultStyle -v`
Expected: FAIL — `write_default_style` does not exist yet.

- [ ] **Step 11: Implement `write_default_style`**

Add to `portolan_cli/style.py`:

```python
def write_default_style(
    collection_path: Path,
    geometry_type: str,
    source_layer: str,
    pmtiles_filename: str,
    config: VectorStyleConfig | None = None,
) -> Path | None:
    """Write a default style file for a collection.

    Creates `{collection_path}/styles/default.json` with a complete Mapbox GL
    style. Does NOT overwrite if the file already exists (preserves user edits).

    Args:
        collection_path: Path to the collection directory.
        geometry_type: OGC geometry type.
        source_layer: Layer name in the PMTiles.
        pmtiles_filename: PMTiles filename (e.g., "data.pmtiles").
        config: Optional style config. Uses defaults if None.

    Returns:
        Path to the written file, or None if file already exists.
    """
    if config is None:
        config = VectorStyleConfig()

    style_dir = collection_path / "styles"
    default_path = style_dir / "default.json"

    if default_path.exists():
        logger.debug("Default style already exists at %s, skipping", default_path)
        return None

    pmtiles_relative_path = f"../{pmtiles_filename}"
    style_dict = build_full_style(
        name="Default",
        geometry_type=geometry_type,
        source_layer=source_layer,
        pmtiles_relative_path=pmtiles_relative_path,
        config=config,
    )

    return write_style_file(style_dir, "default", style_dict)
```

- [ ] **Step 12: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestWriteDefaultStyle -v`
Expected: All 3 tests PASS.

- [ ] **Step 13: Commit**

```bash
git add portolan_cli/style.py tests/unit/test_style.py
git commit -m "feat(style): add build_full_style, write_style_file, write_default_style

Replace build_pmtiles_style (partial Mapbox GL snippet) with
build_full_style (complete Mapbox GL v8 spec with sources).
Add write_style_file and write_default_style for disk I/O."
```

---

### Task 4: Add style discovery and manifest building

**Files:**
- Modify: `portolan_cli/style.py`
- Test: `tests/unit/test_style.py`

- [ ] **Step 1: Write failing tests for `discover_styles`**

Add to `tests/unit/test_style.py`:

```python
class TestDiscoverStyles:
    """Tests for discover_styles function."""

    @pytest.mark.unit
    def test_discovers_style_files(self, tmp_path: Path) -> None:
        """Finds all JSON files in styles/ directory."""
        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "default.json").write_text('{"version":8,"name":"Default","sources":{},"layers":[]}')
        (styles_dir / "by-age.json").write_text('{"version":8,"name":"By Age","sources":{},"layers":[]}')

        styles = discover_styles(tmp_path)

        assert len(styles) == 2
        assert any(s["key"] == "styles/default" for s in styles)
        assert any(s["key"] == "styles/by-age" for s in styles)

    @pytest.mark.unit
    def test_extracts_name_as_title(self, tmp_path: Path) -> None:
        """Uses the style's name field as the asset title."""
        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "by-age.json").write_text('{"version":8,"name":"Buildings by Age","sources":{},"layers":[]}')

        styles = discover_styles(tmp_path)

        assert styles[0]["title"] == "Buildings by Age"

    @pytest.mark.unit
    def test_returns_empty_when_no_styles_dir(self, tmp_path: Path) -> None:
        """Returns empty list when no styles/ directory exists."""
        from portolan_cli.style import discover_styles

        styles = discover_styles(tmp_path)
        assert styles == []

    @pytest.mark.unit
    def test_skips_non_json_files(self, tmp_path: Path) -> None:
        """Ignores non-JSON files in styles/ directory."""
        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "default.json").write_text('{"version":8,"name":"Default","sources":{},"layers":[]}')
        (styles_dir / "README.md").write_text("# Styles")

        styles = discover_styles(tmp_path)
        assert len(styles) == 1

    @pytest.mark.unit
    def test_skips_invalid_json(self, tmp_path: Path) -> None:
        """Skips files that are not valid JSON."""
        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "default.json").write_text('{"version":8,"name":"Default","sources":{},"layers":[]}')
        (styles_dir / "broken.json").write_text("{bad json")

        styles = discover_styles(tmp_path)
        assert len(styles) == 1

    @pytest.mark.unit
    def test_fallback_title_from_filename(self, tmp_path: Path) -> None:
        """Uses filename stem as title when name field is missing."""
        from portolan_cli.style import discover_styles

        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "my-style.json").write_text('{"version":8,"sources":{},"layers":[]}')

        styles = discover_styles(tmp_path)
        assert styles[0]["title"] == "my-style"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestDiscoverStyles -v`
Expected: FAIL — `discover_styles` does not exist yet.

- [ ] **Step 3: Implement `discover_styles`**

Add to `portolan_cli/style.py`:

```python
def discover_styles(collection_path: Path) -> list[dict[str, Any]]:
    """Discover style files in a collection's styles/ directory.

    Args:
        collection_path: Path to the collection directory.

    Returns:
        List of dicts with keys: key, href, title, description, path.
        Empty list if no styles/ directory or no valid style files.
    """
    styles_dir = collection_path / "styles"
    if not styles_dir.is_dir():
        return []

    styles: list[dict[str, Any]] = []
    for style_path in sorted(styles_dir.glob("*.json")):
        try:
            style_data = json.loads(style_path.read_text())
        except (json.JSONDecodeError, OSError):
            logger.warning("Skipping invalid style file: %s", style_path)
            continue

        if not isinstance(style_data, dict):
            continue

        name = style_path.stem
        title = style_data.get("name", name)
        description = style_data.get("description", "")

        styles.append({
            "key": f"styles/{name}",
            "href": f"./styles/{style_path.name}",
            "title": title,
            "description": description,
            "path": style_path,
        })

    return styles
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestDiscoverStyles -v`
Expected: All 6 tests PASS.

- [ ] **Step 5: Write failing tests for `build_styles_manifest`**

Add to `tests/unit/test_style.py`:

```python
class TestBuildStylesManifest:
    """Tests for build_styles_manifest function."""

    @pytest.mark.unit
    def test_default_first(self) -> None:
        """Default style is always first in the manifest."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            {"key": "styles/by-age"},
            {"key": "styles/default"},
            {"key": "styles/by-use"},
        ]

        manifest = build_styles_manifest(styles)

        assert manifest[0] == "styles/default"
        assert len(manifest) == 3

    @pytest.mark.unit
    def test_alphabetical_after_default(self) -> None:
        """Non-default styles are sorted alphabetically."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            {"key": "styles/zebra"},
            {"key": "styles/default"},
            {"key": "styles/alpha"},
        ]

        manifest = build_styles_manifest(styles)

        assert manifest == ["styles/default", "styles/alpha", "styles/zebra"]

    @pytest.mark.unit
    def test_no_default(self) -> None:
        """Works when there's no style named default."""
        from portolan_cli.style import build_styles_manifest

        styles = [
            {"key": "styles/by-use"},
            {"key": "styles/by-age"},
        ]

        manifest = build_styles_manifest(styles)

        assert manifest == ["styles/by-age", "styles/by-use"]

    @pytest.mark.unit
    def test_empty_list(self) -> None:
        """Returns empty list for no styles."""
        from portolan_cli.style import build_styles_manifest

        assert build_styles_manifest([]) == []

    @pytest.mark.unit
    def test_single_style(self) -> None:
        """Single style returns single-element list."""
        from portolan_cli.style import build_styles_manifest

        styles = [{"key": "styles/custom"}]
        manifest = build_styles_manifest(styles)
        assert manifest == ["styles/custom"]
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestBuildStylesManifest -v`
Expected: FAIL — `build_styles_manifest` does not exist yet.

- [ ] **Step 7: Implement `build_styles_manifest`**

Add to `portolan_cli/style.py`:

```python
def build_styles_manifest(styles: list[dict[str, Any]]) -> list[str]:
    """Build the portolan:styles manifest array.

    Orders style keys with "styles/default" first (if present),
    then remaining styles in alphabetical order.

    Args:
        styles: List of style dicts (from discover_styles).

    Returns:
        Ordered list of asset keys for the portolan:styles property.
    """
    keys = [s["key"] for s in styles]
    default_key = "styles/default"

    if default_key in keys:
        keys.remove(default_key)
        return [default_key] + sorted(keys)

    return sorted(keys)
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestBuildStylesManifest -v`
Expected: All 5 tests PASS.

- [ ] **Step 9: Commit**

```bash
git add portolan_cli/style.py tests/unit/test_style.py
git commit -m "feat(style): add discover_styles and build_styles_manifest

Discover style JSON files in styles/ directories and build ordered
portolan:styles manifest arrays for STAC collection properties."
```

---

### Task 5: Remove `pmtiles:style` from PMTiles and metadata modules

**Files:**
- Modify: `portolan_cli/pmtiles.py`
- Modify: `portolan_cli/metadata/pmtiles.py`
- Modify: `tests/unit/test_style.py`

- [ ] **Step 1: Remove `style` field from `PMTilesMetadata`**

In `portolan_cli/metadata/pmtiles.py`:

1. Remove the `style: dict[str, Any] | None = None` field from the `PMTilesMetadata` dataclass (line 39).
2. Remove the `if self.style:` block from `to_dict()` (lines 53-54).
3. Remove the `if self.style:` block from `to_stac_properties()` (lines 75-76).
4. Remove `style: Optional Mapbox GL style spec (Issue #13).` from the docstring (line 30).
5. Remove `from typing import Any` if it's no longer needed (check other uses first — `to_dict` and `to_stac_properties` still return `dict[str, Any]` so keep it).

- [ ] **Step 2: Remove `pmtiles:style` from `add_pmtiles_asset_to_collection`**

In `portolan_cli/pmtiles.py`:

1. Remove the `style: dict[str, Any] | None = None` parameter from `add_pmtiles_asset_to_collection` (line 319).
2. Remove the `style: Optional Mapbox GL style spec to add as pmtiles:style (Issue #13).` docstring line (line 331).
3. Remove the style update block in the "already exists" branch (lines 357-359):
   ```python
   # Remove these lines:
   if style and existing.get("pmtiles:style") != style:
       existing["pmtiles:style"] = style
       needs_update = True
   ```
4. Remove the style insertion in the new asset creation (lines 379-381):
   ```python
   # Remove these lines:
   if style:
       asset_dict["pmtiles:style"] = style
   ```

- [ ] **Step 3: Update `_build_style_for_geoparquet` to use `write_default_style`**

Replace `_build_style_for_geoparquet` in `portolan_cli/pmtiles.py` with:

```python
def _write_default_style_for_geoparquet(
    parquet_path: Path,
    layer_name: str,
    collection_path: Path,
    pmtiles_filename: str,
    catalog_path: Path | None = None,
) -> Path | None:
    """Write a default style file for a PMTiles asset.

    Args:
        parquet_path: Path to source GeoParquet (for geometry type detection).
        layer_name: Layer name in the PMTiles.
        collection_path: Path to the collection directory.
        pmtiles_filename: Name of the PMTiles file.
        catalog_path: Optional catalog path for loading style config.

    Returns:
        Path to the written style file, or None if skipped.
    """
    try:
        from portolan_cli.metadata.geoparquet import extract_geoparquet_metadata
        from portolan_cli.style import (
            VectorStyleConfig,
            get_vector_style_config,
            write_default_style,
        )
    except ImportError:
        logger.debug("Style dependencies not available")
        return None

    try:
        metadata = extract_geoparquet_metadata(parquet_path)
        geometry_type = metadata.geometry_type
        if not geometry_type:
            logger.debug("No geometry type found in %s", parquet_path)
            return None

        config = get_vector_style_config(catalog_path) if catalog_path else VectorStyleConfig()

        return write_default_style(
            collection_path=collection_path,
            geometry_type=geometry_type,
            source_layer=layer_name,
            pmtiles_filename=pmtiles_filename,
            config=config,
        )
    except Exception as e:
        logger.debug("Failed to write default style for %s: %s", parquet_path, e)
        return None
```

- [ ] **Step 4: Update `generate_pmtiles_for_collection` to call the new function**

In the `generate_pmtiles_for_collection` function in `portolan_cli/pmtiles.py`:

1. Remove the `style = _build_style_for_geoparquet(...)` line (line 573) and replace with a call after successful generation.
2. Remove `style=style` from both `add_pmtiles_asset_to_collection` calls (lines 577, 605).
3. After the successful generation block (after `result.generated.append(pmtiles_path)`, around line 611), add the default style file write:

```python
            # Generate default style file (ADR-0044)
            _write_default_style_for_geoparquet(
                parquet_path=parquet_path,
                layer_name=layer_name,
                collection_path=collection_path,
                pmtiles_filename=pmtiles_path.name,
                catalog_path=catalog_root,
            )
```

Also add it to the "skipped" branch — when PMTiles already exists but we still want to ensure a default style exists:

```python
        if not _should_generate(parquet_path, pmtiles_path, force):
            add_pmtiles_asset_to_collection(collection_path, asset_key, pmtiles_href)
            # Ensure default style exists even when PMTiles generation is skipped
            _write_default_style_for_geoparquet(
                parquet_path=parquet_path,
                layer_name=layer_name,
                collection_path=collection_path,
                pmtiles_filename=pmtiles_path.name,
                catalog_path=catalog_root,
            )
            result.skipped.append(pmtiles_path)
            continue
```

- [ ] **Step 5: Remove old build_pmtiles_style from style.py**

Delete the `build_pmtiles_style` function from `portolan_cli/style.py` (lines 120-177). It's fully replaced by `build_full_style`.

- [ ] **Step 6: Update old tests that reference `build_pmtiles_style`**

In `tests/unit/test_style.py`:

1. Delete the entire `TestBuildPmtilesStyle` class (it was replaced by `TestBuildFullStyle` in Task 3).
2. Update `TestStyleInAssetProperties` to use `build_full_style` instead of `build_pmtiles_style`:

```python
class TestStyleInAssetProperties:
    """Tests for style structure validation."""

    @pytest.mark.unit
    def test_full_style_structure(self) -> None:
        """Full style has all required Mapbox GL fields."""
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Test",
            geometry_type="Polygon",
            source_layer="parcels",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        assert "version" in style
        assert "name" in style
        assert "sources" in style
        assert "layers" in style
        assert isinstance(style["layers"], list)
        assert len(style["layers"]) > 0
        assert style["layers"][0]["source"] == "data"

    @pytest.mark.unit
    def test_style_is_json_serializable(self) -> None:
        """Style dict is JSON-serializable."""
        import json
        from portolan_cli.style import VectorStyleConfig, build_full_style

        config = VectorStyleConfig()
        style = build_full_style(
            name="Test",
            geometry_type="Polygon",
            source_layer="layer",
            pmtiles_relative_path="../data.pmtiles",
            config=config,
        )

        serialized = json.dumps(style)
        deserialized = json.loads(serialized)
        assert deserialized == style
```

- [ ] **Step 7: Update style fixture tests**

Update `TestStyleFixtures` in `tests/unit/test_style.py` to check for the new `sources` field:

```python
    @pytest.mark.unit
    def test_valid_point_style_loads(self, valid_style_dir: Path) -> None:
        """Valid point style fixture loads correctly."""
        import json

        style_path = valid_style_dir / "style_point.json"
        style = json.loads(style_path.read_text())

        assert style["version"] == 8
        assert "sources" in style
        assert "data" in style["sources"]
        assert len(style["layers"]) == 1
        assert style["layers"][0]["type"] == "circle"
        assert style["layers"][0]["source"] == "data"
```

Apply the same pattern (add `sources` and `source` assertions) to `test_valid_polygon_style_loads`, `test_valid_line_style_loads`, `test_categorical_style_has_match_expression`, and `test_graduated_style_has_interpolate_expression`.

- [ ] **Step 8: Run all style tests**

Run: `uv run pytest tests/unit/test_style.py -v`
Expected: All tests PASS.

- [ ] **Step 9: Run full test suite**

Run: `uv run pytest -m unit`
Expected: All unit tests PASS. No other modules should be affected since `pmtiles:style` was only set in `pmtiles.py` and `metadata/pmtiles.py`.

- [ ] **Step 10: Commit**

```bash
git add portolan_cli/pmtiles.py portolan_cli/metadata/pmtiles.py portolan_cli/style.py tests/unit/test_style.py
git commit -m "refactor(style): remove pmtiles:style inline approach

Replace inline pmtiles:style on assets with standalone style files
in styles/ directories. PMTiles generation now creates
styles/default.json via write_default_style."
```

---

### Task 6: Integrate style discovery into collection building

**Files:**
- Modify: `portolan_cli/dataset.py` (or wherever collections are built during scan/add)
- Modify: `portolan_cli/pmtiles.py`
- Test: `tests/unit/test_style.py`

- [ ] **Step 1: Write failing test for style registration in collection.json**

Add to `tests/unit/test_style.py`:

```python
class TestRegisterStyleAssets:
    """Tests for registering discovered styles as STAC assets."""

    @pytest.mark.unit
    def test_registers_style_assets_in_collection(self, tmp_path: Path) -> None:
        """Discovered styles are added as assets in collection.json."""
        import json
        from portolan_cli.style import discover_styles, register_style_assets

        # Set up collection.json
        collection_data = {
            "type": "Collection",
            "id": "test",
            "assets": {"data": {"href": "./data.parquet", "type": "application/x-parquet"}},
        }
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        # Set up styles
        styles_dir = tmp_path / "styles"
        styles_dir.mkdir()
        (styles_dir / "default.json").write_text(
            '{"version":8,"name":"Default","sources":{},"layers":[]}'
        )
        (styles_dir / "by-age.json").write_text(
            '{"version":8,"name":"By Age","description":"Buildings colored by construction year","sources":{},"layers":[]}'
        )

        styles = discover_styles(tmp_path)
        register_style_assets(tmp_path, styles)

        updated = json.loads((tmp_path / "collection.json").read_text())

        assert "styles/default" in updated["assets"]
        assert "styles/by-age" in updated["assets"]

        default_asset = updated["assets"]["styles/default"]
        assert default_asset["type"] == "application/json"
        assert default_asset["roles"] == ["style"]
        assert default_asset["title"] == "Default"

        by_age_asset = updated["assets"]["styles/by-age"]
        assert by_age_asset["title"] == "By Age"
        assert by_age_asset["description"] == "Buildings colored by construction year"

        assert updated["portolan:styles"] == ["styles/default", "styles/by-age"]

    @pytest.mark.unit
    def test_no_styles_no_manifest(self, tmp_path: Path) -> None:
        """No portolan:styles property when no styles exist."""
        import json
        from portolan_cli.style import register_style_assets

        collection_data = {"type": "Collection", "id": "test", "assets": {}}
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        register_style_assets(tmp_path, [])

        updated = json.loads((tmp_path / "collection.json").read_text())
        assert "portolan:styles" not in updated

    @pytest.mark.unit
    def test_removes_stale_style_assets(self, tmp_path: Path) -> None:
        """Removes style assets that no longer have files on disk."""
        import json
        from portolan_cli.style import register_style_assets

        collection_data = {
            "type": "Collection",
            "id": "test",
            "portolan:styles": ["styles/default", "styles/old"],
            "assets": {
                "styles/default": {"href": "./styles/default.json", "type": "application/json", "roles": ["style"]},
                "styles/old": {"href": "./styles/old.json", "type": "application/json", "roles": ["style"]},
            },
        }
        (tmp_path / "collection.json").write_text(json.dumps(collection_data))

        # Only default style exists on disk
        current_styles = [{"key": "styles/default", "href": "./styles/default.json", "title": "Default", "description": ""}]
        register_style_assets(tmp_path, current_styles)

        updated = json.loads((tmp_path / "collection.json").read_text())
        assert "styles/old" not in updated["assets"]
        assert updated["portolan:styles"] == ["styles/default"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/test_style.py::TestRegisterStyleAssets -v`
Expected: FAIL — `register_style_assets` does not exist yet.

- [ ] **Step 3: Implement `register_style_assets`**

Add to `portolan_cli/style.py`:

```python
def register_style_assets(
    collection_path: Path,
    styles: list[dict[str, Any]],
) -> None:
    """Register discovered styles as STAC assets and set portolan:styles manifest.

    Updates collection.json to add/update style assets and remove stale ones.

    Args:
        collection_path: Path to the collection directory.
        styles: List of style dicts from discover_styles().
    """
    collection_json_path = collection_path / "collection.json"
    if not collection_json_path.exists():
        return

    data = json.loads(collection_json_path.read_text())
    assets = data.get("assets", {})

    # Remove stale style assets (assets with "style" role that no longer have files)
    current_keys = {s["key"] for s in styles}
    stale_keys = [
        k for k, v in assets.items()
        if k.startswith("styles/") and k not in current_keys
    ]
    for key in stale_keys:
        del assets[key]

    # Add/update style assets
    for style_info in styles:
        asset_dict: dict[str, Any] = {
            "href": style_info["href"],
            "type": "application/json",
            "title": style_info["title"],
            "roles": ["style"],
        }
        if style_info.get("description"):
            asset_dict["description"] = style_info["description"]
        assets[style_info["key"]] = asset_dict

    data["assets"] = assets

    # Set or remove portolan:styles manifest
    if styles:
        data["portolan:styles"] = build_styles_manifest(styles)
    else:
        data.pop("portolan:styles", None)

    collection_json_path.write_text(json.dumps(data, indent=2))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/test_style.py::TestRegisterStyleAssets -v`
Expected: All 3 tests PASS.

- [ ] **Step 5: Wire style registration into PMTiles generation flow**

In `portolan_cli/pmtiles.py`, at the end of `generate_pmtiles_for_collection` (after the `for` loop, before `return result`), add style discovery and registration:

```python
    # Discover and register style assets (ADR-0044)
    from portolan_cli.style import discover_styles, register_style_assets

    styles = discover_styles(collection_path)
    if styles:
        register_style_assets(collection_path, styles)

    return result
```

- [ ] **Step 6: Run full test suite**

Run: `uv run pytest -m unit`
Expected: All tests PASS.

- [ ] **Step 7: Commit**

```bash
git add portolan_cli/style.py portolan_cli/pmtiles.py tests/unit/test_style.py
git commit -m "feat(style): register style assets in collection.json

Add register_style_assets to write style entries to STAC assets and
set portolan:styles manifest. Wired into PMTiles generation flow."
```

---

### Task 7: Update scan classifier for styles/ directory

**Files:**
- Modify: `portolan_cli/scan_classify.py`

- [ ] **Step 1: Update `STYLE_FILENAMES` and classification logic**

In `portolan_cli/scan_classify.py`, the current `STYLE_FILENAMES` only matches `style.json`. We need to also classify files inside `styles/` directories as style files. Find the `classify_file` function and add a check for files in `styles/` directories.

After the existing `STYLE_FILENAMES` check (around line 256), add:

```python
    # Check if file is inside a styles/ directory (ADR-0044)
    if path.parent.name == "styles" and ext == ".json":
        return (
            FileCategory.STYLE,
            SkipReasonType.METADATA_FILE,
            f"{path.name} is a map style definition in styles/ directory",
        )
```

- [ ] **Step 2: Run full tests**

Run: `uv run pytest -m unit`
Expected: All tests PASS.

- [ ] **Step 3: Commit**

```bash
git add portolan_cli/scan_classify.py
git commit -m "fix(scan): classify files in styles/ directories as style metadata

Files inside styles/ directories are map style definitions per ADR-0044
and should be classified as STYLE, not scanned as geo assets."
```

---

### Task 8: Update portolan skill

**Files:**
- Modify: `portolan_cli/skills/sourcecoop.md`

- [ ] **Step 1: Check the current skill file name and content**

Read `portolan_cli/skills/sourcecoop.md` to understand the current structure and find where to add the styles section.

- [ ] **Step 2: Add styles section to the skill**

Add a `## Styles` section to the skill file. Place it after the existing workflow/command sections:

```markdown
## Styles

Portolan supports multiple named visualization styles per collection. Each style is a complete Mapbox GL v8 JSON file stored in `{collection}/styles/`.

### Creating Styles

Style files are complete Mapbox GL v8 specs with relative PMTiles source paths:

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
  "layers": [
    {
      "id": "buildings-by-age",
      "type": "fill",
      "source": "data",
      "source-layer": "layer_name",
      "paint": {
        "fill-color": ["interpolate", ["linear"], ["get", "bouwjaar"],
          1400, "#1a0a00", 1900, "#8B4513", 1960, "#DAA520", 2020, "#FFFF00"
        ],
        "fill-opacity": 0.7
      }
    }
  ]
}
```

A default style is auto-generated during PMTiles creation. Drop additional style files into `styles/` and they'll be discovered automatically.

### Style Best Practices

1. **Create multiple styles for rich datasets.** If a collection has interesting categorical or numeric attributes, create data-driven styles for each. Example: buildings by construction year, by usage type, by height. Don't stop at a single default.

2. **Vary default styles across a catalog.** Each collection should have a visually distinct default color/palette. Use subject matter to inform color choices — water features in blues, vegetation in greens, built environment in warm tones, infrastructure in grays.

3. **Use data-driven styling.** Leverage Mapbox GL expressions (`interpolate`, `match`, `case`, `step`) to reveal patterns in data. For categorical data use `match`; for continuous data use `interpolate` or `step`.

4. **Include a description field on the STAC asset** explaining what the colors/sizes represent. This appears in style pickers and tooltips.

5. **Consider label layers.** For datasets with names (monuments, administrative areas, roads), add a label style layer or a dedicated "with labels" style variant.

6. **Look at the collection's table:columns** to understand what attributes are available for data-driven styling. Interesting fields for visualization include: categories/enums, dates/years, numeric measurements, status fields.

### STAC Registration

Styles are registered as collection-level assets with the `portolan:styles` manifest:

```json
{
  "portolan:styles": ["styles/default", "styles/by-age", "styles/by-use"],
  "assets": {
    "styles/default": {
      "href": "./styles/default.json",
      "type": "application/json",
      "title": "Default",
      "description": "Blue building footprints.",
      "roles": ["style"]
    }
  }
}
```

First entry in `portolan:styles` is the default. `portolan scan` discovers styles and registers them automatically.
```

- [ ] **Step 3: Sync to portolan-skills repo**

Copy the updated skill to the portolan-skills repo:

```bash
cp portolan_cli/skills/sourcecoop.md /Users/cholmes/repos/portolan-skills/skills/portolan-cli/SKILL.md
```

Note: The portolan-skills repo is a separate git repository. Commit there separately:

```bash
cd /Users/cholmes/repos/portolan-skills && git add skills/portolan-cli/SKILL.md && git commit -m "docs: sync portolan-cli skill with styles section"
```

- [ ] **Step 4: Commit in portolan-cli**

```bash
git add portolan_cli/skills/sourcecoop.md
git commit -m "docs(skill): add styles best practices to portolan skill

Guide AI agents on creating rich, varied style sets per collection
with data-driven styling and catalog-wide color diversity."
```

---

### Task 9: Clean up old `build_pmtiles_style` references and run full quality checks

**Files:**
- Various (grep-based cleanup)

- [ ] **Step 1: Search for any remaining `build_pmtiles_style` references**

```bash
grep -rn "build_pmtiles_style" portolan_cli/ tests/ --include="*.py"
```

Fix any remaining references to use `build_full_style`.

- [ ] **Step 2: Search for any remaining `pmtiles:style` references**

```bash
grep -rn "pmtiles:style\|pmtiles_style" portolan_cli/ tests/ --include="*.py"
```

Remove or update any remaining references.

- [ ] **Step 3: Run full test suite**

```bash
uv run pytest -m unit
```

Expected: All tests PASS.

- [ ] **Step 4: Run linting and type checks**

```bash
uv run ruff check .
uv run ruff format .
uv run mypy portolan_cli
```

Expected: All pass clean.

- [ ] **Step 5: Run dead code check**

```bash
uv run vulture portolan_cli tests
```

Expected: No new dead code introduced.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "chore: clean up remaining pmtiles:style references"
```

(Only if there were changes to commit.)

---

## Summary

| Task | What | Files |
|------|------|-------|
| 1 | ADR-0044 + CLAUDE.md index | `context/shared/adr/0044-*`, `0043-*`, `CLAUDE.md` |
| 2 | Update style fixtures | `tests/fixtures/metadata/style/valid/*.json` |
| 3 | `build_full_style` + `write_style_file` + `write_default_style` | `portolan_cli/style.py`, `tests/unit/test_style.py` |
| 4 | `discover_styles` + `build_styles_manifest` | `portolan_cli/style.py`, `tests/unit/test_style.py` |
| 5 | Remove `pmtiles:style` inline approach | `portolan_cli/pmtiles.py`, `metadata/pmtiles.py`, `style.py`, tests |
| 6 | `register_style_assets` + wire into PMTiles flow | `portolan_cli/style.py`, `pmtiles.py`, tests |
| 7 | Scan classifier for `styles/` directory | `portolan_cli/scan_classify.py` |
| 8 | Portolan skill update | `portolan_cli/skills/sourcecoop.md`, portolan-skills repo |
| 9 | Final cleanup + quality checks | Various |
