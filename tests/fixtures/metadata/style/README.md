# Style Test Fixtures

Test fixtures for PMTiles Mapbox GL style specs (Issue #13).

## Valid Styles

| File | Description |
|------|-------------|
| `style_point.json` | Circle layer for point geometries |
| `style_polygon.json` | Fill layer for polygon geometries |
| `style_line.json` | Line layer for linestring geometries |
| `style_categorical.json` | Data-driven categorical color (match expression) |
| `style_graduated.json` | Graduated color ramp (interpolate expression) |

## Invalid Styles

| File | Description |
|------|-------------|
| `style_bad_syntax.json` | Invalid JSON (missing comma) |
| `style_missing_layers.json` | Missing required `layers` field |

## Style Schema

All styles follow Mapbox GL Style Spec v8:

```json
{
  "version": 8,
  "layers": [
    {
      "id": "layer-name",
      "type": "fill|line|circle",
      "source-layer": "layer-in-pmtiles",
      "paint": { ... }
    }
  ]
}
```

## Usage

These fixtures are used by:
- `tests/unit/test_style.py` — Style generation tests
- `tests/integration/test_thumbnail_workflow.py` — End-to-end tests

Portolan generates basic styles automatically based on geometry type.
Advanced styles (categorical, graduated) require manual configuration.
