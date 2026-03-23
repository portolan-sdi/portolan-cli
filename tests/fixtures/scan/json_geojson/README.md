# JSON GeoJSON Detection Fixtures

Test fixtures for Issue #256: Detect if .json files are valid GeoJSON.

## Purpose

Tests that `.json` files with GeoJSON content are correctly detected as geospatial,
while plain `.json` files are correctly skipped.

## Files

| File | Content | Expected Behavior |
|------|---------|-------------------|
| `rec_centers.json` | Valid GeoJSON (FeatureCollection with Points) | Detected as vector, added to `ready` |
| `config.json` | Plain JSON (app config) | Skipped as non-geospatial |

## Background

Per [Issue #256](https://github.com/portolan-sdi/portolan-cli/issues/256), GeoJSON files
are often saved with `.json` extension rather than `.geojson`. Portolan should detect
GeoJSON content by inspecting the file, not just relying on extension.

The detection uses `formats._detect_json_type()` which reads the first 8KB and looks
for GeoJSON type tokens like `"type":"FeatureCollection"`.
