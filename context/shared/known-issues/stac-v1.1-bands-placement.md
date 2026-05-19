# Issue: Raster items place `bands` at `item.properties` instead of on assets

## Status
Known bug. Not yet fixed.

## Symptom

`pystac.Item.validate()` fails on every raster item portolan produces, with an error pointing at `item.properties.bands`:

```
pystac.errors.STACValidationError: Validation failed for Feature at
  <catalog>/elevation/dem-x/dem-x.json
  against schema at https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/item.json
False schema does not allow [{'name': 'band_1', 'data_type': 'float32',
  'statistics': {...}}]
Failed validating None in
  schema['allOf'][0]['allOf'][2]['allOf'][1]['then']['properties']['properties']: False
On instance['properties']:
  [{'name': 'band_1', ...}]
```

Reproduces on a fresh catalog created entirely with `portolan init/scan/add` — no test scripts or external tooling needed. Any COG/GeoTIFF that goes through `portolan add` triggers it.

## Root cause

STAC v1.1.0 unified the bands model: `bands` is an **asset-level field only**. The core item schema (`v1.1.0/item-spec/json-schema/item.json`) explicitly forbids `bands` at `item.properties`:

> *"if bands appears in any asset, it's disallowed in properties; if absent from all assets, it's also disallowed in properties"*
> — schema fragment at `allOf[0].allOf[2].allOf[1].then.properties.properties.bands: false`

Portolan-cli still writes bands at the item-properties level. Concretely:

| Site | What it does |
|------|--------------|
| `portolan_cli/stac.py:839, 843` (`add_raster_extension`) | `item.properties["bands"] = bands` |
| `portolan_cli/dataset.py:1062` (`_compute_and_apply_stats`) | applies per-band statistics into `stac_properties["bands"][i]["statistics"]` before item creation |
| `portolan_cli/dataset.py:_apply_nodata_defaults_to_bands` | injects nodata defaults into the same `stac_properties["bands"]` array |
| `portolan_cli/stac.py:447` (`get_stac_extensions_for_properties`) | uses `"bands" in properties` as a heuristic to auto-declare the raster extension |

The bands array is then carried into the item via `create_item(properties=stac_properties)` and additionally re-set by `add_raster_extension`.

The comment at `stac.py:820` says *"Per STAC v1.1.0: uses top-level 'bands' array instead of raster:bands"* — this was a partial migration from v1.0's `raster:bands` (asset-level, prefixed) to v1.1's unprefixed `bands`. The migration kept it at the wrong nesting level.

## Workaround

None currently in code. Test scripts can skip pystac validation of raster items, but that masks the bug. Downstream STAC consumers (validators, catalogs that re-validate on ingest) will reject these items.

## Proposed fix

Move bands to the data asset(s):

1. **`stac.py:add_raster_extension`**: replace `item.properties["bands"] = bands` with iteration over `item.assets`, setting `asset.extra_fields["bands"] = bands` on each asset whose `roles` includes `"data"`.
2. **`dataset.py:_compute_and_apply_stats` / `_apply_nodata_defaults_to_bands`**: rewrite to operate on `stac_assets[<key>].extra_fields["bands"]` instead of `stac_properties["bands"]`. (Requires the data-asset key to be known at that point — already true; `stac_assets` is built before stats are applied.)
3. **`dataset.py:_create_and_save_item`**: strip `"bands"` out of `stac_properties` before passing to `create_item`, so it doesn't get smuggled back into `item.properties`.
4. **`stac.py:447`**: drop the `"bands" in properties` auto-detect (no longer applicable) or replace with an `any("bands" in a.extra_fields for a in assets)` check.

## Touch surface

| Layer | Files |
|-------|-------|
| Source | `portolan_cli/stac.py`, `portolan_cli/dataset.py` |
| Existing tests asserting `item.properties["bands"]` (29 call sites across 7 files) | `tests/unit/test_dataset.py`, `tests/unit/test_unified_bands.py`, `tests/unit/test_stac_extensions.py`, `tests/unit/test_metadata_cog.py`, `tests/unit/models/test_item.py`, `tests/integration/test_stac_extensions_integration.py`, `tests/integration/test_add_with_defaults.py` |
| Downstream readers | `portolan_cli/readme.py:203` (`summaries.get("eo:bands", []) or summaries.get("raster:bands", [])` — collection-level, OK; verify nothing reads `item.properties["bands"]`) |
| New regression test | Add an integration test that runs `pystac.Catalog.from_file(...).validate_all()` on a freshly added raster item — locks in the fix and catches future regressions |

## References

- STAC v1.1.0 item schema: <https://schemas.stacspec.org/v1.1.0/item-spec/json-schema/item.json>
- STAC v1.1.0 unified bands model: <https://github.com/radiantearth/stac-spec/blob/v1.1.0/commons/common-metadata.md#bands>
- STAC raster extension v1.1.0: <https://github.com/stac-extensions/raster/blob/v1.1.0/README.md>
- Migration notes from `raster:bands` (v1.0) → `bands` (v1.1): <https://github.com/radiantearth/stac-spec/blob/v1.1.0/CHANGELOG.md>
- Surfaced during e2e testing on 2026-05-18 after merging upstream/main into the iceberg-extra branch; reproduces on a clean `init/scan/add` flow against a Copernicus DSM TIF.

## Regression test

To be added with the fix. Suggested location: `tests/integration/test_stac_validation.py` (new file) with a test that:
1. Creates a catalog with a raster item via the public CLI flow
2. Loads it with `pystac.Catalog.from_file(...)`
3. Asserts `catalog.validate_all()` succeeds
4. Asserts `item.properties` does NOT contain `"bands"`
5. Asserts at least one asset's `extra_fields["bands"]` matches the expected schema
