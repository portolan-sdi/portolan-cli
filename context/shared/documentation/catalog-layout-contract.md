# Catalog layout contract

`scan_catalog_metadata` in `portolan_cli/metadata/scan.py` walks the STAC
manifest tree and assumes the layout that `add` writes. Changing either one
without the other breaks freshness checking. This note records the assumptions
and the two boundaries the scanner deliberately refuses to cross.

For the statuses the scanner emits and which are auto-fixable, see
`.claude/rules/scan-check.md`.

## What `add` writes, and what the scanner expects

```
{collection_dir}/
  collection.json           registers collection-level assets
  versions.json             freshness truth, resolved at this level
  data.parquet              single-file vector collection, no item.json
  items.parquet             rollup from `add --stac-geoparquet`
  {item_id}/
    {item_id}.json          item manifest
    <asset>                 item assets, siblings of the item JSON
```

Nested sub-catalogs place `catalog.json` under the collection with item
directories beneath it. Items still belong to the enclosing collection, so
`versions.json` and item assets resolve against `collection_dir` even when the
item sits several levels deeper. `fix_metadata` walks ancestors through
`_resolve_collection_dir` to make those items fixable when the CLI passes the
catalog root.

## The anchor rule

A subdirectory counts as an item needing JSON only when it contains a data file
whose stem matches the directory name. That is the `{item_id}/{item_id}.{ext}`
convention above.

Subdirectories holding loose data files without that anchor, a `scratch/` folder
of exports for example, are reported as ORPHANED. Without the anchor rule,
`--fix` would fabricate a `{dir_name}.json` naming an item that does not exist.

## Registration is not a freshness claim

When a collection-level asset has no matching `versions.json` entry, the scanner
stays silent rather than emitting STALE. A freshly registered rollup index has
nothing to be stale against. Emitting STALE there produces noise on every fresh
catalog.

## The legacy flat layout is unsupported

An `item.json` sitting beside its data file in the collection root, with no
`collection.json.assets` entry, is detected as ORPHANED rather than accepted.
The scanner supports the hierarchical shape and registered collection-level
assets, and nothing else. Catalogs in the flat shape migrate by re-registering
through `portolan add`.

## If you change `add`

The scanner relies on `{item_id}.json` matching its parent directory name.
Renaming what `add` produces requires updating the scanner in the same change.
