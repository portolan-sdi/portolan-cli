# PySTAC Absolute Path Behavior

## Status
Known behavior (not a bug)

## Summary

PySTAC assumes self links are absolute and includes absolute filesystem paths when serializing STAC objects with `to_dict()`. This is problematic when publishing catalogs that should use relative paths.

## Details

When you create a STAC Catalog or Item with pystac and call `to_dict()` or `save()`, the library:

1. **Tracks file locations via self links**: PySTAC uses self links internally to track where objects were read from or will be saved to
2. **Assumes self hrefs are absolute**: The code path for resolving links assumes `self.href` is an absolute path
3. **Includes local paths in output**: If you load a catalog from `/home/user/project/catalog.json`, that absolute path may appear in the serialized output

This causes issues when:
- Publishing catalogs to object storage (S3/GCS) — local paths leak into published files
- Creating portable catalogs that work regardless of where they're cloned
- Generating catalogs programmatically without a filesystem location

## Workarounds

### Option 1: Build JSON manually (recommended for Portolan)

Construct STAC JSON directly instead of using pystac's serialization:

```python
# Instead of:
catalog = pystac.Catalog(id="my-catalog", description="...")
catalog.save(dest_href="./catalog.json")

# Do this:
catalog_dict = {
    "type": "Catalog",
    "stac_version": "1.1.0",
    "id": "my-catalog",
    "description": "...",
    "links": [
        {"rel": "root", "href": "./catalog.json", "type": "application/json"},
        {"rel": "self", "href": "./catalog.json", "type": "application/json"},
    ]
}
with open("catalog.json", "w") as f:
    json.dump(catalog_dict, f, indent=2)
```

### Option 2: Use normalize_hrefs() before saving

```python
catalog.normalize_hrefs(root_href="https://example.com/catalog")
catalog.save(catalog_type=pystac.CatalogType.RELATIVE_PUBLISHED)
```

### Option 3: Post-process to remove absolute paths

```python
# After saving, manually fix the self link
with open("catalog.json") as f:
    data = json.load(f)
for link in data.get("links", []):
    if link.get("rel") == "self":
        link["href"] = "./catalog.json"
with open("catalog.json", "w") as f:
    json.dump(data, f, indent=2)
```

## Portolan Approach

Portolan will:
1. **Use pystac for reading/validation** — it has robust STAC compliance checks
2. **Build JSON manually for writing** — full control over link paths
3. **Always use relative paths** — catalogs should be portable

This follows the pattern established in the Argentina census catalog script.

## References

- [PySTAC Concepts: Self Links](https://pystac.readthedocs.io/en/stable/concepts.html)
- [GitHub Issue #137: Unable to read catalog as SELF_CONTAINED](https://github.com/stac-utils/pystac/issues/137)
- [GitHub PR #574: Relative self hrefs break link resolution](https://github.com/stac-utils/pystac/pull/574)
- Argentina census catalog script (manual JSON construction example)
