# Iceberg API Reference

The Iceberg backend implements the `VersioningBackend` protocol. Install with `pip install portolan-cli[iceberg]`.

## Loading the Backend

```python
from portolan_cli.backends import get_backend

backend = get_backend("iceberg")
```

## IcebergBackend

::: portolan_cli.backends.iceberg.backend.IcebergBackend
    options:
      show_source: false
      show_root_heading: true
      heading_level: 3
      members_order: source
      docstring_style: google

## Methods

### get_current_version

Get the current (latest) version of a collection.

```python
version = backend.get_current_version("demographics")
print(version.version)   # "2.1.0"
print(version.breaking)  # False
print(version.message)   # "Updated population data"
```

**Raises:** `FileNotFoundError` if the collection has no versions.

---

### list_versions

List all versions of a collection, ordered oldest to newest.

```python
versions = backend.list_versions("demographics")
for v in versions:
    print(f"{v.version} ({v.created}): {v.message}")
```

**Returns:** `list[Version]` — empty list if collection doesn't exist.

---

### publish

Publish a new version. Creates the Iceberg table on first publish.

```python
version = backend.publish(
    collection="demographics",
    assets={"data.parquet": "/path/to/data.parquet"},
    schema={"columns": ["id", "geom"], "types": {"id": "int64"}, "hash": "abc123"},
    breaking=False,
    message="Updated population estimates",
)
print(version.version)  # "1.1.0" (minor bump)
```

**Versioning rules:**

| Scenario | Version |
|----------|---------|
| First version | `1.0.0` |
| Non-breaking change | Minor bump (`1.0.0` -> `1.1.0`) |
| Breaking change | Major bump (`1.2.3` -> `2.0.0`) |

---

### rollback

Roll back to a previous version. Uses Iceberg's native snapshot management — instant, no data copy.

```python
rolled = backend.rollback("demographics", "1.0.0")
print(rolled.version)  # "1.0.0"
```

**Raises:** `ValueError` if the target version doesn't exist.

---

### prune

Remove old versions, keeping the N most recent.

```python
# Preview
prunable = backend.prune("demographics", keep=5, dry_run=True)
print(f"Would prune {len(prunable)} versions")

# Execute
pruned = backend.prune("demographics", keep=5, dry_run=False)
```

---

### check_drift

Check for drift between local and remote state. Currently a stub.

```python
report = backend.check_drift("demographics")
print(report["has_drift"])  # False
```
