"""Catalog configuration for Iceberg backend.

Uses PyIceberg's load_catalog() for catalog-type-agnostic initialization.
Defaults to local SQL/SQLite. Users override via standard PyIceberg env vars:
  PYICEBERG_CATALOG__PORTOLAKE__TYPE=rest
  PYICEBERG_CATALOG__PORTOLAKE__URI=https://my-rest-catalog.example.com
  PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE=s3://my-bucket/warehouse

See: https://py.iceberg.apache.org/configuration/
"""

from __future__ import annotations

from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

CATALOG_NAME = "portolake"


def _default_properties(catalog_root: Path) -> dict[str, str]:
    """Build default catalog properties (SQL/SQLite, local warehouse)."""
    return {
        "type": "sql",
        "uri": f"sqlite:///{catalog_root}/.portolan/iceberg.db",
        "warehouse": (catalog_root / ".portolan" / "warehouse").as_uri(),
    }


def _get_external_config() -> dict[str, str] | None:
    """Get PyIceberg config for this catalog (YAML or env vars), if any."""
    from pyiceberg.utils.config import Config

    config = Config()
    result = config.get_catalog_config(CATALOG_NAME)
    if result is None:
        return None
    return {str(k): str(v) for k, v in result.items()}


def create_catalog(catalog_root: Path | None = None) -> Catalog:
    """Create an Iceberg catalog using PyIceberg's load_catalog().

    Precedence:
    1. External config (~/.pyiceberg.yaml or PYICEBERG_CATALOG__PORTOLAKE__* env vars)
       — if present and non-SQLite (e.g., REST/BigLake), use it directly.
    2. Local SQLite defaults using catalog_root (fallback when no external config).
    """
    external = _get_external_config()
    if external and "type" in external and external["type"] != "sql":
        # Force snapshot-loading-mode=all so REST catalogs that advertise
        # 'refs' (e.g. BigLake) still return historical snapshots — required
        # by list_versions/rollback/prune to see prior portolake.version
        # summaries, not just the current ref.
        return load_catalog(CATALOG_NAME, **{"snapshot-loading-mode": "all"})

    # SQLite or no external config: the catalog lives under the catalog root.
    # The current directory is never a fallback, because a SQLite catalog there
    # lands in whatever directory the caller runs from (issue #1056).
    if catalog_root is None:
        if external and "uri" in external:
            # The PyIceberg configuration names the SQLite catalog itself.
            return load_catalog(CATALOG_NAME, **{"type": "sql", **external})
        raise ValueError("The local Iceberg catalog needs the catalog root; none was given.")
    defaults = _default_properties(catalog_root)
    uri = defaults["uri"]
    if uri.startswith("sqlite:///"):
        db_path = Path(uri.removeprefix("sqlite:///"))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    return load_catalog(CATALOG_NAME, **defaults)
