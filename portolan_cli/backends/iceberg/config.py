"""Catalog configuration for Iceberg backend."""

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
    """Create an Iceberg catalog using PyIceberg's load_catalog()."""
    external = _get_external_config()
    if external and "type" in external and external["type"] != "sql":
        # REST catalogs that advertise refs (such as BigLake) return old snapshots only in mode
        # "all".
        return load_catalog(CATALOG_NAME, **{"snapshot-loading-mode": "all"})

    # A SQLite catalog lives under the catalog root, never in the current directory.
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
