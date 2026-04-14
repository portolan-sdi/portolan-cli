"""Tests for catalog configuration and creation."""

import sys
from pathlib import Path

import pytest

from portolan_cli.backends.iceberg.config import _default_properties, create_catalog

_skip_windows_sqlite = pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyIceberg SQL catalog warehouse paths broken on Windows (apache/iceberg-python#1005)",
)


@pytest.mark.unit
def test_default_properties_returns_sql_type():
    """Default catalog type should be 'sql'."""
    props = _default_properties()
    assert props["type"] == "sql"


@pytest.mark.unit
def test_default_properties_uses_portolan_dir_and_iceberg_db():
    """Default paths should use .portolan/iceberg.db (not .portolake/catalog.db)."""
    props = _default_properties()
    assert props["uri"].startswith("sqlite:///")
    assert ".portolan/iceberg.db" in props["uri"]
    assert props["warehouse"].startswith("file:")
    assert ".portolan/warehouse" in props["warehouse"]


@pytest.mark.unit
def test_default_properties_with_catalog_root(tmp_path):
    """catalog_root should determine the base path for SQLite URI and warehouse."""
    props = _default_properties(catalog_root=tmp_path)
    expected_uri = f"sqlite:///{tmp_path}/.portolan/iceberg.db"
    expected_warehouse = (tmp_path / ".portolan" / "warehouse").as_uri()
    assert props["uri"] == expected_uri
    assert props["warehouse"] == expected_warehouse


@pytest.mark.unit
def test_default_properties_without_catalog_root_uses_cwd():
    """Without catalog_root, defaults should derive from Path.cwd()."""
    props = _default_properties()
    cwd = str(Path.cwd())
    assert cwd in props["uri"]
    # warehouse uses Path.as_uri() which normalizes separators
    cwd_uri = Path.cwd().as_uri()
    assert cwd_uri in props["warehouse"]


@_skip_windows_sqlite
@pytest.mark.integration
def test_create_catalog_with_catalog_root(tmp_path, monkeypatch):
    """create_catalog(catalog_root=path) should create iceberg.db under path/.portolan/."""
    from pyiceberg.catalog import Catalog

    # Isolate from ~/.pyiceberg.yaml (e.g., REST/BigLake config)
    monkeypatch.setattr("portolan_cli.backends.iceberg.config._get_external_config", lambda: None)
    catalog = create_catalog(catalog_root=tmp_path)
    assert isinstance(catalog, Catalog)
    assert (tmp_path / ".portolan" / "iceberg.db").exists()


@_skip_windows_sqlite
@pytest.mark.integration
def test_create_catalog_returns_catalog_instance(tmp_path, monkeypatch):
    """create_catalog() should return a working Catalog instance."""
    from pyiceberg.catalog import Catalog

    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__URI",
        f"sqlite:///{tmp_path}/catalog.db",
    )
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE",
        (tmp_path / "warehouse").as_uri(),
    )
    catalog = create_catalog()
    assert isinstance(catalog, Catalog)


@_skip_windows_sqlite
@pytest.mark.integration
def test_pyiceberg_env_vars_override_defaults(tmp_path, monkeypatch):
    """PyIceberg env vars should override default properties."""
    custom_uri = f"sqlite:///{tmp_path}/custom.db"
    monkeypatch.setenv("PYICEBERG_CATALOG__PORTOLAKE__URI", custom_uri)
    monkeypatch.setenv(
        "PYICEBERG_CATALOG__PORTOLAKE__WAREHOUSE",
        (tmp_path / "warehouse").as_uri(),
    )
    catalog = create_catalog()
    # The catalog should be created successfully with the custom URI
    from pyiceberg.catalog import Catalog

    assert isinstance(catalog, Catalog)
