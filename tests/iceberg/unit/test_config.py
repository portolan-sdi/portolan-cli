"""Tests for catalog configuration and creation."""

from __future__ import annotations

import sys

import pytest

from portolan_cli.backends.iceberg.config import _default_properties, create_catalog

_skip_windows_sqlite = pytest.mark.skipif(
    sys.platform == "win32",
    reason="PyIceberg SQL catalog warehouse paths broken on Windows (apache/iceberg-python#1005)",
)


@pytest.mark.unit
def test_default_properties_returns_sql_type(tmp_path):
    """Default catalog type should be 'sql'."""
    props = _default_properties(tmp_path)
    assert props["type"] == "sql"


@pytest.mark.unit
def test_default_properties_with_catalog_root(tmp_path):
    """catalog_root should determine the base path for SQLite URI and warehouse."""
    props = _default_properties(catalog_root=tmp_path)
    expected_uri = f"sqlite:///{tmp_path}/.portolan/iceberg.db"
    expected_warehouse = (tmp_path / ".portolan" / "warehouse").as_uri()
    assert props["uri"] == expected_uri
    assert props["warehouse"] == expected_warehouse


@pytest.mark.unit
def test_the_sqlite_catalog_needs_a_catalog_root(tmp_path, monkeypatch):
    """#1056: no catalog root raises, and nothing lands in the current directory."""
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    with pytest.raises(ValueError, match="catalog root"):
        create_catalog(None)
    assert list(cwd.iterdir()) == []


@_skip_windows_sqlite
@pytest.mark.integration
def test_init_writes_nothing_in_the_current_directory(tmp_path, monkeypatch):
    """#1056: `init <path> --backend iceberg` writes under <path> only.

    On main init checked the backend with no catalog root, so the SQLite
    catalog appeared at <cwd>/.portolan/iceberg.db.
    """
    from click.testing import CliRunner

    from portolan_cli.cli import cli

    cwd = tmp_path / "cwd"
    cwd.mkdir()
    target = tmp_path / "catalog"
    monkeypatch.chdir(cwd)

    result = CliRunner().invoke(
        cli, ["init", str(target), "--backend", "iceberg", "--license", "CC0-1.0", "--auto"]
    )

    assert result.exit_code == 0, result.output
    assert list(cwd.iterdir()) == []
    assert (target / ".portolan" / "config.yaml").is_file()


@_skip_windows_sqlite
@pytest.mark.integration
def test_create_catalog_with_catalog_root(tmp_path):
    """create_catalog(catalog_root=path) should create iceberg.db under path/.portolan/."""
    from pyiceberg.catalog import Catalog

    catalog = create_catalog(catalog_root=tmp_path)
    assert isinstance(catalog, Catalog)
    assert (tmp_path / ".portolan" / "iceberg.db").exists()


@_skip_windows_sqlite
@pytest.mark.integration
def test_create_catalog_returns_catalog_instance(tmp_path, monkeypatch):
    """create_catalog() should return a working Catalog instance."""
    from pyiceberg.catalog import Catalog

    # With no TYPE in the environment, create_catalog keeps the SQLite default
    # and puts .portolan/iceberg.db in the working directory.
    monkeypatch.chdir(tmp_path)

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
    # With no TYPE in the environment, create_catalog keeps the SQLite default
    # and puts .portolan/iceberg.db in the working directory.
    monkeypatch.chdir(tmp_path)
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
