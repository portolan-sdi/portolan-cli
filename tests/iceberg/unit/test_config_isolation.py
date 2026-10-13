"""The suite must ignore a catalog a developer exported in the shell or saved at home."""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from pyiceberg.utils.config import Config

pytestmark = pytest.mark.unit

_LEAKED = "PYICEBERG_CATALOG__PORTOLAKE__URI"


@pytest.fixture(scope="module", autouse=True)
def _exported_catalog():
    """Set the variable before the function-scoped isolation fixture runs.

    pytest sets a module-scoped fixture up before a function-scoped one, so
    this reproduces a developer who exports the catalog in the shell.
    """
    os.environ[_LEAKED] = "https://example.invalid/rest"
    yield
    os.environ.pop(_LEAKED, None)


def test_isolation_removes_an_exported_catalog_definition():
    """PyIceberg reads PYICEBERG_CATALOG__* and prefers it over the test catalog."""
    assert _LEAKED not in os.environ


def test_a_catalog_in_the_home_directory_is_not_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The stub file in PYICEBERG_HOME ends the search before the home directory."""
    home = tmp_path / "developer-home"
    home.mkdir()
    (home / ".pyiceberg.yaml").write_text(
        "catalog:\n  test:\n    uri: https://remote.example/catalog\n", encoding="utf-8"
    )
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("USERPROFILE", str(home))

    assert Config().get_catalog_config("test") is None


def test_the_home_directory_stays_real() -> None:
    """DuckDB finds its installed extensions in ~/.duckdb."""
    assert Path.home().name != "pyiceberg-home"
