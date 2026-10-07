"""The suite must ignore a catalog a developer exported in the shell."""

from __future__ import annotations

import os

import pytest

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
