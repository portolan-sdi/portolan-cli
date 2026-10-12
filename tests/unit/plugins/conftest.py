"""Fixtures shared by the plugin system tests."""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from portolan_cli.plugins import manager

if TYPE_CHECKING:
    from collections.abc import Iterator
    from importlib.metadata import EntryPoint


@pytest.fixture
def installed(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[EntryPoint]]:
    """Replace the installed entry points with a list the test fills.

    The fixture also clears ``PORTOLAN_PLUGINS`` and the cached manager.
    """
    eps: list[EntryPoint] = []

    def _entry_points(*, group: str) -> list[EntryPoint]:
        return [ep for ep in eps if ep.group == group]

    monkeypatch.setattr(manager, "entry_points", _entry_points)
    monkeypatch.delenv(manager.ENV_VAR, raising=False)
    manager.reset_plugin_manager()
    yield eps
    manager.reset_plugin_manager()
