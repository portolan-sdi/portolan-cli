"""The Docker e2e fixtures skip, not error, on a machine without the docker binary."""

from __future__ import annotations

import pytest

from tests.iceberg.e2e import conftest as e2e_conftest

pytestmark = pytest.mark.unit


def test_a_missing_docker_binary_skips_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(e2e_conftest.shutil, "which", lambda name: None)

    with pytest.raises(pytest.skip.Exception) as skipped:
        e2e_conftest.require_docker()

    assert str(skipped.value) == "the e2e tests need the docker binary on PATH"


def test_a_present_docker_binary_does_not_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(e2e_conftest.shutil, "which", lambda name: f"/usr/bin/{name}")

    e2e_conftest.require_docker()
