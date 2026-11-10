"""The Docker e2e fixtures skip, not error, on a machine without the docker binary."""

from __future__ import annotations

import pytest

from tests import iceberg_stack

pytestmark = pytest.mark.unit


def test_a_missing_docker_binary_skips_with_the_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tests.iceberg_stack.shutil.which", lambda name: None)

    with pytest.raises(pytest.skip.Exception) as skipped:
        iceberg_stack.require_docker()

    assert str(skipped.value) == "the e2e tests need the docker binary on PATH"


def test_a_present_docker_binary_does_not_skip(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tests.iceberg_stack.shutil.which", lambda name: f"/usr/bin/{name}")

    iceberg_stack.require_docker()
