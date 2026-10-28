"""Baseline recording must reject failed or networked scenario commands."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pytest

from tests.perf import gates

if TYPE_CHECKING:
    from pathlib import Path

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("exit_code", "network"),
    [(1, []), ("ValueError: broken", []), (0, ["unexpected request"])],
    ids=["exit-status", "exception", "network"],
)
def test_record_rejects_failed_scenarios(
    exit_code: int | str,
    network: list[str],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A failed command must never produce replacement gate constants."""

    def measure(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {
            "exit_code": exit_code,
            "network": network,
            "counters": dict.fromkeys(gates.COUNTER_OWNER, 0),
            "sites": {},
        }

    monkeypatch.setattr(gates, "measure", measure)
    with pytest.raises(RuntimeError, match="add-one-collection.*failed"):
        gates._record("add-one-collection", tmp_path)
