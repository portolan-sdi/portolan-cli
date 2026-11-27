"""One breaker per AUTO rule id. ``set(BREAKERS) | set(UNBREAKABLE)`` equals the AUTO rule ids."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from hypothesis.strategies import DataObject

Breaker = Callable[[Path, DataObject], None]

BREAKERS: dict[str, Breaker] = {}

#: The base catalog each breaker edits, by rule id. A rule that is not here
#: uses the "core" base.
BASE_OF: dict[str, str] = {}

#: AUTO rule ids that no edit of a catalog can trigger, each with the reason.
UNBREAKABLE: dict[str, str] = {}


def breaks(rule_id: str, *, base: str = "core") -> Callable[[Breaker], Breaker]:
    """Register the decorated function as the breaker for ``rule_id``.

    A second breaker for the same id fails at import.
    """

    def register(fn: Breaker) -> Breaker:
        assert rule_id not in BREAKERS, f"duplicate breaker for {rule_id}"
        BREAKERS[rule_id] = fn
        if base != "core":
            BASE_OF[rule_id] = base
        return fn

    return register
