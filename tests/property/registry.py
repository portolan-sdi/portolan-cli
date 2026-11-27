"""One breaker per AUTO rule id.

A breaker takes the root of a copy of a valid catalog and a Hypothesis data
object. It edits the copy so that its rule fires, and it draws from the data
object to choose which node, which asset, or which value to break.
``test_rules_fire.py`` then runs ``check``, ``check --fix``, and ``check``
again. The completeness gate there requires
``set(BREAKERS) | set(UNBREAKABLE)`` to equal the AUTO rule ids in
``RULE_REMEDIATION``.
"""

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
