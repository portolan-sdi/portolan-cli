"""Each AUTO rule fires on a catalog built to break it, and ``check --fix`` repairs it.

``RULE_REMEDIATION`` marks a rule AUTO when ``--fix`` owns its repair. These
properties hold that claim to account. For every AUTO rule id a breaker in
``breakers.py`` edits a copy of a valid catalog. The property then asserts
three things through the real CLI:

1. ``check`` reports the rule as a defect. An INFO is a note, not a defect,
   so a base that already emits an INFO for the rule still needs a new
   finding.
2. ``check --fix`` removes the defect.
3. A second ``check --fix`` changes no byte in the tree.

The completeness gate requires every AUTO rule id to have a breaker or a
reason in ``UNBREAKABLE``.
"""

from __future__ import annotations

import json
import shutil
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from portolan_cli.validation.remediation import RULE_REMEDIATION, Bucket
from tests.conftest import CliHarness
from tests.property import breakers
from tests.property.registry import BASE_OF, BREAKERS, UNBREAKABLE

pytestmark = pytest.mark.integration

AUTO = frozenset(rule for rule, row in RULE_REMEDIATION.items() if row.bucket is Bucket.AUTO)

#: Rules whose repair ``check --fix`` does not make. Each maps to the reason,
#: and the test runs as a strict xfail, so a fix turns it red.
_GEOPARQUET_KEPT = (
    "check --fix classifies an existing GeoParquet as cloud-native and never rewrites it. "
    "The convert fixer is inert and points at `portolan add --force --reconvert`."
)
_COG_KEPT = (
    "check --fix skips a GeoTIFF that passes cog_validate. Only `check --fix --force` "
    "re-optimizes it."
)
NOT_REPAIRED: dict[str, str] = {
    "PTL-DAT-003": (
        "check --fix selects only the inert convert fixer for a type that contradicts the "
        "bytes. No pass derives the media type from the bytes again."
    ),
    "PTL-DAT-004": (
        "check --fix converts the GeoTIFF after the checksum fixer recorded its old bytes, "
        "so file:size and file:checksum go stale and a second --fix rewrites the item."
    ),
    "PTL-DAT-006": _GEOPARQUET_KEPT,
    "PTL-DAT-007": _GEOPARQUET_KEPT,
    "PTL-DAT-008": _GEOPARQUET_KEPT,
    "PTL-DAT-011": _COG_KEPT,
    "PTL-DAT-013": _COG_KEPT,
}

# Each example runs the CLI four times, so the example count is a fraction of
# the profile's. CI gets 2 examples, a local run 4, and nightly 20.
EXAMPLES = max(2, settings().max_examples // 25)

Base = Callable[[str], tuple[Path, set[str]]]


def _defects(payload: dict[str, Any]) -> set[str]:
    """The rule ids that ``check --json`` reports at warning or error."""
    return {f["rule_id"] for f in payload["data"]["findings"] if f["severity"] != "info"}


def _check(run: CliHarness, root: Path) -> set[str]:
    return _defects(json.loads(run("check", str(root), "--json", expect=None).stdout))


def _tree(root: Path) -> dict[str, bytes]:
    return {
        path.relative_to(root).as_posix(): path.read_bytes()
        for path in sorted(root.rglob("*"))
        if path.is_file() and path.name != ".versions.lock"
    }


@pytest.fixture(scope="session")
def base(tmp_path_factory: pytest.TempPathFactory) -> Base:
    """Build each base catalog once, and record the defects its check reports."""
    built: dict[str, tuple[Path, set[str]]] = {}

    def get(name: str) -> tuple[Path, set[str]]:
        if name not in built:
            home = tmp_path_factory.mktemp(f"property-{name}")
            run = CliHarness(home)
            root = home / "catalog"
            breakers.BASES[name](root, run)
            run.verify()
            built[name] = (root, _check(run, root))
        return built[name]

    return get


def test_every_auto_rule_has_a_breaker_or_a_reason() -> None:
    assert set(BREAKERS) | set(UNBREAKABLE) == AUTO
    assert not set(BREAKERS) & set(UNBREAKABLE)
    assert all(reason.strip() for reason in UNBREAKABLE.values())
    assert set(NOT_REPAIRED) <= set(BREAKERS)


@contextmanager
def _broken(rule_id: str, base: Base, data: st.DataObject) -> Iterator[tuple[CliHarness, Path]]:
    """Copy the rule's base, break it, and assert that ``check`` reports the rule."""
    source, base_defects = base(BASE_OF.get(rule_id, "core"))
    assert rule_id not in base_defects
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp) / "catalog"
        shutil.copytree(source, root)
        run = CliHarness(Path(tmp))
        BREAKERS[rule_id](root, data)
        broken = _check(run, root)
        assert rule_id in broken, sorted(broken)
        yield run, root


@pytest.mark.parametrize("rule_id", sorted(BREAKERS))
@settings(max_examples=EXAMPLES)
@given(data=st.data())
def test_the_rule_fires(rule_id: str, base: Base, data: st.DataObject) -> None:
    with _broken(rule_id, base, data):
        pass


def _repairable() -> list[Any]:
    return [
        pytest.param(
            rule,
            id=rule,
            marks=[pytest.mark.xfail(strict=True, reason=NOT_REPAIRED[rule])]
            if rule in NOT_REPAIRED
            else [],
        )
        for rule in sorted(BREAKERS)
    ]


@pytest.mark.parametrize("rule_id", _repairable())
@settings(max_examples=EXAMPLES)
@given(data=st.data())
def test_fix_repairs_the_rule_and_a_second_fix_is_a_no_op(
    rule_id: str, base: Base, data: st.DataObject
) -> None:
    with _broken(rule_id, base, data) as (run, root):
        run("check", str(root), "--fix", expect=None)
        fixed = _check(run, root)
        assert rule_id not in fixed, sorted(fixed)

        before = _tree(root)
        run("check", str(root), "--fix", expect=None)
        after = _tree(root)
        assert {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)} == set()
