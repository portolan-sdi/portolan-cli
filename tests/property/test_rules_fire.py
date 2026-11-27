"""Each AUTO rule fires on a catalog built to break it, and ``check --fix`` repairs it.
A second ``check --fix`` changes no byte; each AUTO rule needs a breaker or an ``UNBREAKABLE`` reason.
"""

from __future__ import annotations

import hashlib
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
from tests.integration.core.test_writer_apply import _tree
from tests.property import breakers
from tests.property.registry import BASE_OF, BREAKERS, UNBREAKABLE

pytestmark = pytest.mark.integration

AUTO = frozenset(rule for rule, row in RULE_REMEDIATION.items() if row.bucket is Bucket.AUTO)

# Each example runs the CLI four times, so the example count is a fraction of
# the profile's. CI gets 2 examples, a local run 4, and nightly 20.
EXAMPLES = max(2, settings().max_examples // 25)

Base = Callable[[str], tuple[Path, set[str]]]


def _defects(payload: dict[str, Any]) -> set[str]:
    """The rule ids that ``check --json`` reports at warning or error."""
    return {f["rule_id"] for f in payload["data"]["findings"] if f["severity"] != "info"}


def _unrecorded(root: Path) -> list[str]:
    """The tracked files whose bytes differ from their versions.json record."""
    wrong = []
    for versions in root.rglob("versions.json"):
        if versions.parent == root:
            continue
        assets = json.loads(versions.read_text())["versions"][-1]["assets"]
        for key, asset in assets.items():
            path = versions.parent / key
            if path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() != asset["sha256"]:
                wrong.append(path.relative_to(root).as_posix())
    return wrong


def _check(run: CliHarness, root: Path) -> set[str]:
    return _defects(json.loads(run("check", str(root), "--json", expect=None).stdout))


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


@pytest.mark.parametrize("rule_id", sorted(BREAKERS))
@settings(max_examples=EXAMPLES)
@given(data=st.data())
def test_fix_repairs_the_rule_and_a_second_fix_is_a_no_op(
    rule_id: str, base: Base, data: st.DataObject
) -> None:
    with _broken(rule_id, base, data) as (run, root):
        run("check", str(root), "--fix", expect=None)
        fixed = _check(run, root)
        assert rule_id not in fixed, sorted(fixed)
        assert _unrecorded(root) == []

        before = _tree(root)
        run("check", str(root), "--fix", expect=None)
        after = _tree(root)
        assert {k for k in before.keys() | after.keys() if before.get(k) != after.get(k)} == set()
