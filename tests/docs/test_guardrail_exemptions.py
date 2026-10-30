"""tests/guardrail_exemptions.txt lists every exemption the guardrails grant (#1035).

The file holds three kinds of line. This module keeps it equal to the code:

- ``xfail``: one line per open issue that a strict xfail, a
  ``KNOWN_INVARIANT_GAPS`` entry, a rashid scenario pin, or an ``invariant``
  reason names. The set of
  issue numbers in the code equals the set in the file, so a fix that deletes
  its marker must delete its line, and a new marker must add one.
- ``invariant``: each ``build_damaged_catalog`` reason. Each one is used by a
  test.
- ``config-key``: each setting that the reachability test cannot observe.

The count of each kind is pinned. A change to a count is a reviewed change to
this module.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

import pytest

from tests.catalog_invariants import EXEMPTIONS_FILE, KNOWN_INVARIANT_GAPS, load_exemptions

pytestmark = pytest.mark.unit

TESTS = Path(__file__).resolve().parents[1]
ISSUE_REASON = re.compile(r"^#(\d+): \S")
ISSUE_KEY = re.compile(r"^#(\d+)$")
OWNERS = frozenset({"A", "B", "C", "D", "E", "F", "G1", "G2", "G3", "G4", "spec", "none"})
# A scenario pin may name a designed result instead of an issue. Each entry is
# a result the spec intends, so no fix will remove it.
DESIGN_REASONS = frozenset(
    {
        "host unset by design",
        "single COG layout: spec and CLI disagree (PTL-COL-001), maintainer decision",
    }
)

EXPECTED_COUNTS = {"xfail": 2, "invariant": 3, "config-key": 7}


def _sources() -> dict[Path, ast.Module]:
    return {
        path: ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for path in sorted(TESTS.rglob("*.py"))
        if "fixtures" not in path.relative_to(TESTS).parts
    }


def _strings(tree: ast.Module) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _xfail_calls(tree: ast.Module) -> list[ast.Call]:
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "xfail"
    ]


def _docstrings(tree: ast.Module) -> set[int]:
    """Return the ids of the docstring nodes, which never hold a marker reason."""
    found: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
            body = node.body
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                found.add(id(body[0].value))
    return found


def xfail_reasons(tree: ast.Module) -> set[str]:
    """Return the reasons of the xfail markers in one module.

    A constant reason counts as written. A reason built from a name, such as a
    dict of reasons per case, counts every ``#N: ...`` string of the module
    that is not a docstring.
    """
    reasons: set[str] = set()
    indirect = False
    for call in _xfail_calls(tree):
        reason = {k.arg: k.value for k in call.keywords}.get("reason")
        if isinstance(reason, ast.Constant) and isinstance(reason.value, str):
            reasons.add(reason.value)
        elif reason is not None:
            indirect = True
    if indirect:
        docstrings = _docstrings(tree)
        reasons |= {
            node.value
            for node in ast.walk(tree)
            if isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
            and ISSUE_REASON.match(node.value)
        }
    return reasons


def _scenario_keys() -> set[str]:
    from tests.integration.test_generated_catalog_conformance import SCENARIOS

    return {
        key
        for row in SCENARIOS.values()
        for key in (*row.known_errors.values(), *row.known_invariants.values())
    }


def _issues_in_code() -> set[str]:
    """Return every issue number that an exemption in the code names."""
    issues = {
        match.group(1)
        for tree in _sources().values()
        for text in xfail_reasons(tree)
        if (match := ISSUE_REASON.match(text))
    }
    # An invariant exemption that names an open bug counts as an xfail of it.
    for row in load_exemptions("invariant"):
        issues.update(re.findall(r"#(\d+)", row[0]))
    for key in {*KNOWN_INVARIANT_GAPS.values(), *_scenario_keys()} - DESIGN_REASONS:
        match = ISSUE_KEY.match(key)
        assert match, f"{key!r} is neither '#N' nor a design reason"
        issues.add(match.group(1))
    return issues


def test_every_line_has_a_known_kind_and_shape() -> None:
    kinds = {"xfail": 3, "invariant": 1, "config-key": 1}
    for line in EXEMPTIONS_FILE.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        kind, *fields = line.split("\t")
        assert kind in kinds, line
        assert len(fields) == kinds[kind], line
        assert all(field.strip() == field and field for field in fields), line


def test_each_kind_has_its_pinned_count() -> None:
    counts = {kind: len(load_exemptions(kind)) for kind in EXPECTED_COUNTS}

    assert counts == EXPECTED_COUNTS


def test_the_xfail_lines_name_the_issues_the_code_names() -> None:
    rows = load_exemptions("xfail")
    in_file = [row[0].removeprefix("#") for row in rows]

    assert len(in_file) == len(set(in_file)), "one line per issue"
    assert set(in_file) == _issues_in_code()


def test_each_xfail_line_names_a_known_owner() -> None:
    assert {row[1] for row in load_exemptions("xfail")} <= OWNERS


def test_every_xfail_is_strict_with_an_issue_reason() -> None:
    """A marker states its issue first, or the upstream issue URL."""
    loose: list[str] = []
    for path, tree in _sources().items():
        for call in _xfail_calls(tree):
            keywords = {k.arg: k.value for k in call.keywords}
            strict = keywords.get("strict")
            reason = keywords.get("reason")
            where = f"{path.relative_to(TESTS)}:{call.lineno}"
            if not (isinstance(strict, ast.Constant) and strict.value is True):
                loose.append(f"{where}: not strict")
            if isinstance(reason, ast.Constant) and not (
                ISSUE_REASON.match(str(reason.value))
                or str(reason.value).startswith("https://github.com/")
            ):
                loose.append(f"{where}: reason {reason.value!r}")
            if reason is None:
                loose.append(f"{where}: no reason")

    assert loose == []


def test_every_invariant_reason_is_used() -> None:
    used = {text for tree in _sources().values() for text in _strings(tree)}
    reasons = [row[0] for row in load_exemptions("invariant")]

    assert [reason for reason in reasons if reason not in used] == []


def test_the_scan_sees_the_markers_it_counts() -> None:
    """Guard the scan: it reads the reason of a strict xfail marker.

    geoparquet-io 1.7.0 fixed gpio#1222, the marker this guard read. The guard
    now reads a module written here, so it does not depend on a marker in the
    tree.
    """
    tree = ast.parse(
        "import pytest\n"
        "@pytest.mark.xfail(strict=True, reason='https://example.org/issues/1: kept')\n"
        "def test_x() -> None: ...\n"
    )

    assert xfail_reasons(tree) == {"https://example.org/issues/1: kept"}
