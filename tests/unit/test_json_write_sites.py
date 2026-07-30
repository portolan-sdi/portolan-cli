"""No module in ``portolan_cli`` may write JSON in place.

An in-place write truncates the destination before the new bytes land, so a
process killed mid-write leaves a zero-length ``collection.json`` where a valid
one used to be. :func:`portolan_cli.json_io.write_json_atomic` writes to a temp
file and ``os.replace``s it into position, which cannot truncate. Issue #682
introduced the helper, #687 finished adopting it, and this sweep keeps a raw
``json.dump(data, handle)`` or ``path.write_text(json.dumps(data))`` from
reappearing.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT / "portolan_cli"

# json_io.py is the one place allowed to serialize straight to a file handle:
# it owns the temp-file-then-replace dance the rest of the package delegates to.
EXEMPT = frozenset({PACKAGE_ROOT / "json_io.py"})


def _is_json_attr(node: ast.expr, name: str) -> bool:
    """True for ``json.dump``/``json.dumps`` (the qualified form we use)."""
    return (
        isinstance(node, ast.Attribute)
        and node.attr == name
        and isinstance(node.value, ast.Name)
        and node.value.id == "json"
    )


def _contains_json_dumps(node: ast.expr) -> bool:
    return any(
        isinstance(inner, ast.Call) and _is_json_attr(inner.func, "dumps")
        for inner in ast.walk(node)
    )


def in_place_json_writes(tree: ast.AST) -> list[int]:
    """Line numbers of writes that serialize JSON straight onto a destination.

    Two shapes: ``json.dump(data, handle)`` (a file object, so two-plus
    positional args) and ``dest.write_text(json.dumps(data))``. A bare
    ``json.dumps(data)`` that feeds a checksum or an upload body is fine — no
    file is truncated — so only these two forms count.
    """
    lines: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if _is_json_attr(node.func, "dump") and len(node.args) >= 2:
            lines.append(node.lineno)
        elif (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "write_text"
            and node.args
            and _contains_json_dumps(node.args[0])
        ):
            lines.append(node.lineno)
    return sorted(lines)


def _package_files() -> list[Path]:
    return sorted(p for p in PACKAGE_ROOT.rglob("*.py") if p not in EXEMPT)


class TestNoInPlaceJsonWrites:
    def test_every_json_write_goes_through_the_atomic_helper(self) -> None:
        offenders: list[str] = []
        for path in _package_files():
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            offenders.extend(
                f"{path.relative_to(REPO_ROOT)}:{line}" for line in in_place_json_writes(tree)
            )
        assert offenders == [], (
            "These writes truncate the destination before the new JSON lands. "
            "Use portolan_cli.json_io.write_json_atomic instead:\n" + "\n".join(offenders)
        )

    def test_the_sweep_reads_the_package_it_grades(self) -> None:
        """A sweep over an empty file list would pass forever."""
        files = _package_files()
        assert len(files) > 100
        assert PACKAGE_ROOT / "viz" / "pmtiles.py" in files

    def test_a_json_dump_to_a_handle_is_detected(self) -> None:
        source = 'with open(p, "w") as f:\n    json.dump(data, f, indent=2)\n'
        assert in_place_json_writes(ast.parse(source)) == [2]

    def test_a_write_text_of_json_dumps_is_detected(self) -> None:
        source = 'p.write_text(json.dumps(data, indent=2), encoding="utf-8")\n'
        assert in_place_json_writes(ast.parse(source)) == [1]

    def test_serializing_without_writing_a_file_is_allowed(self) -> None:
        """``json.dumps`` for a checksum or an upload body truncates nothing."""
        source = 'body = json.dumps(data, indent=2).encode("utf-8")\ndigest = json.dumps(crs)\n'
        assert in_place_json_writes(ast.parse(source)) == []
