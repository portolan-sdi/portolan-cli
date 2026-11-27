"""Cap docstrings and comment blocks at 2 lines and ban history in them.

Usage: ``python scripts/check_comments.py FILE...``. The exit status is 1 on any finding.
"""

from __future__ import annotations

import ast
import os
import re
import sys
import textwrap
import tokenize
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CAP = 2
MONTHS = "january|february|march|april|may|june|july|august|september|october|november|december"
HISTORY = re.compile(
    rf"(?<![\w&/])#\d+\b|(?i:\b(used to|no longer|previously|formerly)\b)"
    rf"|(?i:\b(agent|lane|track))\s+([A-Z]\d*|\d+)\b|\b20\d\d-\d\d-\d\d\b|(?i:\b({MONTHS})\s+20\d\d\b)"
)
PRAGMA = re.compile(r"#!|#:|#\s*(noqa|type:|pragma|fmt:|pyright:|mypy:)")
SECTION = re.compile(r"(Args|Returns|Raises|Yields|Attributes):\s*$")
COVERED = re.compile(r"(^|/)(pyproject\.toml|prek\.toml|\.github/workflows/[^/]+\.yml|[^/]+\.py)$")
DEFS = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
# A synced file is edited in portolan-ops, so this repo cannot fix it.
SYNCED = "synced from portolan-sdi/portolan-ops"


def _hist(text: str) -> bool:
    return "://" not in text and bool(HISTORY.search(text))


def _comments(lines: list[tuple[int, str, bool]]) -> list[tuple[int, str]]:
    """Check ``(row, text, alone)`` comments for block length and history."""
    found, run, last = [], 0, -1
    for row, text, alone in lines:
        if _hist(text):
            found.append((row, "COM-HIST history in a comment"))
        if alone and not PRAGMA.match(text):
            run, last = (run + 1 if row == last + 1 else 1), row
            if run == CAP + 1:
                found.append((row, f"COM-LEN comment block over {CAP} lines"))
    return found


def _too_long(doc: str, rendered: bool) -> bool:
    """Rendered docstrings may add Google sections with one-line entries."""
    head, *rest = doc.splitlines()
    lines = [head.strip(), *textwrap.dedent("\n".join(rest)).splitlines()]
    cut = next((i for i, line in enumerate(lines) if SECTION.match(line)), len(lines))
    if sum(1 for line in lines[: cut if rendered else None] if line.strip()) > CAP:
        return True
    body = [len(s) - len(s.lstrip()) for s in lines[cut:] if s.strip() and not SECTION.match(s)]
    return rendered and any(indent > body[0] for indent in body)


def _is_click(node: ast.AST) -> bool:
    funcs = [d.func if isinstance(d, ast.Call) else d for d in getattr(node, "decorator_list", [])]
    return any(isinstance(f, ast.Attribute) and f.attr in ("command", "group") for f in funcs)


def _docstrings(tree: ast.Module, rel: str, prefixes: set[str]) -> list[tuple[int, str]]:
    found: list[tuple[int, str]] = []
    stack: list[tuple[ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef, str]]
    stack = [(tree, "")]
    while stack:
        node, qual = stack.pop()
        public = not any(p.startswith("_") and not p.startswith("__") for p in qual.split("."))
        shown = any(qual == p or (qual.startswith(f"{p}.") and public) for p in prefixes)
        doc = ast.get_docstring(node, clean=False)
        if doc is not None:
            start = node.body[0].lineno
            lines = enumerate(doc.splitlines())
            found += [(start + n, "DOC-HIST history in a docstring") for n, s in lines if _hist(s)]
            exempt = _is_click(node) or (rel.endswith("portolan_cli/api.py") and not qual)
            if not exempt and _too_long(doc, shown):
                found.append(
                    (start, f"DOC-LEN docstring over {CAP} lines or off the section format")
                )
        elif shown and qual and not qual.rsplit(".", 1)[-1].startswith("_"):
            found.append(
                (getattr(node, "lineno", 1), "DOC-MISSING rendered object has no docstring")
            )
        stack += [(c, f"{qual}.{c.name}".lstrip(".")) for c in node.body if isinstance(c, DEFS)]
    return found


def _top(body: list[ast.stmt]) -> list[ast.stmt]:
    nested = [_top(s.body + s.orelse) for s in body if isinstance(s, (ast.If, ast.Try))]
    return body + [stmt for block in nested for stmt in block]


def _binds(stmt: ast.stmt) -> set[str | None]:
    targets = [*getattr(stmt, "targets", []), getattr(stmt, "target", None)]
    return {getattr(stmt, "name", None), *(getattr(t, "id", None) for t in targets)}


def _resolve(root: Path, dirs: list[str], dotted: str, i: int = 1) -> tuple[str, str] | None:
    """Follow ``dotted`` through modules and ``from`` imports to ``(file, qualname)``."""
    parts = dotted.split(".")
    mod = ".".join(parts[:i])
    while True:
        stem = mod.replace(".", "/")
        cands = [root / d / s for d in dirs for s in (f"{stem}/__init__.py", f"{stem}.py")]
        path = next((c for c in cands if c.is_file()), None)
        if path is None:
            return None
        if i == len(parts):
            return path.relative_to(root).as_posix(), ""
        for stmt in _top(ast.parse(path.read_text(encoding="utf-8")).body):
            if parts[i] in _binds(stmt):
                return path.relative_to(root).as_posix(), ".".join(parts[i:])
            if not isinstance(stmt, ast.ImportFrom):
                continue
            for alias in stmt.names:
                if (alias.asname or alias.name) == parts[i]:
                    pkg = mod.split(".")[: None if path.name == "__init__.py" else -1]
                    base = pkg[: len(pkg) - stmt.level + 1] if stmt.level else []
                    src = ".".join([*base, *filter(None, [stmt.module])])
                    dotted = ".".join([src, alias.name, *parts[i + 1 :]])
                    return _resolve(root, dirs, dotted, len(src.split(".")))
        mod, i = f"{mod}.{parts[i]}", i + 1


def rendered_objects(root: Path) -> dict[str, set[str]]:
    """Map each file to the qualnames that the reference page renders."""
    paths = re.search(r"paths:\s*\[(.*)\]", (root / "mkdocs.yml").read_text(encoding="utf-8"))
    dirs = [d.strip() for d in paths.group(1).split(",")] if paths else ["."]
    page = (root / "docs/reference/python.md").read_text(encoding="utf-8")
    out: dict[str, set[str]] = {}
    for name in re.findall(r"^::: (\S+)", page, flags=re.MULTILINE):
        if hit := _resolve(root, dirs, name):
            out.setdefault(hit[0], set()).add(hit[1])
    return out


def check(path: Path, root: Path, rendered: dict[str, set[str]]) -> list[str]:
    """Return the findings for one file as ``path:line: CODE message`` lines."""
    rel = Path(os.path.relpath(path.resolve(), root.resolve())).as_posix()
    rows = path.read_text(encoding="utf-8").splitlines()
    if any(SYNCED in row for row in rows[:5]):
        return []
    if path.suffix != ".py":
        found = _comments(
            [(n, s.strip(), True) for n, s in enumerate(rows, 1) if s.lstrip()[:1] == "#"]
        )
    else:
        with path.open("rb") as handle:
            toks = [t for t in tokenize.tokenize(handle.readline) if t.type == tokenize.COMMENT]
        found = _comments(
            [(t.start[0], t.string, rows[t.start[0] - 1].lstrip()[:1] == "#") for t in toks]
        )
        found += _docstrings(ast.parse("\n".join(rows)), rel, rendered.get(rel, set()))
    return [f"{path}:{row}: {msg}" for row, msg in sorted(found)]


def main(argv: list[str] | None = None) -> int:
    """Check the given files and print one line per finding."""
    args = sys.argv[1:] if argv is None else argv
    files = [Path(a) for a in args if COVERED.search(Path(a).as_posix())]
    rendered = rendered_objects(ROOT) if files else {}
    findings = [line for f in files for line in check(f, ROOT, rendered)]
    if findings:
        print("\n".join(findings))
    return 1 if findings else 0


if __name__ == "__main__":
    sys.exit(main())
