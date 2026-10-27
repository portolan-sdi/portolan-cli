"""Every Parquet and raster read in portolan_cli counts an open (#992).

The scale gates see a re-open only when the code that opens the file counts
it. pyarrow and rasterio have no choke point in our code, so each function
that opens a file calls ``count("parquet.opens")`` or ``count("raster.opens")``
itself. This test reads the source and fails on a function that opens a file
more times than it counts one.

It reads these calls: ``ParquetFile``, ``ParquetDataset``, ``read_metadata``,
``read_schema``, and ``read_table`` of ``pyarrow.parquet``, and
``rasterio.open`` in read mode. A write with ``rasterio.open(path, "w")``
opens no file to read.

``portolan_cli/backends/iceberg`` is not read. That package imports only
``portolan_cli.api`` and ``portolan_cli.hookspecs`` (the iceberg boundary
contract in pyproject.toml), so it cannot import ``trace``. No scenario runs
the Iceberg backend.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

pytestmark = pytest.mark.unit

PACKAGE = Path(__file__).resolve().parents[2] / "portolan_cli"
#: The package that cannot import trace, and so cannot count, under the package root.
SKIPPED = ("backends", "iceberg")
PARQUET_READS = frozenset(
    {"ParquetFile", "ParquetDataset", "read_metadata", "read_schema", "read_table"}
)


def _aliases(tree: ast.Module) -> tuple[set[str], set[str], set[str]]:
    """Return the names bound to pyarrow.parquet, to its read functions, and to rasterio."""
    parquet, reads, rasterio = set(), set(), set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "pyarrow.parquet" and alias.asname:
                    parquet.add(alias.asname)
                if alias.name == "rasterio":
                    rasterio.add(alias.asname or "rasterio")
        elif isinstance(node, ast.ImportFrom):
            for alias in node.names:
                if node.module == "pyarrow" and alias.name == "parquet":
                    parquet.add(alias.asname or "parquet")
                if node.module == "pyarrow.parquet" and alias.name in PARQUET_READS:
                    reads.add(alias.asname or alias.name)
    return parquet, reads, rasterio


def _read_mode(call: ast.Call) -> bool:
    modes = list(call.args[1:2])
    modes += [kw.value for kw in call.keywords if kw.arg == "mode"]
    return all(isinstance(mode, ast.Constant) and mode.value == "r" for mode in modes)


def _opened(call: ast.Call, aliases: tuple[set[str], set[str], set[str]]) -> str | None:
    """Return the counter that ``call`` must feed, or None when it opens no file to read."""
    parquet, reads, rasterio = aliases
    func = call.func
    if isinstance(func, ast.Name) and func.id in reads:
        return "parquet.opens"
    if not (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)):
        return None
    if func.value.id in parquet and func.attr in PARQUET_READS:
        return "parquet.opens"
    if func.value.id in rasterio and func.attr == "open" and _read_mode(call):
        return "raster.opens"
    return None


def _counted(call: ast.Call) -> str | None:
    func = call.func
    name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
    if name == "count" and call.args and isinstance(call.args[0], ast.Constant):
        return str(call.args[0].value)
    return None


def _own_calls(scope: ast.AST) -> list[ast.Call]:
    """The calls in ``scope``, without those of the functions nested in it."""
    calls: list[ast.Call] = []
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop()
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, ast.Call):
            calls.append(node)
        stack.extend(ast.iter_child_nodes(node))
    return calls


def uncounted_sites(package: Path = PACKAGE) -> list[str]:
    """Return ``path:function: counter opened N, counted M`` for each short function."""
    found: list[str] = []
    for path in sorted(package.rglob("*.py")):
        if path.is_relative_to(package.joinpath(*SKIPPED)):
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        aliases = _aliases(tree)
        scopes = [
            tree,
            *(
                n
                for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef | ast.AsyncFunctionDef | ast.Lambda)
            ),
        ]
        for scope in scopes:
            calls = _own_calls(scope)
            for counter in ("parquet.opens", "raster.opens"):
                opened = sum(_opened(call, aliases) == counter for call in calls)
                counted = sum(_counted(call) == counter for call in calls)
                if opened > counted:
                    name = getattr(scope, "name", "<module>" if scope is tree else "<lambda>")
                    rel = path.relative_to(package.parent).as_posix()
                    found.append(f"{rel}:{name}: {counter} opened {opened}, counted {counted}")
    return found


def test_every_open_site_counts() -> None:
    assert uncounted_sites() == []


def test_the_scan_finds_an_uncounted_read_and_passes_a_counted_one(tmp_path: Path) -> None:
    package = tmp_path / "portolan_cli"
    package.mkdir()
    (package / "reads.py").write_text(
        "import pyarrow.parquet as pq\n"
        "import rasterio\n"
        "from portolan_cli.trace import count\n\n"
        "def counted(path):\n"
        '    count("parquet.opens")\n'
        "    return pq.ParquetFile(path)\n\n"
        "def uncounted(path):\n"
        "    return pq.read_metadata(path), rasterio.open(path)\n\n"
        "def written(path):\n"
        '    return rasterio.open(path, "w")\n',
        encoding="utf-8",
    )

    assert uncounted_sites(package) == [
        "portolan_cli/reads.py:uncounted: parquet.opens opened 1, counted 0",
        "portolan_cli/reads.py:uncounted: raster.opens opened 1, counted 0",
    ]
