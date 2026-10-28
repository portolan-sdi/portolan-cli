"""Every data file read in portolan_cli counts an open (#992).

The scale gates see a re-open only when the code that opens the file counts
it. pyarrow, rasterio, and OGR have no choke point in our code, so each
function that opens a file calls ``count()`` itself. This test reads the
source and fails on a function that opens a file more times than it counts
one. ``READERS`` names each reader and the counter it feeds:

- ``parquet.opens``: the readers of ``pyarrow.parquet``,
  ``pyarrow.dataset.dataset``, ``read_parquet`` of geopandas and pandas, the
  footer reader ``get_geo_metadata`` of geoparquet-io, and a
  DuckDB SQL string that reads Parquet (``read_parquet``, ``parquet_scan``,
  ``parquet_metadata``, ``parquet_schema``) passed to ``execute``, ``sql``, or
  ``query``.
- ``raster.opens``: ``rasterio.open`` in read mode. A write with
  ``rasterio.open(path, "w")`` opens no file to read.
- ``vector.opens``: the OGR readers (``geopandas.read_file``, the readers of
  pyogrio, ``fiona.open``) and ``geopandas.read_feather``.

``portolan_cli/backends/iceberg`` is not read. That package imports only
``portolan_cli.api`` and ``portolan_cli.hookspecs`` (the iceberg boundary
contract in pyproject.toml), so it cannot import ``trace``. No scenario runs
the Iceberg backend.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

# The scan reads portolan_cli, which mutmut rewrites in its sandbox.
pytestmark = [pytest.mark.unit, pytest.mark.source_scan]

PACKAGE = Path(__file__).resolve().parents[2] / "portolan_cli"
#: The package that cannot import trace, and so cannot count, under the package root.
SKIPPED = ("backends", "iceberg")

PARQUET, RASTER, VECTOR = "parquet.opens", "raster.opens", "vector.opens"
YAML = "config.yaml_parses"
#: ``(module, function)`` of each reader, and the counter it feeds.
READERS: dict[tuple[str, str], str] = {
    **{
        ("pyarrow.parquet", name): PARQUET
        for name in (
            "ParquetFile",
            "ParquetDataset",
            "read_metadata",
            "read_schema",
            "read_table",
            "read_pandas",
        )
    },
    ("pyarrow.dataset", "dataset"): PARQUET,
    # geoparquet-io reads the raw footer with pyarrow.
    ("geoparquet_io.core.duckdb_metadata", "get_geo_metadata"): PARQUET,
    ("geopandas", "read_parquet"): PARQUET,
    ("pandas", "read_parquet"): PARQUET,
    ("duckdb", "read_parquet"): PARQUET,
    ("duckdb", "from_parquet"): PARQUET,
    ("rasterio", "open"): RASTER,
    ("geopandas", "read_file"): VECTOR,
    ("geopandas", "read_feather"): VECTOR,
    **{
        ("pyogrio", name): VECTOR
        for name in ("read_dataframe", "read_info", "read_arrow", "open_arrow", "list_layers")
    },
    ("fiona", "open"): VECTOR,
    **{
        ("yaml", name): YAML
        for name in ("load", "safe_load", "full_load", "unsafe_load", "load_all", "safe_load_all")
    },
}
#: Table functions that make DuckDB read a Parquet file.
SQL_PARQUET = ("read_parquet", "parquet_scan", "parquet_metadata", "parquet_schema")
SQL_CALLS = frozenset({"execute", "sql", "query"})


def _aliases(tree: ast.Module) -> dict[str, str]:
    """Map each name that an import binds to the dotted path it names."""
    names: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.asname:
                    names[alias.asname] = alias.name
                else:
                    head = alias.name.split(".")[0]
                    names[head] = head
        elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
            for alias in node.names:
                names[alias.asname or alias.name] = f"{node.module}.{alias.name}"
    return names


def _dotted(node: ast.expr, aliases: dict[str, str]) -> str | None:
    """Return the dotted path that ``node`` names through the imports, such as ``rasterio.open``."""
    if isinstance(node, ast.Name):
        return aliases.get(node.id)
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value, aliases)
        return f"{base}.{node.attr}" if base else None
    if isinstance(node, ast.Call):
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "getattr"
            and len(node.args) == 2
            and isinstance(node.args[1], ast.Constant)
            and isinstance(node.args[1].value, str)
        ):
            base = _dotted(node.args[0], aliases)
            return f"{base}.{node.args[1].value}" if base else None
        if _dotted(node.func, aliases) == "functools.partial" and node.args:
            return _dotted(node.args[0], aliases)
    return None


def _scope_aliases(scope: ast.AST, aliases: dict[str, str]) -> dict[str, str]:
    """Resolve assigned readers without borrowing bindings from a nested function."""
    names = dict(aliases)
    stack = list(ast.iter_child_nodes(scope))
    while stack:
        node = stack.pop(0)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
            continue
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None:
            value = _dotted(node.value, names)
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if value:
                for target in targets:
                    if isinstance(target, ast.Name):
                        names[target.id] = value
        stack[0:0] = ast.iter_child_nodes(node)
    return names


def _read_mode(call: ast.Call) -> bool:
    modes = list(call.args[1:2])
    modes += [kw.value for kw in call.keywords if kw.arg == "mode"]
    # Only a known write-only mode can prove that the call reads no data.
    return not any(
        isinstance(mode, ast.Constant) and mode.value in ("w", "w+", "a", "x") for mode in modes
    )


def _sql_reads_parquet(call: ast.Call) -> bool:
    if not (isinstance(call.func, ast.Attribute) and call.func.attr in SQL_CALLS):
        return False
    texts = [
        node.value
        for arg in call.args[:1]
        for node in ast.walk(arg)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    return any(f"{name}(" in text.lower() for text in texts for name in SQL_PARQUET)


def _opened(call: ast.Call, aliases: dict[str, str]) -> str | None:
    """Return the counter that ``call`` must feed, or None when it opens no file to read."""
    if isinstance(call.func, ast.Attribute) and call.func.attr in ("read_parquet", "from_parquet"):
        # DuckDB connections expose readers as methods as well as SQL functions.
        return PARQUET
    if _sql_reads_parquet(call):
        return PARQUET
    path = _dotted(call.func, aliases)
    if path is None:
        return None
    module, _, name = path.rpartition(".")
    counter = READERS.get((module, name))
    if counter == RASTER and not _read_mode(call):
        return None
    return counter


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
        aliases = _scope_aliases(tree, _aliases(tree))
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
            names = _scope_aliases(scope, aliases)
            for counter in (PARQUET, RASTER, VECTOR, YAML):
                opened = sum(_opened(call, names) == counter for call in calls)
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
        "import geopandas as gpd\n"
        "import pyarrow.parquet as pq\n"
        "import rasterio\n"
        "from pyogrio import read_info\n"
        "from portolan_cli.trace import count\n\n"
        "def counted(path):\n"
        '    count("parquet.opens")\n'
        "    return pq.ParquetFile(path)\n\n"
        "def uncounted(path, con):\n"
        "    pq.read_metadata(path), rasterio.open(path), gpd.read_parquet(path)\n"
        "    read_info(path), gpd.read_file(path)\n"
        "    return con.execute(f\"SELECT * FROM read_parquet('{path}')\")\n\n"
        "def written(path, con):\n"
        '    con.execute("SELECT 1")\n'
        '    return rasterio.open(path, "w")\n',
        encoding="utf-8",
    )

    assert uncounted_sites(package) == [
        "portolan_cli/reads.py:uncounted: parquet.opens opened 3, counted 0",
        "portolan_cli/reads.py:uncounted: raster.opens opened 1, counted 0",
        "portolan_cli/reads.py:uncounted: vector.opens opened 2, counted 0",
    ]


@pytest.mark.parametrize(
    ("source", "counter"),
    [
        ("reader = pq.ParquetFile\n    reader(path)", PARQUET),
        ('getattr(pq, "ParquetFile")(path)', PARQUET),
        ('reader = functools.partial(pq.read_table, columns=["x"])\n    reader(path)', PARQUET),
        ('rasterio.open(path, "r+")', RASTER),
        ("rasterio.open(path, mode=mode)", RASTER),
        ("yaml.safe_load(path.read_text())", "config.yaml_parses"),
        ("duckdb.read_parquet(path).fetchall()", PARQUET),
        ("con.read_parquet(path).fetchall()", PARQUET),
    ],
    ids=[
        "assigned-reader",
        "getattr-reader",
        "partial-reader",
        "update-mode",
        "dynamic-mode",
        "yaml-helper",
        "duckdb-reader",
        "connection-reader",
    ],
)
def test_indirect_reads_cannot_escape_the_gate(tmp_path: Path, source: str, counter: str) -> None:
    """New helper modules must count reads even when the reader is indirect."""
    package = tmp_path / "portolan_cli"
    package.mkdir()
    (package / "helper.py").write_text(
        "import functools\nimport pyarrow.parquet as pq\nimport rasterio\nimport yaml\nimport duckdb\n"
        f"def helper(path, con=None, mode='r'):\n    {source}\n",
        encoding="utf-8",
    )
    assert uncounted_sites(package) == [
        f"portolan_cli/helper.py:helper: {counter} opened 1, counted 0"
    ]


@pytest.mark.integration
def test_delegated_footer_reads_are_counted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live rewrite counts footer reads inside geoparquet-io as well as core."""
    import pyarrow.parquet as pq

    from portolan_cli.formats import in_run
    from portolan_cli.preparation import convert_vector
    from portolan_cli.run_state import RunState
    from portolan_cli.trace import counters, tracing
    from tests.perf.runner import _install_parquet_counter

    source = tmp_path / "points.parquet"
    fixture = Path(__file__).resolve().parents[1] / "fixtures/vector/valid/points.parquet"
    source.write_bytes(fixture.read_bytes())
    actual: list[object] = []
    original = pq.ParquetFile.__init__

    def opened(self: object, *args: object, **kwargs: object) -> None:
        actual.append(args[0])
        original(self, *args, **kwargs)

    monkeypatch.setattr(pq.ParquetFile, "__init__", opened)
    _install_parquet_counter(monkeypatch)
    with tracing(), in_run(RunState.open(tmp_path)):
        convert_vector(source, tmp_path)
        reported = counters()[PARQUET]

    assert len(actual) > 2  # The fixture must reach delegated readers.
    assert reported == len(actual)
