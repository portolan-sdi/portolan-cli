"""Checks for the guide docs/guides/python-writer.md (#1133, #928).

The guide takes all of its code from examples/python-writer/write_catalog.py
and examples/python-writer/build_item.py through pymdownx.snippets sections.
These checks run each section in guide order, validate the catalog that each
script writes, and check that each name the guide shows exists.
"""

from __future__ import annotations

import json
import posixpath
import re
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any

import obstore
import pytest
from click.testing import CliRunner

import portolan_core.writer as writer
from portolan_cli import cli

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GUIDE = PROJECT_ROOT / "docs" / "guides" / "python-writer.md"
EXAMPLE = PROJECT_ROOT / "examples" / "python-writer" / "write_catalog.py"
SECTIONS = ["plan", "inspect", "local", "store", "update"]
BUILDER = PROJECT_ROOT / "examples" / "python-writer" / "build_item.py"
BUILDER_SECTIONS = ["items", "collection", "write"]
SNIPPETS = [(EXAMPLE, s) for s in SECTIONS] + [(BUILDER, s) for s in BUILDER_SECTIONS]
NWI = PROJECT_ROOT / "tests" / "fixtures" / "realdata" / "nwi-wetlands.parquet"
_DIRECTIVE = re.compile(r'^--8<-- "(?P<path>[^":]+):(?P<section>[\w-]+)"$', re.MULTILINE)
_FENCE = re.compile(r"^```[^\n]*\n(?P<body>.*?)^```$", re.MULTILINE | re.DOTALL)


def test_each_snippet_section_exists_once() -> None:
    """A renamed or missing section marker in the example fails here."""
    directives = _DIRECTIVE.findall(GUIDE.read_text(encoding="utf-8"))

    assert [(PROJECT_ROOT / path, section) for path, section in directives] == SNIPPETS
    for path, section in SNIPPETS:
        source = path.read_text(encoding="utf-8")
        assert source.count(f"--8<-- [start:{section}]") == 1
        assert source.count(f"--8<-- [end:{section}]") == 1


def test_each_code_block_of_the_guide_is_a_tested_snippet() -> None:
    """A code block written inline in the guide would not run in CI, so it fails here."""
    bodies = [m["body"].strip() for m in _FENCE.finditer(GUIDE.read_text(encoding="utf-8"))]

    assert bodies == [
        f'--8<-- "{path.relative_to(PROJECT_ROOT).as_posix()}:{s}"' for path, s in SNIPPETS
    ]


def _section(name: str, example: Path = EXAMPLE) -> str:
    """The dedented code between the start and end markers of one section."""
    source = example.read_text(encoding="utf-8")
    body = source.split(f"--8<-- [start:{name}]\n", 1)[1].split("--8<-- [end:", 1)[0]
    return textwrap.dedent(body.rpartition("\n")[0])


def _run(names: list[str]) -> dict[str, Any]:
    namespace: dict[str, Any] = {}
    for name in names:
        exec(_section(name), namespace)  # noqa: S102 - the guide's own example code
    return namespace


def test_the_guide_snippets_run_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A reader who runs the shown snippets one after the other gets each result."""
    monkeypatch.chdir(tmp_path)

    namespace = _run(SECTIONS)

    assert capsys.readouterr().out.splitlines() == [
        "put parks/AGENTS.md",
        "put parks/README.md",
        "put parks/collection.json",
        "put AGENTS.md",
        "put README.md",
        "put catalog.json",
    ]
    store_keys = sorted(m["path"] for batch in obstore.list(namespace["store"]) for m in batch)
    assert store_keys == sorted(op.key for op in namespace["plan"].operations)
    assert sorted(p.name for p in (tmp_path / "city-catalog").iterdir()) == [
        "AGENTS.md",
        "README.md",
        "catalog.json",
        "parks",
    ]
    assert list((tmp_path / "city-catalog" / "parks").iterdir()) == []


def test_the_guide_tells_the_reader_to_supply_each_asset_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The writer writes only metadata, so the guide names each asset file to copy."""
    monkeypatch.chdir(tmp_path)
    namespace = _run(["plan", "inspect", "local"])
    guide = GUIDE.read_text(encoding="utf-8")

    for key, document in namespace["documents"].items():
        for asset in document.get("assets", {}).values():
            path = posixpath.normpath(posixpath.join(posixpath.dirname(key), asset["href"]))
            assert f"`{namespace['output'].as_posix()}/{path}`" in guide, path


def test_the_guide_names_only_public_writer_names() -> None:
    """Each writer name in backticks is in portolan_core.writer.__all__."""
    guide = GUIDE.read_text(encoding="utf-8")
    shown = {"plan_catalog", "apply_plan", "LocalSink", "ObstoreSink", "Sink"}

    for name in shown:
        assert f"`{name}`" in guide, name
        assert name in writer.__all__, name


def test_the_example_writes_a_catalog_the_validator_accepts(tmp_path: Path) -> None:
    """The example runs, and rashid reports no error on what it writes."""
    out = tmp_path / "city-catalog"
    subprocess.run([sys.executable, str(EXAMPLE)], cwd=tmp_path, capture_output=True, check=True)
    check = subprocess.run(
        [sys.executable, "-m", "rashid_core", "check", "--no-data", "--json", str(out)],
        capture_output=True,
        text=True,
        check=False,
    )
    findings = json.loads(check.stdout)["findings"]

    assert not (out / "parks" / "collection.json").exists()
    assert [f for f in findings if f["severity"] == "error"] == []
    assert check.returncode == 0, check.stderr


def _wetlands(folder: Path) -> None:
    """Two GeoParquet files in EPSG:5070, as the example reads them."""
    (folder / "wetlands").mkdir()
    for name in ("dc-2023.parquet", "dc-2024.parquet"):
        shutil.copy(NWI, folder / "wetlands" / name)


def test_the_builder_snippets_make_wgs84_items(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Items carry a WGS84 bbox, not the EPSG:5070 bbox of the file (#928)."""
    monkeypatch.chdir(tmp_path)
    _wetlands(tmp_path)
    namespace: dict[str, Any] = {}
    for name in BUILDER_SECTIONS:
        exec(_section(name, BUILDER), namespace)  # noqa: S102 - the guide's own example code

    item = json.loads(
        (tmp_path / "wetlands-catalog" / "wetlands" / "dc-2023" / "dc-2023.json").read_text()
    )
    assert item["properties"]["proj:code"] == "EPSG:5070"
    assert item["properties"]["proj:bbox"][0] == pytest.approx(1611616.7567)
    assert item["bbox"] == pytest.approx([-77.0933, 38.7501, -76.8747, 39.0001], abs=1e-4)
    assert item["assets"]["data"]["href"] == "./dc-2023.parquet"
    collection = json.loads(
        (tmp_path / "wetlands-catalog" / "wetlands" / "collection.json").read_text()
    )
    # A table field from one file would misstate the collection, so the example sets none.
    assert [key for key in collection if key.startswith("table:")] == []


def test_the_builder_example_writes_a_catalog_that_check_accepts(tmp_path: Path) -> None:
    """The script runs, and `portolan check` reports no error on what it writes (#928)."""
    _wetlands(tmp_path)
    subprocess.run([sys.executable, str(BUILDER)], cwd=tmp_path, capture_output=True, check=True)

    result = CliRunner().invoke(
        cli, ["check", "--no-data", "--json", str(tmp_path / "wetlands-catalog")]
    )
    data = json.loads(result.output)["data"]

    assert data["files_checked"] == 4
    assert [f for f in data["findings"] if f["severity"] == "error"] == []
    assert result.exit_code == 0, result.output
