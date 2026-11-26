"""Checks for the guide docs/guides/python-writer.md (#1133).

The guide takes its code from examples/python-writer/write_catalog.py through
pymdownx.snippets sections. These checks run that script, validate the
catalog it writes, and check that each name the guide shows exists.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

import portolan_core.writer as writer

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GUIDE = PROJECT_ROOT / "docs" / "guides" / "python-writer.md"
EXAMPLE = PROJECT_ROOT / "examples" / "python-writer" / "write_catalog.py"
_DIRECTIVE = re.compile(r'^--8<-- "(?P<path>[^":]+):(?P<section>[\w-]+)"$', re.MULTILINE)


def test_each_snippet_section_exists_once() -> None:
    """A renamed or missing section marker in the example fails here."""
    source = EXAMPLE.read_text(encoding="utf-8")
    directives = _DIRECTIVE.findall(GUIDE.read_text(encoding="utf-8"))

    assert [section for _path, section in directives] == ["plan", "apply", "loop"]
    for path, section in directives:
        assert PROJECT_ROOT / path == EXAMPLE
        assert source.count(f"--8<-- [start:{section}]") == 1
        assert source.count(f"--8<-- [end:{section}]") == 1


def _section(name: str) -> str:
    """The dedented code between the start and end markers of one section."""
    source = EXAMPLE.read_text(encoding="utf-8")
    body = source.split(f"--8<-- [start:{name}]\n", 1)[1].split("--8<-- [end:", 1)[0]
    return textwrap.dedent(body.rpartition("\n")[0])


def test_the_guide_snippets_run_in_order(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A reader who runs the shown snippets one after the other gets a catalog."""
    monkeypatch.chdir(tmp_path)
    namespace: dict[str, object] = {}

    for name in ("plan", "apply", "loop"):
        exec(_section(name), namespace)  # noqa: S102 - the guide's own example code

    assert (tmp_path / "my-catalog" / "catalog.json").is_file()
    assert capsys.readouterr().out.splitlines()[-1] == "PutOperation catalog.json"


def test_the_guide_names_only_public_writer_names() -> None:
    """Each writer name in backticks is in portolan_core.writer.__all__."""
    guide = GUIDE.read_text(encoding="utf-8")
    shown = {"plan_catalog", "apply_plan", "LocalSink", "ObstoreSink", "PutOperation"}
    shown |= {"DeleteOperation", "CatalogWriteError"}

    for name in shown:
        assert f"`{name}`" in guide, name
        assert name in writer.__all__, name


def test_the_example_writes_a_catalog_the_validator_accepts(tmp_path: Path) -> None:
    """The example runs, and rashid reports no error on what it writes."""
    out = tmp_path / "my-catalog"
    run = subprocess.run(
        [sys.executable, str(EXAMPLE)], cwd=tmp_path, capture_output=True, text=True, check=True
    )
    check = subprocess.run(
        [sys.executable, "-m", "rashid_core", "check", "--no-data", "--json", str(out)],
        capture_output=True,
        text=True,
        check=False,
    )
    findings = json.loads(check.stdout)["findings"]

    assert run.stdout.splitlines() == [
        "PutOperation parks/AGENTS.md",
        "PutOperation parks/README.md",
        "PutOperation parks/collection.json",
        "PutOperation AGENTS.md",
        "PutOperation README.md",
        "PutOperation catalog.json",
    ]
    assert [f for f in findings if f["severity"] == "error"] == []
    assert check.returncode == 0, check.stderr
