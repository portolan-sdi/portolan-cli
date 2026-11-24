"""``add`` builds the backend before it writes anything.

A backend that cannot start used to fail at finalization, after
``collection.json`` was saved, and left a collection with a ``describedby``
link to a README that was never written. ``add`` is atomic: a failed add
leaves no item, no copied file, and no version entry.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = pytest.mark.unit

POINTS = Path(__file__).resolve().parents[1] / "fixtures" / "vector" / "valid" / "points.parquet"


def test_a_backend_that_cannot_start_fails_the_add_before_any_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("PORTOLAN_ICEBERG_URI", raising=False)
    root = tmp_path / "catalog"
    runner = CliRunner()
    assert runner.invoke(cli, ["init", str(root), "--auto", "--license", "CC0-1.0"]).exit_code == 0
    monkeypatch.chdir(root)
    assert runner.invoke(cli, ["config", "set", "backend", "iceberg"]).exit_code == 0
    (root / "roads").mkdir()
    source = shutil.copy(POINTS, root / "roads" / "roads.parquet")

    result = runner.invoke(cli, ["add", "--portolan-dir", str(root), str(source)])

    assert result.exit_code == 1, result.output
    assert "PORTOLAN_ICEBERG_URI" in result.output
    assert not (root / "roads" / "collection.json").exists()
    assert not (root / "roads" / "versions.json").exists()
    assert not (root / "roads" / "README.md").exists()
