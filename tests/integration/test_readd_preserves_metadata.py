"""Integration tests: a re-add preserves collection-specific metadata (issue #755).

``portolan add`` regenerates ``collection.json`` on every run. It used to overwrite
the collection title with the catalog root title, and it re-stamped the license and
providers from metadata merged down the tree. A maintainer who fixed a collection by
hand then lost the fix on the next add. These tests add a collection, edit it by hand,
add again, and confirm the hand edits survive.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"


def _init_catalog(root: Path) -> None:
    """Init a catalog whose root metadata.yaml declares a title and a license."""
    result = CliRunner().invoke(
        cli,
        ["init", str(root), "--auto", "--title", "Demo Catalog", "--license", "CC-BY-4.0"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    portolan_dir = root / ".portolan"
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump({"title": "Demo Catalog", "license": "CC-BY-4.0"}),
        encoding="utf-8",
    )


def _add_collection(root: Path) -> Path:
    collection_dir = root / "roads"
    collection_dir.mkdir(parents=True, exist_ok=True)
    dest = collection_dir / "roads.parquet"
    shutil.copy(FIXTURE, dest)
    result = CliRunner().invoke(
        cli,
        ["add", "--portolan-dir", str(root), str(dest)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output
    return collection_dir


def _readd(root: Path, dest: Path) -> None:
    result = CliRunner().invoke(
        cli,
        ["add", "--portolan-dir", str(root), "--force", str(dest)],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def test_readd_keeps_the_hand_edited_collection_title(tmp_path: Path) -> None:
    """The root title never overwrites a title a maintainer set on the collection."""
    root = tmp_path / "catalog"
    _init_catalog(root)
    collection_dir = _add_collection(root)
    collection_json = collection_dir / "collection.json"

    # A maintainer fixes the title by hand.
    data = json.loads(collection_json.read_text(encoding="utf-8"))
    data["title"] = "Municipal Road Network"
    collection_json.write_text(json.dumps(data), encoding="utf-8")

    _readd(root, collection_dir / "roads.parquet")

    reloaded = json.loads(collection_json.read_text(encoding="utf-8"))
    assert reloaded["title"] == "Municipal Road Network"


def test_first_add_does_not_inherit_the_root_title(tmp_path: Path) -> None:
    """A fresh collection keeps its humanized slug title, not the catalog title."""
    root = tmp_path / "catalog"
    _init_catalog(root)
    collection_dir = _add_collection(root)

    data = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
    assert data["title"] == "Roads"


def test_readd_keeps_the_hand_edited_license(tmp_path: Path) -> None:
    """An inherited license never overwrites a license a maintainer set by hand."""
    root = tmp_path / "catalog"
    _init_catalog(root)
    collection_dir = _add_collection(root)
    collection_json = collection_dir / "collection.json"

    data = json.loads(collection_json.read_text(encoding="utf-8"))
    data["license"] = "ODbL-1.0"
    collection_json.write_text(json.dumps(data), encoding="utf-8")

    _readd(root, collection_dir / "roads.parquet")

    reloaded = json.loads(collection_json.read_text(encoding="utf-8"))
    assert reloaded["license"] == "ODbL-1.0"
