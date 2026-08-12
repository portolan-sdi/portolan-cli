"""Unit tests for the license gate on ``portolan add`` (issue #686).

Before this gate, an add with no license in metadata.yaml wrote a collection
carrying ``license: "other"`` and no ``rel="license"`` link, which is a PTL-LIC-002
ERROR. The gate runs after phase 1 and before any conversion, so a refused add
leaves no collection.json behind at all.

The catalog here is hand-built rather than created through ``portolan init``,
because ``init`` now seeds a license of its own and these cases need the state a
pre-gate catalog is in.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.constants import TODO_MARKER

pytestmark = pytest.mark.unit

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _catalog(root: Path) -> Path:
    """Hand-build a managed catalog with no metadata.yaml."""
    portolan_dir = root / ".portolan"
    portolan_dir.mkdir(parents=True)
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "demo",
                "description": "A Portolan-managed STAC catalog",
                "links": [],
            },
            indent=2,
        )
    )
    return root


def _write_metadata(root: Path, metadata: dict[str, object]) -> None:
    (root / ".portolan" / "metadata.yaml").write_text(yaml.dump(metadata, sort_keys=False))


def _source(root: Path, name: str = "roads.parquet", collection: str = "roads") -> Path:
    """Drop a real parquet fixture in a collection dir for add to read."""
    source = root / collection / name
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(FIXTURE.read_bytes())
    return source


def _add(runner: CliRunner, root: Path, source: Path) -> object:
    return runner.invoke(cli, ["add", "--portolan-dir", str(root), str(source)])


class TestAddRefusesAnUnlicensedCollection:
    def test_no_metadata_yaml_at_all(self, runner: CliRunner, tmp_path: Path) -> None:
        """The plainest form of the defect: nothing declares a license anywhere."""
        root = _catalog(tmp_path / "catalog")

        result = _add(runner, root, _source(root))

        assert result.exit_code == 1, result.output
        assert "PRTLN-VAL004" in result.output
        assert "no license is declared" in result.output

    def test_metadata_yaml_without_a_license_field(self, runner: CliRunner, tmp_path: Path) -> None:
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"contact": {"name": "GIS", "email": "gis@example.org"}})

        result = _add(runner, root, _source(root))

        assert result.exit_code == 1, result.output
        assert "PRTLN-VAL004" in result.output

    def test_other_without_a_license_url(self, runner: CliRunner, tmp_path: Path) -> None:
        """PTL-LIC-002 exactly: 'other' is only conformant beside a license link."""
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": "other"})

        result = _add(runner, root, _source(root))

        assert result.exit_code == 1, result.output
        assert "license_url" in result.output

    def test_the_seeded_todo_placeholder(self, runner: CliRunner, tmp_path: Path) -> None:
        """extract seeds this marker, and it used to reach collection.license verbatim."""
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": TODO_MARKER})

        result = _add(runner, root, _source(root))

        assert result.exit_code == 1, result.output
        assert "placeholder" in result.output

    def test_writes_no_collection_json_when_it_refuses(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """A refused add must not leave a non-conformant collection on disk."""
        root = _catalog(tmp_path / "catalog")

        result = _add(runner, root, _source(root))

        assert result.exit_code == 1
        assert list(root.rglob("collection.json")) == []

    def test_names_the_collection_and_the_remedy(self, runner: CliRunner, tmp_path: Path) -> None:
        """check must never report an issue without saying who resolves it."""
        root = _catalog(tmp_path / "catalog")

        result = _add(runner, root, _source(root, "roads.parquet"))

        assert "roads" in result.output
        assert ".portolan/metadata.yaml" in result.output
        assert "CC-BY-4.0" in result.output

    def test_reports_the_error_type_in_json_mode(self, runner: CliRunner, tmp_path: Path) -> None:
        """Agents parse the error class name out of the envelope."""
        root = _catalog(tmp_path / "catalog")

        result = runner.invoke(
            cli, ["add", "--portolan-dir", str(root), str(_source(root)), "--json"]
        )

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["success"] is False
        assert payload["errors"][0]["type"] == "MissingLicenseError"
        assert payload["errors"][0]["code"] == "PRTLN-VAL004"


class TestAddAcceptsALicensedCollection:
    def test_an_spdx_identifier(self, runner: CliRunner, tmp_path: Path) -> None:
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": "CC-BY-4.0"})

        result = _add(runner, root, _source(root))

        assert result.exit_code == 0, result.output
        collection = json.loads((root / "roads" / "collection.json").read_text())
        assert collection["license"] == "CC-BY-4.0"

    def test_other_with_a_license_url(self, runner: CliRunner, tmp_path: Path) -> None:
        """The second conformant shape, and the one extract seeds."""
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": "other", "license_url": "https://x.org/terms"})

        result = _add(runner, root, _source(root))

        assert result.exit_code == 0, result.output
        collection = json.loads((root / "roads" / "collection.json").read_text())
        assert collection["license"] == "other"
        assert any(link["rel"] == "license" for link in collection["links"])

    def test_a_license_already_on_the_collection(self, runner: CliRunner, tmp_path: Path) -> None:
        """A human who licensed collection.json by hand is not asked to repeat it."""
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": "CC-BY-4.0"})
        first = _add(runner, root, _source(root))
        assert first.exit_code == 0, first.output

        # Strip the license back out of metadata.yaml; the collection keeps its own.
        _write_metadata(root, {"contact": {"name": "GIS", "email": "gis@example.org"}})
        second = _add(runner, root, _source(root, "more.parquet"))

        assert second.exit_code == 0, second.output

    def test_a_license_link_already_on_the_collection_rescues_other(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """An existing rel=license link survives every rewrite, so it satisfies the gate."""
        root = _catalog(tmp_path / "catalog")
        _write_metadata(root, {"license": "other", "license_url": "https://x.org/terms"})
        first = _add(runner, root, _source(root))
        assert first.exit_code == 0, first.output

        _write_metadata(root, {"license": "other"})
        second = _add(runner, root, _source(root, "more.parquet"))

        assert second.exit_code == 0, second.output
