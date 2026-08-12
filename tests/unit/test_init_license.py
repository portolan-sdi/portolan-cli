"""Unit tests for the license ``portolan init`` acquires (issue #686).

``add`` refuses to write a collection with no license, so ``init`` collects one up
front and seeds it into ``.portolan/metadata.yaml``. The hierarchical merge then
hands it to every collection, and the gate in ``add_files`` passes without the
human editing anything.

Prompting is interactive-only. On ``--auto`` or ``--json`` there is nobody to ask,
so a missing ``--license`` is an error rather than a silent default, matching how
``extract`` handles a missing ArcGIS password.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.metadata_yaml import validate_metadata

pytestmark = pytest.mark.unit


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _metadata(root: Path) -> dict[str, object]:
    parsed = yaml.safe_load((root / ".portolan" / "metadata.yaml").read_text())
    assert isinstance(parsed, dict)
    return parsed


class TestInitRequiresALicense:
    def test_auto_without_a_license_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--auto"])

        assert result.exit_code == 1, result.output
        assert "PRTLN-VAL004" in result.output
        assert "--license" in result.output

    def test_json_without_a_license_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--json"])

        assert result.exit_code == 1
        payload = json.loads(result.output)
        assert payload["errors"][0]["type"] == "MissingLicenseError"
        assert payload["errors"][0]["code"] == "PRTLN-VAL004"

    def test_other_without_a_url_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--auto", "--license", "other"])

        assert result.exit_code == 1, result.output
        assert "license_url" in result.output

    def test_proprietary_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        """PTL-LIC-003 rejects it, so init will not write it in the first place."""
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--auto", "--license", "proprietary"])

        assert result.exit_code == 1, result.output
        assert "deprecated" in result.output

    def test_writes_nothing_when_it_refuses(self, runner: CliRunner, tmp_path: Path) -> None:
        """The license is validated before init touches the filesystem."""
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--auto"])

        assert result.exit_code == 1
        assert not (root / "catalog.json").exists()
        assert not (root / ".portolan").exists()


class TestInitSeedsTheLicense:
    def test_auto_with_a_license_succeeds(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--auto", "--license", "CC-BY-4.0"])

        assert result.exit_code == 0, result.output
        assert _metadata(root)["license"] == "CC-BY-4.0"

    def test_other_with_a_url_succeeds(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(
            cli,
            [
                "init",
                str(root),
                "--auto",
                "--license",
                "other",
                "--license-url",
                "https://x.org/terms",
            ],
        )

        assert result.exit_code == 0, result.output
        metadata = _metadata(root)
        assert metadata["license"] == "other"
        assert metadata["license_url"] == "https://x.org/terms"

    def test_the_seeded_metadata_carries_no_license_error(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """The seeded file satisfies the license half of metadata validation.

        Contact stays blank, which is what the two expected errors are. Asserting
        on them proves the validator ran and simply had nothing to say about the
        license, rather than the list being empty for some unrelated reason.

        Sorted because ``_validate_contact`` iterates a frozenset, so the two
        contact errors come back in either order depending on the process.
        """
        root = tmp_path / "catalog"
        init = runner.invoke(cli, ["init", str(root), "--auto", "--license", "MIT"])
        assert init.exit_code == 0, init.output

        errors = validate_metadata(_metadata(root))

        assert sorted(errors) == [
            "Field 'contact.email' cannot be empty",
            "Field 'contact.name' cannot be empty",
        ]

    def test_the_license_reaches_the_first_collection(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """The point of seeding it: add inherits the license and is not blocked."""
        root = tmp_path / "catalog"
        init = runner.invoke(cli, ["init", str(root), "--auto", "--license", "ODbL-1.0"])
        assert init.exit_code == 0, init.output

        fixture = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"
        source = root / "roads" / "roads.parquet"
        source.parent.mkdir(parents=True)
        source.write_bytes(fixture.read_bytes())

        add = runner.invoke(cli, ["add", "--portolan-dir", str(root), str(source)])

        assert add.exit_code == 0, add.output
        collection = json.loads((root / "roads" / "collection.json").read_text())
        assert collection["license"] == "ODbL-1.0"


class TestInitPrompts:
    def test_prompts_for_the_license_when_interactive(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root)], input="Demo\nA demo\nCC-BY-4.0\n")

        assert result.exit_code == 0, result.output
        assert "License" in result.output
        assert _metadata(root)["license"] == "CC-BY-4.0"

    def test_prompts_for_the_url_when_the_answer_is_other(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        """'other' alone is not conformant, so the prompt asks for the link too."""
        root = tmp_path / "catalog"

        result = runner.invoke(
            cli, ["init", str(root)], input="Demo\nA demo\nother\nhttps://x.org/terms\n"
        )

        assert result.exit_code == 0, result.output
        metadata = _metadata(root)
        assert metadata["license"] == "other"
        assert metadata["license_url"] == "https://x.org/terms"

    def test_does_not_ask_when_the_flag_is_given(self, runner: CliRunner, tmp_path: Path) -> None:
        root = tmp_path / "catalog"

        result = runner.invoke(cli, ["init", str(root), "--license", "MIT"], input="Demo\nA demo\n")

        assert result.exit_code == 0, result.output
        assert "License (SPDX" not in result.output
        assert _metadata(root)["license"] == "MIT"
