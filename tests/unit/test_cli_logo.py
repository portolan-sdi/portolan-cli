"""Tests for the catalog-logo CLI surface: `init --logo` and `portolan logo`.

The link shape itself is covered by ``tests/unit/test_logo.py``; these tests
cover the wiring, the JSON envelope, and the exit codes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

pytestmark = pytest.mark.unit


@pytest.fixture
def runner() -> CliRunner:
    return CliRunner()


def _png(directory: Path, name: str = "brand.png") -> Path:
    path = directory / name
    path.write_bytes(b"\x89PNG\r\n\x1a\n")
    return path


def _icon_link(catalog_json: Path) -> dict[str, str] | None:
    data = json.loads(catalog_json.read_text(encoding="utf-8"))
    for link in data["links"]:
        if isinstance(link, dict) and link.get("rel") == "icon":
            return link
    return None


class TestInitLogo:
    def test_init_with_logo_writes_link_and_copies_file(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        source = _png(tmp_path)
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--auto",
                    "--license",
                    "CC-BY-4.0",
                    "--logo",
                    str(source),
                    "--logo-title",
                    "Portolan SDI",
                ],
            )
            assert result.exit_code == 0, result.output
            root = Path(cwd)
            assert (root / "_assets" / "brand.png").exists()
            assert _icon_link(root / "catalog.json") == {
                "rel": "icon",
                "href": "./_assets/brand.png",
                "type": "image/png",
                "title": "Portolan SDI",
            }

    def test_init_without_logo_writes_no_icon_link(self, runner: CliRunner, tmp_path: Path) -> None:
        """The logo is a MAY, so a plain init must not invent one."""
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
            assert result.exit_code == 0, result.output
            assert _icon_link(Path(cwd) / "catalog.json") is None
            assert not (Path(cwd) / "_assets").exists()

    def test_unsupported_format_fails_before_creating_the_catalog(
        self, runner: CliRunner, tmp_path: Path
    ) -> None:
        source = tmp_path / "brand.bmp"
        source.write_bytes(b"BM")
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            result = runner.invoke(
                cli,
                ["init", "--auto", "--license", "CC-BY-4.0", "--logo", str(source)],
            )
            assert result.exit_code == 1
            assert not (Path(cwd) / "catalog.json").exists()
            assert not (Path(cwd) / ".portolan").exists()

    def test_json_mode_reports_the_error_type(self, runner: CliRunner, tmp_path: Path) -> None:
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(
                cli,
                [
                    "init",
                    "--auto",
                    "--json",
                    "--license",
                    "CC-BY-4.0",
                    "--logo",
                    "https://example.org/logo.png",
                ],
            )
            assert result.exit_code == 1
            payload = json.loads(result.output)
            assert payload["success"] is False
            assert payload["errors"][0]["type"] == "RemoteLogoSourceError"


class TestLogoCommand:
    def _init(self, runner: CliRunner) -> None:
        result = runner.invoke(cli, ["init", "--auto", "--license", "CC-BY-4.0"])
        assert result.exit_code == 0, result.output

    def test_adds_a_logo_to_an_existing_catalog(self, runner: CliRunner, tmp_path: Path) -> None:
        source = _png(tmp_path)
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            self._init(runner)
            result = runner.invoke(cli, ["logo", str(source), "--title", "Acme"])
            assert result.exit_code == 0, result.output
            assert _icon_link(Path(cwd) / "catalog.json") == {
                "rel": "icon",
                "href": "./_assets/brand.png",
                "type": "image/png",
                "title": "Acme",
            }

    def test_replacing_keeps_one_icon_link(self, runner: CliRunner, tmp_path: Path) -> None:
        first = _png(tmp_path, "one.png")
        second = tmp_path / "two.webp"
        second.write_bytes(b"RIFF")
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            self._init(runner)
            assert runner.invoke(cli, ["logo", str(first)]).exit_code == 0
            assert runner.invoke(cli, ["logo", str(second)]).exit_code == 0
            data = json.loads((Path(cwd) / "catalog.json").read_text(encoding="utf-8"))
            icons = [link for link in data["links"] if link.get("rel") == "icon"]
            assert len(icons) == 1
            assert icons[0]["href"] == "./_assets/two.webp"
            assert not (Path(cwd) / "_assets" / "one.png").exists()

    def test_json_envelope(self, runner: CliRunner, tmp_path: Path) -> None:
        source = _png(tmp_path)
        with runner.isolated_filesystem(temp_dir=tmp_path):
            self._init(runner)
            result = runner.invoke(cli, ["logo", str(source), "--json"])
            assert result.exit_code == 0, result.output
            payload = json.loads(result.output)
            assert payload["success"] is True
            assert payload["command"] == "logo"
            assert payload["data"]["href"] == "./_assets/brand.png"
            assert payload["data"]["type"] == "image/png"

    def test_svg_warns_but_succeeds(self, runner: CliRunner, tmp_path: Path) -> None:
        source = tmp_path / "mark.svg"
        source.write_text("<svg/>", encoding="utf-8")
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            self._init(runner)
            result = runner.invoke(cli, ["logo", str(source)])
            assert result.exit_code == 0, result.output
            assert "STAC Browser" in result.output
            link = _icon_link(Path(cwd) / "catalog.json")
            assert link is not None
            assert link["type"] == "image/svg+xml"

    def test_outside_a_catalog_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        source = _png(tmp_path)
        with runner.isolated_filesystem(temp_dir=tmp_path):
            result = runner.invoke(cli, ["logo", str(source)])
            assert result.exit_code == 1

    def test_unsupported_format_fails(self, runner: CliRunner, tmp_path: Path) -> None:
        source = tmp_path / "brand.tiff"
        source.write_bytes(b"II*")
        with runner.isolated_filesystem(temp_dir=tmp_path) as cwd:
            self._init(runner)
            result = runner.invoke(cli, ["logo", str(source), "--json"])
            assert result.exit_code == 1
            assert json.loads(result.output)["errors"][0]["type"] == "UnsupportedLogoFormatError"
            assert _icon_link(Path(cwd) / "catalog.json") is None
