"""CLI wiring tests for `extract arcgis` auth flags, folder URLs, and coverage."""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli


@pytest.mark.unit
def test_extract_arcgis_has_auth_and_recurse_flags() -> None:
    runner = CliRunner()
    result = runner.invoke(cli, ["extract", "arcgis", "--help"])
    assert result.exit_code == 0
    for flag in ("--token", "--username", "--password", "--no-recurse"):
        assert flag in result.output


@pytest.mark.unit
def test_list_services_accepts_folder_url(monkeypatch) -> None:  # noqa: ANN001
    from portolan_cli.extract.arcgis.discovery import ServiceInfo
    from portolan_cli.extract.arcgis.orchestrator import ServicesRootDiscoveryResult
    from portolan_cli.extract.common.report import FolderCoverage

    def fake_list_services(url, *, service_filter=None, token=None, recurse=True, timeout=60.0):  # noqa: ANN001, ANN202
        return ServicesRootDiscoveryResult(
            services=[ServiceInfo("ecml/active_faults", "MapServer")],
            folders=["ecml"],
            base_url="https://x/server/rest/services",
            coverage=FolderCoverage(
                folders_visited=["ecml"],
                folders_skipped=[("Locked", "499")],
                services_found=1,
            ),
        )

    monkeypatch.setattr(
        "portolan_cli.extract.arcgis.orchestrator.list_services", fake_list_services
    )
    runner = CliRunner()
    result = runner.invoke(
        cli, ["extract", "arcgis", "https://x/server/rest/services/ecml", "--list-services"]
    )
    assert result.exit_code == 0
    assert "ecml/active_faults" in result.output
    assert "skipped" in result.output.lower()


@pytest.mark.unit
def test_list_services_threads_no_recurse_and_token(monkeypatch) -> None:  # noqa: ANN001
    from portolan_cli.extract.arcgis.discovery import ServiceInfo
    from portolan_cli.extract.arcgis.orchestrator import ServicesRootDiscoveryResult

    captured: dict[str, object] = {}

    def fake_list_services(url, *, service_filter=None, token=None, recurse=True, timeout=60.0):  # noqa: ANN001, ANN202
        captured["token"] = token
        captured["recurse"] = recurse
        return ServicesRootDiscoveryResult(
            services=[ServiceInfo("Top", "MapServer")], folders=[], base_url=url, coverage=None
        )

    monkeypatch.setattr("portolan_cli.extract.arcgis.orchestrator.list_services", fake_list_services)
    runner = CliRunner()
    result = runner.invoke(
        cli,
        ["extract", "arcgis", "https://x/server/rest/services", "--list-services", "--no-recurse", "--token", "TKN"],
    )
    assert result.exit_code == 0
    assert captured["token"] == "TKN"
    assert captured["recurse"] is False
