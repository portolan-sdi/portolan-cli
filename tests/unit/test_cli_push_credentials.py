"""Unit tests for the credential warning of the push command.

`check_credentials` builds a hint that names every way to supply credentials.
The push command prints that hint before it contacts the store.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from pathlib import Path

TEST_REMOTE = "s3://test-bucket/catalog"


@pytest.fixture
def basic_catalog(tmp_path: Path) -> Path:
    """Create a catalog with one collection."""
    catalog_root = tmp_path / "catalog"
    catalog_root.mkdir()
    (catalog_root / "catalog.json").write_text('{"type": "Catalog", "id": "test"}')

    collection_dir = catalog_root / "test-collection"
    collection_dir.mkdir()
    (collection_dir / "collection.json").write_text(
        '{"type": "Collection", "id": "test-collection"}'
    )
    return catalog_root


def _run_push(
    catalog: Path, *, dry_run: bool = False, json_output: bool = False, **environment: str
) -> list[str]:
    """Run the push command and return the messages the warning path printed."""
    runner = CliRunner()
    command = ["push", "--collection", "test-collection", "--catalog", str(catalog)]
    if dry_run:
        command.append("--dry-run")
    if json_output:
        command.append("--json")

    with (
        patch("portolan_cli.sync.push.push_async", new_callable=AsyncMock) as mock_push,
        patch("portolan_cli.cli.warn") as mock_warn,
    ):
        mock_push.return_value = MagicMock(
            success=True, files_uploaded=0, versions_pushed=0, conflicts=[], errors=[]
        )
        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE, **environment}):
            runner.invoke(cli, command)

    return [call.args[0] for call in mock_warn.call_args_list]


class TestPushCredentialWarning:
    """Tests that push reports a missing credential source."""

    @pytest.mark.unit
    def test_push_warns_without_credentials(self, basic_catalog: Path) -> None:
        """A push with no credential source must print the hint."""
        warnings = _run_push(basic_catalog)

        assert any("portolan-cli[aws]" in message for message in warnings)
        assert any("aws_access_key_id" in message.lower() for message in warnings)

    @pytest.mark.unit
    def test_push_stays_quiet_with_credentials(self, basic_catalog: Path) -> None:
        """Environment keys must silence the hint."""
        warnings = _run_push(
            basic_catalog,
            AWS_ACCESS_KEY_ID="AKIAENVKEY",
            AWS_SECRET_ACCESS_KEY="envsecret",
        )

        assert not any("portolan-cli[aws]" in message for message in warnings)

    @pytest.mark.unit
    def test_dry_run_warns_too(self, basic_catalog: Path) -> None:
        """A dry run shows what the real push meets, so it reports the hint."""
        warnings = _run_push(basic_catalog, dry_run=True)

        assert any("portolan-cli[aws]" in message for message in warnings)

    @pytest.mark.unit
    def test_json_output_stays_clean(self, basic_catalog: Path) -> None:
        """`--json` carries a machine-readable envelope, so it holds no hint."""
        warnings = _run_push(basic_catalog, json_output=True)

        assert not any("portolan-cli[aws]" in message for message in warnings)
