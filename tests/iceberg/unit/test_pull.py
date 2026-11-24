"""``IcebergBackend.pull``: the assets of the current version, from ``{remote}/{href}``."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from tests.iceberg.fake_lake import Lake

pytestmark = pytest.mark.unit

REMOTE = "gs://test-bucket/catalog"


def test_pull_downloads_each_asset_from_the_remote_plus_its_href(
    lake: Lake, tmp_path: Path
) -> None:
    lake.publish()
    local_root = tmp_path / "local"

    with patch("portolan_cli.sync.download.download_file") as download:
        download.return_value = MagicMock(success=True, files_downloaded=1)
        result = lake.backend.pull(
            remote_url=f"{REMOTE}/", local_root=local_root, collection="roads"
        )

    assert result.success is True
    assert result.files_downloaded == 1
    assert result.remote_version == "1.0.0"
    assert download.call_args.kwargs["source"] == f"{REMOTE}/roads/roads.parquet"
    assert download.call_args.kwargs["destination"] == local_root / "roads" / "roads.parquet"


def test_pull_of_a_collection_without_a_table_is_up_to_date(lake: Lake, tmp_path: Path) -> None:
    result = lake.backend.pull(remote_url=REMOTE, local_root=tmp_path, collection="missing")

    assert result.success is True
    assert result.files_downloaded == 0
    assert result.up_to_date is True


def test_a_dry_run_downloads_nothing_and_counts_the_assets(lake: Lake, tmp_path: Path) -> None:
    lake.publish()

    with patch("portolan_cli.sync.download.download_file") as download:
        result = lake.backend.pull(
            remote_url=REMOTE, local_root=tmp_path, collection="roads", dry_run=True
        )

    download.assert_not_called()
    assert result.dry_run is True
    assert result.files_downloaded == 1


def test_a_failed_download_fails_the_pull(lake: Lake, tmp_path: Path) -> None:
    lake.publish()

    with patch("portolan_cli.sync.download.download_file") as download:
        download.return_value = MagicMock(success=False, files_downloaded=0)
        result = lake.backend.pull(remote_url=REMOTE, local_root=tmp_path, collection="roads")

    assert result.success is False
    assert result.files_downloaded == 0


def test_pull_without_a_collection_pulls_every_collection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#977: ``portolan pull <remote>`` with no ``--collection`` exits 0 on Iceberg.

    The catalog holds one collection with no table yet. The backend reads
    nothing from the catalog here: the version lookup is replaced, because
    the test is about the routing in core, not about the catalog.
    """
    import json

    from click.testing import CliRunner

    from portolan_cli.backends.iceberg.backend import IcebergBackend
    from portolan_cli.cli import cli

    def no_versions(self: IcebergBackend, collection: str) -> None:
        raise FileNotFoundError(f"No versions found for collection: {collection}")

    monkeypatch.setattr(IcebergBackend, "get_current_version", no_versions)
    monkeypatch.setenv("PORTOLAN_ICEBERG_URI", "http://127.0.0.1:9")
    root = tmp_path / "catalog"
    root.mkdir()
    monkeypatch.chdir(root)
    runner = CliRunner()
    init = runner.invoke(cli, ["init", "--backend", "iceberg", "--license", "CC0-1.0", "--auto"])
    assert init.exit_code == 0, init.output
    (root / "demo").mkdir()
    (root / "demo" / "collection.json").write_text(
        json.dumps({"type": "Collection", "id": "demo"}), encoding="utf-8"
    )

    result = runner.invoke(cli, ["pull", "s3://example-bucket/catalog"])

    assert result.exit_code == 0, result.output
    assert "No versions found for collection 'demo'" in result.output
