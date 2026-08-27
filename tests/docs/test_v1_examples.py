"""Behavioral checks for the public end-to-end publishing example."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = PROJECT_ROOT / "examples" / "publish-catalog" / "run.sh"
POINTS_FIXTURE = PROJECT_ROOT / "examples" / "publish-catalog" / "points.parquet"


def _example_environment(catalog_dir: Path, cloned_dir: Path | None = None) -> dict[str, str]:
    """Build the documented workflow environment."""
    environment = os.environ.copy()
    environment.update(
        {
            "CATALOG_DIR": str(catalog_dir),
            "PORTOLAN_EXAMPLE_SOURCE": str(POINTS_FIXTURE),
        }
    )
    if cloned_dir is not None:
        environment.update(
            {
                "CLONED_CATALOG_DIR": str(cloned_dir),
                "PORTOLAN_EXAMPLE_REMOTE": "s3://portolan-docs/catalog",
            }
        )
    return environment


def _run_example(
    catalog_dir: Path, cloned_dir: Path | None = None
) -> subprocess.CompletedProcess[str]:
    """Run the exact script embedded in the public documentation."""
    return subprocess.run(
        [str(EXAMPLE)],
        cwd=PROJECT_ROOT,
        env=_example_environment(catalog_dir, cloned_dir),
        check=False,
        capture_output=True,
        text=True,
    )


def _sha256(path: Path) -> str:
    """Return the hexadecimal SHA-256 digest for one example asset."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.mark.integration
def test_example_builds_a_conformant_catalog(tmp_path: Path) -> None:
    """The documented local workflow produces a complete, valid Collection."""
    catalog_dir = tmp_path / "source-catalog"

    result = _run_example(catalog_dir)

    assert result.returncode == 0, result.stdout + result.stderr
    collection = json.loads((catalog_dir / "places" / "collection.json").read_text())
    versions = json.loads((catalog_dir / "places" / "versions.json").read_text())

    data_assets = [asset for asset in collection["assets"].values() if "data" in asset["roles"]]
    assert [asset["href"] for asset in data_assets] == ["./points.parquet"]
    assert data_assets[0]["type"] == "application/vnd.apache.parquet"

    thumbnail_assets = [
        asset for asset in collection["assets"].values() if "thumbnail" in asset["roles"]
    ]
    assert len(thumbnail_assets) == 1
    assert (catalog_dir / "places" / thumbnail_assets[0]["href"]).is_file()

    providers = {provider["name"]: provider["roles"] for provider in collection["providers"]}
    assert providers == {
        "Portolan Documentation": ["host"],
        "Portolan Project": ["producer", "licensor"],
    }
    assert collection["license"] == "CC-BY-4.0"
    assert any(link["rel"] == "via" for link in collection["links"])

    tracked_assets = versions["versions"][-1]["assets"]
    assert "points.parquet" in tracked_assets
    assert any(name.endswith(".thumb.jpg") for name in tracked_assets)
    assert "Catalog passes the Portolan check." in result.stdout


@pytest.mark.e2e
def test_example_pushes_and_clones_the_same_catalog(tmp_path: Path) -> None:
    """CI publishes the documented workflow to MinIO and verifies the clone."""
    if os.environ.get("PORTOLAN_DOCS_MINIO") != "1":
        pytest.skip("Documentation MinIO service is not running")

    catalog_dir = tmp_path / "source-catalog"
    cloned_dir = tmp_path / "cloned-catalog"

    result = _run_example(catalog_dir, cloned_dir)

    assert result.returncode == 0, result.stdout + result.stderr
    source_asset = catalog_dir / "places" / "points.parquet"
    cloned_asset = cloned_dir / "places" / "points.parquet"
    assert _sha256(cloned_asset) == _sha256(source_asset)

    check = subprocess.run(
        ["portolan", "check", str(cloned_dir), "--no-data", "--strict"],
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )
    assert check.returncode == 0, check.stdout + check.stderr
