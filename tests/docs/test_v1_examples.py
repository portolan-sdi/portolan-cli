"""Executable checks for the public v1 workflows."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
LOCAL_EXAMPLE = PROJECT_ROOT / "examples" / "local-publishing.sh"
STORAGE_EXAMPLE = PROJECT_ROOT / "examples" / "minio-round-trip.sh"
POINTS_FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "vector" / "valid" / "points.geojson"


def _example_environment(catalog_dir: Path) -> dict[str, str]:
    """Build the environment used by both executable examples."""
    environment = os.environ.copy()
    environment.update(
        {
            "CATALOG_DIR": str(catalog_dir),
            "PORTOLAN_EXAMPLE_SOURCE": str(POINTS_FIXTURE),
        }
    )
    return environment


@pytest.mark.integration
def test_local_publishing_example_creates_a_valid_catalog(tmp_path: Path) -> None:
    """The local publishing workflow creates catalog artifacts from the fixture."""
    catalog_dir = tmp_path / "local-catalog"

    result = subprocess.run(
        [str(LOCAL_EXAMPLE)],
        cwd=PROJECT_ROOT,
        env=_example_environment(catalog_dir),
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (catalog_dir / "catalog.json").is_file()
    assert (catalog_dir / "places" / "collection.json").is_file()
    assert (catalog_dir / "places" / "versions.json").is_file()


@pytest.mark.e2e
def test_minio_round_trip_example_clones_catalog_and_assets(tmp_path: Path) -> None:
    """The storage workflow pushes to MinIO and clones the resulting catalog."""
    if os.environ.get("PORTOLAN_DOCS_MINIO") != "1":
        pytest.skip("Documentation MinIO service is not running")

    catalog_dir = tmp_path / "source-catalog"
    cloned_dir = tmp_path / "cloned-catalog"
    environment = _example_environment(catalog_dir)
    environment["CLONED_CATALOG_DIR"] = str(cloned_dir)

    result = subprocess.run(
        [str(STORAGE_EXAMPLE)],
        cwd=PROJECT_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (cloned_dir / "catalog.json").is_file()
    assert (cloned_dir / "places" / "versions.json").is_file()
    assert (cloned_dir / "places" / "points.geojson").is_file()
