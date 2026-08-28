"""Behavioral checks for the Philadelphia housing publishing tutorial."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast

import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
README = PROJECT_ROOT / "README.md"
EXAMPLE_DIR = PROJECT_ROOT / "examples" / "philadelphia-housing"
EXAMPLE = EXAMPLE_DIR / "run.sh"
JOURNEY_STEPS = (
    EXAMPLE_DIR / "01-create-catalog.sh",
    EXAMPLE_DIR / "02-add-context.sh",
    EXAMPLE_DIR / "03-publish.sh",
)
QUERY = EXAMPLE_DIR / "query.py"
FIXTURE_SERVER = PROJECT_ROOT / "tests" / "docs" / "philadelphia_arcgis_server.py"
FIXTURE_DIR = PROJECT_ROOT / "tests" / "fixtures" / "realdata" / "philadelphia-housing"

EXPECTED_QUERY_OUTPUT = """\
district  projects  units
       1        37   2310
       2        62   1886
       3        97   3724
       4        36   1172
       5       119   4774
       6         9    264
       7        44   1915
       8        53   1796
       9         9    310
      10        10    433

Located projects: 476
Located units: 18,584
Projects without geometry: 25
Units without geometry: 665
"""


@contextmanager
def _arcgis_server(tmp_path: Path) -> Iterator[tuple[str, Path, subprocess.Popen[str]]]:
    """Run the deterministic ArcGIS replay used by the public workflow."""
    assert FIXTURE_SERVER.is_file(), f"Missing fixture server: {FIXTURE_SERVER}"
    assert (FIXTURE_DIR / "affordable_housing.parquet").is_file()
    assert (FIXTURE_DIR / "council_districts_2024.parquet").is_file()

    port_file = tmp_path / "arcgis-port.txt"
    request_log = tmp_path / "arcgis-requests.jsonl"
    process = subprocess.Popen(
        [
            sys.executable,
            str(FIXTURE_SERVER),
            "--fixture-dir",
            str(FIXTURE_DIR),
            "--port-file",
            str(port_file),
            "--request-log",
            str(request_log),
        ],
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        startup_deadline = time.monotonic() + 30
        while time.monotonic() < startup_deadline:
            if port_file.is_file():
                break
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                pytest.fail(f"ArcGIS fixture server failed:\n{stdout}{stderr}")
            time.sleep(0.1)
        else:
            process.terminate()
            stdout, stderr = process.communicate(timeout=5)
            pytest.fail(f"ArcGIS fixture server did not start:\n{stdout}{stderr}")

        port = port_file.read_text().strip()
        yield f"http://127.0.0.1:{port}/ArcGIS/rest/services", request_log, process
    finally:
        if process.poll() is None:
            process.terminate()
            process.wait(timeout=5)


def _example_environment(
    catalog_dir: Path,
    arcgis_url: str,
    remote: str | None = None,
) -> dict[str, str]:
    """Build the documented workflow environment."""
    environment = os.environ.copy()
    environment.update(
        {
            "CATALOG_DIR": str(catalog_dir),
            "PORTOLAN_PHL_ARCGIS_URL": arcgis_url,
        }
    )
    if remote is not None:
        environment["PORTOLAN_PHL_REMOTE"] = remote
    return environment


def _run_example(
    catalog_dir: Path,
    arcgis_url: str,
    remote: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Run the exact script embedded in the public documentation."""
    return subprocess.run(
        ["sh", str(EXAMPLE)],
        cwd=PROJECT_ROOT,
        env=_example_environment(catalog_dir, arcgis_url, remote),
        check=False,
        capture_output=True,
        text=True,
    )


def _run_query(catalog_url: str) -> subprocess.CompletedProcess[str]:
    """Run the exact analysis embedded in the public documentation."""
    return subprocess.run(
        [sys.executable, str(QUERY), catalog_url],
        cwd=PROJECT_ROOT,
        env=os.environ.copy(),
        check=False,
        capture_output=True,
        text=True,
    )


def _collection(catalog_dir: Path, collection_id: str) -> dict[str, Any]:
    """Read one generated STAC Collection."""
    value = json.loads((catalog_dir / collection_id / "collection.json").read_text())
    return cast(dict[str, Any], value)


def _requests(request_log: Path) -> list[dict[str, Any]]:
    """Read the ArcGIS fixture server request log."""
    return [cast(dict[str, Any], json.loads(line)) for line in request_log.read_text().splitlines()]


@pytest.mark.unit
def test_readme_describes_portolan_as_a_specification() -> None:
    """The README distinguishes the specification from this CLI implementation."""
    readme = README.read_text()

    assert "Portolan is an opinionated specification" in readme
    assert "This repository contains the Portolan CLI" in readme
    assert "Portolan standard" not in readme
    assert "end-to-end publishing example" in readme
    assert "more tutorials over time" in readme
    assert "CLI reference" in readme
    assert "Python API reference" in readme


@pytest.mark.unit
def test_tutorial_names_catalog_options() -> None:
    """The single tutorial points readers to supported variations."""
    tutorial = (EXAMPLE_DIR / "README.md").read_text()

    assert all(source in tutorial for source in ("ArcGIS REST", "WFS", "CARTO SQL API"))
    assert "SPDX identifier" in tutorial
    assert "`other`" in tutorial
    assert "`license_url`" in tutorial
    assert "`.portolan/metadata.yaml`" in tutorial
    assert "`.portolan/config.yaml`" in tutorial
    assert "subcatalog" in tutorial


@pytest.mark.unit
def test_query_installs_spatial_before_loading_it() -> None:
    """The documented query works when DuckDB has an empty extension cache."""
    source = QUERY.read_text()

    assert source.index('connection.execute("INSTALL spatial")') < source.index(
        'connection.execute("LOAD spatial")'
    )


@pytest.mark.integration
def test_example_builds_and_analyzes_a_philadelphia_catalog(tmp_path: Path) -> None:
    """The public workflow extracts, documents, validates, and analyzes two Collections."""
    catalog_dir = tmp_path / "philadelphia-housing"

    wrapper = EXAMPLE.read_text()
    for step in JOURNEY_STEPS:
        assert step.is_file()
        assert f'"$example_dir/{step.name}"' in wrapper

    tutorial = (EXAMPLE_DIR / "README.md").read_text()
    assert tutorial.index("## 3. Publish the catalog") < tutorial.index(
        "## 4. Use the published catalog"
    )
    assert "✓ Extracted 2/2 layers" in tutorial
    assert "✓ Added 2 files to 2 collections" in tutorial
    assert "✓ Pushed 2 collection(s), 4 version(s), 18 file(s)" in tutorial

    with _arcgis_server(tmp_path) as (arcgis_url, request_log, _process):
        result = _run_example(catalog_dir, arcgis_url)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "Extracted 2/2 layers" in result.stdout
    assert "catalog passes the Portolan check." in result.stdout

    catalog = json.loads((catalog_dir / "catalog.json").read_text())
    child_links = sorted(link["href"] for link in catalog["links"] if link["rel"] == "child")
    assert child_links == [
        "./affordablehousingproduction/collection.json",
        "./council_districts_2024/collection.json",
    ]

    expected = {
        "affordablehousingproduction": {
            "count": 501,
            "source": (
                "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/"
                "AffordableHousingProduction/FeatureServer/0"
            ),
        },
        "council_districts_2024": {
            "count": 10,
            "source": (
                "https://services.arcgis.com/fLeGjb7u4uXqeF9q/ArcGIS/rest/services/"
                "Council_Districts_2024/FeatureServer/0"
            ),
        },
    }
    for collection_id, contract in expected.items():
        collection = _collection(catalog_dir, collection_id)
        assert collection["table:row_count"] == contract["count"]
        assert collection["license"] == "other"
        assert {provider["name"]: provider["roles"] for provider in collection["providers"]} == {
            "City of Philadelphia": ["producer", "licensor"],
            "Portolan Documentation": ["processor", "host"],
        }
        assert contract["source"] in {
            link["href"] for link in collection["links"] if link["rel"] == "via"
        }

        assets = collection["assets"]
        assert any("style" in asset["roles"] for asset in assets.values())
        assert sum("thumbnail" in asset["roles"] for asset in assets.values()) == 1
        assert (catalog_dir / collection_id / "README.md").is_file()
        assert (catalog_dir / collection_id / "AGENTS.md").is_file()

        parquet_path = catalog_dir / collection_id / f"{collection_id}.parquet"
        parquet_file = pq.ParquetFile(parquet_path)
        assert "bbox" in parquet_file.schema_arrow.names
        bbox_columns = [
            index
            for index in range(parquet_file.metadata.num_columns)
            if parquet_file.schema.column(index).path.startswith("bbox.")
        ]
        assert len(bbox_columns) == 4
        assert all(
            parquet_file.metadata.row_group(0).column(index).statistics is not None
            for index in bbox_columns
        )

    requests = _requests(request_log)
    query_requests = [
        request
        for request in requests
        if str(request["path"]).endswith("/query")
        and request["query"].get("returnCountOnly") != ["true"]
    ]
    affordable_offsets = [
        int(request["query"]["resultOffset"][0])
        for request in query_requests
        if "AffordableHousingProduction" in str(request["path"])
    ]
    assert sorted(set(affordable_offsets)) == [0, 100, 200, 300, 400, 500]
    assert Counter(affordable_offsets)[200] == 2
    assert not any("Unrelated_Parks" in str(request["path"]) for request in requests)

    query = _run_query(str(catalog_dir))
    assert query.returncode == 0, query.stdout + query.stderr
    assert query.stdout == EXPECTED_QUERY_OUTPUT


@pytest.mark.e2e
def test_example_publishes_assets_that_remain_queryable_without_arcgis(
    tmp_path: Path,
) -> None:
    """CI queries the published Assets after the source service stops."""
    if os.environ.get("PORTOLAN_DOCS_MINIO") != "1":
        pytest.skip("Documentation MinIO service is not running")

    catalog_dir = tmp_path / "philadelphia-housing"
    remote = "s3://portolan-docs/philadelphia-housing"

    with _arcgis_server(tmp_path) as (arcgis_url, _request_log, process):
        result = _run_example(catalog_dir, arcgis_url, remote)
        assert result.returncode == 0, result.stdout + result.stderr
        process.terminate()
        process.wait(timeout=5)

    query = _run_query("http://localhost:9000/portolan-docs/philadelphia-housing")
    assert query.returncode == 0, query.stdout + query.stderr
    assert query.stdout == EXPECTED_QUERY_OUTPUT
