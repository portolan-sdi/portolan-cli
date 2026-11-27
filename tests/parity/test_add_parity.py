"""Command parity for `add` and `add-external`.
Each library call also checks its result against stated values, so a dropped field cannot pass.
"""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from portolan_cli.add import AddOutcome, run_add
from portolan_cli.external import ExternalAddResult, add_external
from portolan_core.errors import MissingLicenseError

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.parity.harness import Parity

pytestmark = pytest.mark.integration

EXTERNAL_URL = "https://example.org/data/buildings.parquet"
PARQUET = "application/vnd.apache.parquet"
SOURCE = Path("points") / "points.geojson"


@pytest.fixture
def points_source(
    build_conformant_catalog: Callable[..., Path], valid_points_geojson: Path
) -> Callable[[Path], None]:
    """Build a catalog with one untracked GeoJSON file in the collection dir `points`."""

    def build(root: Path) -> None:
        build_conformant_catalog(root)
        (root / "points").mkdir()
        shutil.copy(valid_points_geojson, root / SOURCE)

    return build


def _read(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _add_and_check(root: Path) -> AddOutcome:
    """Add the points file, and check the result and the files it wrote."""
    outcome = run_add([(root / SOURCE).resolve()], root)

    assert outcome.to_dict() == {
        "added": [
            {
                "item_id": "points",
                "collection_id": "points",
                "format_type": "vector",
                "bbox": [-122.5, 37.7, -122.35, 37.85],
            }
        ],
        "skipped": [],
        "failures": [],
    }
    data = (root / "points" / "points.parquet").read_bytes()
    digest = hashlib.sha256(data).hexdigest()
    # A single vector file is a collection-level asset, with no item.
    assert not list((root / "points").glob("*/item.json"))
    asset = _read(root / "points" / "collection.json")["assets"]["points"]
    assert {k: asset[k] for k in ("href", "type", "roles", "file:size", "file:checksum")} == {
        "href": "./points.parquet",
        "type": PARQUET,
        "roles": ["data"],
        "file:size": len(data),
        "file:checksum": "1220" + digest,
    }
    versions = _read(root / "points" / "versions.json")
    assert versions["current_version"] == "1.0.0"
    record = versions["versions"][0]["assets"]["points.parquet"]
    assert (record["sha256"], record["size_bytes"]) == (digest, len(data))
    return outcome


def _add_external_and_check(root: Path) -> ExternalAddResult:
    """Register the remote file, and check the result and the collection it wrote."""
    result = add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0")

    assert result.to_dict() == {
        "collection_id": "buildings",
        "collection_path": str(root / "buildings" / "collection.json"),
        "href": EXTERNAL_URL,
        "media_type": PARQUET,
        "via": EXTERNAL_URL,
        "managed": False,
    }
    collection = _read(root / "buildings" / "collection.json")
    assert collection["assets"] == {
        "data": {
            "href": EXTERNAL_URL,
            "type": PARQUET,
            "title": "External data",
            "roles": ["data", "external"],
        }
    }
    links = collection["links"]
    assert {"rel": "via", "href": EXTERNAL_URL} in [
        {k: link[k] for k in ("rel", "href")} for link in links
    ]
    children = [
        link["href"] for link in _read(root / "catalog.json")["links"] if link["rel"] == "child"
    ]
    assert children == ["./buildings/collection.json"]
    return result


def test_add_file(parity: Parity, points_source: Callable[[Path], None]) -> None:
    """`add <file>` tracks the file and reports the new item."""
    parity("add", [str(SOURCE)], catalog=points_source, call=_add_and_check)


def test_add_unlicensed(parity: Parity, points_source: Callable[[Path], None]) -> None:
    """A catalog without a license fails both sides with the same error."""

    def unlicensed(root: Path) -> None:
        points_source(root)
        (root / ".portolan" / "metadata.yaml").unlink()

    parity.error(
        "add",
        [str(SOURCE)],
        catalog=unlicensed,
        call=lambda root: run_add([(root / SOURCE).resolve()], root),
        raises=MissingLicenseError,
    )


def test_add_external(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """`add-external <url>` registers the remote asset as a collection."""
    parity(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=build_conformant_catalog,
        call=_add_external_and_check,
    )


def test_add_external_exists(parity: Parity, build_conformant_catalog: Callable[..., Path]) -> None:
    """A collection that exists already fails both sides with the same error."""

    def with_collection(root: Path) -> None:
        build_conformant_catalog(root)
        add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0")

    parity.error(
        "add-external",
        [EXTERNAL_URL, "--license", "CC-BY-4.0"],
        catalog=with_collection,
        call=lambda root: add_external(catalog_root=root, url=EXTERNAL_URL, license="CC-BY-4.0"),
        raises=FileExistsError,
    )
