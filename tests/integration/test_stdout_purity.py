"""In JSON mode, stdout holds exactly one JSON document (#1005).

An agent pipes ``portolan --format json <command>`` into a JSON parser. A
notice on stdout before or after the envelope breaks the parse. The add JSON
tests once patched ``add_files``, so the notices that ``add`` prints never
reached them (#1005).

Each case here runs the real command through ``run_cli`` on a catalog that
``init`` and ``add`` build. Nothing in this module patches the code under
test. ``test_the_module_patches_nothing`` enforces that. The moto server
stands in for object storage, and a replay server stands in for ArcGIS.

click 8.4 keeps stdout and stderr apart in ``result.stdout``, so a log line
on stderr does not fail a case.
"""

from __future__ import annotations

import ast
import json
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest
import yaml

if TYPE_CHECKING:
    from tests.catalog_invariants import RunCli

pytestmark = [pytest.mark.integration]

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
POINTS = FIXTURES / "vector" / "valid" / "points.parquet"
PHILADELPHIA = FIXTURES / "realdata" / "philadelphia-housing"

# A real 1x1 transparent PNG, because `logo` reads the image bytes.
LOGO_PNG = bytes.fromhex(
    "89504e470d0a1a0a0000000d4948445200000001000000010806000000"
    "1f15c4890000000a49444154789c630001000005000100"
    "0d0a2db40000000049454e44ae426082"
)


@dataclass(frozen=True)
class Case:
    """One command in JSON mode, and the exit code it must return.

    ``args`` may hold the placeholders ``{root}`` (the catalog), ``{tmp}``
    (the test's temporary directory), and ``{remote}`` (an S3 URL on the
    moto server). ``needs`` names a precondition.
    """

    args: tuple[str, ...]
    expect: int = 0
    needs: frozenset[str] = frozenset()


REMOTE = frozenset({"remote"})
CLONE_REASON = "stdout_purity: clone writes no collection.json (#1042)"
PUSHED = frozenset({"remote", "pushed"})

CASES: dict[str, Case] = {
    "init": Case(("init", "{tmp}/fresh", "--auto", "--license", "CC-BY-4.0")),
    "list": Case(("list",)),
    "status": Case(("status", "--offline")),
    "info": Case(("info", "roads")),
    "check": Case(("check",)),
    "scan": Case(("scan", ".")),
    "add": Case(("add", "places/places.parquet"), needs=frozenset({"new-file"})),
    "add-external": Case(
        (
            "add-external",
            "https://data.example.org/places.parquet",
            "--collection",
            "external",
            "--license",
            "CC-BY-4.0",
            "--bbox",
            "-122.5,37.7,-122.35,37.85",
        )
    ),
    "rm": Case(("rm", "--dry-run", "roads/roads.parquet")),
    "push": Case(("push", "{remote}"), needs=REMOTE),
    "pull": Case(("pull", "{remote}"), needs=PUSHED),
    "sync": Case(("sync", "{remote}", "--collection", "roads"), needs=PUSHED),
    # clone writes no collection.json (#1042), so the clone is registered as a
    # damaged catalog and the run_cli teardown skips it.
    "clone": Case(
        ("clone", "{remote}", "{tmp}/cloned"), needs=frozenset({"remote", "pushed", "clone"})
    ),
    "clean": Case(("clean", "--dry-run")),
    "readme": Case(("readme",)),
    "logo": Case(("logo", "{tmp}/brand.png"), needs=frozenset({"logo"})),
    "partition": Case(("partition", "roads/roads.parquet", "--preview")),
    # A collection-level asset makes no item, so there is nothing to mirror.
    "stac-geoparquet": Case(("stac-geoparquet", "--dry-run"), expect=1),
    "config-get": Case(("config", "get", "backend")),
    "config-list": Case(("config", "list")),
    "config-set": Case(("config", "set", "statistics.enabled", "false")),
    "config-unset": Case(("config", "unset", "statistics.enabled"), needs=frozenset({"setting"})),
    "metadata-init": Case(("metadata", "init", "roads")),
    "metadata-validate": Case(("metadata", "validate")),
    "version-list": Case(("version", "list", "roads")),
    "version-current": Case(("version", "current", "roads")),
    "version-bump": Case(("version", "bump", "roads", "2.0.0", "--yes")),
    "skills-list": Case(("skills", "list")),
    "skills-show": Case(("skills", "show", "portolan-cli")),
}

# The JSON-mode cases on main whose stdout is not one document.
# #1005 names the library modules that print through output.py: the rewrite
# notice in add, and the progress lines in sync/push.py, sync/pull.py, and
# extract/common/source.py.
XFAIL: dict[str, str] = {
    "add": "#1005: add prints the GeoParquet rewrite notice on stdout before the envelope",
    "push": "#1005: push prints its progress lines on stdout before the envelope",
    "pull": "#1005: pull prints its progress lines on stdout before the envelope",
    "sync": "#1005: sync prints its progress lines on stdout before the envelope",
    "clone": "#1005: clone prints its progress lines on stdout before the envelope",
}


def _params() -> list[object]:
    return [
        pytest.param(
            name,
            id=name,
            marks=([pytest.mark.xfail(strict=True, reason=XFAIL[name])] if name in XFAIL else []),
        )
        for name in CASES
    ]


def assert_one_json_document(stdout: str) -> Any:
    """Parse ``stdout`` and fail unless it holds exactly one JSON document."""
    text = stdout.strip()
    try:
        document, end = json.JSONDecoder().raw_decode(text)
    except json.JSONDecodeError as exc:
        pytest.fail(f"stdout does not start with a JSON document ({exc}):\n{stdout}")
    assert end == len(text), f"stdout holds more than one document:\n{stdout}"
    assert json.loads(stdout) == document
    return document


# The metadata.yaml that makes the catalog pass `check` and `metadata validate`.
METADATA = {
    "license": "CC-BY-4.0",
    "contact": {"name": "Data Team", "email": "data@example.org"},
    "source": "Example municipal open-data portal",
    "providers": [
        {
            "name": "Example Statistics Agency",
            "roles": ["producer", "licensor", "host"],
            "url": "https://stats.example.org",
        }
    ],
}


@pytest.fixture
def catalog(tmp_path: Path, run_cli: RunCli) -> Path:
    """A conformant catalog with one collection, built by ``init`` and ``add``."""
    root = tmp_path / "catalog"
    (root / "roads").mkdir(parents=True)
    (root / ".portolan").mkdir()
    (root / ".portolan" / "metadata.yaml").write_text(yaml.dump(METADATA), encoding="utf-8")
    shutil.copy(POINTS, root / "roads" / "roads.parquet")
    run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
    run_cli("add", "--portolan-dir", str(root), str(root / "roads" / "roads.parquet"))
    return root


def _prepare(
    case: Case, root: Path, tmp_path: Path, run_cli: RunCli, request: pytest.FixtureRequest
) -> dict[str, str]:
    """Meet the case's preconditions and return the placeholder values."""
    values = {"root": str(root), "tmp": str(tmp_path), "remote": ""}
    if "remote" in case.needs:
        bucket, _ = request.getfixturevalue("s3_bucket")
        request.getfixturevalue("_aws_env")
        values["remote"] = f"s3://{bucket}/catalog"
    if "pushed" in case.needs:
        run_cli("push", values["remote"], cwd=root)
    if "new-file" in case.needs:
        (root / "places").mkdir()
        shutil.copy(POINTS, root / "places" / "places.parquet")
    if "clone" in case.needs:
        request.getfixturevalue("build_damaged_catalog")(tmp_path / "cloned", CLONE_REASON)
    if "logo" in case.needs:
        (tmp_path / "brand.png").write_bytes(LOGO_PNG)
    if "setting" in case.needs:
        run_cli("config", "set", "statistics.enabled", "false", cwd=root)
    return values


@pytest.mark.parametrize("name", _params())
def test_json_mode_writes_one_document(
    name: str,
    catalog: Path,
    tmp_path: Path,
    run_cli: RunCli,
    request: pytest.FixtureRequest,
) -> None:
    case = CASES[name]
    values = _prepare(case, catalog, tmp_path, run_cli, request)
    args = [arg.format(**values) for arg in case.args]

    result = run_cli("--format", "json", *args, cwd=catalog, expect=None)

    assert_one_json_document(result.stdout)
    assert result.exit_code == case.expect, result.output


@pytest.mark.realdata
@pytest.mark.xfail(
    strict=True,
    reason="#1005: extract arcgis prints the metadata.yaml seed notice on stdout",
)
def test_extract_arcgis_writes_one_document(
    arcgis_url: str, tmp_path: Path, run_cli: RunCli
) -> None:
    result = run_cli(
        "--format",
        "json",
        "extract",
        "arcgis",
        arcgis_url,
        str(tmp_path / "extracted"),
        "--auto",
        "--license",
        "CC-BY-4.0",
        expect=None,
    )

    assert_one_json_document(result.stdout)
    assert result.exit_code == 0, result.output


@pytest.fixture
def iceberg_catalog(tmp_path: Path, run_cli: RunCli) -> Path:
    """A catalog on the Iceberg backend with one collection at version 1.0.0.

    Its iceberg:catalog_uri holds a local path. The run_cli teardown drops
    that code as the known gap #939.
    """
    pytest.importorskip("pyiceberg")
    root = tmp_path / "lake"
    (root / "roads").mkdir(parents=True)
    shutil.copy(POINTS, root / "roads" / "roads.parquet")
    run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
    run_cli("config", "set", "backend", "iceberg", cwd=root)
    run_cli("add", "--portolan-dir", str(root), str(root / "roads" / "roads.parquet"))
    return root


@pytest.mark.parametrize(
    ("args", "expect"),
    [
        pytest.param(("version", "rollback", "roads", "1.0.0", "--json"), 0, id="rollback"),
        pytest.param(("version", "prune", "roads", "--dry-run", "--json"), 0, id="prune"),
    ],
)
@pytest.mark.skipif(
    sys.platform == "win32", reason="PyIceberg SQL catalog warehouse paths break on Windows"
)
def test_iceberg_commands_write_one_document(
    args: tuple[str, ...], expect: int, iceberg_catalog: Path, run_cli: RunCli
) -> None:
    result = run_cli(*args, cwd=iceberg_catalog, expect=None)

    assert_one_json_document(result.stdout)
    assert result.exit_code == expect, result.output


def test_the_module_patches_nothing() -> None:
    """No call in this module patches, so every case runs the real code."""
    tree = ast.parse(Path(__file__).read_text(encoding="utf-8"))
    patches = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and (
            (isinstance(node.func, ast.Name) and node.func.id == "patch")
            or (isinstance(node.func, ast.Attribute) and node.func.attr in {"patch", "setattr"})
        )
    ]

    assert patches == []


VECTOR = FIXTURES / "vector"


def _add_catalog(tmp_path: Path, run_cli: RunCli, files: dict[str, Path]) -> Path:
    """Run ``init`` and copy ``files`` (relative path -> source) into the catalog."""
    root = tmp_path / "catalog"
    for relative, source in files.items():
        (root / relative).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(source, root / relative)
    run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")
    return root


class TestAddEnvelope:
    """The ``add`` envelope from the real ``add_files``, with no mock of it.

    These replace tests that patched ``portolan_cli.cli.add_files`` and so
    never saw what ``add`` prints (#1005).
    """

    def test_add_dot_at_the_catalog_root(self, tmp_path: Path, run_cli: RunCli) -> None:
        """``add .`` at the root infers the collection from the file's directory."""
        root = _add_catalog(
            tmp_path, run_cli, {"col/data.geojson": VECTOR / "valid" / "points.geojson"}
        )

        result = run_cli("--format", "json", "add", "--portolan-dir", str(root), str(root))

        envelope = assert_one_json_document(result.stdout)
        assert envelope["success"] is True
        assert envelope["command"] == "add"
        assert [item["collection_id"] for item in envelope["data"]["added"]] == ["col"]

    def test_a_failure_is_listed_beside_the_item_that_landed(
        self, tmp_path: Path, run_cli: RunCli
    ) -> None:
        root = _add_catalog(
            tmp_path,
            run_cli,
            {
                "collection/points.geojson": VECTOR / "valid" / "points.geojson",
                "collection/malformed.geojson": VECTOR / "invalid" / "malformed.geojson",
            },
        )

        result = run_cli("--format", "json", "add", "collection", cwd=root, expect=1)

        envelope = assert_one_json_document(result.stdout)
        assert envelope["success"] is False
        assert [item["item_id"] for item in envelope["data"]["added"]] == ["points"]
        assert [Path(f["path"]).name for f in envelope["data"]["failures"]] == ["malformed.geojson"]

    def test_only_failures_leave_added_empty(self, tmp_path: Path, run_cli: RunCli) -> None:
        """With no success, data.added is empty and the top-level errors mirror the failures."""
        root = _add_catalog(
            tmp_path,
            run_cli,
            {
                "collection/empty.geojson": VECTOR / "invalid" / "empty.geojson",
                "collection/malformed.geojson": VECTOR / "invalid" / "malformed.geojson",
            },
        )

        result = run_cli("--format", "json", "add", "collection", cwd=root, expect=1)

        envelope = assert_one_json_document(result.stdout)
        assert envelope["success"] is False
        assert envelope["data"]["added"] == []
        assert sorted(Path(f["path"]).name for f in envelope["data"]["failures"]) == [
            "empty.geojson",
            "malformed.geojson",
        ]
        assert len(envelope["errors"]) == 2
