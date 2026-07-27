"""check → fix → re-check, end to end, on a catalog Portolan generated itself.

The acceptance test for the remediation engine. It builds a real catalog, breaks
six things a fixer is supposed to own, and asserts three properties in order:

1. ``check --json`` names the exact ``PTL-*`` ids for the damage, each in the
   AUTO bucket.
2. ``check --fix`` resolves every one of them.
3. What survives is exactly the residue the generator cannot satisfy today —
   the tracked gaps from the Phase-1 conformance gate — and the human output
   lists them under "Action required" with their requirement sentence.

Property 3 is the loop-termination guarantee: an agent reading the output learns
which findings are worth another ``--fix`` (none) and which need it to act.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.constants import PORTOLAN_SCHEMA_URI

pytestmark = [pytest.mark.integration, pytest.mark.slow]

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"

# Errors a generated catalog still carries: thumbnail rendering lives behind an
# optional extra, Portolan has no provider model, and geoparquet-io writes a
# single row group with no per-row-group spatial statistics for a file this
# small. Shared with tests/integration/test_generated_catalog_conformance.py,
# which is the gate that keeps the list honest.
GENERATION_GAPS = frozenset({"PTL-VIZ-001", "PTL-PRV-001", "PTL-DAT-007"})

# What the corruptions below must produce, and nothing else.
EXPECTED_FROM_DAMAGE = frozenset(
    {
        "PTL-TTL-001",  # stripped title
        "PTL-FIL-001",  # deleted AGENTS.md
        "PTL-FIL-002",  # dropped rel:'agents' link
        "PTL-DAT-001",  # mutated file:checksum
        "PTL-CNF-001",  # removed schema URI
        "PTL-LNK-005",  # added rel:'self' link
    }
)


def _build_catalog(root: Path) -> None:
    collection_dir = root / "roads"
    collection_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, collection_dir / "roads.parquet")

    portolan_dir = root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            {
                "license": "CC-BY-4.0",
                "contact": "data@example.org",
                "source": "Example municipal open-data portal",
            }
        ),
        encoding="utf-8",
    )

    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "init",
            str(root),
            "--auto",
            "--title",
            "Roundtrip Catalog",
            "--description",
            "Roads published for the remediation roundtrip.",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    result = runner.invoke(
        cli,
        ["add", "--portolan-dir", str(root), str(collection_dir / "roads.parquet")],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


def _read(path: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return loaded


def _write(path: Path, data: dict[str, Any]) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _corrupt(root: Path) -> None:
    """Break one thing per fixer key the registry claims to own."""
    collection_json = root / "roads" / "collection.json"
    data = _read(collection_json)

    # 1. titles: a collection with no title.
    data.pop("title", None)

    # 2. schema_uri: drop the Portolan profile URI.
    data["stac_extensions"] = [
        uri for uri in data.get("stac_extensions", []) if uri != PORTOLAN_SCHEMA_URI
    ]

    # 3. links: a self link a SELF_CONTAINED catalog must not carry.
    data["links"].append({"rel": "self", "href": "./collection.json", "type": "application/json"})

    # 4. agents: drop the rel='agents' link and the file it points at.
    data["links"] = [link for link in data["links"] if link.get("rel") != "agents"]

    # 5. checksum: a well-formed multihash that does not match the bytes, so the
    #    data pass reports the mismatch rather than the encoding.
    for asset in data["assets"].values():
        if isinstance(asset.get("file:checksum"), str):
            asset["file:checksum"] = "1220" + "0" * 64

    _write(collection_json, data)
    (root / "roads" / "AGENTS.md").unlink()


def _check_json(root: Path, *args: str) -> dict[str, Any]:
    result = CliRunner().invoke(
        cli, ["check", str(root), "--metadata", "--json", *args], catch_exceptions=False
    )
    payload: dict[str, Any] = json.loads(result.output)
    return payload["data"]


def _snapshot(root: Path) -> dict[str, bytes]:
    return {
        str(p.relative_to(root)): p.read_bytes() for p in sorted(root.rglob("*")) if p.is_file()
    }


@pytest.fixture
def catalog(tmp_path: Path) -> Path:
    root = tmp_path / "roundtrip-catalog"
    _build_catalog(root)
    return root


class TestRashidRoundtrip:
    def test_generated_catalog_carries_only_the_tracked_gaps(self, catalog: Path) -> None:
        """Before any damage, the only errors are the ones Phase 1 tracks."""
        data = _check_json(catalog)
        fired = {f["rule_id"] for f in data["findings"] if f["severity"] == "error"}
        assert fired == GENERATION_GAPS

    def test_damage_surfaces_the_expected_ids_and_buckets(self, catalog: Path) -> None:
        _corrupt(catalog)
        data = _check_json(catalog)

        fired = {f["rule_id"] for f in data["findings"] if f["severity"] == "error"}
        assert fired == EXPECTED_FROM_DAMAGE | GENERATION_GAPS

        buckets = {
            f["rule_id"]: f["remediation"]
            for f in data["findings"]
            if f["rule_id"] in EXPECTED_FROM_DAMAGE
        }
        assert buckets == dict.fromkeys(EXPECTED_FROM_DAMAGE, "auto")
        assert all(
            f["auto_fixable"] for f in data["findings"] if f["rule_id"] in EXPECTED_FROM_DAMAGE
        )

    def test_fix_resolves_every_damaged_rule(self, catalog: Path) -> None:
        _corrupt(catalog)

        data = _check_json(catalog, "--fix")

        remaining = {f["rule_id"] for f in data["findings"] if f["severity"] == "error"}
        assert remaining & EXPECTED_FROM_DAMAGE == set()
        assert remaining == GENERATION_GAPS

    def test_fix_payload_reports_what_it_applied_and_what_survived(self, catalog: Path) -> None:
        _corrupt(catalog)

        data = _check_json(catalog, "--fix")
        fix = data["fix"]

        assert {"schema_uri", "required_files", "agents", "links", "titles", "checksum"} <= set(
            fix["applied"]
        )
        # PTL-DAT-007 is an AUTO rule the convert fixer cannot satisfy for a
        # single-row-group file, so it is the honest survivor.
        assert {item["rule_id"] for item in fix["survivors"]} == {"PTL-DAT-007"}
        assert fix["fixed_count"] == fix["auto_count"] - len(fix["survivors"])

    def test_fix_restores_each_broken_artifact(self, catalog: Path) -> None:
        _corrupt(catalog)

        _check_json(catalog, "--fix")

        data = _read(catalog / "roads" / "collection.json")
        rels = [link.get("rel") for link in data["links"]]
        assert (catalog / "roads" / "AGENTS.md").exists()
        assert "agents" in rels
        assert "self" not in rels
        assert PORTOLAN_SCHEMA_URI in data["stac_extensions"]
        assert data["title"]
        assert all(asset["file:checksum"] != "1220" + "0" * 64 for asset in data["assets"].values())

    def test_human_output_splits_fixed_from_action_required(self, catalog: Path) -> None:
        _corrupt(catalog)

        result = CliRunner().invoke(
            cli, ["check", str(catalog), "--metadata", "--fix"], catch_exceptions=False
        )

        assert "Fixed automatically (" in result.output
        assert "Action required (" in result.output
        # Every surviving finding carries its requirement sentence, and the AUTO
        # survivor says the automatic fix did not settle it.
        assert "Render a thumbnail" in result.output
        assert "the automatic fix did not resolve this" in result.output

    def test_dry_run_writes_nothing(self, catalog: Path) -> None:
        _corrupt(catalog)
        before = _snapshot(catalog)

        data = _check_json(catalog, "--fix", "--dry-run")

        assert _snapshot(catalog) == before
        assert data["fix"]["dry_run"] is True
        assert data["fix"]["applied"] != []
