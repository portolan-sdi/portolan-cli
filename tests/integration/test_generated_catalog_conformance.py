"""The conformance gate: a freshly generated catalog passes rashid (issue #654).

Builds a catalog the way a user does — ``portolan init`` then ``portolan add``,
with the metadata.yaml enrichment expects — and runs rashid's metadata
pass over the result. Any generation change that breaks spec conformance fails
here, so the gate is the executable form of "Portolan emits conformant catalogs".
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from click.testing import CliRunner
from rashid import RulesConfig, Severity, validate

from portolan_cli.cli import cli

pytestmark = pytest.mark.integration

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "simple.parquet"

# Rules a generated catalog cannot yet satisfy without machinery that does not
# exist in the CLI. Each is a tracked Phase-3 gap, not an accepted violation.
KNOWN_GAPS = frozenset(
    {
        # PTL-DAT-007: every row group needs spatial statistics. geoparquet-io
        # writes a single row group for a file this small and emits neither a
        # bbox covering column nor native GeospatialStatistics for it, so the
        # rule cannot see per-row-group extents. Surfaced when rashid 0.1.1
        # promoted the data pass to default-on.
        "PTL-DAT-007",
    }
)


def _build_catalog(root: Path) -> None:
    """Run init + add over a geo and a tabular collection, with metadata.yaml enrichment."""
    collection_dir = root / "roads"
    collection_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, collection_dir / "roads.parquet")

    # A three-level id, so generation has to write two intermediate catalogs and
    # the deeper one's parent differs from its root. Flat ids alone let #711
    # ship: root and parent coincide at the top level and the conflation between
    # them stays invisible.
    nested_dir = root / "env" / "air" / "quality"
    nested_dir.mkdir(parents=True)
    shutil.copy(FIXTURE, nested_dir / "quality.parquet")

    # A geometry-less Parquet exercises the tabular path, which writes its
    # collection.json outside finalize_items.
    tabular_dir = root / "demographics"
    tabular_dir.mkdir(parents=True)
    pq.write_table(
        pa.table({"tract_id": ["001", "002"], "population": [5000, 7500]}),
        tabular_dir / "census.parquet",
    )

    portolan_dir = root / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "metadata.yaml").write_text(
        yaml.dump(
            {
                "license": "CC-BY-4.0",
                "contact": "data@example.org",
                "source": "Example municipal open-data portal",
                # A mirror, the more demanding of the two provenance shapes: the
                # producer differs from the host, so generation owes a rel:'via'
                # link and an 'updated' stamp on both collections and, because
                # every collection here is a mirror, on the root catalog too
                # (PTL-PRV-001/002/003, PTL-PRO-001/003).
                "providers": [
                    {
                        "name": "Example Statistics Agency",
                        "roles": ["producer", "licensor"],
                        "url": "https://stats.example.org",
                    },
                    {
                        "name": "Example Municipal Open Data",
                        "roles": ["host"],
                        "url": "https://data.example.org/contact",
                    },
                ],
                "source_url": "https://stats.example.org/downloads/roads",
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
            "Demo Catalog",
            "--description",
            "Roads published for the conformance gate.",
            # init keeps the metadata.yaml written above, so this only satisfies the
            # flag's requirement; the license the catalog uses is the one in that file.
            "--license",
            "CC-BY-4.0",
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output

    config = portolan_dir / "config.yaml"
    config.write_text(config.read_text() + "tabular:\n  enabled: true\n", encoding="utf-8")

    result = runner.invoke(
        cli,
        [
            "add",
            "--portolan-dir",
            str(root),
            str(collection_dir / "roads.parquet"),
            str(nested_dir / "quality.parquet"),
            str(tabular_dir / "census.parquet"),
        ],
        catch_exceptions=False,
    )
    assert result.exit_code == 0, result.output


@pytest.fixture(scope="module")
def generated_catalog(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("conformance") / "demo-catalog"
    _build_catalog(root)
    return root


def _validate(root: Path, **kwargs: Any) -> Any:
    """Run every offline pass rashid has: metadata, STAC 1.1.0 structural, data.

    All three ship their schemas (or read local bytes), so the gate stays
    hermetic — it never touches the network.
    """
    return validate(root, structural=True, data=True, **kwargs)


def _findings(root: Path, severity: Severity) -> list[dict[str, Any]]:
    report = _validate(root, config=RulesConfig(disabled=KNOWN_GAPS))
    return [f.to_dict() for f in report.findings if f.severity is severity]


class TestGeneratedCatalogConformance:
    def test_no_errors(self, generated_catalog: Path) -> None:
        """rashid's metadata pass reports zero errors on a generated catalog."""
        assert _findings(generated_catalog, Severity.ERROR) == []

    def test_known_gaps_are_still_real(self, generated_catalog: Path) -> None:
        """The disabled rules still fire, so the gap list cannot rot silently."""
        report = _validate(generated_catalog)
        fired = {f.rule_id for f in report.findings if f.severity is Severity.ERROR}

        assert fired == KNOWN_GAPS


class TestUnlicensedCatalogIsRefused:
    """The gate above only ever saw the happy path, which is what hid issue #686.

    A catalog whose metadata.yaml omits the license used to generate collections
    carrying ``license: "other"`` with no ``rel="license"`` link, a PTL-LIC-002
    ERROR. Portolan now refuses to write such a collection at all, so the honest
    assertion is that nothing gets written rather than that the output conforms.
    """

    def _catalog_without_a_license(self, root: Path) -> Path:
        """Build a managed catalog whose metadata.yaml declares no license."""
        collection_dir = root / "roads"
        collection_dir.mkdir(parents=True)
        shutil.copy(FIXTURE, collection_dir / "roads.parquet")

        portolan_dir = root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan configuration\n", encoding="utf-8")
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump({"contact": "data@example.org"}), encoding="utf-8"
        )
        (root / "catalog.json").write_text(
            '{"type": "Catalog", "stac_version": "1.1.0", "id": "demo",'
            ' "description": "No license here", "links": []}',
            encoding="utf-8",
        )
        return collection_dir / "roads.parquet"

    def test_add_refuses_and_writes_no_collection(self, tmp_path: Path) -> None:
        root = tmp_path / "unlicensed"
        source = self._catalog_without_a_license(root)

        result = CliRunner().invoke(cli, ["add", "--portolan-dir", str(root), str(source)])

        assert result.exit_code == 1, result.output
        assert "PRTLN-VAL004" in result.output
        assert list(root.rglob("collection.json")) == []

    def test_the_violation_it_prevents_is_real(self, tmp_path: Path) -> None:
        """Prove PTL-LIC-002 is what the gate averts, not a rule we assume exists.

        Takes a conformant generated catalog and puts back exactly what the old
        code emitted: ``license: "other"`` and no ``rel="license"`` link. Without
        this, the test above would keep passing even if the rule it protects
        against were renamed or dropped upstream.
        """
        root = tmp_path / "damaged-catalog"
        _build_catalog(root)
        collection_path = root / "roads" / "collection.json"

        collection = json.loads(collection_path.read_text(encoding="utf-8"))
        collection["license"] = "other"
        collection["links"] = [link for link in collection["links"] if link.get("rel") != "license"]
        collection_path.write_text(json.dumps(collection, indent=2), encoding="utf-8")

        fired = {
            f.rule_id
            for f in _validate(root, config=RulesConfig(disabled=KNOWN_GAPS)).findings
            if f.severity is Severity.ERROR
        }

        assert fired == {"PTL-LIC-002"}


class TestAnIdentifierOutsideThePopularShortlistSurvivesGeneration:
    """Issue #727: the two commands disagreed on real SPDX identifiers.

    ``metadata validate`` judged the license against a hand-written 26-entry
    subset while ``check`` used rashid's full list, so ``EUPL-1.2`` was rejected
    by one and accepted by the other. Proving agreement on a dict is not enough;
    this runs the identifier through generation and validates the output.
    """

    def test_eupl_reaches_the_collection_and_conforms(self, tmp_path: Path) -> None:
        root = tmp_path / "eupl-catalog"
        collection_dir = root / "roads"
        collection_dir.mkdir(parents=True)
        shutil.copy(FIXTURE, collection_dir / "roads.parquet")

        portolan_dir = root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("# Portolan configuration\n", encoding="utf-8")
        (portolan_dir / "metadata.yaml").write_text(
            yaml.dump({"contact": "data@example.org", "license": "EUPL-1.2"}), encoding="utf-8"
        )
        (root / "catalog.json").write_text(
            '{"type": "Catalog", "stac_version": "1.1.0", "id": "demo",'
            ' "description": "Licensed under EUPL-1.2", "links": []}',
            encoding="utf-8",
        )

        result = CliRunner().invoke(
            cli, ["add", "--portolan-dir", str(root), str(collection_dir / "roads.parquet")]
        )

        assert result.exit_code == 0, result.output
        collection = json.loads((collection_dir / "collection.json").read_text(encoding="utf-8"))
        assert collection["license"] == "EUPL-1.2"

        license_findings = [
            f.rule_id
            for f in _validate(root, config=RulesConfig(disabled=KNOWN_GAPS)).findings
            if f.rule_id.startswith("PTL-LIC")
        ]
        assert license_findings == []
