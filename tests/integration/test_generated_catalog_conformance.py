"""The conformance gate: a freshly generated catalog passes rashid (issue #654).

Builds a catalog the way a user does — ``portolan init`` then ``portolan add``,
with the metadata.yaml enrichment ADR-0038 expects — and runs rashid's metadata
pass over the result. Any generation change that breaks spec conformance fails
here, so the gate is the executable form of "Portolan emits conformant catalogs".
"""

from __future__ import annotations

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
        # PTL-VIZ-001: every geospatial collection needs a thumbnail asset.
        # Thumbnail rendering lives behind the optional [thumbnails] extra and
        # is not part of the default add pipeline.
        "PTL-VIZ-001",
        # PTL-PRV-001: every collection needs a provider with the producer role.
        # Portolan has no provider model — metadata.yaml carries `contact` and
        # `attribution`, neither of which maps onto STAC providers without a
        # design decision about roles and ordering (PTL-PRV-002/003).
        "PTL-PRV-001",
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
