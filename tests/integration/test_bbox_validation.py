"""Integration tests for bbox validation (issue #516).

Tests that inf/nan coordinates are properly filtered during add
and detected by the check command.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from collections.abc import Callable

    from tests.catalog_invariants import RunCli

# The #852 extent of two bboxes breaks S-BBOX-COUNT, so run_cli must not
# check this catalog at teardown while the bug is open.
ANTIMERIDIAN_REASON = "bbox_validation: the two-bbox antimeridian extent of #852"


def _write_point_geoparquet(path: Path, bbox: list[float]) -> None:
    """Write a one-row GeoParquet 1.0.0 file whose ``geo`` metadata declares ``bbox``."""
    # WKB for POINT(0 0)
    wkb_point = bytes.fromhex("0101000000000000000000000000000000")
    table = pa.table(
        {
            "geometry": pa.array([wkb_point], type=pa.binary()),
            "id": pa.array([1], type=pa.int64()),
        }
    )
    geo_meta = {
        "version": "1.0.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"], "bbox": bbox}},
    }
    schema_with_meta = table.schema.with_metadata({b"geo": json.dumps(geo_meta).encode()})
    pq.write_table(table.cast(schema_with_meta), path)


def _create_geoparquet_with_inf_bbox(path: Path) -> None:
    """Create a GeoParquet file with inf values in the bbox metadata.

    This simulates the IGN Argentina issue where upstream WFS served
    coordinates like -1.79e308 (near float min, effectively -inf). The bbox
    is the issue #516 sentinel value.
    """
    _write_point_geoparquet(path, [-1.79e308, -1.79e308, 180.0, 66.55])


def _create_valid_geoparquet(path: Path) -> None:
    """Create a valid GeoParquet file with a NYC-area bbox."""
    _write_point_geoparquet(path, [-74.0, 40.0, -73.0, 41.0])


@pytest.mark.integration
class TestBboxValidationIntegration:
    """Integration tests for bbox validation during add and check."""

    def test_add_rejects_file_with_only_invalid_bbox(self) -> None:
        """Add command should reject files where ALL bboxes are invalid (defense-in-depth)."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog by creating .portolan/config.yaml
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path(".portolan/metadata.yaml").write_text('license: "CC-BY-4.0"\n')
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection directory with poisoned parquet
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_geoparquet_with_inf_bbox(parquet_path)

            # Add the file - should fail because all bboxes are invalid
            result = runner.invoke(cli, ["add", str(parquet_path)])

            # The add should fail with a clear error message about invalid bbox
            assert result.exit_code != 0, f"add should have failed: {result.output}"
            assert "invalid" in result.output.lower() or "inf" in result.output.lower(), (
                f"Error should mention invalid/inf: {result.output}"
            )

    def test_add_filters_inf_bbox_from_mixed_collection(self) -> None:
        """Add should succeed when a collection has SOME valid bboxes, filtering invalid ones."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path(".portolan/metadata.yaml").write_text('license: "CC-BY-4.0"\n')
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create a collection with BOTH a valid and an invalid parquet file.
            # The invalid file carries the IGN Argentina poison bbox (effectively
            # -inf sentinels). Adding them together must succeed by filtering the
            # invalid bbox out of the aggregated collection extent (issue #516).
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            valid_path = collection_dir / "data_valid.parquet"
            invalid_path = collection_dir / "data_invalid.parquet"
            _create_valid_geoparquet(valid_path)  # bbox [-74, 40, -73, 41]
            _create_geoparquet_with_inf_bbox(invalid_path)  # bbox [-1.79e308, ...]

            # Add both files in one invocation
            result = runner.invoke(cli, ["add", str(valid_path), str(invalid_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Check the collection extent - should reflect ONLY the valid file
            collection_json = collection_dir / "collection.json"
            assert collection_json.exists(), "collection.json should exist"

            with open(collection_json) as f:
                collection_data = json.load(f)

            extent = collection_data.get("extent", {})
            spatial = extent.get("spatial", {})
            bboxes = spatial.get("bbox", [[]])

            # The invalid file's poison value must NOT appear in any extent bbox,
            # and every coordinate must be finite and in WGS84 range.
            assert bboxes and any(bboxes), "collection extent should have a bbox"
            for bbox in bboxes:
                if bbox:
                    for coord in bbox[:4]:
                        assert np.isfinite(coord), f"Collection bbox has non-finite: {bbox}"
                        assert abs(coord) < 1e9, f"Collection bbox has poison coord: {bbox}"
                    assert -180 <= bbox[0] <= 180, f"west out of range: {bbox}"
                    assert -90 <= bbox[1] <= 90, f"south out of range: {bbox}"
                    assert -180 <= bbox[2] <= 180, f"east out of range: {bbox}"
                    assert -90 <= bbox[3] <= 90, f"north out of range: {bbox}"

            # The surviving extent must be exactly the valid file's bbox.
            assert [b[:4] for b in bboxes if b] == [[-74.0, 40.0, -73.0, 41.0]], (
                f"Expected only the valid bbox to survive, got: {bboxes}"
            )

    def test_check_detects_invalid_bbox_in_collection(self) -> None:
        """Check command should detect collections with invalid bbox."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path(".portolan/metadata.yaml").write_text('license: "CC-BY-4.0"\n')
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection with valid file first
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_valid_geoparquet(parquet_path)

            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Manually corrupt the collection bbox (simulating upstream bug)
            collection_json = collection_dir / "collection.json"
            with open(collection_json) as f:
                data = json.load(f)

            # Inject poisoned bbox
            data["extent"]["spatial"]["bbox"] = [[-1.79e308, -1.79e308, 180.0, 66.55]]

            with open(collection_json, "w") as f:
                json.dump(data, f)

            # Run check - PTL-BBX-001 must name the poisoned extent.
            result = runner.invoke(cli, ["check", ".", "--metadata", "--json"])

            findings = json.loads(result.output)["data"]["findings"]
            bbox_findings = [f for f in findings if f["rule_id"] == "PTL-BBX-001"]
            assert bbox_findings, f"Check should detect invalid bbox: {result.output}"
            assert bbox_findings[0]["path"] == "test-collection/collection.json"

    def test_check_passes_with_valid_bboxes(self) -> None:
        """Check command should pass when all bboxes are valid."""
        runner = CliRunner()

        with runner.isolated_filesystem():
            # Initialize catalog
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path(".portolan/metadata.yaml").write_text('license: "CC-BY-4.0"\n')
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )

            # Create collection with valid file
            collection_dir = Path("test-collection")
            collection_dir.mkdir()

            parquet_path = collection_dir / "data.parquet"
            _create_valid_geoparquet(parquet_path)

            result = runner.invoke(cli, ["add", str(parquet_path)])
            assert result.exit_code == 0, f"add failed: {result.output}"

            # Run check - should pass (or at least not fail on bbox_valid)
            result = runner.invoke(cli, ["check", "."])

            # bbox_valid rule should pass
            # Note: check may have other failures (pmtiles, etc.) but bbox should be OK
            output_lower = result.output.lower()
            assert "invalid bbox" not in output_lower, (
                f"Check should not report invalid bbox: {result.output}"
            )


@pytest.mark.integration
class TestAntimeridianBboxIntegration:
    """Integration tests for anti-meridian crossing bbox handling."""

    @pytest.mark.xfail(
        strict=True,
        reason="#852: an antimeridian-crossing extent is written as exactly two bboxes",
    )
    def test_collection_with_antimeridian_crossing_item(
        self, tmp_path: Path, run_cli: RunCli, build_damaged_catalog: Callable[[Path, str], Path]
    ) -> None:
        """Collection with an anti-meridian crossing item writes a STAC 1.1.0 extent.

        STAC 1.1.0 allows one bbox, or an overall bbox followed by two or more
        sub-bboxes. A list of exactly two is invalid. The one-bbox form keeps
        the crossing (west > east), and the overall bbox of the longer form
        does too. Neither may collapse to a whole-world [-180, ..., 180]
        envelope, which would claim that the data spans the globe.
        """
        root = build_damaged_catalog(tmp_path / "catalog", ANTIMERIDIAN_REASON)
        collection_dir = root / "fiji-data"
        collection_dir.mkdir(parents=True)
        run_cli("init", str(root), "--auto", "--license", "CC-BY-4.0")

        # A Fiji-like bbox that crosses 180 degrees.
        parquet_path = collection_dir / "data.parquet"
        _write_point_geoparquet(parquet_path, [177.0, -20.0, -175.0, -15.0])

        run_cli("add", "--portolan-dir", str(root), str(parquet_path))

        collection_data = json.loads((collection_dir / "collection.json").read_text())
        bboxes = collection_data["extent"]["spatial"]["bbox"]

        # All bboxes are finite and inside WGS84 bounds.
        for bbox in bboxes:
            for coord in bbox[:4]:
                assert np.isfinite(coord), f"Bbox contains non-finite: {bbox}"
            assert -180 <= bbox[0] <= 180
            assert -90 <= bbox[1] <= 90
            assert -180 <= bbox[2] <= 180
            assert -90 <= bbox[3] <= 90
        assert len(bboxes) == 1 or len(bboxes) >= 3, f"STAC 1.1.0 forbids {len(bboxes)} bboxes"
        assert bboxes[0][:4] == [177.0, -20.0, -175.0, -15.0]

        result = run_cli("check", str(root), expect=None)
        assert "invalid bbox" not in result.output.lower()

    def test_add_fails_loudly_when_projected_bbox_assumed_wgs84(self, tmp_path: Path) -> None:
        """A projected source whose CRS is lost in conversion must not publish
        meter-valued extents as degrees (issue #785)."""
        import shutil

        runner = CliRunner()
        fixtures = Path(__file__).parent.parent / "fixtures" / "scan" / "complete_shapefile"

        with runner.isolated_filesystem():
            Path(".portolan").mkdir()
            Path(".portolan/config.yaml").write_text("version: 1\n")
            Path(".portolan/metadata.yaml").write_text('license: "CC-BY-4.0"\n')
            Path("catalog.json").write_text(
                json.dumps(
                    {
                        "type": "Catalog",
                        "id": "test-catalog",
                        "stac_version": "1.1.0",
                        "description": "Test",
                        "links": [],
                    }
                )
            )
            collection_dir = Path("boundaries")
            collection_dir.mkdir()
            for file in fixtures.iterdir():
                shutil.copy(file, collection_dir / file.name)

            result = runner.invoke(cli, ["add", str(collection_dir / "radios_sample.shp")])

            # POSGAR meters must never be published as degrees: either the add
            # fails with the CRS mismatch (conversion dropped the CRS), or a
            # future conversion fix preserves the CRS and the bbox is WGS84.
            if result.exit_code != 0:
                assert "PRTLN-CNV004" in result.output or "mismatch" in result.output.lower()
            else:
                # geoparquet-io 1.4.0 preserves the CRS, so `add` now takes this
                # branch. An Item carries `bbox` at the top level. A Collection
                # carries it under `extent.spatial.bbox`, so read both.
                written = list(collection_dir.glob("*.json"))
                assert written, "expected STAC metadata to be written"
                bboxes: list[list[float]] = []
                for f in written:
                    doc = json.loads(f.read_text())
                    if doc.get("bbox"):
                        bboxes.append(doc["bbox"])
                    bboxes.extend(doc.get("extent", {}).get("spatial", {}).get("bbox", []))
                assert bboxes, f"no bbox found in {[f.name for f in written]}"
                assert all(abs(b[0]) <= 180 and abs(b[1]) <= 90 for b in bboxes), (
                    f"projected coordinates published as degrees: {bboxes}"
                )
