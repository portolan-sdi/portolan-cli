"""Tests for metadata update functions.

Tests for Phase 2c: Update Functions
- update_item_metadata(item_path, file_path)
- create_missing_item(file_path, collection_path)
- update_collection_extent(collection_path)
- update_versions_tracking(file_path, versions_path)
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest
import rasterio

from portolan_cli.catalog import init_catalog
from portolan_cli.collection import (
    create_collection,
    read_collection_json,
    write_collection_json,
)
from portolan_cli.item import create_item, read_item_json, write_item_json
from portolan_cli.metadata.cog import COGMetadata
from portolan_cli.metadata.update import (
    create_missing_item,
    update_collection_extent,
    update_item_metadata,
    update_versions_tracking,
)
from portolan_cli.versions import (
    Asset,
    Version,
    VersionsFile,
    read_versions,
    write_versions,
)

# Path to test fixtures
FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"
REALDATA_FIXTURES = FIXTURES_DIR / "realdata"

# The band-refresh rasters (#737). A 4-band int16 COG, and the 1-band uint8 COG
# that replaces it on disk.
FOUR_BAND_RASTER = REALDATA_FIXTURES / "rapidai4eo-sample.tif"
ONE_BAND_RASTER = FIXTURES_DIR / "raster" / "valid" / "singleband.tif"

# The statistics formats.md requires on every COG band ("Raster statistics"),
# under the STAC field names the raster extension defines.
REQUIRED_BAND_STATISTICS = frozenset({"minimum", "maximum", "mean", "stddev"})

# The 4-band raster's band 1 statistics, as `add` records them. They are the
# stale values a refresh must not carry onto a re-shaped file, and 302 is not
# even representable in the 1-band raster's uint8 bands.
STALE_BAND_1_STATISTICS = {
    "minimum": 302.0,
    "maximum": 1015.0,
    "mean": 460.38215000000383,
    "stddev": 51.76964613919496,
}


def _raster_item(root: Path, source: Path) -> tuple[Path, Path]:
    """Copy `source` into a collection under `root` and write its item.json.

    Returns (item_path, data_path). Initializes a real catalog, because the
    statistics enrichment only runs where its setting resolves.
    """
    if not source.exists():
        pytest.skip(f"Raster fixture not available: {source.name}")
    init_catalog(root, license_id="CC-BY-4.0")
    collection_dir = root / "imagery"
    collection_dir.mkdir(exist_ok=True)
    data_file = collection_dir / "scene1.tif"
    shutil.copy(source, data_file)
    item = create_item(item_id="scene1", data_path=data_file, collection_id="imagery")
    return write_item_json(item, collection_dir), data_file


def _record_bands(item_path: Path, bands: list[dict[str, Any]]) -> None:
    """Write `bands` onto the item's data asset, as a previous `add` would have."""
    item_json = json.loads(item_path.read_text())
    item_json["assets"]["data"]["bands"] = bands
    item_path.write_text(json.dumps(item_json, indent=2))


def _read_bands(item_path: Path) -> list[dict[str, Any]]:
    """The data asset's bands array as it stands on disk."""
    bands: list[dict[str, Any]] = json.loads(item_path.read_text())["assets"]["data"]["bands"]
    return bands


class TestUpdateItemMetadata:
    """Tests for update_item_metadata function."""

    @pytest.mark.unit
    def test_update_item_metadata_updates_bbox(self, tmp_path: Path) -> None:
        """Test that update_item_metadata re-extracts bbox from file."""
        # Setup: Create collection directory with a GeoParquet file
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        # Use WGS84 fixture
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create initial item
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Read original bbox
        original_item = read_item_json(item_path)
        original_bbox = original_item.bbox.copy()

        # Update item metadata (should re-extract from file)
        updated_item = update_item_metadata(item_path, data_file)

        # Verify bbox was extracted (should match since file unchanged)
        assert updated_item.bbox is not None
        assert len(updated_item.bbox) == 4
        # Since file didn't change, bbox should be same
        assert updated_item.bbox == original_bbox

    @pytest.mark.unit
    def test_update_item_metadata_preserves_user_fields(self, tmp_path: Path) -> None:
        """Test that update_item_metadata preserves user-added fields like title, description."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create initial item with user-provided title and description
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
            title="User Title",
            description="User Description",
        )
        item_path = write_item_json(item, collection_dir)

        # Update item metadata
        updated_item = update_item_metadata(item_path, data_file)

        # Verify user fields are preserved
        assert updated_item.title == "User Title"
        assert updated_item.description == "User Description"

    @pytest.mark.unit
    def test_update_item_metadata_keeps_the_recorded_datetime(self, tmp_path: Path) -> None:
        """The refresh keeps the acquisition datetime the item already carries.

        `datetime` records when the observation happened, which the refresh
        cannot read off the bytes. Stamping the time of the run over it published
        a fabricated acquisition date (#709). This test replaces one that
        asserted the old behavior.
        """
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item.properties["datetime"] = "2020-01-01T00:00:00+00:00"
        item_path = write_item_json(item, collection_dir)

        updated_item = update_item_metadata(item_path, data_file)

        assert updated_item.properties.get("datetime") == "2020-01-01T00:00:00+00:00"

    @pytest.mark.unit
    def test_update_item_metadata_keeps_a_null_datetime(self, tmp_path: Path) -> None:
        """`add` writes ``datetime: null`` without ``--datetime``. It survives."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item.properties["datetime"] = None
        item_path = write_item_json(item, collection_dir)

        update_item_metadata(item_path, data_file)

        on_disk = json.loads(item_path.read_text(encoding="utf-8"))
        assert on_disk["properties"]["datetime"] is None

    @pytest.mark.unit
    def test_update_item_metadata_file_not_found(self, tmp_path: Path) -> None:
        """Test that update_item_metadata raises FileNotFoundError for missing file."""
        item_path = tmp_path / "nonexistent_item.json"
        file_path = tmp_path / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            update_item_metadata(item_path, file_path)

    @pytest.mark.unit
    def test_update_item_metadata_item_not_found(self, tmp_path: Path) -> None:
        """Test that update_item_metadata raises FileNotFoundError for missing item."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)
        item_path = tmp_path / "nonexistent_item.json"

        with pytest.raises(FileNotFoundError):
            update_item_metadata(item_path, data_file)

    @pytest.mark.unit
    def test_update_item_metadata_writes_to_disk(self, tmp_path: Path) -> None:
        """Test that update_item_metadata writes updated item to disk."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Update item metadata
        update_item_metadata(item_path, data_file)

        # Verify file was updated on disk
        updated_from_disk = read_item_json(item_path)
        assert updated_from_disk.bbox is not None

    @pytest.mark.unit
    def test_update_item_metadata_preserves_thumbnail_asset(self, tmp_path: Path) -> None:
        """Non-data assets (e.g. the thumbnail from #657) survive the refresh (#659)."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(item_id="data", data_path=data_file, collection_id="my-collection")
        item_path = write_item_json(item, collection_dir)

        # Inject a thumbnail asset alongside the data asset, as `add` does for COGs.
        item_json = json.loads(item_path.read_text())
        item_json["assets"]["thumbnail"] = {
            "href": "./data.thumb.jpg",
            "type": "image/jpeg",
            "roles": ["thumbnail"],
        }
        item_path.write_text(json.dumps(item_json, indent=2))

        update_item_metadata(item_path, data_file)

        # Read the raw JSON, the ItemModel round-trip is itself lossy for these.
        result = json.loads(item_path.read_text())
        assert "thumbnail" in result["assets"], "thumbnail asset was destroyed by the refresh"
        assert result["assets"]["thumbnail"]["href"] == "./data.thumb.jpg"
        assert result["assets"]["thumbnail"]["roles"] == ["thumbnail"]
        assert "data" in result["assets"]

    @pytest.mark.unit
    def test_update_item_metadata_preserves_stac_extensions(self, tmp_path: Path) -> None:
        """stac_extensions (projection, raster, ...) survive the refresh (#659)."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(item_id="data", data_path=data_file, collection_id="my-collection")
        item_path = write_item_json(item, collection_dir)

        extensions = [
            "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
            "https://stac-extensions.github.io/raster/v2.0.0/schema.json",
        ]
        item_json = json.loads(item_path.read_text())
        item_json["stac_extensions"] = extensions
        item_path.write_text(json.dumps(item_json, indent=2))

        update_item_metadata(item_path, data_file)

        result = json.loads(item_path.read_text())
        assert result.get("stac_extensions") == extensions

    @pytest.mark.unit
    def test_update_item_metadata_preserves_data_asset_bands(self, tmp_path: Path) -> None:
        """The data asset's bands / statistics survive the refresh (#659)."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        item = create_item(item_id="data", data_path=data_file, collection_id="my-collection")
        item_path = write_item_json(item, collection_dir)

        bands = [{"name": "b1", "statistics": {"minimum": 0.0, "maximum": 255.0}}]
        item_json = json.loads(item_path.read_text())
        item_json["assets"]["data"]["bands"] = bands
        item_path.write_text(json.dumps(item_json, indent=2))

        update_item_metadata(item_path, data_file)

        result = json.loads(item_path.read_text())
        assert result["assets"]["data"].get("bands") == bands

    @pytest.mark.unit
    def test_update_item_metadata_preserves_cog_media_type(self, tmp_path: Path) -> None:
        """The refresh must not downgrade a COG's media type (PORTO-CORE-026, #659).

        PORTO-CORE-026 makes the ``profile=cloud-optimized`` suffix a MUST for a
        COG data asset, and that is what ``portolan add`` writes. A metadata
        refresh owns the asset's href and type, so it must re-derive the same
        registry-backed string rather than the bare GeoTIFF one.
        """
        cog_file = REALDATA_FIXTURES / "rapidai4eo-sample.tif"
        if not cog_file.exists():
            pytest.skip("COG fixture not available")

        collection_dir = tmp_path / "cog-collection"
        collection_dir.mkdir()

        data_file = collection_dir / "image.tif"
        shutil.copy(cog_file, data_file)

        item = create_item(item_id="image", data_path=data_file, collection_id="cog-collection")
        item_path = write_item_json(item, collection_dir)

        # Write the spec-correct media type that `add` registers for a COG.
        cog_media_type = "image/tiff; application=geotiff; profile=cloud-optimized"
        item_json = json.loads(item_path.read_text())
        item_json["assets"]["data"]["type"] = cog_media_type
        item_path.write_text(json.dumps(item_json, indent=2))

        update_item_metadata(item_path, data_file)

        result = json.loads(item_path.read_text())
        assert result["assets"]["data"]["type"] == cog_media_type

    @pytest.mark.unit
    @pytest.mark.realdata
    def test_update_item_metadata_reextracts_reshaped_raster_bands(self, tmp_path: Path) -> None:
        """A re-shaped raster gets a re-extracted bands array (#737).

        Preserving the array verbatim (#659) leaves 4 int16 bands describing a
        1-band uint8 file. formats.md makes the file the authority for band
        statistics, so the raster refresh re-reads the bytes on disk.
        """
        item_path, data_file = _raster_item(tmp_path, FOUR_BAND_RASTER)
        _record_bands(
            item_path,
            [
                {
                    "name": f"band_{index}",
                    "data_type": "int16",
                    "statistics": dict(STALE_BAND_1_STATISTICS),
                }
                for index in range(1, 5)
            ],
        )

        # Re-shape the raster in place: 4-band int16 becomes 1-band uint8.
        shutil.copy(ONE_BAND_RASTER, data_file)

        update_item_metadata(item_path, data_file)

        bands = _read_bands(item_path)
        assert [band["name"] for band in bands] == ["band_1"]
        assert bands[0]["data_type"] == "uint8"
        statistics = bands[0]["statistics"]
        # The full set formats.md requires, describing the uint8 file. Every
        # value sits inside the type's range, not the int16 raster's 302..1015.
        assert REQUIRED_BAND_STATISTICS <= statistics.keys()
        assert 0 <= statistics["minimum"] <= statistics["maximum"] <= 255
        assert 0 <= statistics["mean"] <= 255

    @pytest.mark.unit
    def test_update_item_metadata_keeps_hand_authored_band_fields(self, tmp_path: Path) -> None:
        """A same-shape refresh keeps the band name and description a human wrote (#737).

        A name and a description are descriptive, not derivable from pixels, so
        the refresh keeps them. The data type is derivable, so it comes from the
        file.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        _record_bands(
            item_path,
            [
                {
                    "name": "red",
                    "description": "Surface reflectance, hand authored",
                    "data_type": "int16",
                }
            ],
        )

        update_item_metadata(item_path, data_file)

        bands = _read_bands(item_path)
        assert len(bands) == 1
        assert bands[0]["name"] == "red"
        assert bands[0]["description"] == "Surface reflectance, hand authored"
        assert bands[0]["data_type"] == "uint8"
        assert REQUIRED_BAND_STATISTICS <= bands[0]["statistics"].keys()

    @pytest.mark.unit
    @pytest.mark.realdata
    def test_update_item_metadata_drops_band_labels_when_count_changes(
        self, tmp_path: Path
    ) -> None:
        """A hand-authored label never lands on a band it no longer describes (#737)."""
        item_path, data_file = _raster_item(tmp_path, FOUR_BAND_RASTER)
        _record_bands(
            item_path,
            [
                {"name": name, "description": f"{name} band", "data_type": "int16"}
                for name in ("red", "green", "blue", "nir")
            ],
        )

        shutil.copy(ONE_BAND_RASTER, data_file)

        update_item_metadata(item_path, data_file)

        bands = _read_bands(item_path)
        assert [band["name"] for band in bands] == ["band_1"]
        assert "description" not in bands[0]

    @pytest.mark.unit
    def test_update_item_metadata_ignores_statistics_sidecar(self, tmp_path: Path) -> None:
        """A PAM sidecar cannot answer for the file's statistics (#737).

        formats.md is explicit that an external ``.aux.xml`` sidecar does not
        satisfy the embedded-statistics requirement. GDAL writes one when it
        computes statistics and never invalidates it when the raster underneath
        is replaced, so the refresh must read past it.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        _record_bands(
            item_path,
            [{"name": "band_1", "data_type": "int16", "statistics": STALE_BAND_1_STATISTICS}],
        )

        # The sidecar `add` left when it read the int16 raster this uint8 file
        # replaced.
        (data_file.parent / f"{data_file.name}.aux.xml").write_text(
            '<PAMDataset><PAMRasterBand band="1"><Metadata>'
            '<MDI key="STATISTICS_MINIMUM">302</MDI>'
            '<MDI key="STATISTICS_MAXIMUM">1015</MDI>'
            '<MDI key="STATISTICS_MEAN">460.38215</MDI>'
            '<MDI key="STATISTICS_STDDEV">51.769646139195</MDI>'
            '<MDI key="STATISTICS_VALID_PERCENT">100</MDI>'
            "</Metadata></PAMRasterBand></PAMDataset>"
        )

        update_item_metadata(item_path, data_file)

        statistics = _read_bands(item_path)[0]["statistics"]
        assert REQUIRED_BAND_STATISTICS <= statistics.keys()
        assert statistics["minimum"] != STALE_BAND_1_STATISTICS["minimum"]
        assert statistics["maximum"] <= 255

    @pytest.mark.unit
    @pytest.mark.parametrize("failure", ["raises", "empty"])
    def test_update_item_metadata_preserves_bands_when_reextraction_fails(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
        failure: str,
    ) -> None:
        """A failed re-extraction keeps the existing array rather than deleting it (#737).

        The lesson of #659. A refresh that cannot read the file must not
        destroy the record it cannot replace.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        bands = [{"name": "band_1", "data_type": "int16", "statistics": STALE_BAND_1_STATISTICS}]
        _record_bands(item_path, bands)

        def broken(self: COGMetadata) -> dict[str, Any]:
            if failure == "raises":
                raise OSError("band extraction failed")
            return {}

        monkeypatch.setattr(COGMetadata, "to_stac_properties", broken)

        update_item_metadata(item_path, data_file)

        assert _read_bands(item_path) == bands

    @pytest.mark.unit
    def test_update_item_metadata_computes_statistics_from_pixels(self, tmp_path: Path) -> None:
        """The refreshed statistics come from the pixels, not from the file's tags (#737).

        This is what keeps the band refresh independent of #748, where a COG
        Portolan writes carries its statistics in a PAM sidecar instead of
        embedded in the file. The refresh reads with ``GDAL_PAM_ENABLED=NO`` and
        the raster below embeds no statistics at all, so a refresh that only
        read recorded values would write none. ``statistics.raster_mode``
        defaults to ``approx``, which computes them, so a full set arrives.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        _record_bands(item_path, [{"name": "band_1", "data_type": "int16"}])

        # The precondition that makes this a real assertion: nothing to read.
        with rasterio.Env(GDAL_PAM_ENABLED="NO"), rasterio.open(data_file) as src:
            assert "STATISTICS_MINIMUM" not in src.tags(bidx=1)
        assert not (data_file.parent / f"{data_file.name}.aux.xml").exists()

        update_item_metadata(item_path, data_file)

        band = _read_bands(item_path)[0]
        assert band["data_type"] == "uint8"
        assert REQUIRED_BAND_STATISTICS <= band["statistics"].keys()
        assert 0 <= band["statistics"]["minimum"] <= band["statistics"]["maximum"] <= 255

    @pytest.mark.unit
    def test_update_item_metadata_keeps_statistics_extraction_cannot_recompute(
        self, tmp_path: Path
    ) -> None:
        """A same-shape refresh never trades recorded statistics for none (#737).

        ``statistics.raster_mode`` defaults to ``approx``, which computes the
        values. A catalog can set ``cached`` instead, and then the extractor
        reads embedded tags only. The raster below has none and the refresh
        disables PAM, so extraction returns no ``statistics`` key at all. The
        merge must therefore keep the recorded object rather than drop it.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        (tmp_path / ".portolan" / "config.yaml").write_text(
            "statistics:\n  enabled: true\n  raster_mode: cached\n"
        )
        _record_bands(
            item_path,
            [{"name": "band_1", "data_type": "int16", "statistics": dict(STALE_BAND_1_STATISTICS)}],
        )

        update_item_metadata(item_path, data_file)

        band = _read_bands(item_path)[0]
        # Derivable from the file, so the refresh owns it.
        assert band["data_type"] == "uint8"
        # Not derivable in this mode, so the recorded object survives intact.
        assert band["statistics"] == STALE_BAND_1_STATISTICS

    @pytest.mark.unit
    def test_update_item_metadata_preserves_bands_for_unreadable_raster(
        self, tmp_path: Path
    ) -> None:
        """A raster extension the COG reader cannot open preserves, it does not raise (#737).

        The guard keys on ``RASTER_EXTENSIONS``, which is every extension that
        routes as raster, so it is wider than the COGs ``extract_cog_metadata``
        reads. A ``.jp2`` GDAL has no driver for, or any raster whose bytes are
        unreadable, must therefore fall through to the preserve path rather than
        failing the refresh.
        """
        item_path, data_file = _raster_item(tmp_path, ONE_BAND_RASTER)
        unreadable = data_file.with_suffix(".jp2")
        data_file.rename(unreadable)
        unreadable.write_bytes(b"not a JPEG 2000 codestream")

        bands = [{"name": "band_1", "data_type": "uint16"}]
        _record_bands(item_path, bands)

        update_item_metadata(item_path, unreadable)

        assert _read_bands(item_path) == bands


class TestCreateMissingItem:
    """Tests for create_missing_item function."""

    @pytest.mark.unit
    def test_create_missing_item_creates_item(self, tmp_path: Path) -> None:
        """Test that create_missing_item creates a new STAC item for a file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create collection first
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create item for the file
        item_path = create_missing_item(data_file, collection_dir)

        # Verify item was created
        assert item_path.exists()
        item = read_item_json(item_path)
        assert item.id == "data"
        assert item.collection == "my-collection"

    @pytest.mark.unit
    def test_create_missing_item_links_to_collection(self, tmp_path: Path) -> None:
        """Test that create_missing_item properly links to parent collection."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        # Verify collection reference
        assert item.collection == "my-collection"

    @pytest.mark.unit
    def test_create_missing_item_extracts_metadata(self, tmp_path: Path) -> None:
        """Test that create_missing_item extracts metadata from file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        # Verify metadata was extracted
        assert item.bbox is not None
        assert len(item.bbox) == 4
        assert item.geometry is not None
        assert item.properties.get("datetime") is not None

    @pytest.mark.unit
    def test_create_missing_item_file_not_found(self, tmp_path: Path) -> None:
        """Test that create_missing_item raises FileNotFoundError for missing file."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()
        file_path = collection_dir / "nonexistent.parquet"

        with pytest.raises(FileNotFoundError):
            create_missing_item(file_path, collection_dir)

    @pytest.mark.unit
    def test_create_missing_item_uses_filename_as_id(self, tmp_path: Path) -> None:
        """Test that create_missing_item uses filename stem as item ID."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "my_custom_name.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item_path = create_missing_item(data_file, collection_dir)
        item = read_item_json(item_path)

        assert item.id == "my_custom_name"


class TestUpdateCollectionExtent:
    """Tests for update_collection_extent function."""

    @pytest.mark.unit
    def test_update_collection_extent_single_item(self, tmp_path: Path) -> None:
        """Test that update_collection_extent calculates extent from single item."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create collection with initial extent
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create an item
        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        write_item_json(item, collection_dir)

        # Update collection extent
        updated_collection = update_collection_extent(collection_dir)

        # Verify extent was updated
        assert updated_collection.extent.spatial.bbox is not None
        assert len(updated_collection.extent.spatial.bbox) >= 1

    @pytest.mark.unit
    def test_update_collection_extent_multiple_items(self, tmp_path: Path) -> None:
        """Test that update_collection_extent computes union bbox from multiple items."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        # Create first data file
        data_file1 = collection_dir / "data1.parquet"
        shutil.copy(source_file, data_file1)

        # Create second data file
        data_file2 = collection_dir / "data2.parquet"
        shutil.copy(source_file, data_file2)

        # Create collection
        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file1,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Create items with different bboxes
        item1 = create_item(
            item_id="data1",
            data_path=data_file1,
            collection_id="my-collection",
        )
        # Manually modify bbox for testing union
        item1 = item1.__class__(
            id=item1.id,
            geometry=item1.geometry,
            bbox=[-10.0, -10.0, 0.0, 0.0],
            properties=item1.properties,
            assets=item1.assets,
            collection=item1.collection,
        )
        write_item_json(item1, collection_dir)

        item2 = create_item(
            item_id="data2",
            data_path=data_file2,
            collection_id="my-collection",
        )
        item2 = item2.__class__(
            id=item2.id,
            geometry=item2.geometry,
            bbox=[0.0, 0.0, 10.0, 10.0],
            properties=item2.properties,
            assets=item2.assets,
            collection=item2.collection,
        )
        write_item_json(item2, collection_dir)

        # Update collection extent
        updated_collection = update_collection_extent(collection_dir)

        # Verify union bbox
        bbox = updated_collection.extent.spatial.bbox[0]
        assert bbox[0] == -10.0  # west
        assert bbox[1] == -10.0  # south
        assert bbox[2] == 10.0  # east
        assert bbox[3] == 10.0  # north

    @pytest.mark.unit
    def test_update_collection_extent_no_items(self, tmp_path: Path) -> None:
        """Test that update_collection_extent handles empty collection."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        # Create collection with default extent (global)
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        # Remove the data file to simulate empty collection
        data_file.unlink()

        # Update collection extent - should return existing extent
        updated_collection = update_collection_extent(collection_dir)

        # Collection extent should still exist (unchanged)
        assert updated_collection.extent is not None

    @pytest.mark.unit
    def test_update_collection_extent_writes_to_disk(self, tmp_path: Path) -> None:
        """Test that update_collection_extent writes updated collection to disk."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        collection = create_collection(
            collection_id="my-collection",
            data_path=data_file,
            description="Test collection",
        )
        write_collection_json(collection, collection_dir)

        item = create_item(
            item_id="data",
            data_path=data_file,
            collection_id="my-collection",
        )
        write_item_json(item, collection_dir)

        # Update collection extent
        update_collection_extent(collection_dir)

        # Verify file was updated
        collection_path = collection_dir / "collection.json"
        assert collection_path.exists()
        updated_from_disk = read_collection_json(collection_path)
        assert updated_from_disk.extent.spatial.bbox is not None

    @pytest.mark.unit
    def test_update_collection_extent_collection_not_found(self, tmp_path: Path) -> None:
        """Test that update_collection_extent raises FileNotFoundError."""
        collection_dir = tmp_path / "nonexistent"

        with pytest.raises(FileNotFoundError):
            update_collection_extent(collection_dir)


class TestUpdateVersionsTracking:
    """Tests for update_versions_tracking function."""

    @pytest.mark.unit
    def test_update_versions_tracking_updates_mtime(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking updates source_mtime."""
        # Create a data file
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)

        # Create versions.json
        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="data.parquet",
                            source_mtime=1000.0,  # Old mtime
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Update versions tracking
        update_versions_tracking(data_file, versions_path)

        # Verify mtime was updated
        updated_versions = read_versions(versions_path)
        current_version = updated_versions.versions[-1]
        asset = current_version.assets.get("data.parquet")
        assert asset is not None
        assert asset.source_mtime != 1000.0  # Should be updated
        assert asset.source_mtime == data_file.stat().st_mtime

    @pytest.mark.unit
    def test_update_versions_tracking_file_not_found(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking raises FileNotFoundError for missing file."""
        file_path = tmp_path / "nonexistent.parquet"
        versions_path = tmp_path / "versions.json"

        with pytest.raises(FileNotFoundError):
            update_versions_tracking(file_path, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_versions_not_found(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking raises FileNotFoundError for missing versions.json."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)
        versions_path = tmp_path / "nonexistent_versions.json"

        with pytest.raises(FileNotFoundError):
            update_versions_tracking(data_file, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_asset_not_in_versions(self, tmp_path: Path) -> None:
        """Test update_versions_tracking when asset doesn't exist in versions.json."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "new_data.parquet"
        shutil.copy(source_file, data_file)

        # Create versions.json without this asset
        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "other.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="other.parquet",
                        )
                    },
                    changes=["other.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Should raise KeyError for asset not found
        with pytest.raises(KeyError):
            update_versions_tracking(data_file, versions_path)

    @pytest.mark.unit
    def test_update_versions_tracking_preserves_other_fields(self, tmp_path: Path) -> None:
        """Test that update_versions_tracking preserves other asset fields."""
        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = tmp_path / "data.parquet"
        shutil.copy(source_file, data_file)

        versions_path = tmp_path / "versions.json"
        versions_file = VersionsFile(
            spec_version="1.0.0",
            current_version="1.0.0",
            versions=[
                Version(
                    version="1.0.0",
                    created=datetime.now(timezone.utc),
                    breaking=False,
                    assets={
                        "data.parquet": Asset(
                            sha256="abc123",
                            size_bytes=1000,
                            href="data.parquet",
                            source_path="original.geojson",
                            source_mtime=1000.0,
                        )
                    },
                    changes=["data.parquet"],
                )
            ],
        )
        write_versions(versions_path, versions_file)

        # Update versions tracking
        update_versions_tracking(data_file, versions_path)

        # Verify other fields are preserved
        updated_versions = read_versions(versions_path)
        current_version = updated_versions.versions[-1]
        asset = current_version.assets.get("data.parquet")
        assert asset is not None
        assert asset.sha256 == "abc123"
        assert asset.size_bytes == 1000
        assert asset.source_path == "original.geojson"


class TestEdgeCases:
    """Edge case tests for update functions."""

    @pytest.mark.unit
    def test_update_item_metadata_cog_file(self, tmp_path: Path) -> None:
        """Test update_item_metadata with COG file."""
        # Use real COG fixture
        cog_file = REALDATA_FIXTURES / "rapidai4eo-sample.tif"
        if not cog_file.exists():
            pytest.skip("COG fixture not available")

        collection_dir = tmp_path / "cog-collection"
        collection_dir.mkdir()

        data_file = collection_dir / "image.tif"
        shutil.copy(cog_file, data_file)

        item = create_item(
            item_id="image",
            data_path=data_file,
            collection_id="cog-collection",
        )
        item_path = write_item_json(item, collection_dir)

        # Update should work for COG files too
        updated_item = update_item_metadata(item_path, data_file)
        assert updated_item.bbox is not None

    @pytest.mark.unit
    def test_create_missing_item_collection_json_missing(self, tmp_path: Path) -> None:
        """Test create_missing_item when collection.json doesn't exist."""
        collection_dir = tmp_path / "my-collection"
        collection_dir.mkdir()

        source_file = REALDATA_FIXTURES / "open-buildings.parquet"
        if not source_file.exists():
            pytest.skip("Test fixture not available")

        data_file = collection_dir / "data.parquet"
        shutil.copy(source_file, data_file)

        # Don't create collection.json
        # Should raise FileNotFoundError
        with pytest.raises(FileNotFoundError):
            create_missing_item(data_file, collection_dir)
