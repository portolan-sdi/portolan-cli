"""Unit tests for external / remote dataset registration.

Covers `portolan add-external` and the underlying
`portolan_cli.external.add_external_dataset`, which register a remote
cloud-native dataset as a catalog collection WITHOUT downloading or
converting it (referenced in place).

Motivating case: Overture Maps places — planet-scale GeoParquet on Overture's
own public S3.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli
from portolan_cli.external import (
    _validate_bbox,
    add_external_dataset,
    derive_collection_id_from_url,
    infer_media_type,
    is_external_href,
)
from portolan_cli.metadata.scan import scan_catalog_metadata
from portolan_cli.validation import InputValidationError

pytestmark = [pytest.mark.unit]

OVERTURE_URL = "s3://overturemaps-us-west-2/release/2024-09-18.0/theme=places/type=place/*"


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def setup_catalog(path: Path) -> None:
    """Create an initialized Portolan catalog (per ADR-0023 and ADR-0029)."""
    portolan_dir = path / ".portolan"
    portolan_dir.mkdir()
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    catalog_data = {
        "type": "Catalog",
        "stac_version": "1.0.0",
        "id": "portolan-catalog",
        "description": "A Portolan-managed STAC catalog",
        "links": [],
    }
    (path / "catalog.json").write_text(json.dumps(catalog_data, indent=2))


class TestHelpers:
    """Tests for the small inference/validation helpers."""

    def test_is_external_href(self) -> None:
        assert is_external_href("s3://bucket/key")
        assert is_external_href("https://example.org/x.parquet")
        assert not is_external_href("/tmp/local.parquet")
        assert not is_external_href("./relative/path.parquet")

    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://example.org/data/buildings.parquet", "application/vnd.apache.parquet"),
            ("s3://b/theme=x/type=y/*.parquet", "application/vnd.apache.parquet"),
            ("https://example.org/tiles/world.pmtiles", "application/vnd.pmtiles"),
            # Glob with no extension anywhere -> honest fallback.
            (OVERTURE_URL, "application/octet-stream"),
        ],
    )
    def test_infer_media_type(self, url: str, expected: str) -> None:
        assert infer_media_type(url) == expected

    def test_derive_collection_id_from_hive_segment(self) -> None:
        # Last meaningful segment is the Hive partition "type=place".
        assert derive_collection_id_from_url(OVERTURE_URL) == "place"

    def test_derive_collection_id_from_filename(self) -> None:
        assert (
            derive_collection_id_from_url("https://example.org/data/buildings.parquet")
            == "buildings"
        )

    def test_derive_collection_id_no_path_raises(self) -> None:
        """URLs with no meaningful path segments should error."""
        with pytest.raises(ValueError, match="Could not derive"):
            derive_collection_id_from_url("s3://bucket/")


class TestBboxValidation:
    """Tests for the _validate_bbox helper."""

    def test_valid_bbox(self) -> None:
        _validate_bbox([-180, -90, 180, 90])  # global extent
        _validate_bbox([-122.5, 37.5, -122.0, 38.0])  # SF area

    def test_bbox_wrong_count(self) -> None:
        with pytest.raises(ValueError, match="exactly 4 values"):
            _validate_bbox([1, 2, 3])

    def test_longitude_out_of_range(self) -> None:
        with pytest.raises(ValueError, match="Longitude"):
            _validate_bbox([-200, 0, 0, 0])

    def test_latitude_out_of_range(self) -> None:
        with pytest.raises(ValueError, match="Latitude"):
            _validate_bbox([0, -100, 0, 0])

    def test_min_greater_than_max_x(self) -> None:
        with pytest.raises(ValueError, match="min_x"):
            _validate_bbox([10, 0, 5, 10])  # min_x > max_x

    def test_min_greater_than_max_y(self) -> None:
        with pytest.raises(ValueError, match="min_y"):
            _validate_bbox([0, 10, 10, 5])  # min_y > max_y


class TestADR0030URLValidation:
    """Tests for ADR-0030 input validation compliance (F1, F2)."""

    def test_rejects_file_scheme(self, tmp_path: Path) -> None:
        """file:// URLs are local paths disguised as URIs — must reject."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="Unsupported URL scheme"):
            add_external_dataset(catalog_root=tmp_path, url="file:///etc/passwd", collection_id="x")

    def test_rejects_path_traversal(self, tmp_path: Path) -> None:
        """Path traversals in URLs must be rejected."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="traversal"):
            add_external_dataset(
                catalog_root=tmp_path, url="s3://bucket/../etc/passwd", collection_id="x"
            )

    def test_rejects_control_characters(self, tmp_path: Path) -> None:
        """Control characters in URLs must be rejected."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="Control characters"):
            add_external_dataset(
                catalog_root=tmp_path, url="s3://bucket/key\x00.parquet", collection_id="x"
            )

    def test_rejects_empty_host(self, tmp_path: Path) -> None:
        """URLs without host/bucket must be rejected."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="missing host"):
            add_external_dataset(catalog_root=tmp_path, url="s3://", collection_id="x")

    def test_rejects_unsupported_scheme(self, tmp_path: Path) -> None:
        """Only s3, gs, az, http, https are allowed."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="Unsupported URL scheme"):
            add_external_dataset(
                catalog_root=tmp_path, url="ftp://example.org/data.parquet", collection_id="x"
            )

    def test_accepts_valid_s3_url(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)
        result = add_external_dataset(
            catalog_root=tmp_path,
            url="s3://bucket/path/to/data.parquet",
            collection_id="test",
        )
        assert result.collection_id == "test"

    def test_accepts_valid_https_url(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)
        result = add_external_dataset(
            catalog_root=tmp_path,
            url="https://example.org/data.parquet",
            collection_id="test",
        )
        assert result.collection_id == "test"

    def test_accepts_valid_gs_url(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)
        result = add_external_dataset(
            catalog_root=tmp_path,
            url="gs://bucket/path/data.parquet",
            collection_id="test",
        )
        assert result.collection_id == "test"

    def test_accepts_valid_az_url(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)
        result = add_external_dataset(
            catalog_root=tmp_path,
            url="az://container/blob.parquet",
            collection_id="test",
        )
        assert result.collection_id == "test"


class TestOverwriteProtection:
    """Tests for collection overwrite protection (F3)."""

    def test_rejects_overwrite_without_force(self, tmp_path: Path) -> None:
        """Existing collections must not be overwritten without --force."""
        setup_catalog(tmp_path)
        add_external_dataset(catalog_root=tmp_path, url=OVERTURE_URL, collection_id="test")
        with pytest.raises(FileExistsError, match="already exists"):
            add_external_dataset(catalog_root=tmp_path, url=OVERTURE_URL, collection_id="test")

    def test_allows_overwrite_with_force(self, tmp_path: Path) -> None:
        """With force=True, existing collections can be overwritten."""
        setup_catalog(tmp_path)
        add_external_dataset(
            catalog_root=tmp_path,
            url=OVERTURE_URL,
            collection_id="test",
            title="Original",
        )
        result = add_external_dataset(
            catalog_root=tmp_path,
            url=OVERTURE_URL,
            collection_id="test",
            title="Updated",
            force=True,
        )
        assert result.collection_id == "test"
        data = json.loads(result.collection_path.read_text())
        assert data["title"] == "Updated"


class TestAddExternalDataset:
    """Tests for the add_external_dataset core function."""

    def test_creates_collection_with_remote_asset(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)

        result = add_external_dataset(
            catalog_root=tmp_path,
            url=OVERTURE_URL,
            collection_id="overture-places",
            title="Overture Maps — Places",
            media_type="application/vnd.apache.parquet",
            via_url="https://docs.overturemaps.org/guides/places/",
        )

        assert result.collection_id == "overture-places"
        assert result.href == OVERTURE_URL
        collection_path = tmp_path / "overture-places" / "collection.json"
        assert collection_path == result.collection_path
        assert collection_path.exists()

        data = json.loads(collection_path.read_text())

        # data asset points at the remote URL, kept as-is, marked external.
        asset = data["assets"]["data"]
        assert asset["href"] == OVERTURE_URL
        assert asset["type"] == "application/vnd.apache.parquet"
        assert asset["portolan:managed"] is False
        assert "external" in asset["roles"]
        assert "data" in asset["roles"]

        # rel:via provenance link present.
        via_links = [link for link in data["links"] if link["rel"] == "via"]
        assert len(via_links) == 1
        assert via_links[0]["href"] == "https://docs.overturemaps.org/guides/places/"

    def test_no_local_file_written(self, tmp_path: Path) -> None:
        """Nothing is downloaded: only metadata JSON exists in the collection."""
        setup_catalog(tmp_path)
        add_external_dataset(
            catalog_root=tmp_path, url=OVERTURE_URL, collection_id="overture-places"
        )
        collection_dir = tmp_path / "overture-places"
        files = sorted(p.name for p in collection_dir.iterdir())
        assert files == ["collection.json"]

    def test_links_collection_into_root_catalog(self, tmp_path: Path) -> None:
        setup_catalog(tmp_path)
        add_external_dataset(
            catalog_root=tmp_path, url=OVERTURE_URL, collection_id="overture-places"
        )
        catalog = json.loads((tmp_path / "catalog.json").read_text())
        child_hrefs = [link["href"] for link in catalog["links"] if link["rel"] == "child"]
        assert "./overture-places/collection.json" in child_hrefs

    def test_check_scanner_does_not_flag_remote_asset(self, tmp_path: Path) -> None:
        """The metadata scanner must not report the remote asset as MISSING."""
        setup_catalog(tmp_path)
        add_external_dataset(
            catalog_root=tmp_path, url=OVERTURE_URL, collection_id="overture-places"
        )
        report = scan_catalog_metadata(tmp_path)
        # No result should reference the external href as a problem.
        assert all(OVERTURE_URL not in str(r.file_path) for r in report.results)

    def test_rejects_local_path(self, tmp_path: Path) -> None:
        """Local filesystem paths must be rejected (use 'portolan add' instead)."""
        setup_catalog(tmp_path)
        with pytest.raises(InputValidationError, match="Unsupported URL scheme"):
            add_external_dataset(catalog_root=tmp_path, url="/tmp/local.parquet", collection_id="x")

    def test_rejects_uninitialised_catalog(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Not a Portolan catalog"):
            add_external_dataset(catalog_root=tmp_path, url=OVERTURE_URL, collection_id="x")


class TestAddExternalCommand:
    """Tests for the `portolan add-external` CLI command."""

    def test_command_registers_external_collection(self, runner: CliRunner) -> None:
        with runner.isolated_filesystem():
            catalog = Path.cwd()
            setup_catalog(catalog)
            result = runner.invoke(
                cli,
                [
                    "add-external",
                    OVERTURE_URL,
                    "--collection",
                    "overture-places",
                    "--media-type",
                    "application/vnd.apache.parquet",
                    "--via",
                    "https://docs.overturemaps.org/guides/places/",
                ],
            )
            assert result.exit_code == 0, result.output
            assert (catalog / "overture-places" / "collection.json").exists()

    def test_command_json_output(self, runner: CliRunner) -> None:
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(
                cli,
                ["add-external", OVERTURE_URL, "--collection", "overture-places", "--json"],
            )
            assert result.exit_code == 0, result.output
            payload = json.loads(result.output)
            assert payload["success"] is True
            assert payload["data"]["collection_id"] == "overture-places"
            assert payload["data"]["managed"] is False
            assert payload["data"]["href"] == OVERTURE_URL

    def test_command_rejects_local_path(self, runner: CliRunner) -> None:
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(cli, ["add-external", "/tmp/local.parquet", "--collection", "x"])
            assert result.exit_code == 1
            assert "Unsupported URL scheme" in result.output

    def test_command_rejects_file_scheme(self, runner: CliRunner) -> None:
        """file:// URLs must be rejected via CLI."""
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(cli, ["add-external", "file:///etc/passwd", "--collection", "x"])
            assert result.exit_code == 1
            assert "Unsupported URL scheme" in result.output

    def test_command_rejects_path_traversal(self, runner: CliRunner) -> None:
        """Path traversals in URLs must be rejected via CLI."""
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(
                cli, ["add-external", "s3://bucket/../etc/passwd", "--collection", "x"]
            )
            assert result.exit_code == 1
            assert "traversal" in result.output

    def test_command_with_force_flag(self, runner: CliRunner) -> None:
        """--force allows overwriting existing collections."""
        with runner.isolated_filesystem():
            catalog = Path.cwd()
            setup_catalog(catalog)
            # Create initial collection
            runner.invoke(cli, ["add-external", OVERTURE_URL, "--collection", "test"])
            # Without --force, should fail
            result = runner.invoke(cli, ["add-external", OVERTURE_URL, "--collection", "test"])
            assert result.exit_code == 1
            assert "already exists" in result.output
            # With --force, should succeed
            result = runner.invoke(
                cli, ["add-external", OVERTURE_URL, "--collection", "test", "--force"]
            )
            assert result.exit_code == 0

    def test_command_with_bbox(self, runner: CliRunner) -> None:
        """--bbox option sets spatial extent."""
        with runner.isolated_filesystem():
            catalog = Path.cwd()
            setup_catalog(catalog)
            result = runner.invoke(
                cli,
                [
                    "add-external",
                    OVERTURE_URL,
                    "--collection",
                    "test",
                    "--bbox",
                    "-122.5,37.5,-122.0,38.0",
                ],
            )
            assert result.exit_code == 0, result.output
            data = json.loads((catalog / "test" / "collection.json").read_text())
            assert data["extent"]["spatial"]["bbox"] == [[-122.5, 37.5, -122.0, 38.0]]

    def test_command_with_invalid_bbox(self, runner: CliRunner) -> None:
        """Invalid --bbox values must be rejected."""
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(
                cli,
                ["add-external", OVERTURE_URL, "--collection", "test", "--bbox", "-200,0,0,0"],
            )
            assert result.exit_code == 1
            assert "Longitude" in result.output

    def test_command_with_license(self, runner: CliRunner) -> None:
        """--license option sets SPDX license identifier."""
        with runner.isolated_filesystem():
            catalog = Path.cwd()
            setup_catalog(catalog)
            result = runner.invoke(
                cli,
                ["add-external", OVERTURE_URL, "--collection", "test", "--license", "CC-BY-4.0"],
            )
            assert result.exit_code == 0, result.output
            data = json.loads((catalog / "test" / "collection.json").read_text())
            assert data["license"] == "CC-BY-4.0"

    def test_command_with_description(self, runner: CliRunner) -> None:
        """--description option sets collection description."""
        with runner.isolated_filesystem():
            catalog = Path.cwd()
            setup_catalog(catalog)
            result = runner.invoke(
                cli,
                [
                    "add-external",
                    OVERTURE_URL,
                    "--collection",
                    "test",
                    "--description",
                    "Custom description",
                ],
            )
            assert result.exit_code == 0, result.output
            data = json.loads((catalog / "test" / "collection.json").read_text())
            assert data["description"] == "Custom description"

    def test_command_global_format_json(self, runner: CliRunner) -> None:
        """Global --format=json option works."""
        with runner.isolated_filesystem():
            setup_catalog(Path.cwd())
            result = runner.invoke(
                cli,
                ["--format", "json", "add-external", OVERTURE_URL, "--collection", "test"],
            )
            assert result.exit_code == 0, result.output
            payload = json.loads(result.output)
            assert payload["success"] is True
            assert payload["data"]["collection_id"] == "test"
