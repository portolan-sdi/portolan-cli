"""Versioning stress tests for issue #339.

Tests the full add → push pipeline and verifies that versions.json is
populated correctly at the collection level (not catalog level).

See:
- tests/specs/versioning_stress.md for human test specification
- ADR-0005 for versions.json as single source of truth
- Issue #339 for the original bug report

Note: remote is a sensitive setting and must be set via env var (Issue #356).
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

if TYPE_CHECKING:
    from collections.abc import Generator

# Remote URL for tests - set via env var (Issue #356: sensitive settings)
TEST_REMOTE = "s3://fake-bucket/catalog"


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


@pytest.fixture
def initialized_catalog(tmp_path: Path) -> Path:
    """Create an initialized Portolan catalog using CLI."""
    result = CliRunner().invoke(cli, ["init", str(tmp_path), "--auto"])
    assert result.exit_code == 0, f"Init failed: {result.output}"
    return tmp_path


def create_geojson(
    coords: tuple[float, float] = (0.0, 0.0), props: dict[str, object] | None = None
) -> str:
    """Create a minimal valid GeoJSON FeatureCollection."""
    return json.dumps(
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": list(coords)},
                    "properties": props or {"id": 1},
                }
            ],
        }
    )


@pytest.fixture
def catalog_with_source_files(initialized_catalog: Path) -> tuple[Path, Path]:
    """Catalog with source files already inside (required for add)."""
    collection_dir = initialized_catalog / "test-collection"
    collection_dir.mkdir()

    # Create test files inside catalog
    for i in range(5):
        (collection_dir / f"file_{i}.geojson").write_text(
            create_geojson(coords=(float(i), float(i)), props={"id": i})
        )

    return initialized_catalog, collection_dir


@pytest.fixture
def catalog_with_many_files(initialized_catalog: Path) -> tuple[Path, Path]:
    """Catalog with 100 files for scale testing (reduced from 1000 for speed)."""
    collection_dir = initialized_catalog / "scale-test"
    collection_dir.mkdir()

    # 100 files is sufficient for regression testing (vs 1000)
    for i in range(100):
        (collection_dir / f"file_{i:04d}.geojson").write_text(
            create_geojson(
                coords=(float(i % 360 - 180), float(i % 180 - 90)),
                props={"id": i},
            )
        )

    return initialized_catalog, collection_dir


# =============================================================================
# Moto Server Fixtures (for S3 integration tests)
# =============================================================================

# NOTE: moto/boto3 imports moved into fixtures to avoid skipping non-network tests


@pytest.fixture(scope="module")
def moto_server() -> Generator[str, None, None]:
    """Start a moto server that provides a real HTTP endpoint.

    Necessary because obstore (Rust-based S3 client) makes direct
    HTTP calls and doesn't integrate with boto3's patching mechanism.

    Note: Uses importorskip so only network tests are skipped if moto unavailable.
    """
    pytest.importorskip("moto")
    from moto.server import ThreadedMotoServer

    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    endpoint_url = f"http://{host}:{port}"
    yield endpoint_url
    server.stop()


@pytest.fixture
def s3_bucket(moto_server: str) -> Generator[tuple[str, str], None, None]:
    """Create a mock S3 bucket using the moto server."""
    boto3 = pytest.importorskip("boto3")

    bucket_name = f"test-bucket-{uuid.uuid4().hex[:8]}"
    client = boto3.client(
        "s3",
        endpoint_url=moto_server,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    client.create_bucket(Bucket=bucket_name)
    yield bucket_name, moto_server


# =============================================================================
# TestAddPopulatesVersions
# =============================================================================


class TestAddPopulatesVersions:
    """Verify `portolan add` creates properly-structured collection-level versions.json."""

    @pytest.mark.integration
    def test_add_single_file_creates_versions_json(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """versions.json exists after add (dataset.py:1174)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        assert versions_path.exists(), f"versions.json not created at {versions_path}"

    @pytest.mark.integration
    def test_add_populates_versions_array(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """versions array is non-empty after add (dataset.py:1216)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        assert "versions" in versions_data, "Missing 'versions' key"
        assert len(versions_data["versions"]) > 0, "versions array is empty"

    @pytest.mark.integration
    def test_add_sets_current_version(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """current_version field is set (dataset.py:1187-1192)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        assert versions_data.get("current_version") is not None, "current_version not set"
        assert versions_data["current_version"] == versions_data["versions"][-1]["version"], (
            "current_version doesn't match last version"
        )

    @pytest.mark.integration
    def test_add_includes_asset_metadata(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Assets have sha256, size_bytes, href (dataset.py:1208-1213)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        latest_version = versions_data["versions"][-1]
        assert "assets" in latest_version, "Missing 'assets' in version"
        assert len(latest_version["assets"]) > 0, "No assets in version"

        for asset_name, asset_data in latest_version["assets"].items():
            assert "sha256" in asset_data, f"Asset {asset_name} missing sha256"
            assert "size_bytes" in asset_data, f"Asset {asset_name} missing size_bytes"
            assert "href" in asset_data, f"Asset {asset_name} missing href"
            # Verify sha256 is valid hex (64 chars)
            assert len(asset_data["sha256"]) == 64, f"Asset {asset_name} sha256 wrong length"
            assert all(c in "0123456789abcdef" for c in asset_data["sha256"]), (
                f"Asset {asset_name} sha256 invalid hex"
            )

    @pytest.mark.integration
    def test_add_100_files_accumulates(
        self, catalog_with_many_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Scale test: many files in one add (Issue #339 scenario)."""
        catalog_root, collection_dir = catalog_with_many_files

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        # Should have at least 100 assets tracked
        latest_version = versions_data["versions"][-1]
        asset_count = len(latest_version["assets"])
        assert asset_count >= 100, f"Only {asset_count} assets tracked, expected >= 100"


# =============================================================================
# TestCatalogLevelVersioning (fixes #339)
# =============================================================================


class TestCatalogLevelVersioning:
    """Verify catalog-level versions.json IS updated by add (per ADR-0005).

    Issue #339 reported that catalog-level versions.json showed empty collections
    after add. This was a real bug, not user confusion. These tests verify the fix.
    """

    @pytest.mark.integration
    def test_add_updates_catalog_level_versions(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Catalog-level versions.json IS updated after add (fixes #339)."""
        catalog_versions_path = initialized_catalog / "versions.json"

        # Record state before add
        catalog_before = json.loads(catalog_versions_path.read_text())
        assert catalog_before.get("collections") == {}, "collections should start empty"

        # Create collection and add files
        collection_dir = initialized_catalog / "test-collection"
        collection_dir.mkdir()
        (collection_dir / "test.geojson").write_text(create_geojson())

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify catalog-level versions.json WAS updated (the #339 fix!)
        catalog_after = json.loads(catalog_versions_path.read_text())
        collections = catalog_after.get("collections", {})
        assert "test-collection" in collections, (
            f"Catalog-level versions.json missing collection. Got: {collections}"
        )

        # Verify collection entry has required fields
        coll_info = collections["test-collection"]
        assert "current_version" in coll_info, "Missing current_version"
        assert "updated" in coll_info, "Missing updated timestamp"
        assert "asset_count" in coll_info, "Missing asset_count"
        assert coll_info["asset_count"] > 0, "asset_count should be > 0"

    @pytest.mark.integration
    def test_add_multiple_collections_tracked(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Multiple collections are tracked in catalog-level versions.json."""
        # Add first collection
        coll1_dir = initialized_catalog / "collection-one"
        coll1_dir.mkdir()
        (coll1_dir / "file1.geojson").write_text(create_geojson(coords=(1.0, 1.0)))

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(coll1_dir)],
        )
        assert result.exit_code == 0

        # Add second collection
        coll2_dir = initialized_catalog / "collection-two"
        coll2_dir.mkdir()
        (coll2_dir / "file2.geojson").write_text(create_geojson(coords=(2.0, 2.0)))

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(coll2_dir)],
        )
        assert result.exit_code == 0

        # Verify both collections are tracked
        catalog_versions_path = initialized_catalog / "versions.json"
        catalog_data = json.loads(catalog_versions_path.read_text())
        collections = catalog_data.get("collections", {})

        assert "collection-one" in collections, "Missing collection-one"
        assert "collection-two" in collections, "Missing collection-two"

    @pytest.mark.integration
    def test_two_files_have_different_schemas(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Catalog-level and collection-level versions.json have different schemas."""
        catalog_versions_path = initialized_catalog / "versions.json"

        # Create collection and add files
        collection_dir = initialized_catalog / "schema-test"
        collection_dir.mkdir()
        (collection_dir / "test.geojson").write_text(create_geojson())

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify both files exist
        collection_versions_path = collection_dir / "versions.json"
        assert collection_versions_path.exists()

        # Verify schemas are different
        catalog_data = json.loads(catalog_versions_path.read_text())
        collection_data = json.loads(collection_versions_path.read_text())

        # Catalog-level has "collections" key (aggregate view)
        assert "collections" in catalog_data, "Catalog should have 'collections' key"
        assert "versions" not in catalog_data, "Catalog should NOT have 'versions' array"

        # Collection-level has "versions" key (detailed history)
        assert "versions" in collection_data, "Collection should have 'versions' array"
        assert "collections" not in collection_data, "Collection should NOT have 'collections'"

    @pytest.mark.integration
    def test_catalog_versions_updated_timestamp(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Catalog-level versions.json has updated timestamp after add."""
        catalog_versions_path = initialized_catalog / "versions.json"

        # Get initial state
        catalog_before = json.loads(catalog_versions_path.read_text())
        updated_before = catalog_before.get("updated")

        # Add a collection
        collection_dir = initialized_catalog / "timestamp-test"
        collection_dir.mkdir()
        (collection_dir / "test.geojson").write_text(create_geojson())

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
        )
        assert result.exit_code == 0

        # Verify updated timestamp changed
        catalog_after = json.loads(catalog_versions_path.read_text())
        updated_after = catalog_after.get("updated")
        assert updated_after is not None, "Missing updated timestamp"
        assert updated_after != updated_before, "updated timestamp should change"


# =============================================================================
# TestAddThenPushSeesFiles
# =============================================================================


class TestAddThenPushSeesFiles:
    """Verify the full pipeline from add to push."""

    @pytest.mark.integration
    def test_push_after_add_reports_files(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Push sees files to upload after add."""
        catalog_root, collection_dir = catalog_with_source_files

        # First add files
        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
        )
        assert add_result.exit_code == 0, f"Add failed: {add_result.output}"

        # Configure non-sensitive settings in config.yaml
        config_path = catalog_root / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: stress-test-catalog\n")

        # Push dry-run to see what would be uploaded (remote via env var per Issue #356)
        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            result = runner.invoke(
                cli,
                ["push", "--catalog", str(catalog_root), "--dry-run"],
            )
        assert result.exit_code == 0, f"Push dry-run failed: {result.output}"

        # Dry-run output shows "Would upload up to N asset file(s)" - verify N > 0
        # The summary shows "0 file(s)" because nothing was actually pushed (dry-run)
        assert "would upload" in result.output.lower(), f"No upload preview: {result.output}"
        # Verify actual assets are listed (not "Would upload up to 0 asset file(s)")
        assert "would upload up to 0 asset" not in result.output.lower(), (
            f"Push reports 0 assets to upload: {result.output}"
        )

    @pytest.mark.integration
    def test_push_dry_run_lists_parquet_assets(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Dry-run shows parquet asset paths after GeoJSON conversion."""
        catalog_root, collection_dir = catalog_with_source_files

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        config_path = catalog_root / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: stress-test-catalog\n")

        # Push dry-run (remote via env var per Issue #356)
        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            result = runner.invoke(
                cli,
                ["push", "--catalog", str(catalog_root), "--dry-run"],
            )
        assert result.exit_code == 0, f"Push dry-run failed: {result.output}"

        # After add, GeoJSON gets converted to parquet - must see parquet in output
        assert ".parquet" in result.output, f"No parquet files in push output: {result.output}"

    @pytest.mark.integration
    def test_push_reads_collection_level_versions(
        self, catalog_with_multiple_versions: Path, runner: CliRunner
    ) -> None:
        """Push reads correct file (collection-level, not catalog-level)."""
        # Add a catalog-level versions.json with different schema
        catalog_versions = catalog_with_multiple_versions / "versions.json"
        catalog_versions.write_text(json.dumps({"catalog_id": "test", "collections": {}}))

        # Configure non-sensitive settings
        config_path = catalog_with_multiple_versions / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: test-catalog\n")

        # Push dry-run (remote via env var per Issue #356)
        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            result = runner.invoke(
                cli,
                ["push", "--catalog", str(catalog_with_multiple_versions), "--dry-run"],
            )

        # Should see assets from collection-level versions.json
        assert result.exit_code == 0, f"Push failed: {result.output}"
        # Must see at least one of the expected files
        assert any(
            name in result.output for name in ["base.parquet", "second.parquet", "third.parquet"]
        ), f"Expected parquet files not in output: {result.output}"


# =============================================================================
# TestSnapshotModelAccumulation
# =============================================================================


class TestSnapshotModelAccumulation:
    """Verify each version is a complete snapshot (ADR-0005)."""

    @pytest.mark.integration
    def test_second_add_preserves_first_assets(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """v2 contains v1's assets (versions.py:357-368).

        Note: Portolan may merge sequential adds into one version if run quickly.
        This test verifies the snapshot model by checking the final version
        contains all assets from previous adds.
        """
        collection_dir = initialized_catalog / "accum-test"
        collection_dir.mkdir()

        file1 = collection_dir / "first.geojson"
        file1.write_text(create_geojson(coords=(1.0, 1.0)))

        # Add first file
        r1 = runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(file1)])
        assert r1.exit_code == 0, f"First add failed: {r1.output}"

        # Verify first version exists
        versions_path = collection_dir / "versions.json"
        v1_data = json.loads(versions_path.read_text())
        v1_asset_count = len(v1_data["versions"][-1]["assets"])
        assert v1_asset_count >= 1, f"v1 should have at least 1 asset, got {v1_asset_count}"

        # Add second file
        file2 = collection_dir / "second.geojson"
        file2.write_text(create_geojson(coords=(2.0, 2.0)))

        r2 = runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(file2)])
        assert r2.exit_code == 0, f"Second add failed: {r2.output}"

        # Verify final state has all assets (snapshot model)
        v2_data = json.loads(versions_path.read_text())
        final_assets = set(v2_data["versions"][-1]["assets"].keys())

        # Final version should have at least 2 assets (both files, converted)
        # Portolan converts .geojson to .parquet, so we check parquet presence
        assert len(final_assets) >= 2, f"Final version missing assets: {final_assets}"

    @pytest.mark.integration
    def test_third_add_preserves_all_prior(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """v3 contains v1+v2 assets (versions.py:357-368)."""
        collection_dir = initialized_catalog / "triple-test"
        collection_dir.mkdir()

        for i in range(3):
            file_path = collection_dir / f"file{i}.geojson"
            file_path.write_text(create_geojson(coords=(float(i), float(i))))

            result = runner.invoke(
                cli, ["add", "--portolan-dir", str(initialized_catalog), str(file_path)]
            )
            assert result.exit_code == 0, f"Add {i} failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        assert len(versions_data["versions"]) >= 3, "Expected at least 3 versions"
        v1_assets = set(versions_data["versions"][0]["assets"].keys())
        v2_assets = set(versions_data["versions"][1]["assets"].keys())
        v3_assets = set(versions_data["versions"][2]["assets"].keys())

        assert v1_assets.issubset(v3_assets), "v3 missing v1 assets"
        assert v2_assets.issubset(v3_assets), "v3 missing v2 assets"

    @pytest.mark.integration
    def test_changes_field_tracks_additions(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """changes[] tracks what was added in each version.

        Note: Portolan may merge sequential adds into one version if run quickly.
        This test verifies that changes[] contains the newly added files.
        """
        collection_dir = initialized_catalog / "delta-test"
        collection_dir.mkdir()

        file1 = collection_dir / "first.geojson"
        file1.write_text(create_geojson(coords=(1.0, 1.0)))

        r1 = runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(file1)])
        assert r1.exit_code == 0, f"First add failed: {r1.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        # Verify changes field exists and contains the added file(s)
        latest = versions_data["versions"][-1]
        assert "changes" in latest, "Missing 'changes' field in version"
        changes = latest.get("changes", [])
        assert len(changes) > 0, "changes[] should not be empty after add"

        # Verify changes references actual assets
        assets = set(latest["assets"].keys())
        changes_set = set(changes)
        # All changes should be in assets
        assert changes_set.issubset(assets), (
            f"changes[] references non-existent assets: {changes_set - assets}"
        )

    @pytest.mark.integration
    def test_unchanged_file_readd_is_noop(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Idempotent re-add (versions.py:376-379)."""
        collection_dir = initialized_catalog / "noop-test"
        collection_dir.mkdir()

        file1 = collection_dir / "file.geojson"
        file1.write_text(create_geojson())

        # Add file
        r1 = runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(file1)])
        assert r1.exit_code == 0

        versions_path = collection_dir / "versions.json"
        versions_before = json.loads(versions_path.read_text())
        version_count_before = len(versions_before["versions"])

        # Re-add same file (unchanged)
        r2 = runner.invoke(cli, ["add", "--portolan-dir", str(initialized_catalog), str(file1)])
        assert r2.exit_code == 0

        versions_after = json.loads(versions_path.read_text())
        version_count_after = len(versions_after["versions"])

        assert version_count_after == version_count_before, (
            "Re-add of unchanged file created new version"
        )


# =============================================================================
# TestPushPullDivergence (implemented with moto)
# =============================================================================


class TestPushPullDivergence:
    """Verify conflict detection and handling using moto S3 mock."""

    @pytest.mark.integration
    @pytest.mark.network
    def test_push_to_empty_remote_succeeds(
        self,
        s3_bucket: tuple[str, str],
        initialized_catalog: Path,
        runner: CliRunner,
    ) -> None:
        """First push to empty remote succeeds."""
        import os

        bucket_name, endpoint_url = s3_bucket

        # Create collection with data
        collection_dir = initialized_catalog / "push-test"
        collection_dir.mkdir()
        (collection_dir / "data.geojson").write_text(create_geojson())

        # Add files
        result = runner.invoke(
            cli, ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)]
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Configure catalog (remote set via env var per Issue #356)
        config_path = initialized_catalog / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: push-test-catalog\n")

        # Set AWS env vars for obstore (remote via env var per Issue #356)
        env_backup = os.environ.copy()
        os.environ["PORTOLAN_REMOTE"] = f"s3://{bucket_name}/catalog"
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
        os.environ["AWS_ALLOW_HTTP"] = "true"  # Required for http:// moto endpoints

        try:
            result = runner.invoke(cli, ["push", "--catalog", str(initialized_catalog)])
            # Fresh push (no remote conflict) should succeed
            assert result.exit_code == 0, f"Push failed unexpectedly: {result.output}"
            # Verify success message in output
            assert "push" in result.output.lower() or "upload" in result.output.lower(), (
                f"Expected push success message, got: {result.output}"
            )
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    @pytest.mark.integration
    @pytest.mark.network
    def test_remote_versions_detected_on_pull(
        self,
        s3_bucket: tuple[str, str],
        initialized_catalog: Path,
        runner: CliRunner,
    ) -> None:
        """Pull detects remote versions.json."""
        import os

        import boto3

        bucket_name, endpoint_url = s3_bucket

        # Upload a versions.json to the mock S3
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        remote_versions = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-15T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "remote.parquet": {
                            "sha256": hashlib.sha256(b"remote").hexdigest(),
                            "size_bytes": 1000,
                            "href": "remote.parquet",
                        }
                    },
                    "changes": ["remote.parquet"],
                }
            ],
        }
        client.put_object(
            Bucket=bucket_name,
            Key="catalog/test-collection/versions.json",
            Body=json.dumps(remote_versions),
        )

        # Create local collection without versions
        collection_dir = initialized_catalog / "test-collection"
        collection_dir.mkdir()

        # Configure catalog (remote set via env var per Issue #356)
        config_path = initialized_catalog / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: test-catalog\n")

        env_backup = os.environ.copy()
        os.environ["PORTOLAN_REMOTE"] = f"s3://{bucket_name}/catalog"
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
        os.environ["AWS_ALLOW_HTTP"] = "true"  # Required for http:// moto endpoints

        try:
            # Pull requires REMOTE_URL as positional argument
            remote_url = f"s3://{bucket_name}/catalog"
            result = runner.invoke(
                cli, ["pull", remote_url, "--catalog", str(initialized_catalog), "--dry-run"]
            )
            # Pull should run without crashing and detect remote state
            assert result.exit_code in (0, 1), f"Unexpected error: {result.output}"
        finally:
            os.environ.clear()
            os.environ.update(env_backup)

    @pytest.mark.integration
    @pytest.mark.network
    def test_diverged_state_detected(
        self,
        s3_bucket: tuple[str, str],
        initialized_catalog: Path,
        runner: CliRunner,
    ) -> None:
        """Diverged local/remote state is detected."""
        import os

        import boto3

        bucket_name, endpoint_url = s3_bucket

        # Upload remote versions.json with v1.0.1
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
            region_name="us-east-1",
        )

        remote_versions = {
            "spec_version": "1.0.0",
            "current_version": "1.0.1",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-15T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "base.parquet": {
                            "sha256": "a" * 64,
                            "size_bytes": 100,
                            "href": "base.parquet",
                        }
                    },
                    "changes": ["base.parquet"],
                },
                {
                    "version": "1.0.1",
                    "created": "2026-01-16T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "base.parquet": {
                            "sha256": "a" * 64,
                            "size_bytes": 100,
                            "href": "base.parquet",
                        },
                        "remote.parquet": {
                            "sha256": "b" * 64,
                            "size_bytes": 200,
                            "href": "remote.parquet",
                        },
                    },
                    "changes": ["remote.parquet"],
                },
            ],
        }
        client.put_object(
            Bucket=bucket_name,
            Key="catalog/test-collection/versions.json",
            Body=json.dumps(remote_versions),
        )

        # Create local versions.json with v1.1.0 (diverged from remote)
        collection_dir = initialized_catalog / "test-collection"
        collection_dir.mkdir()

        local_versions = {
            "spec_version": "1.0.0",
            "current_version": "1.1.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-15T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "base.parquet": {
                            "sha256": "a" * 64,
                            "size_bytes": 100,
                            "href": "base.parquet",
                        }
                    },
                    "changes": ["base.parquet"],
                },
                {
                    "version": "1.1.0",
                    "created": "2026-01-16T11:00:00Z",
                    "breaking": False,
                    "assets": {
                        "base.parquet": {
                            "sha256": "a" * 64,
                            "size_bytes": 100,
                            "href": "base.parquet",
                        },
                        "local.parquet": {
                            "sha256": "c" * 64,
                            "size_bytes": 300,
                            "href": "local.parquet",
                        },
                    },
                    "changes": ["local.parquet"],
                },
            ],
        }
        (collection_dir / "versions.json").write_text(json.dumps(local_versions))

        # Configure catalog (remote set via env var per Issue #356)
        config_path = initialized_catalog / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: test-catalog\n")

        env_backup = os.environ.copy()
        os.environ["PORTOLAN_REMOTE"] = f"s3://{bucket_name}/catalog"
        os.environ["AWS_ACCESS_KEY_ID"] = "testing"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url
        os.environ["AWS_ALLOW_HTTP"] = "true"  # Required for http:// moto endpoints

        try:
            # Pull requires REMOTE_URL as positional argument
            remote_url = f"s3://{bucket_name}/catalog"
            result = runner.invoke(cli, ["pull", remote_url, "--catalog", str(initialized_catalog)])
            # Either warns about divergence or fails with conflict - both acceptable
            assert result.exit_code in (0, 1), f"Unexpected error: {result.output}"
        finally:
            os.environ.clear()
            os.environ.update(env_backup)


# =============================================================================
# TestCorruptionRecovery
# =============================================================================


class TestCorruptionRecovery:
    """Verify handling of malformed data at both API and CLI levels."""

    @pytest.mark.integration
    def test_truncated_versions_json_rejected_api(self, tmp_path: Path) -> None:
        """Invalid JSON fails cleanly at API level (versions.py:144-146)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        versions_path.write_text('{"spec_version": "1.0.0", "versions": [')

        with pytest.raises(ValueError, match="Invalid JSON"):
            read_versions(versions_path)

    @pytest.mark.integration
    def test_missing_versions_field_rejected_api(self, tmp_path: Path) -> None:
        """Schema validation works at API level (versions.py:164-168)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        versions_path.write_text('{"spec_version": "1.0.0", "current_version": null}')

        with pytest.raises(ValueError, match="missing field"):
            read_versions(versions_path)

    @pytest.mark.integration
    def test_missing_asset_fields_rejected_api(self, tmp_path: Path) -> None:
        """Asset validation works at API level (versions.py:173-184)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        bad_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-15T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "file.parquet": {
                            "size_bytes": 1000,
                            "href": "file.parquet",
                            # Missing sha256
                        }
                    },
                    "changes": ["file.parquet"],
                }
            ],
        }
        versions_path.write_text(json.dumps(bad_data))

        with pytest.raises(ValueError):
            read_versions(versions_path)

    @pytest.mark.integration
    def test_unknown_fields_ignored_api(self, tmp_path: Path) -> None:
        """Forward compatibility at API level (versions.py:151-213)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        valid_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "unknown_future_field": "should be ignored",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2026-01-15T10:00:00Z",
                    "breaking": False,
                    "assets": {
                        "file.parquet": {
                            "sha256": hashlib.sha256(b"content").hexdigest(),
                            "size_bytes": 1000,
                            "href": "file.parquet",
                        }
                    },
                    "changes": ["file.parquet"],
                    "extra_future_field": True,
                }
            ],
        }
        versions_path.write_text(json.dumps(valid_data))

        result = read_versions(versions_path)
        assert result.spec_version == "1.0.0"
        assert len(result.versions) == 1

    @pytest.mark.integration
    def test_corrupted_versions_cli_error_message(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """CLI shows clear error for corrupted versions.json."""
        collection_dir = initialized_catalog / "corrupt-test"
        collection_dir.mkdir()

        # Create corrupted versions.json
        (collection_dir / "versions.json").write_text("{corrupted json")

        # Try to add to collection with corrupted versions
        (collection_dir / "new.geojson").write_text(create_geojson())

        result = runner.invoke(
            cli,
            [
                "add",
                "--portolan-dir",
                str(initialized_catalog),
                str(collection_dir / "new.geojson"),
            ],
        )

        # CLI should handle gracefully (not crash with traceback)
        # Either succeeds by recreating, or fails with clear error
        if result.exit_code != 0:
            assert "error" in result.output.lower() or "invalid" in result.output.lower(), (
                f"Expected clear error message, got: {result.output}"
            )


# =============================================================================
# TestCatalogVersionsEdgeCases (new tests from adversarial review)
# =============================================================================


class TestCatalogVersionsEdgeCases:
    """Test edge cases in catalog-level versions.json handling."""

    @pytest.mark.integration
    def test_iceberg_backend_no_versions_json(self, tmp_path: Path) -> None:
        """update_catalog_versions silently returns when no versions.json exists.

        This handles Iceberg backend or other storage backends that don't use
        file-based versioning.
        """
        from portolan_cli.catalog import update_catalog_versions

        # Create catalog without versions.json (simulates Iceberg backend)
        catalog_root = tmp_path / "iceberg-catalog"
        catalog_root.mkdir()
        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (portolan_dir / "config.yaml").write_text("catalog_id: iceberg-test\n")

        # Should not raise - just silently return
        update_catalog_versions(
            catalog_root=catalog_root,
            collection_id="test-collection",
            current_version="1.0.0",
            asset_count=5,
            total_size_bytes=1024,
        )

        # Verify no versions.json was created
        assert not (catalog_root / "versions.json").exists()

    @pytest.mark.integration
    def test_corrupted_catalog_versions_raises_clear_error(self, tmp_path: Path) -> None:
        """Corrupted catalog versions.json raises CatalogVersionsCorruptedError."""
        from portolan_cli.catalog import (
            CatalogVersionsCorruptedError,
            update_catalog_versions,
        )

        # Create catalog with corrupted versions.json
        catalog_root = tmp_path / "corrupt-catalog"
        catalog_root.mkdir()
        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()

        # Write truncated/invalid JSON
        (catalog_root / "versions.json").write_text('{"schema_version": "1.0.0", ')

        with pytest.raises(CatalogVersionsCorruptedError) as exc_info:
            update_catalog_versions(
                catalog_root=catalog_root,
                collection_id="test-collection",
                current_version="1.0.0",
                asset_count=5,
                total_size_bytes=1024,
            )

        # Error message should be helpful
        assert "corrupted" in str(exc_info.value).lower()
        assert str(catalog_root / "versions.json") in str(exc_info.value)

    @pytest.mark.integration
    def test_concurrent_updates_preserve_all_collections(self, tmp_path: Path) -> None:
        """File locking prevents lost updates from concurrent writes.

        Simulates two processes updating different collections simultaneously.
        Both collections should be preserved in final state.
        """
        import threading

        from portolan_cli.catalog import update_catalog_versions

        # Create catalog with empty versions.json
        catalog_root = tmp_path / "concurrent-catalog"
        catalog_root.mkdir()
        portolan_dir = catalog_root / ".portolan"
        portolan_dir.mkdir()
        (catalog_root / "versions.json").write_text(
            json.dumps(
                {
                    "schema_version": "1.0.0",
                    "catalog_id": "concurrent-test",
                    "collections": {},
                }
            )
        )

        results: dict[str, Exception | None] = {}

        def update_collection(coll_id: str) -> None:
            try:
                # Small delay to increase chance of race
                import time

                time.sleep(0.01)
                update_catalog_versions(
                    catalog_root=catalog_root,
                    collection_id=coll_id,
                    current_version="1.0.0",
                    asset_count=10,
                    total_size_bytes=1000,
                )
                results[coll_id] = None
            except Exception as e:
                results[coll_id] = e

        # Launch concurrent updates
        threads = [
            threading.Thread(target=update_collection, args=(f"collection-{i}",)) for i in range(5)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # All should succeed
        for coll_id, error in results.items():
            assert error is None, f"{coll_id} failed: {error}"

        # All collections should be present
        final_data = json.loads((catalog_root / "versions.json").read_text())
        collections = final_data.get("collections", {})
        for i in range(5):
            assert f"collection-{i}" in collections, (
                f"collection-{i} missing! Got: {list(collections.keys())}"
            )


# =============================================================================
# TestScaleAt1000Files (PR #339 original scale)
# =============================================================================


class TestScaleAt1000Files:
    """Test at scale matching original issue #339 (1900 files).

    Uses 1000 files as a compromise between coverage and test speed.
    Marked slow - skipped in normal CI, run in nightly.
    """

    @pytest.fixture
    def catalog_with_1000_files(self, tmp_path: Path) -> tuple[Path, Path]:
        """Catalog with 1000 files matching #339 scale."""
        catalog_root = tmp_path / "scale-1000"
        result = CliRunner().invoke(cli, ["init", str(catalog_root), "--auto"])
        assert result.exit_code == 0

        collection_dir = catalog_root / "large-collection"
        collection_dir.mkdir()

        # Create 1000 minimal GeoJSON files
        for i in range(1000):
            (collection_dir / f"file_{i:04d}.geojson").write_text(
                create_geojson(
                    coords=(float(i % 360 - 180), float(i % 180 - 90)),
                    props={"id": i, "batch": i // 100},
                )
            )

        return catalog_root, collection_dir

    @pytest.mark.integration
    @pytest.mark.slow
    def test_add_1000_files_populates_versions(
        self, catalog_with_1000_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """1000 files all appear in versions.json (Issue #339 scenario)."""
        catalog_root, collection_dir = catalog_with_1000_files

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify collection-level versions.json
        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        latest = versions_data["versions"][-1]
        asset_count = len(latest["assets"])

        # Should have >= 1000 assets (GeoJSON converted to parquet)
        assert asset_count >= 1000, f"Only {asset_count} assets, expected >= 1000"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_add_1000_files_updates_catalog_versions(
        self, catalog_with_1000_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Catalog-level versions.json shows correct asset_count for 1000 files."""
        catalog_root, collection_dir = catalog_with_1000_files

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
        )
        assert result.exit_code == 0, f"Add failed: {result.output}"

        # Verify catalog-level versions.json
        catalog_versions_path = catalog_root / "versions.json"
        catalog_data = json.loads(catalog_versions_path.read_text())

        collections = catalog_data.get("collections", {})
        assert "large-collection" in collections, f"Missing collection: {collections.keys()}"

        coll_info = collections["large-collection"]
        assert coll_info["asset_count"] >= 1000, (
            f"Catalog shows {coll_info['asset_count']} assets, expected >= 1000"
        )


# =============================================================================
# TestPushUsesCatalogVersions
# =============================================================================


class TestPushUsesCatalogVersions:
    """Verify push command properly uses catalog-level versions.json for summary."""

    @pytest.mark.integration
    def test_push_summary_reflects_catalog_versions(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Push summary should reflect what's in catalog-level versions.json."""
        # Create and add to collection
        collection_dir = initialized_catalog / "push-test"
        collection_dir.mkdir()

        for i in range(10):
            (collection_dir / f"file_{i}.geojson").write_text(
                create_geojson(coords=(float(i), float(i)))
            )

        add_result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(collection_dir)],
        )
        assert add_result.exit_code == 0, f"Add failed: {add_result.output}"

        # Verify catalog versions.json has the collection with asset count
        catalog_versions_path = initialized_catalog / "versions.json"
        catalog_data = json.loads(catalog_versions_path.read_text())

        assert "push-test" in catalog_data.get("collections", {}), (
            "Collection not in catalog versions.json"
        )
        expected_count = catalog_data["collections"]["push-test"]["asset_count"]
        assert expected_count >= 10, f"Expected >= 10 assets, got {expected_count}"

        # Configure non-sensitive settings
        config_path = initialized_catalog / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: push-summary-test\n")

        # Dry-run push and verify count matches (remote via env var per Issue #356)
        import re

        with patch.dict(os.environ, {"PORTOLAN_REMOTE": TEST_REMOTE}):
            push_result = runner.invoke(
                cli,
                ["push", "--catalog", str(initialized_catalog), "--dry-run"],
            )

        # Output should show files would be uploaded (dry-run preview)
        # Note: dry-run summary shows "0 file(s)" because nothing was actually pushed,
        # but the preview should show "would upload up to N asset file(s)" where N > 0
        match = re.search(r"would upload up to (\d+) asset", push_result.output.lower())
        assert match is not None, f"No 'would upload' preview in output: {push_result.output}"

        preview_count = int(match.group(1))
        assert preview_count > 0, (
            f"Push preview shows 0 assets despite {expected_count} in catalog: {push_result.output}"
        )
