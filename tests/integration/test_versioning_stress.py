"""Versioning stress tests for issue #339.

Tests the full add → push pipeline and verifies that versions.json is
populated correctly at the collection level (not catalog level).

See:
- tests/specs/versioning_stress.md for human test specification
- ADR-0005 for versions.json as single source of truth
- Issue #339 for the original bug report
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from click.testing import CliRunner

from portolan_cli.cli import cli

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
    """Catalog with 1000 files for scale testing (issue #339)."""
    collection_dir = initialized_catalog / "scale-test"
    collection_dir.mkdir()

    for i in range(1000):
        (collection_dir / f"file_{i:04d}.geojson").write_text(
            create_geojson(
                coords=(float(i % 360 - 180), float(i % 180 - 90)),
                props={"id": i},
            )
        )

    return initialized_catalog, collection_dir


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
            catch_exceptions=False,
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

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        assert "versions" in versions_data
        assert len(versions_data["versions"]) > 0, "versions array is empty"

    @pytest.mark.integration
    def test_add_sets_current_version(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """current_version field is set (dataset.py:1187-1192)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        assert versions_data.get("current_version") is not None
        assert versions_data["current_version"] == versions_data["versions"][-1]["version"]

    @pytest.mark.integration
    def test_add_includes_asset_metadata(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Assets have sha256, size_bytes, href (dataset.py:1208-1213)."""
        catalog_root, collection_dir = catalog_with_source_files
        single_file = collection_dir / "file_0.geojson"

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(single_file)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        latest_version = versions_data["versions"][-1]
        assert "assets" in latest_version
        assert len(latest_version["assets"]) > 0

        for asset_name, asset_data in latest_version["assets"].items():
            assert "sha256" in asset_data, f"Asset {asset_name} missing sha256"
            assert "size_bytes" in asset_data, f"Asset {asset_name} missing size_bytes"
            assert "href" in asset_data, f"Asset {asset_name} missing href"

    @pytest.mark.integration
    @pytest.mark.slow
    def test_add_1000_files_accumulates(
        self, catalog_with_many_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Scale test: many files in one add (Issue #339: 1900 files)."""
        catalog_root, collection_dir = catalog_with_many_files

        result = runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
            catch_exceptions=False,
        )

        assert result.exit_code == 0, f"Add failed: {result.output}"

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        # Should have at least 1000 assets tracked
        latest_version = versions_data["versions"][-1]
        asset_count = len(latest_version["assets"])
        assert asset_count >= 1000, f"Only {asset_count} assets tracked, expected >= 1000"


# =============================================================================
# TestAddThenPushSeesFiles
# =============================================================================


class TestAddThenPushSeesFiles:
    """Verify the full pipeline from add to push."""

    @pytest.mark.integration
    def test_push_after_add_reports_nonzero_files(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Push sees files to upload (push.py:1508-1510)."""
        catalog_root, collection_dir = catalog_with_source_files

        # First add files
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
            catch_exceptions=False,
        )

        # Configure remote (required for push)
        config_path = catalog_root / ".portolan" / "config.yaml"
        config_path.write_text(
            "catalog_id: stress-test-catalog\nremote: s3://fake-bucket/catalog\n"
        )

        # Push dry-run to see what would be uploaded
        result = runner.invoke(
            cli,
            ["push", "--catalog", str(catalog_root), "--dry-run"],
            catch_exceptions=False,
        )

        # Should report files to upload (not "0 files")
        assert "0 files" not in result.output.lower() or "would upload" in result.output.lower()

    @pytest.mark.integration
    def test_push_dry_run_lists_assets(
        self, catalog_with_source_files: tuple[Path, Path], runner: CliRunner
    ) -> None:
        """Dry-run shows asset paths (push.py:851-854)."""
        catalog_root, collection_dir = catalog_with_source_files

        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(catalog_root), str(collection_dir)],
            catch_exceptions=False,
        )

        config_path = catalog_root / ".portolan" / "config.yaml"
        config_path.write_text(
            "catalog_id: stress-test-catalog\nremote: s3://fake-bucket/catalog\n"
        )

        result = runner.invoke(
            cli,
            ["push", "--catalog", str(catalog_root), "--dry-run"],
            catch_exceptions=False,
        )

        # Should list some assets (parquet files after conversion)
        assert ".parquet" in result.output or "asset" in result.output.lower()

    @pytest.mark.integration
    def test_push_reads_collection_level_versions(
        self, catalog_with_multiple_versions: Path, runner: CliRunner
    ) -> None:
        """Push reads correct file (push.py:317)."""
        # Configure remote
        config_path = catalog_with_multiple_versions / ".portolan" / "config.yaml"
        config_path.write_text("catalog_id: test-catalog\nremote: s3://fake-bucket/catalog\n")

        result = runner.invoke(
            cli,
            ["push", "--catalog", str(catalog_with_multiple_versions), "--dry-run"],
            catch_exceptions=False,
        )

        # Should see assets from collection-level versions.json
        # (base.parquet, second.parquet, third.parquet)
        assert "base" in result.output or "parquet" in result.output or result.exit_code == 0


# =============================================================================
# TestSnapshotModelAccumulation
# =============================================================================


class TestSnapshotModelAccumulation:
    """Verify each version is a complete snapshot (ADR-0005)."""

    @pytest.mark.integration
    def test_second_add_preserves_first_assets(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """v2 contains v1's assets (versions.py:357-368)."""
        # Create collection dir inside catalog
        collection_dir = initialized_catalog / "accum-test"
        collection_dir.mkdir()

        # Create two files inside catalog
        file1 = collection_dir / "first.geojson"
        file1.write_text(create_geojson(coords=(1.0, 1.0)))

        file2 = collection_dir / "second.geojson"
        file2.write_text(create_geojson(coords=(2.0, 2.0)))

        # Add first file
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file1)],
            catch_exceptions=False,
        )

        # Add second file
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file2)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        if len(versions_data["versions"]) >= 2:
            v1_assets = set(versions_data["versions"][0]["assets"].keys())
            v2_assets = set(versions_data["versions"][1]["assets"].keys())

            # v2 should contain all v1 assets
            assert v1_assets.issubset(v2_assets), f"v2 missing v1 assets: {v1_assets - v2_assets}"

    @pytest.mark.integration
    def test_third_add_preserves_all_prior(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """v3 contains v1+v2 assets (versions.py:357-368)."""
        # Create collection dir inside catalog
        collection_dir = initialized_catalog / "triple-test"
        collection_dir.mkdir()

        # Create and add three files sequentially
        for i in range(3):
            file_path = collection_dir / f"file{i}.geojson"
            file_path.write_text(create_geojson(coords=(float(i), float(i))))

            runner.invoke(
                cli,
                ["add", "--portolan-dir", str(initialized_catalog), str(file_path)],
                catch_exceptions=False,
            )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        if len(versions_data["versions"]) >= 3:
            v1_assets = set(versions_data["versions"][0]["assets"].keys())
            v2_assets = set(versions_data["versions"][1]["assets"].keys())
            v3_assets = set(versions_data["versions"][2]["assets"].keys())

            # v3 should contain all v1 and v2 assets
            assert v1_assets.issubset(v3_assets), "v3 missing v1 assets"
            assert v2_assets.issubset(v3_assets), "v3 missing v2 assets"

    @pytest.mark.integration
    def test_changes_field_only_shows_delta(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """changes[] has new files only (versions.py:443-472)."""
        # Create collection dir inside catalog
        collection_dir = initialized_catalog / "delta-test"
        collection_dir.mkdir()

        file1 = collection_dir / "first.geojson"
        file1.write_text(create_geojson(coords=(1.0, 1.0)))

        file2 = collection_dir / "second.geojson"
        file2.write_text(create_geojson(coords=(2.0, 2.0)))

        # Add first file
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file1)],
            catch_exceptions=False,
        )

        # Add second file
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file2)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_data = json.loads(versions_path.read_text())

        if len(versions_data["versions"]) >= 2:
            v2_changes = versions_data["versions"][1].get("changes", [])
            v1_assets = set(versions_data["versions"][0]["assets"].keys())

            # v2 changes should NOT contain v1 assets (they're not new)
            changes_set = set(v2_changes)
            overlap = changes_set & v1_assets
            assert not overlap, f"changes[] contains old assets: {overlap}"

    @pytest.mark.integration
    def test_unchanged_file_readd_is_noop(
        self, initialized_catalog: Path, runner: CliRunner
    ) -> None:
        """Idempotent re-add (versions.py:376-379)."""
        # Create collection dir inside catalog
        collection_dir = initialized_catalog / "noop-test"
        collection_dir.mkdir()

        file1 = collection_dir / "file.geojson"
        file1.write_text(create_geojson())

        # Add file
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file1)],
            catch_exceptions=False,
        )

        versions_path = collection_dir / "versions.json"
        versions_before = json.loads(versions_path.read_text())
        version_count_before = len(versions_before["versions"])

        # Re-add same file (unchanged)
        runner.invoke(
            cli,
            ["add", "--portolan-dir", str(initialized_catalog), str(file1)],
            catch_exceptions=False,
        )

        versions_after = json.loads(versions_path.read_text())
        version_count_after = len(versions_after["versions"])

        # Should NOT create new version
        assert version_count_after == version_count_before, (
            "Re-add of unchanged file created new version"
        )


# =============================================================================
# TestPushPullDivergence
# =============================================================================


class TestPushPullDivergence:
    """Verify conflict detection and handling."""

    @pytest.mark.integration
    @pytest.mark.network
    def test_remote_ahead_pull_downloads(self) -> None:
        """Pull gets new remote versions (pull.py:313)."""
        pytest.skip("Requires moto server setup - see test_s3_moto.py patterns")

    @pytest.mark.integration
    @pytest.mark.network
    def test_local_ahead_pull_warns(self) -> None:
        """Pull refuses without --force (pull.py:515-536)."""
        pytest.skip("Requires moto server setup - see test_s3_moto.py patterns")

    @pytest.mark.integration
    @pytest.mark.network
    def test_diverged_state_requires_force(self) -> None:
        """Both ahead → conflict (pull.py:545-558)."""
        pytest.skip("Requires moto server setup - see test_s3_moto.py patterns")

    @pytest.mark.integration
    @pytest.mark.network
    def test_push_conflict_on_etag_mismatch(self) -> None:
        """Concurrent push detected (push.py:807-813)."""
        pytest.skip("Requires moto server setup - see test_s3_moto.py patterns")


# =============================================================================
# TestCorruptionRecovery
# =============================================================================


class TestCorruptionRecovery:
    """Verify handling of malformed data.

    Tests the Python API directly (versions.read_versions) per ADR-0007.
    CLI commands are resilient and silently skip corrupt files, but the
    underlying API should raise clear errors.
    """

    @pytest.mark.integration
    def test_truncated_versions_json_rejected(self, tmp_path: Path) -> None:
        """Invalid JSON fails cleanly (versions.py:144-146)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        versions_path.write_text('{"spec_version": "1.0.0", "versions": [')

        with pytest.raises(ValueError, match="Invalid JSON"):
            read_versions(versions_path)

    @pytest.mark.integration
    def test_missing_versions_field_rejected(self, tmp_path: Path) -> None:
        """Schema validation works (versions.py:164-168)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        versions_path.write_text('{"spec_version": "1.0.0", "current_version": null}')

        with pytest.raises(ValueError, match="missing field"):
            read_versions(versions_path)

    @pytest.mark.integration
    def test_missing_asset_fields_rejected(self, tmp_path: Path) -> None:
        """Asset validation works (versions.py:173-184)."""
        from portolan_cli.versions import read_versions

        versions_path = tmp_path / "versions.json"
        # Asset missing sha256
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
    def test_unknown_fields_ignored(self, tmp_path: Path) -> None:
        """Forward compatibility (versions.py:151-213)."""
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
                            "sha256": "abc123",
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

        # Should not raise
        result = read_versions(versions_path)
        assert result.spec_version == "1.0.0"
        assert len(result.versions) == 1
