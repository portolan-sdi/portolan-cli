"""Integration tests for portolan readme recursive behavior.

Tests that readme (recursive by default) regenerates all collection READMEs
along with the catalog README. Use --no-recursive to limit scope.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalog_invariants import RunCli


class TestReadmeRecursive:
    """Tests for portolan readme recursive behavior (recursive by default)."""

    @pytest.fixture
    def catalog_with_collections(self, tmp_path: Path) -> Path:
        """Create a catalog with multiple collections."""
        # Initialize catalog structure
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("version: '1.0'\n")

        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "test-catalog",
                    "title": "Test Catalog",
                    "description": "A test catalog with multiple collections",
                    "stac_version": "1.0.0",
                }
            )
        )

        # Create two collections
        for coll_id in ["alpha", "beta"]:
            coll_dir = tmp_path / coll_id
            coll_dir.mkdir()
            (coll_dir / "collection.json").write_text(
                json.dumps(
                    {
                        "type": "Collection",
                        "id": coll_id,
                        "title": f"Collection {coll_id.upper()}",
                        "description": f"Description for {coll_id}",
                        "stac_version": "1.0.0",
                        "extent": {
                            "spatial": {"bbox": [[0, 0, 1, 1]]},
                            "temporal": {"interval": [[None, None]]},
                        },
                        "links": [],
                        "license": "MIT",
                    }
                )
            )
            # Create .portolan for each collection
            (coll_dir / ".portolan").mkdir()
            (coll_dir / ".portolan" / "metadata.yaml").write_text(
                f"title: Collection {coll_id.upper()}\n"
            )

        return tmp_path

    @pytest.mark.integration
    def test_recursive_generates_all_readmes(
        self, catalog_with_collections: Path, run_cli: RunCli
    ) -> None:
        """readme (recursive by default) should generate README for catalog and all collections."""
        run_cli("readme", cwd=catalog_with_collections)

        # Check catalog README exists
        assert (catalog_with_collections / "README.md").exists()

        # Check collection READMEs exist
        assert (catalog_with_collections / "alpha" / "README.md").exists()
        assert (catalog_with_collections / "beta" / "README.md").exists()

    @pytest.mark.integration
    def test_recursive_reports_count(self, catalog_with_collections: Path, run_cli: RunCli) -> None:
        """readme (recursive by default) should report how many READMEs were generated."""
        result = run_cli("readme", cwd=catalog_with_collections)
        # The catalog README plus one per collection.
        assert "Generated 3 README(s)" in result.output

    @pytest.mark.integration
    def test_no_recursive_only_generates_target(
        self, catalog_with_collections: Path, run_cli: RunCli
    ) -> None:
        """With --no-recursive, only the target README is generated."""
        run_cli("readme", "--no-recursive", cwd=catalog_with_collections)

        # Only catalog README should exist
        assert (catalog_with_collections / "README.md").exists()

        # Collection READMEs should NOT exist (not generated with --no-recursive)
        assert not (catalog_with_collections / "alpha" / "README.md").exists()
        assert not (catalog_with_collections / "beta" / "README.md").exists()

    @pytest.mark.integration
    def test_recursive_scopes_to_path_argument(
        self, catalog_with_collections: Path, run_cli: RunCli
    ) -> None:
        """readme PATH should scope generation to that subtree, not the whole catalog."""
        run_cli("readme", "alpha", cwd=catalog_with_collections)

        # Only the targeted collection README should be generated
        assert (catalog_with_collections / "alpha" / "README.md").exists()

        # Out-of-scope READMEs should NOT be generated
        assert not (catalog_with_collections / "beta" / "README.md").exists()
        assert not (catalog_with_collections / "README.md").exists()

    @pytest.mark.integration
    def test_recursive_stdout_rejection_json_is_valid(
        self, catalog_with_collections: Path, run_cli: RunCli
    ) -> None:
        """readme --stdout in JSON mode should emit a JSON error, not plain text."""
        result = run_cli(
            "--format", "json", "readme", "--stdout", cwd=catalog_with_collections, expect=None
        )

        assert result.exit_code == 1
        # Output must be parseable JSON (not a plain-text error line)
        output = json.loads(result.stdout)
        assert output["success"] is False
        assert "stdout" in result.output.lower()

    @pytest.mark.integration
    def test_recursive_json_output(self, catalog_with_collections: Path, run_cli: RunCli) -> None:
        """readme --json (recursive by default) should output structured results."""
        result = run_cli("--format", "json", "readme", cwd=catalog_with_collections)

        output = json.loads(result.stdout)
        assert output["success"] is True
        assert "generated" in output["data"]
        assert output["data"]["count"] == 3  # catalog + 2 collections

    @pytest.mark.integration
    def test_recursive_check_mode(self, catalog_with_collections: Path, run_cli: RunCli) -> None:
        """readme --check (recursive by default) should check all READMEs."""
        # First generate all READMEs
        run_cli("readme", cwd=catalog_with_collections)

        # Then check - should pass
        run_cli("readme", "--check", cwd=catalog_with_collections)

    @pytest.mark.integration
    def test_recursive_check_fails_when_stale(
        self, catalog_with_collections: Path, run_cli: RunCli
    ) -> None:
        """readme --check (recursive by default) should fail if any README is stale."""
        # Generate all READMEs
        run_cli("readme", cwd=catalog_with_collections)

        # Modify a collection to make its README stale. The alpha collection has
        # a metadata.yaml title, which is the highest-precedence source for the
        # README heading (#534), so mutate that to actually change the output.
        meta_yaml = catalog_with_collections / "alpha" / ".portolan" / "metadata.yaml"
        meta_yaml.write_text("title: MODIFIED TITLE\n")

        # Check should now fail
        result = run_cli("readme", "--check", cwd=catalog_with_collections, expect=1)
        assert "stale" in result.output.lower() or "alpha" in result.output


class TestReadmeVerbose:
    """Integration tests for portolan readme --verbose flag."""

    @pytest.fixture
    def catalog_with_metadata(self, tmp_path: Path) -> Path:
        """Create a catalog with collections and metadata.yaml files."""
        # Initialize catalog structure
        (tmp_path / ".portolan").mkdir()
        (tmp_path / ".portolan" / "config.yaml").write_text("version: '1.0'\n")
        (tmp_path / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Test\n  email: test@example.com\nlicense: MIT\n"
        )

        (tmp_path / "catalog.json").write_text(
            json.dumps(
                {
                    "type": "Catalog",
                    "id": "verbose-test",
                    "title": "Verbose Test Catalog",
                    "description": "Testing verbose output",
                    "stac_version": "1.0.0",
                }
            )
        )

        # Create collection with metadata
        coll_dir = tmp_path / "mydata"
        coll_dir.mkdir()
        (coll_dir / "collection.json").write_text(
            json.dumps(
                {
                    "type": "Collection",
                    "id": "mydata",
                    "title": "My Data Collection",
                    "description": "Test collection",
                    "stac_version": "1.0.0",
                    "extent": {
                        "spatial": {"bbox": [[0, 0, 1, 1]]},
                        "temporal": {"interval": [[None, None]]},
                    },
                    "links": [],
                    "license": "MIT",
                }
            )
        )
        (coll_dir / ".portolan").mkdir()
        (coll_dir / ".portolan" / "metadata.yaml").write_text(
            "contact:\n  name: Test\n  email: test@example.com\nlicense: MIT\n"
        )

        return tmp_path

    @pytest.mark.integration
    def test_verbose_shows_file_reads_for_collection(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """--verbose should show which files are being read for a collection."""
        result = run_cli("readme", "mydata", "--verbose", cwd=catalog_with_metadata)
        assert "collection.json" in result.output
        assert "metadata.yaml" in result.output
        assert "mydata" in result.output

    @pytest.mark.integration
    def test_verbose_shows_file_reads_for_catalog(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """--verbose should show which files are being read for catalog root."""
        result = run_cli("readme", "--verbose", cwd=catalog_with_metadata)
        assert "catalog.json" in result.output
        assert "metadata.yaml" in result.output

    @pytest.mark.integration
    def test_verbose_recursive_shows_all_file_reads(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """readme --verbose (recursive by default) should show file reads for every entity."""
        result = run_cli("readme", "--verbose", cwd=catalog_with_metadata)
        # Should show processing messages
        assert "Processing" in result.output
        # Should show file-level reads for catalog root
        assert "catalog.json" in result.output
        # Should show file-level reads for collection
        assert "collection.json" in result.output
        # Should show metadata reads
        assert "metadata.yaml" in result.output

    @pytest.mark.integration
    def test_verbose_with_json_produces_valid_json(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """--verbose --json should not corrupt JSON output."""
        result = run_cli("--format", "json", "readme", "--verbose", cwd=catalog_with_metadata)
        # Must be valid JSON
        output = json.loads(result.stdout)
        assert output["success"] is True
        # Verbose text should NOT appear in JSON output
        assert "Reading" not in result.output

    @pytest.mark.integration
    def test_verbose_recursive_json_produces_valid_json(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """readme --verbose --json (recursive by default) should produce valid JSON."""
        result = run_cli("--format", "json", "readme", "--verbose", cwd=catalog_with_metadata)
        output = json.loads(result.stdout)
        assert output["success"] is True
        assert "generated" in output["data"]

    @pytest.mark.integration
    def test_default_no_verbose_is_quiet(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """Without --verbose, file read messages should not appear."""
        result = run_cli("readme", cwd=catalog_with_metadata)
        # Should NOT show "Reading" messages
        assert "Reading" not in result.output

    @pytest.mark.integration
    def test_verbose_check_mode_shows_reads(
        self, catalog_with_metadata: Path, run_cli: RunCli
    ) -> None:
        """--check --verbose should show file reads during check."""
        # First generate
        run_cli("readme", cwd=catalog_with_metadata)

        # Then check with verbose
        result = run_cli("readme", "--check", "--verbose", cwd=catalog_with_metadata)
        assert "catalog.json" in result.output or "metadata.yaml" in result.output
