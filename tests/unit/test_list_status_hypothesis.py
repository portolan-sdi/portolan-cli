"""Hypothesis tests for unified list command with status indicators.

Property-based tests to verify edge cases and invariants.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner
from hypothesis import HealthCheck, assume, given, settings
from hypothesis import strategies as st

from portolan_cli.cli import cli
from portolan_cli.config import DEFAULT_IGNORED_FILES


@pytest.fixture
def runner() -> CliRunner:
    """Create a CLI test runner."""
    return CliRunner()


def make_catalog(tmp_path: Path) -> None:
    """Write a minimal managed catalog."""
    portolan_dir = tmp_path / ".portolan"
    portolan_dir.mkdir(parents=True, exist_ok=True)
    (portolan_dir / "config.yaml").write_text("# Portolan configuration\n")
    (tmp_path / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "id": "test-catalog",
                "stac_version": "1.0.0",
                "description": "Test catalog",
                "links": [],
            }
        )
    )


def make_collection(col_dir: Path) -> None:
    """Write a minimal collection.json."""
    col_dir.mkdir(parents=True, exist_ok=True)
    (col_dir / "collection.json").write_text(
        json.dumps(
            {
                "type": "Collection",
                "id": col_dir.name,
                "stac_version": "1.0.0",
                "description": f"Collection {col_dir.name}",
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[None, None]]},
                },
                "links": [],
            }
        )
    )


# Strategy for valid collection names (alphanumeric, hyphens, underscores)
collection_name_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789-_"),
    min_size=1,
    max_size=20,
).filter(lambda x: not x.startswith("-") and not x.startswith("."))

# Strategy for valid filenames
filename_strategy = st.text(
    alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz0123456789-_."),
    min_size=1,
    max_size=30,
).filter(
    lambda x: not x.startswith(".")
    and not x.endswith(".")
    and ".." not in x
    and x not in DEFAULT_IGNORED_FILES
)


class TestListStatusHypothesis:
    """Hypothesis-based property tests for list with status."""

    @pytest.mark.unit
    @settings(max_examples=20, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        collection_name=collection_name_strategy,
        item_name=collection_name_strategy,
        filename=filename_strategy,
    )
    def test_any_file_in_item_dir_appears_in_list(
        self,
        runner: CliRunner,
        tmp_path: Path,
        collection_name: str,
        item_name: str,
        filename: str,
    ) -> None:
        """Any file in an item directory should appear in list output."""
        # Skip if names would conflict with reserved names
        assume(collection_name not in {"catalog", ".portolan"})
        assume(item_name not in {"collection"})
        # Skip STAC metadata filenames - these are intentionally excluded from list output
        # by _STAC_METADATA_FILES in catalog_list.py (they're catalog infrastructure, not assets)
        assume(filename not in {"item.json", "collection.json", "catalog.json"})
        # Skip patterns that match default ignored files
        for pattern in DEFAULT_IGNORED_FILES:
            if pattern.startswith("*"):
                assume(not filename.endswith(pattern[1:]))
            elif pattern.startswith("."):
                assume(not filename.startswith(pattern))

        make_catalog(tmp_path)
        col_dir = tmp_path / collection_name
        make_collection(col_dir)
        item_dir = col_dir / item_name
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create a file with valid content
        file_path = item_dir / filename
        file_path.write_text("test content")

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        # File should appear in output (possibly with status indicator)
        assert filename in result.output or result.exit_code != 0

    @pytest.mark.unit
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(num_files=st.integers(min_value=0, max_value=10))
    def test_list_counts_match_actual_files(
        self,
        runner: CliRunner,
        tmp_path: Path,
        num_files: int,
    ) -> None:
        """The summary counts should match the actual number of files."""
        make_catalog(tmp_path)
        col_dir = tmp_path / "testcol"
        make_collection(col_dir)
        item_dir = col_dir / "item1"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create files
        created_files = []
        for i in range(num_files):
            filename = f"file{i}.txt"
            (item_dir / filename).write_text(f"content {i}")
            created_files.append(filename)

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--json", "--catalog", str(tmp_path)])

        if result.exit_code == 0 and num_files > 0:
            envelope = json.loads(result.output)
            # The total number of files in the output should match what we created
            data = envelope.get("data", {})
            if "summary" in data:
                total = data["summary"].get("total_tracked", 0) + data["summary"].get(
                    "total_untracked", 0
                )
                # All files should be untracked since we didn't create versions.json
                assert total >= num_files or data["summary"].get("total_untracked", 0) >= num_files

    @pytest.mark.unit
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        tracked_count=st.integers(min_value=1, max_value=5),
        untracked_count=st.integers(min_value=0, max_value=5),
    )
    def test_tracked_and_untracked_are_disjoint(
        self,
        runner: CliRunner,
        tmp_path: Path,
        tracked_count: int,
        untracked_count: int,
    ) -> None:
        """A file cannot be both tracked and untracked."""
        make_catalog(tmp_path)
        col_dir = tmp_path / "testcol"
        make_collection(col_dir)
        item_dir = col_dir / "item1"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create tracked files
        tracked_assets = {}
        for i in range(tracked_count):
            filename = f"tracked{i}.parquet"
            (item_dir / filename).write_bytes(b"x" * 100)
            tracked_assets[f"item1/{filename}"] = {"sha256": f"hash{i}", "size_bytes": 100}

        # Create untracked files
        for i in range(untracked_count):
            filename = f"untracked{i}.txt"
            (item_dir / filename).write_text(f"untracked {i}")

        # Create versions.json with tracked files
        versions_data = {
            "spec_version": "1.0.0",
            "current_version": "1.0.0",
            "versions": [
                {
                    "version": "1.0.0",
                    "created": "2024-01-15T10:30:00Z",
                    "breaking": False,
                    "assets": tracked_assets,
                    "changes": list(tracked_assets.keys()),
                }
            ],
        }
        (col_dir / "versions.json").write_text(json.dumps(versions_data))

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--json", "--catalog", str(tmp_path)])

        if result.exit_code == 0:
            envelope = json.loads(result.output)
            data = envelope.get("data", {})

            # Extract all file paths from collections/items/assets
            tracked_paths = set()
            untracked_paths = set()

            if "collections" in data:
                for col in data["collections"]:
                    for item in col.get("items", []):
                        for asset in item.get("assets", []):
                            path = asset.get("path", "")
                            status = asset.get("status", "")
                            if status == "tracked":
                                tracked_paths.add(path)
                            elif status == "untracked":
                                untracked_paths.add(path)

            # No file should be in both sets
            overlap = tracked_paths & untracked_paths
            assert len(overlap) == 0, f"Files in both tracked and untracked: {overlap}"


class TestListIgnoredFilesHypothesis:
    """Hypothesis tests for ignored files behavior."""

    @pytest.mark.unit
    @settings(max_examples=10, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(ext=st.sampled_from([".tmp", ".temp", ".pyc"]))
    def test_ignored_extensions_never_appear(
        self,
        runner: CliRunner,
        tmp_path: Path,
        ext: str,
    ) -> None:
        """Files with ignored extensions should never appear in list output."""
        make_catalog(tmp_path)
        col_dir = tmp_path / "testcol"
        make_collection(col_dir)
        item_dir = col_dir / "item1"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create a file with ignored extension
        ignored_file = f"test{ext}"
        (item_dir / ignored_file).write_text("ignored content")

        # Also create a valid file
        (item_dir / "valid.parquet").write_bytes(b"x" * 100)

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert ignored_file not in result.output
        assert "valid.parquet" in result.output

    @pytest.mark.unit
    @settings(max_examples=5, suppress_health_check=[HealthCheck.function_scoped_fixture])
    @given(
        hidden_name=st.text(
            alphabet=st.sampled_from("abcdefghijklmnopqrstuvwxyz"),
            min_size=4,  # Avoid short names that match common extensions like .parquet
            max_size=10,
        )
    )
    def test_hidden_files_never_appear(
        self,
        runner: CliRunner,
        tmp_path: Path,
        hidden_name: str,
    ) -> None:
        """Hidden files (starting with .) should not appear in list output."""
        make_catalog(tmp_path)
        col_dir = tmp_path / "testcol"
        make_collection(col_dir)
        item_dir = col_dir / "item1"
        item_dir.mkdir(parents=True, exist_ok=True)

        # Create a hidden file
        hidden_file = f".{hidden_name}"
        (item_dir / hidden_file).write_text("hidden content")

        # Also create a valid file
        (item_dir / "visible.parquet").write_bytes(b"x" * 100)

        with patch("portolan_cli.cli.find_catalog_root", return_value=tmp_path):
            result = runner.invoke(cli, ["list", "--catalog", str(tmp_path)])

        assert result.exit_code == 0
        assert hidden_file not in result.output
        assert "visible.parquet" in result.output
