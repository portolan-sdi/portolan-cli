"""Tests for clone command git-style ergonomics (Issue #146).

These tests cover:
1. URL inference for local path (infer_local_path_from_url)
2. Listing remote collections (list_remote_collections)
3. Clone with optional collection (clone all collections)
4. Clone to current directory ('.')
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

if TYPE_CHECKING:
    from click.testing import CliRunner

# =============================================================================
# Tests for infer_local_path_from_url
# =============================================================================


class TestInferLocalPathFromUrl:
    """Tests for inferring local directory name from remote URL."""

    @pytest.mark.unit
    def test_infer_from_s3_url(self) -> None:
        """Should extract catalog name from S3 URL."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("s3://mybucket/my-catalog")
        assert result == Path("my-catalog")

    @pytest.mark.unit
    def test_infer_from_s3_url_trailing_slash(self) -> None:
        """Should handle trailing slashes."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("s3://mybucket/my-catalog/")
        assert result == Path("my-catalog")

    @pytest.mark.unit
    def test_infer_from_nested_path(self) -> None:
        """Should extract last component from nested path."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("s3://mybucket/path/to/my-catalog")
        assert result == Path("my-catalog")

    @pytest.mark.unit
    def test_infer_from_gcs_url(self) -> None:
        """Should work with GCS URLs."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("gs://mybucket/catalog-name")
        assert result == Path("catalog-name")

    @pytest.mark.unit
    def test_infer_from_azure_url(self) -> None:
        """Should work with Azure URLs."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("az://container/my-data")
        assert result == Path("my-data")

    @pytest.mark.unit
    def test_infer_handles_multiple_trailing_slashes(self) -> None:
        """Should handle multiple trailing slashes."""
        from portolan_cli.sync import infer_local_path_from_url

        result = infer_local_path_from_url("s3://bucket/catalog///")
        assert result == Path("catalog")

    @pytest.mark.unit
    def test_infer_raises_on_bucket_only_url(self) -> None:
        """Should raise error for bucket-only URLs (no catalog name)."""
        from portolan_cli.sync import infer_local_path_from_url

        with pytest.raises(ValueError, match="Cannot infer"):
            infer_local_path_from_url("s3://mybucket")

    @pytest.mark.unit
    def test_infer_raises_on_bucket_only_url_with_slash(self) -> None:
        """Should raise error for bucket-only URLs with trailing slash."""
        from portolan_cli.sync import infer_local_path_from_url

        with pytest.raises(ValueError, match="Cannot infer"):
            infer_local_path_from_url("s3://mybucket/")


class TestInferLocalPathFromUrlHypothesis:
    """Property-based tests for URL inference."""

    @pytest.mark.unit
    @given(
        catalog_name=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=50,
        ).filter(lambda x: x.strip() and not x.startswith("-")),
    )
    @settings(max_examples=50)
    def test_infer_extracts_catalog_name(self, catalog_name: str) -> None:
        """Property: inferred path should match catalog name from URL."""
        from portolan_cli.sync import infer_local_path_from_url

        url = f"s3://somebucket/{catalog_name}"
        result = infer_local_path_from_url(url)
        assert result == Path(catalog_name)

    @pytest.mark.unit
    @given(
        prefix=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="-_/"
            ),
            min_size=1,
            max_size=30,
        ).filter(lambda x: x.strip() and "/" not in x[-1:]),
        catalog_name=st.text(
            alphabet=st.characters(
                whitelist_categories=("Ll", "Lu", "Nd"), whitelist_characters="-_"
            ),
            min_size=1,
            max_size=20,
        ).filter(lambda x: x.strip() and not x.startswith("-")),
    )
    @settings(max_examples=30)
    def test_infer_extracts_last_component(self, prefix: str, catalog_name: str) -> None:
        """Property: should extract last path component regardless of prefix."""
        from portolan_cli.sync import infer_local_path_from_url

        # Clean up prefix - remove leading/trailing slashes
        prefix_clean = prefix.strip("/")
        if prefix_clean:
            url = f"s3://bucket/{prefix_clean}/{catalog_name}"
        else:
            url = f"s3://bucket/{catalog_name}"

        result = infer_local_path_from_url(url)
        assert result == Path(catalog_name)


# =============================================================================
# Tests for list_remote_collections
# =============================================================================


class TestListRemoteCollections:
    """Tests for listing collections from a remote catalog."""

    @pytest.mark.unit
    def test_list_collections_parses_stac_links(self, tmp_path: Path) -> None:
        """Should parse child links from STAC catalog.json."""
        from portolan_cli.sync import list_remote_collections

        # Mock catalog.json content
        catalog_json = {
            "type": "Catalog",
            "id": "test-catalog",
            "description": "Test catalog",
            "links": [
                {"rel": "root", "href": "./catalog.json"},
                {"rel": "child", "href": "./demographics/collection.json"},
                {"rel": "child", "href": "./imagery/collection.json"},
                {"rel": "child", "href": "./boundaries/collection.json"},
            ],
        }

        with patch("portolan_cli.sync._fetch_remote_catalog_json") as mock_fetch:
            mock_fetch.return_value = catalog_json

            result = list_remote_collections("s3://bucket/catalog")

        assert result == ["demographics", "imagery", "boundaries"]

    @pytest.mark.unit
    def test_list_collections_returns_empty_for_no_children(self) -> None:
        """Should return empty list if catalog has no child collections."""
        from portolan_cli.sync import list_remote_collections

        catalog_json = {
            "type": "Catalog",
            "id": "empty-catalog",
            "description": "Empty catalog",
            "links": [
                {"rel": "root", "href": "./catalog.json"},
                {"rel": "self", "href": "./catalog.json"},
            ],
        }

        with patch("portolan_cli.sync._fetch_remote_catalog_json") as mock_fetch:
            mock_fetch.return_value = catalog_json

            result = list_remote_collections("s3://bucket/catalog")

        assert result == []

    @pytest.mark.unit
    def test_list_collections_handles_absolute_hrefs(self) -> None:
        """Should handle absolute href paths in child links."""
        from portolan_cli.sync import list_remote_collections

        catalog_json = {
            "type": "Catalog",
            "id": "test-catalog",
            "links": [
                {"rel": "child", "href": "s3://bucket/catalog/collection-a/collection.json"},
                {"rel": "child", "href": "./collection-b/collection.json"},
            ],
        }

        with patch("portolan_cli.sync._fetch_remote_catalog_json") as mock_fetch:
            mock_fetch.return_value = catalog_json

            result = list_remote_collections("s3://bucket/catalog")

        assert "collection-a" in result
        assert "collection-b" in result

    @pytest.mark.unit
    def test_list_collections_propagates_profile(self) -> None:
        """Should pass AWS profile to fetch function."""
        from portolan_cli.sync import list_remote_collections

        with patch("portolan_cli.sync._fetch_remote_catalog_json") as mock_fetch:
            mock_fetch.return_value = {"type": "Catalog", "links": []}

            list_remote_collections("s3://bucket/catalog", profile="my-profile")

        mock_fetch.assert_called_once_with("s3://bucket/catalog", profile="my-profile")


class TestFetchRemoteCatalogJson:
    """Tests for fetching and parsing remote catalog.json."""

    @pytest.mark.unit
    def test_fetch_downloads_and_parses_json(self, tmp_path: Path) -> None:
        """Should download catalog.json and parse it."""
        from portolan_cli.sync import _fetch_remote_catalog_json

        catalog_content = {"type": "Catalog", "id": "test"}

        with (
            patch("portolan_cli.sync.download_file") as mock_download,
            patch("builtins.open", MagicMock()),
            patch("portolan_cli.sync.json.load") as mock_json_load,
            patch("pathlib.Path.unlink"),
            patch("pathlib.Path.exists", return_value=True),
        ):
            mock_download.return_value = MagicMock(success=True)
            mock_json_load.return_value = catalog_content

            result = _fetch_remote_catalog_json("s3://bucket/catalog")

        assert result == catalog_content
        # Should fetch catalog.json from remote
        mock_download.assert_called_once()
        call_source = mock_download.call_args.kwargs.get("source")
        assert "catalog.json" in call_source

    @pytest.mark.unit
    def test_fetch_raises_on_download_failure(self) -> None:
        """Should raise error if download fails."""
        from portolan_cli.sync import CloneError, _fetch_remote_catalog_json

        with patch("portolan_cli.sync.download_file") as mock_download:
            # errors is list[tuple[Path, Exception]] per DownloadResult
            mock_download.return_value = MagicMock(
                success=False,
                errors=[(Path("/tmp/test.json"), ConnectionError("Connection refused"))],
            )

            with pytest.raises(CloneError, match="Failed to fetch"):
                _fetch_remote_catalog_json("s3://bucket/catalog")


# =============================================================================
# Tests for clone with optional collection
# =============================================================================


class TestCloneAllCollections:
    """Tests for cloning all collections when --collection is not specified."""

    @pytest.mark.unit
    def test_clone_all_collections_when_none_specified(self, tmp_path: Path) -> None:
        """Should clone all collections when collection=None."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_list.return_value = ["collection-a", "collection-b"]
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection=None,  # Clone all
            )

        assert result.success is True
        # Should have called pull for each collection
        assert mock_pull.call_count == 2
        call_collections = [call.kwargs["collection"] for call in mock_pull.call_args_list]
        assert "collection-a" in call_collections
        assert "collection-b" in call_collections

    @pytest.mark.unit
    def test_clone_single_collection_still_works(self, tmp_path: Path) -> None:
        """Should still support explicit collection specification."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=5,
                remote_version="1.0.0",
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection="specific-collection",
            )

        assert result.success is True
        # Should NOT list remote collections
        mock_list.assert_not_called()
        # Should call pull with specific collection
        mock_pull.assert_called_once()
        assert mock_pull.call_args.kwargs["collection"] == "specific-collection"

    @pytest.mark.unit
    def test_clone_fails_gracefully_when_no_collections(self, tmp_path: Path) -> None:
        """Should fail with helpful message when remote has no collections."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog"),
        ):
            mock_list.return_value = []  # No collections

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection=None,
            )

        assert result.success is False
        assert any("no collections" in err.lower() for err in result.errors)

    @pytest.mark.unit
    def test_clone_partial_failure_reports_all_errors(self, tmp_path: Path) -> None:
        """Should report errors for each failed collection in multi-clone."""
        from portolan_cli.sync import clone

        target = tmp_path / "new_catalog"

        with (
            patch("portolan_cli.sync.list_remote_collections") as mock_list,
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_list.return_value = ["good-collection", "bad-collection", "another-good"]

            # First and third succeed, second fails
            mock_pull.side_effect = [
                MagicMock(success=True, files_downloaded=3, remote_version="1.0.0"),
                MagicMock(success=False, remote_version="1.0.0", uncommitted_changes=[]),
                MagicMock(success=True, files_downloaded=2, remote_version="1.0.0"),
            ]

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=target,
                collection=None,
            )

        # Partial success - some collections cloned
        assert result.success is False  # Overall failure due to partial
        assert any("bad-collection" in err for err in result.errors)


class TestCloneToCurrentDirectory:
    """Tests for cloning to current directory ('.')."""

    @pytest.mark.unit
    def test_clone_to_dot_with_empty_directory(self, tmp_path: Path) -> None:
        """Should allow cloning to '.' if directory is empty."""
        from portolan_cli.sync import clone

        # tmp_path is empty

        with (
            patch("portolan_cli.sync.init_catalog"),
            patch("portolan_cli.sync.pull") as mock_pull,
        ):
            mock_pull.return_value = MagicMock(
                success=True,
                files_downloaded=3,
                remote_version="1.0.0",
            )

            result = clone(
                remote_url="s3://bucket/catalog",
                local_path=tmp_path,  # Equivalent to '.'
                collection="test",
            )

        assert result.success is True
        assert result.local_path == tmp_path

    @pytest.mark.unit
    def test_clone_to_dot_fails_if_not_empty(self, tmp_path: Path) -> None:
        """Should fail if '.' is not empty."""
        from portolan_cli.sync import clone

        # Create a file to make directory non-empty
        (tmp_path / "existing.txt").write_text("content")

        result = clone(
            remote_url="s3://bucket/catalog",
            local_path=tmp_path,
            collection="test",
        )

        assert result.success is False
        assert any("not empty" in err for err in result.errors)


# =============================================================================
# Tests for CloneResult with multiple collections
# =============================================================================


class TestCloneResultMultiCollection:
    """Tests for CloneResult when cloning multiple collections."""

    @pytest.mark.unit
    def test_clone_result_stores_multiple_pull_results(self, tmp_path: Path) -> None:
        """CloneResult should store results for multiple collections."""
        from portolan_cli.sync import CloneResult

        # When cloning multiple collections, pull_results should be a list
        # or we store aggregate stats
        result = CloneResult(
            success=True,
            pull_result=None,  # For multi-collection, we may aggregate
            local_path=tmp_path / "cloned",
            collections_cloned=["a", "b", "c"],
            total_files_downloaded=15,
        )

        assert result.success is True
        assert len(result.collections_cloned) == 3
        assert result.total_files_downloaded == 15


# =============================================================================
# CLI Integration Tests
# =============================================================================


class TestCloneCLIErgonomics:
    """Tests for CLI argument handling."""

    @pytest.fixture
    def cli_runner(self) -> CliRunner:
        """Create Click CLI test runner."""
        from click.testing import CliRunner

        return CliRunner()

    @pytest.mark.unit
    def test_cli_local_path_is_optional(self, cli_runner: CliRunner) -> None:
        """LOCAL_PATH should be optional in CLI."""
        from portolan_cli.cli import clone

        with (
            patch("portolan_cli.sync.clone") as mock_clone,
        ):
            mock_clone.return_value = MagicMock(
                success=True,
                local_path=Path("my-catalog"),
                pull_result=None,
                errors=[],
                collections_cloned=["test"],
                total_files_downloaded=5,
            )

            # Should work without LOCAL_PATH (inferred from URL)
            result = cli_runner.invoke(
                clone,
                ["s3://bucket/my-catalog", "-c", "test"],
            )

        # Should infer local_path from URL
        assert result.exit_code == 0
        mock_clone.assert_called_once()
        call_kwargs = mock_clone.call_args.kwargs
        assert call_kwargs["local_path"] == Path("my-catalog")

    @pytest.mark.unit
    def test_cli_collection_is_optional(self, cli_runner: CliRunner) -> None:
        """--collection should be optional in CLI."""
        from portolan_cli.cli import clone

        with (
            patch("portolan_cli.sync.clone") as mock_clone,
        ):
            mock_clone.return_value = MagicMock(
                success=True,
                local_path=Path("local"),
                pull_result=None,
                errors=[],
                collections_cloned=["a", "b"],
                total_files_downloaded=10,
            )

            # Should work without --collection
            result = cli_runner.invoke(
                clone,
                ["s3://bucket/my-catalog", "./local"],
            )

        # Should call clone with collection=None
        assert result.exit_code == 0
        mock_clone.assert_called_once()
        call_kwargs = mock_clone.call_args.kwargs
        assert call_kwargs.get("collection") is None

    @pytest.mark.unit
    def test_cli_explicit_local_path_honored(self, cli_runner: CliRunner) -> None:
        """Explicit LOCAL_PATH should override inference."""
        from portolan_cli.cli import clone

        with (
            patch("portolan_cli.sync.clone") as mock_clone,
        ):
            mock_clone.return_value = MagicMock(
                success=True,
                local_path=Path("./custom-dir"),
                pull_result=None,
                errors=[],
                collections_cloned=["test"],
                total_files_downloaded=3,
            )

            result = cli_runner.invoke(
                clone,
                ["s3://bucket/my-catalog", "./custom-dir", "-c", "test"],
            )

        assert result.exit_code == 0
        mock_clone.assert_called_once()
        call_kwargs = mock_clone.call_args.kwargs
        assert call_kwargs["local_path"] == Path("./custom-dir")
