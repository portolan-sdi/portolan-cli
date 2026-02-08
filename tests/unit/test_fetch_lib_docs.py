"""Unit tests for scripts/fetch_lib_docs.py.

Tests the import detection and session caching logic.
"""

from __future__ import annotations

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest

if TYPE_CHECKING:
    pass

# Import will fail until we implement the module - that's expected in TDD
pytestmark = pytest.mark.unit


class TestDetectImports:
    """Tests for detect_imports function."""

    def test_detect_geoparquet_io_import_statement(self) -> None:
        """Detect 'import geoparquet_io' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "import geoparquet_io\n"
        result = detect_imports(content)
        assert "geoparquet_io" in result

    def test_detect_geoparquet_io_from_import(self) -> None:
        """Detect 'from geoparquet_io import X' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "from geoparquet_io import read_geoparquet\n"
        result = detect_imports(content)
        assert "geoparquet_io" in result

    def test_detect_geoparquet_io_submodule(self) -> None:
        """Detect 'from geoparquet_io.something import X' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "from geoparquet_io.core import something\n"
        result = detect_imports(content)
        assert "geoparquet_io" in result

    def test_detect_rio_cogeo_import_statement(self) -> None:
        """Detect 'import rio_cogeo' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "import rio_cogeo\n"
        result = detect_imports(content)
        assert "rio_cogeo" in result

    def test_detect_rio_cogeo_from_import(self) -> None:
        """Detect 'from rio_cogeo import X' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "from rio_cogeo.cogeo import cog_validate\n"
        result = detect_imports(content)
        assert "rio_cogeo" in result

    def test_detect_obstore_import_statement(self) -> None:
        """Detect 'import obstore' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "import obstore\n"
        result = detect_imports(content)
        assert "obstore" in result

    def test_detect_obstore_from_import(self) -> None:
        """Detect 'from obstore import X' statement."""
        from scripts.fetch_lib_docs import detect_imports

        content = "from obstore import ObjectStore\n"
        result = detect_imports(content)
        assert "obstore" in result

    def test_detect_multiple_imports(self) -> None:
        """Detect multiple library imports in same file."""
        from scripts.fetch_lib_docs import detect_imports

        content = """
import geoparquet_io
from rio_cogeo import cog_validate
from obstore import ObjectStore
"""
        result = detect_imports(content)
        assert "geoparquet_io" in result
        assert "rio_cogeo" in result
        assert "obstore" in result

    def test_no_imports_detected(self) -> None:
        """Return empty set when no tracked imports found."""
        from scripts.fetch_lib_docs import detect_imports

        content = """
import pandas
from pathlib import Path
import json
"""
        result = detect_imports(content)
        assert result == set()

    def test_ignore_commented_imports(self) -> None:
        """Ignore imports in comments."""
        from scripts.fetch_lib_docs import detect_imports

        content = "# import geoparquet_io\n"
        result = detect_imports(content)
        assert result == set()

    def test_ignore_string_imports(self) -> None:
        """Ignore imports mentioned in strings."""
        from scripts.fetch_lib_docs import detect_imports

        content = 'print("import geoparquet_io")\n'
        result = detect_imports(content)
        assert result == set()


class TestSessionCache:
    """Tests for session caching functionality."""

    def test_get_session_cache_path(self) -> None:
        """Session cache path includes session ID."""
        from scripts.fetch_lib_docs import get_session_cache_path

        path = get_session_cache_path("test-session-123")
        assert "test-session-123" in str(path)
        assert path.parent == Path(tempfile.gettempdir())

    def test_is_library_fetched_returns_false_when_not_cached(self) -> None:
        """Return False when library not in cache."""
        from scripts.fetch_lib_docs import is_library_fetched

        # Use a unique session ID to ensure clean state
        result = is_library_fetched("geoparquet_io", "unique-session-abc123")
        assert result is False

    def test_mark_library_fetched_persists(self) -> None:
        """Marking library as fetched persists across calls."""
        from scripts.fetch_lib_docs import is_library_fetched, mark_library_fetched

        session_id = "test-persist-session-xyz"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            mark_library_fetched("geoparquet_io", session_id)
            result = is_library_fetched("geoparquet_io", session_id)
            assert result is True
        finally:
            # Clean up after test
            if cache_path.exists():
                cache_path.unlink()

    def test_session_cache_prevents_refetch(self) -> None:
        """Already-fetched libraries should not be re-fetched."""
        from scripts.fetch_lib_docs import (
            get_unfetched_libraries,
            mark_library_fetched,
        )

        session_id = "test-refetch-session-456"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            detected = {"geoparquet_io", "rio_cogeo"}
            mark_library_fetched("geoparquet_io", session_id)

            unfetched = get_unfetched_libraries(detected, session_id)
            assert unfetched == {"rio_cogeo"}
        finally:
            if cache_path.exists():
                cache_path.unlink()


class TestRepoMapping:
    """Tests for library to GitHub repo mapping."""

    def test_get_repo_url_geoparquet_io(self) -> None:
        """Get correct GitHub URL for geoparquet_io."""
        from scripts.fetch_lib_docs import get_repo_url

        url = get_repo_url("geoparquet_io")
        assert url == "https://github.com/geoparquet/geoparquet-io"

    def test_get_repo_url_rio_cogeo(self) -> None:
        """Get correct GitHub URL for rio_cogeo."""
        from scripts.fetch_lib_docs import get_repo_url

        url = get_repo_url("rio_cogeo")
        assert url == "https://github.com/cogeotiff/rio-cogeo"

    def test_get_repo_url_obstore(self) -> None:
        """Get correct GitHub URL for obstore."""
        from scripts.fetch_lib_docs import get_repo_url

        url = get_repo_url("obstore")
        assert url == "https://github.com/developmentseed/obstore"

    def test_get_repo_url_unknown_library(self) -> None:
        """Return None for unknown library."""
        from scripts.fetch_lib_docs import get_repo_url

        url = get_repo_url("unknown_lib")
        assert url is None


class TestFetchDocs:
    """Tests for the main fetch_docs function."""

    def test_fetch_docs_calls_gitingest(self) -> None:
        """fetch_docs should call gitingest with correct URL and filtering."""
        from scripts.fetch_lib_docs import fetch_docs

        with patch("gitingest.ingest") as mock_ingest:
            mock_ingest.return_value = ("summary", "tree", "content")
            result = fetch_docs("geoparquet_io")

            mock_ingest.assert_called_once_with(
                "https://github.com/geoparquet/geoparquet-io",
                include_patterns={"*.py", "*.md", "*.rst", "*.txt"},
                exclude_patterns={"tests/*", "test/*", "*.png", "*.jpg", "*.gif", "*.svg"},
                max_file_size=1024 * 1024,
            )
            assert result is not None

    def test_fetch_docs_returns_none_on_error(self) -> None:
        """fetch_docs should return None on gitingest error."""
        from scripts.fetch_lib_docs import fetch_docs

        with patch("gitingest.ingest") as mock_ingest:
            mock_ingest.side_effect = Exception("Network error")
            result = fetch_docs("geoparquet_io")

            assert result is None

    def test_fetch_docs_unknown_library(self) -> None:
        """fetch_docs should return None for unknown library."""
        from scripts.fetch_lib_docs import fetch_docs

        result = fetch_docs("unknown_lib")
        assert result is None
