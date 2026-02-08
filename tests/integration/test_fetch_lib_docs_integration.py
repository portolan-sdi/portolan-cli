"""Integration tests for scripts/fetch_lib_docs.py.

Tests the full hook processing pipeline and real gitingest fetches.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

pytestmark = pytest.mark.integration


class TestProcessHookInput:
    """Tests for the full hook processing pipeline."""

    def test_process_hook_input_python_file_with_import(self) -> None:
        """Process hook input for Python file with tracked import."""
        from scripts.fetch_lib_docs import process_hook_input

        session_id = "test-hook-integration-1"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            hook_input = {
                "tool_input": {"file_path": "/path/to/file.py"},
                "tool_result": {"content": "from geoparquet_io import read"},
            }

            with patch("gitingest.ingest") as mock_ingest:
                mock_ingest.return_value = ("summary", "tree", "content")
                result = process_hook_input(hook_input, session_id)

            assert result is not None
            assert "systemMessage" in result
            assert "geoparquet_io" in result["systemMessage"]
            assert "<auto-fetched-docs>" in result["systemMessage"]
        finally:
            if cache_path.exists():
                cache_path.unlink()

    def test_process_hook_input_non_python_file(self) -> None:
        """Skip non-Python files."""
        from scripts.fetch_lib_docs import process_hook_input

        hook_input = {
            "tool_input": {"file_path": "/path/to/file.js"},
            "tool_result": {"content": "import geoparquet_io"},
        }

        result = process_hook_input(hook_input, "test-session")
        assert result is None

    def test_process_hook_input_no_tracked_imports(self) -> None:
        """Skip files without tracked imports."""
        from scripts.fetch_lib_docs import process_hook_input

        hook_input = {
            "tool_input": {"file_path": "/path/to/file.py"},
            "tool_result": {"content": "import pandas\nimport numpy"},
        }

        result = process_hook_input(hook_input, "test-session")
        assert result is None

    def test_process_hook_input_already_fetched(self) -> None:
        """Skip libraries already fetched this session."""
        from scripts.fetch_lib_docs import mark_library_fetched, process_hook_input

        session_id = "test-hook-already-fetched"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            # Mark as already fetched
            mark_library_fetched("geoparquet_io", session_id)

            hook_input = {
                "tool_input": {"file_path": "/path/to/file.py"},
                "tool_result": {"content": "from geoparquet_io import read"},
            }

            result = process_hook_input(hook_input, session_id)
            assert result is None
        finally:
            if cache_path.exists():
                cache_path.unlink()

    def test_process_hook_input_multiple_libraries(self) -> None:
        """Fetch docs for multiple libraries in same file."""
        from scripts.fetch_lib_docs import process_hook_input

        session_id = "test-hook-multi-libs"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            hook_input = {
                "tool_input": {"file_path": "/path/to/file.py"},
                "tool_result": {
                    "content": "from geoparquet_io import read\nfrom rio_cogeo import cog"
                },
            }

            with patch("gitingest.ingest") as mock_ingest:
                mock_ingest.return_value = ("summary", "tree", "content")
                result = process_hook_input(hook_input, session_id)

            assert result is not None
            assert "geoparquet_io" in result["systemMessage"]
            assert "rio_cogeo" in result["systemMessage"]
        finally:
            if cache_path.exists():
                cache_path.unlink()


class TestHookEndToEnd:
    """End-to-end tests simulating actual hook JSON I/O."""

    def test_main_with_valid_input(self) -> None:
        """Test main() with valid JSON input."""
        from io import StringIO

        import scripts.fetch_lib_docs as fetch_lib_docs

        session_id = "test-e2e-main"
        cache_path = Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"

        # Clean up before test
        if cache_path.exists():
            cache_path.unlink()

        try:
            hook_input = {
                "tool_input": {"file_path": "/path/to/file.py"},
                "tool_result": {"content": "from obstore import ObjectStore"},
            }

            # Capture stdout
            import sys

            old_stdin = sys.stdin
            old_stdout = sys.stdout
            sys.stdin = StringIO(json.dumps(hook_input))
            sys.stdout = captured_output = StringIO()

            with (
                patch.dict("os.environ", {"CLAUDE_SESSION_ID": session_id}),
                patch("gitingest.ingest") as mock_ingest,
            ):
                mock_ingest.return_value = ("summary", "tree", "content")
                fetch_lib_docs.main()

            sys.stdin = old_stdin
            sys.stdout = old_stdout

            output = captured_output.getvalue()
            assert output  # Should have output
            result = json.loads(output)
            assert "systemMessage" in result
            assert "obstore" in result["systemMessage"]
        finally:
            if cache_path.exists():
                cache_path.unlink()

    def test_main_with_invalid_json(self) -> None:
        """Test main() gracefully handles invalid JSON."""
        import sys
        from io import StringIO

        import scripts.fetch_lib_docs as fetch_lib_docs

        old_stdin = sys.stdin
        old_stdout = sys.stdout
        sys.stdin = StringIO("not valid json {{{")
        sys.stdout = captured_output = StringIO()

        # Should exit with code 0 (silent failure)
        with pytest.raises(SystemExit) as exc_info:
            fetch_lib_docs.main()

        sys.stdin = old_stdin
        sys.stdout = old_stdout

        assert exc_info.value.code == 0
        output = captured_output.getvalue()
        assert output == ""  # No output on invalid input


@pytest.mark.network
class TestRealGitingestFetch:
    """Tests that actually fetch from GitHub.

    These are marked with @pytest.mark.network and skipped locally.
    Run with: pytest -m network
    """

    def test_fetch_docs_real_geoparquet_io(self) -> None:
        """Actually fetch geoparquet-io docs from GitHub."""
        from scripts.fetch_lib_docs import fetch_docs

        result = fetch_docs("geoparquet_io")

        assert result is not None
        assert "geoparquet" in result.lower()
        assert len(result) > 1000  # Should have substantial content

    def test_fetch_docs_real_rio_cogeo(self) -> None:
        """Actually fetch rio-cogeo docs from GitHub."""
        from scripts.fetch_lib_docs import fetch_docs

        result = fetch_docs("rio_cogeo")

        assert result is not None
        assert "cog" in result.lower()
        assert len(result) > 1000
