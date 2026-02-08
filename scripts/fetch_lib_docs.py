#!/usr/bin/env python3
"""Fetch API documentation for core dependencies using gitingest.

This module is used by the Claude hook system to auto-fetch current API docs
when reading Python files that import geoparquet_io, rio_cogeo, or obstore.

Usage as CLI:
    python scripts/fetch_lib_docs.py <file_content> <session_id>

Usage as module:
    from scripts.fetch_lib_docs import detect_imports, fetch_docs
"""

from __future__ import annotations

import json
import logging
import re
import sys
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Mapping of import names to GitHub repository URLs
REPO_MAPPING: dict[str, str] = {
    "geoparquet_io": "https://github.com/geoparquet/geoparquet-io",
    "rio_cogeo": "https://github.com/cogeotiff/rio-cogeo",
    "obstore": "https://github.com/developmentseed/obstore",
}

# Libraries we track for auto-fetching
TRACKED_LIBRARIES = frozenset(REPO_MAPPING.keys())


def detect_imports(content: str) -> set[str]:
    """Detect imports of tracked libraries in Python source code.

    Args:
        content: Python source code as a string.

    Returns:
        Set of library names that are imported in the content.
    """
    detected: set[str] = set()

    for lib in TRACKED_LIBRARIES:
        # Match:
        # - import lib
        # - import lib.submodule
        # - from lib import ...
        # - from lib.submodule import ...
        #
        # But NOT:
        # - # import lib (commented)
        # - "import lib" (in string)
        pattern = rf"^(?!\s*#).*(?:^|\s)(?:import\s+{lib}|from\s+{lib})(?:\s|\.|$)"

        for line in content.split("\n"):
            # Skip lines that are entirely comments
            stripped = line.strip()
            if stripped.startswith("#"):
                continue

            # Skip if the import keyword is inside a string
            # Simple heuristic: check if 'import' or 'from' appears before any quote
            if re.search(pattern, line, re.IGNORECASE):
                # Additional check: make sure it's not in a string
                # Find position of import/from keyword
                import_match = re.search(rf"\b(import\s+{lib}|from\s+{lib})\b", line)
                if import_match:
                    prefix = line[: import_match.start()]
                    # Count quotes before the match - odd count means we're inside a string
                    single_quotes = prefix.count("'") - prefix.count("\\'")
                    double_quotes = prefix.count('"') - prefix.count('\\"')
                    if single_quotes % 2 == 0 and double_quotes % 2 == 0:
                        detected.add(lib)
                        break  # Found this lib, move to next

    return detected


def get_session_cache_path(session_id: str) -> Path:
    """Get the path to the session cache file.

    Args:
        session_id: Unique identifier for the current Claude session.

    Returns:
        Path to the cache file in the temp directory.
    """
    return Path(tempfile.gettempdir()) / f"claude-libdocs-fetched-{session_id}"


def is_library_fetched(library: str, session_id: str) -> bool:
    """Check if a library's docs have already been fetched this session.

    Args:
        library: Name of the library (e.g., 'geoparquet_io').
        session_id: Unique identifier for the current Claude session.

    Returns:
        True if the library was already fetched, False otherwise.
    """
    cache_path = get_session_cache_path(session_id)
    if not cache_path.exists():
        return False

    fetched_libs = cache_path.read_text().strip().split("\n")
    return library in fetched_libs


def mark_library_fetched(library: str, session_id: str) -> None:
    """Mark a library as fetched in the session cache.

    Args:
        library: Name of the library (e.g., 'geoparquet_io').
        session_id: Unique identifier for the current Claude session.
    """
    cache_path = get_session_cache_path(session_id)

    # Append to existing cache or create new
    if cache_path.exists():
        existing = cache_path.read_text().strip()
        if library not in existing.split("\n"):
            cache_path.write_text(f"{existing}\n{library}")
    else:
        cache_path.write_text(library)


def get_unfetched_libraries(detected: set[str], session_id: str) -> set[str]:
    """Filter detected libraries to only those not yet fetched this session.

    Args:
        detected: Set of detected library imports.
        session_id: Unique identifier for the current Claude session.

    Returns:
        Set of libraries that need to be fetched.
    """
    return {lib for lib in detected if not is_library_fetched(lib, session_id)}


def get_repo_url(library: str) -> str | None:
    """Get the GitHub repository URL for a library.

    Args:
        library: Name of the library (e.g., 'geoparquet_io').

    Returns:
        GitHub URL string, or None if library is not tracked.
    """
    return REPO_MAPPING.get(library)


def fetch_docs(library: str) -> str | None:
    """Fetch documentation for a library using gitingest.

    Args:
        library: Name of the library (e.g., 'geoparquet_io').

    Returns:
        Documentation content as a string, or None on error.
    """
    url = get_repo_url(library)
    if url is None:
        return None

    try:
        from gitingest import ingest

        summary, tree, content = ingest(url)
        # Combine all parts into a useful format
        return f"# {library} Documentation\n\n## Summary\n{summary}\n\n## Structure\n{tree}\n\n## Content\n{content}"
    except ImportError:
        logger.warning("gitingest not installed, skipping doc fetch")
        return None
    except Exception as e:
        logger.warning(f"Failed to fetch docs for {library}: {e}")
        return None


def compress_with_distill(content: str) -> str:
    """Compress content using Distill MCP if available.

    Args:
        content: Documentation content to compress.

    Returns:
        Compressed content, or original if Distill unavailable.
    """
    # Distill MCP is called externally by the hook script
    # This is a placeholder for potential future Python-based compression
    return content


def process_hook_input(hook_input: dict[str, object], session_id: str) -> dict[str, str] | None:
    """Process hook input and return docs for detected libraries.

    Args:
        hook_input: JSON input from Claude hook system.
        session_id: Unique identifier for the current Claude session.

    Returns:
        Dict with 'systemMessage' key containing docs, or None if no docs to inject.
    """
    # Extract file path and content from hook input
    tool_input = hook_input.get("tool_input", {})
    tool_result = hook_input.get("tool_result", {})

    if not isinstance(tool_input, dict) or not isinstance(tool_result, dict):
        return None

    file_path = tool_input.get("file_path", "")
    content = tool_result.get("content", "")

    if not isinstance(file_path, str) or not isinstance(content, str):
        return None

    # Only process Python files
    if not file_path.endswith(".py"):
        return None

    # Detect imports
    detected = detect_imports(content)
    if not detected:
        return None

    # Filter to unfetched libraries
    to_fetch = get_unfetched_libraries(detected, session_id)
    if not to_fetch:
        return None

    # Fetch docs for each library
    all_docs: list[str] = []
    for lib in to_fetch:
        docs = fetch_docs(lib)
        if docs:
            mark_library_fetched(lib, session_id)
            all_docs.append(docs)

    if not all_docs:
        return None

    # Combine all docs
    combined = "\n\n---\n\n".join(all_docs)
    message = f"<auto-fetched-docs>\nThe following API documentation was auto-fetched for libraries detected in the file you just read:\n\n{combined}\n</auto-fetched-docs>"

    return {"systemMessage": message}


def main() -> None:
    """Main entry point for CLI usage."""
    # Read JSON from stdin (hook input)
    try:
        hook_input = json.load(sys.stdin)
    except json.JSONDecodeError:
        sys.exit(0)  # Silent failure

    # Get session ID from environment or generate one
    import os

    session_id = os.environ.get("CLAUDE_SESSION_ID", "default-session")

    result = process_hook_input(hook_input, session_id)
    if result:
        print(json.dumps(result))


if __name__ == "__main__":
    main()
