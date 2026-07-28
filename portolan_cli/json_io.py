"""Atomic JSON writing shared by every module that persists STAC/Portolan JSON.

One helper, :func:`write_json_atomic`, so that every JSON file Portolan writes
lands the same way: UTF-8, two-space indent, unescaped non-ASCII, a trailing
newline, and an atomic ``os.replace`` so an interrupted write can never leave a
half-written ``collection.json`` behind.

Deliberately stdlib-only, so validation and generation paths alike can import it
without dragging ``click``/``rich``/``config`` into a leaf module.
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def write_json_atomic(path: Path, data: Any) -> None:
    """Serialize ``data`` to ``path`` as JSON, atomically.

    Writes to a temporary file in the destination's own directory and then
    ``os.replace``s it into position, so readers observe either the previous
    content or the complete new content — never a truncated file. The temporary
    file is removed if serialization or writing fails, and ``path`` is left
    untouched in that case.

    Output format (identical at every call site):
    ``json.dumps(data, indent=2, ensure_ascii=False)`` plus a trailing newline,
    encoded UTF-8. ``ensure_ascii=False`` keeps accented titles ("Córdoba")
    literal instead of ``\\u00f3`` escapes.

    Args:
        path: Destination file. Parent directories are created if needed.
        data: Any JSON-serializable object.

    Raises:
        TypeError: If ``data`` is not JSON-serializable (nothing is written).
        OSError: If the temporary file cannot be written or renamed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
        # Atomic rename (POSIX guarantees atomicity for same-filesystem renames).
        os.replace(tmp_path, path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
