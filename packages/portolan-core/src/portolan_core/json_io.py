"""Atomic JSON writing for every module that persists STAC or Portolan JSON.

Stdlib-only apart from ``trace``, so leaf modules can import it; the perf gates count writes here.
"""

from __future__ import annotations

import contextlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from portolan_core.trace import count

#: The file names whose writes ``trace`` counts, and the counter for each.
_COUNTED_WRITES = {
    "versions.json": "versions_json.writes",
    "collection.json": "collection_json.writes",
}


def write_json_atomic(path: Path, data: Any) -> None:
    """Write ``data`` to ``path`` as UTF-8 JSON through a temp file and ``os.replace``.

    Readers see the old content or the full new content, never a truncated file.

    Args:
        path: Destination file. Parent directories are created if needed.
        data: Any JSON-serializable object.

    Raises:
        TypeError: If ``data`` is not JSON-serializable (nothing is written).
        OSError: If the temporary file cannot be written or renamed.
    """
    write_text_atomic(path, json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    counter = _COUNTED_WRITES.get(path.name)
    if counter is not None:
        count(counter)


def write_text_atomic(path: Path, content: str) -> None:
    """Write ``content`` verbatim to ``path`` atomically, UTF-8.

    The non-JSON half of :func:`write_json_atomic`, for files such as ``config.yaml``.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp_name = tempfile.mkstemp(
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    )
    tmp_path = Path(tmp_name)
    try:
        # newline="" stops Windows writing \r\n, so catalogs stay byte-identical.
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(content)
        # Atomic: POSIX renames within one filesystem are atomic.
        tmp_path.replace(path)
    except BaseException:
        with contextlib.suppress(OSError):
            tmp_path.unlink()
        raise
