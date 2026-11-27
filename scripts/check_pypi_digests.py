"""Fail when PyPI holds a file of this release with other bytes than the local build.

Usage: ``python scripts/check_pypi_digests.py PROJECT VERSION DIR``.
"""

from __future__ import annotations

import hashlib
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path


def mismatches(local: dict[str, str], published: dict[str, str]) -> list[str]:
    """Return each filename that PyPI holds with another sha256, sorted."""
    return sorted(name for name, digest in local.items() if published.get(name, digest) != digest)


def published_digests(project: str, version: str) -> dict[str, str]:
    """Return the sha256 of each file that PyPI holds for the release. No release gives {}."""
    url = f"https://pypi.org/pypi/{project}/{version}/json"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            release = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {}
        raise
    return {file["filename"]: file["digests"]["sha256"] for file in release["urls"]}


def main(argv: list[str]) -> int:
    """Compare the files in DIR with PyPI. Return 1 when one has other bytes."""
    project, version, directory = argv
    local = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(Path(directory).iterdir())
        if path.name.endswith((".whl", ".tar.gz"))
    }
    if not local:
        print(f"{directory} holds no wheel or sdist", file=sys.stderr)
        return 1
    published = published_digests(project, version)
    bad = mismatches(local, published)
    for name in bad:
        print(
            f"PyPI holds {name} with sha256 {published[name]}, "
            f"the local build has {local[name]}. Do not publish this build.",
            file=sys.stderr,
        )
    if not bad:
        held = sorted(set(local) & set(published))
        print(f"{project} {version}: {len(local)} files, {len(held)} on PyPI with the same bytes")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
