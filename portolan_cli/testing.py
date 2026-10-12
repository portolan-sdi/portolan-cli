"""Conformance checks for plugin authors (#947).

``isinstance`` on a ``runtime_checkable`` protocol checks member names only.
These checks call the members and validate what they return. Run them in the
plugin's own test suite::

    from portolan_cli.testing import check_backend


    def test_backend_conforms(tmp_path):
        check_backend(ExampleBackend, tmp_path)

A failed check raises ``ConformanceError`` with one line per problem. This
module follows semantic versioning, as ``portolan_cli.api`` does.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

__all__ = ["ConformanceError", "check_backend"]

_COLLECTION = "conformance"


class ConformanceError(AssertionError):
    """A plugin object does not meet its protocol."""


def check_backend(factory: Callable[..., Any], catalog_root: Path) -> None:
    """Check a versioning backend factory against ``VersioningBackend``.

    The check creates the backend in ``catalog_root``, publishes two versions
    of one collection, and reads them back. ``rollback`` and ``prune`` may
    raise ``NotImplementedError``, as the built-in ``file`` backend does.

    Args:
        factory: What ``portolan_backends`` returns for the backend name.
        catalog_root: An empty directory the backend may write to.

    Raises:
        ConformanceError: With one line per problem found.
    """
    from portolan_cli.backends.protocol import DriftReport, SchemaFingerprint, VersioningBackend
    from portolan_cli.versions import Version

    backend = factory(catalog_root=catalog_root)
    if not isinstance(backend, VersioningBackend):
        raise ConformanceError(f"{type(backend).__name__} does not implement VersioningBackend.")

    data = catalog_root / _COLLECTION / "data.parquet"
    data.parent.mkdir(parents=True, exist_ok=True)
    data.write_bytes(b"conformance")
    schema = SchemaFingerprint(columns=["id"], types={"id": "int64"}, hash="conformance")

    problems: list[str] = []
    first = backend.publish(_COLLECTION, {"data.parquet": str(data)}, schema, False, "first")
    second = backend.publish(_COLLECTION, {"data.parquet": str(data)}, schema, True, "second")
    for label, value in (("publish", first), ("publish", second)):
        if not isinstance(value, Version):
            problems.append(f"{label} returned {type(value).__name__}, expected Version.")
    if problems:
        raise ConformanceError("\n".join(problems))
    if first.version == second.version:
        problems.append(f"publish returned version {first.version} twice.")

    current = backend.get_current_version(_COLLECTION)
    if not isinstance(current, Version):
        problems.append(f"get_current_version returned {type(current).__name__}, expected Version.")
    elif current.version != second.version:
        problems.append(
            f"get_current_version returned {current.version}, expected {second.version}."
        )

    history = backend.list_versions(_COLLECTION)
    wrong = sorted({type(v).__name__ for v in history if not isinstance(v, Version)})
    if wrong:
        problems.append(f"list_versions returned {', '.join(wrong)} items, expected Version.")
    elif [v.version for v in history] != [first.version, second.version]:
        problems.append(
            f"list_versions returned {[v.version for v in history]}, "
            f"expected {[first.version, second.version]}."
        )

    problems += _check_drift_report(backend.check_drift(_COLLECTION), DriftReport)
    problems += _check_optional(backend, first.version)

    if problems:
        raise ConformanceError("\n".join(problems))


def _check_drift_report(report: Any, report_type: type) -> list[str]:
    if not isinstance(report, dict):
        return [f"check_drift returned {type(report).__name__}, expected a DriftReport dict."]
    missing = sorted(set(get_type_hints(report_type)) - set(report))
    if missing:
        return [f"check_drift is missing the keys: {', '.join(missing)}."]
    return []


def _check_optional(backend: Any, first_version: str) -> list[str]:
    """Check ``prune`` and ``rollback`` when the backend implements them."""
    from portolan_cli.versions import Version

    problems: list[str] = []
    try:
        pruned = backend.prune(_COLLECTION, keep=2, dry_run=True)
    except NotImplementedError:
        pass
    else:
        if not isinstance(pruned, list) or any(not isinstance(v, Version) for v in pruned):
            problems.append("prune returned something other than a list of Version.")
    try:
        rolled_back = backend.rollback(_COLLECTION, first_version)
    except NotImplementedError:
        pass
    else:
        if not isinstance(rolled_back, Version):
            problems.append(f"rollback returned {type(rolled_back).__name__}, expected Version.")
    return problems
