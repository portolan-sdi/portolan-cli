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

from pathlib import Path
from typing import TYPE_CHECKING, Any, get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["ConformanceError", "check_backend", "check_format_handler"]

_COLLECTION = "conformance"
_ASSET_LEVELS = frozenset({"collection", "item"})


class ConformanceError(AssertionError):
    """A plugin object does not meet its protocol."""


def check_format_handler(handler: Any, sample: Path, output_dir: Path) -> None:
    """Check a format handler against ``FormatHandler`` with a sample file.

    The check reads the handler attributes, converts ``sample`` into
    ``output_dir``, and reads the metadata of the result.

    Args:
        handler: What ``portolan_formats`` returns.
        sample: A small source file in a format the handler claims.
        output_dir: An empty directory the handler may write to.

    Raises:
        ConformanceError: With one line per problem found.
    """
    from portolan_cli.plugins.formats import FormatHandler, FormatMetadata, extension_of

    if not isinstance(handler, FormatHandler):
        raise ConformanceError(f"{type(handler).__name__} does not implement FormatHandler.")

    problems = _check_handler_attributes(handler)
    if extension_of(sample) not in handler.extensions:
        problems.append(f"extensions does not hold {extension_of(sample)}, the sample's.")
    if not handler.claims(sample):
        problems.append(f"claims returned False for the sample {sample.name}.")
    if problems:
        raise ConformanceError("\n".join(problems))

    output = handler.convert(sample, output_dir, None)
    if not isinstance(output, Path) or not output.exists():
        raise ConformanceError(f"convert returned {output!r}, expected an existing Path.")

    metadata = handler.read_metadata(output)
    if not isinstance(metadata, FormatMetadata):
        raise ConformanceError(
            f"read_metadata returned {type(metadata).__name__}, expected FormatMetadata."
        )
    bbox = list(metadata.bbox or [])
    if len(bbox) != 4 or not all(isinstance(v, (int, float)) for v in bbox):
        problems.append(f"read_metadata bbox is {bbox!r}, expected four numbers.")
    elif bbox[0] > bbox[2] or bbox[1] > bbox[3]:
        problems.append(f"read_metadata bbox {bbox!r} has a minimum above its maximum.")
    if not isinstance(metadata.to_stac_properties(), dict):
        problems.append("to_stac_properties returned something other than a dict.")
    if problems:
        raise ConformanceError("\n".join(problems))


def _check_handler_attributes(handler: Any) -> list[str]:
    problems: list[str] = []
    for attribute in ("name", "family", "display_name", "media_type"):
        value = getattr(handler, attribute)
        if not isinstance(value, str) or not value:
            problems.append(f"{attribute} is {value!r}, expected a non-empty string.")
    extensions = handler.extensions
    if not isinstance(extensions, frozenset) or not extensions:
        problems.append("extensions must be a non-empty frozenset.")
    else:
        wrong = sorted(e for e in extensions if not e.startswith(".") or e != e.lower())
        if wrong:
            problems.append(f"extensions must be lowercase and start with a dot: {wrong}.")
    if handler.asset_level not in _ASSET_LEVELS:
        problems.append(f"asset_level is {handler.asset_level!r}, expected collection or item.")
    return problems


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
