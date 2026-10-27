"""Conformance checks for plugin authors (#947).

``isinstance`` on a ``runtime_checkable`` protocol checks member names only.
These checks call the members and validate what they return. Run them in the
plugin's own test suite::

    from portolan_cli.testing import check_backend


    def test_backend_conforms(tmp_path):
        check_backend(ExampleBackend, tmp_path)

``check_extractor`` checks an extractor the same way. Pass a source and an
output directory to run ``extract()`` too.

A failed check raises ``ConformanceError`` with one line per problem. This
module follows semantic versioning, as ``portolan_cli.api`` does.
"""

from __future__ import annotations

import contextlib
import re
from datetime import UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any, get_type_hints

if TYPE_CHECKING:
    from collections.abc import Callable

__all__ = ["ConformanceError", "check_backend", "check_extractor", "check_format_handler"]

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
    if handler.output_path(sample, output_dir) != output:
        problems.append("output_path does not name the file that convert wrote.")
    problems += _check_describe(handler, sample, output, metadata, output_dir)
    problems += _check_optional_methods(handler, output)
    if problems:
        raise ConformanceError("\n".join(problems))


def _check_describe(
    handler: Any, sample: Path, output: Path, metadata: Any, output_dir: Path
) -> list[str]:
    """Run ``describe_item``, ``describe_collection``, and ``inspect`` on stand-in objects."""
    from datetime import datetime

    import pystac

    from portolan_cli.plugins.formats import FormatContext
    from portolan_cli.stac import build_spatial_extent

    context = FormatContext(catalog_root=output_dir, collection_dir=output_dir, source=sample)
    problems: list[str] = []
    bbox = [float(v) for v in metadata.bbox]
    item = pystac.Item(
        id="conformance",
        geometry=None,
        bbox=bbox,
        datetime=datetime.now(UTC),
        properties=dict(metadata.to_stac_properties()),
        assets={"data": pystac.Asset(href=f"./{output.name}", media_type=handler.media_type)},
    )
    handler.describe_item(item, output, metadata, context)
    if item.id != "conformance" or "data" not in item.assets:
        problems.append("describe_item removed the item id or its data asset.")
    collection = pystac.Collection(
        id="conformance",
        description="Conformance check",
        extent=pystac.Extent(
            build_spatial_extent([bbox], wgs84_only=False),
            pystac.TemporalExtent([[datetime.now(UTC), None]]),
        ),
    )
    handler.describe_collection(collection, [metadata], context)
    if collection.id != "conformance":
        problems.append("describe_collection changed the collection id.")
    # inspect may raise ValueError for a format ``portolan info`` does not read.
    with contextlib.suppress(ValueError):
        handler.inspect(output, None)
    return problems


def _check_optional_methods(handler: Any, output: Path) -> list[str]:
    """Check ``partition`` and ``reoptimize`` when the handler has them."""
    problems: list[str] = []
    partition = getattr(handler, "partition", None)
    if partition is not None and not isinstance(partition(output, None), list):
        problems.append("partition returned something other than a list.")
    for method_name in ("reoptimize", "reconvert"):
        method = getattr(handler, method_name, None)
        if method is not None and output.exists():
            result = method(output, None)
            if not isinstance(result, Path) or not result.exists():
                problems.append(f"{method_name} returned {result!r}, expected an existing Path.")
    return problems


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

    # The backend under test is plugin code. Its return values are object
    # until checked, so the isinstance checks below stay reachable for mypy.
    first: object = backend.publish(
        _COLLECTION, {"data.parquet": str(data)}, schema, False, "first"
    )
    second: object = backend.publish(
        _COLLECTION, {"data.parquet": str(data)}, schema, True, "second"
    )
    if not isinstance(first, Version) or not isinstance(second, Version):
        raise ConformanceError(
            "\n".join(
                f"publish returned {type(value).__name__}, expected Version."
                for value in (first, second)
                if not isinstance(value, Version)
            )
        )
    problems: list[str] = []
    if first.version == second.version:
        problems.append(f"publish returned version {first.version} twice.")

    current: object = backend.get_current_version(_COLLECTION)
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


def check_extractor(
    extractor: Any,
    source: str | None = None,
    output_dir: Path | None = None,
    **options: Any,
) -> None:
    """Check an extractor against ``Extractor``.

    The check reads ``name`` and ``summary`` and builds the Click command.
    With ``source`` and ``output_dir``, it also runs ``extract()`` and checks
    the report.

    Args:
        extractor: What ``portolan_extractors`` returns.
        source: A source to extract, or None to skip the run.
        output_dir: An empty directory for the run.
        **options: Options for ``extract()``, such as ``license``.

    Raises:
        ConformanceError: With one line per problem found.
    """
    from portolan_cli.extract.common.report import ExtractionReport
    from portolan_cli.plugins.extractors import Extractor

    if not isinstance(extractor, Extractor):
        raise ConformanceError(f"{type(extractor).__name__} does not implement Extractor.")

    problems: list[str] = []
    if not isinstance(extractor.name, str) or not re.fullmatch(r"[a-z][a-z0-9-]*", extractor.name):
        problems.append(f"name {extractor.name!r} is not a lowercase command name.")
    if not isinstance(extractor.summary, str) or not extractor.summary.strip():
        problems.append("summary is empty.")
    try:
        command = extractor.cli_command()
    except Exception as err:  # noqa: BLE001 - report any load failure as a problem
        problems.append(f"cli_command() raised {type(err).__name__}: {err}.")
    else:
        _check_command(extractor.name, command, problems)

    if source is not None and output_dir is not None:
        report: object = extractor.extract(source, output_dir, **options)
        if not isinstance(report, ExtractionReport):
            problems.append(
                f"extract() returned {type(report).__name__}, expected ExtractionReport."
            )
        elif not (output_dir / ".portolan" / "extraction-report.json").is_file():
            problems.append("extract() wrote no .portolan/extraction-report.json.")

    if problems:
        raise ConformanceError("\n".join(problems))


def _check_command(name: str, command: object, problems: list[str]) -> None:
    import click

    if not isinstance(command, click.Command):
        problems.append(
            f"cli_command() returned {type(command).__name__}, expected a Click command."
        )
    elif command.name != name:
        problems.append(f"cli_command() is named {command.name!r}, expected {name!r}.")
