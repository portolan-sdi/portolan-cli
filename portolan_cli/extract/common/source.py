"""LayerSource: a data source made of layers, and the driver that extracts it (#947).

A service that serves named layers implements ``LayerSource``.
``run_layer_extraction`` runs it through the extraction lifecycle: filters,
the license check, resume, fetch with retry (one layer at a time or in
parallel), the extraction report, catalog setup, styles, provenance links,
and collection metadata. ``extractor_from`` wraps a ``LayerSource`` as an
``Extractor``, so it becomes ``portolan extract <name>``.

The built-in ArcGIS, Carto, and WFS extractors are ``LayerSource``
implementations too. Source-specific work goes through the protocol:

- ``discover`` does everything that happens once per run, such as WFS
  version negotiation or ArcGIS services-root traversal. It returns a
  ``Discovery`` with the layers and the catalog-level metadata.
- Each ``LayerRef`` carries the collection path and the source's own
  ``data`` for the layer, such as a service URL or a negotiated version.
- ``LayerSourceOptions.workers`` and ``layer_timeout`` select parallel
  downloads with a per-layer deadline.
"""

from __future__ import annotations

import contextlib
import importlib
import logging
import re
import shutil
import threading
import time
import uuid
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

from portolan_cli.errors import PortolanError
from portolan_cli.trace import span, submit

if TYPE_CHECKING:
    from collections.abc import Callable

    import click

    from portolan_cli.extract.common.progress import ExtractionProgress
    from portolan_cli.extract.common.report import (
        ExtractionReport,
        FolderCoverage,
        LayerResult,
        MetadataExtracted,
    )
    from portolan_cli.licensing import ResolvedLicense
    from portolan_cli.metadata_extraction import ExtractedMetadata

logger = logging.getLogger(__name__)

# Collection metadata fetches run in parallel, as WFS ISO record fetches did.
_METADATA_WORKERS = 4


class EmptyLayerError(Exception):
    """Raised by ``fetch`` when the layer has no features.

    The driver does not retry it. A layer that the user did not name is
    recorded as ``empty``, and a resumed run does not try it again. A layer
    that the user named is ``failed`` (issue #1034).
    """


class NoLayersSelectedError(PortolanError):
    """Raised when a run selects no layer to extract (issue #1034).

    Error code: PRTLN-EXT003
    """

    code = "PRTLN-EXT003"


@dataclass(frozen=True)
class LayerRef:
    """One layer that a source serves, and the one identity of that layer.

    Attributes:
        id: Layer id as the source knows it, such as an ArcGIS layer id. A
            numeric filter pattern matches it (issue #1003). The report
            records it.
        name: Qualified layer name as the source knows it, such as a WFS
            ``workspace:layer``. Filters match it and its leaf after the
            last ``:`` (issue #768). Resume matches on it.
        path: Collection directory relative to the catalog, ``/``-separated.
            The data file is ``<path>/<last segment>.parquet``. None uses a
            slug of the name.
        data: The source's own data for the layer, for ``fetch`` and the
            other methods. The driver does not read it.
        notes: Warnings that a dry run reports for the layer.
        skip_reason: Why the layer cannot be fetched, such as a source with
            no query support. The driver records the layer as ``skipped``
            and does not call ``fetch``. None means fetch the layer.
        kind: ``layer`` for a spatial layer, ``table`` for a table with no
            geometry, ``raster`` for an image layer.
        explicit: True when the user named this layer, with a ``--layers``
            name or id, or with a layer URL. A named layer with 0 features
            is ``failed``, not ``empty`` (issue #1034). ``filter_refs`` sets
            it for a name match. A source sets it for a layer URL.
    """

    id: int
    name: str
    path: str | None = None
    data: Any = None
    notes: tuple[str, ...] = ()
    skip_reason: str | None = None
    kind: Literal["layer", "table", "raster"] = "layer"
    explicit: bool = False


@dataclass
class Discovery:
    """What ``discover`` found at a source.

    Attributes:
        source_url: URL that the report records and that the other methods
            receive as ``source``.
        layers: Layers in discovery order.
        metadata: Service metadata for the report. None records the URL only.
        catalog_seed: Metadata that seeds the catalog ``metadata.yaml``.
        license_url: License URL that the source publishes, if any.
        catalog_title: Title for a new catalog, or None for the default.
        catalog_description: Description for a new catalog, or None.
        root_metadata: ``(title, description)`` written to ``catalog.json``
            after a new catalog is created, or None to leave it.
        folder_coverage: Folders that a services-root traversal visited.
        empty_reason: Why ``layers`` is empty, for the error that a run with
            no layers raises, such as a service that lists only tables.
        filtered: True when ``discover`` already applied the layer filters
            with ``filter_refs``. The driver then does not filter again.
    """

    source_url: str
    layers: list[LayerRef]
    metadata: MetadataExtracted | None = None
    catalog_seed: ExtractedMetadata | None = None
    license_url: str | None = None
    catalog_title: str | None = None
    catalog_description: str | None = None
    root_metadata: tuple[str | None, str | None] | None = None
    folder_coverage: FolderCoverage | None = None
    empty_reason: str | None = None
    filtered: bool = False


@dataclass(frozen=True)
class LayerMetadata:
    """Descriptive metadata for one layer's collection.

    Attributes:
        title: Collection title.
        description: Collection description.
        keywords: Discovery keywords.
        layer_name: Layer name for the processing notes. None uses the name.
        source_type: ``source_type`` in ``metadata.yaml``. None uses the
            source's name.
        source_url: Layer URL for provenance. None uses the source link.
        record: A full metadata record, such as an ISO 19139 record. It
            replaces the fields above in ``metadata.yaml``, and ``title``
            and ``description`` go to ``collection.json``.
        record_label: Name of the record kind for the progress message.
    """

    title: str | None = None
    description: str | None = None
    keywords: list[str] | None = None
    layer_name: str | None = None
    source_type: str | None = None
    source_url: str | None = None
    record: ExtractedMetadata | None = None
    record_label: str = "metadata record"


@dataclass(frozen=True)
class SourceLink:
    """A provenance ``via`` link from a collection to the original layer."""

    href: str
    title: str


@dataclass(frozen=True)
class LayerSourceOptions:
    """Options for ``run_layer_extraction``.

    Attributes:
        layer_filter: Glob patterns of layer names to include. None includes all.
        layer_exclude: Glob patterns of layer names to exclude.
        resume: Skip layers that succeeded or were empty in the previous report.
        dry_run: Report the layers without writing anything.
        raw: Write the layer files and the report, but no catalog.
        retries: Attempts per layer.
        retry_delay: Seconds before the first retry. Later retries double it.
        workers: Layers to fetch at the same time.
        layer_timeout: With more than one worker, seconds a layer may run
            before it is recorded as failed. None sets no limit.
        catalog_id: Catalog id. None derives it from the output directory.
        license: SPDX license identifier, or ``other`` with ``license_url``.
        license_url: URL of the license text.
    """

    layer_filter: list[str] | None = None
    layer_exclude: list[str] | None = None
    resume: bool = False
    dry_run: bool = False
    raw: bool = False
    retries: int = 3
    retry_delay: float = 1.0
    workers: int = 1
    layer_timeout: float | None = None
    catalog_id: str | None = None
    license: str | None = None
    license_url: str | None = None


@runtime_checkable
class LayerSource(Protocol):
    """A data source that serves named layers.

    Attributes:
        name: The subcommand name, ``portolan extract <name>``. It is also
            the default ``source_type`` in seeded metadata.
        summary: One line that describes the source.
    """

    name: str
    summary: str

    def discover(self, source: str, options: LayerSourceOptions) -> Discovery:
        """Find the layers at ``source`` and the catalog-level metadata.

        Do the work that happens once per run here, such as protocol
        negotiation or a walk through a services root. Put what ``fetch``
        needs for a layer into ``LayerRef.data``.
        """
        ...

    def fetch(self, source: str, layer: LayerRef, dest: Path) -> int:
        """Write ``layer`` to ``dest`` as Parquet. Return the row count.

        Raise ``EmptyLayerError`` for a layer with no features. Raise any
        other exception for a failure. The driver retries a failure only.
        """
        ...

    def layer_metadata(self, source: str, layer: LayerRef) -> LayerMetadata | None:
        """Return the metadata for the layer's collection, or None to skip it."""
        ...

    def styles(self, source: str, layer: LayerRef, collection_dir: Path) -> None:
        """Write style or legend files next to the layer's data, or do nothing."""
        ...

    def source_link(self, source: str, layer: LayerRef) -> SourceLink | None:
        """Return the ``via`` link to the original layer, or None."""
        ...


def slugify(name: str) -> str:
    """Convert a name to a filesystem-safe slug, ``unnamed`` when nothing is left."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower())
    slug = re.sub(r"_+", "_", slug).strip("_")
    return slug or "unnamed"


def layer_path(layer: LayerRef) -> str:
    """Return the layer's data file, relative to the catalog.

    Raises:
        InputValidationError: When ``layer.path`` could leave the catalog. It
            must be relative and ``/``-separated, with no empty, ``.``, or
            ``..`` segment. A ``:`` is refused too, because ``C:x`` names a
            Windows drive.
    """
    from portolan_cli.input_hardening import InputValidationError

    collection = layer.path or slugify(layer.name)
    segments = collection.split("/")
    if any(char in collection for char in "\\:") or any(
        part in ("", ".", "..") for part in segments
    ):
        raise InputValidationError(
            f"Layer '{layer.name}' has the collection path {collection!r}, which is not "
            "a relative /-separated path inside the catalog."
        )
    return f"{collection}/{segments[-1]}.parquet"


def filter_refs(
    layers: list[LayerRef], include: list[str] | None, exclude: list[str] | None
) -> list[LayerRef]:
    """Keep the layers that the include and exclude patterns select.

    A pattern matches a layer as ``filters.layer_matches`` says. A layer
    that a non-glob include pattern matches becomes ``explicit``, because
    the user named it. A pattern that matches no layer logs a warning that
    names the pattern and 3 layers of the source (issue #1034).

    Args:
        layers: The discovered layers, in discovery order.
        include: Patterns to keep. None keeps every layer.
        exclude: Patterns to drop, applied after ``include``.

    Returns:
        The kept layers, in discovery order.
    """
    from portolan_cli.extract.common.filters import is_glob, layer_matches

    include = include or []
    exclude = exclude or []
    sample = ", ".join(layer.name for layer in layers[:3]) or "none"
    for pattern in (*include, *exclude):
        if not any(layer_matches(layer.id, layer.name, pattern) for layer in layers):
            logger.warning(
                "Layer pattern %r matches no layer. Layers at the source include: %s",
                pattern,
                sample,
            )
    kept: list[LayerRef] = []
    for layer in layers:
        hits = [p for p in include if layer_matches(layer.id, layer.name, p)]
        if include and not hits:
            continue
        if any(layer_matches(layer.id, layer.name, p) for p in exclude):
            continue
        named = any(not is_glob(p) for p in hits)
        kept.append(replace(layer, explicit=True) if named else layer)
    return kept


def _select_layers(discovery: Discovery, options: LayerSourceOptions) -> list[LayerRef]:
    """Filter the discovered layers. Raise when the run selects none (issue #1034).

    Raises:
        NoLayersSelectedError: When no layer is left. The message names the
            cause: the source lists no layers, or the filters match none.
    """
    layers = discovery.layers
    filters = options.layer_filter or options.layer_exclude
    if not discovery.filtered:
        layers = filter_refs(layers, options.layer_filter, options.layer_exclude)
    if layers:
        return layers
    source = discovery.source_url
    if filters and (discovery.layers or discovery.filtered):
        count = len(discovery.layers)
        noun = "layer" if count == 1 else "layers"
        listed = f" The source lists {count} {noun}." if not discovery.filtered else ""
        raise NoLayersSelectedError(
            f"No layer at {source} matches the layer filters.{listed}", source=source
        )
    reason = discovery.empty_reason or "The source lists no layers."
    raise NoLayersSelectedError(f"No layers to extract from {source}. {reason}", source=source)


def _package_version(name: str) -> str:
    try:
        return version(name)
    except PackageNotFoundError:
        return "unknown"


def build_report(
    discovery: Discovery,
    results: list[LayerResult],
    *,
    wall_duration_seconds: float | None = None,
) -> ExtractionReport:
    """Build the extraction report for ``results``.

    Args:
        discovery: The discovery the results come from.
        results: One result per layer, in discovery order.
        wall_duration_seconds: The wall-clock time of the fetch stage.

    Returns:
        The report with its summary.
    """
    from portolan_cli.extract.common.report import (
        ExtractionReport,
        ExtractionSummary,
        MetadataExtracted,
    )
    from portolan_cli.extract.common.urls import sanitize_published_string

    def scrub(text: str) -> str:
        # The report keeps a local path, because push never publishes .portolan.
        # A credential goes, also from a URL that an error quotes (issue #981).
        return sanitize_published_string(text) or text

    source_url = scrub(discovery.source_url)
    results = [
        replace(r, error=r.error and scrub(r.error), warnings=[scrub(w) for w in r.warnings])
        for r in results
    ]

    summary = ExtractionSummary(
        total_layers=len(results),
        succeeded=sum(1 for r in results if r.status == "success"),
        failed=sum(1 for r in results if r.status == "failed"),
        skipped=sum(1 for r in results if r.status == "skipped"),
        empty=sum(1 for r in results if r.status == "empty"),
        total_features=sum(r.features or 0 for r in results),
        total_size_bytes=sum(r.size_bytes or 0 for r in results),
        total_duration_seconds=sum(r.duration_seconds or 0.0 for r in results),
        wall_duration_seconds=wall_duration_seconds,
    )
    metadata = replace(discovery.metadata, source_url=source_url) if discovery.metadata else None
    metadata = metadata or MetadataExtracted(
        source_url=source_url,
        description=None,
        attribution=None,
        keywords=None,
        contact_name=None,
        processing_notes=None,
        known_issues=None,
        license_info_raw=None,
    )
    return ExtractionReport(
        extraction_date=datetime.now(timezone.utc).isoformat(),
        source_url=source_url,
        portolan_version=_package_version("portolan-cli"),
        gpio_version=_package_version("geoparquet-io"),
        metadata_extracted=metadata,
        layers=results,
        summary=summary,
        folder_coverage=discovery.folder_coverage,
    )


def _result(layer: LayerRef, status: str, **fields: Any) -> LayerResult:
    from portolan_cli.extract.common.report import LayerResult

    values: dict[str, Any] = {
        "features": None,
        "size_bytes": None,
        "duration_seconds": None,
        "output_path": None,
        "warnings": [],
        "error": None,
        "attempts": 0,
    }
    values.update(fields)
    return LayerResult(id=layer.id, name=layer.name, status=status, **values)


def _skipped(layer: LayerRef) -> LayerResult:
    return _result(
        layer,
        "skipped",
        features=0,
        size_bytes=0,
        duration_seconds=0.0,
        output_path="",
        error=layer.skip_reason,
    )


def _pending(layer: LayerRef) -> LayerResult:
    if layer.skip_reason:
        return _skipped(layer)
    return _result(
        layer,
        "pending",
        features=0,
        size_bytes=0,
        duration_seconds=0.0,
        output_path="",
        warnings=list(layer.notes),
    )


@dataclass(frozen=True)
class _Staged:
    """A fetched layer before it moves into the catalog."""

    result: LayerResult
    path: Path | None


@dataclass
class _Run:
    """One extraction: the source, its discovery, and where it writes."""

    source: LayerSource
    discovery: Discovery
    output_dir: Path
    options: LayerSourceOptions
    on_progress: Callable[[ExtractionProgress], None] | None
    # Data files of the layers that timed out. Their retries stop.
    abandoned: set[str] = field(default_factory=set)
    # Set after the fetch step. A late worker then removes the staging root.
    ended: threading.Event = field(default_factory=threading.Event)

    def fetch_layer(self, layer: LayerRef) -> LayerResult:
        """Fetch one layer with retry, move it into place, and return its result."""
        return self.promote(self.stage_layer(layer))

    def stage_layer(self, layer: LayerRef) -> _Staged:
        """Stage one layer inside an ``extract.layer`` span (issue #991)."""
        with span("extract.layer", layer=layer.name) as stage:
            staged = self._stage_layer(layer)
            result = staged.result
            stage.set(
                status=result.status,
                features=result.features,
                duration_seconds=result.duration_seconds,
                attempts=result.attempts,
            )
            return staged

    def _stage_layer(self, layer: LayerRef) -> _Staged:
        """Fetch one layer with retry, and write its styles, into a staging tree.

        A worker thread runs this step. Each layer gets its own tree under
        ``.portolan/staging``, so nested collection paths do not mix. The
        catalog changes only in ``promote``, so a timed-out fetch cannot
        change it.
        """
        from portolan_cli.extract.common.retry import RetryConfig, retry_with_backoff

        relative = layer_path(layer)
        root = self.output_dir / ".portolan" / "staging" / uuid.uuid4().hex
        staged = root / relative
        source_url = self.discovery.source_url

        def attempt() -> tuple[int, float]:
            if relative in self.abandoned:
                raise TimeoutError(f"Layer {layer.name} timed out")
            staged.parent.mkdir(parents=True, exist_ok=True)
            start = time.monotonic()
            rows = self.source.fetch(source_url, layer, staged)
            if rows == 0:
                # A source that writes 0 rows has no features, as one that raises.
                raise EmptyLayerError(f"Layer {layer.name} has no features")
            return rows, time.monotonic() - start

        outcome = retry_with_backoff(
            attempt,
            RetryConfig(
                max_attempts=max(self.options.retries, 1), initial_delay=self.options.retry_delay
            ),
            give_up_on=(EmptyLayerError,),
            on_retry=lambda count, err: logger.debug(
                "Retry %d for layer %s: %s", count, layer.name, err
            ),
        )
        if outcome.success and outcome.value is not None and relative not in self.abandoned:
            self._styles(layer, staged.parent)
        if relative in self.abandoned:
            # The run gave up on the layer and can have cleaned up already.
            shutil.rmtree(root, ignore_errors=True)
            if self.ended.is_set():
                # Other workers make their trees under it while the run lasts.
                with contextlib.suppress(OSError):
                    root.parent.rmdir()
            return _Staged(_result(layer, "failed", error="Timed out"), None)
        if outcome.success and outcome.value is not None:
            rows, duration = outcome.value
            result = _result(
                layer,
                "success",
                features=rows,
                size_bytes=staged.stat().st_size if staged.exists() else 0,
                duration_seconds=duration,
                output_path=relative,
                attempts=outcome.attempts,
            )
            return _Staged(result, staged)
        shutil.rmtree(root, ignore_errors=True)
        error = str(outcome.error) if outcome.error else "Unknown error"
        if isinstance(outcome.error, EmptyLayerError) and layer.explicit:
            # The user asked for this layer, so no data is a failure (issue #963).
            error = f"Layer {layer.name} returned 0 features, and the run named it: {error}"
        elif isinstance(outcome.error, EmptyLayerError):
            logger.warning("Layer %s is empty (0 features), skipping", layer.name)
            return _Staged(
                _result(
                    layer,
                    "empty",
                    features=0,
                    size_bytes=0,
                    duration_seconds=0.0,
                    warnings=["Layer has no features"],
                    error=error,
                    attempts=outcome.attempts,
                ),
                None,
            )
        return _Staged(_result(layer, "failed", error=error, attempts=outcome.attempts), None)

    def promote(self, staged: _Staged) -> LayerResult:
        """Move a successful layer's data file and styles into its collection."""
        result = staged.result
        if staged.path is None or result.output_path is None:
            return result
        collection = (self.output_dir / result.output_path).parent
        for path in sorted(staged.path.parent.rglob("*")):
            if path.is_file():
                target = collection / path.relative_to(staged.path.parent)
                target.parent.mkdir(parents=True, exist_ok=True)
                path.replace(target)
        collection.mkdir(parents=True, exist_ok=True)
        return result

    def _styles(self, layer: LayerRef, collection_dir: Path) -> None:
        try:
            self.source.styles(self.discovery.source_url, layer, collection_dir)
        except Exception:  # noqa: BLE001 - a missing style must not fail the layer
            logger.warning(
                "Style extraction failed for layer '%s'; skipping style.", layer.name, exc_info=True
            )

    def emit(
        self, index: int, total: int, layer: LayerRef, status: str, error: str | None = None
    ) -> None:
        from portolan_cli.extract.common.progress import emit_progress

        emit_progress(self.on_progress, index, total, layer.name, status, error=error)


def _previous_results(report_path: Path) -> tuple[dict[str, LayerResult], dict[str, LayerResult]]:
    """Read the previous report: done layers by data file, other layers by name.

    A done layer's output path is normalized to ``/``, because a report
    written on Windows holds backslashes. The by-name map holds the empty
    layers. It also holds the "skipped" layers with no output path, which an
    older Carto report wrote for a table from an earlier run.
    """
    from portolan_cli.extract.common.report import load_report

    by_path: dict[str, LayerResult] = {}
    by_name: dict[str, LayerResult] = {}
    for result in load_report(report_path).layers:
        if result.status in ("success", "skipped") and result.output_path:
            output_path = PurePath(result.output_path.replace("\\", "/")).as_posix()
            by_path[output_path] = replace(result, output_path=output_path)
        elif result.status in ("empty", "skipped"):
            by_name[result.name] = result
    return by_path, by_name


def _resumed_result(
    layer: LayerRef,
    output_dir: Path,
    done_by_path: dict[str, LayerResult],
    by_name: dict[str, LayerResult],
) -> LayerResult | None:
    """Return the earlier result that a resumed run keeps, or None to fetch."""
    relative = layer_path(layer)
    if relative in done_by_path:
        return done_by_path[relative]
    previous = by_name.get(layer.name)
    if previous is None:
        return None
    if previous.status == "empty":
        # A named layer must have data, so a resumed run tries it again.
        return None if layer.explicit else previous
    # A legacy "skipped" result counts only when its data file is still there.
    # It becomes "success", because the catalog build adds only those.
    data_file = output_dir / relative
    if not data_file.is_file():
        return None
    import pyarrow as pa
    import pyarrow.parquet as pq

    try:
        rows = pq.ParquetFile(data_file).metadata.num_rows
    except (pa.ArrowInvalid, OSError):
        # An interrupted older run can leave a partial file. Fetch it again.
        return None
    # The legacy result holds 0 rows, 0 bytes, and an "Already extracted" error.
    return replace(
        previous,
        status="success",
        output_path=relative,
        features=rows,
        size_bytes=data_file.stat().st_size,
        error=None,
    )


def _fetch_sequential(
    run: _Run, plan: list[tuple[int, LayerRef, LayerResult | None]], total: int
) -> list[tuple[int, LayerResult]]:
    """Fetch the layers in order. A layer with a kept result reports at its own index.

    The ``[i/N]`` index then only increases, also with skipped and resumed
    layers in the run (issue #1012).
    """
    results: list[tuple[int, LayerResult]] = []
    for index, layer, kept in plan:
        if kept is not None:
            _report_kept(run, index, total, layer)
            results.append((index, kept))
            continue
        run.emit(index, total, layer, "starting")
        run.emit(index, total, layer, "extracting")
        result = run.fetch_layer(layer)
        run.emit(
            index, total, layer, result.status, result.error if result.status == "failed" else None
        )
        results.append((index, result))
    return results


def _report_kept(run: _Run, index: int, total: int, layer: LayerRef) -> None:
    """Report a layer that the run does not fetch: a skip reason, or a resumed result."""
    # "starting" prints the "[i/N] <name>" line that the skip line follows.
    run.emit(index, total, layer, "starting")
    run.emit(index, total, layer, "skipped", layer.skip_reason)


def _fetch_parallel(
    run: _Run, jobs: list[tuple[int, LayerRef]], total: int
) -> list[tuple[int, LayerResult]]:
    """Fetch layers on worker threads, with a deadline per started layer.

    A layer's deadline starts when a worker picks it up, not when it is
    queued, so a queued layer is never timed out (issues #508, #526). A
    timed-out thread runs on, but the run does not wait for it. Its staged
    file never moves into the catalog. Python joins the thread when the
    process exits.
    """
    workers = min(run.options.workers, len(jobs))
    timeout = run.options.layer_timeout
    logger.info("Extracting %d layers with %d workers", len(jobs), workers)
    lock = threading.Lock()
    started: dict[int, float] = {}
    results: list[tuple[int, LayerResult]] = []

    def task(index: int, layer: LayerRef) -> _Staged:
        with lock:
            started[index] = time.monotonic()
        return run.stage_layer(layer)

    def deadline(future: Future[_Staged]) -> float | None:
        index = futures[future][0]
        with lock:
            start = started.get(index)
        return None if start is None or timeout is None else start + timeout

    # No context manager: its exit waits for a timed-out thread.
    executor = ThreadPoolExecutor(max_workers=workers)
    timed_out = False
    try:
        futures = {submit(executor, task, index, layer): (index, layer) for index, layer in jobs}
        pending: set[Future[_Staged]] = set(futures)
        while pending:
            now = time.monotonic()
            deadlines = [d for f in pending if (d := deadline(f)) is not None]
            wait_for = max(0.1, min(d - now for d in deadlines)) if deadlines else 0.5
            done, pending = wait(pending, timeout=wait_for, return_when=FIRST_COMPLETED)
            for future in done:
                index, layer = futures[future]
                try:
                    result = run.promote(future.result())
                except Exception as err:  # noqa: BLE001 - one layer must not stop the run
                    logger.error("Layer %s failed: %s", layer.name, err)
                    result = _result(layer, "failed", error=str(err), attempts=1)
                run.emit(
                    index,
                    total,
                    layer,
                    result.status,
                    result.error if result.status == "failed" else None,
                )
                results.append((index, result))
            now = time.monotonic()
            # A future that ended while the main thread was busy counts as done.
            expired = [
                f for f in pending if not f.done() and (d := deadline(f)) is not None and d <= now
            ]
            for future in expired:
                index, layer = futures[future]
                with lock:
                    elapsed = now - started[index]
                error = f"Timeout after {timeout}s"
                logger.error("Layer %s timed out after %ss", layer.name, timeout)
                run.emit(index, total, layer, "failed", error)
                results.append(
                    (
                        index,
                        _result(layer, "failed", duration_seconds=elapsed, error=error, attempts=1),
                    )
                )
                future.cancel()
                pending.discard(future)
                run.abandoned.add(layer_path(layer))
                timed_out = True
    finally:
        executor.shutdown(wait=not timed_out, cancel_futures=timed_out)
    return results


def _resolve_license(discovery: Discovery, options: LayerSourceOptions) -> ResolvedLicense | None:
    from portolan_cli.licensing import resolve_harvest_license

    if options.raw:
        return None
    return resolve_harvest_license(
        cli_license=options.license,
        cli_license_url=options.license_url,
        harvested_license_url=discovery.license_url,
    )


def _post_init(
    source_name: str, discovery: Discovery, resolved_license: ResolvedLicense | None
) -> Callable[[Path, list[Path], bool], None]:
    def post_init(out: Path, files: list[Path], fresh: bool) -> None:
        from portolan_cli.config import set_setting
        from portolan_cli.extract.common.orchestrator_base import seed_catalog_metadata
        from portolan_cli.formats import is_geoparquet
        from portolan_cli.metadata_extraction import ExtractedMetadata
        from portolan_cli.stac import update_stac_metadata

        # A plain Parquet output, from a table with no geometry, becomes a
        # tabular collection. add_files accepts it only with tabular support.
        if any(not is_geoparquet(path) for path in files):
            set_setting(out, "tabular.enabled", True)
        # Overwrite the init defaults only on a new catalog. A second
        # extraction into it must keep the root metadata (issue #832).
        if fresh and discovery.root_metadata is not None:
            title, description = discovery.root_metadata
            update_stac_metadata(out / "catalog.json", title=title, description=description)
        # The license must be in place before add_files checks for it (issue #686).
        # A source that harvests no catalog metadata still gets a metadata.yaml
        # for the --license value. Without it, add_files rejects the catalog.
        seed = discovery.catalog_seed
        if seed is None and resolved_license is not None:
            seed = ExtractedMetadata(source_type=source_name, source_url=discovery.source_url)
        seed_catalog_metadata(out, seed, resolved_license)

    return post_init


def _seed_collection(
    layer_source: LayerSource,
    source_url: str,
    collection_dir: Path,
    layer: LayerRef,
    meta: LayerMetadata,
) -> None:
    from portolan_cli.extract.common.metadata_seeding import seed_collection_metadata
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.stac import update_stac_metadata

    if meta.record is not None:
        logger.info("Using %s for %s", meta.record_label, layer.name)
        # No placeholder: a collection inherits the catalog's license and contact.
        seed_metadata_yaml(
            meta.record, collection_dir / ".portolan" / "metadata.yaml", placeholders=False
        )
        update_stac_metadata(
            collection_dir / "collection.json", title=meta.title, description=meta.description
        )
        return
    link = layer_source.source_link(source_url, layer)
    seed_collection_metadata(
        collection_dir,
        source_type=meta.source_type or layer_source.name,
        source_url=meta.source_url or (link.href if link else source_url),
        layer_name=meta.layer_name or layer.name,
        title=meta.title,
        description=meta.description,
        keywords=meta.keywords,
    )


def seed_collections(
    layer_source: LayerSource,
    discovery: Discovery,
    output_dir: Path,
    report: ExtractionReport,
) -> None:
    """Seed ``metadata.yaml`` for each successful layer's collection.

    ``layer_metadata`` runs for the layers in parallel, because it can fetch a
    remote record. Seeding then runs in report order.

    Args:
        layer_source: The source the layers come from.
        discovery: The discovery that holds the layers.
        output_dir: Catalog directory.
        report: The extraction report.
    """
    by_path = {layer_path(layer): layer for layer in discovery.layers}
    done = [
        (result, by_path[result.output_path])
        for result in report.layers
        if result.status == "success" and result.output_path in by_path
    ]
    if not done:
        return
    source_url = discovery.source_url
    with ThreadPoolExecutor(max_workers=_METADATA_WORKERS) as executor:
        metadata = list(
            executor.map(lambda item: layer_source.layer_metadata(source_url, item[1]), done)
        )
    for (result, layer), meta in zip(done, metadata, strict=True):
        if meta is not None and result.output_path:
            _seed_collection(
                layer_source,
                source_url,
                output_dir / PurePath(result.output_path).parent,
                layer,
                meta,
            )


def build_catalog(
    layer_source: LayerSource,
    discovery: Discovery,
    output_dir: Path,
    report: ExtractionReport,
    *,
    resolved_license: ResolvedLicense | None = None,
    catalog_id: str | None = None,
) -> None:
    """Build or extend the catalog around the layers that succeeded.

    Steps: init the catalog and add the files, register styles and legends,
    add a ``via`` link to each collection, seed collection metadata, then
    draw the thumbnails.

    Args:
        layer_source: The source the layers come from.
        discovery: The discovery that holds the layers.
        output_dir: Catalog directory.
        report: The extraction report.
        resolved_license: The license to seed (issue #686).
        catalog_id: Catalog id. None derives it from the directory name.
    """
    from portolan_cli.collection_thumbnail import ensure_collection_thumbnails
    from portolan_cli.extract.common.orchestrator_base import (
        add_source_links,
        init_extracted_catalog,
        register_collection_styles,
    )

    added = init_extracted_catalog(
        output_dir,
        report,
        catalog_id=catalog_id,
        title=discovery.catalog_title,
        description=discovery.catalog_description,
        post_init=_post_init(layer_source.name, discovery, resolved_license),
    )
    if added is None:
        return
    register_collection_styles(output_dir, report, include_legends=True)
    by_path = {layer_path(layer): layer for layer in discovery.layers}

    def link(result: LayerResult) -> SourceLink | None:
        layer = by_path.get(result.output_path or "")
        return layer_source.source_link(discovery.source_url, layer) if layer else None

    add_source_links(
        output_dir,
        report,
        url_builder=lambda source, result: found.href if (found := link(result)) else source,
        title_builder=lambda result: found.title if (found := link(result)) else result.name,
    )
    seed_collections(layer_source, discovery, output_dir, report)
    # Thumbnails come last, as in `add`. The renderer's GUI objects then never
    # meet the metadata thread pool above. A Tk object that a worker thread
    # frees aborts the process on Windows.
    files, versioned = added
    ensure_collection_thumbnails(
        output_dir,
        {path.parent.relative_to(output_dir).as_posix() for path in files},
        versioned_collections=versioned,
    )


def run_layer_extraction(
    layer_source: LayerSource,
    source: str,
    output_dir: Path,
    options: LayerSourceOptions | None = None,
    *,
    on_progress: Callable[[ExtractionProgress], None] | None = None,
) -> ExtractionReport:
    """Extract the layers of ``source`` into a Portolan catalog.

    Steps: discover, filter, check the license, resume, fetch with retry,
    save the report to ``.portolan/extraction-report.json``, then build the
    catalog unless ``options.raw`` is set.

    Args:
        layer_source: The source implementation.
        source: Service URL or other source locator.
        output_dir: Catalog directory.
        options: Extraction options. None uses the defaults.
        on_progress: Called with progress for each layer.

    Returns:
        The extraction report.

    Raises:
        MissingLicenseError: Before any download, when no license can be
            resolved and ``options.raw`` is not set.
        InputValidationError: Before any write, when a layer's ``path`` could
            leave the catalog.
    """
    from portolan_cli.extract.common.report import save_report

    options = options or LayerSourceOptions()
    with span("extract.discover") as stage:
        discovery = layer_source.discover(source, options)
        # A plugin source sets LayerRef.path. Check every discovered layer before
        # any write, because the catalog build reads the unfiltered list too.
        for layer in discovery.layers:
            layer_path(layer)
        stage.set(layers=len(discovery.layers))
    with span("extract.filter") as stage:
        layers = _select_layers(discovery, options)
        stage.set(layers=len(layers))
    if options.dry_run:
        return build_report(discovery, [_pending(layer) for layer in layers])

    resolved_license = _resolve_license(discovery, options)
    report_path = output_dir / ".portolan" / "extraction-report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    done_by_path, by_name = (
        _previous_results(report_path) if options.resume and report_path.exists() else ({}, {})
    )

    run = _Run(layer_source, discovery, output_dir, options, on_progress)
    total = len(layers)
    plan = [
        (
            index,
            layer,
            _skipped(layer)
            if layer.skip_reason
            else _resumed_result(layer, output_dir, done_by_path, by_name),
        )
        for index, layer in enumerate(layers)
    ]
    jobs = [(index, layer) for index, layer, kept in plan if kept is None]

    fetch_start = time.monotonic()
    with span("extract.fetch", layers=len(jobs), workers=options.workers):
        if options.workers > 1 and len(jobs) > 1:
            # Parallel downloads end in any order, so the kept layers report first.
            collected = [(index, kept) for index, _, kept in plan if kept is not None]
            for index, layer, kept in plan:
                if kept is not None:
                    _report_kept(run, index, total, layer)
            collected += _fetch_parallel(run, jobs, total)
        else:
            collected = _fetch_sequential(run, plan, total)
    wall_duration = time.monotonic() - fetch_start
    collected.sort(key=lambda item: item[0])
    # A timed-out worker can still write here. After this event, that worker
    # removes its own tree and the empty staging root.
    run.ended.set()
    shutil.rmtree(output_dir / ".portolan" / "staging", ignore_errors=True)

    with span("extract.report"):
        report = build_report(
            discovery,
            [result for _, result in collected],
            wall_duration_seconds=round(wall_duration, 3),
        )
        save_report(report, report_path)
    if not options.raw:
        with span("extract.build_catalog"):
            build_catalog(
                layer_source,
                discovery,
                output_dir,
                report,
                resolved_license=resolved_license,
                catalog_id=options.catalog_id,
            )
    return report


class LayerSourceExtractor:
    """An ``Extractor`` that runs a ``LayerSource`` through the shared driver."""

    def __init__(self, layer_source: LayerSource) -> None:
        self.layer_source = layer_source
        self.name = layer_source.name
        self.summary = layer_source.summary

    def extract(self, source: str, output_dir: Path, **options: Any) -> ExtractionReport:
        """Run ``run_layer_extraction``. ``options`` holds LayerSourceOptions fields."""
        on_progress = options.pop("on_progress", None)
        return run_layer_extraction(
            self.layer_source,
            source,
            output_dir,
            LayerSourceOptions(**options),
            on_progress=on_progress,
        )

    def cli_command(self) -> click.Command:
        """Build ``portolan extract <name>`` for the source.

        The command module is part of the CLI layer. It loads here, when the
        user runs the command.
        """
        layer_cli = importlib.import_module("portolan_cli.extract.common.layer_cli")
        command: click.Command = layer_cli.build_command(self.layer_source)
        return command


def extractor_from(layer_source: LayerSource) -> LayerSourceExtractor:
    """Wrap a ``LayerSource`` as an ``Extractor`` for ``portolan_extractors``.

    Args:
        layer_source: The source implementation.

    Returns:
        An extractor with the source's name and summary.
    """
    return LayerSourceExtractor(layer_source)
