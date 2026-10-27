"""Shared post-extraction catalog lifecycle for extraction orchestrators.

The ArcGIS, WFS, and Carto orchestrators each run the same lifecycle after their
(genuinely source-specific) layer extraction finishes:

1. Collect the successfully-extracted assets.
2. Initialize a Portolan catalog and add those assets.
3. Register any discovered style/legend sidecars as STAC assets, with
   ``file:size`` and ``file:checksum`` (PTL-AST-003).
4. Add a provenance ``via`` link to each collection.
5. Seed the catalog-level ``metadata.yaml`` from harvested service metadata.
   The source's attribution names the producer (PTL-PRV-001).
6. Give each collection a thumbnail (PTL-VIZ-001). ``source.build_catalog``
   does it last, as ``add`` does.

The output of ``portolan extract`` passes ``portolan check`` when the source
publishes an attribution.

Only three things actually differ between the sources: the per-source title used
for a ``via`` link, the source-URL that link points at, and the metadata
serializer that produces the catalog-level ``ExtractedMetadata``. This module
provides the shared skeleton and parametrizes those pieces via small callables,
so each orchestrator keeps only its source-specific glue (: all logic
lives in the library layer).

All library imports are function-local, matching the orchestrators' existing
pattern, to keep import time low and sidestep import cycles through ``add.py``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.extract.common.report import ExtractionReport, LayerResult
    from portolan_cli.licensing import ResolvedLicense
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.query import ItemInfo
    from portolan_cli.run_state import RunState
    from portolan_cli.viz.style import LegendInfo

logger = logging.getLogger(__name__)


def collect_successful_parquet_files(
    output_dir: Path,
    report: ExtractionReport,
) -> list[Path]:
    """Return absolute paths of every successfully-extracted asset in the report.

    Layers that failed, were skipped, or produced no output are excluded. The
    returned list is what gets handed to ``add_files``; an empty list means there
    is nothing to build a catalog around.

    Args:
        output_dir: The catalog output directory (asset paths are relative to it).
        report: The extraction report.

    Returns:
        Absolute paths to successfully-extracted parquet files.
    """
    return [
        output_dir / result.output_path
        for result in report.layers
        if result.status == "success" and result.output_path
    ]


def parquet_row_count(path: Path) -> int:
    """Return the row count of an extracted Parquet file, or 0 when the file is missing.

    The footer read counts as one ``parquet.opens``.

    Args:
        path: The Parquet file that a source wrote.

    Returns:
        The number of rows in the file.
    """
    if not path.exists():
        return 0
    import pyarrow.parquet as pq

    from portolan_cli.trace import count

    count("parquet.opens")
    return int(pq.read_metadata(path).num_rows)


def init_extracted_catalog(
    output_dir: Path,
    report: ExtractionReport,
    *,
    catalog_id: str | None = None,
    title: str | None,
    description: str | None,
    post_init: Callable[[Path, list[Path], bool], None] | None = None,
    state: RunState | None = None,
) -> tuple[list[Path], set[str]] | None:
    """Initialize a catalog and add the extracted assets.

    Shared core of ``build_catalog`` and the ImageServer catalog step: collect
    assets, bail out if there are none, initialize the catalog, run an optional
    ``post_init`` hook (used for source-specific work that must happen *after*
    ``init_catalog`` but *before* ``add_files`` — e.g. re-applying richer catalog
    metadata, or enabling tabular support for non-geo outputs), then add the
    assets.

    When ``output_dir`` is already a Portolan catalog, the ``init_catalog`` step
    is skipped and the assets are added as new collections (issue #767). This lets
    a caller extract more layers into a catalog they already built, rather than
    abort after the download wrote the data.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report.
        catalog_id: Catalog id for ``init_catalog``. None derives it from the
            output directory name, which is the behavior before issue #821.
        title: Catalog title for ``init_catalog`` (already filtered by caller).
        description: Catalog description for ``init_catalog``.
        post_init: Optional hook called as
            ``post_init(output_dir, parquet_files, fresh)`` between ``init_catalog``
            and ``add_files``. ``fresh`` is True only when this call created the
            catalog. A hook must not overwrite root catalog metadata when ``fresh``
            is False, because the catalog already belongs to an earlier extraction
            (issue #832).
        state: The run state of the extraction. The add stages its versions
            in it, and the caller commits them. None commits the add at once.

    Returns:
        The added parquet files and the collections that this add wrote a
        version for, or ``None`` when there was nothing to add. The caller
        then stops, because no catalog exists.
    """
    from portolan_cli.catalog import CatalogState, detect_state, init_catalog
    from portolan_cli.output import warn

    parquet_files = collect_successful_parquet_files(output_dir, report)
    if not parquet_files:
        return None

    # A directory that is already a Portolan catalog gets the new collections added
    # to it, not an abort after the download already wrote the data (issue #767). A
    # caller who runs extract into a directory they already extracted into wants the
    # new layers added, not a refusal. init_catalog raises CatalogAlreadyExistsError
    # on a MANAGED directory, so skip it. The catalog already carries a license, so
    # the add license gate (issue #686) still passes.
    fresh = detect_state(output_dir) is not CatalogState.MANAGED
    if fresh:
        # license_id=None: extract owns metadata.yaml, seeding it from harvested
        # service metadata in post_init below. Writing a template here first would
        # make that seeder's O_EXCL create a no-op and drop everything harvested
        # (issue #686).
        # init_catalog reports what it had to guess. `init` prints those warnings,
        # so extract prints them too. Otherwise a derived id that names a tooling
        # artifact reaches a published catalog unflagged (issue #821).
        _, init_warnings = init_catalog(
            output_dir,
            catalog_id=catalog_id,
            title=title,
            description=description,
            license_id=None,
        )
        for message in init_warnings:
            warn(message)

    elif catalog_id is not None:
        # The catalog already exists and keeps the id it was created with. Say so,
        # rather than accept the flag and change nothing (issue #821).
        warn(
            f"Ignored --id '{catalog_id}'. {output_dir} is already a Portolan "
            "catalog and keeps the id it was created with."
        )

    if post_init is not None:
        post_init(output_dir, parquet_files, fresh)

    added = add_extracted_files(parquet_files, output_dir, state=state)
    return parquet_files, {item.collection_id for item in added}


def add_extracted_files(
    paths: list[Path], catalog_root: Path, *, state: RunState | None = None
) -> list[ItemInfo]:
    """Run ``add_files`` and log each file that it leaves out of the catalog.

    ``add_files`` prints nothing itself. A file that ``ignored_files`` matches,
    or that has no valid collection id, would otherwise vanish in silence.
    With ``state``, the add stages its versions there and the caller commits
    them. Without it, the add commits its own.

    Returns:
        The items that the add wrote.
    """
    from portolan_cli.add import SKIPS_THAT_TRACK, add_files

    reasons: dict[Path, str] = {}
    added, _skipped, failures = add_files(
        paths=paths, catalog_root=catalog_root, skip_reasons=reasons, state=state
    )
    for path, reason in reasons.items():
        if reason not in SKIPS_THAT_TRACK:
            logger.warning("Skipping %s: %s", path.name, reason)
    for failure in failures:
        logger.warning("Could not add %s: %s", failure.path.name, failure.error)
    return added


def register_collection_styles(
    output_dir: Path,
    report: ExtractionReport,
    *,
    include_legends: bool = False,
) -> None:
    """Register discovered style (and optionally legend) sidecars as STAC assets.

    Extraction writes style/legend files next to each collection's data; this
    scans every successful collection and registers whatever it finds (Issue
    #490 styles, Issue #498 legends).

    Args:
        output_dir: The catalog output directory.
        report: The extraction report.
        include_legends: Also discover and register legend sidecars (WFS/WMS).
    """
    from portolan_cli.viz.style import (
        discover_legends,
        discover_styles,
        register_legend_assets,
        register_style_assets,
    )

    for result in report.layers:
        if result.status != "success" or not result.output_path:
            continue
        collection_dir = output_dir / Path(result.output_path).parent

        styles = discover_styles(collection_dir)
        if styles:
            register_style_assets(collection_dir, styles)
            logger.debug("Registered %d style(s) for %s", len(styles), result.name)

        if include_legends:
            legends = discover_legends(collection_dir)
            if legends:
                register_legend_assets(collection_dir, legends)
                # Each asset states its size and checksum (PTL-AST-003).
                _stamp_legends(collection_dir, legends)
                logger.debug("Registered %d legend(s) for %s", len(legends), result.name)


def _stamp_legends(collection_dir: Path, legends: list[LegendInfo]) -> None:
    """Write ``file:size`` and ``file:checksum`` on the legend assets (PTL-AST-003)."""
    from portolan_cli.json_io import write_json_atomic
    from portolan_cli.stac_parquet import stamp_file_fields

    collection_json = collection_dir / "collection.json"
    if not collection_json.exists():
        return
    data = json.loads(collection_json.read_text(encoding="utf-8"))
    assets = data.get("assets", {})
    keys = [legend.key for legend in legends if legend.key in assets]
    stamped = [stamp_file_fields(assets[key], collection_dir) for key in keys]
    if any(stamped):
        write_json_atomic(collection_json, data)


def add_source_links(
    output_dir: Path,
    report: ExtractionReport,
    *,
    url_builder: Callable[[str, LayerResult], str],
    title_builder: Callable[[LayerResult], str],
) -> None:
    """Add a provenance ``via`` link to each successfully-extracted collection.

    Per Issue #353 every collection points back at the source it came from. The
    href and title are source-specific, so they are supplied as callables.

    Args:
        output_dir: The catalog output directory.
        report: The extraction report (its ``source_url`` is passed to
            ``url_builder``).
        url_builder: Builds the ``via`` href from ``(source_url, layer)``.
        title_builder: Builds the ``via`` link title from ``layer``.
    """
    from portolan_cli.extract.common.urls import sanitize_published_string
    from portolan_cli.stac import add_via_link

    source_url = report.source_url

    for layer in report.layers:
        if layer.status != "success" or not layer.output_path:
            continue

        # Derive the collection directory from the asset's parent so nested
        # layouts like "service/layer/layer.parquet" resolve correctly.
        collection_path = output_dir / Path(layer.output_path).parent / "collection.json"
        if not collection_path.exists():
            continue

        # The link is published, so it carries no credential (issue #981).
        href = sanitize_published_string(url_builder(source_url, layer))
        if href is not None:
            add_via_link(collection_path, href, title=title_builder(layer))


def seed_catalog_metadata(
    output_dir: Path,
    extracted: ExtractedMetadata | None,
    license_override: ResolvedLicense | None = None,
) -> None:
    """Seed the catalog-level ``metadata.yaml`` from harvested service metadata.

    Writes ``{output_dir}/.portolan/metadata.yaml`` from the given serialized
    metadata, emitting a user-facing confirmation when a file is created. A
    ``None`` argument (no metadata harvested) is a no-op.

    Args:
        output_dir: The catalog output directory.
        extracted: Source-agnostic metadata to seed, or ``None`` to skip.
        license_override: License resolved from the command line, which wins over
            anything harvested (issue #686).
    """
    from portolan_cli.metadata_seeding import seed_metadata_yaml
    from portolan_cli.output import info, warn

    if extracted is None:
        return

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path, license_override=license_override):
        info(f"Seeded metadata.yaml from {extracted.source_type}")
        if not (extracted.producer or extracted.attribution):
            # A producer is a fact of the source. Portolan does not invent one.
            warn(
                "No producer provider: the source publishes no author and no attribution. "
                f"Add a provider with the producer role to {metadata_path}. Until then, "
                "check reports PTL-PRV-001."
            )
        # The host serves the catalog. Only the person who runs extract knows it,
        # and a placeholder is never published as one (issue #1039).
        # A collection takes an inherited contact only on its first add, so the
        # warning names the collection file, which a re-add applies (#755).
        warn(
            "No host provider: set contact.name and contact.email in "
            f"{output_dir / '<collection>' / '.portolan' / 'metadata.yaml'}, then run "
            "'portolan add --force <collection>'. Until then, check reports PTL-PRV-002."
        )
