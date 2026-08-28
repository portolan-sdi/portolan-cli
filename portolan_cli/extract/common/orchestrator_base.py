"""Shared post-extraction catalog lifecycle for extraction orchestrators.

The ArcGIS, WFS, and Carto orchestrators each run the same lifecycle after their
(genuinely source-specific) layer extraction finishes:

1. Collect the successfully-extracted assets.
2. Initialize a Portolan catalog and add those assets.
3. Register any discovered style/legend sidecars as STAC assets.
4. Add a provenance ``via`` link to each collection.
5. Seed the catalog-level ``metadata.yaml`` from harvested service metadata.

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

import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.extract.common.report import ExtractionReport, LayerResult
    from portolan_cli.licensing import ResolvedLicense
    from portolan_cli.metadata_extraction import ExtractedMetadata

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


def init_extracted_catalog(
    output_dir: Path,
    report: ExtractionReport,
    *,
    catalog_id: str | None = None,
    title: str | None,
    description: str | None,
    post_init: Callable[[Path, list[Path], bool], None] | None = None,
) -> list[Path] | None:
    """Initialize a catalog and add the extracted assets.

    Shared core of every orchestrator's ``_auto_init_catalog``: collect assets,
    bail out if there are none, initialize the catalog, run an optional
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

    Returns:
        The list of added parquet files, or ``None`` when there was nothing to
        add (the caller should then stop — no catalog was created).
    """
    from portolan_cli.catalog import CatalogState, add_files, detect_state, init_catalog

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
        if init_warnings:
            from portolan_cli.output import warn

            for message in init_warnings:
                warn(message)

    elif catalog_id is not None:
        # The catalog already exists and keeps the id it was created with. Say so,
        # rather than accept the flag and change nothing (issue #821).
        from portolan_cli.output import warn

        warn(
            f"Ignored --id '{catalog_id}'. {output_dir} is already a Portolan "
            "catalog and keeps the id it was created with."
        )

    if post_init is not None:
        post_init(output_dir, parquet_files, fresh)

    add_files(paths=parquet_files, catalog_root=output_dir)
    return parquet_files


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
                logger.debug("Registered %d legend(s) for %s", len(legends), result.name)


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

        add_via_link(
            collection_path,
            url_builder(source_url, layer),
            title=title_builder(layer),
        )


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
    from portolan_cli.output import info

    if extracted is None:
        return

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path, license_override=license_override):
        info(f"Seeded metadata.yaml from {extracted.source_type}")
