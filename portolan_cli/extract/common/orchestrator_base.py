"""Shared post-extraction catalog lifecycle for extraction orchestrators."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from portolan_cli.extract.common.progress import MessageCallback
    from portolan_cli.extract.common.report import ExtractionReport, LayerResult
    from portolan_cli.metadata_extraction import ExtractedMetadata
    from portolan_cli.viz.style import LegendInfo
    from portolan_core.licensing import ResolvedLicense
    from portolan_core.query import ItemInfo
    from portolan_core.run_state import RunState

logger = logging.getLogger(__name__)


def collect_successful_parquet_files(
    output_dir: Path,
    report: ExtractionReport,
) -> list[Path]:
    """Return absolute paths of every successfully-extracted asset in the report."""
    return [
        output_dir / result.output_path
        for result in report.layers
        if result.status == "success" and result.output_path
    ]


def parquet_row_count(path: Path) -> int:
    """Return the row count of an extracted Parquet file, or 0 when the file is missing."""
    if not path.exists():
        return 0
    import pyarrow.parquet as pq

    from portolan_core.trace import count

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
    on_message: MessageCallback | None = None,
) -> tuple[list[Path], set[str]] | None:
    """Initialize a catalog and add the extracted assets."""
    from portolan_cli.catalog import CatalogState, detect_state, init_catalog
    from portolan_cli.extract.common.progress import emit_message
    from portolan_cli.formats import run_scope

    parquet_files = collect_successful_parquet_files(output_dir, report)
    if not parquet_files:
        return None

    # An existing catalog gets the new collections added. It already has a license for the add gate.
    fresh = detect_state(output_dir) is not CatalogState.MANAGED
    if fresh:
        # license_id=None: a template would make the seeder's O_EXCL create a no-op.
        # Print what init_catalog had to guess, the way `init` does.
        _, init_warnings = init_catalog(
            output_dir,
            catalog_id=catalog_id,
            title=title,
            description=description,
            license_id=None,
        )
        for message in init_warnings:
            emit_message(on_message, "warn", message)

    elif catalog_id is not None:
        # The catalog already exists and keeps the id it was created with. Say so,
        # rather than accept the flag and change nothing.
        emit_message(
            on_message,
            "warn",
            f"Ignored --id '{catalog_id}'. {output_dir} is already a Portolan "
            "catalog and keeps the id it was created with.",
        )

    if post_init is not None:
        # The hook reads footers. The add reads them again through the run memo.
        with run_scope(state):
            post_init(output_dir, parquet_files, fresh)

    from portolan_cli.report import reporting

    # The add reports each notice here, as `add` prints it.
    with reporting(on_message):
        added = add_extracted_files(parquet_files, output_dir, state=state)
    return parquet_files, {item.collection_id for item in added}


def add_extracted_files(
    paths: list[Path], catalog_root: Path, *, state: RunState | None = None
) -> list[ItemInfo]:
    """Run ``add_files`` and log each file that it leaves out of the catalog."""
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
    on_message: MessageCallback | None = None,
) -> None:
    """Register discovered style (and optionally legend) sidecars as STAC assets."""
    from portolan_cli.report import reporting
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
            with reporting(on_message):
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
    from portolan_cli.stac_parquet import stamp_file_fields
    from portolan_core.json_io import write_json_atomic

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
    """Add a provenance ``via`` link to each successfully-extracted collection."""
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

        # The link is published, so it carries no credential.
        href = sanitize_published_string(url_builder(source_url, layer))
        if href is not None:
            add_via_link(collection_path, href, title=title_builder(layer))


def seed_catalog_metadata(
    output_dir: Path,
    extracted: ExtractedMetadata | None,
    license_override: ResolvedLicense | None = None,
    *,
    on_message: MessageCallback | None = None,
) -> None:
    """Seed the catalog-level ``metadata.yaml`` from harvested service metadata."""
    from portolan_cli.extract.common.progress import emit_message
    from portolan_cli.metadata_seeding import seed_metadata_yaml

    if extracted is None:
        return

    metadata_path = output_dir / ".portolan" / "metadata.yaml"
    if seed_metadata_yaml(extracted, metadata_path, license_override=license_override):
        emit_message(on_message, "info", f"Seeded metadata.yaml from {extracted.source_type}")
        if not (extracted.producer or extracted.attribution):
            # A producer is a fact of the source. Portolan does not invent one.
            emit_message(
                on_message,
                "warn",
                "No producer provider: the source publishes no author and no attribution. "
                f"Add a provider with the producer role to {metadata_path}. Until then, "
                "check reports PTL-PRV-001.",
            )
        # Only the person who runs extract knows the host. A collection takes an
        # inherited contact only on its first add, so the warning names the collection file.
        emit_message(
            on_message,
            "warn",
            "No host provider: set contact.name and contact.email in "
            f"{output_dir / '<collection>' / '.portolan' / 'metadata.yaml'}, then run "
            "'portolan add --force <collection>'. Until then, check reports PTL-PRV-002.",
        )
