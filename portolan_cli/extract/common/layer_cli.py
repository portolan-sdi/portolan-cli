"""The ``portolan extract <name>`` command for a LayerSource (#947).

``extractor_from(source).cli_command()`` builds the command here. It has the
options that every layer-based extractor shares, and it reports through the
same helpers as the built-in extract commands. This module is part of the
CLI layer.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from portolan_cli.cli import should_output_json
from portolan_cli.errors import MissingLicenseError
from portolan_cli.extract.common import cli_helpers

if TYPE_CHECKING:
    from portolan_cli.extract.common.source import LayerSource


def build_command(layer_source: LayerSource) -> click.Command:
    """Build the Click command that runs ``layer_source``.

    Args:
        layer_source: The source to extract.

    Returns:
        A command named after the source.
    """
    name = layer_source.name
    command_name = f"extract-{name}"

    @click.command(name, help=layer_source.summary)
    @cli_helpers.source_arguments
    @click.option(
        "--layers",
        type=str,
        default=None,
        help="Include layers matching glob patterns (comma-separated).",
    )
    @click.option(
        "--exclude-layers",
        type=str,
        default=None,
        help="Exclude layers matching glob patterns (comma-separated).",
    )
    @click.option(
        "--retries",
        type=int,
        default=3,
        help="Attempts per layer (default: 3).",
    )
    @cli_helpers.run_options("layers")
    @cli_helpers.catalog_options
    @click.pass_context
    def command(
        ctx: click.Context,
        catalog_id: str | None,
        license_id: str | None,
        license_url: str | None,
        url: str,
        output_dir: Path | None,
        layers: str | None,
        exclude_layers: str | None,
        retries: int,
        resume: bool,
        dry_run: bool,
        json_output: bool,
        auto: bool,
        raw: bool,
    ) -> None:
        from portolan_cli.extract.common.source import LayerSourceOptions, run_layer_extraction
        from portolan_cli.output import detail, info

        use_json = should_output_json(ctx, json_output)
        cli_helpers.check_extract_catalog_id(
            catalog_id, raw=raw, url=url, command=command_name, use_json=use_json
        )
        target = output_dir if output_dir is not None else Path(f"{name}_extract")
        include = cli_helpers.parse_filter_patterns(layers)
        exclude = cli_helpers.parse_filter_patterns(exclude_layers)
        cli_helpers.confirm_extraction(
            url=url,
            output_dir=target,
            include=include,
            exclude=exclude,
            filter_label="Layer filter",
            line=info,
            sub_line=detail,
            auto=auto,
            dry_run=dry_run,
            use_json=use_json,
        )
        options = LayerSourceOptions(
            layer_filter=include,
            layer_exclude=exclude,
            resume=resume,
            dry_run=dry_run,
            raw=raw,
            retries=retries,
            catalog_id=catalog_id,
            license=license_id,
            license_url=license_url,
        )
        try:
            report = run_layer_extraction(
                layer_source,
                url,
                target,
                options,
                on_progress=None if use_json else cli_helpers.make_extract_progress_callback(),
            )
        except MissingLicenseError as e:
            cli_helpers.fail_missing_license(e, use_json=use_json, url=url, command=command_name)
        except Exception as e:
            cli_helpers.output_extract_error(
                use_json, type(e).__name__, f"Extraction failed: {e}", url, command=command_name
            )
            raise SystemExit(1) from None
        cli_helpers.output_extract_result(report, target, use_json, dry_run, command=command_name)

    return command
