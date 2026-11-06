"""``portolan version rollback``, ``prune``, and ``refresh`` (#947).

Only the Iceberg backend implements these commands. portolan-cli declares
them as ``version.rollback``, ``version.prune``, and ``version.refresh`` in the
``portolan.commands`` entry point group, the same way an outside plugin adds a
command. The module imports Click and ``portolan_cli.api`` only, and
neither DuckDB nor httpx.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any, NoReturn

import click

from portolan_cli import api

if TYPE_CHECKING:
    from portolan_cli.api import VersioningBackend


class BackendRequiredError(Exception):
    """Raised when a required backend is not configured."""

    def __init__(self, command_name: str, required: str, current: str | None) -> None:
        self.command_name = command_name
        self.required = required
        self.current = current or "file"
        super().__init__(
            f"'portolan version {command_name}' requires the '{required}' backend. "
            f"Current backend: '{self.current}'"
        )


def require_iceberg_backend(
    catalog_path: Path, command_name: str, collection: str | None = None
) -> VersioningBackend:
    """Load the iceberg backend or raise BackendRequiredError.

    The backend is a per-collection setting, and ``add``, ``push``, and
    ``pull`` read it for the collection they act on. This guard reads it
    the same way (#1059), so a collection that sets ``backend: iceberg``
    under a file-backend catalog gets its rollback and prune.

    Args:
        catalog_path: Path to catalog root.
        command_name: Name of the command (for error messages).
        collection: The collection the command acts on, or None.

    Returns:
        The iceberg VersioningBackend instance.

    Raises:
        BackendRequiredError: If the collection's backend is not 'iceberg'.
    """
    backend_name = api.get_setting("backend", catalog_path=catalog_path, collection=collection)
    if backend_name != "iceberg":
        raise BackendRequiredError(command_name, "iceberg", backend_name)
    backend: VersioningBackend = api.get_backend("iceberg", catalog_root=catalog_path)
    return backend


def _use_json(ctx: click.Context, json_flag: bool) -> bool:
    """Honor the root ``--format json`` option and the command's ``--json`` flag."""
    root_obj = ctx.find_root().obj or {}
    return json_flag or root_obj.get("format", "text") == "json"


def _fail(command: str, error_type: str, message: str, *, use_json: bool) -> NoReturn:
    """Report an error as a JSON envelope or an output line, then exit 1."""
    if use_json:
        detail = api.ErrorDetail(type=error_type, message=message)
        click.echo(api.error_envelope(command, [detail]).to_json())
    else:
        api.error(message)
    raise SystemExit(1)


def _iceberg_backend(
    ctx: click.Context, catalog_path: Path | None, json_flag: bool, name: str, collection: str
) -> Any:
    """Resolve the catalog root and the iceberg backend, or exit with an error."""
    command = f"version {name}"
    use_json = _use_json(ctx, json_flag)
    if catalog_path is None:
        catalog_path = api.find_catalog_root()
        if catalog_path is None:
            _fail(
                command,
                "NotACatalogError",
                "fatal: not a portolan catalog (or any parent up to mount point)",
                use_json=use_json,
            )
    try:
        return require_iceberg_backend(catalog_path, name, collection)
    except (BackendRequiredError, ValueError) as e:
        # get_backend raises ValueError when the backend cannot start, for
        # example without PORTOLAN_ICEBERG_URI in the environment.
        _fail(command, "BackendError", str(e), use_json=use_json)


_CATALOG_OPTION = click.option(
    "--catalog",
    "catalog_path",
    type=click.Path(path_type=Path),
    default=None,
    help="Path to catalog root (default: auto-detect).",
)


@click.command()
@click.argument("collection")
@click.argument("target_version")
@_CATALOG_OPTION
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def rollback(
    ctx: click.Context,
    collection: str,
    target_version: str,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    r"""Rollback a collection to a previous version.

    Uses Iceberg's native snapshot management to set the current snapshot
    pointer back to TARGET_VERSION. No data is copied — this is instant.

    \b
    Examples:
        portolan version rollback boundaries 1.0.0
        portolan version rollback boundaries 2.0.0 --json
    """
    use_json = _use_json(ctx, json_output)
    backend = _iceberg_backend(ctx, catalog_path, json_output, "rollback", collection)

    try:
        restored = backend.rollback(collection, target_version)
    except Exception as e:  # noqa: BLE001 - any backend failure is reported
        _fail("version rollback", type(e).__name__, str(e), use_json=use_json)

    if use_json:
        data = {
            "collection": collection,
            "restored_version": restored.version,
            "created": restored.created.isoformat(),
        }
        click.echo(api.success_envelope("version rollback", data).to_json())
    else:
        api.success(f"Rolled back '{collection}' to version {restored.version}")


@click.command()
@click.argument("collection")
@click.option(
    "--keep",
    "-k",
    type=click.IntRange(min=1),
    default=5,
    show_default=True,
    help="Number of recent versions to keep.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would be pruned without deleting.",
)
@_CATALOG_OPTION
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def prune(
    ctx: click.Context,
    collection: str,
    keep: int,
    dry_run: bool,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    r"""Remove old versions, keeping the N most recent.

    \b
    Examples:
        portolan version prune boundaries                # Keep 5 most recent
        portolan version prune boundaries --keep 3       # Keep 3 most recent
        portolan version prune boundaries --dry-run      # Preview without deleting
    """
    use_json = _use_json(ctx, json_output)
    backend = _iceberg_backend(ctx, catalog_path, json_output, "prune", collection)

    try:
        pruned = backend.prune(collection, keep=keep, dry_run=dry_run)
    except Exception as e:  # noqa: BLE001 - any backend failure is reported
        _fail("version prune", type(e).__name__, str(e), use_json=use_json)

    if use_json:
        data = {
            "collection": collection,
            "pruned": [{"version": v.version, "created": v.created.isoformat()} for v in pruned],
            "kept": keep,
            "dry_run": dry_run,
        }
        click.echo(api.success_envelope("version prune", data).to_json())
        return
    prefix = "[DRY RUN] " if dry_run else ""
    if not pruned:
        api.info(f"{prefix}Nothing to prune (≤{keep} versions exist)")
        return
    action = "Would prune" if dry_run else "Pruned"
    api.info(f"{prefix}{action} {len(pruned)} version(s), keeping {keep}:")
    for v in pruned:
        api.detail(f"  {v.version}  {v.created.strftime('%Y-%m-%d %H:%M:%S')}")


@click.command()
@click.argument("collection")
@_CATALOG_OPTION
@click.option("--json", "json_output", is_flag=True, help="Output as JSON.")
@click.pass_context
def refresh(
    ctx: click.Context,
    collection: str,
    catalog_path: Path | None,
    json_output: bool,
) -> None:
    r"""Rewrite the Iceberg fields of a collection from its table.

    The maintenance of a hosted catalog writes a new table metadata file.
    This command reads the table again and writes the current
    iceberg:metadata_location and the other Iceberg fields into
    collection.json. With a remote configured, it also uploads
    collection.json.

    \b
    Examples:
        portolan version refresh boundaries
        portolan version refresh boundaries --json
    """
    use_json = _use_json(ctx, json_output)
    backend = _iceberg_backend(ctx, catalog_path, json_output, "refresh", collection)
    root = catalog_path if catalog_path is not None else api.find_catalog_root()
    remote = api.get_setting("remote", catalog_path=root, collection=collection)

    try:
        result = backend.refresh(collection, remote=remote)
    except Exception as e:  # noqa: BLE001 - any backend failure is reported
        _fail("version refresh", type(e).__name__, str(e), use_json=use_json)

    if use_json:
        data = {
            "collection": collection,
            "previous_location": result.previous_location,
            "metadata_location": result.metadata_location,
            "changed": result.changed,
            "remote": remote,
        }
        click.echo(api.success_envelope("version refresh", data).to_json())
        return
    if result.changed:
        api.success(
            f"Refreshed '{collection}'. The metadata location is now {result.metadata_location}"
        )
    else:
        api.info(f"'{collection}' is current. The metadata location is {result.metadata_location}")
    if remote is not None:
        api.detail(f"Uploaded {collection}/collection.json to {remote}")
